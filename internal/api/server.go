// Package api defines the HTTP API server and request handlers for job submission,
// status polling, and event streaming. The Server manages authenticated requests,
// spawns background job orchestrators, and gracefully shuts down in-flight jobs.
package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/notify"
	"cvmfs.io/prepub/internal/spool"
	"cvmfs.io/prepub/pkg/observe"
)

// maxTarSize is the maximum accepted tar body (10 GiB).
const maxTarSize = 10 << 30

// Server is the HTTP API server for job submission, status queries, and event streaming.
// It enforces bearer token authentication and manages background job goroutines.
type Server struct {
	// httpServer is the underlying HTTP server instance.
	httpServer *http.Server
	// router is the Gorilla mux router for path dispatching.
	router *mux.Router
	// obs provides logging, tracing, and metrics.
	obs *observe.Provider
	// apiToken is the expected bearer token for authenticated routes. Empty disables auth (dev mode).
	apiToken string
	// orch is the orchestrator instance that executes jobs.
	orch *Orchestrator
	// sp is the spool manager for persistent job state.
	sp *spool.Spool
	// notifyBus is the event bus for job state changes.
	notifyBus *notify.Bus
	// spoolRoot is the root directory for job state storage.
	spoolRoot string
	// stagingRoot is the operator-configured directory from which tar_path
	// references (JSON submissions) are allowed.  Empty disables JSON/tar_path
	// mode — callers must upload the tar as multipart/form-data instead.
	stagingRoot string
	// jobWg tracks all background job goroutines so Shutdown can wait for them
	// to reach a terminal state before the process exits.
	jobWg sync.WaitGroup
}

// New creates a new API server.
// apiToken is the expected bearer token for authenticated routes.
// Pass an empty string to disable authentication (development only).
// stagingRoot, when non-empty, enables the JSON tar_path submission mode and
// restricts acceptable tar_path values to files within that directory tree.
func New(obs *observe.Provider, apiToken string, orch *Orchestrator, sp *spool.Spool, nb *notify.Bus, spoolRoot, stagingRoot string) *Server {
	router := mux.NewRouter()
	s := &Server{
		router:      router,
		obs:         obs,
		apiToken:    apiToken,
		orch:        orch,
		sp:          sp,
		notifyBus:   nb,
		spoolRoot:   spoolRoot,
		stagingRoot: stagingRoot,
		httpServer: &http.Server{
			Handler: router,
		},
	}

	// Unauthenticated routes.
	s.router.Handle("/api/v1/metrics", promhttp.Handler())
	s.router.HandleFunc("/api/v1/health", s.health).Methods("GET")

	// Critical #4: All job routes require a valid bearer token.
	auth := s.router.PathPrefix("/api/v1/jobs").Subrouter()
	auth.Use(s.requireAuth)
	auth.HandleFunc("", s.listJobs).Methods("GET")
	auth.HandleFunc("", s.submitJob).Methods("POST")
	auth.HandleFunc("/{id}", s.getJob).Methods("GET")
	auth.HandleFunc("/{id}/abort", s.abortJobHandler).Methods("POST")
	auth.HandleFunc("/{id}/events", s.jobEvents).Methods("GET")

	return s
}

// requireAuth is a middleware that validates the Authorization: Bearer <token> header.
// If the server was created with an empty token, auth is skipped (dev mode).
func (s *Server) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.apiToken == "" {
			next.ServeHTTP(w, r)
			return
		}

		authHeader := r.Header.Get("Authorization")
		token := strings.TrimPrefix(authHeader, "Bearer ")
		if token == authHeader || token == "" {
			http.Error(w, `{"error":"missing or malformed Authorization header"}`, http.StatusUnauthorized)
			return
		}

		if subtle.ConstantTimeCompare([]byte(token), []byte(s.apiToken)) != 1 {
			s.obs.Logger.Warn("rejected request with invalid API token",
				"remote_addr", r.RemoteAddr,
				"method", r.Method,
				"path", r.URL.Path,
			)
			http.Error(w, `{"error":"invalid token"}`, http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// ListenAndServe starts the HTTP server on addr and blocks until the server
// exits (either due to an error or a call to Shutdown).
func (s *Server) ListenAndServe(addr string) error {
	s.httpServer.Addr = addr
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully stops the HTTP server and waits for all background job
// goroutines and webhook deliveries to finish. The provided context caps the
// total wait—if it expires before all jobs complete, Shutdown returns ctx.Err()
// and the caller should force-exit.
func (s *Server) Shutdown(ctx context.Context) error {
	httpErr := s.httpServer.Shutdown(ctx)

	// Wait for in-flight jobs and their webhook goroutines, but respect the deadline.
	done := make(chan struct{})
	go func() {
		s.jobWg.Wait()
		s.orch.webhookWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		if httpErr == nil {
			httpErr = ctx.Err()
		}
	}
	return httpErr
}

// submitJob handles POST /api/v1/jobs
//
// Two submission modes are supported, selected by Content-Type:
//
// ── Multipart upload (Content-Type: multipart/form-data) ─────────────────────
//
//	repo        — repository name (e.g. "software.cern.ch")
//	path        — gateway lease sub-path (e.g. "atlas/24.0")
//	tar         — the tar file to publish (binary)
//	tar_sha256  — optional hex-encoded SHA-256 of the tar; verified if present
//	webhook_url — optional URL to POST when the job reaches a terminal state
//
// ── Staged tar reference (Content-Type: application/json) ────────────────────
//
// Used when the tar has already been transferred to the server's staging
// directory (e.g. via rsync).  Requires --staging-root to be configured.
//
//	{
//	  "repo":        "software.cern.ch",
//	  "path":        "atlas/24.0",
//	  "tar_path":    "/staging/atlas/payload-abc123.tar",
//	  "tar_sha256":  "e3b0c44...",   // required — verified before accepting job
//	  "webhook_url": "https://..."   // optional
//	}
//
// Returns 202 Accepted with {"job_id": "<uuid>"}.  The caller should poll
// listJobs handles GET /api/v1/jobs.
// It scans every spool state directory and returns a JSON array of all jobs
// (active and terminal), sorted by creation time newest-first.
// Individual manifests that cannot be read are silently skipped so a single
// corrupt entry does not break the list.
func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	_, span := s.obs.Tracer.Start(r.Context(), "api.list_jobs")
	defer span.End()

	allStates := []job.State{
		job.StateIncoming,
		job.StateStaging,
		job.StateUploading,
		job.StateDistributing,
		job.StateLeased,
		job.StateCommitting,
		job.StatePublished,
		job.StateFailed,
		job.StateAborted,
	}

	type jobEntry struct {
		JobID            string    `json:"job_id"`
		State            string    `json:"state"`
		Repo             string    `json:"repo"`
		Path             string    `json:"path,omitempty"`
		TagName          string    `json:"tag_name,omitempty"`
		NObjects         int       `json:"n_objects,omitempty"`
		NBytesRaw        int64     `json:"n_bytes_raw,omitempty"`
		NBytesCompressed int64     `json:"n_bytes_compressed,omitempty"`
		NewRootHash      string    `json:"new_root_hash,omitempty"`
		Error            string    `json:"error,omitempty"`
		CreatedAt        time.Time `json:"created_at"`
		UpdatedAt        time.Time `json:"updated_at"`
	}

	var jobs []jobEntry
	for _, state := range allStates {
		stateDir := filepath.Join(s.spoolRoot, string(state))
		entries, err := os.ReadDir(stateDir)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			span.RecordError(err)
			continue
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			dir := filepath.Join(stateDir, entry.Name())
			j, err := s.sp.ReadManifest(dir)
			if err != nil {
				continue
			}
			jobs = append(jobs, jobEntry{
				JobID:            j.ID,
				State:            string(j.State),
				Repo:             j.Repo,
				Path:             j.Path,
				TagName:          j.TagName,
				NObjects:         j.NObjects,
				NBytesRaw:        j.NBytesRaw,
				NBytesCompressed: j.NBytesCompressed,
				NewRootHash:      j.NewRootHash,
				Error:            j.Error,
				CreatedAt:        j.CreatedAt,
				UpdatedAt:        j.UpdatedAt,
			})
		}
	}

	// Newest first.
	sort.Slice(jobs, func(i, k int) bool {
		return jobs[i].CreatedAt.After(jobs[k].CreatedAt)
	})

	if jobs == nil {
		jobs = []jobEntry{} // return [] not null
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}

// GET /api/v1/jobs/{id} or subscribe to GET /api/v1/jobs/{id}/events.
func (s *Server) submitJob(w http.ResponseWriter, r *http.Request) {
	_, span := s.obs.Tracer.Start(r.Context(), "api.submit_job")
	defer span.End()

	contentType := r.Header.Get("Content-Type")

	var (
		repo, subPath, webhookURL  string
		tagName, tagDescription    string
		spoolTarPath               string // final path inside the spool
		submittedSHA256            string // caller-supplied; may be empty
	)

	jobID := uuid.New().String()
	jobDir := filepath.Join(s.spoolRoot, "incoming", jobID)

	if strings.HasPrefix(contentType, "application/json") {
		// ── JSON / tar_path mode ─────────────────────────────────────────────
		if s.stagingRoot == "" {
			http.Error(w, `{"error":"tar_path submissions require --staging-root to be configured on this server"}`, http.StatusServiceUnavailable)
			return
		}

		var req struct {
			Repo           string `json:"repo"`
			Path           string `json:"path"`
			TarPath        string `json:"tar_path"`
			TarSHA256      string `json:"tar_sha256"`
			WebhookURL     string `json:"webhook_url"`
			TagName        string `json:"tag_name"`
			TagDescription string `json:"tag_description"`
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, `{"error":"failed to read request body"}`, http.StatusBadRequest)
			return
		}
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, `{"error":"invalid JSON body"}`, http.StatusBadRequest)
			return
		}
		if req.Repo == "" {
			http.Error(w, `{"error":"repo field is required"}`, http.StatusBadRequest)
			return
		}
		if req.TarPath == "" {
			http.Error(w, `{"error":"tar_path field is required"}`, http.StatusBadRequest)
			return
		}
		// tar_sha256 is mandatory for JSON mode — it's the integrity guarantee.
		if req.TarSHA256 == "" {
			http.Error(w, `{"error":"tar_sha256 is required when using tar_path submission"}`, http.StatusBadRequest)
			return
		}
		// Validate tag name before any filesystem I/O so an invalid tag never
		// causes the staging tar to be moved into the spool only to be cleaned up.
		if err := job.ValidateTagName(req.TagName); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
			return
		}

		// Resolve and validate the path is within stagingRoot.
		resolvedPath, err := resolveLocalTarPath(s.stagingRoot, req.TarPath)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"invalid tar_path: %s"}`, jsonEscape(err.Error())), http.StatusBadRequest)
			return
		}

		// Verify SHA-256 before touching the spool.
		if err := verifySHA256(resolvedPath, req.TarSHA256); err != nil {
			span.RecordError(err)
			http.Error(w, fmt.Sprintf(`{"error":"tar_sha256 mismatch: %s"}`, jsonEscape(err.Error())), http.StatusBadRequest)
			return
		}

		// Create spool job directory and move/link the tar into it.
		if err := os.MkdirAll(jobDir, 0700); err != nil {
			span.RecordError(err)
			http.Error(w, `{"error":"internal error creating job directory"}`, http.StatusInternalServerError)
			return
		}

		spoolTarPath = filepath.Join(jobDir, "payload.tar")
		if err := moveOrLink(resolvedPath, spoolTarPath); err != nil {
			span.RecordError(err)
			os.RemoveAll(jobDir)
			http.Error(w, `{"error":"internal error moving tar to spool"}`, http.StatusInternalServerError)
			return
		}

		repo = req.Repo
		subPath = req.Path
		webhookURL = req.WebhookURL
		submittedSHA256 = req.TarSHA256
		tagName = req.TagName
		tagDescription = req.TagDescription

	} else {
		// ── Multipart upload mode (default) ─────────────────────────────────
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			http.Error(w, `{"error":"invalid multipart form"}`, http.StatusBadRequest)
			return
		}

		repo = r.FormValue("repo")
		if repo == "" {
			http.Error(w, `{"error":"repo field is required"}`, http.StatusBadRequest)
			return
		}
		subPath = r.FormValue("path")
		webhookURL = r.FormValue("webhook_url")
		submittedSHA256 = r.FormValue("tar_sha256") // optional
		tagName = r.FormValue("tag_name")
		tagDescription = r.FormValue("tag_description")

		if err := job.ValidateTagName(tagName); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
			return
		}

		tarFile, _, err := r.FormFile("tar")
		if err != nil {
			http.Error(w, `{"error":"tar field is required"}`, http.StatusBadRequest)
			return
		}
		defer tarFile.Close()

		if err := os.MkdirAll(jobDir, 0700); err != nil {
			span.RecordError(err)
			http.Error(w, `{"error":"internal error creating job directory"}`, http.StatusInternalServerError)
			return
		}

		spoolTarPath = filepath.Join(jobDir, "payload.tar")
		spoolFile, err := os.OpenFile(spoolTarPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
		if err != nil {
			span.RecordError(err)
			os.RemoveAll(jobDir)
			http.Error(w, `{"error":"internal error creating tar file"}`, http.StatusInternalServerError)
			return
		}

		// Cap incoming tar size and optionally compute SHA-256 as we stream.
		hasher := sha256.New()
		dst := io.Writer(spoolFile)
		if submittedSHA256 != "" {
			dst = io.MultiWriter(spoolFile, hasher)
		}
		n, copyErr := io.Copy(dst, io.LimitReader(tarFile, maxTarSize+1))
		spoolFile.Close()
		if copyErr != nil {
			span.RecordError(copyErr)
			os.RemoveAll(jobDir)
			http.Error(w, `{"error":"error writing tar to spool"}`, http.StatusInternalServerError)
			return
		}
		if n > maxTarSize {
			os.RemoveAll(jobDir)
			http.Error(w, `{"error":"tar exceeds maximum allowed size"}`, http.StatusRequestEntityTooLarge)
			return
		}

		// Verify the optional checksum.
		if submittedSHA256 != "" {
			computed := hex.EncodeToString(hasher.Sum(nil))
			if !strings.EqualFold(computed, submittedSHA256) {
				os.RemoveAll(jobDir)
				http.Error(w, fmt.Sprintf(`{"error":"tar_sha256 mismatch: got %s, expected %s"}`, computed, submittedSHA256), http.StatusBadRequest)
				return
			}
		}
	}

	j := job.NewJob(jobID, repo, "", spoolTarPath)
	j.Path = subPath
	j.WebhookURL = webhookURL
	j.TarSHA256 = submittedSHA256
	j.TagName = tagName
	j.TagDescription = tagDescription

	// Extract provenance metadata — from OIDC token (verified) or plain headers.
	if s.orch.Provenance != nil {
		if rec := s.orch.Provenance.ExtractFromRequest(r); rec != nil {
			j.Provenance = &job.Provenance{
				GitRepo:     rec.GitRepo,
				GitSHA:      rec.GitSHA,
				GitRef:      rec.GitRef,
				Actor:       rec.Actor,
				PipelineID:  rec.PipelineID,
				BuildSystem: rec.BuildSystem,
				OIDCIssuer:  rec.OIDCIssuer,
				OIDCSubject: rec.OIDCSubject,
				Verified:    rec.Verified,
			}
		}
	}

	if err := s.sp.WriteManifest(j); err != nil {
		span.RecordError(err)
		os.RemoveAll(jobDir)
		http.Error(w, `{"error":"internal error writing manifest"}`, http.StatusInternalServerError)
		return
	}

	// Launch orchestrator in the background — the caller gets job_id immediately.
	// The cancel func is registered so abortJobHandler can interrupt the job,
	// and jobWg ensures Shutdown waits for all in-flight jobs to finish.
	runCtx, cancel := context.WithCancel(context.Background())
	s.orch.registerJob(jobID, cancel)
	s.jobWg.Add(1)
	go func() {
		defer s.jobWg.Done()
		defer s.orch.unregisterJob(jobID)
		defer cancel()
		if err := s.orch.Run(runCtx, j); err != nil {
			s.obs.Logger.Error("background job failed", "job_id", jobID, "error", err)
		}
	}()

	s.obs.Metrics.JobsSubmitted.Inc()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, `{"job_id":%q}`, jobID)
}

// resolveLocalTarPath resolves tarPath to an absolute path and verifies that
// it is contained within stagingRoot.  Returns an error if tarPath attempts a
// directory traversal (e.g. via "../" components) or points outside the staging
// tree.
func resolveLocalTarPath(stagingRoot, tarPath string) (string, error) {
	absStaging, err := filepath.Abs(stagingRoot)
	if err != nil {
		return "", fmt.Errorf("resolving staging root: %w", err)
	}
	abs, err := filepath.Abs(tarPath)
	if err != nil {
		return "", fmt.Errorf("resolving tar_path: %w", err)
	}
	// filepath.Rel returns a path starting with ".." when abs is outside absStaging.
	rel, err := filepath.Rel(absStaging, abs)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("tar_path %q is outside the configured staging directory %q", tarPath, stagingRoot)
	}
	if _, err := os.Stat(abs); err != nil {
		return "", fmt.Errorf("tar_path does not exist or is not accessible: %w", err)
	}
	return abs, nil
}

// verifySHA256 opens the file at path, streams it through a SHA-256 hasher,
// and compares the result against expectedHex (case-insensitive).  Returns a
// descriptive error on mismatch.
func verifySHA256(path, expectedHex string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening file for SHA-256 check: %w", err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("hashing file: %w", err)
	}
	computed := hex.EncodeToString(h.Sum(nil))
	if !strings.EqualFold(computed, expectedHex) {
		return fmt.Errorf("SHA-256 mismatch: file on disk=%s, caller supplied=%s", computed, expectedHex)
	}
	return nil
}

// moveOrLink moves src to dst using the fastest available mechanism:
//  1. os.Rename  — atomic and zero-copy when src and dst are on the same filesystem
//  2. os.Link    — creates a hard link (zero-copy; both names refer to the same inode)
//  3. copyFile   — full byte copy across filesystems; removes src on success
func moveOrLink(src, dst string) error {
	// Try atomic rename first.
	if err := os.Rename(src, dst); err == nil {
		return nil
	}
	// Try hard link (works only on the same filesystem, unlike Rename across mounts).
	if err := os.Link(src, dst); err == nil {
		// Remove the staging copy so the staging directory does not accumulate stale files.
		_ = os.Remove(src)
		return nil
	}
	// Fall back to a full copy.
	if err := copyFile(src, dst); err != nil {
		return err
	}
	_ = os.Remove(src)
	return nil
}

// copyFile copies the contents of src to dst (created with 0600 permissions).
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("opening source %q: %w", src, err)
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("creating destination %q: %w", dst, err)
	}

	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(dst)
		return fmt.Errorf("copying data from %q to %q: %w", src, dst, err)
	}
	return out.Close()
}

// jsonEscape returns s with double-quote and backslash characters escaped so
// it can be safely embedded as a JSON string value without a full marshal.
// It is intentionally minimal — only the characters that break inline JSON
// string literals are escaped.
func jsonEscape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return s
}

// getJob returns the current state of a job.
func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	_, span := s.obs.Tracer.Start(r.Context(), "api.get_job")
	defer span.End()

	id := mux.Vars(r)["id"]
	j, err := s.sp.FindJob(id)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, `{"error":"job not found"}`, http.StatusNotFound)
			return
		}
		span.RecordError(err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	type response struct {
		JobID            string    `json:"job_id"`
		State            string    `json:"state"`
		Repo             string    `json:"repo"`
		Path             string    `json:"path,omitempty"`
		NObjects         int       `json:"n_objects,omitempty"`
		NBytesRaw        int64     `json:"n_bytes_raw,omitempty"`
		NBytesCompressed int64     `json:"n_bytes_compressed,omitempty"`
		NewRootHash      string    `json:"new_root_hash,omitempty"`
		Error            string    `json:"error,omitempty"`
		CreatedAt        time.Time `json:"created_at"`
		UpdatedAt        time.Time `json:"updated_at"`
	}

	resp := response{
		JobID:            j.ID,
		State:            string(j.State),
		Repo:             j.Repo,
		Path:             j.Path,
		NObjects:         j.NObjects,
		NBytesRaw:        j.NBytesRaw,
		NBytesCompressed: j.NBytesCompressed,
		NewRootHash:      j.NewRootHash,
		Error:            j.Error,
		CreatedAt:        j.CreatedAt,
		UpdatedAt:        j.UpdatedAt,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// abortJobHandler handles POST /api/v1/jobs/{id}/abort.
//
// It looks up the job, rejects the request if the job is already terminal,
// and signals the running goroutine to stop via the registered cancel function.
// The actual state transition to StateAborted is performed by the orchestrator
// when it detects context cancellation — the HTTP response is 202 Accepted to
// reflect that the abort has been requested, not necessarily completed.
func (s *Server) abortJobHandler(w http.ResponseWriter, r *http.Request) {
	_, span := s.obs.Tracer.Start(r.Context(), "api.abort_job")
	defer span.End()

	id := mux.Vars(r)["id"]
	w.Header().Set("Content-Type", "application/json")

	if id == "" {
		http.Error(w, `{"error":"job not found"}`, http.StatusNotFound)
		return
	}

	j, err := s.sp.FindJob(id)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			http.Error(w, `{"error":"job not found"}`, http.StatusNotFound)
			return
		}
		span.RecordError(err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	if job.IsTerminal(j.State) {
		http.Error(w, `{"error":"job is already in a terminal state"}`, http.StatusConflict)
		return
	}

	if !s.orch.CancelJob(id) {
		// The job exists and is not terminal, but is not in the running map.
		// This is a narrow race (job completed between FindJob and CancelJob).
		http.Error(w, `{"error":"job is not currently running"}`, http.StatusConflict)
		return
	}

	s.obs.Metrics.PipelineAbortCount.Inc()
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, `{"status":"aborting"}`)
}

// jobEvents streams state-change events for a job using Server-Sent Events.
// The connection stays open until the job reaches a terminal state or the
// client disconnects.
//
// Event format (text/event-stream):
//
//	event: state_change
//	data: {"job_id":"...","state":"...","time":"...","error":"..."}
func (s *Server) jobEvents(w http.ResponseWriter, r *http.Request) {
	_, span := s.obs.Tracer.Start(r.Context(), "api.job_events")
	defer span.End()

	id := mux.Vars(r)["id"]

	if _, err := s.sp.FindJob(id); err != nil {
		if os.IsNotExist(err) {
			http.Error(w, `{"error":"job not found"}`, http.StatusNotFound)
			return
		}
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, `{"error":"streaming not supported by this server"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // tell nginx not to buffer SSE

	ch, cancel := s.notifyBus.Subscribe(id)
	defer cancel()

	for {
		select {
		case <-r.Context().Done():
			return

		case e, ok := <-ch:
			if !ok {
				return
			}

			data, err := json.Marshal(e)
			if err != nil {
				s.obs.Logger.Warn("SSE: marshal error", "job_id", id, "error", err)
				continue
			}

			fmt.Fprintf(w, "event: state_change\ndata: %s\n\n", data)
			flusher.Flush()

			// Close stream once the job is in a terminal state.
			if job.IsTerminal(e.State) {
				return
			}
		}
	}
}

// health returns a liveness probe response.
func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	_, span := s.obs.Tracer.Start(r.Context(), "api.health")
	defer span.End()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status":"healthy"}`)
}
