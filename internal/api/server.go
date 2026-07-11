// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

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
	"golang.org/x/net/netutil"
	"io"
	"net"
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

	"cvmfs.io/prepub/internal/broker"
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
	// dynaSem limits the number of concurrently active jobs and adjusts its
	// effective slot count dynamically with the system load (non-nil when
	// minConcurrentJobs > 0 was passed to New).  Jobs wait in StateIncoming
	// until a slot opens; the per-job timeout starts AFTER the slot is
	// acquired, so queue-wait time does not count against the deadline.
	dynaSem *DynamicSemaphore
}

// New creates a new API server.
// apiToken is the expected bearer token for authenticated routes.
// Pass an empty string to disable authentication (development only).
// stagingRoot, when non-empty, enables the JSON tar_path submission mode and
// restricts acceptable tar_path values to files within that directory tree.
// minConcurrentJobs is the guaranteed floor for the dynamic concurrency limit
// (effective slots = max(minConcurrentJobs, numCPU - load1min)).  Pass 0 to
// disable the limit (all submitted jobs start immediately — legacy behaviour).
// maxConcurrentJobs caps the dynamic limit at an explicit ceiling; 0 means
// runtime.NumCPU().
func New(obs *observe.Provider, apiToken string, orch *Orchestrator, sp *spool.Spool, nb *notify.Bus, spoolRoot, stagingRoot string, minConcurrentJobs, maxConcurrentJobs int) *Server {
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
			// Slowloris defenses (the control plane may be internet-exposed; do not
			// rely on a firewall). ReadHeaderTimeout bounds slow header attacks;
			// IdleTimeout reaps idle keep-alives. No Read/Write timeout so large tar
			// uploads and streaming job-event responses are not truncated; per-route
			// timeouts gate the small control endpoints.
			ReadHeaderTimeout: 10 * time.Second,
			IdleTimeout:       120 * time.Second,
		},
	}
	if minConcurrentJobs > 0 {
		s.dynaSem = NewDynamicSemaphore(minConcurrentJobs, maxConcurrentJobs, obs.Logger)
		obs.Logger.Info("server: dynamic job concurrency enabled",
			"min_slots", minConcurrentJobs,
			"max_slots", s.dynaSem.maxSlots,
			"note", "effective limit = max(min_slots, max_slots - load1min); timeout starts after slot acquisition")
	}

	// Unauthenticated routes.
	// Use the observer's isolated registry — promhttp.Handler() would serve the
	// process-global default registry, which does not contain our custom metrics.
	s.router.Handle("/api/v1/metrics", promhttp.HandlerFor(obs.Registry, promhttp.HandlerOpts{}))
	s.router.HandleFunc("/api/v1/health", s.health).Methods("GET")

	// Console — unauthenticated (read-only, no secrets exposed).
	s.router.HandleFunc("/", s.consoleHandler).Methods("GET")
	s.router.HandleFunc("/jobs", s.consoleHandler).Methods("GET")
	s.router.HandleFunc("/jobs/{id}", s.jobDetailHandler).Methods("GET")

	// Critical #4: All job routes require a valid bearer token.
	auth := s.router.PathPrefix("/api/v1/jobs").Subrouter()
	auth.Use(s.requireAuth)
	auth.HandleFunc("", s.listJobs).Methods("GET")
	auth.HandleFunc("", s.submitJob).Methods("POST")
	auth.HandleFunc("/{id}", s.getJob).Methods("GET")
	auth.HandleFunc("/{id}/abort", s.abortJobHandler).Methods("POST")
	auth.HandleFunc("/{id}/events", s.jobEvents).Methods("GET")
	auth.HandleFunc("/{id}/log", s.jobLogHandler).Methods("GET")

	return s
}

// MountDiscovery mounts the signed discovery document (GET /cvmfs/{repo}/.cvmfsbits)
// on the API router so Stratum 1 receivers can learn the control-plane broker URL
// from a fixed S0 endpoint (ADR-0001 D10).
func (s *Server) MountDiscovery(h http.Handler) {
	if h != nil {
		s.router.Handle("/cvmfs/{repo}/.cvmfsbits", h).Methods("GET")
	}
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
// maxConnections caps concurrent accepted connections so a connection flood
// cannot exhaust file descriptors / goroutines (R-DoS).
const maxConnections = 1024

func (s *Server) ListenAndServe(addr string) error {
	s.httpServer.Addr = addr
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.httpServer.Serve(netutil.LimitListener(ln, maxConnections))
}

// Shutdown gracefully stops the HTTP server and waits for all background job
// goroutines, webhook deliveries, and distribution workers to finish.
// The provided context caps the total wait — if it expires before all work
// completes, Shutdown returns ctx.Err() and the caller should force-exit.
// Distribution workers that are mid-backoff stop immediately; in-flight
// transfers finish their current attempt.  Pending spool items are retried on
// the next start.
func (s *Server) Shutdown(ctx context.Context) error {
	// Stop the dynamic semaphore load-poller first so it doesn't interfere
	// with the graceful drain below.
	if s.dynaSem != nil {
		s.dynaSem.Stop()
	}

	httpErr := s.httpServer.Shutdown(ctx)

	// Phase 1: wait for all job goroutines and webhook deliveries.
	// After this, no new items will be enqueued in DistManager.
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
		return httpErr
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
		JobID    string `json:"job_id"`
		State    string `json:"state"`
		Repo     string `json:"repo"`
		Path     string `json:"path,omitempty"`
		TagName  string `json:"tag_name,omitempty"`
		TarName  string `json:"tar_name,omitempty"`
		TarSize  int64  `json:"tar_size,omitempty"`
		NObjects int    `json:"n_objects,omitempty"`
		// NNewObjects is the count of objects freshly uploaded in this pipeline
		// run (dedup hits excluded).  Used by the S1 distribution backlog so
		// the object count matches what is actually being pushed to S1.
		NNewObjects      int    `json:"n_new_objects,omitempty"`
		NBytesRaw        int64  `json:"n_bytes_raw,omitempty"`
		NBytesCompressed int64  `json:"n_bytes_compressed,omitempty"`
		NewRootHash      string `json:"new_root_hash,omitempty"`
		Error            string `json:"error,omitempty"`
		// FailedAtState is the FSM state the job was in when it failed
		// (e.g. "leased", "committing").  Empty for non-failed jobs.
		FailedAtState string    `json:"failed_at_state,omitempty"`
		CreatedAt     time.Time `json:"created_at"`
		UpdatedAt     time.Time `json:"updated_at"`
		// Pipeline stage timestamps — omitted when zero (bits-method jobs only).
		// Used by the console Monitoring chart to build per-job stage breakdowns.
		PipelineStartedAt time.Time `json:"pipeline_started_at,omitempty"`
		PipelineEndedAt   time.Time `json:"pipeline_ended_at,omitempty"`
		LeasedAt          time.Time `json:"leased_at,omitempty"`
		PublishedAt       time.Time `json:"published_at,omitempty"`
		// Distribution timestamps and counters for S1 backlog display in the console.
		// DistributingStartedAt / DistributingEndedAt use omitempty so zero-value
		// time.Time values are omitted; the JS checks for field presence.
		DistributingStartedAt time.Time `json:"distributing_started_at,omitempty"`
		DistributingEndedAt   time.Time `json:"distributing_ended_at,omitempty"`
		DistributionConfirmed int       `json:"distribution_confirmed,omitempty"`
		DistributionTotal     int       `json:"distribution_total,omitempty"`
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
				JobID:                 j.ID,
				State:                 string(j.State),
				Repo:                  j.Repo,
				Path:                  j.Path,
				TagName:               j.TagName,
				TarName:               j.TarName,
				TarSize:               j.TarSize,
				NObjects:              j.NObjects,
				NNewObjects:           j.NNewObjects,
				NBytesRaw:             j.NBytesRaw,
				NBytesCompressed:      j.NBytesCompressed,
				NewRootHash:           j.NewRootHash,
				Error:                 j.Error,
				FailedAtState:         j.FailedAtState,
				CreatedAt:             j.CreatedAt,
				UpdatedAt:             j.UpdatedAt,
				PipelineStartedAt:     j.PipelineStartedAt,
				PipelineEndedAt:       j.PipelineEndedAt,
				LeasedAt:              j.LeasedAt,
				PublishedAt:           j.PublishedAt,
				DistributingStartedAt: j.DistributingStartedAt,
				DistributingEndedAt:   j.DistributingEndedAt,
				DistributionConfirmed: j.DistributionConfirmed,
				DistributionTotal:     j.DistributionTotal,
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
		repo, subPath, webhookURL string
		tagName, tagDescription   string
		spoolTarPath              string   // final path inside the spool
		submittedSHA256           string   // caller-supplied; may be empty
		preloadExe                string   // optional: repo-relative exe path for preload
		preloadPaths              []string // optional: repo-relative paths opened at startup
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
			Repo           string   `json:"repo"`
			Path           string   `json:"path"`
			TarPath        string   `json:"tar_path"`
			TarSHA256      string   `json:"tar_sha256"`
			WebhookURL     string   `json:"webhook_url"`
			TagName        string   `json:"tag_name"`
			TagDescription string   `json:"tag_description"`
			PreloadExe     string   `json:"preload_exe"`
			PreloadPaths   []string `json:"preload_paths"`
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
		// Reject repo names that would produce structurally broken MQTT topics
		// (/, +, #, NUL).  Validated here so downstream topic constructors
		// (which panic on invalid input) never receive bad data.
		if err := broker.ValidateRepo(req.Repo); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
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
		preloadExe = req.PreloadExe
		preloadPaths = req.PreloadPaths

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
		if err := broker.ValidateRepo(repo); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
			return
		}
		subPath = r.FormValue("path")
		webhookURL = r.FormValue("webhook_url")
		submittedSHA256 = r.FormValue("tar_sha256") // optional
		tagName = r.FormValue("tag_name")
		tagDescription = r.FormValue("tag_description")
		preloadExe = r.FormValue("preload_exe") // optional
		// preload_paths is a JSON-encoded []string (e.g. '["bin/root","lib/libCore.so"]')
		if raw := r.FormValue("preload_paths"); raw != "" {
			if err := json.Unmarshal([]byte(raw), &preloadPaths); err != nil {
				http.Error(w, `{"error":"preload_paths must be a JSON array of strings"}`, http.StatusBadRequest)
				return
			}
		}

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
	j.PreloadExe = preloadExe
	j.PreloadPaths = preloadPaths

	// Record the original filename and size for the console tooltip.
	// Use Stat on the spool copy since the original may have been moved.
	j.TarName = filepath.Base(spoolTarPath)
	if fi, statErr := os.Stat(spoolTarPath); statErr == nil {
		j.TarSize = fi.Size()
	}

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
	//
	// Concurrency-limited path (jobSem != nil):
	//   The goroutine first waits in StateIncoming for a semaphore slot.
	//   Only after acquiring the slot does it create the execution context
	//   (with JobTimeout if set).  This means queue-wait time is NOT counted
	//   against the per-job timeout, so large batches do not time out simply
	//   because they had to wait behind earlier jobs.
	//
	// Unlimited path (jobSem == nil):
	//   All jobs start immediately with no queuing.  The timeout (if any)
	//   starts at goroutine launch, identical to the previous behaviour.
	//
	// Either way, a cancel function is registered immediately so that
	// abortJobHandler can interrupt the job at any point — including while
	// it is waiting for a concurrency slot.
	abortCtx, abortCancel := context.WithCancel(context.Background())
	s.orch.registerJob(jobID, abortCancel)

	// Read-ahead Phase 0: start the tar scan NOW, before waiting for the
	// concurrency slot.  The tar is already on disk in the spool; scanning it
	// costs only I/O and memory, not a pipeline slot.  For queued jobs the
	// scan overlaps with earlier jobs' compress/upload work so that when the
	// slot opens the compress workers start immediately with sorted entries
	// already in memory rather than waiting for another full tar read.
	s.orch.StartPrefetch(abortCtx, j)

	s.jobWg.Add(1)
	go func() {
		defer s.jobWg.Done()
		defer s.orch.unregisterJob(jobID)
		defer abortCancel()

		// ── Wait for a concurrency slot (if the limit is configured) ──────────
		// The semaphore limits concurrent pipeline (compress/upload) workers.
		// The slot is released EARLY — before the per-repo commit mutex — by the
		// onStagingComplete hook passed to Run().  This lets the next queued job
		// start its own compress pipeline while this job does its gateway commit.
		// The defer below is a safety net: if Run() returns without ever calling
		// the hook (e.g. early error during staging, local mode) the slot is
		// still released exactly once via sync.Once.
		var semOnce sync.Once
		releaseSem := func() {
			semOnce.Do(func() {
				if s.dynaSem != nil {
					s.dynaSem.Release()
					s.obs.Logger.Info("released concurrency slot (pipeline complete)",
						"job_id", jobID)
				}
			})
		}

		if s.dynaSem != nil {
			s.obs.Logger.Info("job queued — waiting for concurrency slot",
				"job_id", jobID, "repo", j.Repo)
			// Use abortCtx so that a manual abort unblocks the wait
			// immediately rather than holding the slot indefinitely.
			if err := s.dynaSem.Acquire(abortCtx, j.TarSize); err != nil {
				// abortCancel fired (operator abort or server shutdown) while
				// the job was queued; mark it as aborted without running.
				s.obs.Logger.Info("job aborted while waiting for slot",
					"job_id", jobID, "error", err)
				_ = s.orch.abortJob(context.Background(), j,
					fmt.Errorf("aborted while waiting for concurrency slot: %w", err))
				return
			}
			s.obs.Logger.Info("job acquired concurrency slot", "job_id", jobID)
		}
		defer releaseSem() // safety net — no-op if hook already fired

		// ── Build the execution context (timeout starts here, not at submit) ──
		var runCtx context.Context
		var runCancel context.CancelFunc
		if s.orch.JobTimeout > 0 {
			runCtx, runCancel = context.WithTimeout(abortCtx, s.orch.JobTimeout)
		} else {
			runCtx, runCancel = context.WithCancel(abortCtx)
		}
		defer runCancel()

		// Re-register with the timeout-aware cancel so abortJobHandler also
		// cancels the execution context (not just the abort context).
		s.orch.registerJob(jobID, runCancel)

		if err := s.orch.Run(runCtx, j, releaseSem); err != nil {
			if s.orch.JobTimeout > 0 && runCtx.Err() != nil {
				s.obs.Logger.Error("background job timed out", "job_id", jobID, "timeout", s.orch.JobTimeout, "error", err)
			} else {
				s.obs.Logger.Error("background job failed", "job_id", jobID, "error", err)
			}
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

// jobLogHandler handles GET /api/v1/jobs/{id}/log.
// Returns a JSON object with the job manifest and its full FSM journal.
// Requires the standard bearer token (authenticated route).
func (s *Server) jobLogHandler(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	j, err := s.sp.FindJob(id)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			http.Error(w, `{"error":"job not found"}`, http.StatusNotFound)
			return
		}
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	entries, _ := s.sp.ReadJobJournal(id) // best-effort; nil on error

	type transition struct {
		Time time.Time `json:"time"`
		From string    `json:"from"`
		To   string    `json:"to"`
		Note string    `json:"note,omitempty"`
	}
	var transitions []transition
	for _, e := range entries {
		transitions = append(transitions, transition{
			Time: e.T,
			From: string(e.From),
			To:   string(e.To),
			Note: e.Note,
		})
	}
	if transitions == nil {
		transitions = []transition{}
	}

	resp := map[string]any{
		"job":         j,
		"transitions": transitions,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// consoleHandler serves the self-contained Publish Jobs web console.
// It is unauthenticated (read-only; no secrets exposed) so operators can
// check job status in a browser without copying tokens.
func (s *Server) consoleHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, consoleHTML)
}

// jobDetailHandler serves the per-job log page at GET /jobs/{id}.
// It is the same self-contained SPA shell as the console — the JS reads
// the job ID from the URL and fetches /api/v1/jobs/{id}/log directly.
func (s *Server) jobDetailHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, consoleHTML)
}

// consoleHTML is the self-contained single-page console application.
// It renders both the job list (when at /jobs) and the per-job detail
// page (when at /jobs/{id}).  No external dependencies — all CSS and JS
// are inline.
//
// Features:
//   - Auto-refreshes the job list every 5 s via polling.
//   - Job ID shown as a link to /jobs/{id} with a tooltip displaying the
//     original tar filename and size.
//   - Detail page shows full FSM transition history with elapsed times,
//     flags stuck states (> 2 min in a non-terminal state), and surfaces
//     the error message when the job failed.
const consoleHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>CVMFS Publish Jobs</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,sans-serif;font-size:14px;background:#f5f7fa;color:#1a1a2e}
header{background:#1a1a2e;color:#fff;padding:12px 20px;display:flex;align-items:center;gap:12px}
header h1{font-size:18px;font-weight:600}
header a{color:#7ec8e3;text-decoration:none;font-size:13px}
.container{max-width:1400px;margin:0 auto;padding:16px}
.table-wrap{border-radius:8px;overflow:visible;box-shadow:0 1px 4px rgba(0,0,0,.08);background:#fff;border-radius:8px}
table{width:100%;border-collapse:collapse;background:transparent}
th{background:#f0f4f8;text-align:left;padding:10px 12px;font-weight:600;
  border-bottom:2px solid #dde3ea;white-space:nowrap}
thead tr th:first-child{border-radius:8px 0 0 0}
thead tr th:last-child{border-radius:0 8px 0 0}
tr:last-child td:first-child{border-radius:0 0 0 8px}
tr:last-child td:last-child{border-radius:0 0 8px 0}
td{padding:9px 12px;border-bottom:1px solid #edf0f3;vertical-align:top;word-break:break-all}
tr:last-child td{border-bottom:none}
tr:hover td{background:#f8fafc}
.badge{display:inline-block;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:600;white-space:nowrap}
.s-incoming{background:#e8eaf6;color:#3949ab}
.s-staging{background:#e3f2fd;color:#1565c0}
.s-uploading{background:#e8f5e9;color:#2e7d32}
.s-distributing{background:#fff8e1;color:#f57f17}
.s-leased{background:#fce4ec;color:#c62828}
.s-committing{background:#f3e5f5;color:#6a1b9a}
.s-published{background:#e8f5e9;color:#1b5e20}
.s-failed{background:#ffebee;color:#b71c1c}
.s-aborted{background:#fafafa;color:#616161}
.job-link{color:#1565c0;text-decoration:underline;font-family:monospace;font-size:12px}
.job-link:hover{color:#003c8f}
.tip{position:relative;display:inline-block}
.tip .tiptext{visibility:hidden;background:#333;color:#fff;border-radius:4px;
  padding:5px 8px;position:absolute;z-index:9999;bottom:125%;left:50%;
  transform:translateX(-50%);white-space:nowrap;font-size:11px;pointer-events:none;
  opacity:0;transition:opacity .15s}
.tip:hover .tiptext{visibility:visible;opacity:1}
.mono{font-family:monospace;font-size:12px}
.err{color:#b71c1c;font-size:12px;max-width:300px}
.stuck{color:#f57f17;font-weight:600}
#refresh-info{font-size:12px;color:#888;margin-bottom:8px}
/* detail page */
.card{background:#fff;border-radius:8px;padding:20px;box-shadow:0 1px 4px rgba(0,0,0,.08);margin-bottom:16px}
.card h2{font-size:16px;margin-bottom:12px;color:#1a1a2e}
.kv{display:grid;grid-template-columns:160px 1fr;gap:6px 12px;font-size:13px}
.kv dt{color:#666;font-weight:500}
.kv dd{word-break:break-all}
.timeline{list-style:none;position:relative;padding-left:24px}
.timeline::before{content:'';position:absolute;left:8px;top:0;bottom:0;
  width:2px;background:#dde3ea}
.timeline li{position:relative;padding:6px 0 6px 16px;font-size:13px}
.timeline li::before{content:'';position:absolute;left:-8px;top:12px;
  width:10px;height:10px;border-radius:50%;background:#7ec8e3;border:2px solid #fff;
  box-shadow:0 0 0 2px #7ec8e3}
.timeline li.ok::before{background:#4caf50}
.timeline li.fail::before{background:#ef5350}
.timeline li.warn::before{background:#ff9800}
.elapsed{color:#888;font-size:11px;margin-left:8px}
.back{display:inline-block;margin-bottom:12px;color:#1565c0;text-decoration:none;font-size:13px}
.back:hover{text-decoration:underline}
.stuck-banner{background:#fff3e0;border:1px solid #ff9800;border-radius:6px;
  padding:10px 14px;margin-bottom:12px;font-size:13px;color:#e65100}
</style>
</head>
<body>
<header>
  <h1>CVMFS Publish Jobs</h1>
  <a href="/jobs">All Jobs</a>
</header>
<div class="container" id="app">Loading…</div>
<script>
const POLL_MS = 5000;
const STATE_ORDER = ['incoming','staging','uploading','distributing','leased','committing','published','failed','aborted'];
const TERMINAL = new Set(['published','failed','aborted']);
const NON_TERMINAL_WARN_MS = 2 * 60 * 1000; // flag if stuck > 2 min

function fmtBytes(b){
  if(!b) return '—';
  if(b<1024) return b+' B';
  if(b<1048576) return (b/1024).toFixed(1)+' KB';
  if(b<1073741824) return (b/1048576).toFixed(1)+' MB';
  return (b/1073741824).toFixed(2)+' GB';
}
function fmtDuration(ms){
  if(ms<0) ms=0;
  const s=Math.floor(ms/1000), m=Math.floor(s/60), h=Math.floor(m/60);
  if(h>0) return h+'h '+( m%60)+'m';
  if(m>0) return m+'m '+(s%60)+'s';
  return s+'s';
}
function fmtTime(iso){
  if(!iso) return '—';
  const d=new Date(iso);
  return d.toLocaleString(undefined,{month:'short',day:'2-digit',
    hour:'2-digit',minute:'2-digit',second:'2-digit'});
}
function ago(iso){
  if(!iso) return '—';
  return fmtDuration(Date.now()-new Date(iso).getTime())+' ago';
}
function badgeClass(state){
  return 's-'+state.replace(/[^a-z]/g,'');
}
function stateLabel(state){
  const map={incoming:'Incoming',staging:'Staging',uploading:'Uploading',
    distributing:'Distributing',leased:'Leased',committing:'Committing',
    published:'Published',failed:'Failed',aborted:'Aborted'};
  return map[state]||state;
}
function jobShortID(id){ return id.substring(0,8); }

// ── List page ──────────────────────────────────────────────────────────────
function renderList(jobs, lastRefresh){
  const now=Date.now();
  let rows='';
  for(const j of jobs){
    const stateAge=now-new Date(j.updated_at).getTime();
    const isStuck=!TERMINAL.has(j.state)&&stateAge>NON_TERMINAL_WARN_MS;
    const shortID=jobShortID(j.job_id);
    const tipLines=['<span style="font-size:11px;color:#999">'+escHtml(j.job_id)+'</span>'];
    if(j.tar_name) tipLines.push(escHtml(j.tar_name)+(j.tar_size?' &nbsp;'+fmtBytes(j.tar_size):''));
    if(isStuck) tipLines.push('<b style="color:#e65100">⚠ Stuck '+fmtDuration(stateAge)+'</b>');
    const tipText='<span class="tiptext">'+tipLines.join('<br>')+'</span>';
    const idCell='<span class="tip"><a class="job-link" href="/jobs/'+encodeURIComponent(j.job_id)+'">'+shortID+'</a>'+tipText+'</span>';
    const stateCell='<span class="badge '+badgeClass(j.state)+(isStuck?' stuck':'')+'">'
      +stateLabel(j.state)+(isStuck?' ⚠':'')+' </span>';
    const repoCell=escHtml(j.repo)+(j.path?'<br><span style="color:#666;font-size:11px">'+escHtml(j.path)+'</span>':'');
    const statsCell=j.n_objects?('<span class="mono">'+j.n_objects+'</span> obj<br>'
      +'<span class="mono">'+fmtBytes(j.n_bytes_raw)+'</span>'):'—';
    const errCell=j.error?'<span class="err" title="'+escHtml(j.error)+'">'+escHtml(j.error.substring(0,80))+(j.error.length>80?'…':'')+'</span>':'';
    rows+='<tr>'
      +'<td>'+idCell+'</td>'
      +'<td>'+stateCell+'</td>'
      +'<td>'+repoCell+'</td>'
      +'<td>'+statsCell+'</td>'
      +'<td>'+fmtBytes(j.n_bytes_compressed)+'</td>'
      +'<td class="mono">'+ago(j.created_at)+'</td>'
      +'<td class="mono">'+ago(j.updated_at)+'</td>'
      +'<td>'+errCell+'</td>'
      +'</tr>';
  }
  const infoLine='<div id="refresh-info">'+jobs.length+' jobs &nbsp;·&nbsp; last refreshed '+
    new Date(lastRefresh).toLocaleTimeString()+' &nbsp;·&nbsp; auto-refreshes every 5 s</div>';
  return infoLine+'<div class="table-wrap"><table><thead><tr>'
    +'<th>Job ID</th><th>State</th><th>Repo / Path</th>'
    +'<th>Objects / Raw</th><th>Compressed</th>'
    +'<th>Submitted</th><th>Updated</th><th>Error</th>'
    +'</tr></thead><tbody>'+rows+'</tbody></table></div>';
}

// ── Detail page ────────────────────────────────────────────────────────────
function renderDetail(data){
  const j=data.job;
  const transitions=data.transitions||[];
  const now=Date.now();
  const stateAge=now-new Date(j.UpdatedAt||j.updated_at).getTime();
  const state=j.State||j.state;
  const isStuck=!TERMINAL.has(state)&&stateAge>NON_TERMINAL_WARN_MS;

  let stuckBanner='';
  if(isStuck){
    stuckBanner='<div class="stuck-banner">⚠ Job has been in <b>'+stateLabel(state)+'</b> for '
      +fmtDuration(stateAge)+'. It may be stuck.<br>'
      +(state==='leased'?'Possible cause: waiting for per-repo serialisation lock (another job is committing).'
       :state==='committing'?'Possible cause: cvmfs_receiver is processing the catalog graft (30–150 s normal).'
       :state==='staging'?'Possible cause: large tar or slow CAS — pipeline is compressing/uploading.'
       :state==='distributing'?'Possible cause: waiting for Stratum 1 quorum confirmation.'
       :'Check service logs for details.')
      +'</div>';
  }

  // Build kv pairs from manifest
  const kvs=[
    ['State', '<span class="badge '+badgeClass(state)+'">'+stateLabel(state)+'</span>'],
    ['Job ID', '<span class="mono">'+escHtml(j.ID||j.job_id)+'</span>'],
    ['Repo', escHtml(j.Repo||j.repo||'—')],
    ['Path', escHtml(j.Path||j.path||'(root)')],
    ['Tar file', escHtml(j.TarName||j.tar_name||'—')],
    ['Tar size', fmtBytes(j.TarSize||j.tar_size)],
    ['Objects (total)', (j.NObjects||j.n_objects||0).toString()],
    ['Objects (new)', (j.NNewObjects||j.n_new_objects||0).toString()],
    ['Raw size', fmtBytes(j.NBytesRaw||j.n_bytes_raw)],
    ['Compressed', fmtBytes(j.NBytesCompressed||j.n_bytes_compressed)],
    ['Tag', escHtml(j.TagName||j.tag_name||'—')],
    ['New root hash', j.NewRootHash||j.new_root_hash?'<span class="mono">'+(j.NewRootHash||j.new_root_hash)+'</span>':'—'],
    ['Created', fmtTime(j.CreatedAt||j.created_at)+' ('+ago(j.CreatedAt||j.created_at)+')'],
    ['Updated', fmtTime(j.UpdatedAt||j.updated_at)+' ('+ago(j.UpdatedAt||j.updated_at)+')'],
  ];
  if(j.Error||j.error){
    kvs.push(['Error', '<span style="color:#b71c1c">'+escHtml(j.Error||j.error)+'</span>']);
  }
  if(j.FailedAtState||j.failed_at_state){
    kvs.push(['Failed at state', '<span class="badge s-failed">'+escHtml(j.FailedAtState||j.failed_at_state)+'</span>']);
  }

  let kvHtml='<dl class="kv">';
  for(const[k,v] of kvs) kvHtml+='<dt>'+escHtml(k)+'</dt><dd>'+v+'</dd>';
  kvHtml+='</dl>';

  // Timeline
  let prevTime=null;
  let tlHtml='<ul class="timeline">';
  for(const t of transitions){
    const isTerminal=TERMINAL.has(t.to);
    const cls=t.to==='published'?'ok':t.to==='failed'||t.to==='aborted'?'fail':'';
    const elapsed=prevTime?'<span class="elapsed">+'+fmtDuration(new Date(t.time).getTime()-new Date(prevTime).getTime())+'</span>':'';
    tlHtml+='<li class="'+cls+'"><b>'+escHtml(stateLabel(t.from))+'</b> → <b>'
      +escHtml(stateLabel(t.to))+'</b>&nbsp; '
      +'<span style="color:#888;font-size:11px">'+fmtTime(t.time)+'</span>'
      +elapsed
      +(t.note?'<br><span style="color:#666;font-size:12px">'+escHtml(t.note)+'</span>':'')
      +'</li>';
    prevTime=t.time;
  }
  // Add "currently in" entry if job is still active
  if(!TERMINAL.has(state)&&transitions.length>0){
    const elapsed=prevTime?'<span class="elapsed stuck">still here, '+fmtDuration(now-new Date(prevTime).getTime())+'</span>':'';
    tlHtml+='<li class="warn"><b>'+escHtml(stateLabel(state))+'</b> (current) '+elapsed+'</li>';
  }
  if(transitions.length===0){
    tlHtml+='<li>No state transitions recorded yet.</li>';
  }
  tlHtml+='</ul>';

  return '<a class="back" href="/jobs">← All Jobs</a>'
    +stuckBanner
    +'<div class="card"><h2>Job Detail</h2>'+kvHtml+'</div>'
    +'<div class="card"><h2>State Transitions</h2>'+tlHtml+'</div>';
}

// ── Router ─────────────────────────────────────────────────────────────────
function escHtml(s){
  if(!s) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;')
    .replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

const path=window.location.pathname;
const app=document.getElementById('app');
const jobDetailMatch=path.match(/^\/jobs\/([^\/]+)$/);

if(jobDetailMatch){
  // ── Detail view ──────────────────────────────────────────────────────────
  const jobID=jobDetailMatch[1];
  document.title='Job '+jobID.substring(0,8)+' — CVMFS';
  let token='';
  try{ token=localStorage.getItem('prepub_token')||''; }catch(_){}

  async function loadDetail(){
    try{
      const headers=token?{Authorization:'Bearer '+token}:{};
      const r=await fetch('/api/v1/jobs/'+jobID+'/log',{headers});
      if(r.status===401){
        app.innerHTML='<div class="card"><h2>Authentication required</h2>'
          +'<p style="margin-top:8px">Enter your API token to view job details:</p>'
          +'<input id="tok" type="password" placeholder="Bearer token" style="margin:8px 0;padding:6px;width:300px;border:1px solid #ccc;border-radius:4px">'
          +'<button onclick="saveToken()" style="padding:6px 12px;margin-left:6px;cursor:pointer">Save</button></div>';
        return;
      }
      const data=await r.json();
      app.innerHTML=renderDetail(data);
    }catch(e){
      app.innerHTML='<div class="card"><p style="color:red">Error: '+escHtml(e.message)+'</p></div>';
    }
  }
  window.saveToken=function(){
    const t=document.getElementById('tok').value.trim();
    try{localStorage.setItem('prepub_token',t);}catch(_){}
    token=t;
    loadDetail();
  };
  loadDetail();
  // Refresh detail every 5 s if job is not terminal
  setInterval(async()=>{
    try{
      const headers=token?{Authorization:'Bearer '+token}:{};
      const r=await fetch('/api/v1/jobs/'+jobID+'/log',{headers});
      if(!r.ok) return;
      const data=await r.json();
      const state=data.job&&(data.job.State||data.job.state);
      if(state&&!TERMINAL.has(state)) app.innerHTML=renderDetail(data);
    }catch(_){}
  }, POLL_MS);

} else {
  // ── List view ─────────────────────────────────────────────────────────────
  document.title='CVMFS Publish Jobs';
  let listToken='';
  try{ listToken=localStorage.getItem('prepub_token')||''; }catch(_){}

  function showListAuthForm(){
    app.innerHTML='<div class="card"><h2>Authentication required</h2>'
      +'<p style="margin-top:8px">Enter your API token to view publish jobs:</p>'
      +'<input id="ltok" type="password" placeholder="Bearer token" style="margin:8px 0;padding:6px;width:300px;border:1px solid #ccc;border-radius:4px">'
      +'<button onclick="saveListToken()" style="padding:6px 12px;margin-left:6px;cursor:pointer">Save</button></div>';
  }
  window.saveListToken=function(){
    const t=document.getElementById('ltok').value.trim();
    try{localStorage.setItem('prepub_token',t);}catch(_){}
    listToken=t;
    loadList();
  };

  async function loadList(){
    try{
      const headers=listToken?{Authorization:'Bearer '+listToken}:{};
      const r=await fetch('/api/v1/jobs?_='+Date.now(),{headers});
      if(r.status===401){ showListAuthForm(); return; }
      if(!r.ok){ app.innerHTML='<p>Failed to load jobs ('+r.status+')</p>'; return; }
      const jobs=await r.json();
      app.innerHTML=renderList(jobs,Date.now());
    }catch(e){
      app.innerHTML='<p style="color:red">Error: '+escHtml(e.message)+'</p>';
    }
  }
  loadList();
  setInterval(loadList, POLL_MS);
}
</script>
</body>
</html>`

// health returns a liveness probe response.
func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	_, span := s.obs.Tracer.Start(r.Context(), "api.health")
	defer span.End()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status":"healthy"}`)
}
