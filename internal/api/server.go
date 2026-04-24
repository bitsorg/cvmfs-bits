// Package api defines the HTTP API server and request handlers for job submission,
// status polling, and event streaming. The Server manages authenticated requests,
// spawns background job orchestrators, and gracefully shuts down in-flight jobs.
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
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
	// jobWg tracks all background job goroutines so Shutdown can wait for them
	// to reach a terminal state before the process exits.
	jobWg sync.WaitGroup
}

// New creates a new API server.
// apiToken is the expected bearer token for authenticated routes.
// Pass an empty string to disable authentication (development only).
func New(obs *observe.Provider, apiToken string, orch *Orchestrator, sp *spool.Spool, nb *notify.Bus, spoolRoot string) *Server {
	router := mux.NewRouter()
	s := &Server{
		router:    router,
		obs:       obs,
		apiToken:  apiToken,
		orch:      orch,
		sp:        sp,
		notifyBus: nb,
		spoolRoot: spoolRoot,
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
// Accepts multipart/form-data with fields:
//
//	repo        — repository name (e.g. "software.cern.ch")
//	path        — gateway lease sub-path (e.g. "atlas/24.0")
//	tar         — the tar file to publish (binary)
//	webhook_url — optional URL to POST when the job reaches a terminal state
//
// Returns 202 Accepted with {"job_id": "<uuid>"}.  The caller should poll
// GET /api/v1/jobs/{id} or subscribe to GET /api/v1/jobs/{id}/events.
func (s *Server) submitJob(w http.ResponseWriter, r *http.Request) {
	_, span := s.obs.Tracer.Start(r.Context(), "api.submit_job")
	defer span.End()

	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, `{"error":"invalid multipart form"}`, http.StatusBadRequest)
		return
	}

	repo := r.FormValue("repo")
	if repo == "" {
		http.Error(w, `{"error":"repo field is required"}`, http.StatusBadRequest)
		return
	}
	subPath := r.FormValue("path")
	webhookURL := r.FormValue("webhook_url")

	tarFile, _, err := r.FormFile("tar")
	if err != nil {
		http.Error(w, `{"error":"tar field is required"}`, http.StatusBadRequest)
		return
	}
	defer tarFile.Close()

	jobID := uuid.New().String()
	jobDir := filepath.Join(s.spoolRoot, "incoming", jobID)
	if err := os.MkdirAll(jobDir, 0700); err != nil {
		span.RecordError(err)
		http.Error(w, `{"error":"internal error creating job directory"}`, http.StatusInternalServerError)
		return
	}

	tarPath := filepath.Join(jobDir, "payload.tar")
	spoolFile, err := os.OpenFile(tarPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
	if err != nil {
		span.RecordError(err)
		os.RemoveAll(jobDir)
		http.Error(w, `{"error":"internal error creating tar file"}`, http.StatusInternalServerError)
		return
	}

	// Cap incoming tar size before writing to spool.
	n, copyErr := io.Copy(spoolFile, io.LimitReader(tarFile, maxTarSize+1))
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

	j := job.NewJob(jobID, repo, "", tarPath)
	j.Path = subPath
	j.WebhookURL = webhookURL

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
