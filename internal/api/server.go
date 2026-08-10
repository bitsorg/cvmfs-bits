// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Package api defines the HTTP API server and request handlers for job submission,
// status polling, and event streaming. The Server manages authenticated requests,
// spawns background job orchestrators, and gracefully shuts down in-flight jobs.
package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/net/netutil"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"cvmfs.io/prepub/internal/broker"
	"cvmfs.io/prepub/internal/buildset"
	"cvmfs.io/prepub/internal/httpsig"
	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/lease"
	"cvmfs.io/prepub/internal/notify"
	"cvmfs.io/prepub/internal/spool"
	"cvmfs.io/prepub/pkg/cvmfscatalog"
	"cvmfs.io/prepub/pkg/observe"
)

// maxTarSize is the maximum accepted tar body (10 GiB).
const maxTarSize = 10 << 30

// Limits for streamed multipart submissions.  ParseMultipartForm applied its
// own implicit bounds; since submitJob now reads the parts itself, the bounds
// are explicit.
const (
	// maxFormFieldSize caps any single non-payload form field.  The largest
	// legitimate field is preload_paths (a JSON array of repo-relative paths).
	maxFormFieldSize = 1 << 20
	// maxMultipartParts caps the number of parts in one submission.  The API
	// defines ten fields plus the payload; the limit leaves room for growth
	// while still bounding the loop.
	maxMultipartParts = 64
)

// Server is the HTTP API server for job submission, status queries, and event streaming.
// It enforces bearer token authentication and manages background job goroutines.
type Server struct {
	// httpServer is the underlying HTTP server instance.
	httpServer *http.Server
	// router is the Gorilla mux router for path dispatching.
	router *mux.Router
	// obs provides logging, tracing, and metrics.
	obs *observe.Provider
	// apiToken is the shared secret for authenticated routes. Empty disables
	// auth (dev mode). It is used two ways depending on authMode: as a bearer
	// token compared directly, and/or as the HMAC key for a signed request.
	apiToken string
	// authMode selects which credentials are accepted:
	//
	//	AuthBearer — legacy only: the token travels on every request.
	//	AuthBoth   — either; the migration setting, and the default.
	//	AuthHMAC   — signed requests only; the token stops travelling.
	//
	// See ADR-0008 D3. The point of AuthHMAC is that observing a request no
	// longer yields a reusable credential.
	authMode AuthMode
	// nonces prevents a captured signature from being replayed.
	nonces *httpsig.NonceCache
	// signSkew bounds the accepted clock difference for signed requests.
	signSkew time.Duration
	// stopNonceSweeper ages the replay cache on a quiet service.
	stopNonceSweeper func()
	// allowedPublishPrefixes are the authorized CVMFS roots (full paths, e.g.
	// "/cvmfs/sft-nightlies-test.cern.ch/lcg"). A reserve/publish whose target does
	// not canonicalize under one of these is rejected (containment: a build can only
	// write inside an authorized group namespace). Empty ⇒ check disabled.
	allowedPublishPrefixes []string
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
		authMode:    AuthBoth,
		nonces:      httpsig.NewNonceCache(0, 0),
		signSkew:    httpsig.DefaultSkew,
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
	s.nonces.SetPressureHook(s.noncePressure)
	s.stopNonceSweeper = s.nonces.StartSweeper()
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

	// Fail-fast namespace reservation (POST /api/v1/reserve). Authenticated.
	reserve := s.router.PathPrefix("/api/v1/reserve").Subrouter()
	reserve.Use(s.requireAuth)
	reserve.HandleFunc("", s.reserveHandler).Methods("POST")

	// Coarse publish finalize (ADR-0007): publish a whole build's accumulated
	// packages in one commit. Authenticated.
	builds := s.router.PathPrefix("/api/v1/builds").Subrouter()
	builds.Use(s.requireAuth)
	builds.HandleFunc("/{id}/finalize", s.finalizeBuild).Methods("POST")
	// Seal: "I have submitted N jobs for this build" — prepub finalizes on its
	// own once N have accumulated, so the producer need not poll.
	builds.HandleFunc("/{id}/seal", s.sealBuild).Methods("POST")
	// Build status: one cheap call that tells a producer whether its build has
	// accumulated, is being finalized, or has finished — the alternative to
	// polling every package job to a terminal state.
	builds.HandleFunc("/{id}", s.buildStatus).Methods("GET")

	return s
}

// SetAllowedPublishPrefixes configures the authorized CVMFS roots (full paths).
// Called once at startup; empty leaves the containment check disabled (so existing
// single-namespace deployments are unaffected).
func (s *Server) SetAllowedPublishPrefixes(prefixes []string) {
	out := make([]string, 0, len(prefixes))
	for _, p := range prefixes {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, path.Clean(p))
		}
	}
	s.allowedPublishPrefixes = out
}

// validateSubPath rejects a job path that is not repository-relative.
//
// The submitted path is joined onto /cvmfs/<repo> everywhere downstream, and
// BOTH joins silently absorb an absolute path instead of rejecting it:
//
//	filepath.Join("/cvmfs", "test.cvmfs.io", "/cvmfs/bits.cern.ch/alice/x")
//	  => "/cvmfs/test.cvmfs.io/cvmfs/bits.cern.ch/alice/x"
//
// so a fully-qualified path from another repository lands *inside* this one and
// passes every containment check, because it genuinely is under the root — just
// at a nonsense location. Observed in production as
//
//	cvmfs_server ingest -N test.cvmfs.io -B cvmfs/bits.cern.ch/alice/el9-x86_64/...
//
// after a Testbed build reused packages whose .meta.json still carried the
// production prefix. Nothing complained until the gateway had a transaction
// open.
//
// The "cvmfs" leading-segment rule is the one that catches that case, and it is
// worth the small loss of generality: no legitimate repository-relative path
// starts with a `cvmfs/` component, and a caller that sends one has almost
// certainly passed a full /cvmfs/<repo>/... path by mistake.
func validateSubPath(p string) error {
	if p == "" {
		return nil // publish at the repository root
	}
	if strings.HasPrefix(p, "/") {
		return fmt.Errorf("path must be repository-relative, not absolute (got %q) — "+
			"send \"a/b/c\", not \"/cvmfs/<repo>/a/b/c\"", p)
	}
	clean := path.Clean(p)
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return fmt.Errorf("path escapes the repository (got %q)", p)
	}
	if clean == "cvmfs" || strings.HasPrefix(clean, "cvmfs/") {
		return fmt.Errorf("path starts with a %q component (got %q) — this is "+
			"almost always a full /cvmfs/<repo>/... path submitted where a "+
			"repository-relative one was expected; it would be published at "+
			"<repo>/%s", "cvmfs", p, clean)
	}
	return nil
}

// publishAuthorized reports whether a {repo, subPath} target resolves to a path
// under an authorized CVMFS root. path.Clean collapses any ".." so a traversal
// cannot escape the namespace. No configured prefixes ⇒ allowed (check disabled).
func (s *Server) publishAuthorized(repo, subPath string) bool {
	if len(s.allowedPublishPrefixes) == 0 {
		return true
	}
	full := path.Clean("/cvmfs/" + repo + "/" + subPath)
	for _, pre := range s.allowedPublishPrefixes {
		if full == pre || strings.HasPrefix(full, pre+"/") {
			return true
		}
	}
	return false
}

// reserveHandler handles POST /api/v1/reserve. Fail-fast namespace check:
// acquire a single-attempt gateway lease on {"repo","path"} and release it at
// once. Returns 204 free, 409 taken, 400 bad body, 502 gateway error.
func (s *Server) reserveHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.obs.Tracer.Start(r.Context(), "api.reserve")
	defer span.End()

	var req struct {
		Repo string `json:"repo"`
		Path string `json:"path"`
	}
	// Cap the body like the submit path (1 MiB): an authenticated client must
	// not be able to balloon server memory with an arbitrarily large JSON body.
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid JSON body"}`, http.StatusBadRequest)
		return
	}
	if req.Repo == "" {
		http.Error(w, `{"error":"repo is required"}`, http.StatusBadRequest)
		return
	}
	// Same repo-name validation as the submit path: nothing malformed may
	// reach the Stratum0 URL builder or the gateway lease request.
	if err := broker.ValidateRepo(req.Repo); err != nil {
		http.Error(w, `{"error":"invalid repo"}`, http.StatusBadRequest)
		return
	}

	// Containment: the target must be under an authorized CVMFS root, so a build
	// cannot reserve (and then publish into) another group's namespace.
	if !s.publishAuthorized(req.Repo, req.Path) {
		s.obs.Logger.Warn("reserve: target outside authorized namespace",
			"repo", req.Repo, "path", req.Path)
		http.Error(w, `{"error":"forbidden: target path is outside this deployment's authorized CVMFS namespace"}`, http.StatusForbidden)
		return
	}

	// Fail-fast reservation is a gateway concern: only the gateway *lease.Client
	// can take a single-attempt lease on a path. Single-host (local) mode has no
	// shared gateway lease to conflict on, so the namespace is always reservable.
	cl, ok := s.orch.Lease.(*lease.Client)
	if !ok {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Reject a path that is already published: a package/version is published
	// once, so a duplicate must fail before it wastes a build. The gateway lease
	// probe below cannot catch this (a lease on an existing path is granted), so
	// walk the published catalog. Best-effort: a probe error must not block a
	// legitimate publish, so on error we log and fall through to the lease probe.
	if s.orch.Stratum0URL != "" && req.Path != "" {
		if exists, exErr := cvmfscatalog.PathExists(ctx, nil, s.orch.Stratum0URL, req.Repo, req.Path); exErr != nil {
			s.obs.Logger.Warn("reserve: existence check failed — allowing",
				"repo", req.Repo, "path", req.Path, "error", exErr)
		} else if exists {
			s.obs.Logger.Info("reserve: path already published", "repo", req.Repo, "path", req.Path)
			http.Error(w, `{"error":"already published: this package/version already exists in the repository"}`, http.StatusConflict)
			return
		}
	}

	// TryAcquireOnce (no path_busy retry): fail immediately if the namespace is
	// taken instead of waiting out the gateway's max_lease_time like publish does.
	token, err := cl.TryAcquireOnce(ctx, req.Repo, req.Path)
	if err != nil {
		if errors.Is(err, lease.ErrPathBusy) {
			s.obs.Logger.Info("reserve: namespace taken", "repo", req.Repo, "path", req.Path)
			http.Error(w, `{"error":"namespace taken: another publisher holds the lease"}`, http.StatusConflict)
			return
		}
		// Log the detail; return a generic error so gateway internals are not leaked.
		s.obs.Logger.Warn("reserve: lease acquire failed", "repo", req.Repo, "path", req.Path, "error", err)
		http.Error(w, `{"error":"reservation failed"}`, http.StatusBadGateway)
		return
	}

	// Release without committing — this was only a reservation probe.
	if err := cl.Release(ctx, token, false); err != nil {
		s.obs.Logger.Warn("reserve: lease release failed (will expire on its own)",
			"repo", req.Repo, "path", req.Path, "error", err)
	}
	w.WriteHeader(http.StatusNoContent)
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
	if s.stopNonceSweeper != nil {
		s.stopNonceSweeper()
	}

	httpErr := s.httpServer.Shutdown(ctx)

	// Phase 1: wait for all job goroutines and webhook deliveries.
	// After this, no new items will be enqueued in DistManager.
	done := make(chan struct{})
	go func() {
		s.jobWg.Wait()
		// Auto-finalize runs detached from the job that triggered it, so it is
		// not covered by jobWg.  Waiting here is what stops a restart from
		// SIGKILLing an ingestsql commit mid-flight — the claim marker would
		// then keep auto-finalize off for that build permanently.
		s.orch.finalizeWg.Wait()
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
		buildID                   string   // optional: groups a build's packages (ADR-0007)
		buildExpect               int      // optional: package count → auto-finalize when reached
		finalize                  bool     // coarse-publish finalize job (no tar payload)
		publishPath               string   // optional: "prepub" (default) or "ingest"
		preWarm                   *bool    // optional: nil = node default
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
			BuildID        string   `json:"build_id"`
			BuildExpect    int      `json:"build_expect"`
			PublishPath    string   `json:"publish_path"`
			PreWarm        *bool    `json:"prewarm"`
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, `{"error":"failed to read request body"}`, http.StatusBadRequest)
			return
		}
		// This route is exempt from the middleware's body binding because it is
		// shared with the multi-gigabyte multipart upload, so the JSON branch
		// binds its own body — BEFORE a single field is looked at, so that no
		// part of the handler acts on bytes the signature has not committed to.
		if err := requireSignedJSONBody(r, body); err != nil {
			s.rejectAuth(w, r, "signature rejected: "+err.Error())
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
		buildID = req.BuildID
		buildExpect = req.BuildExpect
		publishPath = req.PublishPath
		preWarm = req.PreWarm

	} else {
		// ── Multipart upload mode (default) ─────────────────────────────────
		//
		// The payload is streamed part-by-part instead of going through
		// r.ParseMultipartForm.  ParseMultipartForm spools everything beyond
		// its in-memory threshold to a temporary file, which the handler then
		// copies into the spool: every tar is written to disk twice, and the
		// producer waits for both writes before it receives a job_id.  Reading
		// the parts ourselves writes the payload exactly once, straight into
		// the job directory.
		//
		// Parts are processed in transmission order and field values are not
		// available until their part arrives, so validation that depends on
		// them happens after the loop.  A rejected submission removes jobDir,
		// exactly as before — and ParseMultipartForm would have written the
		// whole body to disk before rejecting it anyway, so nothing regresses.
		// Bound the whole body.  ParseMultipartForm inherited no such bound
		// either, but it is worth adding here: the per-part LimitReader below
		// stops us from STORING more than maxTarSize, while multipart.Part.Close
		// drains whatever remains, so without this a client could make the
		// server read an unbounded stream after the limit had already tripped.
		r.Body = http.MaxBytesReader(w, r.Body, maxTarSize+maxFormFieldSize*maxMultipartParts)

		mr, mrErr := r.MultipartReader()
		if mrErr != nil {
			http.Error(w, `{"error":"invalid multipart form"}`, http.StatusBadRequest)
			return
		}

		// First value wins, matching r.FormValue's behaviour for duplicated
		// fields.
		fields := make(map[string]string, maxMultipartParts)
		setField := func(k, v string) {
			if _, dup := fields[k]; !dup {
				fields[k] = v
			}
		}
		hasher := sha256.New()
		sawTar := false

		for i := 0; ; i++ {
			part, partErr := mr.NextPart()
			if errors.Is(partErr, io.EOF) {
				break
			}
			if partErr != nil {
				os.RemoveAll(jobDir)
				http.Error(w, `{"error":"invalid multipart form"}`, http.StatusBadRequest)
				return
			}
			// Bound the part count so a malicious client cannot keep the
			// handler (and a spool job directory) alive indefinitely.
			if i >= maxMultipartParts {
				part.Close()
				os.RemoveAll(jobDir)
				http.Error(w, `{"error":"too many multipart parts"}`, http.StatusBadRequest)
				return
			}

			// The payload is the part named "tar" that carries a filename.
			// r.FormFile required one (returning ErrMissingFile otherwise), so a
			// plain text field called "tar" must stay a field, not become a
			// package.
			if part.FormName() != "tar" || part.FileName() == "" {
				// Ordinary form field: small, safe to buffer, but still capped.
				v, readErr := io.ReadAll(io.LimitReader(part, maxFormFieldSize+1))
				name := part.FormName()
				part.Close()
				if readErr != nil {
					os.RemoveAll(jobDir)
					http.Error(w, `{"error":"invalid multipart form"}`, http.StatusBadRequest)
					return
				}
				if int64(len(v)) > maxFormFieldSize {
					os.RemoveAll(jobDir)
					http.Error(w, fmt.Sprintf(`{"error":"form field %q exceeds %d bytes"}`, name, maxFormFieldSize), http.StatusRequestEntityTooLarge)
					return
				}
				setField(name, string(v))
				continue
			}

			// ── The payload ──────────────────────────────────────────────
			if sawTar {
				part.Close()
				os.RemoveAll(jobDir)
				http.Error(w, `{"error":"duplicate tar part"}`, http.StatusBadRequest)
				return
			}
			sawTar = true

			if err := os.MkdirAll(jobDir, 0700); err != nil {
				part.Close()
				span.RecordError(err)
				http.Error(w, `{"error":"internal error creating job directory"}`, http.StatusInternalServerError)
				return
			}
			spoolTarPath = filepath.Join(jobDir, "payload.tar")
			spoolFile, openErr := os.OpenFile(spoolTarPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
			if openErr != nil {
				part.Close()
				span.RecordError(openErr)
				os.RemoveAll(jobDir)
				http.Error(w, `{"error":"internal error creating tar file"}`, http.StatusInternalServerError)
				return
			}

			// Always hash: tar_sha256 may not have been seen yet (field order
			// is the client's choice), and hashing a stream we are already
			// writing costs far less than a second pass over the file.
			n, copyErr := io.Copy(io.MultiWriter(spoolFile, hasher), io.LimitReader(part, maxTarSize+1))
			closeErr := spoolFile.Close()
			// Reject an oversized payload BEFORE part.Close(), which drains the
			// remainder of the part — otherwise the server reads the entire
			// body it has just decided to refuse.
			if n > maxTarSize {
				os.RemoveAll(jobDir)
				http.Error(w, `{"error":"tar exceeds maximum allowed size"}`, http.StatusRequestEntityTooLarge)
				return
			}
			part.Close()
			if copyErr != nil || closeErr != nil {
				os.RemoveAll(jobDir)
				// A client that disconnects mid-upload is not a server fault;
				// ParseMultipartForm surfaced this as a 400 and so do we.
				if closeErr == nil {
					http.Error(w, `{"error":"upload interrupted"}`, http.StatusBadRequest)
					return
				}
				span.RecordError(errors.Join(copyErr, closeErr))
				http.Error(w, `{"error":"error writing tar to spool"}`, http.StatusInternalServerError)
				return
			}
		}

		// Form fields ONLY. r.FormValue used to merge URL query parameters, and
		// an earlier version of this handler preserved that for compatibility —
		// but the signature covers the form fields, so a query parameter was a
		// way to set webhook_url, finalize, build_expect, tag_name or
		// publish_path on a request whose MAC still verified. Nothing sends
		// these as query parameters, so the compatibility was worth strictly
		// less than the hole it opened.
		// ── Second half of signature verification ────────────────────────────
		// The middleware checked the MAC before any body was read; only now are
		// the fields and the payload known, so only now can we confirm they are
		// the ones the signature committed to. A signed request that skipped
		// this would be authenticated in name only — the header would be
		// genuine while the body had been replaced in flight.
		//
		// This runs BEFORE the fields are interpreted or validated. Doing it
		// afterwards still refuses the request, but it first lets an attacker
		// who cannot forge a MAC learn — from whether the reply is a 400 about
		// a specific field or the generic 401 — which of his substitutions
		// would have been well-formed. Unbound input gets no answers at all.
		computed := hex.EncodeToString(hasher.Sum(nil))
		if sig := signatureFrom(r); sig != nil {
			bodyHash := computed
			if !sawTar {
				bodyHash = "" // Bound() normalises this to the no-payload marker
			}
			if err := requireSignatureBinding(r, fields, bodyHash); err != nil {
				os.RemoveAll(jobDir)
				s.obs.Logger.Warn("signed submission does not match its signature",
					"remote_addr", r.RemoteAddr, "error", err)
				http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusUnauthorized)
				return
			}
			// bh already binds the payload directly, so this is not about
			// coverage — it closes a cross-branch replay. A signature made for
			// a JSON submission has fd=NoFields and bh=sha256(document); resend
			// it as a multipart carrying zero form fields and that document as
			// the tar part and Bound() is satisfied exactly. Requiring
			// tar_sha256 makes such a request impossible to construct, since
			// the JSON signature's empty field set cannot contain it.
			if sawTar && fields["tar_sha256"] == "" {
				os.RemoveAll(jobDir)
				http.Error(w, fmt.Sprintf(`{"error":%q}`, errSignedWithoutDigest.Error()), http.StatusBadRequest)
				return
			}
		}

		field := func(k string) string { return fields[k] }

		repo = field("repo")
		subPath = field("path")
		webhookURL = field("webhook_url")
		submittedSHA256 = field("tar_sha256") // optional
		tagName = field("tag_name")
		tagDescription = field("tag_description")
		preloadExe = field("preload_exe") // optional
		buildID = field("build_id")       // optional (ADR-0007 coarse publish)
		finalize = field("finalize") == "true"
		// build_expect: how many packages this build will contain.  When set,
		// prepub finalizes the build itself once that many have accumulated,
		// so the producer can exit after its last upload.
		if raw := field("build_expect"); raw != "" {
			n, convErr := strconv.Atoi(strings.TrimSpace(raw))
			if convErr != nil || n < 0 {
				os.RemoveAll(jobDir)
				http.Error(w, `{"error":"build_expect must be a non-negative integer"}`, http.StatusBadRequest)
				return
			}
			buildExpect = n
		}
		publishPath = field("publish_path")
		// prewarm is tri-state: absent means "use the node default", so an
		// unset field must NOT be read as false.
		if raw := field("prewarm"); raw != "" {
			v, convErr := strconv.ParseBool(strings.TrimSpace(raw))
			if convErr != nil {
				os.RemoveAll(jobDir)
				http.Error(w, `{"error":"prewarm must be a boolean"}`, http.StatusBadRequest)
				return
			}
			preWarm = &v
		}

		if repo == "" {
			os.RemoveAll(jobDir)
			http.Error(w, `{"error":"repo field is required"}`, http.StatusBadRequest)
			return
		}
		if err := broker.ValidateRepo(repo); err != nil {
			os.RemoveAll(jobDir)
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
			return
		}
		// preload_paths is a JSON-encoded []string (e.g. '["bin/root","lib/libCore.so"]')
		if raw := fields["preload_paths"]; raw != "" {
			if err := json.Unmarshal([]byte(raw), &preloadPaths); err != nil {
				os.RemoveAll(jobDir)
				http.Error(w, `{"error":"preload_paths must be a JSON array of strings"}`, http.StatusBadRequest)
				return
			}
		}
		if err := job.ValidateTagName(tagName); err != nil {
			os.RemoveAll(jobDir)
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
			return
		}

		switch {
		case finalize:
			// A finalize job carries no payload.  If the client sent one
			// anyway, drop it rather than leaving an orphan in the spool.
			if sawTar {
				os.RemoveAll(jobDir)
				spoolTarPath = ""
			}
		case !sawTar:
			os.RemoveAll(jobDir)
			http.Error(w, `{"error":"tar field is required"}`, http.StatusBadRequest)
			return
		case submittedSHA256 != "":
			if !strings.EqualFold(computed, submittedSHA256) {
				os.RemoveAll(jobDir)
				http.Error(w, fmt.Sprintf(`{"error":"tar_sha256 mismatch: got %s, expected %s"}`, computed, submittedSHA256), http.StatusBadRequest)
				return
			}
		}
	}

	// A finalize job requires a build_id and carries no payload.
	if finalize && buildID == "" {
		http.Error(w, `{"error":"finalize requires build_id"}`, http.StatusBadRequest)
		return
	}

	// Shape: the path must be REPOSITORY-RELATIVE. Checked before containment,
	// because a malformed path defeats the containment check rather than
	// tripping it (see validateSubPath).
	if !finalize {
		if err := validateSubPath(subPath); err != nil {
			os.RemoveAll(jobDir)
			s.obs.Logger.Warn("submit: malformed target path",
				"repo", repo, "path", subPath, "error", err)
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
			return
		}
	}

	// Containment: a payload job must publish inside this deployment's authorized
	// CVMFS namespace. Finalize carries no path and only commits packages that
	// already passed this check at submit time, so it is exempt.
	if !finalize && !s.publishAuthorized(repo, subPath) {
		os.RemoveAll(jobDir)
		s.obs.Logger.Warn("submit: target outside authorized namespace", "repo", repo, "path", subPath)
		http.Error(w, `{"error":"forbidden: target path is outside this deployment's authorized CVMFS namespace"}`, http.StatusForbidden)
		return
	}

	// The publish path must be one this deployment can actually serve.  Failing
	// here — rather than falling back to the default — is deliberate: the paths
	// differ in where content is chunked and deduped, whether it can be
	// pre-warmed, and whether the commit is per package or per build.  A job
	// that quietly took the other one would look identical and be wrong.
	publishPath = strings.TrimSpace(publishPath)
	if !s.orch.HasPublishPath(publishPath) {
		os.RemoveAll(jobDir)
		s.obs.Logger.Warn("submit: unsupported publish path",
			"publish_path", publishPath, "available", s.orch.PublishPathNames())
		http.Error(w, fmt.Sprintf(`{"error":"publish path %q is not configured on this prepub (available: %s)"}`,
			jsonEscape(publishPath), jsonEscape(strings.Join(s.orch.PublishPathNames(), ", "))),
			http.StatusBadRequest)
		return
	}
	if publishPath != "" && publishPath != DefaultPublishPath {
		// Pre-warming is a property of the prepub pipeline: the ingest path
		// commits through the gateway, so there is no window in which the
		// objects exist and the catalog has not yet flipped. Accepting the
		// request and ignoring it would be worse than saying so.
		if preWarm != nil && *preWarm {
			os.RemoveAll(jobDir)
			http.Error(w, fmt.Sprintf(`{"error":"publish path %q cannot pre-warm Stratum 1 caches; drop prewarm or use the %q path"}`,
				jsonEscape(publishPath), DefaultPublishPath), http.StatusBadRequest)
			return
		}
		// Likewise coarse publish: an alternative path commits each package as
		// it arrives, so there is nothing to accumulate and a finalize would
		// never fire. Silently dropping build_id would leave the producer
		// waiting on a build that can never complete.
		if buildID != "" {
			os.RemoveAll(jobDir)
			http.Error(w, fmt.Sprintf(`{"error":"publish path %q commits each package on arrival and cannot take part in a coarse build; drop build_id or use the %q path"}`,
				jsonEscape(publishPath), DefaultPublishPath), http.StatusBadRequest)
			return
		}
	}

	// Record the build's expected package count before the job can accumulate,
	// so that the last package to finish sees a complete declaration and can
	// trigger the finalize itself.  Every package of the build carries the same
	// value; the write is atomic and idempotent.  A finalize job never declares
	// (it IS the finalize).
	if buildID != "" && buildExpect > 0 && !finalize {
		if err := buildset.SetExpect(s.spoolRoot, buildID, buildExpect); err != nil {
			span.RecordError(err)
			os.RemoveAll(jobDir)
			http.Error(w, `{"error":"internal error recording build expectation"}`, http.StatusInternalServerError)
			return
		}
	}

	j := job.NewJob(jobID, repo, "", spoolTarPath)
	j.Path = subPath
	j.BuildID = buildID
	j.Finalize = finalize
	j.WebhookURL = webhookURL
	j.TarSHA256 = submittedSHA256
	j.TagName = tagName
	j.TagDescription = tagDescription
	j.PreloadExe = preloadExe
	j.PreloadPaths = preloadPaths
	j.PublishPath = publishPath
	j.PreWarm = preWarm

	// Record the original filename and size for the console tooltip.
	// Use Stat on the spool copy since the original may have been moved.
	// Finalize jobs carry no payload, so there is nothing to stat.
	if spoolTarPath != "" {
		j.TarName = filepath.Base(spoolTarPath)
		if fi, statErr := os.Stat(spoolTarPath); statErr == nil {
			j.TarSize = fi.Size()
		}
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
		// grantedWeight is the admission cost this job was charged; Release must
		// return exactly that, not a recomputed value — the effective limit (and
		// hence the clamp inside jobWeight) can change while the job runs.
		grantedWeight := 0
		releaseSem := func() {
			semOnce.Do(func() {
				if s.dynaSem != nil {
					s.dynaSem.Release(grantedWeight)
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
			gw, err := s.dynaSem.Acquire(abortCtx, j.TarSize)
			if err != nil {
				// abortCancel fired (operator abort or server shutdown) while
				// the job was queued; mark it as aborted without running.
				s.obs.Logger.Info("job aborted while waiting for slot",
					"job_id", jobID, "error", err)
				_ = s.orch.abortJob(context.Background(), j,
					fmt.Errorf("aborted while waiting for concurrency slot: %w", err))
				return
			}
			grantedWeight = gw
			s.obs.Logger.Info("job acquired concurrency slot",
				"job_id", jobID, "weight", gw, "tar_bytes", j.TarSize)
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

	// Advertise the publish paths this node serves. A producer otherwise finds
	// out only by uploading a package and getting a 400 for every job — the
	// console's per-community toggle can be enabled for a node that was never
	// started with the corresponding backend.
	nonces, rejectedFull := s.nonces.Stats()
	body := struct {
		Status       string   `json:"status"`
		PublishPaths []string `json:"publish_paths"`
		AuthMode     string   `json:"auth_mode"`
		// FinalizeReady reports whether a sealed coarse build can actually be
		// published here. False means uploads succeed and the commit never
		// happens, which is invisible to a producer that has already exited.
		FinalizeReady bool `json:"finalize_ready"`
		// ReplayCache surfaces the fail-closed counter: a non-zero
		// rejected_full means signed requests are being refused for capacity
		// reasons, which looks like an auth problem from the client side and
		// is invisible otherwise.
		ReplayCache struct {
			Entries      int    `json:"entries"`
			RejectedFull uint64 `json:"rejected_full"`
		} `json:"replay_cache"`
	}{Status: "healthy", AuthMode: string(s.authMode)}
	body.ReplayCache.Entries = nonces
	body.ReplayCache.RejectedFull = rejectedFull
	if s.orch != nil {
		body.PublishPaths = s.orch.PublishPathNames()
		body.FinalizeReady = s.orch.IngestConfigPrefix != ""
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(body)
}
