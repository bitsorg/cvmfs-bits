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
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"

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

// defaultMaxTarSize is the largest accepted tar unless SetUploadLimits
// raises it (10 GiB).
const defaultMaxTarSize = 10 << 30

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
	// The point of AuthHMAC is that observing a request no
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
	// maxTarSize is the largest tar a submission may carry.
	maxTarSize int64
	// spoolMinFree is the free space an upload must leave on the spool
	// filesystem; 0 disables the check.
	spoolMinFree int64
	// stagingRoot is the operator-configured directory from which tar_path
	// references (JSON submissions) are allowed.  Empty disables JSON/tar_path
	// mode — callers must upload the tar as multipart/form-data instead.
	stagingRoot string
	// jobWg tracks all background job goroutines so Shutdown can wait for them
	// to reach a terminal state before the process exits.
	jobWg sync.WaitGroup
	// launchMu and draining keep launch from adding to jobWg once Shutdown
	// has started waiting on it (recovery may still be launching jobs).
	launchMu sync.Mutex
	draining bool
	// stop ends when Shutdown starts, so jobs waiting to retry stop waiting
	// (they stay in incoming for the next start) instead of holding it up.
	stop       context.Context
	stopCancel context.CancelFunc
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
		maxTarSize:  defaultMaxTarSize,
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
	s.stop, s.stopCancel = context.WithCancel(context.Background())
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

	// Measurements (internal/measure): the per-publish records behind the
	// performance comparisons. PUBLIC by design — read-only performance
	// stats a CI run or a person fetches without a token, so they can be
	// captured per build without log scraping. They expose repository paths,
	// object/byte counts and failure causes, judged non-sensitive; nothing
	// here mutates state (GET only).
	meas := s.router.PathPrefix("/api/v1/measurements").Subrouter()
	meas.HandleFunc("", s.measurementBuildsHandler).Methods("GET")
	meas.HandleFunc("/{build}", s.measurementsHandler).Methods("GET")

	// Fail-fast namespace reservation (POST /api/v1/reserve). Authenticated.
	reserve := s.router.PathPrefix("/api/v1/reserve").Subrouter()
	reserve.Use(s.requireAuth)
	reserve.HandleFunc("", s.reserveHandler).Methods("POST")

	// Is a path already published, and by which build (POST /api/v1/published)?
	// Lets a producer skip a package that is already there. Authenticated.
	published := s.router.PathPrefix("/api/v1/published").Subrouter()
	published.Use(s.requireAuth)
	published.HandleFunc("", s.publishedHandler).Methods("POST")
	// The metadata files bits keeps in published trees (.meta.json,
	// .bits-view.json), read in one batch: a merged view is rebuilt from them.
	published.HandleFunc("/files", s.publishedFilesHandler).Methods("POST")

	// Coarse publish finalize: publish a whole build's accumulated
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

// SetUploadLimits sets the largest accepted tar and the free space an upload
// must leave on the spool. maxTar <= 0 keeps the default; minFree <= 0
// disables the free-space check.
func (s *Server) SetUploadLimits(maxTar, minFree int64) {
	if maxTar > 0 {
		s.maxTarSize = maxTar
	}
	s.spoolMinFree = max(minFree, 0)
}

// uploadRefusal says why a multipart body of the declared size (-1 when
// unknown) cannot be stored, or "" when it can. Refusing on the header stores
// nothing; a client that waits for the answer (curl, Expect: 100-continue) is
// told why, one that keeps sending may still only see the connection close,
// so producers should also check max_tar_size from /api/v1/health. Each
// upload is checked on its own, so concurrent ones can overshoot the floor.
func (s *Server) uploadRefusal(size int64) (string, int) {
	if size > s.maxTarSize+maxFormFieldSize*maxMultipartParts {
		return fmt.Sprintf(`{"error":"upload of %d bytes exceeds this node's limit of %d bytes (max_tar_size_gib)"}`,
			size, s.maxTarSize), http.StatusRequestEntityTooLarge
	}
	if s.spoolMinFree == 0 {
		return "", 0
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(s.spoolRoot, &st); err != nil {
		return "", 0 // cannot tell: let the write itself fail if it must
	}
	free := int64(st.Bavail) * int64(st.Bsize)
	if free-max(size, 0) < s.spoolMinFree {
		return fmt.Sprintf(`{"error":"spool full: %d bytes free, upload of %d bytes would leave less than %d (spool_min_free_gib)"}`,
			free, max(size, 0), s.spoolMinFree), http.StatusInsufficientStorage
	}
	return "", 0
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

// validateReplace accepts replace only where it can mean one thing: the
// job's own path, on a node that allows replacing. What is then replaced is
// decided at commit time, by a readable published hash that differs from
// identity_hash; a shared root (a view, a modules directory) has none.
func (s *Server) validateReplace(finalize bool, subPath, identity, hash string) error {
	switch {
	case s.orch == nil || !s.orch.ReplaceAllowed():
		return fmt.Errorf("replace is not enabled on this prepub (replace_on_conflict)")
	case finalize:
		return fmt.Errorf("replace does not apply to a finalize job")
	case strings.Trim(subPath, "/") == "":
		return fmt.Errorf("replace never applies to the repository root")
	case identity == "" || path.Clean(identity) != path.Clean(subPath) || hash == "":
		return fmt.Errorf("replace requires identity_path equal to path and an identity_hash")
	}
	return nil
}

// validateIdentityPath accepts an empty identity, or one at or under the job's
// path: a job may only claim to be satisfied by content inside its own lease.
func validateIdentityPath(subPath, identity string) error {
	if identity == "" {
		return nil
	}
	if err := validateSubPath(identity); err != nil {
		return fmt.Errorf("identity_path: %w", err)
	}
	id, base := path.Clean(identity), path.Clean(subPath)
	if subPath == "" || id == base || strings.HasPrefix(id, base+"/") {
		return nil
	}
	return fmt.Errorf("identity_path %q is not at or under path %q", identity, subPath)
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

// publishedHandler handles POST /api/v1/published {"repo","path"}. Answers
// {"exists": false} or {"exists": true, "hash": "<build hash>"}; the hash is
// the package hash from <path>/.meta.json, empty when there is none (e.g. a
// modulefile). 501 without a stratum0 to read from, 502 when it cannot be read.
func (s *Server) publishedHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.obs.Tracer.Start(r.Context(), "api.published")
	defer span.End()
	w.Header().Set("Content-Type", "application/json")

	var req struct {
		Repo string `json:"repo"`
		Path string `json:"path"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid JSON body"}`, http.StatusBadRequest)
		return
	}
	if req.Repo == "" || req.Path == "" || broker.ValidateRepo(req.Repo) != nil ||
		validateSubPath(req.Path) != nil {
		http.Error(w, `{"error":"a valid repo and path are required"}`, http.StatusBadRequest)
		return
	}
	if !s.publishAuthorized(req.Repo, req.Path) {
		http.Error(w, `{"error":"forbidden: target path is outside this deployment's authorized CVMFS namespace"}`, http.StatusForbidden)
		return
	}
	if s.orch.Stratum0URL == "" {
		http.Error(w, `{"error":"no stratum0 configured"}`, http.StatusNotImplemented)
		return
	}
	exists, err := cvmfscatalog.PathExists(ctx, nil, s.orch.Stratum0URL, req.Repo, req.Path)
	if err != nil {
		s.obs.Logger.Warn("published: lookup failed", "repo", req.Repo, "path", req.Path, "error", err)
		http.Error(w, `{"error":"cannot read the published repository"}`, http.StatusBadGateway)
		return
	}
	resp := struct {
		Exists bool   `json:"exists"`
		Hash   string `json:"hash,omitempty"`
	}{Exists: exists}
	if exists {
		hash, _, rerr := publishedPackageHash(ctx, s.orch.Stratum0URL, req.Repo, req.Path)
		if rerr != nil {
			// Unknown, not "exists with no hash": the producer then publishes.
			s.obs.Logger.Warn("published: .meta.json read failed", "repo", req.Repo, "path", req.Path, "error", rerr)
			http.Error(w, `{"error":"cannot read the published .meta.json"}`, http.StatusBadGateway)
			return
		}
		resp.Hash = hash
	}
	json.NewEncoder(w).Encode(resp)
}

// Limits of POST /api/v1/published/files: paths per request (a producer
// batches), the size of one file and of all the files of one answer.
const (
	maxPublishedFiles     = 512
	maxPublishedFileBytes = 16 << 20
	maxPublishedFilesSum  = 64 << 20
	publishedFileWorkers  = 8
)

// publishedFileNames are the files POST /api/v1/published/files reads: the
// metadata bits writes into what it publishes. Not a general file server.
var publishedFileNames = map[string]bool{".meta.json": true, ".bits-view.json": true}

// readPublishedFilesFn is a test seam; production reads the published
// catalogs on stratum0.
var readPublishedFilesFn = cvmfscatalog.ReadPublishedFiles

// publishedFilesHandler handles POST /api/v1/published/files
// {"repo","paths":[...]}: the content of each path, all read from the same
// published revision, as {"files": {"<path>": <its JSON> | null}}. null means
// not published; a file that is not valid JSON is null and listed in
// "invalid". Only .meta.json and .bits-view.json can be read.
func (s *Server) publishedFilesHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.obs.Tracer.Start(r.Context(), "api.published.files")
	defer span.End()
	w.Header().Set("Content-Type", "application/json")

	var req struct {
		Repo  string   `json:"repo"`
		Paths []string `json:"paths"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<20)).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid JSON body"}`, http.StatusBadRequest)
		return
	}
	if req.Repo == "" || broker.ValidateRepo(req.Repo) != nil || len(req.Paths) == 0 {
		http.Error(w, `{"error":"a valid repo and at least one path are required"}`, http.StatusBadRequest)
		return
	}
	if len(req.Paths) > maxPublishedFiles {
		http.Error(w, fmt.Sprintf(`{"error":"at most %d paths per request"}`, maxPublishedFiles),
			http.StatusRequestEntityTooLarge)
		return
	}
	paths := make([]string, 0, len(req.Paths))
	seen := make(map[string]bool, len(req.Paths))
	for _, p := range req.Paths {
		// Canonical paths only: the read looks the path up as given.
		if p == "" || p != path.Clean(p) || validateSubPath(p) != nil || !publishedFileNames[path.Base(p)] {
			http.Error(w, `{"error":"each path must be a canonical repository-relative .meta.json or .bits-view.json"}`,
				http.StatusBadRequest)
			return
		}
		if !s.publishAuthorized(req.Repo, p) {
			http.Error(w, `{"error":"forbidden: a path is outside this deployment's authorized CVMFS namespace"}`,
				http.StatusForbidden)
			return
		}
		if !seen[p] {
			seen[p] = true
			paths = append(paths, p)
		}
	}
	if s.orch.Stratum0URL == "" {
		http.Error(w, `{"error":"no stratum0 configured"}`, http.StatusNotImplemented)
		return
	}
	data, oversized, err := readPublishedFilesFn(ctx, nil, s.orch.Stratum0URL, req.Repo,
		paths, cvmfscatalog.ReadLimits{Workers: publishedFileWorkers,
			MaxFileBytes: maxPublishedFileBytes, MaxTotalBytes: maxPublishedFilesSum})
	if errors.Is(err, cvmfscatalog.ErrTooLarge) {
		http.Error(w, fmt.Sprintf(`{"error":"the files exceed %d bytes: ask for fewer paths"}`,
			maxPublishedFilesSum), http.StatusRequestEntityTooLarge)
		return
	}
	if err != nil {
		s.obs.Logger.Warn("published/files: read failed", "repo", req.Repo, "error", err)
		http.Error(w, `{"error":"cannot read the published repository"}`, http.StatusBadGateway)
		return
	}
	resp := struct {
		Files   map[string]json.RawMessage `json:"files"`
		Invalid []string                   `json:"invalid,omitempty"`
	}{Files: make(map[string]json.RawMessage, len(paths))}
	big := make(map[string]bool, len(oversized))
	for _, p := range oversized {
		big[p] = true
	}
	for _, p := range paths {
		b, ok := data[p]
		switch {
		case big[p] || (ok && !json.Valid(b)):
			resp.Files[p] = json.RawMessage("null")
			resp.Invalid = append(resp.Invalid, p)
		case !ok:
			resp.Files[p] = json.RawMessage("null")
		default:
			resp.Files[p] = json.RawMessage(b)
		}
	}
	json.NewEncoder(w).Encode(resp)
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
// from a fixed S0 endpoint.
func (s *Server) MountDiscovery(h http.Handler) {
	if h != nil {
		s.router.Handle("/cvmfs/{repo}/.cvmfsbits", h).Methods("GET")
	}
}

// RevokePath and UnrevokePath are the API routes that revoke a receiver's
// control-plane access and lift that revocation.
const (
	RevokePath   = "/api/v1/control/revoke"
	UnrevokePath = "/api/v1/control/unrevoke"
)

// MountRevoke mounts revoke at POST RevokePath and unrevoke at POST
// UnrevokePath behind the normal API auth, so an operator can manage
// revocations without the separate TLS control listener. It mounts nothing,
// and returns false, when the API token is empty: auth is then off (dev mode)
// and anyone could revoke or un-revoke receivers.
func (s *Server) MountRevoke(revoke, unrevoke http.Handler) bool {
	if revoke == nil || unrevoke == nil || s.apiToken == "" {
		return false
	}
	for path, h := range map[string]http.Handler{RevokePath: revoke, UnrevokePath: unrevoke} {
		rv := s.router.PathPrefix(path).Subrouter()
		rv.Use(s.requireAuth)
		rv.Handle("", h).Methods(http.MethodPost)
	}
	return true
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
	s.stopCancel()
	// Stop the dynamic semaphore load-poller first so it doesn't interfere
	// with the graceful drain below.
	if s.dynaSem != nil {
		s.dynaSem.Stop()
	}
	if s.stopNonceSweeper != nil {
		s.stopNonceSweeper()
	}

	httpErr := s.httpServer.Shutdown(ctx)

	s.launchMu.Lock()
	s.draining = true
	s.launchMu.Unlock()

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
		job.StateAccumulated, // coarse-publish package jobs (committed by their build's finalize)
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
		// omitzero, not omitempty: omitempty never omits a struct, so a zero
		// time went out as "0001-01-01T00:00:00Z" and looked set to the console.
		// Used by the console Monitoring chart to build per-job stage breakdowns.
		PipelineStartedAt time.Time `json:"pipeline_started_at,omitzero"`
		PipelineEndedAt   time.Time `json:"pipeline_ended_at,omitzero"`
		LeasedAt          time.Time `json:"leased_at,omitzero"`
		PublishedAt       time.Time `json:"published_at,omitzero"`
		// When S1 pre-warming was launched, for the console's backlog display;
		// omitted when zero, and the JS checks for field presence.
		DistributingStartedAt time.Time `json:"distributing_started_at,omitzero"`
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
		stagedTar                 string   // tar_path submission: moved into the spool after validation
		tarName                   string   // original file name, for display
		submittedSHA256           string   // caller-supplied; may be empty
		preloadExe                string   // optional: repo-relative exe path for preload
		preloadPaths              []string // optional: repo-relative paths opened at startup
		buildID                   string   // optional: the CI pipeline identity of this run
		coarseField               string   // optional: "true"/"false"; empty means "infer"
		buildExpect               int      // optional: package count → auto-finalize when reached
		finalize                  bool     // coarse-publish finalize job (no tar payload)
		directS3                  bool     // pass --direct-s3 to cvmfs_server ingest (this job only)
		objectList                bool     // collect the S3 object list (needs directS3)
		stagingPrefix             string   // S3 prefix a producer already filled with prepared objects
		catalogHash               string   // suffixed subtree catalog hash to graft
		publishPath               string   // optional: "prepub" (default) or "ingest"
		preWarm                   *bool    // optional: nil = not requested
		identityPath              string   // optional: see job.IdentityPath
		identityHash              string   // optional: see job.IdentityHash
		replace                   bool     // optional: see job.Replace
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
			Coarse         *bool    `json:"coarse"`
			BuildExpect    int      `json:"build_expect"`
			PublishPath    string   `json:"publish_path"`
			PreWarm        *bool    `json:"prewarm"`
			// Staged publish. Present here as well as in the multipart branch:
			// this mode accepts publish_path, so a producer will reasonably send
			// them, and silently dropping them would answer 202 for an ordinary
			// tar publish instead.
			StagingPrefix string `json:"staging_prefix"`
			CatalogHash   string `json:"catalog_hash"`
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
		// Staged publish is multipart-only, and this must be refused BEFORE the
		// tar_path requirement below: this mode exists for a tar already on the
		// server's filesystem, while a staged submission has no payload at all.
		// Checked after it, the client is told "tar_path field is required",
		// which names the wrong thing entirely.
		if strings.TrimSpace(req.StagingPrefix) != "" || strings.TrimSpace(req.CatalogHash) != "" {
			http.Error(w, `{"error":"staging_prefix/catalog_hash are only supported in multipart submissions"}`,
				http.StatusBadRequest)
			return
		}
		// The path has to be refused here as well as the fields. Blocking only
		// the fields leaves `publish_path: staged` with a tar_path accepted, and
		// the staged backend has no code that reads a tar — it would commit an
		// empty transaction and report the job published.
		if req.PublishPath == StagedPublishPath {
			http.Error(w, fmt.Sprintf(
				`{"error":"the \"%s\" publish path is only supported in multipart submissions: it publishes prepared objects, not a tar"}`,
				StagedPublishPath), http.StatusBadRequest)
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
		if err := validateWebhookURL(req.WebhookURL); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
			return
		}

		// Resolve and validate the path is within stagingRoot.
		resolvedPath, err := resolveLocalTarPath(s.stagingRoot, req.TarPath)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"invalid tar_path: %s"}`, jsonEscape(err.Error())), http.StatusBadRequest)
			return
		}

		// The file stays where it is until every check below has passed: a
		// rejected submission must leave the producer's tar in place.
		stagedTar = resolvedPath
		spoolTarPath = filepath.Join(jobDir, "payload.tar")
		tarName = sanitizeTarName(req.TarPath)

		repo = req.Repo
		subPath = req.Path
		webhookURL = req.WebhookURL
		submittedSHA256 = req.TarSHA256
		tagName = req.TagName
		tagDescription = req.TagDescription
		preloadExe = req.PreloadExe
		preloadPaths = req.PreloadPaths
		buildID = req.BuildID
		if req.Coarse != nil {
			coarseField = strconv.FormatBool(*req.Coarse)
		}
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
		if msg, code := s.uploadRefusal(r.ContentLength); msg != "" {
			http.Error(w, msg, code)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, s.maxTarSize+maxFormFieldSize*maxMultipartParts)

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
			tarName = sanitizeTarName(part.FileName())

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
			n, copyErr := io.Copy(io.MultiWriter(spoolFile, hasher), io.LimitReader(part, s.maxTarSize+1))
			closeErr := spoolFile.Close()
			// Reject an oversized payload BEFORE part.Close(), which drains the
			// remainder of the part — otherwise the server reads the entire
			// body it has just decided to refuse.
			if n > s.maxTarSize {
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
		buildID = field("build_id")       // optional: the CI pipeline identity
		coarseField = field("coarse")     // optional: "true"/"false"; see below
		finalize = field("finalize") == "true"
		// Parsed, not compared against "true": this knob exists to A/B the two
		// transports, and a typo that silently means false yields a full
		// gateway-path run recorded as a direct-S3 run. Fail loudly instead.
		if raw := field("direct_s3"); raw != "" {
			v, convErr := strconv.ParseBool(strings.TrimSpace(raw))
			if convErr != nil {
				os.RemoveAll(jobDir)
				http.Error(w, `{"error":"direct_s3 must be a boolean"}`, http.StatusBadRequest)
				return
			}
			directS3 = v
		}
		// Same reasoning as direct_s3: parsed, not compared against "true", so
		// a typo fails loudly instead of yielding a run with no list that is
		// recorded as a run with one.
		if raw := field("object_list"); raw != "" {
			v, convErr := strconv.ParseBool(strings.TrimSpace(raw))
			if convErr != nil {
				os.RemoveAll(jobDir)
				http.Error(w, `{"error":"object_list must be a boolean"}`, http.StatusBadRequest)
				return
			}
			objectList = v
		}
		// staging_prefix / catalog_hash: the producer prepared the objects and
		// tells prepub where they are and which catalog to graft. Taken as
		// opaque strings here; both are validated below, where the publish path
		// is known.
		stagingPrefix = strings.TrimSpace(field("staging_prefix"))
		catalogHash = strings.TrimSpace(field("catalog_hash"))
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
		identityPath = strings.TrimSpace(field("identity_path")) // optional
		identityHash = strings.TrimSpace(field("identity_hash")) // optional
		// prewarm: absent means not requested; only true asks (and only a node
		// with pre-warming enabled honours it).
		if raw := field("prewarm"); raw != "" {
			v, convErr := strconv.ParseBool(strings.TrimSpace(raw))
			if convErr != nil {
				os.RemoveAll(jobDir)
				http.Error(w, `{"error":"prewarm must be a boolean"}`, http.StatusBadRequest)
				return
			}
			preWarm = &v
		}
		if raw := field("replace"); raw != "" {
			v, convErr := strconv.ParseBool(strings.TrimSpace(raw))
			if convErr != nil {
				os.RemoveAll(jobDir)
				http.Error(w, `{"error":"replace must be a boolean"}`, http.StatusBadRequest)
				return
			}
			replace = v
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
		if err := validateWebhookURL(webhookURL); err != nil {
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
		case stagingPrefix != "" || catalogHash != "" || publishPath == StagedPublishPath:
			// A staged job carries no payload either: its objects are already in
			// the store, which is the whole point. Accepting a tar as well would
			// publish the same subtree twice by two different routes -- an ingest
			// and a graft -- with no rule saying which wins, so refuse rather
			// than silently dropping it as finalize does.
			//
			// Either field alone lands here too, so that the pairing check below
			// reports what is actually wrong rather than "tar field is required".
			//
			// The PATH is in this condition as well as the fields, so that
			// `publish_path=staged` with neither field and no tar is answered by
			// the check that names the missing prefix. Without it the generic
			// "tar field is required" fires first and tells the client to add
			// the one thing this path can never use.
			if sawTar {
				os.RemoveAll(jobDir)
				http.Error(w, `{"error":"a staged submission (staging_prefix) must not carry a tar payload"}`,
					http.StatusBadRequest)
				return
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
		if err := validateIdentityPath(subPath, identityPath); err != nil {
			os.RemoveAll(jobDir)
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
			return
		}
	}
	if replace {
		if err := s.validateReplace(finalize, subPath, identityPath, identityHash); err != nil {
			os.RemoveAll(jobDir)
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
	if replace && !s.orch.canReplaceOn(publishPath) {
		os.RemoveAll(jobDir)
		http.Error(w, fmt.Sprintf(`{"error":"publish path %q cannot replace published content; use ingest"}`,
			jsonEscape(publishPath)), http.StatusBadRequest)
		return
	}
	// direct_s3 is a property of the ingest path: it becomes --direct-s3 on
	// cvmfs_server. On any other path nothing reads it, and accepting it would
	// hand back a 202 for a request whose central instruction was dropped —
	// the same reasoning applied to prewarm and build_id below, and the same
	// silent-success failure this flag was added to remove.
	if directS3 && publishPath != "ingest" {
		os.RemoveAll(jobDir)
		http.Error(w, fmt.Sprintf(
			`{"error":"direct_s3 is only supported on the \"ingest\" publish path (got %q)"}`,
			jsonEscape(publishPath)), http.StatusBadRequest)
		return
	}
	// object_list rides on the direct-S3 uploader: only that one writes a list,
	// and cvmfs_server ABORTS the transaction when given --object-list without
	// --direct-s3. Accepting it here would hand back a 202 for a request that
	// either drops its instruction or fails at commit. Refuse both mismatches
	// separately so the message names the one that is actually wrong.
	if objectList && publishPath != "ingest" {
		os.RemoveAll(jobDir)
		http.Error(w, fmt.Sprintf(
			`{"error":"object_list is only supported on the \"ingest\" publish path (got \"%s\")"}`,
			jsonEscape(publishPath)), http.StatusBadRequest)
		return
	}
	if objectList && !directS3 {
		os.RemoveAll(jobDir)
		http.Error(w, `{"error":"object_list requires direct_s3"}`, http.StatusBadRequest)
		return
	}
	// staging_prefix and catalog_hash are one instruction in two fields: the
	// objects to promote and the catalog that references them. Either alone
	// cannot be acted on, and accepting one would hand back a 202 for a request
	// that silently does nothing — the failure mode every knob on this handler
	// exists to avoid.
	if (stagingPrefix == "") != (catalogHash == "") {
		os.RemoveAll(jobDir)
		http.Error(w, `{"error":"staging_prefix and catalog_hash must be given together"}`,
			http.StatusBadRequest)
		return
	}
	// The "staged" path, not "ingest": the two are different mechanisms. The
	// ingest backend hands a tar to `cvmfs_server ingest`; it has no use for a
	// staging prefix and would reject the job for want of a payload. Staged
	// jobs take the lease and graft, which is what StagedBackend does.
	if stagingPrefix != "" && publishPath != StagedPublishPath {
		os.RemoveAll(jobDir)
		http.Error(w, fmt.Sprintf(
			`{"error":"staging_prefix is only supported on the \"%s\" publish path (got \"%s\")"}`,
			StagedPublishPath, jsonEscape(publishPath)), http.StatusBadRequest)
		return
	}
	// ...and the converse, which is the dangerous direction. The staged path
	// publishes ONLY what staging_prefix names; it has no code that reads a tar.
	// Without this, `publish_path=staged` with a payload and no prefix is
	// accepted, the tar is silently discarded, an empty transaction is committed
	// to the gateway, and the job reports "published" — a success answer for
	// content that was never published. Every other check in this handler exists
	// to prevent exactly that, and this one was missing until a review probed it.
	if publishPath == StagedPublishPath && stagingPrefix == "" {
		os.RemoveAll(jobDir)
		http.Error(w, fmt.Sprintf(
			`{"error":"the \"%s\" publish path requires staging_prefix and catalog_hash: it publishes prepared objects and ignores any tar payload"}`,
			StagedPublishPath), http.StatusBadRequest)
		return
	}
	// The receiver refuses a graft whose hash lacks the catalog suffix
	// ("DirectGraft requires a catalog hash"). Catching it here names the field;
	// catching it there costs a lease, a promotion and an opaque commit failure.
	if stagingPrefix != "" && !job.ValidStagingPrefix(stagingPrefix) {
		os.RemoveAll(jobDir)
		http.Error(w, `{"error":"staging_prefix must be slash-separated segments of [A-Za-z0-9._-], at most 128 bytes, and must not end in \"data\""}`,
			http.StatusBadRequest)
		return
	}
	// direct_s3 and object_list are NOT re-checked against staging_prefix here:
	// they require publish_path "ingest" (checked above) and a staged job
	// requires "staged", so the combination cannot be expressed. Whichever
	// check runs first refuses it and names the path, which IS the conflict.
	// A second "cannot be combined" check would be unreachable, and
	// unreachable validation rots -- it stops being exercised while still
	// looking like a guarantee.
	if catalogHash != "" && !job.ValidCatalogHash(catalogHash) {
		os.RemoveAll(jobDir)
		http.Error(w, `{"error":"catalog_hash must be a CVMFS catalog hash: hex with the catalog suffix, e.g. 0123…C"}`,
			http.StatusBadRequest)
		return
	}
	if publishPath != "" && publishPath != DefaultPublishPath {
		// Off the prepub pipeline, pre-warming needs the list of objects the
		// publisher stored: only ingest with direct_s3 and object_list has it,
		// and it warms right after the commit. Accepting the request and
		// ignoring it would be worse than saying so.
		if preWarm != nil && *preWarm && !(publishPath == "ingest" && directS3 && objectList) {
			os.RemoveAll(jobDir)
			http.Error(w, fmt.Sprintf(`{"error":"publish path %q cannot pre-warm Stratum 1 caches; use ingest with direct_s3 and object_list, or the %q path"}`,
				jsonEscape(publishPath), DefaultPublishPath), http.StatusBadRequest)
			return
		}
		// Coarse publish, however, is genuinely impossible here: an alternative
		// path commits each package as it arrives, so there is nothing to
		// accumulate and a finalize would never fire.
		//
		// It is the COARSE REQUEST that is refused, not the build id. The build
		// id is the CI pipeline identity -- carried by every job of the run,
		// used for the views and the signed common manifest, and the key an
		// operator uses to find the run's measurement records. Refusing it here
		// forced the producer to send none at all on these paths.
		if v, perr := strconv.ParseBool(strings.TrimSpace(coarseField)); coarseField != "" && perr == nil && v {
			os.RemoveAll(jobDir)
			http.Error(w, fmt.Sprintf(`{"error":"publish path %q commits each package on arrival and cannot take part in a coarse build; drop coarse=true or use the %q path"}`,
				jsonEscape(publishPath), DefaultPublishPath), http.StatusBadRequest)
			return
		}
	}

	// Resolve the coarse decision ONCE, here, so every consumer asks the same
	// question instead of re-deriving it from build_id.
	//
	// Absent field keeps the historical behaviour exactly: a build id on the
	// default path meant "accumulate". An explicit value wins, which is what
	// lets a producer carry the pipeline identity on a per-package path
	// without joining a coarse build.
	// publishPath is normalised: "" and "prepub" are the same path everywhere
	// else, and treating them differently here would silently stop an explicit
	// publish_path=prepub from accumulating.
	onDefaultPath := publishPath == "" || publishPath == DefaultPublishPath
	coarse := buildID != "" && onDefaultPath && !finalize
	if coarseField != "" {
		// Parsed, not compared against "true", for the same reason as
		// direct_s3 above: a typo must not silently mean the opposite. A
		// producer that says "no" and gets a coarse build is the failure this
		// handler exists to prevent.
		v, perr := strconv.ParseBool(strings.TrimSpace(coarseField))
		if perr != nil {
			os.RemoveAll(jobDir)
			http.Error(w, `{"error":"coarse must be true or false"}`, http.StatusBadRequest)
			return
		}
		coarse = v
	}
	// A coarse job accumulates into a build keyed by its id; without one the
	// buildset write fails only AFTER the payload has been pipelined and
	// uploaded (or, with build_expect, 500s here). Refuse it at submit, the
	// same way finalize-without-build_id is refused above.
	if coarse && buildID == "" {
		os.RemoveAll(jobDir)
		http.Error(w, `{"error":"coarse requires build_id"}`, http.StatusBadRequest)
		return
	}
	// A finalize job IS the coarse commit; it does not accumulate.
	if finalize {
		coarse = false
	}
	// Local mode publishes every package on arrival. A job marked coarse would
	// still be published alone, but lose its retries and leave its build
	// waiting for a finalize that never comes.
	if coarse && !s.orch.CoarseSupported() {
		coarse = false
	}

	// tar_path: the digest is the last check, as it reads the whole file.
	if stagedTar != "" {
		if err := verifySHA256(stagedTar, submittedSHA256); err != nil {
			span.RecordError(err)
			http.Error(w, fmt.Sprintf(`{"error":"tar_sha256 mismatch: %s"}`, jsonEscape(err.Error())), http.StatusBadRequest)
			return
		}
	}

	// Record the build's expected package count before the job can accumulate,
	// so that the last package to finish sees a complete declaration and can
	// trigger the finalize itself.  Every package of the build carries the same
	// value; the write is atomic and idempotent.  A finalize job never declares
	// (it IS the finalize).
	if coarse && buildExpect > 0 {
		if err := buildset.SetExpect(s.spoolRoot, buildID, buildExpect); err != nil {
			span.RecordError(err)
			os.RemoveAll(jobDir)
			http.Error(w, `{"error":"internal error recording build expectation"}`, http.StatusInternalServerError)
			return
		}
	}

	// tar_path: move the file only now that the request has been accepted,
	// so a refusal never consumes the producer's tar.
	if stagedTar != "" {
		if err := os.MkdirAll(jobDir, 0700); err != nil {
			span.RecordError(err)
			http.Error(w, `{"error":"internal error creating job directory"}`, http.StatusInternalServerError)
			return
		}
		if err := moveOrLink(stagedTar, spoolTarPath); err != nil {
			span.RecordError(err)
			os.RemoveAll(jobDir)
			http.Error(w, `{"error":"internal error moving tar to spool"}`, http.StatusInternalServerError)
			return
		}
	}

	j := job.NewJob(jobID, repo, "", spoolTarPath)
	j.Path = subPath
	j.BuildID = buildID
	j.Coarse = &coarse
	j.Finalize = finalize
	j.DirectS3 = directS3
	j.ObjectList = objectList
	j.StagingPrefix = stagingPrefix
	j.CatalogHash = catalogHash
	j.WebhookURL = webhookURL
	j.TarSHA256 = submittedSHA256
	j.TagName = tagName
	j.TagDescription = tagDescription
	j.PreloadExe = preloadExe
	j.PreloadPaths = preloadPaths
	j.PublishPath = publishPath
	j.PreWarm = preWarm
	if identityPath != "" {
		j.IdentityPath = path.Clean(identityPath)
		j.IdentityHash = identityHash
	}
	j.Replace = replace

	// Record the original filename and size for the console tooltip.
	// Use Stat on the spool copy since the original may have been moved.
	// Finalize jobs carry no payload, so there is nothing to stat.
	if spoolTarPath != "" {
		j.TarName = tarName
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
		if stagedTar != "" {
			_ = moveOrLink(spoolTarPath, stagedTar) // hand the producer's file back
		}
		os.RemoveAll(jobDir)
		http.Error(w, `{"error":"internal error writing manifest"}`, http.StatusInternalServerError)
		return
	}

	s.launch(j)

	s.obs.Metrics.JobsSubmitted.Inc()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, `{"job_id":%q}`, jobID)
}

// launch runs an accepted job in the background: it waits for a concurrency
// slot, runs, and retries while the job asks for it. New submissions and jobs
// recovered at startup both go through here, so both obey the same limit.
func (s *Server) launch(j *job.Job) {
	s.launchMu.Lock()
	defer s.launchMu.Unlock()
	if s.draining {
		s.obs.Logger.Info("shutting down — job left in incoming for the next start", "job_id", j.ID)
		return
	}
	s.jobWg.Add(1)

	// Runs in the background — a submitter gets its job_id immediately.
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
	s.orch.registerJob(j.ID, abortCancel)

	// Read-ahead Phase 0: start the tar scan NOW, before waiting for the
	// concurrency slot.  The tar is already on disk in the spool; scanning it
	// costs only I/O and memory, not a pipeline slot.  For queued jobs the
	// scan overlaps with earlier jobs' compress/upload work so that when the
	// slot opens the compress workers start immediately with sorted entries
	// already in memory rather than waiting for another full tar read.
	// Not for a job waiting to retry: its scan would sit on the budget, and
	// on disk, until the job is due.
	if j.NextAttemptAt == nil {
		s.orch.StartPrefetch(abortCtx, j)
	}

	go func() {
		defer s.jobWg.Done()
		defer s.orch.unregisterJob(j.ID)
		defer abortCancel()

		// One attempt: wait for a slot, run, release. A retryable failure
		// leaves the job in incoming with a due time; wait for it and go again.
		attempt := func() error {
			// ── Wait for a concurrency slot (if the limit is configured) ──────
			// The semaphore limits concurrent pipeline (compress/upload) workers.
			// The slot is released EARLY — before the per-repo commit mutex — by
			// the onStagingComplete hook passed to Run().  This lets the next
			// queued job start its own compress pipeline while this job does its
			// gateway commit. The defer below is a safety net: if Run() returns
			// without ever calling the hook (e.g. early error during staging,
			// local mode) the slot is still released exactly once via sync.Once.
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
							"job_id", j.ID)
					}
				})
			}

			if s.dynaSem != nil {
				s.obs.Logger.Info("job queued — waiting for concurrency slot",
					"job_id", j.ID, "repo", j.Repo)
				// A manual abort or a server shutdown ends the wait at once.
				acqCtx, acqCancel := context.WithCancel(abortCtx)
				stop := context.AfterFunc(s.stop, acqCancel)
				gw, err := s.dynaSem.Acquire(acqCtx, j.TarSize)
				stop()
				acqCancel()
				if err != nil {
					if abortCtx.Err() == nil {
						// Shutdown: the job never ran; it stays in incoming
						// and recovers on the next start.
						s.obs.Logger.Info("shutting down — queued job left in incoming for the next start",
							"job_id", j.ID)
						return nil
					}
					// Operator abort while queued: abort without running.
					s.obs.Logger.Info("job aborted while waiting for slot",
						"job_id", j.ID, "error", err)
					return s.orch.abortJob(context.Background(), j,
						fmt.Errorf("aborted while waiting for concurrency slot: %w", err))
				}
				grantedWeight = gw
				s.obs.Logger.Info("job acquired concurrency slot",
					"job_id", j.ID, "weight", gw, "tar_bytes", j.TarSize)
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
			s.orch.registerJob(j.ID, runCancel)

			err := s.orch.Run(runCtx, j, releaseSem)
			switch {
			case err == nil, errors.Is(err, ErrRetryScheduled):
				// published, or scheduleRetry has said what happens next
			case s.orch.JobTimeout > 0 && runCtx.Err() != nil:
				s.obs.Logger.Error("background job timed out", "job_id", j.ID, "timeout", s.orch.JobTimeout, "error", err)
			default:
				s.obs.Logger.Error("background job failed", "job_id", j.ID, "error", err)
			}
			return err
		}
		for {
			// Immediate for a new job; a job waiting to retry (including one
			// recovered at startup) waits for its due time first.
			waitCtx, waitCancel := context.WithCancel(abortCtx)
			stop := context.AfterFunc(s.stop, waitCancel)
			due := WaitForAttempt(waitCtx, j)
			stop()
			waitCancel()
			if !due {
				if abortCtx.Err() != nil {
					_ = s.orch.abortJob(context.Background(), j,
						fmt.Errorf("aborted while waiting to retry: %w", abortCtx.Err()))
				}
				return // shutdown: the job stays in incoming for the next start
			}
			err := attempt()
			if !errors.Is(err, ErrRetryScheduled) {
				return
			}
			// The attempt registered its own cancel; an abort while waiting
			// must reach this wait instead. One that landed in between
			// cancelled the finished attempt only, and is honoured here.
			s.orch.registerJob(j.ID, abortCancel)
			if s.orch.Aborted(j.ID) {
				abortCancel()
			}
		}
	}()
}

// RecoverJob resumes a job found in flight at startup (see
// Orchestrator.PrepareRecovery) through the same concurrency limit as a new
// submission, so a restart with many interrupted jobs does not run them all
// at once.
func (s *Server) RecoverJob(ctx context.Context, j *job.Job, afterCleanShutdown bool) error {
	resume, err := s.orch.PrepareRecovery(ctx, j, afterCleanShutdown)
	if err != nil || !resume {
		return err
	}
	if j.TarPath != "" {
		// The reset moved the job directory; prefetch opens this path.
		j.TarPath = filepath.Join(s.sp.JobDir(j), "payload.tar")
	}
	s.launch(j)
	return nil
}

// sanitizeTarName reduces a client-supplied file name to a display-safe base
// name: no directories (either separator), no control or non-printable
// characters, at most 255 bytes. Returns "" when nothing usable remains.
func sanitizeTarName(name string) string {
	name = path.Base(strings.ReplaceAll(name, `\`, "/"))
	name = strings.TrimSpace(strings.Map(func(r rune) rune {
		if !unicode.IsPrint(r) {
			return -1
		}
		return r
	}, name))
	for len(name) > 255 {
		_, size := utf8.DecodeLastRuneInString(name)
		name = name[:len(name)-size]
	}
	if name == "." || name == "/" || name == ".." {
		return ""
	}
	return name
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
		// Retries: how many attempts failed, the latest cause (also the cause
		// of a final failure), and when a waiting job runs again.
		Attempts      int        `json:"attempts,omitempty"`
		LastError     string     `json:"last_error,omitempty"`
		NextAttemptAt *time.Time `json:"next_attempt_at,omitempty"`
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
		Attempts:         j.Attempts,
		LastError:        j.LastError,
		NextAttemptAt:    j.NextAttemptAt,
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

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, `{"error":"streaming not supported by this server"}`, http.StatusInternalServerError)
		return
	}

	// Subscribe BEFORE reading the job, so a transition between the two is
	// delivered rather than lost; at worst a state is sent twice.
	ch, cancel := s.notifyBus.Subscribe(id)
	defer cancel()

	j, err := s.sp.FindJob(id)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, `{"error":"job not found"}`, http.StatusNotFound)
			return
		}
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // tell nginx not to buffer SSE

	// send writes one event and reports whether the stream should end.
	send := func(e notify.Event) bool {
		data, err := json.Marshal(e)
		if err != nil {
			s.obs.Logger.Warn("SSE: marshal error", "job_id", id, "error", err)
			return false
		}
		fmt.Fprintf(w, "event: state_change\ndata: %s\n\n", data)
		flusher.Flush()
		return job.IsTerminal(e.State)
	}

	// The current state first: a job that has already finished emits no
	// further events, and the subscriber would otherwise wait forever.
	if send(notify.Event{JobID: j.ID, State: j.State, Error: j.Error, Time: j.UpdatedAt}) {
		return
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case e, ok := <-ch:
			if !ok || send(e) {
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
		"job":         redactedJob(j),
		"transitions": transitions,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// redactedJob returns a copy of j safe to return to API callers: the gateway
// lease token is dropped and the webhook URL is cut to scheme and host, since
// its path or query commonly carries the receiver's secret.
func redactedJob(j *job.Job) *job.Job {
	c := *j
	c.LeaseToken = ""
	if c.WebhookURL != "" {
		c.WebhookURL = "[redacted]"
		if u, err := url.Parse(j.WebhookURL); err == nil && u.Host != "" {
			c.WebhookURL = u.Scheme + "://" + u.Host + "/[redacted]"
		}
	}
	return &c
}

// validateWebhookURL accepts only an absolute http(s) URL with a host.
func validateWebhookURL(raw string) error {
	if raw == "" {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("webhook_url must be an absolute http:// or https:// URL")
	}
	return nil
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
		// MaxTarSize lets a producer refuse an oversized package itself
		// instead of uploading it to be cut off.
		MaxTarSize int64 `json:"max_tar_size"`
		// ReplaceAllowed lets a producer refuse a package another build
		// published before uploading it, when this node cannot replace it.
		ReplaceAllowed bool `json:"replace_allowed"`
		// ReplayCache surfaces the fail-closed counter: a non-zero
		// rejected_full means signed requests are being refused for capacity
		// reasons, which looks like an auth problem from the client side and
		// is invisible otherwise.
		ReplayCache struct {
			Entries      int    `json:"entries"`
			RejectedFull uint64 `json:"rejected_full"`
		} `json:"replay_cache"`
	}{Status: "healthy", AuthMode: string(s.authMode), MaxTarSize: s.maxTarSize}
	body.ReplayCache.Entries = nonces
	body.ReplayCache.RejectedFull = rejectedFull
	if s.orch != nil {
		body.PublishPaths = s.orch.PublishPathNames()
		body.FinalizeReady = s.orch.IngestConfigPrefix != "" && s.orch.CoarseSupported()
		body.ReplaceAllowed = s.orch.ReplaceAllowed()
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(body)
}
