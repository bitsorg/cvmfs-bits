// SPDX-FileCopyrightText: 2026 CERN (European Organization for Nuclear Research)
// SPDX-License-Identifier: Apache-2.0

// Package api orchestrates the HTTP server and job lifecycle management for the CVMFS pre-publisher.
// It coordinates the pipeline (unpack, compress, deduplicate), distribution to Stratum 1 replicas,
// gateway lease acquisition, and publish operations.
package api

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"cvmfs.io/prepub/internal/broker"
	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/internal/distribute"
	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/lease"
	"cvmfs.io/prepub/internal/notify"
	"cvmfs.io/prepub/internal/pipeline"
	"cvmfs.io/prepub/internal/provenance"
	"cvmfs.io/prepub/internal/spool"
	"cvmfs.io/prepub/pkg/cvmfscatalog"
	"cvmfs.io/prepub/pkg/cvmfshash"
	"cvmfs.io/prepub/pkg/observe"
)

// MaxRecoveries is the maximum number of times a single job may be
// automatically reset and re-processed.  Jobs that exceed this limit are
// moved to the failed state so a human can inspect them.
const MaxRecoveries = 3

// Orchestrator manages the end-to-end lifecycle of a publish job.
// It coordinates pipeline stages, distribution to Stratum 1 endpoints,
// transaction/lease acquisition, and commit operations via a pluggable
// lease.Backend.  Both gateway and single-host (local) deployments are
// supported through the same code path.
type Orchestrator struct {
	// Spool manages persistent job state and directory transitions.
	Spool *spool.Spool
	// CAS is the content-addressable storage backend (gateway mode only;
	// unused when Lease.NeedsPipeline() returns false).
	CAS cas.Backend
	// Lease is the publish transaction backend.  Use lease.NewClient for
	// gateway mode or lease.NewLocalBackend for single-host mode.
	Lease lease.Backend
	// JobTimeout is the maximum wall-clock duration a single job may run
	// before its context is cancelled and the job is failed.  A value of
	// zero disables the per-job timeout (backward-compatible default).
	// Recommended starting value: 30m for gateway mode, 60m for large repos.
	JobTimeout time.Duration
	// CVMFSMount is the filesystem root where CVMFS repositories are mounted
	// (e.g. "/cvmfs").  Only used in local publish mode; ignored by the
	// gateway backend.
	CVMFSMount string
	// Stratum0URL is the HTTP base URL of the Stratum 0 CAS, used to fetch the
	// current .cvmfspublished manifest and download the root catalog for the
	// direct SQLite catalog merge (gateway mode only).
	// Example: "http://stratum0.example.org"
	// If empty, the catalog merge step is skipped and the commit will use an
	// empty old_root_hash (only safe for the initial publish of a new repository).
	Stratum0URL string
	// DirectGraft enables the fast-path commit on the receiver side.
	//
	// When true, the commit POST body carries "direct_graft":true, instructing
	// cvmfs_receiver to skip DiffRec and graft the pre-built subtree catalog
	// directly into the parent catalog.  This is correct only when the lease
	// path is a brand-new directory with no pre-existing content.
	//
	// Set to false (default) to use the standard CommitProcessor/DiffRec path,
	// which handles arbitrary add/remove/modify operations safely.  Both paths
	// produce identical repository state for "publish new subtree"; DirectGraft
	// is purely a performance optimisation that can be toggled at runtime via
	// --gateway-direct-graft for A/B comparison and integrity verification.
	DirectGraft bool
	// Distribute contains configuration for Stratum 1 push distribution.
	// nil disables distribution (typical for local mode).
	Distribute *distribute.Config

	// DistManager is the queue-driven distribution manager.  When non-nil it
	// is used instead of the legacy fire-and-forget goroutine; each publish job
	// enqueues a WorkItem and DistManager handles retries, backoff, and
	// per-endpoint parallelism.  Typically created alongside Distribute.
	DistManager *distribute.Manager

	// BrokerConfig is the MQTT broker configuration used for publishing commit
	// notifications (PublishedMessage) after a successful catalog commit.  When
	// non-nil, a short-lived client is created per commit to publish the message
	// and then disconnected.  nil disables MQTT publish notifications.
	//
	// This is separate from Distribute.BrokerConfig: distribution uses the
	// broker for the announce/ready exchange BEFORE the commit, while this
	// config is for the post-commit "published" notification.  In typical
	// deployments both configs reference the same broker.
	BrokerConfig *broker.Config
	// Pipeline contains configuration for the compression/dedup pipeline.
	// Only used when Lease.NeedsPipeline() returns true (gateway mode).
	Pipeline pipeline.Config

	// Notify is the event bus for job state changes. nil disables event publishing.
	Notify *notify.Bus
	// Provenance records build identity and Rekor receipts. nil disables provenance recording.
	Provenance *provenance.Provider
	// Obs provides logging, tracing, and metrics.
	Obs *observe.Provider

	// per-job cancel functions, registered by Run and removed when the job
	// reaches a terminal state.  CancelJob uses this to abort a running job.
	runningMu sync.Mutex
	running   map[string]context.CancelFunc

	// webhookWg tracks in-flight webhook delivery goroutines so Server.Shutdown
	// can wait for them to finish before the process exits.
	webhookWg sync.WaitGroup

	// GatewayQueue replaces the exponential-backoff retry loop in Lease.Acquire
	// with a short-interval (1 s) poll that also wakes up immediately when an
	// in-process job releases its lease via NotifyRelease.  Only used when the
	// backend is a *lease.Client (gateway mode); nil in local mode.
	GatewayQueue *GatewayQueue

	// commitMu serialises the manifest-fetch + commit phase per repository.
	// Only one job per repo may be in this critical section at a time.
	//
	// Background: two concurrent jobs publishing to different sub-paths of the
	// same repo both call FetchManifestRootHash to obtain old_root_hash.  If
	// they run concurrently, both see the same hash.  The first commit updates
	// the manifest; the second commit then presents a stale old_root_hash to
	// the gateway, which causes cvmfs_receiver to block indefinitely (observed
	// in the testbed, resulting in StateCommitting hangs).
	//
	// The subtree catalog build (Phase 2.6) runs before the mutex so multiple
	// jobs build their catalogs in parallel while one holds the lock.
	commitMu sync.Map // map[string]*sync.Mutex — keyed by repo name

	// prefetchResults maps job ID to a buffered channel that receives the
	// Phase-0 (collect+sort) result started by StartPrefetch.  The channel
	// has capacity 1 so the goroutine never blocks after writing.
	// Run() drains the channel with a context-aware receive; if the prefetch
	// goroutine hasn't finished yet, Run() waits for it (still faster than
	// a full re-read because at worst both overlap briefly).  On prefetch
	// failure the channel receives nil and Run() falls back to pipeline.Run().
	prefetchResults sync.Map // map[string]chan *pipeline.PrefetchResult

	// knownPaths caches "repo!pathComponent" keys for CVMFS path components
	// confirmed to exist in the repository.  ensureParentDirs uses this to
	// skip redundant mkdir-p commits on every publish after the first one to
	// a given path hierarchy.  The map is never deleted from (paths, once
	// created, persist for the lifetime of the repository).
	knownPaths sync.Map // map["repo!path"] → struct{}

	// mkdirMu provides per-"repo!graftPath" serialisation for the mkdir-p
	// gateway commit inside ensureParentDirs.  A dedicated map (rather than
	// repoMu) keeps mkdir-p contention isolated from the regular content-commit
	// critical section and allows a double-checked-lock pattern:
	//   1. Fast path (no lock): check knownPaths — miss → proceed.
	//   2. Lock mkdirMu for this graftPath.
	//   3. Re-check knownPaths inside the lock — if now hit, return nil.
	//   4. Only ONE goroutine per graftPath ever reaches the gateway commit.
	mkdirMu sync.Map // map["repo!graftPath"] → *sync.Mutex
}

// repoMutex returns the per-repo mutex for repo, creating it on first call.
// The same *sync.Mutex is returned for every call with the same repo string.
func (o *Orchestrator) repoMutex(repo string) *sync.Mutex {
	v, _ := o.commitMu.LoadOrStore(repo, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// mkdirMutex returns the per-"repo!graftPath" mutex used by ensureParentDirs.
// Using a separate map from commitMu keeps mkdir-p serialisation isolated from
// the regular content-commit critical section.
func (o *Orchestrator) mkdirMutex(key string) *sync.Mutex {
	v, _ := o.mkdirMu.LoadOrStore(key, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// StartPrefetch starts a background goroutine that performs Phase 0 of the
// pipeline (collect+validate+sort all tar entries into memory) BEFORE the
// concurrency slot is acquired.  This means the blocking tar scan overlaps
// with earlier jobs' compress/upload work instead of serialising with it.
//
// Call immediately after a job is accepted (before jobSem.Acquire).  Run()
// will pick up the result via the prefetchResults map and skip Phase 0.
//
// If the pipeline is not needed (local mode) or if the tar is unavailable,
// the call is a no-op and Run() will fall back to pipeline.Run() as usual.
//
// Race-safety: the tar file lives at j.TarPath (inside incoming/<jobID>/).
// When the concurrency slot is eventually acquired, the orchestrator renames
// incoming/<jobID>/ → staging/<jobID>/, making the original path invalid.
// To avoid a TOCTOU race where the goroutine tries to open the file after
// the rename, we open the file descriptor HERE (synchronously, on the caller's
// goroutine) before launching the background goroutine.  An open fd holds a
// kernel inode reference that survives directory renames, so the goroutine
// can read all content through the fd even after the rename completes.
func (o *Orchestrator) StartPrefetch(ctx context.Context, j *job.Job) {
	if !o.Lease.NeedsPipeline() {
		return // local mode: no pipeline, no prefetch
	}
	if j.TarPath == "" {
		return
	}

	// Open the file synchronously to obtain a stable inode reference before
	// any directory rename can occur.
	f, err := os.Open(j.TarPath)
	if err != nil {
		o.Obs.Logger.Warn("prefetch: cannot open tar — will fall back to pipeline.Run()",
			"job_id", j.ID, "path", j.TarPath, "error", err)
		return // no channel stored → takePrefetch returns nil → fallback
	}

	ch := make(chan *pipeline.PrefetchResult, 1)
	o.prefetchResults.Store(j.ID, ch)

	go func() {
		defer f.Close()
		result, err := pipeline.PrefetchFromReader(ctx, f, o.Obs)
		if err != nil {
			o.Obs.Logger.Warn("prefetch failed — Run() will fall back to pipeline.Run()",
				"job_id", j.ID, "error", err)
			ch <- nil // nil signals failure
			return
		}
		ch <- result
		o.Obs.Logger.Info("prefetch ready",
			"job_id", j.ID, "entries", len(result.SortedEntries))
	}()
}

// takePrefetch retrieves and removes the prefetch result for jobID.
// It blocks until the prefetch goroutine finishes or ctx is cancelled.
// Returns nil when no prefetch was started, the prefetch failed, or ctx fired.
func (o *Orchestrator) takePrefetch(ctx context.Context, jobID string) *pipeline.PrefetchResult {
	v, ok := o.prefetchResults.LoadAndDelete(jobID)
	if !ok {
		return nil
	}
	ch := v.(chan *pipeline.PrefetchResult)
	select {
	case r := <-ch:
		return r // may be nil if prefetch failed
	case <-ctx.Done():
		return nil
	}
}

// ensureParentDirs guarantees that the intermediate directory components of
// j.Path exist in the CVMFS repository before the content subtree is grafted.
//
// # Why this is necessary
//
// cvmfs_receiver grafts a subtree catalog at the exact lease path.  For the
// FUSE client to traverse to that path, every ancestor directory must appear
// as a directory entry in an ancestor catalog.  The gateway does not create
// missing intermediate directories automatically, so a fresh publish at a
// deep path (e.g. "releases/ROOT/v6-36-04/el9-x86_64") leaves "releases/",
// "releases/ROOT/", and "releases/ROOT/v6-36-04/" invisible unless they were
// written by a prior commit.
//
// # Approach — cheap gateway-only mkdir-p
//
// A tiny directory-only subtree catalog is built for the first missing
// ancestor component (e.g. "releases") containing bare directory entries for
// every missing level down to j.Path.  This catalog is committed to the
// gateway exactly like a normal content publish:
//
//   1. BuildSubtree  — produces a minimal SQLite catalog (a few KB at most)
//   2. CAS.Put       — uploads the catalog to the local CAS
//   3. repoMu.Lock   — serialise with respect to other jobs for this repo
//   4. FetchManifestRootHash — lightweight manifest GET (~200 bytes)
//   5. GatewayQueue.Acquire / Lease.Acquire for graftPath
//   6. Lease.Commit  — SubmitPayload + commit POST (creates the dir entries)
//   7. Mark ancestors in knownPaths so subsequent publishes skip this step
//
// The root catalog SQLite file is never downloaded.
//
// # Call contract
//
// Must be called AFTER Phase 2.6 (BuildSubtree for content) but BEFORE
// Phase 2.7 (leaf lease acquisition), so that no overlapping path leases
// exist when the parent lease is acquired.
func (o *Orchestrator) ensureParentDirs(ctx context.Context, j *job.Job) error {
	if o.Stratum0URL == "" || j.Path == "" || !o.Lease.NeedsPipeline() {
		return nil
	}

	// Decompose j.Path into its ancestor path components (not including j.Path
	// itself, which the content commit will create).
	// "releases/ROOT/v6-36-04/el9-x86_64" →
	//   ancestors = ["releases", "releases/ROOT", "releases/ROOT/v6-36-04"]
	parts := strings.Split(strings.Trim(j.Path, "/"), "/")
	if len(parts) <= 1 {
		return nil // top-level lease — no intermediate dirs needed
	}
	ancestors := make([]string, 0, len(parts)-1)
	for i := 1; i < len(parts); i++ {
		ancestors = append(ancestors, strings.Join(parts[:i], "/"))
	}

	// Fast path: check in-memory cache.  All ancestors known → nothing to do.
	firstMissing := -1
	for i, anc := range ancestors {
		if _, ok := o.knownPaths.Load(j.Repo + "!" + anc); !ok {
			firstMissing = i
			break
		}
	}
	if firstMissing == -1 {
		return nil
	}

	graftPath := ancestors[firstMissing]

	// Serialize mkdir-p per graftPath with a double-checked lock.
	//
	// Problem without this: N concurrent jobs all see the empty knownPaths
	// cache above and all proceed to build a dir-only catalog and commit it,
	// serializing through repoMu one by one — turning a one-time O(1) gateway
	// commit into an O(N) sequential bottleneck that stalls all jobs in
	// StateLeased.
	//
	// Solution: lock a per-"repo!graftPath" mutex (mkdirMu) and re-check
	// knownPaths inside it.  After the first job commits and populates
	// knownPaths, every subsequent job that was waiting on mkdirMu sees the
	// cache hit and returns immediately without touching the gateway.
	mkdirKey := j.Repo + "!" + graftPath
	mkdirMuForPath := o.mkdirMutex(mkdirKey)
	mkdirMuForPath.Lock()
	defer mkdirMuForPath.Unlock()

	// Double-check: another goroutine committed the dirs while we waited.
	if _, ok := o.knownPaths.Load(j.Repo + "!" + graftPath); ok {
		return nil
	}

	logger := o.Obs.Logger.With("job_id", j.ID, "repo", j.Repo, "graft_path", graftPath)
	logger.Info("mkdir-p: creating missing parent directory chain", "full_path", j.Path)

	// Build directory entries for the subtree rooted at graftPath.
	// "." resolves to graftPath itself; deeper entries are relative to it.
	now := time.Now().Unix()
	dirEntries := []cvmfscatalog.Entry{
		{FullPath: ".", Mode: fs.ModeDir | 0o755, Mtime: now, LinkCount: 2},
	}
	for _, anc := range ancestors[firstMissing+1:] {
		rel := strings.TrimPrefix(anc, graftPath+"/")
		dirEntries = append(dirEntries, cvmfscatalog.Entry{
			FullPath: rel, Mode: fs.ModeDir | 0o755, Mtime: now, LinkCount: 2,
		})
	}
	// When using DirectGraft, do NOT add j.Path as a plain directory entry.
	// GraftNestedCatalog will insert the nested catalog mountpoint at that
	// location itself; pre-creating it as a regular directory causes the
	// "invalid attempt to graft nested catalog into existing directory" PANIC.
	// For the standard DiffRec path we still add a placeholder so that the
	// content commit can replace it.
	if !o.DirectGraft {
		leafRel := strings.TrimPrefix(j.Path, graftPath+"/")
		dirEntries = append(dirEntries, cvmfscatalog.Entry{
			FullPath: leafRel, Mode: fs.ModeDir | 0o755, Mtime: now, LinkCount: 2,
		})
	}

	// Build the tiny directory-only subtree catalog.
	tmpDir, err := os.MkdirTemp("", "cvmfs-mkdir-p-*")
	if err != nil {
		return fmt.Errorf("mkdir-p: create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	mkdirResult, err := cvmfscatalog.BuildSubtree(ctx, cvmfscatalog.SubtreeConfig{
		LeasePath: graftPath,
		TempDir:   tmpDir,
	}, dirEntries)
	if err != nil {
		return fmt.Errorf("mkdir-p: build subtree for %q: %w", graftPath, err)
	}

	// Upload the directory-only catalog(s) to the local CAS so Commit can
	// stream them to the gateway via SubmitPayload.
	for _, catHash := range mkdirResult.AllCatalogHashes {
		catFile := filepath.Join(tmpDir, cvmfshash.ObjectPath(catHash)+"C")
		f, openErr := os.Open(catFile)
		if openErr != nil {
			return fmt.Errorf("mkdir-p: open catalog %s: %w", catHash, openErr)
		}
		fi, statErr := f.Stat()
		if statErr != nil {
			f.Close()
			return fmt.Errorf("mkdir-p: stat catalog %s: %w", catHash, statErr)
		}
		putErr := o.CAS.Put(ctx, catHash+"C", f, fi.Size())
		f.Close()
		if putErr != nil {
			return fmt.Errorf("mkdir-p: CAS upload catalog %s: %w", catHash, putErr)
		}
	}

	// Acquire the gateway lease for graftPath BEFORE repoMu so that waiting
	// for a busy gateway does not block regular content commits from entering
	// their manifest-fetch critical section.  This mirrors the Phase 2.7
	// approach in Run(): lease acquired outside the mutex, only the lightweight
	// manifest-GET + commit-POST happen inside.
	var mkdirToken string
	var leaseErr error
	if o.GatewayQueue != nil {
		mkdirToken, leaseErr = o.GatewayQueue.Acquire(ctx, j.Repo, graftPath, 0)
	} else {
		mkdirToken, leaseErr = o.Lease.Acquire(ctx, j.Repo, graftPath)
	}
	if leaseErr != nil {
		return fmt.Errorf("mkdir-p: acquire lease for %q: %w", graftPath, leaseErr)
	}

	// Short critical section: serialise manifest-fetch + commit-POST per repo.
	// repoMu prevents two concurrent mkdir-p (or mkdir-p + content commit)
	// operations for the same repo from presenting a stale old_root_hash to
	// the gateway receiver.  The lease is already held so there is no expiry
	// pressure; a simple blocking Lock() is sufficient.
	repoMu := o.repoMutex(j.Repo)
	repoMu.Lock()
	defer repoMu.Unlock()

	// Fetch current root hash from .cvmfspublished (~200-byte HTTP GET).
	mkdirOldRoot, fetchErr := cvmfscatalog.FetchManifestRootHash(ctx, nil, o.Stratum0URL, j.Repo)
	if fetchErr != nil {
		_ = o.Lease.Abort(ctx, mkdirToken)
		return fmt.Errorf("mkdir-p: fetch manifest: %w", fetchErr)
	}

	// Commit the directory-only subtree catalog.
	// Lease.Commit handles SubmitPayload + commit POST.  On failure, abort the
	// lease so the gateway releases it promptly instead of waiting for expiry.
	commitErr := o.Lease.Commit(ctx, lease.CommitRequest{
		Token:               mkdirToken,
		OldRootHash:         mkdirOldRoot,
		NewRootHashSuffixed: mkdirResult.CatalogHashSuffixed,
		CatalogHash:         mkdirResult.CatalogHashSuffixed,
		ObjectStore:         o.CAS,
		// ObjectHashes intentionally empty: no data objects in a dir-only catalog.
	})
	if commitErr != nil {
		_ = o.Lease.Abort(ctx, mkdirToken)
		return fmt.Errorf("mkdir-p: commit for %q: %w", graftPath, commitErr)
	}

	// Wake any job waiting in GatewayQueue.Acquire for graftPath or this repo.
	if o.GatewayQueue != nil {
		o.GatewayQueue.NotifyRelease(j.Repo)
	}

	// Mark all ancestors (and j.Path itself) as known so future publishes to
	// any sub-path under graftPath skip this step.
	for _, anc := range ancestors {
		o.knownPaths.Store(j.Repo+"!"+anc, struct{}{})
	}
	o.knownPaths.Store(j.Repo+"!"+j.Path, struct{}{})

	logger.Info("mkdir-p: parent directory chain committed",
		"catalogs", len(mkdirResult.AllCatalogHashes),
		"entries", len(dirEntries))
	return nil
}

// registerJob records the cancel function for a running job so CancelJob can
// interrupt it.  Called from the goroutine that owns the job, before Run().
func (o *Orchestrator) registerJob(id string, cancel context.CancelFunc) {
	o.runningMu.Lock()
	defer o.runningMu.Unlock()
	if o.running == nil {
		o.running = make(map[string]context.CancelFunc)
	}
	o.running[id] = cancel
}

// unregisterJob removes a job from the cancel map.  Called with defer after Run().
func (o *Orchestrator) unregisterJob(id string) {
	o.runningMu.Lock()
	defer o.runningMu.Unlock()
	delete(o.running, id)
}

// CancelJob cancels a running job and returns true.  Returns false if the job
// is not currently in the running map (already completed or never started).
func (o *Orchestrator) CancelJob(id string) bool {
	o.runningMu.Lock()
	cancel, ok := o.running[id]
	o.runningMu.Unlock()
	if ok {
		cancel()
	}
	return ok
}

// transition moves j to the next state, writes the journal, and publishes a
// notify.Event so SSE subscribers and webhook endpoints learn about the change.
func (o *Orchestrator) transition(ctx context.Context, j *job.Job, to job.State) error {
	if err := o.Spool.Transition(ctx, j, to); err != nil {
		return err
	}
	if o.Notify != nil {
		o.Notify.Publish(notify.Event{
			JobID: j.ID,
			State: to,
			Time:  time.Now(),
		})
	}
	return nil
}

// publishMQTTNotification creates a short-lived MQTT client, publishes a
// PublishedMessage to the per-repo published topic, and disconnects.
//
// It is called in a goroutine after StatePublished so that a slow broker does
// not block the job completion path.  Failures are logged but not propagated —
// a missed notification means S1 receivers on the native ingest path won't
// pull immediately, but they will pull on the next notification.
//
// A per-call client is used (rather than a persistent one) to avoid managing
// lifecycle state in the Orchestrator: the client is connected, one message is
// published, and it is disconnected — analogous to a single-use HTTP POST.
// The overhead (one TCP handshake + one MQTT CONNECT) is negligible compared
// to the catalog commit that just completed.
func (o *Orchestrator) publishMQTTNotification(repo, newRootHash string) {
	if o.BrokerConfig == nil || o.BrokerConfig.BrokerURL == "" {
		return
	}

	// Derive a unique ClientID for this notification so that concurrent calls
	// from different jobs do not collide.  Use the first 8 chars of the root
	// hash for uniqueness; prefix with "pub-notify-" to distinguish from job
	// distributor clients ("pub-<payloadID[:8]>").
	suffix := newRootHash
	if len(suffix) > 8 {
		suffix = suffix[:8]
	}
	cfg := *o.BrokerConfig
	if cfg.ClientID == "" {
		cfg.ClientID = "cvmfs-prepub-notify-" + suffix
	} else {
		cfg.ClientID = cfg.ClientID + "-notify-" + suffix
	}

	client, err := broker.New(cfg)
	if err != nil {
		o.Obs.Logger.Warn("mqtt: failed to connect for publish notification",
			"repo", repo, "error", err)
		return
	}
	defer client.Disconnect(500)

	// Hashes are intentionally omitted: for bits path S1 receivers already
	// hold all pre-warmed objects; for native ingest the receiver pulls the
	// root catalog using NewRootHash.  Omitting hashes keeps the message small.
	msg := broker.PublishedMessage{
		Repo:        repo,
		NewRootHash: newRootHash,
		PublishedAt: time.Now(),
	}

	topic := broker.PublishedTopic(repo)
	if err := client.Publish(topic, 1, false, msg); err != nil {
		o.Obs.Logger.Warn("mqtt: failed to publish commit notification",
			"repo", repo, "new_root_hash", newRootHash, "error", err)
		return
	}

	o.Obs.Logger.Info("mqtt: commit notification published",
		"repo", repo,
		"new_root_hash", newRootHash)
}

// Run executes the job through all pipeline stages.
//
// ── Gateway mode (Lease.NeedsPipeline() == true) ─────────────────────────────
//
//  1. Compress + dedup + upload objects to CAS           (no lease held)
//  2. Pre-push objects to Stratum 1 endpoints            (no lease held)
//  3. Acquire gateway lease                              (lease window starts)
//  4. SubmitPayload + Release(commit=true) via Commit    (lease window ends)
//
// ── Local mode (Lease.NeedsPipeline() == false) ──────────────────────────────
//
//  1. Acquire CVMFS transaction (cvmfs_server transaction)
//  2. Extract tar + cvmfs_server publish via Commit
//
// The lease window in gateway mode covers only SubmitPayload + Release,
// reducing contention on the same sub-path to O(seconds) rather than
// O(pipeline duration).
// Run executes the job through all pipeline stages.
// onStagingComplete, when non-nil, is called exactly once after all CPU/network-
// intensive pipeline work is done but BEFORE the goroutine blocks on the per-repo
// commit serialisation mutex.  The server uses this hook to release the concurrency
// semaphore slot early so a new job can start its own staging phase while this job
// waits for the mutex and executes the commit POST.  For local mode (no pipeline),
// it is called immediately before the Commit call.  Passing nil is safe (Recover
// uses nil since it runs outside the server semaphore).
func (o *Orchestrator) Run(ctx context.Context, j *job.Job, onStagingComplete func()) error {
	ctx, span := o.Obs.Tracer.Start(ctx, "orchestrator.run")
	defer span.End()

	logger := o.Obs.Logger.With("job_id", j.ID)

	// Invariant: gateway mode requires a non-nil CAS backend.
	if o.Lease.NeedsPipeline() && o.CAS == nil {
		err := fmt.Errorf("misconfiguration: gateway mode requires a non-nil CAS backend")
		span.RecordError(err)
		return o.abortJob(ctx, j, err)
	}

	// Record total-S0 start time: from the moment Run() is invoked (job was
	// already in StateIncoming when the goroutine was scheduled).
	jobStartTS := time.Now()

	// ── Phase 1 + 2: pipeline + distribution (gateway mode only) ─────────────
	var pipelineResult *pipeline.Result

	if o.Lease.NeedsPipeline() {
		logger.Info("staging", "tar", j.TarPath)
		if err := o.transition(ctx, j, job.StateStaging); err != nil {
			span.RecordError(err)
			return o.abortJob(ctx, j, err)
		}

		// The job directory was just renamed from incoming/ to staging/.
		// Update TarPath to reflect the new location — the old absolute path
		// now points to a non-existent directory.
		j.TarPath = filepath.Join(o.Spool.JobDir(j), "payload.tar")
		logger.Info("running pipeline", "tar", j.TarPath)
		phaseStart := time.Now()
		j.PipelineStartedAt = phaseStart
		var err error
		// Use a per-job pipeline config so that upload.log and any other
		// SpoolDir-relative files are written to the job's own directory rather
		// than the shared spool root.  Without this, every concurrent job appends
		// to the same spoolRoot/upload.log, making crash-recovery impossible
		// (the log cannot be associated with a specific job).
		jobPipelineCfg := o.Pipeline
		jobPipelineCfg.SpoolDir = o.Spool.JobDir(j)
		jobPipelineCfg.PreloadExe = j.PreloadExe
		jobPipelineCfg.PreloadPaths = j.PreloadPaths

		// Use the prefetch result (Phase 0 done before the concurrency slot was
		// acquired) if it is available and succeeded.  This saves the O(tar-scan)
		// blocking gate — compress workers start immediately rather than waiting
		// for the full tar to be read from disk.
		if prefetch := o.takePrefetch(ctx, j.ID); prefetch != nil {
			logger.Info("using prefetched tar entries (phase 0 already done)",
				"entries", len(prefetch.SortedEntries))
			pipelineResult, err = pipeline.RunFromPrefetch(ctx, prefetch, jobPipelineCfg)
		} else {
			pipelineResult, err = pipeline.Run(ctx, j.TarPath, jobPipelineCfg)
		}
		if err != nil {
			span.RecordError(err)
			logger.Error("pipeline failed", "error", err)
			return o.abortJob(ctx, j, err)
		}
		j.PipelineEndedAt = time.Now()
		pipelineDuration := time.Since(phaseStart)
		o.Obs.Metrics.JobPhaseDuration.WithLabelValues("pipeline").Observe(pipelineDuration.Seconds())

		j.NObjects = len(pipelineResult.ObjectHashes)
		j.NNewObjects = len(pipelineResult.NewObjectHashes)
		j.NBytesRaw = pipelineResult.NBytesRaw
		j.NBytesCompressed = pipelineResult.NBytesComp
		logger.Info("pipeline complete",
			"duration", pipelineDuration.Round(time.Millisecond),
			"objects_total", j.NObjects,
			"objects_new", j.NNewObjects,
			"bytes_raw", j.NBytesRaw,
			"bytes_compressed", j.NBytesCompressed,
		)
		if err := o.Spool.WriteManifest(j); err != nil {
			logger.Warn("best-effort manifest write failed", "job_id", j.ID, "error", err)
		}

		if err := o.transition(ctx, j, job.StateUploading); err != nil {
			span.RecordError(err)
			return o.abortJob(ctx, j, err)
		}

		// Pre-push to Stratum 1s before acquiring the lease to keep the lease
		// window as short as possible.  The DistManager handles concurrency,
		// retries, and backoff per endpoint in its own goroutine pools.
		// The job proceeds immediately to the serialised commit section below.
		shouldDistribute := o.DistManager != nil ||
			(o.Distribute != nil &&
				(len(o.Distribute.Endpoints) > 0 ||
					(o.Distribute.BrokerConfig != nil && o.Distribute.BrokerConfig.BrokerURL != "")))
		if shouldDistribute {
			logger.Info("enqueuing S1 pre-warming (non-blocking)",
				"objects", len(pipelineResult.ObjectHashes),
				"new_objects", len(pipelineResult.NewObjectHashes))

			if err := o.transition(ctx, j, job.StateDistributing); err != nil {
				span.RecordError(err)
				return o.abortJob(ctx, j, err)
			}

			// Record start time and persist it so the console can show that
			// distribution is in progress before it completes.
			j.DistributingStartedAt = time.Now()
			if err := o.Spool.WriteManifest(j); err != nil {
				logger.Warn("best-effort manifest write failed (distributing_started_at)",
					"job_id", j.ID, "error", err)
			}

			if o.DistManager != nil {
				// Queue-driven path: per-endpoint worker pools with retry/backoff.
				distJobID := j.ID
				item := distribute.WorkItem{
					JobID: j.ID,
					// Use NewObjectHashes (freshly uploaded objects only) rather
					// than the full ObjectHashes set.
					//
					// ObjectHashes includes every dedup hit — objects that were
					// already in CAS from previous jobs and were already pushed to
					// S1 when those jobs ran.  Re-pushing them here means the
					// DistManager reads the entire accumulated CAS for every job
					// that shares objects with the first big publish (e.g. a 1.3 GB
					// Python installation that later jobs all link against).  With
					// 32 packages and 99% dedup rates this reads 1.3 GB from CAS
					// for each small subsequent job, explaining the "CAS scanning"
					// slowdown observed after the first large job.
					//
					// NewObjectHashes contains only objects that did not exist in
					// CAS before this pipeline run — these are the only ones S1
					// has not yet seen.  Dedup-hit objects were already present on
					// S1 from the jobs that originally uploaded them.
					Hashes:     append([]string(nil), pipelineResult.NewObjectHashes...),
					TotalBytes: pipelineResult.NBytesComp,
					Repo:       j.Repo,
					EnqueuedAt: j.DistributingStartedAt,
				}
				o.DistManager.Enqueue(item, func(jobID string, confirmed, total int) {
					// Fired from a DistManager worker goroutine when all endpoints finish.
					freshJ, readErr := o.Spool.FindJob(distJobID)
					if readErr != nil {
						o.Obs.Logger.Warn("dist-result: cannot reload manifest",
							"job_id", distJobID, "error", readErr)
						return
					}
					freshJ.DistributingEndedAt = time.Now()
					freshJ.DistributionConfirmed = confirmed
					freshJ.DistributionTotal = total
					// Ghost-directory guard: only write if the job directory still
					// exists at the path derived from freshJ.State.  A narrow race
					// between FindJob (which read state="leased") and this write can
					// occur when abortJob concurrently renames leased/<id> →
					// failed/<id>.  Without the guard, WriteManifest (which calls
					// MkdirAll) would recreate spool/leased/<id>/, producing a ghost
					// entry that listJobs returns and Scan recovers on restart.
					if _, statErr := os.Stat(o.Spool.JobDir(freshJ)); statErr == nil {
						if writeErr := o.Spool.WriteManifest(freshJ); writeErr != nil {
							o.Obs.Logger.Warn("dist-result: manifest update failed",
								"job_id", distJobID, "error", writeErr)
						}
					}
					o.Obs.Logger.Info("dist-result: all endpoints finished",
						"job_id", distJobID, "confirmed", confirmed, "total", total)
				})
			}
			// Job continues immediately to the serialised commit section below.
		}
	} else {
		logger.Info("local publish mode — skipping pipeline, tar will be extracted during Commit")
		// Local mode has no CPU-intensive staging phase.  Release the concurrency
		// slot immediately so the next queued job can start.
		if onStagingComplete != nil {
			onStagingComplete()
		}
	}

	// ── Lease-management variables (used across Phases 2.7, 3, 3.5, 4) ────────
	// Declared at this scope so they can be set by whichever phase acquires
	// the lease (Phase 2.7 for subtree+gateway, Phase 3 for all other cases).
	//
	// Defaults are no-ops so the deferred cleanup is safe even when no lease
	// was acquired (e.g. local mode or early error return).
	var (
		token           string
		leaseCtx        context.Context    = ctx
		leaseCancel     context.CancelFunc = func() {}
		cancelHeartbeat func()             = func() {}
		// preMutexLease is true when the gateway lease AND SubmitPayload were
		// both completed BEFORE the per-repo serialisation mutex.  When true:
		//   • Phase 3 skips lease acquisition (already done)
		//   • Phase 4 calls CommitFinalizeOnly instead of full Commit
		preMutexLease bool
		// err is used across Phases 3.5 and 4 for catalog and commit operations.
		err error
	)
	defer func() { cancelHeartbeat(); leaseCancel() }()

	// ── Phase 2.5–4: per-repo serialisation ─────────────────────────────────
	// Acquire a per-repo mutex so that only ONE job per repo is in the
	// manifest-fetch → commit POST critical section at a time.
	// This prevents two concurrent jobs (publishing to different sub-paths of
	// the same repo) from both reading the same old manifest hash and racing
	// to commit — a situation where the second commit presents a stale
	// old_root_hash to the gateway receiver, which in the testbed causes the
	// receiver subprocess to block indefinitely.
	//
	// Phase 2.6 (subtree catalog build) runs BEFORE the mutex — parallel across
	// all active jobs.  Phase 2.7 (lease acquisition + catalog upload to the
	// gateway) also runs BEFORE the mutex for subtree+gateway jobs, so that
	// job N+1's catalog upload overlaps with job N's commit POST.  The only
	// serialised work remaining inside the mutex is:
	//   • lightweight manifest GET (old_root_hash, ~500 bytes)
	//   • commit POST (cvmfs_receiver graft, the dominant latency term)
	//
	// Transition to StateLeased BEFORE the subtree build and the mutex acquire
	// so that the console shows "leased" (in or approaching the commit window)
	// rather than "distributing" (pre-warming done, stuck on build/mutex).
	//
	// These variables are declared here (outside all if blocks) so that:
	//   • Phase 3.5 (inside the mutex) can read them after BuildSubtree completes.
	//   • Phase 4 (commit) can read all three regardless of which code path ran.
	var subtreeResult *cvmfscatalog.SubtreeResult
	var oldRootHash string
	if o.Lease.NeedsPipeline() {
		if err := o.transition(ctx, j, job.StateLeased); err != nil {
			span.RecordError(err)
			return o.abortJob(ctx, j, err)
		}

		// ── Phase 2.6 / 2.7: catalog work before the per-repo mutex ─────────────
		//
		// All heavy catalog I/O runs here — outside the repoMu critical section —
		// so that jobs for the same repo can overlap their catalog work with each
		// other's commit POST (cvmfs_receiver).
		//
		// Phase 2.6 builds a subtree catalog (BuildSubtree) for path-scoped
		// publishes (j.Path != "").  The gateway receiver grafts this subtree
		// into the existing repository at LeasePath during the commit step; it
		// never needs to see the full repository catalog.
		//
		// Root-level publishes (j.Path == "") are NOT handled here.  The
		// gateway handles root-level leases without a catalog submission from
		// the publisher — only the commit POST is required.
		//
		// Phase 2.7 (gateway mode, GatewayQueue != nil) acquires the lease and
		// uploads the catalog BEFORE the per-repo mutex so that catalog upload
		// overlaps with the previous job's commit POST.  The mutex only guards
		// the lightweight manifest GET + commit POST (O(milliseconds)).
		if pipelineResult != nil && o.Stratum0URL != "" && j.Path != "" {
			// ── Phase 2.6: build subtree catalog ──────────────────────────────
			subtreePhaseStart := time.Now()
			logger.Info("building subtree catalog", "repo", j.Repo, "path", j.Path)

			var buildErr error
			subtreeResult, buildErr = cvmfscatalog.BuildSubtree(ctx, cvmfscatalog.SubtreeConfig{
				LeasePath:     j.Path,
				TempDir:       o.Spool.JobDir(j),
				DirtabContent: pipelineResult.DirtabContent,
				DirectGraft:   o.DirectGraft,
			}, pipelineResult.CatalogEntries)
			if buildErr != nil {
				span.RecordError(buildErr)
				return o.abortJob(ctx, j, fmt.Errorf("subtree catalog build: %w", buildErr))
			}

			// Upload the subtree catalog file(s) to the local CAS so that
			// SubmitPayload (inside the mutex) can stream them to the gateway.
			for _, catHash := range subtreeResult.AllCatalogHashes {
				catFilePath := filepath.Join(o.Spool.JobDir(j),
					cvmfshash.ObjectPath(catHash)+"C")
				f, openErr := os.Open(catFilePath)
				if openErr != nil {
					return o.abortJob(ctx, j,
						fmt.Errorf("opening subtree catalog %s: %w", catHash, openErr))
				}
				fi, statErr := f.Stat()
				if statErr != nil {
					f.Close()
					return o.abortJob(ctx, j,
						fmt.Errorf("stat subtree catalog %s: %w", catHash, statErr))
				}
				putErr := o.CAS.Put(ctx, catHash+"C", f, fi.Size())
				closeErr := f.Close()
				if putErr != nil {
					return o.abortJob(ctx, j,
						fmt.Errorf("uploading subtree catalog %s to CAS: %w", catHash, putErr))
				}
				if closeErr != nil {
					logger.Warn("subtree catalog file close error (CAS upload already complete)",
						"hash", catHash, "error", closeErr)
				}
			}

			o.Obs.Metrics.JobPhaseDuration.WithLabelValues("subtree_build").Observe(
				time.Since(subtreePhaseStart).Seconds())
			logger.Info("subtree catalog ready",
				"catalogs", len(subtreeResult.AllCatalogHashes),
				"root_hash", subtreeResult.CatalogHash)

			// ── Phase 2.65: ensure parent directories exist in CVMFS ──────────
			// cvmfs_receiver grafts a subtree at the exact lease path, but does
			// NOT create missing intermediate directory entries in ancestor
			// catalogs.  Without those entries the FUSE client returns ENOENT for
			// any traversal through those directories.
			//
			// ensureParentDirs commits a tiny directory-only subtree catalog for
			// the first missing ancestor component.  The root catalog SQLite file
			// is never downloaded — only .cvmfspublished (~200 bytes) is fetched.
			// A process-lifetime knownPaths cache makes subsequent publishes to
			// the same hierarchy a no-op.
			//
			// Must run BEFORE Phase 2.7 so that no overlapping path lease exists
			// when the parent-path lease is acquired inside ensureParentDirs.
			if ensureErr := o.ensureParentDirs(ctx, j); ensureErr != nil {
				span.RecordError(ensureErr)
				return o.abortJob(ctx, j, ensureErr)
			}

			// ── Phase 2.7: pre-acquire lease + submit subtree catalog ──────────
			// Acquire the gateway lease and upload the catalog BEFORE the mutex so
			// this work overlaps with the previous job's commit POST.
			//
			// Different-path jobs hold non-overlapping leases simultaneously →
			// lease acquisition and catalog upload run fully in parallel.
			//
			// Same-path jobs: GatewayQueue.Acquire blocks on path_busy until the
			// current holder commits.  The upload then runs, and by the time the
			// mutex is free the catalog is already on the gateway.
			if o.GatewayQueue != nil {
				preMutexLease = true

				// Release the pipeline concurrency slot before gateway I/O.
				// BuildSubtree (Phase 2.6) has just completed — all CPU-intensive
				// compress+catalog work is done.  The remaining work (lease acquire,
				// payload upload) is pure network I/O that does not consume pipeline
				// CPU budget.  Releasing early lets the next queued job start its
				// own compress pipeline immediately rather than waiting for the full
				// lease-acquire + SubmitPayload duration.
				// onStagingComplete is sync.Once-guarded by the server; the safety-net
				// call below (after the mutex wait setup) is a no-op if fired here first.
				if onStagingComplete != nil {
					onStagingComplete()
				}

				logger.Info("acquiring gateway lease (pre-commit-lock)",
					"repo", j.Repo, "path", j.Path)
				leaseAcquireStart := time.Now()
				var leaseErr error
				token, leaseErr = o.GatewayQueue.Acquire(ctx, j.Repo, j.Path, 0)
				if leaseErr != nil {
					span.RecordError(leaseErr)
					return o.abortJob(ctx, j, fmt.Errorf("pre-mutex lease acquire: %w", leaseErr))
				}
				logger.Info("gateway lease acquired (pre-commit-lock)",
					"duration", time.Since(leaseAcquireStart).Round(time.Millisecond))

				j.LeasedAt = time.Now()
				j.LeaseToken = token
				if writeErr := o.Spool.WriteManifest(j); writeErr != nil {
					logger.Warn("best-effort manifest write failed (pre-mutex lease token)",
						"job_id", j.ID, "error", writeErr)
				}

				// Start heartbeat — monitoring-only (gateway HTTP 405 on renewal).
				leaseCtx, leaseCancel = context.WithCancel(ctx)
				cancelHeartbeat = o.Lease.Heartbeat(ctx, token, 10*time.Second, leaseCancel)

				// Upload the subtree catalog(s) to the gateway before the mutex.
				// Split catalogs first, subtree root last — gateway referential integrity.
				// Type-assertion to *lease.Client is safe: GatewayQueue != nil implies
				// gateway mode.
				lc := o.Lease.(*lease.Client)
				var preCatalogHash string
				var preObjectHashes []string
				nn := len(subtreeResult.AllCatalogHashes)
				for i, h := range subtreeResult.AllCatalogHashes {
					if i < nn-1 {
						preObjectHashes = append(preObjectHashes, h+"C")
					} else {
						preCatalogHash = h + "C"
					}
				}

				submitStart := time.Now()
				logger.Info("uploading subtree catalog(s) to gateway (pre-commit-lock)",
					"repo", j.Repo, "catalogs", len(subtreeResult.AllCatalogHashes))
				if submitErr := lc.SubmitPayload(leaseCtx, token, preCatalogHash, preObjectHashes, o.CAS); submitErr != nil {
					span.RecordError(submitErr)
					return o.abortJob(ctx, j, fmt.Errorf("pre-mutex submit payload: %w", submitErr))
				}
				o.Obs.Metrics.JobPhaseDuration.WithLabelValues("submit_payload").Observe(
					time.Since(submitStart).Seconds())
				logger.Info("subtree catalog(s) uploaded to gateway",
					"duration", time.Since(submitStart).Round(time.Millisecond),
					"catalogs", len(subtreeResult.AllCatalogHashes))
			}

		}

		// ── Release pipeline concurrency slot (gateway mode) ──────────────────
		// All CPU-intensive work is complete: compress workers finished (pipeline),
		// subtree catalog was built (Phase 2.6), and catalog was submitted to the
		// gateway (Phase 2.7).  What remains is:
		//   • a context-aware mutex wait (no CPU)
		//   • a manifest GET (~500 bytes, network only)
		//   • a commit POST to cvmfs_receiver (network / gateway I/O, no compress)
		//
		// Releasing the slot here lets the next queued job start its own
		// compress pipeline immediately, overlapping with this job's commit wait.
		// Without this early release, every waiting job in StateIncoming was
		// blocked behind the mutex + commit POST duration even though those phases
		// consume no pipeline-level CPU.
		if onStagingComplete != nil {
			onStagingComplete()
		}

		// Fix: replace the non-interruptible Lock() with a context-aware acquire.
		//
		// sync.Mutex.Lock() cannot be interrupted by context cancellation.  When
		// a job's JobTimeout fires (or the operator cancels a job via CancelJob),
		// the goroutine would block here forever even though its context is done,
		// keeping the job stuck in StateLeased indefinitely and preventing future
		// jobs for the same repo from ever acquiring the mutex (livelock).
		//
		// Pattern: spawn a goroutine that calls the blocking Lock() and closes
		// a channel when it succeeds.  The select below races that channel against
		// ctx.Done().  If the context fires first, a cleanup goroutine waits for
		// the channel (the blocking Lock will eventually return once the current
		// lock-holder finishes) and immediately unlocks so the mutex is not
		// permanently abandoned.
		repoMu := o.repoMutex(j.Repo)
		lockCh := make(chan struct{})
		go func() {
			repoMu.Lock()
			close(lockCh)
		}()

		lockWaitStart := time.Now()
		select {
		case <-lockCh:
			defer repoMu.Unlock()
			if waited := time.Since(lockWaitStart); waited > 5*time.Second {
				logger.Warn("waited for per-repo commit serialisation lock",
					"repo", j.Repo, "waited", waited.Round(time.Millisecond))
			}
		case <-leaseCtx.Done():
			// The pre-acquired lease expired (or heartbeat fired onExpire) while
			// we were queued waiting for the mutex.  Hand the mutex goroutine off
			// to a cleanup goroutine so future jobs are not permanently blocked.
			go func() { <-lockCh; repoMu.Unlock() }()
			leaseErr := fmt.Errorf(
				"gateway lease expired while waiting for commit lock (waited %s): %w",
				time.Since(lockWaitStart).Round(time.Millisecond), leaseCtx.Err())
			span.RecordError(leaseErr)
			return o.abortJob(ctx, j, leaseErr)
		case <-ctx.Done():
			// Our goroutine will eventually acquire the lock; hand it off to a
			// cleanup goroutine that immediately releases it so future jobs
			// for this repo are not permanently blocked.
			go func() { <-lockCh; repoMu.Unlock() }()
			ctxErr := fmt.Errorf("cancelled waiting for commit serialisation lock (waited %s): %w",
				time.Since(lockWaitStart).Round(time.Millisecond), ctx.Err())
			span.RecordError(ctxErr)
			return o.abortJob(ctx, j, ctxErr)
		}
		lockWaitTotal := time.Since(lockWaitStart)
		logger.Info("acquired per-repo commit serialisation lock",
			"repo", j.Repo,
			"waited", lockWaitTotal.Round(time.Millisecond),
		)
	}

	// ── Phase 3: acquire lease / open transaction ─────────────────────────────
	// For subtree publishes in gateway mode (preMutexLease == true), the lease
	// was already acquired and the heartbeat already started in Phase 2.7.
	// Skip acquisition here to avoid a redundant gateway round-trip.
	if !o.Lease.NeedsPipeline() {
		// Local mode: job skipped all pipeline states and is still StateIncoming.
		// Transition to StateLeased here so the FSM is consistent before Commit.
		j.LeasedAt = time.Now()
		if err := o.transition(ctx, j, job.StateLeased); err != nil {
			span.RecordError(err)
			return o.abortJob(ctx, j, err)
		}
	}

	if !preMutexLease {
		// Fallback/local path: lease was not pre-acquired in Phase 2.7
		// (either GatewayQueue is nil, or NeedsPipeline is false).
		// Acquire here inside the per-repo mutex so that FetchManifestRootHash
		// (Phase 3.5) sees the fully committed manifest from the previous job.
		//
		// j.Path == "" means a root-level repo lease; the gateway expects
		// the path as "repo/" which Acquire handles internally.
		logger.Info("acquiring lease", "repo", j.Repo, "path", j.Path)
		j.LeasedAt = time.Now()
		var leaseErr error
		if o.GatewayQueue != nil {
			token, leaseErr = o.GatewayQueue.Acquire(ctx, j.Repo, j.Path, 0)
		} else {
			token, leaseErr = o.Lease.Acquire(ctx, j.Repo, j.Path)
		}
		if leaseErr != nil {
			span.RecordError(leaseErr)
			logger.Error("failed to acquire lease", "error", leaseErr)
			return o.abortJob(ctx, j, leaseErr)
		}

		j.LeaseToken = token
		if writeErr := o.Spool.WriteManifest(j); writeErr != nil {
			logger.Warn("best-effort manifest write failed (lease token)", "job_id", j.ID, "error", writeErr)
		}

		// leaseCtx lets the heartbeat abort the publish if consecutive renewals
		// fail (gateway mode only; the no-op heartbeat never calls onExpire in
		// local mode).
		leaseCtx, leaseCancel = context.WithCancel(ctx)
		cancelHeartbeat = o.Lease.Heartbeat(ctx, token, 10*time.Second, leaseCancel)
	} else {
		logger.Info("using pre-acquired gateway lease (Phase 2.7)",
			"repo", j.Repo, "path", j.Path)
	}

	// ── Phase 3.5: fetch manifest for old_root_hash (subtree publishes only) ──
	// The subtree catalog was built and uploaded to local CAS in Phase 2.6.
	// Only a lightweight manifest GET (~500 bytes) is needed here to obtain
	// old_root_hash for the commit POST.
	//
	// old_root_hash is safe to read here because the per-repo mutex ensures no
	// other job for this repo can commit between this fetch and our commit POST.
	//
	// For root-level publishes (j.Path == "") subtreeResult is nil; skip the
	// manifest fetch and leave oldRootHash empty (commit with empty old_root_hash).
	if pipelineResult != nil && o.Stratum0URL != "" {
		if subtreeResult != nil {
			logger.Info("fetching manifest for old_root_hash", "repo", j.Repo)
			manifestPhaseStart := time.Now()
			oldRootHash, err = cvmfscatalog.FetchManifestRootHash(leaseCtx, nil, o.Stratum0URL, j.Repo)
			if err != nil {
				span.RecordError(err)
				logger.Error("manifest fetch failed", "error", err)
				cancelHeartbeat()
				leaseCancel()
				return o.abortJob(ctx, j, fmt.Errorf("fetching manifest root hash: %w", err))
			}
			o.Obs.Metrics.JobPhaseDuration.WithLabelValues("manifest_fetch").Observe(
				time.Since(manifestPhaseStart).Seconds())
			logger.Info("manifest fetched",
				"old_root", oldRootHash,
				"new_root", subtreeResult.CatalogHash)
		}
	} else if pipelineResult != nil && o.Stratum0URL == "" {
		logger.Warn("Stratum0URL not configured — skipping catalog step; " +
			"commit will use empty old_root_hash (only correct for initial publish)")
	}

	// ── Phase 4: commit ───────────────────────────────────────────────────────
	j.CommittingAt = time.Now() // record commit-phase start before transition
	if err := o.transition(leaseCtx, j, job.StateCommitting); err != nil {
		span.RecordError(err)
		cancelHeartbeat()
		leaseCancel()
		// Do NOT call o.Lease.Abort here — abortJob sees j.LeaseToken and
		// releases the transaction using a fresh cleanup context.  Calling
		// Abort twice would result in a spurious cvmfs_server abort / gateway
		// DELETE request.
		return o.abortJob(ctx, j, err)
	}

	// Build the commit request, populating fields for whichever backend is active.
	cvmfsDir := filepath.Join(o.CVMFSMount, j.Repo, j.Path)
	req := lease.CommitRequest{
		Token:          token,
		TarPath:        j.TarPath,
		CVMFSDir:       cvmfsDir,
		TagName:        j.TagName,
		TagDescription: j.TagDescription,
		DirectGraft:    o.DirectGraft,
	}
	if preMutexLease {
		// Catalog already uploaded to the gateway in Phase 2.7 (BuildSubtree).
		// Only supply the hashes needed for the commit POST — do NOT populate
		// ObjectStore/ObjectHashes/CatalogHash (already uploaded; re-uploading
		// wastes time inside the critical section).
		req.OldRootHash = oldRootHash
		req.NewRootHashSuffixed = subtreeResult.CatalogHashSuffixed
		// ObjectStore, ObjectHashes, CatalogHash intentionally left empty.
	} else {
		// Fallback path (local mode or no GatewayQueue): SubmitPayload will be
		// called inside Commit() below.
		if pipelineResult != nil {
			req.ObjectStore = o.CAS
		}
		if subtreeResult != nil {
			// BuildSubtree catalog without pre-acquired lease (GatewayQueue nil).
			// Build catalog hashes for Commit to upload.
			n := len(subtreeResult.AllCatalogHashes)
			for i, h := range subtreeResult.AllCatalogHashes {
				if i < n-1 {
					req.ObjectHashes = append(req.ObjectHashes, h+"C")
				} else {
					req.CatalogHash = h + "C"
				}
			}
			req.OldRootHash = oldRootHash
			req.NewRootHashSuffixed = subtreeResult.CatalogHashSuffixed
		}
	}

	cancelHeartbeat() // stop renewal before committing (idempotent; defer fires again at return)
	leaseCancel()     // release leaseCtx resources early; Commit uses the parent ctx

	logger.Info("committing")
	commitPhaseStart := time.Now()

	var commitErr error
	if preMutexLease {
		// Catalog already uploaded in Phase 2.7.  Only the commit POST remains.
		// Type-assertion is safe: preMutexLease is only set when o.GatewayQueue != nil.
		lc := o.Lease.(*lease.Client)
		commitErr = lc.CommitFinalizeOnly(ctx, req)
	} else {
		commitErr = o.Lease.Commit(ctx, req)
	}

	if commitErr != nil {
		if errors.Is(commitErr, lease.ErrCommittedNotRemounted) {
			// The catalog IS in the repository — only the FUSE remount failed.
			// Log a warning and continue to StatePublished; the operator must
			// manually restore the mount.
			logger.Warn("publish committed but CVMFS FUSE remount failed — restore mount manually",
				"repo", j.Repo,
				"hint", "mount "+filepath.Join(o.CVMFSMount, j.Repo))
			// Fall through to provenance + StatePublished.
		} else {
			span.RecordError(commitErr)
			logger.Error("commit failed", "error", commitErr)
			return o.abortJob(ctx, j, commitErr)
		}
	}
	o.Obs.Metrics.JobPhaseDuration.WithLabelValues("commit").Observe(time.Since(commitPhaseStart).Seconds())

	// Notify the gateway queue that this repo's lease has been released so any
	// goroutine waiting in GatewayQueue.Acquire wakes up immediately instead of
	// waiting for the next poll interval.
	if o.GatewayQueue != nil {
		o.GatewayQueue.NotifyRelease(j.Repo)
	}

	// Store the new catalog root hash so callers can poll Stratum 1 for propagation.
	//
	// cvmfs_receiver produces the final merged root hash during the graft; we
	// don't know it ahead of time.  Fetch the updated manifest post-commit to
	// record it in j.NewRootHash.  This is a small HTTP GET (~500 bytes) and
	// the manifest is immediately updated after a successful commit.
	if subtreeResult != nil && o.Stratum0URL != "" {
		if newRoot, fetchErr := cvmfscatalog.FetchManifestRootHash(ctx, nil, o.Stratum0URL, j.Repo); fetchErr != nil {
			logger.Warn("could not fetch new root hash from manifest post-commit",
				"error", fetchErr)
		} else {
			// FetchManifestRootHash returns hash+"C"; strip the suffix for
			// j.NewRootHash which is always plain hex (no content-type suffix).
			j.NewRootHash = strings.TrimSuffix(newRoot, "C")
		}
	}

	// Set PublishedAt before the transition so Spool.Transition's internal
	// WriteManifest persists it in the same atomic rename operation.
	j.PublishedAt = time.Now()
	if err := o.transition(ctx, j, job.StatePublished); err != nil {
		span.RecordError(err)
		return err
	}

	// Record total S0 wall time (submission → StatePublished).
	o.Obs.Metrics.JobPhaseDuration.WithLabelValues("total_s0").Observe(time.Since(jobStartTS).Seconds())

	// Publish a commit notification over MQTT so that Stratum 1 receivers can
	// pull any new objects from S0 that were not pre-warmed via the bits
	// pipeline (e.g. native ingest path publishing to the same repo).
	//
	// This is fire-and-forget: a failed publish does not fail the job.  The
	// bits pipeline already pre-warmed all objects before the commit, so the
	// notification is supplemental for S1 receivers on the native ingest path.
	//
	// Hashes are intentionally omitted from the notification: for the bits path
	// S1 receivers already hold all pre-warmed objects, so there is nothing to
	// fetch.  For the native ingest path S1 receivers use the NewRootHash to
	// fetch just the root catalog from S0.  Including the full hash list would
	// make the MQTT message proportionally large (63 bytes × N hashes) and
	// could exceed the broker's message_size_limit for large payloads.
	if o.BrokerConfig != nil && j.NewRootHash != "" {
		go o.publishMQTTNotification(j.Repo, j.NewRootHash)
	}

	// ── Provenance (non-fatal) ────────────────────────────────────────────────
	if o.Provenance != nil && j.Provenance != nil {
		var catalogHash string
		var objectHashes []string
		if subtreeResult != nil {
			catalogHash = subtreeResult.CatalogHash // subtree root catalog hash (plain hex)
			objectHashes = append(objectHashes, subtreeResult.AllCatalogHashes...)
		}
		if pipelineResult != nil {
			objectHashes = append(pipelineResult.ObjectHashes, objectHashes...)
		}
		rec := &provenance.Record{
			JobID:        j.ID,
			Repo:         j.Repo,
			Path:         j.Path,
			CatalogHash:  catalogHash,
			ObjectHashes: objectHashes,
			GitRepo:      j.Provenance.GitRepo,
			GitSHA:       j.Provenance.GitSHA,
			GitRef:       j.Provenance.GitRef,
			Actor:        j.Provenance.Actor,
			PipelineID:   j.Provenance.PipelineID,
			BuildSystem:  j.Provenance.BuildSystem,
			OIDCIssuer:   j.Provenance.OIDCIssuer,
			OIDCSubject:  j.Provenance.OIDCSubject,
			Verified:     j.Provenance.Verified,
		}
		if submitErr := o.Provenance.Submit(ctx, rec); submitErr != nil {
			logger.Warn("provenance: Rekor submission failed (continuing)", "error", submitErr)
		} else if rec.Submitted() {
			j.Provenance.RekorServer = rec.RekorServer
			j.Provenance.RekorUUID = rec.RekorUUID
			j.Provenance.RekorLogIndex = rec.RekorLogIndex
			j.Provenance.RekorIntegratedTime = rec.RekorIntegratedTime
			j.Provenance.RekorSET = rec.RekorSET
			if err := o.Spool.WriteManifest(j); err != nil {
				logger.Warn("best-effort manifest write failed (rekor receipt)", "job_id", j.ID, "error", err)
			}
		}
	}

	// ── Webhook (async, non-fatal) ────────────────────────────────────────────
	if o.Notify != nil && j.WebhookURL != "" {
		webhookCtx, wcancel := context.WithTimeout(context.Background(), 30*time.Second)
		o.webhookWg.Add(1)
		go func() {
			defer o.webhookWg.Done()
			defer wcancel()
			notify.DeliverWebhook(webhookCtx, j.WebhookURL, notify.Event{
				JobID: j.ID,
				State: job.StatePublished,
				Time:  time.Now(),
			}, o.Obs)
		}()
	}

	o.Obs.Metrics.JobsCompleted.Inc()
	logger.Info("job completed successfully",
		"objects", j.NObjects,
		"bytes_raw", j.NBytesRaw,
		"bytes_compressed", j.NBytesCompressed,
	)
	return nil
}

// Recover attempts to re-process a job found in a non-terminal state at
// service startup.  Stale transactions are aborted before the job is reset.
// After MaxRecoveries attempts, jobs are moved to StateFailed.
func (o *Orchestrator) Recover(ctx context.Context, j *job.Job) error {
	ctx, span := o.Obs.Tracer.Start(ctx, "orchestrator.recover")
	defer span.End()

	logger := o.Obs.Logger.With("job_id", j.ID, "state", j.State, "recovery_count", j.RecoveryCount)

	if j.RecoveryCount >= MaxRecoveries {
		err := fmt.Errorf("job %s has reached the maximum recovery limit (%d attempts)", j.ID, MaxRecoveries)
		span.RecordError(err)
		logger.Error("job exceeded max recovery attempts — marking as failed")
		_ = o.abortJob(ctx, j, err)
		return err
	}

	logger.Info("recovering job")

	// Release any stale transaction.  The token may have already been
	// released or expired — Abort is idempotent and errors are non-fatal here.
	if j.LeaseToken != "" {
		if releaseErr := o.Lease.Abort(ctx, j.LeaseToken); releaseErr != nil {
			logger.Warn("failed to abort stale transaction during recovery (ignoring)",
				"token", j.LeaseToken, "error", releaseErr)
		}
		j.LeaseToken = ""
	}

	if err := o.Spool.ResetForRecovery(j); err != nil {
		span.RecordError(err)
		return fmt.Errorf("resetting job for recovery: %w", err)
	}

	logger.Info("job reset to incoming — restarting")
	// Recover runs outside the server semaphore (it is called at startup, not
	// from a job goroutine).  Pass nil so Run skips the early-release hook.
	return o.Run(ctx, j, nil)
}

// abortJob records the failure, writes the manifest, and transitions the job
// to StateFailed.  It always returns its err argument so callers can use it
// in a return statement.
//
// Context independence: ctx may already be cancelled when abortJob is called.
// All cleanup I/O uses a fresh context so it is not short-circuited.
func (o *Orchestrator) abortJob(ctx context.Context, j *job.Job, err error) error {
	o.Obs.Metrics.JobsFailed.Inc()

	class := ClassOf(err)
	o.Obs.Metrics.JobFailuresByClass.WithLabelValues(class.String()).Inc()

	o.Obs.Logger.Error("job failed", "job_id", j.ID, "error", err, "class", class)
	j.Error = "job processing failed — see service logs for details"
	// Record which FSM state the job was in when it failed.  This is used by
	// the console miniPipeline view to highlight the correct pipeline step.
	// Must be captured BEFORE the Transition call below changes j.State.
	if !job.IsTerminal(j.State) && j.FailedAtState == "" {
		j.FailedAtState = string(j.State)
	}
	// NOTE: do NOT set j.State = StateFailed here.  WriteManifest uses j.State
	// to compute the target directory (spool/<state>/<id>/).  If we set
	// j.State = StateFailed prematurely, WriteManifest creates a new empty
	// spool/failed/<id>/ directory while the real job directory is still in
	// spool/<previous-state>/<id>/.  IsTerminal then returns true so
	// Spool.Transition is skipped, leaving the job un-moved.  FindJob searches
	// non-terminal directories first and finds the unrenamed directory with the
	// stale "staging" (or earlier) manifest — so GET /api/v1/jobs/{id} never
	// returns state="failed", and pollers hang until the overall deadline.

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cleanupCancel()

	if j.LeaseToken != "" {
		if abortErr := o.Lease.Abort(cleanupCtx, j.LeaseToken); abortErr != nil {
			// Log at ERROR so operators know the gateway lease was NOT released.
			// The lease will eventually expire on the gateway (max_lease_time),
			// but until then new jobs for the same repo/path will get path_busy.
			o.Obs.Logger.Error("lease abort failed — stale lease left on gateway",
				"job_id", j.ID,
				"token", j.LeaseToken,
				"error", abortErr,
				"hint", "DELETE "+j.LeaseToken+" via gateway API or wait for lease expiry",
			)
		}
		// Notify the gateway queue even when Abort failed: the queue will poll
		// the gateway every 1 s anyway, and notifying on error is harmless.
		if o.GatewayQueue != nil {
			o.GatewayQueue.NotifyRelease(j.Repo)
		}
	}

	// Move the job directory to spool/failed/ (Transition renames it and sets
	// j.State = StateFailed), then write the manifest so the error info and
	// terminal state are visible to FindJob.  WriteManifest must come AFTER
	// Transition so it writes to the correct spool/failed/<id>/ location.
	if !job.IsTerminal(j.State) {
		_ = o.Spool.Transition(cleanupCtx, j, job.StateFailed)
	}
	_ = o.Spool.WriteManifest(j)

	if o.Notify != nil {
		o.Notify.Publish(notify.Event{
			JobID: j.ID,
			State: job.StateFailed,
			Error: j.Error,
			Time:  time.Now(),
		})
		if j.WebhookURL != "" {
			webhookCtx, wcancel := context.WithTimeout(context.Background(), 30*time.Second)
			o.webhookWg.Add(1)
			go func() {
				defer o.webhookWg.Done()
				defer wcancel()
				notify.DeliverWebhook(webhookCtx, j.WebhookURL, notify.Event{
					JobID: j.ID,
					State: job.StateFailed,
					Error: j.Error,
					Time:  time.Now(),
				}, o.Obs)
			}()
		}
	}

	return err
}
