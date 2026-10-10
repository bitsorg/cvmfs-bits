// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Package api orchestrates the HTTP server and job lifecycle management for the CVMFS pre-publisher.
// It coordinates the pipeline (unpack, compress, deduplicate), distribution to Stratum 1 replicas,
// gateway lease acquisition, and publish operations.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	"cvmfs.io/prepub/internal/broker"
	"cvmfs.io/prepub/internal/buildset"
	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/internal/distribute"
	"cvmfs.io/prepub/internal/distribute/manifest"
	"cvmfs.io/prepub/internal/distribute/serve"
	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/lease"
	"cvmfs.io/prepub/internal/measure"
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

// MaxInterrupts is the ceiling on resets caused by a CLEAN service restart.
// These are not the job's fault, so the limit is loose — it exists only so a
// restart loop cannot re-run the same job forever.
const MaxInterrupts = 20

// graftsAt reports whether this job's content commit will graft its subtree
// catalog rather than let the receiver diff it.
//
// One definition, because two places depend on the answer and they must agree:
// the commit itself, and ensureParentDirs, which must not pre-create the leaf
// directory that a graft is going to insert a nested catalog at.
func (o *Orchestrator) graftsAt(j *job.Job) bool {
	// A staged job always grafts: its producer built the catalog, so there is
	// nothing for the receiver to diff against.
	return o.DirectGraft || (j != nil && j.StagingPrefix != "")
}

// promoter is the part of a CAS backend a staged publish needs: moving objects
// a producer prepared under some prefix into the store proper, server-side.
//
// Declared as an interface here, at the point of use, rather than asserting the
// concrete *cas.S3 -- that assertion cannot be satisfied in this package's
// tests, since the S3 fake is private to internal/cas, and it would have left
// the whole staged path unexercised.
type promoter interface {
	PromoteFrom(ctx context.Context, stagingAlias string, workers int) (cas.PromoteResult, error)
}

// CanPromote reports whether b can serve the staged publish path (only the S3
// CAS can promote a staging prefix).
func CanPromote(b cas.Backend) bool {
	_, ok := b.(promoter)
	return ok
}

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
	// Lease is the DEFAULT publish transaction backend.  Use lease.NewClient
	// for gateway mode or lease.NewLocalBackend for single-host mode.  A job
	// that does not name a publish path is published through this one.
	Lease lease.Backend
	// PublishPaths maps a publish-path name to the backend that serves it,
	// letting a producer choose how its package reaches the repository:
	//
	//	"prepub" — the default: compress/dedup/CAS pipeline, then a gateway
	//	           commit; supports pre-warming and coarse (whole-build) publish.
	//	"ingest" — relay: hand the tar to `cvmfs_server ingest` and let the
	//	           gateway do the work.
	//
	// The map is populated at startup from the deployment's configuration, so
	// a path a node cannot serve simply is not there and jobs asking for it are
	// rejected rather than silently published a different way.  This is also
	// the registry that per-repository backends need: the key
	// becomes (repo, path) when one instance serves several repositories.
	PublishPaths map[string]lease.Backend
	// JobTimeout is the maximum wall-clock duration a single job may run
	// before its context is cancelled and the job is failed.  A value of
	// zero disables the per-job timeout (backward-compatible default).
	// Recommended starting value: 30m for gateway mode, 60m for large repos.
	JobTimeout time.Duration

	// RetryWindow is how long, from submission, a job whose attempts fail for
	// a retryable reason keeps being retried before it is failed for good.
	// Zero disables retries: every failure is final (the old behaviour).
	RetryWindow time.Duration
	// CVMFSMount is the filesystem root where CVMFS repositories are mounted
	// (e.g. "/cvmfs").  Only used in local publish mode; ignored by the
	// gateway backend.
	CVMFSMount string
	// Ingest* configure the coarse-publish finalize: one
	// cvmfs_swissknife ingestsql invocation that publishes a whole build's
	// accumulated packages in a single commit. Set at startup; used by
	// FinalizeBuild (the finalize job and the /builds finalize endpoint).
	// Empty IngestConfigPrefix disables finalize (returns a clear error).
	IngestSwissknife   string   // path to cvmfs_swissknife (default: "cvmfs_swissknife")
	IngestConfigPrefix string   // ingestsql -C gateway-client config dir
	IngestEnv          []string // extra env for ingestsql, e.g. "LD_LIBRARY_PATH=..."
	// Stratum0URL is the HTTP base URL of the Stratum 0 CAS, used to fetch the
	// current .cvmfspublished manifest and download the root catalog for the
	// direct SQLite catalog merge (gateway mode only).
	// Example: "http://stratum0.example.org"
	// If empty, the catalog merge step is skipped and the commit will use an
	// empty old_root_hash (only safe for the initial publish of a new repository).
	Stratum0URL string
	// Measurements, when non-nil, records one structured line per publish
	// (internal/measure) so a comparison table is read rather than
	// reconstructed from prose logs. nil disables recording entirely.
	Measurements *measure.Writer
	// measAcc holds the in-flight measurement per job id (map[string]*measAccum).
	measAcc sync.Map
	// ReplaceOnConflict authorises the orchestrator to REPLACE what another
	// build published at a job's path, for jobs that ask (job.Replace): when
	// the published hash differs, delete the existing subtree (the backend's
	// DeleteSubtree, its own committed transaction) and then commit. A failed
	// commit never deletes anything.
	// What is destroyed: the published subtree at the job's path, and nothing
	// else; what recreates it: this job's payload. Prior revisions still
	// reference the old objects until GC.
	//
	// Default false: such a job then fails with a clearly named error. Both
	// the node and the job must ask — deletion of published state must never
	// be a side effect nobody asked for, and a shared root never qualifies.
	ReplaceOnConflict bool

	// PromoteWorkers is the concurrency of the staged path's server-side copy
	// (--promote-workers). Zero keeps cas.PromoteFrom's own default, so an
	// Orchestrator built without setting it behaves exactly as before.
	PromoteWorkers int

	// DirectGraft enables the fast-path commit on the receiver side.
	//
	// When true, the finalise step POSTs to the dedicated gateway graft
	// endpoint (/api/v1/leases/<token>/graft) instead of the standard commit
	// endpoint, instructing cvmfs_receiver to skip DiffRec and graft the
	// pre-built subtree catalog directly into the parent catalog.  This is
	// correct only when the lease path is a brand-new directory with no
	// pre-existing content.
	//
	// Set to false (default) to use the standard CommitProcessor/DiffRec path,
	// which handles arbitrary add/remove/modify operations safely.  Both paths
	// produce identical repository state for "publish new subtree"; DirectGraft
	// is purely a performance optimisation that can be toggled at runtime via
	// --gateway-direct-graft for A/B comparison and integrity verification.
	DirectGraft bool
	// Distribute carries the control-plane broker configuration used to emit the
	// pre-commit pull announce. nil disables the announce (typical for
	// local mode); receivers then converge on the post-commit published broadcast.
	Distribute *distribute.Config
	// PreWarm makes Stratum 1 cache pre-warming available on this node; a job
	// still has to ask for it (preWarmFor). OFF by default: with no S1
	// receivers there is nothing to warm. Enable it (--prewarm) once
	// authoritative S1 receivers exist; the testbed enables it for testing. Post-commit pull
	// (manifests + the published broadcast) is independent of this and stays on
	// whenever the broker is configured, so S1 receivers still converge.
	PreWarm bool

	// BrokerConfig is the MQTT broker configuration used for publishing commit
	// notifications (PublishedMessage) after a successful catalog commit.  When
	// non-nil, a short-lived client is created per commit to publish the message
	// and then disconnected.  nil disables MQTT publish notifications.
	//
	// This is separate from Distribute.BrokerConfig, which is used for the
	// pre-commit announce; this config is for the post-commit "published"
	// notification.  In typical
	// deployments both configs reference the same broker.
	BrokerConfig *broker.Config
	// Manifests, when non-nil, is the per-transaction manifest store used in pull
	// mode. The orchestrator records a manifest for each distributed
	// transaction so a receiver can fetch GET /s1/{txn}/manifest and pull the
	// objects it is missing. Shares the instance passed to MountDistributeServing.
	Manifests serve.ManifestStore
	// PullObjectBaseURL is this publisher's externally reachable base URL for
	// content-addressed object GETs (e.g. http://cvmfs-prepub:8080), embedded in
	// each manifest's BaseURLs as PullObjectBaseURL + "/cvmfs/{repo}/data".
	PullObjectBaseURL string
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
	// cancelled marks jobs an operator aborted, so their failure is final.
	cancelled sync.Map

	// finalizeWg tracks in-flight auto-finalize goroutines.  They are detached
	// from the job goroutine that spawned them (an ingestsql commit outlives the
	// job whose accumulation triggered it), so without this Shutdown would let
	// systemd SIGKILL a commit half-way through.
	finalizeWg sync.WaitGroup
	// finalizeMu serialises FinalizeBuild per build_id across its three entry
	// points (finalize job, /builds/{id}/finalize, auto-finalize).
	// map[buildID]*sync.Mutex; entries are not evicted — one mutex per build is
	// negligible next to the build's spool footprint.
	finalizeMu sync.Map
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

	// prefetchSem bounds concurrent tar scans. Phase 0 runs outside the job
	// concurrency semaphore by design (so a job's compress workers can start the
	// moment it gets a slot), which means it needs a limit of its own or a
	// burst of submissions starts one scan per package at once.
	prefetchSem   *semaphore.Weighted
	prefetchLimit int
	prefetchOnce  sync.Once
	// prefetchDisabled turns phase 0 look-ahead off entirely, so every job
	// scans its own tar inline under its concurrency slot and each archive is
	// read exactly once. See SetPrefetchLimit.
	prefetchDisabled bool
	// prefetchHook, when set, runs inside the scan goroutine while it holds a
	// slot. Tests use it to make concurrency observable; nil in production.
	prefetchHook func()

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

// acquireCommitLock acquires j's per-repo commit serialisation mutex, honouring
// context cancellation. It returns an unlock func (always non-nil, idempotent)
// that the caller must defer to release the lock at Run() return, plus an error
// if ctx fired before the lock was obtained.
//
// sync.Mutex.Lock() is not interruptible, so a blocking Lock() is run in a
// goroutine and raced against ctx.Done(). On ctx-cancel the still-pending Lock()
// is handed to a cleanup goroutine that unlocks as soon as it wins, so the mutex
// is never abandoned and future jobs for the repo are not permanently blocked.
//
// The lock must be taken BEFORE the repo is mutated — i.e. before ensureParentDirs
// (Phase 2.65) for subtree jobs — and held through the content commit and its
// serialize-until-published barrier (Phase 4), so a package's parent-dir creation
// and content graft form one serialised, fully-propagated unit.
func (o *Orchestrator) acquireCommitLock(ctx context.Context, j *job.Job) (func(), error) {
	repo := j.Repo
	repoMu := o.repoMutex(repo)
	lockCh := make(chan struct{})
	go func() {
		repoMu.Lock()
		close(lockCh)
	}()
	start := time.Now()
	// The wait is recorded either way: a job cancelled while queued for the
	// lock spent exactly that time on it.
	defer func() { o.measLockWait(j, time.Since(start)) }()
	select {
	case <-lockCh:
		if waited := time.Since(start); waited > 5*time.Second {
			o.Obs.Logger.Warn("waited for per-repo commit serialisation lock",
				"repo", repo, "waited", waited.Round(time.Millisecond))
		}
		var once sync.Once
		return func() { once.Do(repoMu.Unlock) }, nil
	case <-ctx.Done():
		go func() { <-lockCh; repoMu.Unlock() }()
		return func() {}, fmt.Errorf(
			"cancelled waiting for commit serialisation lock (waited %s): %w",
			time.Since(start).Round(time.Millisecond), ctx.Err())
	}
}

// mkdirMutex returns the per-"repo!graftPath" mutex used by ensureParentDirs.
// Using a separate map from commitMu keeps mkdir-p serialisation isolated from
// the regular content-commit critical section.
func (o *Orchestrator) mkdirMutex(key string) *sync.Mutex {
	v, _ := o.mkdirMu.LoadOrStore(key, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// waitForManifestPropagation is the serialize-until-published barrier. After a
// commit advances the repository, the receiver's NEXT commit grafts against the
// base manifest it fetches from stratum0 — which lags the gateway's committed
// state under rapid sequential commits, so the next graft can miss the parent
// directory this commit just created (a spurious merge_error). Called while the
// per-repo commit lock is still held, it blocks until stratum0's published root
// advances past baseRoot (the root this commit was applied against) so the next
// job's commit sees a current base. This is a barrier, not a retry.
//
// It is bounded: on timeout (or ctx cancellation) it logs loudly and returns the
// last-seen root rather than hanging — the commit itself already succeeded, so a
// wedged stratum0 degrades to the pre-barrier racy behaviour instead of blocking
// the pipeline forever. Returns the observed root (suffixed), or "" if never seen.
func (o *Orchestrator) waitForManifestPropagation(ctx context.Context, repo, path, baseRoot string) string {
	if o.Stratum0URL == "" {
		return ""
	}
	const barrierTimeout = 60 * time.Second
	const barrierPoll = 250 * time.Millisecond
	start := time.Now()
	deadline := start.Add(barrierTimeout)
	var last string
	for {
		root, err := cvmfscatalog.FetchManifestRootHash(ctx, nil, o.Stratum0URL, repo)
		if err == nil {
			last = root
			if root != baseRoot {
				o.Obs.Logger.Info("serialize-until-published: stratum0 reflects commit",
					"repo", repo, "path", path, "waited", time.Since(start).Round(time.Millisecond))
				return root
			}
		} else {
			o.Obs.Logger.Warn("serialize-until-published: manifest fetch failed — retrying",
				"repo", repo, "error", err)
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			o.Obs.Logger.Error("serialize-until-published: stratum0 did not reflect the commit before the barrier deadline; the next publish to this repo may race on a stale base",
				"repo", repo, "path", path, "waited", time.Since(start).Round(time.Millisecond))
			return last
		}
		select {
		case <-ctx.Done():
		case <-time.After(barrierPoll):
		}
	}
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
// DefaultPublishPath is the publish path used when a job does not name one.
const DefaultPublishPath = "prepub"

// StagedPublishPath is the path a job names when its content was prepared by a
// producer and only needs grafting (see lease.StagedBackend). Named here rather
// than written as a literal in the handler and the wiring, because those two
// have to agree or submissions are rejected for naming a path nobody serves.
const StagedPublishPath = "staged"

// leaseFor returns the publish backend for a job.  A job that names a publish
// path gets the backend registered for it; everything else gets the default.
//
// An unknown or unconfigured path falls back to the default rather than
// panicking, because this is on the failure path too (abortJob must be able to
// release a lease for a job whose configuration has since changed). Submission
// is where an unserviceable path is rejected — see Server.publishPathAvailable
// — and Run re-checks before doing any work.
func (o *Orchestrator) leaseFor(j *job.Job) lease.Backend {
	if j != nil && j.PublishPath != "" && j.PublishPath != DefaultPublishPath {
		if b, ok := o.PublishPaths[j.PublishPath]; ok && b != nil {
			return b
		}
	}
	return o.Lease
}

// HasPublishPath reports whether this deployment can serve a publish path.
// The empty name and the default always resolve to the default backend.
func (o *Orchestrator) HasPublishPath(name string) bool {
	if name == "" || name == DefaultPublishPath {
		return o.Lease != nil
	}
	b, ok := o.PublishPaths[name]
	return ok && b != nil
}

// ReplaceAllowed reports whether jobs may ask to replace content another
// build published: the node opted in (replace_on_conflict) and can read the
// published hashes that decide it.
func (o *Orchestrator) ReplaceAllowed() bool {
	return o.ReplaceOnConflict && o.Stratum0URL != ""
}

// canReplaceOn reports whether a publish path's backend can delete a subtree
// in this deployment.
func (o *Orchestrator) canReplaceOn(name string) bool {
	b := o.leaseFor(&job.Job{PublishPath: name})
	if _, ok := b.(subtreeDeleter); !ok {
		return false
	}
	if c, ok := b.(interface{ CanDeleteSubtree() bool }); ok {
		return c.CanDeleteSubtree()
	}
	return true
}

// CoarseSupported reports whether this deployment can accumulate coarse
// builds: only the default path does, and only with a pipeline backend (not in
// local mode, which publishes every package on arrival).
func (o *Orchestrator) CoarseSupported() bool {
	return o.Lease != nil && o.Lease.NeedsPipeline()
}

// isCoarse is j.IsCoarse() limited to what j's backend can do, so a job
// recorded as coarse is never treated as one where nothing accumulates. No
// backend at all means not coarse.
func (o *Orchestrator) isCoarse(j *job.Job) bool {
	if !j.IsCoarse() {
		return false
	}
	b := o.leaseFor(j)
	return b != nil && b.NeedsPipeline()
}

// PublishPathNames lists the publish paths this deployment can serve, for
// startup logging and error messages.
func (o *Orchestrator) PublishPathNames() []string {
	names := make([]string, 0, len(o.PublishPaths)+1)
	if o.Lease != nil {
		names = append(names, DefaultPublishPath)
	}
	for name, b := range o.PublishPaths {
		if b != nil && name != DefaultPublishPath {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

// SetPrefetchLimit sets the concurrent-scan budget. Call at startup.
//
// The unit is 128 MiB of archive, not one goroutine: see prefetchWeight. A
// budget of 4 admits four ordinary packages at once, or one 512 MiB package, or
// any mixture summing to the budget.
//
// n <= 0 uses the default budget. Whether the look-ahead runs AT ALL is a
// separate, explicit setting — see SetPrefetchEnabled — rather than a sentinel
// value of this one: "off" and "how much" are different questions, and encoding
// both in a single integer makes the config unreadable at the point of use.
func (o *Orchestrator) SetPrefetchLimit(n int) {
	o.prefetchOnce.Do(func() {}) // claim the lazy init; explicit config wins
	if n <= 0 {
		n = defaultPrefetchLimit
	}
	o.prefetchSem = semaphore.NewWeighted(int64(n))
	o.prefetchLimit = n
}

// SetPrefetchEnabled turns the phase-0 look-ahead on or off. Call at startup.
//
// Disabling is the right choice on I/O-bound storage. The look-ahead reads the
// whole tar and spills the unpacked entries back to disk, and the pipeline then
// reads the spill: on fast storage that is a good trade, because phase 0
// overlaps the wait for a concurrency slot. On a volume doing single-digit MB/s
// it roughly doubles the I/O on the one resource that is already saturated, to
// buy overlap that is worthless when nothing is waiting on CPU. Off, each tar is
// read exactly once, inline, under the job's own concurrency slot.
func (o *Orchestrator) SetPrefetchEnabled(on bool) {
	o.prefetchOnce.Do(func() {})
	o.prefetchDisabled = !on
}

// PrefetchEnabled reports whether phase 0 runs ahead of the concurrency slot.
func (o *Orchestrator) PrefetchEnabled() bool { return !o.prefetchDisabled }

// defaultPrefetchLimit is used when SetPrefetchLimit was never called.
const defaultPrefetchLimit = 8

// prefetchUnitBytes is one unit of scan budget.
//
// A flat per-scan count is the wrong meter. What a scan actually consumes is
// disk bandwidth and memory, and both scale with the size of the archive: a
// 4 KiB modulefile tar and a 600 MiB ROOT tar are not interchangeable, but a
// count treats them as identical. In practice a limit tuned so that ordinary
// packages flow freely is far too loose the moment several large ones coincide
// — which is exactly when the contention matters, and exactly what was observed
// with a limit of 4.
//
// 128 MiB is chosen so that the overwhelming majority of packages weigh 1 and
// the handful of genuinely large ones are charged in proportion.
const prefetchUnitBytes = 128 << 20

// prefetchWeight converts a tar size into scan-budget units.
//
// Clamped at both ends. At least 1, so a tiny tar still occupies the budget it
// really does cost (an open, a scan, a spill directory). At most the whole
// budget, so an archive larger than the entire budget is still admissible when
// the system is idle — otherwise TryAcquire could never succeed for it and the
// largest packages would be permanently denied a prefetch, which is precisely
// backwards.
func prefetchWeight(size int64, limit int) int64 {
	if limit < 1 {
		limit = 1
	}
	units := (size + prefetchUnitBytes - 1) / prefetchUnitBytes // ceil
	if units < 1 {
		units = 1
	}
	if units > int64(limit) {
		units = int64(limit)
	}
	return units
}

func (o *Orchestrator) prefetchSlots() *semaphore.Weighted {
	o.prefetchOnce.Do(func() {
		if o.prefetchSem == nil {
			o.prefetchSem = semaphore.NewWeighted(defaultPrefetchLimit)
			o.prefetchLimit = defaultPrefetchLimit
		}
	})
	return o.prefetchSem
}

// StartPrefetch begins phase 0 — reading and sorting the tar's entries — before
// the job competes for a concurrency slot, so the compress workers can start
// immediately once it gets one.
//
// It is BOUNDED, and that bound is the point. This is called from submitJob,
// once per job, at submission time. A producer that uploads a whole build in one
// burst therefore used to start one tar scan per package simultaneously — a
// 174-package build meant 174 concurrent scans, each reading an archive and
// spilling its large entries to the spool. The job semaphore did not help: it
// bounds the pipeline, while the expensive part ran outside it. The result was a
// publisher at 0% CPU with every job in I/O wait, taking four minutes to do
// sixteen seconds of pipeline work, and getting worse the more work it was given.
//
// The budget is charged BY SIZE (prefetchWeight), not per scan. Disk bandwidth
// and memory are what a scan consumes, and both scale with the archive; a count
// tuned to let ordinary packages flow admits far too much work the moment
// several large ones arrive together.
//
// When the budget is exhausted the prefetch is SKIPPED rather than queued.
// Queueing would just move the contention and delay the job that is actually
// running; skipping falls through to takePrefetch returning nil, and
// pipeline.Run does phase 0 inline under the job's own slot — correct, already
// exercised, and self-limiting because it is gated by the job semaphore.
func (o *Orchestrator) StartPrefetch(ctx context.Context, j *job.Job) {
	if !o.leaseFor(j).NeedsPipeline() {
		return // local mode: no pipeline, no prefetch
	}
	if j.TarPath == "" {
		return
	}
	if o.prefetchDisabled {
		return // phase 0 runs inline: one read per tar, no spill round trip
	}

	// Open the file synchronously to obtain a stable inode reference before
	// any directory rename can occur. Opened BEFORE the budget is charged so
	// the size can be taken from the handle rather than the path — the
	// directory may be renamed underneath us at any moment.
	f, err := os.Open(j.TarPath)
	if err != nil {
		o.Obs.Logger.Warn("prefetch: cannot open tar — will fall back to pipeline.Run()",
			"job_id", j.ID, "path", j.TarPath, "error", err)
		return // no channel stored → takePrefetch returns nil → fallback
	}

	var size int64
	if fi, serr := f.Stat(); serr == nil {
		size = fi.Size()
	} // a failed stat weighs 1: cheap to admit, and the scan still bounds itself

	sem := o.prefetchSlots()
	weight := prefetchWeight(size, o.prefetchLimit)
	if !sem.TryAcquire(weight) {
		f.Close()
		o.Obs.Logger.Debug("prefetch skipped — scan budget exhausted; phase 0 will run inline",
			"job_id", j.ID, "tar_bytes", size, "weight", weight, "budget", o.prefetchLimit)
		return // no channel stored → takePrefetch returns nil → inline fallback
	}

	ch := make(chan *pipeline.PrefetchResult, 1)
	o.prefetchResults.Store(j.ID, ch)

	go func() {
		defer f.Close()
		defer sem.Release(weight)
		if o.prefetchHook != nil {
			o.prefetchHook()
		}
		// Spill large entries under the spool dir. Without this the prefetch
		// holds the ENTIRE uncompressed package in memory until the job runs,
		// which is what OOM-killed the service on an 8 GB host. Read from the
		// handle opened above, not the path, to keep the stable inode reference.
		result, err := pipeline.PrefetchFromReaderWithSpill(ctx, f, o.Pipeline.SpoolDir, o.Pipeline.EntryLimit(), o.Obs)
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
//  1. BuildSubtree  — produces a minimal SQLite catalog (a few KB at most)
//  2. CAS.Put       — uploads the catalog to the local CAS
//  3. FetchManifestRootHash — lightweight manifest GET (~200 bytes)
//  4. GatewayQueue.Acquire / Lease.Acquire for graftPath
//  5. Lease.Commit  — SubmitPayload + commit POST (creates the dir entries)
//  6. Mark ancestors in knownPaths so subsequent publishes skip this step
//
// The caller (Run, Phase 2.65) already holds the per-repo commit lock — this
// function does NOT take it — so the mkdir commit and the content commit that
// follows are one serialised, fully-propagated unit.
//
// The root catalog SQLite file is never downloaded.
//
// # Call contract
//
// Must be called AFTER Phase 2.6 (BuildSubtree for content) but BEFORE
// Phase 2.7 (leaf lease acquisition), so that no overlapping path leases
// exist when the parent lease is acquired.
func (o *Orchestrator) ensureParentDirs(ctx context.Context, j *job.Job) error {
	// A staged job needs parent directories for the same reason a pipeline job
	// does -- both graft a subtree at the lease path -- but it has no pipeline,
	// so NeedsPipeline alone would exclude it.
	if o.Stratum0URL == "" || j.Path == "" ||
		(!o.leaseFor(j).NeedsPipeline() && j.StagingPrefix == "") {
		return nil
	}
	// This function uploads the catalog it builds, so it needs a CAS. The
	// invariant check at startup only covers pipeline backends, and a staged job
	// is not one -- without this it would panic on o.CAS.Put below, before
	// reaching the staged path's own clear "needs a CAS that can promote" error.
	if o.CAS == nil {
		return fmt.Errorf("mkdir-p: no CAS configured; cannot create parent " +
			"directories for a grafted publish")
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
	//
	// The test is the EFFECTIVE graft mode, which must be computed exactly as
	// the content commit computes it. A staged job always grafts -- its producer
	// built the catalog, so there is nothing to diff -- regardless of the node's
	// o.DirectGraft setting. Testing o.DirectGraft alone would, on a node run
	// with --gateway-direct-graft=false, pre-create j.Path here and then graft
	// into it. That fails as a generic merge_error, and the handler's PathExists
	// check then finds the directory this function just created and reports
	// "already published" -- a first-ever publish rejected as a duplicate.
	if !o.graftsAt(j) {
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
		// Parent directories only: this subtree is merged into the existing
		// catalog, so graftPath does not become a nested catalog root and must
		// not receive a .cvmfscatalog marker.
		DirsOnly: true,
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
		// Acquire (and the abort on the failure paths below) stay on the job's
		// backend while the commit above uses o.Lease. That is not an
		// inconsistency: StagedBackend embeds the very same *lease.Client, so
		// the token it issues is the token o.Lease commits and aborts. Only
		// Commit differs between them, and only in whether it submits a payload.
		mkdirToken, leaseErr = o.leaseFor(j).Acquire(ctx, j.Repo, graftPath)
	}
	if leaseErr != nil {
		return fmt.Errorf("mkdir-p: acquire lease for %q: %w", graftPath, leaseErr)
	}

	// The caller (Run, Phase 2.65) holds the per-repo commit lock across this
	// mkdir-p commit AND the content commit that follows, so no other job for
	// this repo can commit in between and present a stale old_root_hash to the
	// gateway receiver. This function does not take the lock itself.

	// Fetch current root hash from .cvmfspublished (~200-byte HTTP GET).
	mkdirOldRoot, fetchErr := cvmfscatalog.FetchManifestRootHash(ctx, nil, o.Stratum0URL, j.Repo)
	if fetchErr != nil {
		o.abortLeaseDetached(j, mkdirToken)
		return fmt.Errorf("mkdir-p: fetch manifest: %w", fetchErr)
	}

	// Commit the directory-only subtree catalog through the DEFAULT backend, not
	// the job's own.
	//
	// Creating a parent directory is an ordinary small publish that happens to
	// precede another one; it is not a staged publish. o.Lease.Commit does
	// SubmitPayload + commit POST, which is what puts this freshly built catalog
	// where the gateway can read it. A staged job's own backend deliberately
	// skips SubmitPayload -- correct for its own content, which is already in
	// the store, and wrong for a catalog built here seconds ago.
	//
	// Behaviour-preserving for every path that reached this function before:
	// the guard above admitted only backends with NeedsPipeline() true, and the
	// gateway Client is the only one, so o.leaseFor(j) WAS o.Lease.
	//
	// On failure, abort the lease so the gateway releases it promptly instead of
	// waiting for expiry.
	commitErr := o.Lease.Commit(ctx, lease.CommitRequest{
		Token:               mkdirToken,
		OldRootHash:         mkdirOldRoot,
		NewRootHashSuffixed: mkdirResult.CatalogHashSuffixed,
		CatalogHash:         mkdirResult.CatalogHashSuffixed,
		ObjectStore:         o.CAS,
		// ObjectHashes intentionally empty: no data objects in a dir-only catalog.
	})
	if commitErr != nil {
		o.abortLeaseDetached(j, mkdirToken)
		return fmt.Errorf("mkdir-p: commit for %q: %w", graftPath, commitErr)
	}

	// Serialize-until-published: block until stratum0 reflects this parent-dir
	// commit before returning, so the content graft that immediately follows (and
	// any concurrent job) grafts onto a base that already contains these
	// directories. Same barrier as the content commit; bounded, best-effort on
	// timeout.
	_ = o.waitForManifestPropagation(ctx, j.Repo, graftPath, mkdirOldRoot)

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
	o.cancelled.Delete(id)
}

// CancelJob cancels a running job and returns true.  Returns false if the job
// is not currently in the running map (already completed or never started).
func (o *Orchestrator) CancelJob(id string) bool {
	o.runningMu.Lock()
	cancel, ok := o.running[id]
	o.runningMu.Unlock()
	if ok {
		o.cancelled.Store(id, true)
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

	msg := broker.PublishedMessage{
		Repo:        repo,
		NewRootHash: newRootHash,
		PublishedAt: time.Now(),
	}

	topic := broker.PublishedTopic(repo)
	if err := client.Publish(topic, 1, true, msg); err != nil { // retain=true: reconnecting receivers catch up on the latest commit
		o.Obs.Logger.Warn("mqtt: failed to publish commit notification",
			"repo", repo, "new_root_hash", newRootHash, "error", err)
		return
	}

	o.Obs.Logger.Info("mqtt: commit notification published",
		"repo", repo,
		"new_root_hash", newRootHash)
}

// preWarmFor reports whether this job should pre-warm Stratum 1 caches.
//
// Opt-in at both levels: the node must be started with --prewarm (prewarm:
// true in config.yaml), which only makes pre-warming available, and the job
// must ask for it. Pre-warming is expensive for the receivers and pointless for
// a package nobody will read soon, so neither alone turns it on.
func (o *Orchestrator) preWarmFor(j *job.Job) bool {
	return o.PreWarm && j != nil && j.PreWarm != nil && *j.PreWarm
}

// publishAnnounce broadcasts the pre-commit AnnounceMessage directly on the
// control-plane broker so Stratum 1 receivers begin pulling the transaction's
// objects before the catalog flips (pull mode). It mirrors
// publishMQTTNotification: a single-use broker.Client connects with the
// distribution BrokerConfig (which carries the token CredentialsProvider so it
// authenticates to the embedded broker), publishes one message to the repo
// announce topic, and disconnects.
//
// The announce is best-effort: a failed broadcast is logged but never blocks or
// fails the publish. Receivers also converge on the retained post-commit
// published message, so a missed announce only delays warming.
func (o *Orchestrator) publishAnnounce(j *job.Job, repo, payloadID string, totalBytes int64) {
	if !o.preWarmFor(j) {
		return // S1 cache pre-warming is opt-in; off by default.
	}
	if o.Distribute == nil || o.Distribute.BrokerConfig == nil ||
		o.Distribute.BrokerConfig.BrokerURL == "" {
		return
	}

	// Per-call ClientID so concurrent announces from different jobs do not
	// collide on the broker (which would force-disconnect the earlier session).
	suffix := payloadID
	if len(suffix) > 8 {
		suffix = suffix[:8]
	}
	cfg := *o.Distribute.BrokerConfig
	if cfg.ClientID == "" {
		cfg.ClientID = "cvmfs-prepub-announce-" + suffix
	} else {
		cfg.ClientID = cfg.ClientID + "-announce-" + suffix
	}

	client, err := broker.New(cfg)
	if err != nil {
		o.Obs.Logger.Warn("mqtt: failed to connect for announce",
			"repo", repo, "payload_id", payloadID, "error", err)
		return
	}
	defer client.Disconnect(500)

	// PublisherID matches the legacy distributeMQTT scheme ("pub-"+payloadID) so
	// the announce is identical on the wire to what receivers handled before.
	msg := broker.AnnounceMessage{
		PayloadID:   payloadID,
		PublisherID: "pub-" + payloadID,
		Repo:        repo,
		TotalBytes:  totalBytes,
	}
	if err := client.Publish(broker.AnnounceTopic(repo), 1, false, msg); err != nil {
		o.Obs.Logger.Warn("mqtt: failed to publish announce",
			"repo", repo, "payload_id", payloadID, "error", err)
		return
	}
	o.Obs.Logger.Info("mqtt: announce published",
		"payload_id", payloadID,
		"repo", repo)
}

// publishedBytes is a job's payload size for the throughput counter: the
// submitted tar, else the pipeline's uncompressed content (a staged job has
// neither and counts zero).
func publishedBytes(j *job.Job) int64 {
	if j.TarSize > 0 {
		return j.TarSize
	}
	return j.NBytesRaw
}

// storePullManifest records the transaction manifest a receiver fetches after
// an announce (GET /s1/{txn}/manifest), keyed by j.ID -- the payload id the
// announce carries. Objects are content-addressed and hash-verified by the
// receiver, so the fetch does not depend on the catalog root. Reports whether a
// manifest was stored: without one an announce would only produce 404s. No-op
// unless pull serving is configured.
func (o *Orchestrator) storePullManifest(ctx context.Context, j *job.Job, hashes []string, totalBytes int64, logger *slog.Logger) bool {
	if o.Manifests == nil || o.PullObjectBaseURL == "" {
		return false
	}
	objs := make([]manifest.ObjRef, 0, len(hashes))
	for _, h := range hashes {
		objs = append(objs, manifest.ObjRef{Hash: h})
	}
	rootHash := j.NewRootHash
	if rootHash == "" {
		rootHash = j.ID // placeholder: not needed for an object pull
	}
	mf := &manifest.Manifest{
		TransactionID:  j.ID,
		Repo:           j.Repo,
		TargetRootHash: rootHash,
		BaseURLs:       []string{strings.TrimRight(o.PullObjectBaseURL, "/") + "/cvmfs/" + j.Repo + "/data"},
		Generator:      manifest.GeneratorPipeline,
		Auth:           manifest.AuthPublic,
		CreatedAt:      time.Now(),
		TotalSize:      totalBytes,
		Objects:        objs,
	}
	if err := o.Manifests.Put(ctx, mf); err != nil {
		logger.Warn("pull: failed to store transaction manifest", "txn", j.ID, "error", err)
		return false
	}
	logger.Info("pull: transaction manifest stored", "txn", j.ID, "objects", len(objs))
	return true
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
// it is called immediately before the Commit call.  Passing nil is safe (a
// caller outside the server semaphore has no slot to release).
func (o *Orchestrator) Run(ctx context.Context, j *job.Job, onStagingComplete func()) error {
	ctx, span := o.Obs.Tracer.Start(ctx, "orchestrator.run")
	defer span.End()

	logger := o.Obs.Logger.With("job_id", j.ID)
	// Start recording before the first thing that can fail, so a job rejected
	// on arrival is measured too: a run's failures are as interesting as its
	// successes, and the failures are what needed measuring most.
	o.measBegin(j)
	if j.PreWarm != nil && *j.PreWarm && !o.PreWarm {
		logger.Info("prewarm requested but pre-warming is not enabled on this node (--prewarm / prewarm: true); not pre-warming")
	}
	// Backstop: whatever exit Run takes, the accumulator is released and a
	// record is written. The explicit measFinish calls on the success and
	// abort paths claim the interesting outcomes first; this catches the
	// rest, including the coarse-publish accumulate return that leaves a job
	// legitimately unfinished.
	defer o.measSweep(j)
	j.NextAttemptAt = nil // due now; persisted with the next state change

	// The publish path is checked at submission, but a job can also arrive here
	// from crash recovery after the deployment's configuration changed. Publish
	// it a different way than the producer asked for and the build would look
	// fine while having taken a path with different dedup, pre-warming and
	// commit-granularity properties — so fail instead.
	if !o.HasPublishPath(j.PublishPath) {
		return o.abortJob(ctx, j, fmt.Errorf(
			"publish path %q is not configured on this prepub (available: %s)",
			j.PublishPath, strings.Join(o.PublishPathNames(), ", ")))
	}

	// ── Coarse-publish finalize job ──────────────────────────────────────────
	// A finalize job carries no payload: it publishes all of BuildID's
	// accumulated packages in one ingestsql commit. Release the concurrency slot
	// immediately (no pipeline work) and commit.
	if j.Finalize {
		if onStagingComplete != nil {
			onStagingComplete()
		}
		if j.BuildID == "" {
			return o.abortJob(ctx, j, fmt.Errorf("finalize job requires a build_id"))
		}
		if err := o.transition(ctx, j, job.StateCommitting); err != nil {
			span.RecordError(err)
			return o.abortJob(ctx, j, err)
		}
		res, ferr := o.FinalizeBuild(ctx, j.BuildID)
		if ferr != nil {
			if res != nil {
				// Log the tail of the captured ingestsql output: on a crash or
				// guard abort the actual reason is only in that stderr (the
				// client-facing job error is sanitized for internal failures).
				out := res.Output
				if len(out) > 2000 {
					out = "…" + out[len(out)-2000:]
				}
				logger.Error("build finalize failed", "build_id", j.BuildID,
					"published", res.Published, "conflicts", len(res.Conflicts),
					"ingest_output", out)
			}
			span.RecordError(ferr)
			return o.abortJob(ctx, j, fmt.Errorf("finalize build %s: %w", j.BuildID, ferr))
		}
		logger.Info("build finalized", "build_id", j.BuildID,
			"packages", res.Packages, "published", res.Published, "conflicts", len(res.Conflicts))
		j.PublishedAt = time.Now()
		// Record BEFORE the sweep can: a finalize that succeeded is a publish,
		// and letting the backstop label it "incomplete:published" put a
		// successful build in the failure column of every summary.
		o.measFinish(j, "published", nil)
		return o.transition(ctx, j, job.StatePublished)
	}

	// Invariant: gateway mode requires a non-nil CAS backend.
	if o.leaseFor(j).NeedsPipeline() && o.CAS == nil {
		err := fmt.Errorf("misconfiguration: gateway mode requires a non-nil CAS backend")
		span.RecordError(err)
		return o.abortJob(ctx, j, err)
	}

	// Record total-S0 start time: from the moment Run() is invoked (job was
	// already in StateIncoming when the goroutine was scheduled).
	jobStartTS := time.Now()

	// ── Phase 1 + 2: pipeline + distribution (gateway mode only) ─────────────
	var pipelineResult *pipeline.Result

	if o.leaseFor(j).NeedsPipeline() {
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
			// Release spilled content as soon as the pipeline is done with it;
			// otherwise a failed job leaves the package on disk until restart.
			prefetch.Cleanup()
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
		shouldDistribute := o.Distribute != nil &&
			o.Distribute.BrokerConfig != nil &&
			o.Distribute.BrokerConfig.BrokerURL != ""
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

			// Pull mode: record the transaction manifest so a receiver
			// triggered by the announce can GET /s1/{txn}/manifest and pull the
			// objects it is missing. Keyed by j.ID — the same payloadID the announce
			// carries. Objects are content-addressed (self-verifying), so the fetch
			// is independent of the catalog root, which is not known until commit.
			stored := o.storePullManifest(ctx, j, pipelineResult.ObjectHashes, pipelineResult.NBytesComp, logger)
			// Pull mode: publish the pre-commit announce directly on the
			// embedded broker so receivers begin pulling the new objects before the
			// catalog flips. This mirrors publishMQTTNotification (the post-commit
			// "published" broadcast): a single-use broker.Client connects, publishes
			// one AnnounceMessage, and disconnects. Its CredentialsProvider (carried
			// on BrokerConfig) authenticates to the token-gated broker. A failed
			// announce only means receivers converge on the post-commit published
			// broadcast.
			if stored && o.Distribute != nil && o.Distribute.BrokerConfig != nil &&
				o.Distribute.BrokerConfig.BrokerURL != "" {
				o.publishAnnounce(j, j.Repo, j.ID, pipelineResult.NBytesComp)
			}
			// Job continues immediately to the serialised commit section below.
		}
	} else {
		// The backend (ingest, local) takes the tar as is at commit time.
		logger.Info("no prepub pipeline for this backend — the tar is handed to the backend at commit",
			"backend", fmt.Sprintf("%T", o.leaseFor(j)))
		// No CPU-intensive staging phase here.  Release the concurrency slot
		// immediately so the next queued job can start.
		if onStagingComplete != nil {
			onStagingComplete()
		}
	}

	// ── Coarse publish: accumulate entries, defer the commit ─────────────────
	// When the job belongs to a build (BuildID set), its objects are already in
	// CAS and pre-warmed to the Stratum 1s above.  Record its catalog entries in
	// the build-scoped accumulator and finish in StateAccumulated; a single
	// end-of-build finalize (POST /builds/{id}/finalize) then publishes the whole
	// set in one gateway commit via ingestsql. Keyed on the job's coarse
	// decision, not on having a build id: the id is the run's identity and is
	// carried on every path, including the ones that commit on arrival.
	if o.isCoarse(j) && pipelineResult != nil && j.Path != "" {
		if onStagingComplete != nil {
			onStagingComplete()
		}
		if recErr := buildset.Record(o.Spool.Root, j.BuildID, buildset.Member{
			JobID:           j.ID,
			Repo:            j.Repo,
			Path:            j.Path,
			BitsFingerprint: j.TarSHA256,
			Entries:         pipelineResult.CatalogEntries,
			Dirtab:          string(pipelineResult.DirtabContent),
		}); recErr != nil {
			span.RecordError(recErr)
			return o.abortJob(ctx, j, fmt.Errorf("buildset record: %w", recErr))
		}
		if err := o.transition(ctx, j, job.StateAccumulated); err != nil {
			span.RecordError(err)
			return err
		}
		logger.Info("accumulated into build (deferred publish)",
			"build_id", j.BuildID, "path", j.Path,
			"entries", len(pipelineResult.CatalogEntries))
		o.maybeAutoFinalize(j.BuildID)
		return nil
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
		// releaseCommit unlocks the per-repo commit serialisation mutex. It is
		// assigned when the lock is acquired — before Phase 2.65 for subtree jobs,
		// or before Phase 3 for root-level jobs — and fires at Run() return, so the
		// lock spans the whole repo-mutation phase (parent dirs → content commit →
		// serialize-until-published barrier). commitLockHeld guards against a
		// double acquire between the two entry points.
		releaseCommit  = func() {}
		commitLockHeld bool
	)
	defer func() { cancelHeartbeat(); leaseCancel() }()
	defer func() { releaseCommit() }()

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
	// Set when BuildSubtree synthesized a .cvmfscatalog marker: its empty-file
	// object must be submitted to the gateway alongside the catalogs.
	var markerObjectHash string
	if o.leaseFor(j).NeedsPipeline() {
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

			// A synthesized .cvmfscatalog marker references the empty-file
			// object, which no tar necessarily contained — store it so the
			// entry does not point at a missing object (checked by
			// `cvmfs_swissknife check -c` and fetched by clients like any
			// other file). Content-addressed and idempotent: at most one
			// 8-byte object per repository, whoever writes it first wins.
			if subtreeResult.NeedsMarkerObject && o.CAS != nil {
				mHash, _, mObj := cvmfscatalog.NestedMarkerObject()
				if putErr := o.CAS.Put(ctx, mHash, bytes.NewReader(mObj),
					int64(len(mObj))); putErr != nil {
					return o.abortJob(ctx, j,
						fmt.Errorf("storing nested-catalog marker object %s: %w", mHash, putErr))
				}
				markerObjectHash = mHash
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

			// All CPU-intensive work (pipeline + subtree build) is done. Release
			// the concurrency slot now so the next queued job can start staging
			// while this job holds the per-repo commit lock for its serialised
			// mutation phase below. (sync.Once-guarded: safe to call again later.)
			if onStagingComplete != nil {
				onStagingComplete()
			}

			// ── Acquire the per-repo commit lock BEFORE mutating the repo ─────────
			// Hold it across ensureParentDirs (Phase 2.65), the pre-commit lease +
			// SubmitPayload (Phase 2.7) and the content commit + barrier (Phase 4),
			// so each package's parent-dir creation and content graft are one
			// serialised, fully-propagated unit. Without this the cold-start burst
			// races: many jobs commit content against a base whose parent dirs are
			// not yet committed/propagated, all fail merge_error, and we do not retry.
			unlockCommit, lockErr := o.acquireCommitLock(ctx, j)
			if lockErr != nil {
				span.RecordError(lockErr)
				return o.abortJob(ctx, j, lockErr)
			}
			releaseCommit = unlockCommit
			commitLockHeld = true
			logger.Info("acquired per-repo commit serialisation lock (pre-mutation)", "repo", j.Repo)

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
			// when the parent-path lease is acquired inside ensureParentDirs. The
			// per-repo commit lock is already held (above), so the mkdir commit and
			// the content commit are serialised together.
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
				cancelHeartbeat = o.leaseFor(j).Heartbeat(ctx, token, 10*time.Second, leaseCancel)

				// Upload the subtree catalog(s) to the gateway before the mutex.
				// Split catalogs first, subtree root last — gateway referential integrity.
				// Type-assertion to *lease.Client is safe: GatewayQueue != nil implies
				// gateway mode.
				lc := o.leaseFor(j).(*lease.Client)
				var preCatalogHash string
				var preObjectHashes []string
				// The marker's empty-file object is a content object, so it
				// must reach the gateway BEFORE the catalog referencing it
				// (SubmitPayload sends the catalog last for exactly this
				// referential-integrity reason).
				if markerObjectHash != "" {
					preObjectHashes = append(preObjectHashes, markerObjectHash)
				}
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

		// Release the pipeline concurrency slot (idempotent). Subtree jobs already
		// released it and took the commit lock above (before Phase 2.65). This
		// covers root-level publishes (j.Path == "") which skip that block.
		if onStagingComplete != nil {
			onStagingComplete()
		}

		// Root-level publishes did not enter the subtree block above, so acquire
		// the per-repo commit lock here — still BEFORE Phase 3 lease acquisition,
		// so FetchManifestRootHash sees the previous job's fully committed manifest.
		if !commitLockHeld {
			unlockCommit, lockErr := o.acquireCommitLock(ctx, j)
			if lockErr != nil {
				span.RecordError(lockErr)
				return o.abortJob(ctx, j, lockErr)
			}
			releaseCommit = unlockCommit
			commitLockHeld = true
			logger.Info("acquired per-repo commit serialisation lock", "repo", j.Repo)
		}
	}

	// ── Phase 3: acquire lease / open transaction ─────────────────────────────
	// For subtree publishes in gateway mode (preMutexLease == true), the lease
	// was already acquired and the heartbeat already started in Phase 2.7.
	// Skip acquisition here to avoid a redundant gateway round-trip.
	if !o.leaseFor(j).NeedsPipeline() {
		// ── Staged publish: promote, BEFORE the lock and the lease ────────────
		//
		// A producer running the canonical publisher elsewhere already chunked,
		// compressed and hashed this package into a prefix of the repository's
		// own bucket, and built the subtree catalog. There is no tar: prepub
		// moves the objects into the CAS with a server-side copy and asks the
		// gateway to graft the catalog the producer named.
		//
		// The receiver fetches that catalog from stratum0 by content hash
		// (receiver/commit_processor.cc), so the objects must be in the store
		// before the commit -- otherwise the graft downloads what is not there.
		// "Before the commit" is the whole constraint, and it does NOT imply
		// holding anything: promotion adds content-addressed objects that no
		// catalog references yet, so they are invisible to clients and harmless
		// to repeat. It publishes nothing.
		//
		// Of the two, only "before the lease" is observed by a test (the fake
		// backend samples the promotion count inside Acquire). "Before the lock"
		// rests on the placement being read, not on an assertion.
		//
		// Doing it here rather than after Phase 3 matters at scale. A
		// multi-thousand-object copy took minutes while holding a gateway lease
		// that CANNOT be renewed -- Renew returns ErrRenewalNotSupported and the
		// heartbeat then disables itself, so the copy simply burned the
		// gateway's max_lease_time -- and while holding the per-repo commit
		// lock, which the surrounding design says should cover milliseconds of
		// manifest read plus commit POST. Every other publish to that repository
		// waited behind a byte copy that needed no exclusivity at all.
		if j.StagingPrefix != "" {
			p, ok := o.CAS.(promoter)
			if !ok {
				return o.abortJob(ctx, j, fmt.Errorf(
					"staged publish needs a CAS that can promote a staging prefix "+
						"(cas.type: s3); this prepub has %T", o.CAS))
			}
			promoteStart := time.Now()
			res, promoteErr := p.PromoteFrom(ctx, j.StagingPrefix,
				o.PromoteWorkers)
			if promoteErr != nil {
				span.RecordError(promoteErr)
				return o.abortJob(ctx, j, fmt.Errorf(
					"promoting staged objects from %q: %w", j.StagingPrefix, promoteErr))
			}
			logger.Info("staged publish: promoted objects into the CAS",
				"prefix", j.StagingPrefix, "copied", res.Copied, "skipped", res.Skipped,
				"rejected", res.Rejected, "bytes", res.Bytes,
				"duration", time.Since(promoteStart).String(),
				// Without the setting the duration cannot be interpreted
				// afterwards, which is the whole point of a tunable.
				"workers", o.PromoteWorkers)

			// Record what moved. Without this a staged publish
			// reports objects=0 bytes=0 on completion and in every accounting
			// built on the job record -- which is what the first end-to-end run
			// showed: a publish of 6 objects and 102 kB
			// looking empty.
			//
			// NObjects counts everything this publish needs in the store,
			// including what deduplication already put there; NNewObjects counts
			// only what this promotion actually copied. The gap between them IS
			// the dedup hit rate, which is the number worth watching at O2
			// scale.
			//
			// NBytesRaw stays 0 and that is not an oversight: promotion moves
			// COMPRESSED objects and never sees the uncompressed sizes. Only the
			// producer knows those, and it does not report them. Recording the
			// compressed figure as if it were raw would quietly corrupt every
			// compression ratio computed downstream.
			j.NObjects = res.Copied + res.Skipped
			j.NNewObjects = res.Copied
			j.NBytesCompressed = res.Bytes

			// Confirm the CATALOG is in the store, not merely that something was
			// promoted. An empty or mistyped prefix lists nothing and copies
			// nothing WITHOUT erroring, and grafting then publishes a catalog
			// whose objects were never moved -- surfacing much later as EIO on a
			// client, a long way from the cause.
			//
			// Counting promoted objects is the weaker test and gets two cases
			// wrong: a prefix holding one unrelated object passes it, and a retry
			// whose producer has since cleaned up its prefix fails it even though
			// every object is already in the CAS. Asking after the one object the
			// graft actually needs is exact, and it is a single HEAD.
			haveCatalog, existsErr := o.CAS.Exists(ctx, j.CatalogHash)
			if existsErr != nil {
				span.RecordError(existsErr)
				return o.abortJob(ctx, j, fmt.Errorf(
					"checking the promoted catalog %s: %w", j.CatalogHash, existsErr))
			}
			if !haveCatalog {
				return o.abortJob(ctx, j, fmt.Errorf(
					"staged publish: catalog %s is not in the store after promoting %q "+
						"(copied %d, skipped %d, rejected %d) — nothing to graft",
					j.CatalogHash, j.StagingPrefix, res.Copied, res.Skipped, res.Rejected))
			}
		}

		// No-pipeline backends (local extraction, ingest relay) skipped every
		// pipeline state and are still StateIncoming. They also skipped the
		// per-repo commit lock taken inside the pipeline branch above — which
		// mattered little when such a backend was the only one on a node, but a
		// node can now serve both paths at once, and two backends committing to
		// one repository concurrently is the stale-root-hash race the lock
		// exists to prevent.
		if !commitLockHeld {
			unlockCommit, lockErr := o.acquireCommitLock(ctx, j)
			if lockErr != nil {
				span.RecordError(lockErr)
				return o.abortJob(ctx, j, lockErr)
			}
			releaseCommit = unlockCommit
			commitLockHeld = true
			logger.Info("acquired per-repo commit serialisation lock", "repo", j.Repo)
		}

		// A staged publish grafts a subtree at the lease path, exactly as a
		// pipeline publish does, so it needs the same intermediate directory
		// entries: cvmfs_receiver does not create them, and without them the
		// FUSE client returns ENOENT for any traversal through them.
		//
		// The mechanism is Phase 2.65's and is unchanged -- a directory-only
		// catalog committed for the first missing ancestor. Staged jobs simply
		// did not reach it, because both it and its call site sit inside the
		// pipeline branch. Called here for the same reason it is called there:
		// after the per-repo commit lock (so the mkdir commit and the content
		// graft are one serialised unit) and BEFORE the lease is acquired, so no
		// overlapping path lease exists when ensureParentDirs takes its own.
		if j.StagingPrefix != "" {
			if ensureErr := o.ensureParentDirs(ctx, j); ensureErr != nil {
				span.RecordError(ensureErr)
				return o.abortJob(ctx, j, ensureErr)
			}
		}

		// Transition to StateLeased so the FSM is consistent before Commit.
		// This RENAMES the job directory, so the tar path recorded at
		// submission no longer resolves — refresh it before Commit reads it.
		j.LeasedAt = time.Now()
		if err := o.transition(ctx, j, job.StateLeased); err != nil {
			span.RecordError(err)
			return o.abortJob(ctx, j, err)
		}
		if j.TarPath != "" {
			j.TarPath = filepath.Join(o.Spool.JobDir(j), "payload.tar")
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
		// The GatewayQueue fronts the GATEWAY lease client only. A job on a
		// backend that does not use the pipeline (local extraction, or the
		// ingest relay) manages its own serialisation and must not be handed a
		// gateway lease token: it would hold a real lease on the very path its
		// own publish is about to lease, and the token would then be fed to a
		// backend that cannot release it.
		if o.GatewayQueue != nil && o.leaseFor(j).NeedsPipeline() {
			token, leaseErr = o.GatewayQueue.Acquire(ctx, j.Repo, j.Path, 0)
		} else {
			token, leaseErr = o.leaseFor(j).Acquire(ctx, j.Repo, j.Path)
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
		cancelHeartbeat = o.leaseFor(j).Heartbeat(ctx, token, 10*time.Second, leaseCancel)
	} else {
		logger.Info("using pre-acquired gateway lease",
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

	// Re-derive TarPath from the job's CURRENT directory.
	//
	// The spool moves the job directory on every state transition
	// (incoming -> staging -> ... -> leased -> committing), so any absolute
	// path captured earlier is stale the moment the job advances. The pipeline
	// branch above already refreshes it after the incoming->staging rename —
	// but ONLY there, and IngestBackend.NeedsPipeline() is false, so a job on
	// the `ingest` publish path never passed through that code and carried an
	// incoming/ path all the way to the backend:
	//
	//   cvmfs_server ingest -T /data/spool/leased/<job>/payload.tar
	//   Impossible to open the archive: Failed to open '...'
	//
	// after the gateway transaction had already been opened — so it presented
	// as a broken payload rather than a path bug. Derived here, once, for every
	// backend, because the spool layout is the orchestrator's business and the
	// backends should not have to know which states rename what.
	if j.TarPath != "" {
		j.TarPath = filepath.Join(o.Spool.JobDir(j), "payload.tar")
	}

	// The graft commits against the repository's current root, read here -- late,
	// and after ensureParentDirs, so it reflects any parent-dir commit this job
	// just made. The pipeline path's fetch is gated on pipelineResult, which is
	// nil here, so it has not run.
	//
	// This read is authoritative only because the previous holder of the commit
	// lock held it until its own commit was visible on stratum0 -- the
	// serialize-until-published barrier at the end of this function. The lock
	// alone would not be enough: it was not enough until that barrier's gate was
	// widened to cover staged jobs, and before then two staged publishes in a
	// row could read a root that predated the first one's commit.
	if j.StagingPrefix != "" && o.Stratum0URL != "" {
		oldRootHash, err = cvmfscatalog.FetchManifestRootHash(leaseCtx, nil, o.Stratum0URL, j.Repo)
		if err != nil {
			span.RecordError(err)
			cancelHeartbeat()
			leaseCancel()
			return o.abortJob(ctx, j, fmt.Errorf("fetching manifest root hash: %w", err))
		}
	}

	// Build the commit request, populating fields for whichever backend is active.
	cvmfsDir := filepath.Join(o.CVMFSMount, j.Repo, j.Path)
	req := lease.CommitRequest{
		Token:          token,
		TarPath:        j.TarPath,
		CVMFSDir:       cvmfsDir,
		TagName:        j.TagName,
		TagDescription: j.TagDescription,
		// A staged job always grafts: the producer built the subtree catalog, so
		// there is nothing for DiffRec to diff against.
		DirectGraft: o.graftsAt(j),
		DirectS3:    j.DirectS3,
		ObjectList:  j.ObjectList,
		// The backend fills this in with what only it can know -- the tool's
		// own duration, the payload it handed over, the objects it confirmed.
		// nil when nothing is recording.
		Stats: o.measStats(j),
	}
	// Ingest with an object list can pre-warm: the publisher reports the data
	// objects it stored, and they are announced once the commit has succeeded.
	var confirmed []string
	if j.ObjectList && j.DirectS3 && o.preWarmFor(j) {
		req.ConfirmedObjects = &confirmed
	}
	if j.StagingPrefix != "" {
		// The producer named the catalog; the receiver downloads it by this hash.
		// Suffixed, which ingress has already checked -- the receiver refuses a
		// graft whose hash carries no catalog suffix.
		req.OldRootHash = oldRootHash
		req.NewRootHashSuffixed = j.CatalogHash
	} else if preMutexLease {
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

	// Holding the repository's slot here, so every earlier commit has landed.
	precheckStart := time.Now()
	skip, replace, preErr := o.preCommitChecks(ctx, j, &req, logger)
	o.measPrecheck(j, time.Since(precheckStart))
	if preErr == nil && replace {
		preErr = o.replaceFirst(ctx, j, &req, logger)
	}
	if preErr != nil || skip {
		held := token
		if replace { // replaceFirst released that one and may hold a new one
			held = j.LeaseToken
		}
		if held != "" {
			if abErr := o.abortLeaseDetachedErr(j, held); abErr != nil {
				logger.Warn("releasing the unused lease failed", "error", abErr)
			}
		}
		j.LeaseToken = ""
		if o.GatewayQueue != nil {
			o.GatewayQueue.NotifyRelease(j.Repo)
		}
		if preErr != nil {
			span.RecordError(preErr)
			return o.abortJob(ctx, j, preErr)
		}
		j.PublishedAt = time.Now()
		if err := o.transition(ctx, j, job.StatePublished); err != nil {
			span.RecordError(err)
			return err
		}
		o.webhookPublished(j)
		o.Obs.Metrics.JobsCompleted.Inc()
		o.measFinish(j, "already_published", nil)
		return nil
	}

	logger.Info("committing")
	commitPhaseStart := time.Now()

	var commitErr error
	if preMutexLease {
		// Catalog already uploaded in Phase 2.7.  Only the commit POST remains.
		// Type-assertion is safe: preMutexLease is only set when o.GatewayQueue != nil.
		lc := o.leaseFor(j).(*lease.Client)
		commitErr = lc.CommitFinalizeOnly(ctx, req)
	} else {
		commitErr = o.leaseFor(j).Commit(ctx, req)
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
			// Replacement is decided before the commit (preCommitChecks), on
			// the published hash; a failed commit never deletes anything.
			//
			// A DirectGraft commit is rejected with a generic "merge_error" when
			// the target subtree already exists (receiver TryGraftNestedCatalog →
			// "invalid attempt to graft nested catalog into existing directory").
			// Confirm against the published catalog and surface a clear, terminal
			// "already published" error instead of the cryptic gateway reason —
			// a package/version publishes once and is never retried.
			if o.Stratum0URL != "" && j.Path != "" &&
				strings.Contains(commitErr.Error(), "merge_error") {
				if exists, exErr := cvmfscatalog.PathExists(ctx, nil, o.Stratum0URL, j.Repo, j.Path); exErr == nil && exists {
					clearErr := Classify(ErrClassPermanent, fmt.Errorf(
						"already published: %s/%s already exists in the repository and "+
							"was not replaced: replacement happens only before the commit, "+
							"when the published hash is readable and differs, for a job that "+
							"sends replace on a node with replace_on_conflict",
						j.Repo, j.Path))
					span.RecordError(clearErr)
					logger.Error("commit rejected: target already published",
						"repo", j.Repo, "path", j.Path)
					return o.abortJob(ctx, j, clearErr)
				}
			}
			span.RecordError(commitErr)
			logger.Error("commit failed", "error", commitErr)
			return o.abortJob(ctx, j, commitErr)
		}
	}
	if replace {
		o.measConflict(j, true)
		logger.Info("replaced", "repo", j.Repo, "path", j.Path)
	}
	o.Obs.Metrics.JobPhaseDuration.WithLabelValues("commit").Observe(time.Since(commitPhaseStart).Seconds())
	o.measCommit(j, time.Since(commitPhaseStart))

	// The lease/slot is gone once Commit returns — every backend releases it,
	// successfully or not. Clearing the token stops crash recovery from later
	// "releasing" it again: PrepareRecovery aborts any job it finds carrying a token,
	// and for a slot-based backend that abort would free whichever job holds
	// the slot at that moment, not this long-finished one.
	j.LeaseToken = ""

	// Notify the gateway queue that this repo's lease has been released so any
	// goroutine waiting in GatewayQueue.Acquire wakes up immediately instead of
	// waiting for the next poll interval.
	if o.GatewayQueue != nil {
		o.GatewayQueue.NotifyRelease(j.Repo)
	}

	// Post-commit pre-warm for ingest: receivers pull the confirmed objects now,
	// ahead of their next snapshot, which then finds them in place. Off the
	// job's path (the broker round-trip can take seconds), like the published
	// broadcast. Size is the tar's, i.e. uncompressed: informational only.
	if len(confirmed) > 0 {
		go func(names []string, mctx context.Context) {
			if o.storePullManifest(mctx, j, names, j.TarSize, logger) {
				o.publishAnnounce(j, j.Repo, j.ID, j.TarSize)
			}
		}(confirmed, context.WithoutCancel(ctx))
	}

	// ── Serialize-until-published barrier ────────────────────────────────────
	// Hold the per-repo commit lock until stratum0 reflects this commit, so the
	// next package's graft sees a base that already contains the parent dirs this
	// commit created — otherwise, under rapid sequential commits, stratum0 lags
	// and the next graft fails with a spurious merge_error. cvmfs_receiver
	// produces the final merged root during the graft; we learn it by polling the
	// manifest until it advances past the base we committed against (oldRootHash),
	// which doubles as recording j.NewRootHash for S1 propagation tracking.
	//
	// A staged job needs this barrier at least as much as a pipeline job, and
	// was not getting it: subtreeResult is the pipeline's catalog build and is
	// always nil here, so the gate excluded the one publish kind that ALWAYS
	// grafts. Two staged publishes in a row would then read old_root_hash from a
	// stratum0 that had not yet caught up, and the second graft fails with the
	// spurious merge_error this barrier exists to prevent -- which the handler
	// reports as "already published" once PathExists sees the path.
	//
	// It also fills j.NewRootHash, without which the post-commit MQTT broadcast
	// is skipped and Stratum 1 receivers only learn of the publish from the
	// backstop poll.
	//
	// The gate is now "did this commit graft a subtree", which is what the
	// barrier is actually about, rather than "did the pipeline build one".
	if (subtreeResult != nil || j.StagingPrefix != "") && o.Stratum0URL != "" {
		if newRoot := o.waitForManifestPropagation(ctx, j.Repo, j.Path, oldRootHash); newRoot != "" {
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
	// This is fire-and-forget: a failed publish does not fail the job. S1
	// receivers use NewRootHash to fetch the new root catalog from S0.
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
			if err := o.Spool.WriteProvenanceRecord(j, rec.SignedPayload); err != nil {
				logger.Warn("provenance: writing signed record failed (continuing)", "job_id", j.ID, "error", err)
			}
			if err := o.Spool.WriteManifest(j); err != nil {
				logger.Warn("best-effort manifest write failed (rekor receipt)", "job_id", j.ID, "error", err)
			}
		}
	}

	// ── Webhook (async, non-fatal) ────────────────────────────────────────────
	o.webhookPublished(j)

	o.Obs.Metrics.JobsCompleted.Inc()
	o.Obs.Metrics.PublishedBytes.Add(float64(publishedBytes(j)))
	o.measFinish(j, "published", nil)
	logger.Info("job completed successfully",
		"objects", j.NObjects,
		"bytes_raw", j.NBytesRaw,
		"bytes_compressed", j.NBytesCompressed,
	)
	return nil
}

// PrepareRecovery decides the fate of a job found in a non-terminal state at
// service startup and resets it to incoming; Server.RecoverJob then runs it
// under the normal concurrency limit. It reports whether the job should run
// again; when it should not, the job has already been failed and the error
// says why. Stale transactions are aborted before the job is reset.
//
// afterCleanShutdown distinguishes the two cases. False (a crash) counts the
// attempt against MaxRecoveries, so a job that kills the service is eventually
// failed instead of crash-looping. True (an operator restart) does not: the job
// was interrupted, which is not evidence of anything wrong with it. Conflating
// them meant three routine `systemctl restart`s during one debugging session
// terminally failed every in-flight job of a 174-package build.
func (o *Orchestrator) PrepareRecovery(ctx context.Context, j *job.Job, afterCleanShutdown bool) (bool, error) {
	ctx, span := o.Obs.Tracer.Start(ctx, "orchestrator.recover")
	defer span.End()

	logger := o.Obs.Logger.With("job_id", j.ID, "state", j.State,
		"recovery_count", j.RecoveryCount, "interrupt_count", j.InterruptCount)

	// A job that was waiting for a retry was not interrupted: it resumes
	// waiting, uncounted.
	if j.State == job.StateIncoming && j.NextAttemptAt != nil {
		logger.Info("resuming a job waiting to retry",
			"attempt", j.Attempts, "next_attempt_at", j.NextAttemptAt.Format(time.RFC3339))
		return true, nil
	}

	if !afterCleanShutdown && j.RecoveryCount >= MaxRecoveries {
		err := Classify(ErrClassPermanent, fmt.Errorf("job %s has reached the maximum recovery limit (%d attempts)", j.ID, MaxRecoveries))
		span.RecordError(err)
		logger.Error("job exceeded max recovery attempts — marking as failed")
		_ = o.abortJob(ctx, j, err)
		return false, err
	}
	if afterCleanShutdown && j.InterruptCount >= MaxInterrupts {
		err := Classify(ErrClassPermanent, fmt.Errorf("job %s has been interrupted by a service restart %d times", j.ID, MaxInterrupts))
		span.RecordError(err)
		logger.Error("job interrupted too many times — marking as failed")
		_ = o.abortJob(ctx, j, err)
		return false, err
	}

	if afterCleanShutdown {
		logger.Info("recovering job interrupted by a clean restart (not counted as a failed attempt)")
	} else {
		logger.Info("recovering job")
	}

	// Release any stale transaction.  The token may have already been
	// released or expired — Abort is idempotent and errors are non-fatal here.
	if j.LeaseToken != "" {
		// Detached context: recovery runs at startup and during shutdown, where
		// the caller's ctx is routinely already cancelled — an abort issued on
		// it is a silent no-op and strands the lease until the gateway expires
		// it. Same reason as the rollback paths in ensureParentDirs.
		if releaseErr := o.abortLeaseDetachedErr(j, j.LeaseToken); releaseErr != nil {
			logger.Warn("failed to abort stale transaction during recovery (ignoring)",
				"token", j.LeaseToken, "error", releaseErr)
		}
		j.LeaseToken = ""
	}

	if err := o.Spool.ResetForRecovery(j, !afterCleanShutdown); err != nil {
		span.RecordError(err)
		return false, fmt.Errorf("resetting job for recovery: %w", err)
	}

	logger.Info("job reset to incoming — restarting")
	return true, nil
}

// webhookPublished tells the job's webhook, if any, that it was published.
// Async and non-fatal.
func (o *Orchestrator) webhookPublished(j *job.Job) {
	if o.Notify == nil || j.WebhookURL == "" {
		return
	}
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

// preCommitChecks looks at the published catalogs just before a commit, with
// the repository's slot held so they reflect every earlier commit. It returns
// skip when the job's identity is already published -- a rerun queued behind
// the original, which committing would only fail on the existing entries.
// When another build published there (IdentityHash does not match) it
// returns replace if the job asked for it and the node allows it, and an
// error otherwise, so another build's content never passes as this one.
// Otherwise it sets req.BaseExists for the ingest path. Lookup failures leave
// things as they were: commit, and ask for the nested catalog.
func (o *Orchestrator) preCommitChecks(ctx context.Context, j *job.Job, req *lease.CommitRequest, logger *slog.Logger) (skip, replace bool, err error) {
	if o.Stratum0URL == "" {
		return false, false, nil
	}
	if j.IdentityPath != "" {
		exists, exErr := pathExistsFn(ctx, nil, o.Stratum0URL, j.Repo, j.IdentityPath)
		switch {
		case exErr != nil:
			logger.Warn("identity check failed — committing anyway",
				"identity_path", j.IdentityPath, "error", exErr)
		case exists && j.IdentityHash != "":
			got, found, herr := publishedHashFn(ctx, o.Stratum0URL, j.Repo, j.IdentityPath)
			if herr != nil {
				logger.Warn("identity hash check failed — committing anyway",
					"identity_path", j.IdentityPath, "error", herr)
				break
			}
			if found && got == j.IdentityHash {
				logger.Info("already published since submission — skipping the commit",
					"identity_path", j.IdentityPath)
				return true, false, nil
			}
			// Only content recognisably another build's: a readable, different
			// hash. No hash there (a shared root, no .meta.json) never qualifies.
			if found && got != "" && o.replaces(j) {
				logger.Warn("published by another build — replacing it",
					"path", j.Path, "published_hash", got, "job_hash", j.IdentityHash)
				return false, true, nil
			}
			return false, false, Classify(ErrClassPermanent, fmt.Errorf("%s is already published by another build "+
				"(hash %q, this job %q); not overwriting it (needs replace on the job and "+
				"replace_on_conflict on the node)", j.IdentityPath, got, j.IdentityHash))
		case exists:
			logger.Info("already published since submission — skipping the commit",
				"identity_path", j.IdentityPath)
			return true, false, nil
		}
	}
	// The exact entry a second nested catalog would collide with.
	if _, ingest := o.leaseFor(j).(*lease.IngestBackend); ingest && j.Path != "" {
		marker := strings.TrimSuffix(j.Path, "/") + "/.cvmfscatalog"
		if exists, exErr := pathExistsFn(ctx, nil, o.Stratum0URL, j.Repo, marker); exErr == nil {
			req.BaseExists = exists
		}
	}
	return false, false, nil
}

// replaces reports whether j may replace what is published at its path: the
// node allows it, the job asked, and its identity is its own path with a
// hash, so only content recognisably another build's is ever replaced.
// Submission enforces the same; this keeps a spooled job from older code honest.
func (o *Orchestrator) replaces(j *job.Job) bool {
	return o.ReplaceAllowed() && j.Replace && j.IdentityHash != "" &&
		strings.Trim(j.Path, "/") != "" &&
		path.Clean(j.IdentityPath) == path.Clean(j.Path)
}

// publishedHashFn is a test seam for the published package hash.
var publishedHashFn = publishedPackageHash

// publishedPackageHash reads the package hash bits records in
// <path>/.meta.json. found is false when there is no such file.
func publishedPackageHash(ctx context.Context, stratum0URL, repo, p string) (string, bool, error) {
	data, found, err := cvmfscatalog.ReadPublishedFile(ctx, nil, stratum0URL, repo,
		strings.TrimSuffix(p, "/")+"/.meta.json")
	if err != nil || !found {
		return "", false, err
	}
	var meta struct {
		Package struct {
			Hash string `json:"hash"`
		} `json:"package"`
	}
	if json.Unmarshal(data, &meta) != nil {
		return "", true, nil
	}
	return meta.Package.Hash, true, nil
}

// pathExistsFn is a test seam; production resolves against the published
// catalogs on stratum0.
var pathExistsFn = cvmfscatalog.PathExists

// subtreeDeleter is the capability replaceFirst needs from a publish
// backend: remove a published subtree in a committed transaction of its own.
// A backend without it cannot replace.
//
// Implementing this is NOT just about being able to delete. The commit after
// the delete uses the CommitRequest built before it, under a fresh lease, so
// a backend may only implement it when all three hold — today they hold for
// IngestBackend and StagedBackend:
//
//  1. Commit is self-contained: no catalog/objects were uploaded under the
//     released lease that the commit would omit. Ingest ingests the spool
//     tar; the staged commit grafts objects already promoted into the store.
//  2. Heartbeat is a no-op by then: none is restarted for the fresh lease.
//     Run cancels the heartbeat before preCommitChecks.
//  3. Commit does not depend on OldRootHash: the delete advances the
//     repository root, so any root read before it is stale — and the graft
//     does NOT enforce old_root_hash (proven on the testbed), so the stale
//     value the request still carries is harmless.
type subtreeDeleter interface {
	DeleteSubtree(ctx context.Context, repo, relPath string) error
}

// replaceFirst deletes the subtree another build published at j's path, so
// that the commit that follows publishes this job's content in its place
// instead of failing on it. preCommitChecks decided it: the published hash
// differs, the job asked, the node allows.
//
// Ordering guarantees: the caller holds the per-repo commit serialisation
// lock, so no other job of this repository can interleave between the delete
// and the commit. The path is nevertheless absent for one revision, the
// documented cost of replacing.
func (o *Orchestrator) replaceFirst(ctx context.Context, j *job.Job,
	req *lease.CommitRequest, logger *slog.Logger) error {
	backend := o.leaseFor(j)
	deleter, ok := backend.(subtreeDeleter)
	if !ok {
		return o.cannotReplace(j, nil)
	}
	o.measConflict(j, false) // replaced only once the commit lands
	logger.Warn("deleting the subtree another build published, before the commit",
		"repo", j.Repo, "path", j.Path,
		"destroys", "the published subtree at this path only; prior revisions "+
			"keep their objects until GC")
	// Release the job's lease before deleting: the delete takes the
	// repository's slot (ingest, `cvmfs_server ingest -f`) or a gateway lease
	// on the same path (staged), which a lease still held would block. The
	// commit lock keeps the repository to this job meanwhile.
	if j.LeaseToken != "" {
		if relErr := backend.Abort(ctx, j.LeaseToken); relErr != nil {
			return fmt.Errorf("replace: could not release the lease on %s/%s "+
				"before deleting the subtree: %w", j.Repo, j.Path, relErr)
		}
		j.LeaseToken = ""
	}
	if delErr := deleter.DeleteSubtree(ctx, j.Repo, j.Path); delErr != nil {
		// A backend that implements the method but cannot do the work in this
		// deployment (the staged path without the ingest path) declines.
		if errors.Is(delErr, lease.ErrSubtreeDeleteUnsupported) {
			return o.cannotReplace(j, delErr)
		}
		return fmt.Errorf("replace: deleting the subtree another build published "+
			"at %s/%s failed: %w", j.Repo, j.Path, delErr)
	}
	token, acqErr := backend.Acquire(ctx, j.Repo, j.Path)
	if acqErr != nil {
		return fmt.Errorf("replace: subtree %s/%s deleted, but re-acquiring for "+
			"the commit failed — the path is now ABSENT until republished: %w",
			j.Repo, j.Path, acqErr)
	}
	j.LeaseToken = token
	req.Token = token
	// The subtree, and with it its nested catalog, is gone: ask for a new one.
	req.BaseExists = false
	return nil
}

// cannotReplace is the permanent failure of a job whose publish path cannot
// delete a subtree here; nothing was deleted.
func (o *Orchestrator) cannotReplace(j *job.Job, cause error) error {
	err := fmt.Errorf("%s/%s is published by another build and replace was "+
		"requested, but publish path %q cannot delete a subtree here",
		j.Repo, j.Path, j.PublishPath)
	if cause != nil {
		err = fmt.Errorf("%w: %w", err, cause)
	}
	return Classify(ErrClassPermanent, err)
}

// abortJob ends a failed attempt. A retryable failure within the retry window
// puts the job back in incoming (see retryAt) and returns err wrapped in
// ErrRetryScheduled; otherwise it records the failure, writes the manifest,
// transitions the job to StateFailed and returns err. Either way callers can
// use it in a return statement.
//
// Context independence: ctx may already be cancelled when abortJob is called.
// All cleanup I/O uses a fresh context so it is not short-circuited.
func (o *Orchestrator) abortJob(ctx context.Context, j *job.Job, err error) error {
	if next, ok := o.retryAt(j, err); ok {
		if rqErr := o.scheduleRetry(j, err, next); rqErr == nil {
			return fmt.Errorf("%w: %w", ErrRetryScheduled, err)
		} else {
			o.Obs.Logger.Error("could not requeue the job for a retry — failing it",
				"job_id", j.ID, "error", rqErr)
		}
	}
	o.Obs.Metrics.JobsFailed.Inc()
	j.LastError = truncateErr(err)

	class := ClassOf(err)
	o.Obs.Metrics.JobFailuresByClass.WithLabelValues(class.String()).Inc()

	o.Obs.Logger.Error("job failed", "job_id", j.ID, "error", err, "class", class)
	// Record BEFORE j.Error is replaced with the generic operator-facing
	// string: the measurement wants the real cause, which is the whole reason
	// these records exist instead of another grep over the service log.
	o.measFinish(j, "failed", err)
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
		if abortErr := o.leaseFor(j).Abort(cleanupCtx, j.LeaseToken); abortErr != nil {
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

	// Coarse publish: tell the build that one of its jobs is terminal, so a
	// sealed build can still reach a decision.  Without this the declared count
	// is never met and the build waits forever for a package that will never
	// arrive — with the producer long gone, nobody would notice.
	if o.isCoarse(j) {
		if mErr := buildset.MarkFailed(o.Spool.Root, j.BuildID, j.ID, ClassOf(err).String()); mErr != nil {
			o.Obs.Logger.Warn("could not mark build member failed",
				"build_id", j.BuildID, "job_id", j.ID, "error", mErr)
		}
		o.maybeAutoFinalize(j.BuildID)
	}

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

// Retry backoff: the first retry after retryBase, doubling up to retryMax.
// Variables only so tests can shorten them.
var (
	retryBase = time.Minute
	retryMax  = 30 * time.Minute
)

// retryAt decides whether a failed attempt is retried, and when. Not when
// retries are off, for a coarse member or finalize (their build accounting
// has no notion of a retry), for an operator abort, for a permanent failure,
// or when the next attempt would fall outside the retry window.
func (o *Orchestrator) retryAt(j *job.Job, err error) (time.Time, bool) {
	if o.RetryWindow <= 0 || o.isCoarse(j) || j.Finalize || job.IsTerminal(j.State) {
		return time.Time{}, false
	}
	if _, aborted := o.cancelled.Load(j.ID); aborted || isPermanent(err) {
		return time.Time{}, false
	}
	delay := retryMax
	if j.Attempts < 5 { // 1, 2, 4, 8, 16 minutes, then the cap
		delay = min(retryBase<<j.Attempts, retryMax)
	}
	next := time.Now().Add(delay)
	if next.After(j.CreatedAt.Add(o.RetryWindow)) {
		return time.Time{}, false
	}
	return next, true
}

// scheduleRetry releases what the failed attempt held and puts the job back
// in incoming, due at next. The job keeps its payload.
func (o *Orchestrator) scheduleRetry(j *job.Job, err error, next time.Time) error {
	if j.LeaseToken != "" {
		if abErr := o.abortLeaseDetachedErr(j, j.LeaseToken); abErr != nil {
			o.Obs.Logger.Error("lease abort failed — stale lease left on gateway",
				"job_id", j.ID, "token", j.LeaseToken, "error", abErr)
		}
		if o.GatewayQueue != nil {
			o.GatewayQueue.NotifyRelease(j.Repo)
		}
	}
	j.Attempts++
	j.LastError = truncateErr(err)
	j.NextAttemptAt = &next
	if rqErr := o.Spool.Requeue(j); rqErr != nil {
		j.Attempts-- // not retried after all: abortJob fails it
		j.NextAttemptAt = nil
		return rqErr
	}
	o.measFinish(j, "retry", err)
	o.Obs.Logger.Warn("job attempt failed — will retry", "job_id", j.ID,
		"attempt", j.Attempts, "next_attempt_at", next.Format(time.RFC3339), "error", err)
	if o.Notify != nil {
		o.Notify.Publish(notify.Event{JobID: j.ID, State: job.StateIncoming, Error: j.LastError, Time: time.Now()})
	}
	return nil
}

// Aborted reports whether an operator asked to abort the job.
func (o *Orchestrator) Aborted(id string) bool {
	_, ok := o.cancelled.Load(id)
	return ok
}

// truncateErr keeps an error message to a size fit for a manifest.
func truncateErr(err error) string {
	const max = 4000
	s := err.Error()
	if len(s) > max {
		s = s[:max] + "…"
	}
	return s
}

// WaitForAttempt blocks until a job waiting to retry is due. It returns false
// when ctx ends first; the job then stays in incoming for whoever runs next.
func WaitForAttempt(ctx context.Context, j *job.Job) bool {
	if j.NextAttemptAt == nil {
		return ctx.Err() == nil
	}
	t := time.NewTimer(time.Until(*j.NextAttemptAt))
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// leaseAbortTimeout bounds a rollback issued on a detached context.
// Matches abortJob's cleanupCtx budget: one number for "abort a lease",
// and short enough to fit inside the 30s shutdown budget rather than
// guaranteeing it cannot complete there.
const leaseAbortTimeout = 30 * time.Second

// abortLeaseDetached releases a lease using a FRESH context.
//
// The usual reason to be rolling back is that the job's context is already
// cancelled or past its deadline — and an abort issued on that context is a
// silent no-op: exec.Cmd.Start returns ctx.Err() before forking, and an HTTP
// abort fails the same way. The lease then sits until the gateway expires it,
// blocking the repository. abortJob already takes this precaution; these paths
// did not, which mattered little while a cancelled cvmfs_server never returned
// at all, and matters now that the process group is killed on cancel.
func (o *Orchestrator) abortLeaseDetached(j *job.Job, token string) {
	if err := o.abortLeaseDetachedErr(j, token); err != nil {
		o.Obs.Logger.Warn("could not abort lease during rollback",
			"repo", j.Repo, "token", token, "error", err)
	}
}

// abortLeaseDetachedErr is abortLeaseDetached for callers that report the error
// themselves.
func (o *Orchestrator) abortLeaseDetachedErr(j *job.Job, token string) error {
	ctx, cancel := context.WithTimeout(context.Background(), leaseAbortTimeout)
	defer cancel()
	return o.leaseFor(j).Abort(ctx, token)
}
