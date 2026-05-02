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
	// Distribute contains configuration for Stratum 1 push distribution.
	// nil disables distribution (typical for local mode).
	Distribute *distribute.Config
	// Pipeline contains configuration for the compression/dedup pipeline.
	// Only used when Lease.NeedsPipeline() returns true.
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

	// commitMu serialises the ensureAncestors + catalog-merge + commit phase
	// per repository.  Only one job per repo may be in this critical section
	// at a time.
	//
	// Background: both ensureAncestors and the main Merge download the current
	// manifest and record its hash as old_root_hash.  If two jobs for the same
	// repo run these steps concurrently, both see the same old_root_hash.  The
	// first commit updates the manifest to a new hash; the second commit then
	// presents a stale old_root_hash to the gateway, which spawns a second
	// cvmfs_receiver that either conflicts with the first or returns an error.
	// In the testbed the receiver is observed to block indefinitely in this
	// case, causing the job to hang in StateCommitting forever.
	//
	// Serialising within this section preserves concurrent pipeline throughput
	// (compress / distribute) while ensuring each commit sees a fresh manifest.
	commitMu sync.Map // map[string]*sync.Mutex — keyed by repo name
}

// repoMutex returns the per-repo mutex for repo, creating it on first call.
// The same *sync.Mutex is returned for every call with the same repo string.
func (o *Orchestrator) repoMutex(repo string) *sync.Mutex {
	v, _ := o.commitMu.LoadOrStore(repo, &sync.Mutex{})
	return v.(*sync.Mutex)
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
func (o *Orchestrator) Run(ctx context.Context, j *job.Job) error {
	ctx, span := o.Obs.Tracer.Start(ctx, "orchestrator.run")
	defer span.End()

	logger := o.Obs.Logger.With("job_id", j.ID)

	// Invariant: gateway mode requires a non-nil CAS backend.  Catch
	// misconfiguration before touching the spool or network.
	if o.Lease.NeedsPipeline() && o.CAS == nil {
		err := fmt.Errorf("misconfiguration: gateway mode requires a non-nil CAS backend")
		span.RecordError(err)
		return o.abortJob(ctx, j, err)
	}

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
		var err error
		pipelineResult, err = pipeline.Run(ctx, j.TarPath, o.Pipeline)
		if err != nil {
			span.RecordError(err)
			logger.Error("pipeline failed", "error", err)
			return o.abortJob(ctx, j, err)
		}

		j.NObjects = len(pipelineResult.ObjectHashes)
		j.NBytesRaw = pipelineResult.NBytesRaw
		j.NBytesCompressed = pipelineResult.NBytesComp
		if err := o.Spool.WriteManifest(j); err != nil {
			logger.Warn("best-effort manifest write failed", "job_id", j.ID, "error", err)
		}

		if err := o.transition(ctx, j, job.StateUploading); err != nil {
			span.RecordError(err)
			return o.abortJob(ctx, j, err)
		}

		// Pre-push to Stratum 1s before acquiring the lease to keep the lease
		// window as short as possible.
		shouldDistribute := o.Distribute != nil &&
			(len(o.Distribute.Endpoints) > 0 ||
				(o.Distribute.BrokerConfig != nil && o.Distribute.BrokerConfig.BrokerURL != ""))
		if shouldDistribute {
			logger.Info("pre-pushing to Stratum 1s before lease acquire",
				"endpoints", len(o.Distribute.Endpoints),
				"mqtt", o.Distribute.BrokerConfig != nil && o.Distribute.BrokerConfig.BrokerURL != "")

			if err := o.transition(ctx, j, job.StateDistributing); err != nil {
				span.RecordError(err)
				return o.abortJob(ctx, j, err)
			}

			distLog := distribute.OpenDistLog(o.Spool.JobDir(j) + "/dist.log")
			defer distLog.Close()

			distCfg := *o.Distribute
			distCfg.Repo = j.Repo
			distCfg.TotalBytes = pipelineResult.NBytesComp

			confirmed, total, distErr := distribute.Distribute(
				ctx, j.ID, pipelineResult.ObjectHashes, o.CAS, distCfg, distLog)
			if distErr != nil {
				logger.Warn("distribution failed or quorum not reached",
					"error", distErr, "confirmed", confirmed, "total", total)
				// Continue — gateway serves clients from CAS directly if S1s are
				// incomplete; quorum enforcement is a policy decision.
			}
		}
	} else {
		logger.Info("local publish mode — skipping pipeline, tar will be extracted during Commit")
	}

	// ── Phase 2.5–4: per-repo serialisation ─────────────────────────────────
	// Acquire a per-repo mutex before ensureAncestors so that only ONE job per
	// repo is in the manifest-download → merge → commit critical section at a
	// time.  This prevents two concurrent jobs from both downloading the same
	// old manifest hash, producing diverging new catalogs, and then racing to
	// commit — a situation where the second commit presents a stale
	// old_root_hash to the gateway receiver, which in the testbed causes the
	// receiver subprocess to block indefinitely (hang in StateCommitting).
	//
	// Pipeline stages (compress / distribute) run freely before this point, so
	// the only serialised work is the gateway round-trips (ensureAncestors
	// root-lease commit + main lease acquire + merge + commit), which are
	// latency-bound by cvmfs_receiver anyway.
	if o.Lease.NeedsPipeline() {
		repoMu := o.repoMutex(j.Repo)
		repoMu.Lock()
		defer repoMu.Unlock()
		logger.Info("acquired per-repo commit serialisation lock", "repo", j.Repo)
	}

	// ── Phase 2.5: ensure ancestor directories exist (gateway mode) ──────────
	// For a nested lease path like "test/smoke", the CVMFS receiver needs the
	// parent directory "/test" to already exist in the writable catalog when it
	// calls GraftNestedCatalog("/test/smoke").  WritableCatalogManager::FindCatalog
	// calls LookupPath("/test") which panics if the entry is absent.
	//
	// Fix: before acquiring the main lease, do a preliminary root-level publish
	// (LeasePath="") that adds synthetic directory entries for every intermediate
	// path component.  After this commit the repo's root catalog contains "/test",
	// so the main publish succeeds.
	if pipelineResult != nil && o.Stratum0URL != "" && strings.Contains(j.Path, "/") {
		if ensureErr := o.ensureAncestors(ctx, j); ensureErr != nil {
			span.RecordError(ensureErr)
			return o.abortJob(ctx, j, ensureErr)
		}
	}

	// ── Phase 3: acquire lease / open transaction ─────────────────────────────
	logger.Info("acquiring lease", "repo", j.Repo, "path", j.Path)
	if err := o.transition(ctx, j, job.StateLeased); err != nil {
		span.RecordError(err)
		return o.abortJob(ctx, j, err)
	}

	// j.Path == "" means a root-level repo lease; acquireLease handles that by
	// constructing the path as "repo/" which is what the gateway expects.
	token, err := o.Lease.Acquire(ctx, j.Repo, j.Path)
	if err != nil {
		span.RecordError(err)
		logger.Error("failed to acquire lease", "error", err)
		return o.abortJob(ctx, j, err)
	}

	j.LeaseToken = token
	if err := o.Spool.WriteManifest(j); err != nil {
		logger.Warn("best-effort manifest write failed (lease token)", "job_id", j.ID, "error", err)
	}

	// leaseCtx lets the heartbeat abort the publish if consecutive renewals
	// fail (gateway mode only; the no-op heartbeat never calls onExpire in
	// local mode).
	leaseCtx, leaseCancel := context.WithCancel(ctx)
	defer leaseCancel()

	cancelHeartbeat := o.Lease.Heartbeat(ctx, token, 10*time.Second, leaseCancel)
	defer cancelHeartbeat()

	// ── Phase 3.5: catalog merge (gateway mode only) ──────────────────────────
	// Merge happens AFTER lease acquisition so that the manifest/catalog we
	// download reflects the version we are merging onto — no other publisher
	// can commit to the same path while we hold the lease.
	//
	// The merge: fetches the current root manifest → downloads the root SQLite
	// catalog → applies the new entries → finalises (compress+hash) → writes
	// compressed catalog(s) to the spool job directory → uploads them to CAS.
	var mergeResult *cvmfscatalog.MergeResult
	if pipelineResult != nil && o.Stratum0URL != "" {
		logger.Info("merging catalog", "repo", j.Repo, "path", j.Path)
		mergeResult, err = cvmfscatalog.Merge(leaseCtx, cvmfscatalog.MergeConfig{
			Stratum0URL:   o.Stratum0URL,
			RepoName:      j.Repo,
			LeasePath:     j.Path,
			TempDir:       o.Spool.JobDir(j),
			DirtabContent: pipelineResult.DirtabContent,
		}, pipelineResult.CatalogEntries)
		if err != nil {
			span.RecordError(err)
			logger.Error("catalog merge failed", "error", err)
			cancelHeartbeat()
			leaseCancel()
			return o.abortJob(ctx, j, fmt.Errorf("catalog merge: %w", err))
		}

		// Upload the merged catalog file(s) to the CAS so SubmitPayload can
		// stream them to the gateway.  Finalize() wrote each catalog to
		// TempDir/data/XY/hashC (CVMFS on-disk format).
		//
		// The CAS key includes the 'C' content-type suffix ("sha1C", 41 chars)
		// so that SubmitPayload can propagate the suffix into the ObjectPack C
		// line.  The receiver's LocalUploader then stores the object at
		// data/XY/sha1C, and CommitProcessor can find it there when it fetches
		// the new root catalog from stratum0.
		for _, catHash := range mergeResult.AllCatalogHashes {
			// Path written by cvmfscatalog.Finalize: data/XY/hash + "C"
			catFilePath := filepath.Join(o.Spool.JobDir(j),
				cvmfshash.ObjectPath(catHash)+"C")
			f, openErr := os.Open(catFilePath)
			if openErr != nil {
				cancelHeartbeat()
				leaseCancel()
				return o.abortJob(ctx, j,
					fmt.Errorf("opening merged catalog %s: %w", catHash, openErr))
			}
			fi, statErr := f.Stat()
			if statErr != nil {
				f.Close()
				cancelHeartbeat()
				leaseCancel()
				return o.abortJob(ctx, j,
					fmt.Errorf("stat merged catalog %s: %w", catHash, statErr))
			}
			// Use "sha1C" (with 'C' suffix) as the CAS key so that
			// ObjectPath("sha1C") resolves to data/XY/sha1C — the same path
			// where Finalize wrote the compressed catalog file.
			putErr := o.CAS.Put(leaseCtx, catHash+"C", f, fi.Size())
			closeErr := f.Close()
			if putErr != nil {
				cancelHeartbeat()
				leaseCancel()
				return o.abortJob(ctx, j,
					fmt.Errorf("uploading merged catalog %s: %w", catHash, putErr))
			}
			if closeErr != nil {
				logger.Warn("merged catalog file close error (upload already complete)",
					"hash", catHash, "error", closeErr)
			}
		}
		logger.Info("catalog merge complete",
			"old_root", mergeResult.OldRootHash,
			"new_root", mergeResult.NewRootHash,
			"catalogs", len(mergeResult.AllCatalogHashes))
	} else if pipelineResult != nil && o.Stratum0URL == "" {
		logger.Warn("Stratum0URL not configured — skipping catalog merge; " +
			"commit will use empty old_root_hash (only correct for initial publish)")
	}

	// ── Phase 4: commit ───────────────────────────────────────────────────────
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
	}
	if pipelineResult != nil {
		// File object hashes (catalog hashes added below from merge result).
		req.ObjectHashes = pipelineResult.ObjectHashes
		req.ObjectStore = o.CAS
	}
	if mergeResult != nil {
		// Catalog hashes go into ObjectHashes so SubmitPayload uploads them.
		// The root catalog goes last (Merge returns leaf-first, root-last).
		// Each catalog hash gets the 'C' content-type suffix so SubmitPayload
		// can propagate it to the ObjectPack C line, causing the receiver to
		// store each catalog at data/XY/sha1C (CVMFS catalog content-type path).
		for _, h := range mergeResult.AllCatalogHashes {
			req.ObjectHashes = append(req.ObjectHashes, h+"C")
		}
		req.OldRootHash = mergeResult.OldRootHash
		req.NewRootHashSuffixed = mergeResult.NewRootHashSuffixed
	}

	cancelHeartbeat() // stop renewal before committing (idempotent; defer fires again at return)
	leaseCancel()     // release leaseCtx resources early; Commit uses the parent ctx

	logger.Info("committing")
	if commitErr := o.Lease.Commit(ctx, req); commitErr != nil {
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

	if err := o.transition(ctx, j, job.StatePublished); err != nil {
		span.RecordError(err)
		return err
	}

	// ── Provenance (non-fatal) ────────────────────────────────────────────────
	if o.Provenance != nil && j.Provenance != nil {
		var catalogHash string
		var objectHashes []string
		if mergeResult != nil {
			catalogHash = mergeResult.NewRootHash // root catalog hash (plain hex)
			objectHashes = append(objectHashes, mergeResult.AllCatalogHashes...)
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
	return o.Run(ctx, j)
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

// ensureAncestors ensures that every intermediate directory component of
// j.Path exists in the repository before the main publish.
//
// Background: For a lease path "test/smoke" the CVMFS receiver maps this to
// the catalog-relative path "/test/smoke".  When grafting the nested catalog
// it calls GraftNestedCatalog("test/smoke") which internally needs the parent
// directory "/test" to already exist in the writable catalog.  On first
// publish the repo is empty so that lookup panics.
//
// Fix: acquire a root-level lease, run a Merge that upserts synthetic
// directory entries for each intermediate path component (e.g. "/test"),
// and commit.  After this preliminary publish the repo's root catalog
// contains "/test" and the main publish succeeds.
//
// This is only called when j.Path contains at least one "/" (depth > 1) and
// Stratum0URL is configured (gateway mode).
func (o *Orchestrator) ensureAncestors(ctx context.Context, j *job.Job) error {
	logger := o.Obs.Logger.With("job_id", j.ID, "phase", "ensure_ancestors")

	// Build the list of intermediate path components.
	// For "test/smoke" → ["/test"]
	// For "a/b/c"      → ["/a", "/a/b"]
	parts := strings.Split(strings.Trim(j.Path, "/"), "/")
	now := time.Now().Unix()
	ancestors := make([]cvmfscatalog.Entry, 0, len(parts)-1)
	for i := 1; i < len(parts); i++ {
		absPath := "/" + strings.Join(parts[:i], "/")
		ancestors = append(ancestors, cvmfscatalog.Entry{
			FullPath: absPath,
			// Name is set by Merge's path-rewriting step (path.Base(absPath)).
			Mode:  fs.ModeDir | 0o755,
			Size:  4096,
			Mtime: now,
			UID:   0,
			GID:   0,
		})
	}
	logger.Info("adding ancestor directories via root-level publish",
		"path", j.Path, "ancestors", len(ancestors))

	// Use a dedicated sub-directory so ancestor catalogs don't collide with
	// the main merge catalogs written to JobDir later.
	ancTempDir := filepath.Join(o.Spool.JobDir(j), "ancestors")
	if mkErr := os.MkdirAll(ancTempDir, 0o750); mkErr != nil {
		return fmt.Errorf("ensure_ancestors: creating temp dir: %w", mkErr)
	}

	// Acquire a root-level lease so the merge sees the latest repo manifest
	// and no concurrent publisher can modify the root catalog under us.
	logger.Info("acquiring root-level lease for ancestor publish")
	ancToken, leaseErr := o.Lease.Acquire(ctx, j.Repo, "")
	if leaseErr != nil {
		return fmt.Errorf("ensure_ancestors: acquiring root lease: %w", leaseErr)
	}

	// Ensure the lease is released (aborted) on any error path, including
	// context cancellation and a failed Commit.
	//
	// We use context.Background() for the Abort call so that a cancelled ctx
	// does not prevent the DELETE /api/v1/leases/<token> request from being
	// sent — failing to abort leaves the lease held until gateway expiry and
	// blocks subsequent publishers on the same repo.
	committed := false
	defer func() {
		if !committed {
			if abortErr := o.Lease.Abort(context.Background(), ancToken); abortErr != nil {
				logger.Warn("ensure_ancestors: failed to abort ancestor lease on error path",
					"token", ancToken, "error", abortErr)
			}
		}
	}()

	// Merge the synthetic ancestor entries into the root catalog.
	// LeasePath="" → all paths are reportable → AddDirectory is called for
	// each ancestor by the receiver's CatalogMergeTool.
	ancMerge, mergeErr := cvmfscatalog.Merge(ctx, cvmfscatalog.MergeConfig{
		Stratum0URL: o.Stratum0URL,
		RepoName:    j.Repo,
		LeasePath:   "",
		TempDir:     ancTempDir,
	}, ancestors)
	if mergeErr != nil {
		return fmt.Errorf("ensure_ancestors: catalog merge: %w", mergeErr)
	}

	// Upload the resulting catalog(s) to the CAS.
	for _, catHash := range ancMerge.AllCatalogHashes {
		catFilePath := filepath.Join(ancTempDir, cvmfshash.ObjectPath(catHash)+"C")
		f, openErr := os.Open(catFilePath)
		if openErr != nil {
			return fmt.Errorf("ensure_ancestors: opening catalog %s: %w", catHash, openErr)
		}
		fi, statErr := f.Stat()
		if statErr != nil {
			f.Close()
			return fmt.Errorf("ensure_ancestors: stat catalog %s: %w", catHash, statErr)
		}
		putErr := o.CAS.Put(ctx, catHash+"C", f, fi.Size())
		f.Close()
		if putErr != nil {
			return fmt.Errorf("ensure_ancestors: uploading catalog %s: %w", catHash, putErr)
		}
	}

	// Commit the preliminary root-level publish.
	ancReq := lease.CommitRequest{
		Token:               ancToken,
		ObjectStore:         o.CAS,
		OldRootHash:         ancMerge.OldRootHash,
		NewRootHashSuffixed: ancMerge.NewRootHashSuffixed,
	}
	for _, h := range ancMerge.AllCatalogHashes {
		ancReq.ObjectHashes = append(ancReq.ObjectHashes, h+"C")
	}

	logger.Info("committing ancestor publish",
		"old_root", ancMerge.OldRootHash,
		"new_root", ancMerge.NewRootHashSuffixed)
	if commitErr := o.Lease.Commit(ctx, ancReq); commitErr != nil {
		// defer will abort the lease.
		return fmt.Errorf("ensure_ancestors: commit failed: %w", commitErr)
	}

	committed = true // disarm the deferred abort
	logger.Info("ancestor publish complete")
	return nil
}
