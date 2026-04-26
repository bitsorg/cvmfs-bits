// Package api orchestrates the HTTP server and job lifecycle management for the CVMFS pre-publisher.
// It coordinates the pipeline (unpack, compress, deduplicate), distribution to Stratum 1 replicas,
// gateway lease acquisition, and publish operations.
package api

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
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
		Token:    token,
		TarPath:  j.TarPath,
		CVMFSDir: cvmfsDir,
	}
	if pipelineResult != nil {
		req.CatalogHash = pipelineResult.CatalogHash
		req.ObjectHashes = pipelineResult.ObjectHashes
		req.ObjectStore = o.CAS
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
		if pipelineResult != nil {
			catalogHash = pipelineResult.CatalogHash
			objectHashes = pipelineResult.ObjectHashes
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
	j.State = job.StateFailed
	_ = o.Spool.WriteManifest(j)

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

	if !job.IsTerminal(j.State) {
		_ = o.Spool.Transition(cleanupCtx, j, job.StateFailed)
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
