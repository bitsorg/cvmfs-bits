// Package api orchestrates the HTTP server and job lifecycle management for the CVMFS pre-publisher.
// It coordinates the pipeline (unpack, compress, deduplicate), distribution to Stratum 1 replicas,
// gateway lease acquisition, and publish operations.
package api

import (
	"context"
	"fmt"
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
// It coordinates pipeline stages, distribution to Stratum 1 endpoints, gateway lease acquisition,
// and commit operations. It also manages webhook delivery and provenance recording.
type Orchestrator struct {
	// Spool manages persistent job state and directory transitions.
	Spool *spool.Spool
	// CAS is the content-addressable storage backend.
	CAS cas.Backend
	// Lease is the gateway lease client for managing publish locks.
	Lease *lease.Client
	// Distribute contains configuration for Stratum 1 push distribution.
	Distribute *distribute.Config
	// Pipeline contains configuration for the compression/dedup pipeline.
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

// Run executes the job through all pipeline stages: compression, deduplication,
// upload to CAS, distribution to Stratum 1 endpoints, lease acquisition, and
// final gateway commit.
//
// Lease-window optimization: The gateway lease is acquired AFTER the full
// compress→upload→distribute sequence completes. The lease window covers only:
//   - SubmitPayload (send catalog hash + object list to the gateway)
//   - Release with commit=true
//
// This reduces the lease hold time from O(minutes) to O(seconds), allowing other
// publishers on the same sub-path to work with far less contention.
//
// The job state is transitioned through the FSM at each stage, and provenance
// records are submitted to Rekor (non-fatal) and webhooks are delivered
// asynchronously to the registered URL when the job reaches a terminal state.
func (o *Orchestrator) Run(ctx context.Context, j *job.Job) error {
	ctx, span := o.Obs.Tracer.Start(ctx, "orchestrator.run")
	defer span.End()

	logger := o.Obs.Logger.With("job_id", j.ID)

	// ── Phase 1: compress + upload to CAS (no lease held) ──────────────────
	logger.Info("staging", "tar", j.TarPath)
	if err := o.transition(ctx, j, job.StateStaging); err != nil {
		span.RecordError(err)
		return o.abortJob(ctx, j, err)
	}

	logger.Info("running pipeline")
	pipelineResult, err := pipeline.Run(ctx, j.TarPath, o.Pipeline)
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

	// ── Phase 2: pre-push objects to Stratum 1 (no lease held) ─────────────
	//
	// Distribution is triggered when either:
	//   (a) HTTP announce path: Distribute != nil && len(Endpoints) > 0
	//   (b) MQTT path: Distribute != nil && BrokerConfig is set
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
		defer distLog.Close() // ensure file is flushed even on early return

		// Copy the shared distribute config and inject per-job fields so that
		// concurrent jobs running the same Orchestrator do not race on Repo/TotalBytes.
		distCfg := *o.Distribute
		distCfg.Repo = j.Repo
		distCfg.TotalBytes = pipelineResult.NBytesComp

		confirmed, total, distErr := distribute.Distribute(
			ctx, j.ID, pipelineResult.ObjectHashes, o.CAS, distCfg, distLog)
		if distErr != nil {
			logger.Warn("distribution failed or quorum not reached",
				"error", distErr, "confirmed", confirmed, "total", total)
			// Continue — the gateway will serve clients from CAS directly if
			// Stratum 1s are incomplete; quorum enforcement is a policy decision.
		}
	}

	// ── Phase 3: acquire lease — window starts here ─────────────────────────
	logger.Info("acquiring lease", "repo", j.Repo, "path", j.Path)
	if err := o.transition(ctx, j, job.StateLeased); err != nil {
		span.RecordError(err)
		return o.abortJob(ctx, j, err)
	}

	// Use Path for sub-path lease scoping when set; fall back to Repo for
	// backwards compatibility with jobs that pre-date the Path field.
	leasePath := j.Path
	if leasePath == "" {
		leasePath = j.Repo
	}
	l, err := o.Lease.Acquire(ctx, j.Repo, leasePath)
	if err != nil {
		span.RecordError(err)
		logger.Error("failed to acquire lease", "error", err)
		return o.abortJob(ctx, j, err)
	}

	j.LeaseToken = l.Token
	if err := o.Spool.WriteManifest(j); err != nil {
		logger.Warn("best-effort manifest write failed (lease token)", "job_id", j.ID, "error", err)
	}

	// leaseCtx is a child of ctx that the heartbeat goroutine cancels if it
	// detects maxConsecutiveHeartbeatFailures back-to-back renewal errors.
	// Cancelling leaseCtx causes SubmitPayload to return immediately, aborting
	// the publish before it blocks on a gateway that has already evicted the
	// expired lease.  releaseJob still uses the parent ctx so cleanup proceeds
	// even after leaseCtx is cancelled.
	leaseCtx, leaseCancel := context.WithCancel(ctx)
	defer leaseCancel()

	// Heartbeat keeps the lease alive during SubmitPayload.  It is idempotent —
	// safe to call the cancel function multiple times.
	cancelHeartbeat := o.Lease.Heartbeat(ctx, l, 10*time.Second, leaseCancel)
	defer cancelHeartbeat()

	// ── Phase 4: commit — lease window ─────────────────────────────────────
	logger.Info("submitting payload to gateway")
	if err := o.transition(leaseCtx, j, job.StateCommitting); err != nil {
		span.RecordError(err)
		cancelHeartbeat()
		leaseCancel()
		_ = o.releaseJob(ctx, j, false)
		return o.abortJob(ctx, j, err)
	}

	if err := o.Lease.SubmitPayload(
		leaseCtx, l.Token, pipelineResult.CatalogHash, pipelineResult.ObjectHashes,
	); err != nil {
		span.RecordError(err)
		logger.Error("failed to submit payload", "error", err)
		cancelHeartbeat()
		leaseCancel()
		_ = o.releaseJob(ctx, j, false)
		return o.abortJob(ctx, j, err)
	}

	cancelHeartbeat() // stop renewal before committing
	leaseCancel()     // release leaseCtx resources
	logger.Info("committing to gateway")
	if err := o.releaseJob(ctx, j, true); err != nil {
		span.RecordError(err)
		logger.Error("failed to commit", "error", err)
		return o.abortJob(ctx, j, err)
	}
	// ── Lease window ends here ───────────────────────────────────────────────

	if err := o.transition(ctx, j, job.StatePublished); err != nil {
		span.RecordError(err)
		return err
	}

	// Submit provenance record to Rekor (non-fatal — a failed submission only
	// means the job lacks a transparency log receipt, not that it was lost).
	if o.Provenance != nil && j.Provenance != nil {
		rec := &provenance.Record{
			JobID:        j.ID,
			Repo:         j.Repo,
			Path:         j.Path,
			CatalogHash:  pipelineResult.CatalogHash,
			ObjectHashes: pipelineResult.ObjectHashes,
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
			logger.Warn("provenance: Rekor submission failed (continuing)",
				"error", submitErr)
		} else if rec.Submitted() {
			// Persist the Rekor receipt in the job manifest.
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

	// Fire webhook asynchronously so a slow receiver does not delay the caller.
	// Use a fresh context: runCtx is cancelled by defer cancel() in server.go
	// the instant Run() returns, which would abort the webhook before it fires.
	// webhookWg is tracked so Server.Shutdown can wait for delivery to finish.
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

func (o *Orchestrator) releaseJob(ctx context.Context, j *job.Job, commit bool) error {
	return o.Lease.Release(ctx, j.LeaseToken, commit)
}

// Recover attempts to re-process a job found in a non-terminal state at service startup.
// This handles crash recovery: if a previous service instance crashed mid-flight, jobs
// are reset to the incoming state and reprocessed. After MaxRecoveries reset attempts,
// jobs are marked as failed to prevent indefinite retry loops.
//
// Recovery also releases any stale gateway lease and clears the recovery error flag
// before calling Run to reprocess the job.
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

	// Release any stale lease.  The token may have already expired on the
	// gateway side — ignore errors.
	if j.LeaseToken != "" {
		if releaseErr := o.Lease.Release(ctx, j.LeaseToken, false); releaseErr != nil {
			logger.Warn("failed to release stale lease during recovery (ignoring)",
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

// abortJob records the failure, writes the manifest, and transitions the job to StateFailed.
// It always returns its err argument so callers can use it in a return statement.
//
// Context independence: ctx may already be cancelled when abortJob is called
// (e.g. abort signal fired, cancelling runCtx, which caused the in-progress operation
// to fail with context.Canceled). All cleanup I/O—lease release, state transition,
// webhook—creates a fresh context so they are not short-circuited by the cancelled
// job context. This ensures the job is properly transitioned to a terminal state
// and the gateway lease is released even after context cancellation.
func (o *Orchestrator) abortJob(ctx context.Context, j *job.Job, err error) error {
	o.Obs.Metrics.JobsFailed.Inc()

	class := ClassOf(err)
	o.Obs.Metrics.JobFailuresByClass.WithLabelValues(class.String()).Inc()

	o.Obs.Logger.Error("job failed", "job_id", j.ID, "error", err, "class", class)
	j.Error = "job processing failed — see service logs for details"
	j.State = job.StateFailed
	_ = o.Spool.WriteManifest(j)

	// cleanupCtx is independent of the job context so that a cancelled job
	// context (abort signal) does not prevent the lease from being released
	// or the terminal state transition from being recorded.
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cleanupCancel()

	if j.LeaseToken != "" {
		_ = o.Lease.Release(cleanupCtx, j.LeaseToken, false)
	}

	if !job.IsTerminal(j.State) {
		_ = o.Spool.Transition(cleanupCtx, j, job.StateFailed)
	}

	// Notify subscribers of the failure.
	if o.Notify != nil {
		o.Notify.Publish(notify.Event{
			JobID: j.ID,
			State: job.StateFailed,
			Error: j.Error,
			Time:  time.Now(),
		})
		// Webhook uses a fresh context for the same reason as cleanupCtx above.
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
