// worker.go — per-endpoint distribution worker with retry and backoff.
//
// Multiple worker goroutines share a single *endpointWorker; they compete for
// items on the queue channel.  Each goroutine is independent: it calls
// processWithRetry which retries with exponential backoff until the item is
// delivered, the retry budget is exhausted, or ctx is cancelled (shutdown).
package distribute

import (
	"context"
	"sort"
	"time"

	"cvmfs.io/prepub/internal/cas"
)

// endpointWorker drives distribution to a single Stratum 1 endpoint.
// Multiple goroutines launched by Manager.Start share the same struct; they
// all pull from the same queue channel, achieving per-endpoint parallelism.
type endpointWorker struct {
	endpoint string
	cfg      Config
	cas      cas.Backend

	// queue is the in-memory work channel; capacity == cfg.queueDepth().
	// When closed, workers drain remaining items and then exit.
	queue chan WorkItem

	// spoolDir is the persistent queue directory; empty disables persistence.
	spoolDir string

	// manager is the parent Manager that created this worker.
	// reportEndpoint is called when the worker finishes an item.
	manager *Manager
}

// run is the main goroutine loop.  It drains the queue until ctx is cancelled.
func (w *endpointWorker) run(ctx context.Context) {
	for {
		select {
		case item, ok := <-w.queue:
			if !ok {
				return // queue channel closed — no more work
			}
			w.processWithRetry(ctx, item)

		case <-ctx.Done():
			return
		}
	}
}

// processWithRetry delivers item to the endpoint, retrying with exponential
// backoff on failure.  Returns when:
//   - the item is successfully delivered (reports success to manager), or
//   - WorkerMaxAttempts is exceeded (reports failure to manager), or
//   - ctx is cancelled (leaves the spool file for the next restart; no report).
func (w *endpointWorker) processWithRetry(ctx context.Context, item WorkItem) {
	// Sort hashes largest-first so the most expensive transfers run early.
	// Smaller objects fill the tail and complete quickly, keeping the worker
	// busy without stalling the job-completion signal behind one huge file.
	// We sort once here (before the retry loop) so retries reuse the same order.
	sortHashesBySize(ctx, w.cfg.Obs.Logger, w.cas, item.Hashes)

	backoff := w.cfg.workerInitialBackoff()
	maxBackoff := w.cfg.workerMaxBackoff()
	maxAttempts := w.cfg.WorkerMaxAttempts // 0 = unlimited

	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			// Shutdown in progress — leave spool file intact for next start.
			return
		}

		w.cfg.Obs.Logger.Debug("dist-worker: starting attempt",
			"endpoint", w.endpoint, "job_id", item.JobID, "attempt", attempt)

		// Per-attempt context: short timeout so stalled connections fail fast
		// and the backoff+retry loop can recover without blocking the worker.
		attemptCtx, cancel := context.WithTimeout(ctx, w.cfg.workerAttemptTimeout())
		ok := w.attempt(attemptCtx, item)
		cancel()

		if ok {
			w.cfg.Obs.Logger.Info("dist-worker: delivered",
				"endpoint", w.endpoint, "job_id", item.JobID,
				"objects", len(item.Hashes), "attempt", attempt)
			removeSpoolItem(w.spoolDir, item.JobID)
			w.manager.reportEndpoint(item.JobID, w.endpoint, true)
			return
		}

		// Attempt failed.
		if maxAttempts > 0 && attempt >= maxAttempts {
			w.cfg.Obs.Logger.Warn("dist-worker: max attempts reached — giving up",
				"endpoint", w.endpoint, "job_id", item.JobID, "attempts", attempt)
			removeSpoolItem(w.spoolDir, item.JobID)
			w.manager.reportEndpoint(item.JobID, w.endpoint, false)
			return
		}

		w.cfg.Obs.Logger.Warn("dist-worker: attempt failed, will retry",
			"endpoint", w.endpoint, "job_id", item.JobID,
			"attempt", attempt, "backoff", backoff)

		// Exponential backoff — blocked or interrupted by shutdown.
		select {
		case <-ctx.Done():
			// Shutdown — leave spool file for next start.
			return
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// sortHashesBySize reorders hashes in-place, largest CAS object first.
// This ensures that the most expensive transfers start immediately, leaving
// smaller objects to fill the tail — which minimises the long-tail wait.
//
// Size lookups are best-effort: if a stat fails (object temporarily missing,
// filesystem hiccup, etc.) the hash keeps its current position (treated as
// size 0).  The sort is stable so ties preserve insertion order.
func sortHashesBySize(ctx context.Context, log interface {
	Debug(msg string, args ...any)
}, backend cas.Backend, hashes []string) {
	if len(hashes) <= 1 {
		return
	}
	sizes := make([]int64, len(hashes))
	for i, h := range hashes {
		sz, err := backend.Size(ctx, h)
		if err != nil {
			log.Debug("dist-worker: size lookup failed (will use 0)", "hash", h, "error", err)
		}
		sizes[i] = sz
	}
	// Stable sort so that equal-sized objects keep their pipeline order.
	sort.SliceStable(hashes, func(i, j int) bool {
		return sizes[i] > sizes[j] // largest first
	})
}

// attempt performs a single distribution attempt to the endpoint by delegating
// to the existing Distribute function with a single-endpoint config.
// Reuses all existing announce, Bloom-delta, batch and per-object retry logic.
func (w *endpointWorker) attempt(ctx context.Context, item WorkItem) bool {
	cfg := w.cfg
	// Scope to this endpoint only; quorum is irrelevant for a single endpoint.
	cfg.Endpoints = []string{w.endpoint}
	cfg.Quorum = 0
	cfg.Repo = item.Repo
	cfg.TotalBytes = item.TotalBytes

	confirmed, _, err := Distribute(ctx, item.JobID, item.Hashes, w.cas, cfg, NopDistLog())
	if err != nil {
		w.cfg.Obs.Logger.Debug("dist-worker: attempt error",
			"endpoint", w.endpoint, "job_id", item.JobID, "error", err)
		return false
	}
	return confirmed > 0
}
