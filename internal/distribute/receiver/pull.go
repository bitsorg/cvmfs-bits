// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package receiver

import (
	"time"

	"cvmfs.io/prepub/internal/distribute/puller"
)

// pullConcurrency bounds the number of in-flight pull-warming goroutines, so a
// flood of announces (a misbehaving or compromised broker) cannot exhaust the
// receiver's goroutines or outbound connections.
const pullConcurrency = 4

// recordPullMetrics emits the receiver-side pull-distribution metrics for one
// warming attempt (ADR-0001 monitoring). Safe when metrics are unconfigured.
func (r *Receiver) recordPullMetrics(res puller.Result, err error, dur time.Duration) {
	if r.cfg.Obs == nil || r.cfg.Obs.Metrics == nil {
		return
	}
	m := r.cfg.Obs.Metrics
	m.PullDuration.Observe(dur.Seconds())
	if err != nil || res.Failed > 0 {
		m.PullTransactions.WithLabelValues("failed").Inc()
	} else {
		m.PullTransactions.WithLabelValues("warmed").Inc()
	}
	if res.Fetched > 0 {
		m.PullObjects.WithLabelValues("fetched").Add(float64(res.Fetched))
	}
	if res.Skipped > 0 {
		m.PullObjects.WithLabelValues("skipped").Add(float64(res.Skipped))
	}
	if res.Failed > 0 {
		m.PullObjects.WithLabelValues("failed").Add(float64(res.Failed))
	}
}

// startPull launches a bounded, deduplicated pull for a transaction (ADR-0001
// pull mode). Concurrent announces for the same transaction are coalesced; when
// the concurrency limit is reached the announce is dropped — announces are
// repeated/retained and content addressing makes a dropped warm safe to retry,
// so dropping is preferable to unbounded resource growth.
func (r *Receiver) startPull(payloadID, repo string) {
	if r.pullCoordinator == nil {
		return
	}
	if _, dup := r.pullInflight.LoadOrStore(payloadID, struct{}{}); dup {
		return // a pull for this transaction is already running
	}
	select {
	case r.pullSem <- struct{}{}:
	default:
		r.pullInflight.Delete(payloadID)
		r.cfg.Obs.Logger.Warn("pull: concurrency limit reached, dropping announce",
			"payload_id", payloadID, "repo", repo, "limit", pullConcurrency)
		return
	}
	go func() {
		defer func() {
			<-r.pullSem
			r.pullInflight.Delete(payloadID)
		}()
		start := time.Now()
		res, err := r.pullCoordinator.OnTransaction(r.bgCtx, payloadID)
		r.recordPullMetrics(res, err, time.Since(start))
		if err != nil {
			r.cfg.Obs.Logger.Warn("pull: transaction failed",
				"payload_id", payloadID, "repo", repo, "error", err)
			// Report the failed warm so the publisher does not count this node
			// toward quorum (it still degrades to a timeout-commit if needed).
			if r.cfg.OnWarmed != nil {
				r.cfg.OnWarmed(payloadID, repo, false)
			}
			return
		}
		r.cfg.Obs.Logger.Info("pull: transaction warmed",
			"payload_id", payloadID, "repo", repo,
			"fetched", res.Fetched, "skipped", res.Skipped, "failed", res.Failed)
		// Ack the warm to the publisher's WarmGate (ADR-0001 D6). A non-nil
		// Failed count means some objects could not be fetched/verified, so the
		// node is not warm.
		if r.cfg.OnWarmed != nil {
			r.cfg.OnWarmed(payloadID, repo, res.Failed == 0)
		}
	}()
}
