// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Package api — GatewayQueue: per-repository serialised lease acquisition.
//
// Problem: the gateway allows only one lease per repository path at a time.
// When two jobs compete for the same repo, the second hits "path_busy" and
// retries with exponential backoff (up to 30 s per attempt).  On a busy server
// this accumulates: a job arriving just after a 10-second native ingest may
// wait 5 s + 10 s + 20 s = 35 s even though the gateway was free after 10 s.
//
// Solution: GatewayQueue wraps lease.Client.TryAcquireOnce in a short-interval
// polling loop (default 1 s) that also listens for an in-process "release
// notification".  When a cvmfs-prepub job commits or aborts its lease, it calls
// NotifyRelease(repo); any goroutine blocked inside Acquire for that repo wakes
// up immediately and re-tries the gateway without waiting for the poll timer.
//
// The per-repo repoMutex in the orchestrator still provides the FIFO serialisation
// for the catalog-merge critical section; GatewayQueue only improves the latency
// of the gateway poll between consecutive jobs.
package api

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"cvmfs.io/prepub/internal/lease"
	"cvmfs.io/prepub/pkg/observe"
)

const (
	// defaultGatewayPollInterval is how long Acquire sleeps between retries
	// when the gateway returns path_busy and no in-process notification arrives.
	// 1 s gives < 1 s latency in picking up a freed lease from an external
	// publisher, compared to up to 30 s with the old exponential backoff.
	defaultGatewayPollInterval = 1 * time.Second
)

// GatewayQueue serialises gateway lease acquisition per-repository and
// provides immediate wake-up when an in-process lease holder releases.
//
// Usage:
//
//	q := NewGatewayQueue(client, obs)
//	token, err := q.Acquire(ctx, repo, path, retryMax)
//	// … commit or abort …
//	q.NotifyRelease(repo)
type GatewayQueue struct {
	client       *lease.Client
	pollInterval time.Duration
	obs          *observe.Provider

	mu     sync.Mutex
	notify map[string]chan struct{} // per-repo; closed on release to broadcast
}

// NewGatewayQueue creates a GatewayQueue backed by client.
func NewGatewayQueue(client *lease.Client, obs *observe.Provider) *GatewayQueue {
	return &GatewayQueue{
		client:       client,
		pollInterval: defaultGatewayPollInterval,
		obs:          obs,
		notify:       make(map[string]chan struct{}),
	}
}

// Acquire waits until the gateway grants a lease for repo/path, then returns
// the opaque token.
//
// Contention behaviour:
//   - If the gateway returns path_busy, Acquire sleeps for at most pollInterval
//     (default 1 s) OR until NotifyRelease(repo) is called — whichever comes
//     first.  This eliminates the old 5 s → 30 s exponential-backoff delay.
//   - retryMax caps the total wait time (same semantics as the original
//     Client.Acquire).  Pass 0 to use the client's configured default.
//
// Hard errors (auth failures, network errors) are returned immediately without
// retrying.
func (q *GatewayQueue) Acquire(ctx context.Context, repo, path string, retryMax time.Duration) (string, error) {
	if retryMax <= 0 {
		retryMax = q.client.EffectiveRetryMax()
	}
	deadline := time.Now().Add(retryMax)
	attempt := 0

	for {
		token, err := q.client.TryAcquireOnce(ctx, repo, path)
		if err == nil {
			if attempt > 0 {
				q.obs.Logger.Info("lease acquired after queued wait",
					"repo", repo, "path", path, "attempts", attempt)
			}
			return token, nil
		}

		if !errors.Is(err, lease.ErrPathBusy) {
			// Hard error — do not retry.
			return "", err
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return "", fmt.Errorf("lease acquisition: path still busy after %s (%d attempts)", retryMax, attempt)
		}

		sleep := q.pollInterval
		if sleep > remaining {
			sleep = remaining
		}
		attempt++
		q.obs.Logger.Info("lease path_busy — queued (waiting for release or poll)",
			"attempt", attempt,
			"repo", repo,
			"path", path,
			"retry_in", sleep,
			"time_remaining", remaining.Round(time.Second),
		)

		notifyCh := q.waitChan(repo)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(sleep):
			// Poll interval elapsed — retry immediately.
		case <-notifyCh:
			// In-process job released its lease — retry immediately without
			// waiting for the poll timer.
			q.obs.Logger.Debug("lease queue: woken by in-process release",
				"repo", repo)
		}
	}
}

// NotifyRelease signals that an in-process lease holder for repo has released
// its lease (committed or aborted).  Any goroutine blocked inside Acquire for
// repo wakes up immediately and retries the gateway.
//
// This should be called after every successful Commit and after every Abort
// that was preceded by an Acquire.  Calling it when no goroutine is waiting
// is a no-op.
func (q *GatewayQueue) NotifyRelease(repo string) {
	q.mu.Lock()
	ch, exists := q.notify[repo]
	if exists {
		// Close the channel to broadcast to all waiters, then remove it so
		// the next Acquire creates a fresh channel.
		close(ch)
		delete(q.notify, repo)
	}
	q.mu.Unlock()
}

// waitChan returns the current notification channel for repo, creating one if
// none exists.  The channel is closed by NotifyRelease to wake all waiters.
func (q *GatewayQueue) waitChan(repo string) chan struct{} {
	q.mu.Lock()
	defer q.mu.Unlock()
	if ch, ok := q.notify[repo]; ok {
		return ch
	}
	ch := make(chan struct{})
	q.notify[repo] = ch
	return ch
}
