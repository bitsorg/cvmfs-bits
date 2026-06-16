// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package commit

import (
	"context"
	"sync"
	"time"
)

// WarmGate tracks per-transaction warming acks and decides when an authoritative
// quorum of Stratum 1 replicas is warm, so the catalog commit may proceed
// (ADR-0001 D6). "Committed" is decoupled from "globally warm": only the
// configured authoritative replicas count toward quorum; non-authoritative or
// late replicas converge afterwards via catch-up.
type WarmGate struct {
	authoritative map[string]bool
	quorum        int

	mu  sync.Mutex
	txn map[string]*txnWarm
}

type txnWarm struct {
	acked  map[string]bool
	done   chan struct{} // closed once quorum is reached
	closed bool
}

// NewWarmGate configures the authoritative replica set and the quorum size.
// A quorum <= 0 or greater than the set size defaults to "all authoritative".
func NewWarmGate(authoritative []string, quorum int) *WarmGate {
	a := make(map[string]bool, len(authoritative))
	for _, n := range authoritative {
		a[n] = true
	}
	if quorum <= 0 || quorum > len(a) {
		quorum = len(a)
	}
	return &WarmGate{authoritative: a, quorum: quorum, txn: map[string]*txnWarm{}}
}

func (g *WarmGate) stateLocked(txn string) *txnWarm {
	s := g.txn[txn]
	if s == nil {
		s = &txnWarm{acked: map[string]bool{}, done: make(chan struct{})}
		// Quorum of zero (no authoritative replicas) is satisfied immediately:
		// there is nothing to wait for, so the commit may proceed (degrade).
		if g.quorum == 0 {
			close(s.done)
			s.closed = true
		}
		g.txn[txn] = s
	}
	return s
}

// Ack records that node has finished warming txn. Acks from non-authoritative
// nodes are ignored for quorum (harmless to send).
func (g *WarmGate) Ack(txn, node string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.authoritative[node] {
		return
	}
	s := g.stateLocked(txn)
	s.acked[node] = true
	if !s.closed && len(s.acked) >= g.quorum {
		close(s.done)
		s.closed = true
	}
}

// Warmth reports how many authoritative replicas have acked txn and how many are
// required (for metrics / observability).
func (g *WarmGate) Warmth(txn string) (acked, required int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if s := g.txn[txn]; s != nil {
		return len(s.acked), g.quorum
	}
	return 0, g.quorum
}

// Reached reports whether the authoritative quorum for txn is already met.
func (g *WarmGate) Reached(txn string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	s := g.txn[txn]
	return s != nil && s.closed
}

// WaitQuorum blocks until an authoritative quorum has acked txn, ctx is done, or
// timeout elapses. It returns true only if quorum was reached — on timeout the
// caller commits anyway (ADR D6) and lets laggards catch up post-commit.
//
// Lifecycle contract: calling WaitQuorum (or Ack) lazily creates per-txn state
// in an internal map. The caller MUST call Forget(txn) once the transaction is
// resolved — on BOTH the true (quorum) and false (timeout/ctx) returns — or the
// map grows without bound. Do not call Forget while another goroutine may still
// WaitQuorum/Ack the same txn: Forget orphans the current done channel, so a
// blocked waiter will only wake via ctx/timeout and a later Ack starts fresh
// state. In the commit flow Forget is the last step after commit or abort.
func (g *WarmGate) WaitQuorum(ctx context.Context, txn string, timeout time.Duration) bool {
	g.mu.Lock()
	s := g.stateLocked(txn)
	done := s.done
	closed := s.closed
	g.mu.Unlock()
	if closed {
		return true
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	case <-timer.C:
		return false
	}
}

// Forget drops a transaction's warm state once it has been committed or aborted,
// so the map does not grow without bound.
func (g *WarmGate) Forget(txn string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.txn, txn)
}
