// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package credential

import (
	"testing"
	"time"
)

// Security review M3: eviction must be least-recently-USED, not FIFO. A client
// that keeps making requests must NOT be evicted by a flood of one-shot IPs.
// Under the old FIFO behaviour the oldest-INSERTED entry (the active client A)
// would be wrongly dropped; LRU keeps it and evicts the idle one (B).
func TestRateLimiterLRUEviction(t *testing.T) {
	// Huge per-IP/global budgets so the token bucket never denies — we test only
	// which bucket is evicted when maxIPs is exceeded.
	l := NewIPRateLimiter(1e6, 1e6, 2, 1e9, 1e9)
	t0 := time.Now()
	l.allow("A", t0)                          // track A
	l.allow("B", t0.Add(1*time.Millisecond))  // track B (now at capacity=2)
	l.allow("A", t0.Add(2*time.Millisecond))  // A active -> most-recently-used
	l.allow("C", t0.Add(3*time.Millisecond))  // new IP at capacity -> evict LRU (B)

	l.mu.Lock()
	_, aTracked := l.buckets["A"]
	_, bTracked := l.buckets["B"]
	_, cTracked := l.buckets["C"]
	n := len(l.buckets)
	l.mu.Unlock()

	if !aTracked {
		t.Error("recently-used A was evicted (FIFO behaviour); LRU must keep it")
	}
	if bTracked {
		t.Error("least-recently-used B should have been evicted")
	}
	if !cTracked {
		t.Error("new IP C should be tracked")
	}
	if n != 2 {
		t.Errorf("maxIPs=2 must be respected; got %d tracked", n)
	}
}

// The per-IP token bucket still denies once the burst is spent, and refills.
func TestRateLimiterTokenBudget(t *testing.T) {
	l := NewIPRateLimiter(1, 2, 16, 1e9, 1e9) // 1 tok/s, burst 2
	t0 := time.Now()
	if !l.allow("x", t0) || !l.allow("x", t0) {
		t.Fatal("first two requests (within burst) must be allowed")
	}
	if l.allow("x", t0) {
		t.Error("third immediate request must be denied (burst exhausted)")
	}
	if !l.allow("x", t0.Add(1100*time.Millisecond)) {
		t.Error("after ~1s a refilled token must allow the request")
	}
}
