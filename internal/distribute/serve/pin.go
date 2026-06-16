// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package serve

import (
	"sync"
	"time"

	"cvmfs.io/prepub/internal/distribute/commit"
)

// Pinner protects a transaction's objects from garbage collection during the
// prepare→commit window (ADR R2). P1 provides an in-memory registry with TTL;
// integration with cvmfs_server gc (a temporary named tag, or holding the
// gateway lease for the window) is wired alongside the commit in P3. The TTL +
// Sweep guard against pins leaked by a crash.
type Pinner interface {
	// Pin protects hashes for txn for at least ttl. Re-pinning a txn replaces it.
	Pin(txn string, hashes []string, ttl time.Duration)
	// Release drops a txn's pins immediately (on commit or abort).
	Release(txn string)
	// IsPinned reports whether any live txn pins hash.
	IsPinned(hash string) bool
	// Sweep releases pins whose TTL has elapsed and returns the released txns.
	Sweep(now time.Time) []string
}

type pinEntry struct {
	hashes  map[string]struct{}
	expires time.Time
}

// MemPinner is a concurrency-safe in-memory Pinner.
type MemPinner struct {
	mu    sync.Mutex
	byTxn map[string]*pinEntry
	count map[string]int // hash -> number of live txns pinning it (reference count)
}

// NewMemPinner returns an empty MemPinner.
func NewMemPinner() *MemPinner {
	return &MemPinner{byTxn: map[string]*pinEntry{}, count: map[string]int{}}
}

func (p *MemPinner) Pin(txn string, hashes []string, ttl time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if old, ok := p.byTxn[txn]; ok {
		p.removeLocked(txn, old)
	}
	e := &pinEntry{hashes: make(map[string]struct{}, len(hashes)), expires: time.Now().Add(ttl)}
	for _, h := range hashes {
		if _, dup := e.hashes[h]; dup {
			continue
		}
		e.hashes[h] = struct{}{}
		p.count[h]++
	}
	p.byTxn[txn] = e
}

func (p *MemPinner) Release(txn string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.byTxn[txn]; ok {
		p.removeLocked(txn, e)
	}
}

// removeLocked drops e's reference counts; caller holds p.mu.
func (p *MemPinner) removeLocked(txn string, e *pinEntry) {
	for h := range e.hashes {
		if p.count[h] <= 1 {
			delete(p.count, h)
		} else {
			p.count[h]--
		}
	}
	delete(p.byTxn, txn)
}

func (p *MemPinner) IsPinned(hash string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.count[hash] > 0
}

func (p *MemPinner) Sweep(now time.Time) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var released []string
	for txn, e := range p.byTxn {
		if now.After(e.expires) {
			p.removeLocked(txn, e)
			released = append(released, txn)
		}
	}
	return released
}

var _ Pinner = (*MemPinner)(nil)

// MemPinner also satisfies the orchestrator's Pinner (Pin+Release subset), so it
// can back commit.Orchestrator directly without an adapter.
var _ commit.Pinner = (*MemPinner)(nil)
