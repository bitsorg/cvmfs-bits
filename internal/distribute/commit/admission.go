// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Package commit implements the Stratum-0 coordination mechanisms of ADR-0001
// phase P3: server-side admission control (leases), the warm-gate that decides
// when an authoritative quorum of replicas is warm enough to commit, and the
// durable transaction journal used to reconcile a crashed publisher on restart.
//
// These are transport-agnostic building blocks: they hold no broker or HTTP
// dependency, so they can be driven by the MQTT control plane, an SSE control
// plane (ADR P-B), or directly in tests.
package commit

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// Budget is the transfer allowance granted with a lease (ADR D6).
type Budget struct {
	MaxBytesPerSec int64 // 0 = unlimited
	Slots          int   // concurrent object fetches the receiver may run
}

// Lease is an admission grant for one receiver to pull one transaction.
type Lease struct {
	ID      string
	Node    string
	Txn     string
	Expires time.Time
	Budget  Budget
}

// Options configures an Admission controller.
type Options struct {
	MaxConcurrent int           // global cap on active leases (0 = unlimited)
	MaxPerNode    int           // per-receiver cap (0 = unlimited)
	TTL           time.Duration // lease lifetime (0 = 5m default)
	Budget        Budget        // budget handed out with each lease
}

// Admission is the server-side admission controller (ADR D6). It bounds how many
// receivers may pull concurrently — globally and per node — and issues TTL'd
// leases so an unused or stalled lease frees its slot on the next sweep.
type Admission struct {
	maxConcurrent int
	maxPerNode    int
	ttl           time.Duration
	budget        Budget
	newID         func() string

	mu     sync.Mutex
	active map[string]*Lease
	byNode map[string]int
}

// NewAdmission builds an Admission from o.
func NewAdmission(o Options) *Admission {
	ttl := o.TTL
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &Admission{
		maxConcurrent: o.MaxConcurrent,
		maxPerNode:    o.MaxPerNode,
		ttl:           ttl,
		budget:        o.Budget,
		newID:         randomID,
		active:        map[string]*Lease{},
		byNode:        map[string]int{},
	}
}

// Grant issues a lease to node for txn, or returns ok=false when the global or
// per-node concurrency cap is reached. Expired leases are swept first so a stalled
// holder does not permanently occupy a slot.
func (a *Admission) Grant(node, txn string) (Lease, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sweepLocked(time.Now())

	if a.maxConcurrent > 0 && len(a.active) >= a.maxConcurrent {
		return Lease{}, false
	}
	if a.maxPerNode > 0 && a.byNode[node] >= a.maxPerNode {
		return Lease{}, false
	}
	l := &Lease{
		ID:      a.newID(),
		Node:    node,
		Txn:     txn,
		Expires: time.Now().Add(a.ttl),
		Budget:  a.budget,
	}
	a.active[l.ID] = l
	a.byNode[node]++
	return *l, true
}

// Renew extends a lease's TTL. Returns false if the lease is unknown/expired.
func (a *Admission) Renew(id string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	l, ok := a.active[id]
	if !ok {
		return false
	}
	l.Expires = time.Now().Add(a.ttl)
	return true
}

// Release frees a lease immediately (on completion).
func (a *Admission) Release(id string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.releaseLocked(id)
}

func (a *Admission) releaseLocked(id string) {
	l, ok := a.active[id]
	if !ok {
		return
	}
	delete(a.active, id)
	if a.byNode[l.Node] <= 1 {
		delete(a.byNode, l.Node)
	} else {
		a.byNode[l.Node]--
	}
}

// Sweep releases all leases that expired by now and returns the count released.
func (a *Admission) Sweep(now time.Time) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.sweepLocked(now)
}

func (a *Admission) sweepLocked(now time.Time) int {
	n := 0
	for id, l := range a.active {
		if now.After(l.Expires) {
			a.releaseLocked(id)
			n++
		}
	}
	return n
}

// Active returns the number of live leases (for metrics).
func (a *Admission) Active() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.active)
}

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// A crypto entropy failure is unrecoverable; returning a zero ID would
		// collide leases and corrupt admission accounting, so fail loudly.
		panic("commit: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
