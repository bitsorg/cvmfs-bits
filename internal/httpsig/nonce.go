// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package httpsig

import (
	"errors"
	"sync"
	"time"
)

// ErrCacheFull indicates the replay cache could not accept another entry, so
// the request was refused rather than admitted un-remembered.
//
// Distinct from ErrReplay on purpose: an operator seeing "nonce already used"
// for what is actually a capacity problem would look in entirely the wrong
// place.
var ErrCacheFull = errors.New("replay cache is full")

// NonceCache remembers recently accepted signatures so that a captured request
// cannot be replayed.
//
// The timestamp window already bounds a replay; the cache closes it entirely
// for as long as it remembers, which is why retention is tied to that same
// window. An entry older than the window can be forgotten safely: a replay
// carrying it now fails the timestamp check instead.
//
// # Why two generations rather than one map
//
// Expiry by scanning is O(n) and has to happen under the lock, so a full cache
// turns every authenticated request into a full-map walk — the service gets
// slowest exactly when it is busiest. Instead entries go into `current`, and
// every ttl the maps rotate: `previous` is dropped whole (O(1)) and `current`
// becomes `previous`. A lookup checks both, so an entry is remembered for
// between one and two ttl — never less than the replay window, which is what
// correctness needs.
//
// # Bounding
//
// An attacker holding the secret can mint unlimited distinct nonces, so an
// unbounded map is a memory-exhaustion bug wearing a security feature's
// clothes. On reaching the cap the cache REFUSES the request rather than
// admitting one it cannot remember: failing closed, because the alternative is
// to silently stop preventing replays at the moment someone is attacking.
//
// Callers that cannot verify a MAC must never reach Use — otherwise filling the
// cache costs an attacker nothing.
type NonceCache struct {
	mu       sync.Mutex
	current  map[string]struct{}
	previous map[string]struct{}
	rotated  time.Time
	ttl      time.Duration
	maxSize  int
	// rejectedFull counts requests refused because the cache was at capacity.
	// Non-zero means the cap is too small for legitimate traffic, or the
	// service is under attack; both want looking at, so it is reported rather
	// than only logged.
	rejectedFull uint64
	// onPressure, when set, is called the first time occupancy crosses
	// pressureRatio, and again after a rotation drops it back below. Failing
	// closed at the cap is correct but abrupt — the operator's first symptom
	// would be authenticated publishers getting 401s — so the approach is
	// announced while there is still room to raise the cap.
	//
	// It is invoked with the mutex RELEASED. It is operator-supplied and ends
	// up in a log sink, and a sink that blocks while holding this lock would
	// stall every authenticated request on the service.
	onPressure    func(entries, maxSize int)
	underPressure bool
}

// pressureRatio is the occupancy at which onPressure fires.
const pressureRatio = 0.8

// DefaultMaxNonces caps the cache. At ~100 bytes per entry this is a few MB,
// far above any legitimate rate: a 100-package build makes ~200 requests.
const DefaultMaxNonces = 50_000

// NewNonceCache creates a cache retaining entries for at least ttl
// (0 = 2×DefaultSkew) with at most maxSize entries (0 = DefaultMaxNonces).
func NewNonceCache(ttl time.Duration, maxSize int) *NonceCache {
	if ttl <= 0 {
		ttl = 2 * DefaultSkew
	}
	if maxSize <= 0 {
		maxSize = DefaultMaxNonces
	}
	return &NonceCache{
		current:  make(map[string]struct{}),
		previous: make(map[string]struct{}),
		rotated:  time.Now(),
		ttl:      ttl,
		maxSize:  maxSize,
	}
}

// Use records a signature as seen, returning ErrReplay if it already was or
// ErrCacheFull if it cannot be remembered.
//
// It must be called only AFTER the MAC has verified.
func (c *NonceCache) Use(sig *Signature, now time.Time) error {
	key := sig.cacheKey()

	// Unlocked explicitly rather than by defer: the pressure notification must
	// run with the lock released (see onPressure), and unlocking, notifying and
	// re-locking just to satisfy a defer would open a window in which another
	// goroutine mutates the cache between the two halves of one operation.
	c.mu.Lock()

	c.rotateLocked(now)

	if _, dup := c.current[key]; dup {
		c.mu.Unlock()
		return ErrReplay
	}
	if _, dup := c.previous[key]; dup {
		c.mu.Unlock()
		return ErrReplay
	}

	n := len(c.current) + len(c.previous)
	if n >= c.maxSize {
		c.rejectedFull++
		c.mu.Unlock()
		return ErrCacheFull
	}

	c.current[key] = struct{}{}
	notify := c.pressureCrossedLocked(n + 1)
	c.mu.Unlock()

	if notify != nil {
		notify()
	}
	return nil
}

// pressureCrossedLocked records a crossing of the high-water mark and returns
// the notification to run once the lock is released, or nil. Callers hold c.mu.
func (c *NonceCache) pressureCrossedLocked(n int) func() {
	over := float64(n) >= pressureRatio*float64(c.maxSize)
	if over == c.underPressure {
		return nil
	}
	c.underPressure = over
	if !over || c.onPressure == nil {
		return nil
	}
	fn, max := c.onPressure, c.maxSize
	return func() { fn(n, max) }
}

// SetPressureHook installs the high-water callback. Call before serving.
func (c *NonceCache) SetPressureHook(fn func(entries, maxSize int)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onPressure = fn
}

// rotateLocked ages out the older generation once per ttl. Callers hold c.mu.
func (c *NonceCache) rotateLocked(now time.Time) {
	if now.Sub(c.rotated) < c.ttl {
		return
	}
	// A long idle gap means both generations are older than the window; drop
	// both rather than promoting stale entries.
	if now.Sub(c.rotated) >= 2*c.ttl {
		c.previous = make(map[string]struct{})
	} else {
		c.previous = c.current
	}
	c.current = make(map[string]struct{})
	c.rotated = now
	// A rotation only ever drops the count, so this can clear the flag but
	// never raise it — no notification is possible and none is returned.
	c.pressureCrossedLocked(len(c.current) + len(c.previous))
}

// Sweep ages the cache without a request arriving, so a quiet service does not
// hold entries indefinitely. Correctness does not depend on it — Use rotates on
// demand — but memory does.
func (c *NonceCache) Sweep(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rotateLocked(now)
}

// StartSweeper runs Sweep until stop is closed. Returns a function that stops it.
func (c *NonceCache) StartSweeper() (stop func()) {
	done := make(chan struct{})
	var once sync.Once
	go func() {
		t := time.NewTicker(c.ttl)
		defer t.Stop()
		for {
			select {
			case now := <-t.C:
				c.Sweep(now)
			case <-done:
				return
			}
		}
	}()
	return func() { once.Do(func() { close(done) }) }
}

// Stats reports the current entry count and how many requests have been
// rejected because the cache was full.
func (c *NonceCache) Stats() (entries int, rejectedFull uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.current) + len(c.previous), c.rejectedFull
}
