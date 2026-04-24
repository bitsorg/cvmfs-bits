// Package dedup provides fast deduplication checking via Bloom filters with
// CAS confirmation to avoid false positives.
package dedup

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"

	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/pkg/observe"
)

// listTimeout caps the CAS listing at startup so a slow/hung backend cannot
// block service startup indefinitely.  Fix #19.
const listTimeout = 30 * time.Second

// Checker is a thread-safe deduplication checker backed by a Bloom filter
// with a CAS confirmation step for true-positive verification.
//
// Two-step dedup pattern (Fix #8):
//   1. Fast path: check Bloom filter with read lock (negative test: "definitely not present")
//   2. Slow path: if present in filter, confirm with CAS HEAD to rule out false positives
//
// The filter is protected by an RWMutex so concurrent upload workers can call Add()
// after each successful CAS.Put without racing. Without this, the filter is never
// updated during a job run, so files uploaded in the same run are re-uploaded on
// the next job instead of being deduped.
type Checker struct {
	// mu protects filter for safe concurrent reads and updates.
	mu sync.RWMutex
	// filter is the Bloom filter seeded from existing CAS contents.
	filter *bloom.BloomFilter
	// cas is the backend storage for CAS confirmation.
	cas cas.Backend
	// obs provides logging and metrics.
	obs *observe.Provider
}

// New creates a new Checker, seeding the Bloom filter from the existing CAS contents.
// Uses default filter parameters (1,000,000 items, 1% FPR).
//
// Fix #19: the CAS listing is bounded by listTimeout so a slow backend cannot
// block startup indefinitely.
func New(ctx context.Context, casBackend cas.Backend, obs *observe.Provider) (*Checker, error) {
	return newWithConfig(ctx, casBackend, SharedFilterConfig{}, obs)
}

// NewWithConfig is like New but uses filter parameters from cfg so that the
// constructed filter is compatible with peer snapshots produced by other nodes
// using the same configuration. Use this when SharedFilter snapshots are enabled
// to ensure filters are mergeable across nodes.
func NewWithConfig(ctx context.Context, casBackend cas.Backend, cfg SharedFilterConfig, obs *observe.Provider) (*Checker, error) {
	return newWithConfig(ctx, casBackend, cfg, obs)
}

func newWithConfig(ctx context.Context, casBackend cas.Backend, cfg SharedFilterConfig, obs *observe.Provider) (*Checker, error) {
	ctx, span := obs.Tracer.Start(ctx, "dedup.new")
	defer span.End()

	filter := bloom.NewWithEstimates(cfg.filterCapacity(), cfg.filterFPRate())

	// Apply a hard timeout for the CAS listing.
	listCtx, cancel := context.WithTimeout(ctx, listTimeout)
	defer cancel()

	hashes, err := casBackend.List(listCtx)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("listing CAS for dedup seed: %w", err)
	}

	for _, hash := range hashes {
		filter.AddString(hash)
	}

	return &Checker{
		filter: filter,
		cas:    casBackend,
		obs:    obs,
	}, nil
}

// Add registers hash in the Bloom filter so that subsequent Check() calls in
// the same process lifetime can dedup against it without a round-trip to CAS.
// Safe to call concurrently from multiple upload workers. Called by the upload
// stage after each successful CAS.Put to allow intra-job deduplication.
func (c *Checker) Add(hash string) {
	c.mu.Lock()
	c.filter.AddString(hash)
	c.mu.Unlock()
}

// Check returns true if hash already exists in CAS, false otherwise.
// It uses a two-step pattern: fast-path Bloom filter check (negative test), then
// CAS HEAD confirmation if present in the filter to rule out false positives.
// Increments PipelineDedupHits metric only on confirmed CAS hits.
func (c *Checker) Check(ctx context.Context, hash string) (bool, error) {
	ctx, span := c.obs.Tracer.Start(ctx, "dedup.check")
	defer span.End()

	// Fast path: read-locked Bloom filter test.
	c.mu.RLock()
	inFilter := c.filter.TestString(hash)
	c.mu.RUnlock()

	if !inFilter {
		return false, nil
	}

	// Confirm with CAS to rule out false positives.
	exists, err := c.cas.Exists(ctx, hash)
	if err != nil {
		span.RecordError(err)
		return false, fmt.Errorf("checking CAS existence for %s: %w", hash, err)
	}

	if exists {
		c.obs.Metrics.PipelineDedupHits.Inc()
	}

	return exists, nil
}
