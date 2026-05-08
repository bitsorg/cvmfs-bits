// Package dedup provides fast deduplication checking via Bloom filters with
// CAS confirmation to avoid false positives.
package dedup

import (
	"context"
	"errors"
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

// NewFromCatalog creates a Checker seeded by walking the CVMFS catalog tree
// rather than scanning the CAS filesystem.  This is the preferred startup path:
//
//   - Catalog queries hit compact SQLite files; the CAS walk hits potentially
//     millions of filesystem entries.  Catalog seeding is 10–100× faster for
//     large repositories.
//   - Only committed objects are seeded, excluding orphan objects from failed
//     publishes, giving a tighter, more accurate filter.
//
// stratum0URL is the HTTP base URL of the Stratum 0 server.
// repoName is the CVMFS repository name (e.g. "atlas.cern.ch").
// tempDir is a writable directory for temporary SQLite files created during
// the walk (each removed immediately after its catalog is processed).
//
// Graceful fallbacks:
//   - stratum0URL empty: falls back to CAS walk (same as newWithConfig).
//   - Repository not yet published (404 on .cvmfspublished): starts with empty
//     filter — correct for a first publish.
//   - Context deadline/cancel mid-walk: seeds with partial results (acceptable).
//   - Any other network/parse error: logs a warning and falls back to CAS walk
//     so that a transiently unavailable Stratum 0 never blocks service startup.
func NewFromCatalog(ctx context.Context, stratum0URL, repoName, tempDir string, casBackend cas.Backend, cfg SharedFilterConfig, obs *observe.Provider) (*Checker, error) {
	ctx, span := obs.Tracer.Start(ctx, "dedup.new_from_catalog")
	defer span.End()

	if stratum0URL == "" || repoName == "" {
		// No Stratum 0 configured — fall back to CAS walk.
		return newWithConfig(ctx, casBackend, cfg, obs)
	}

	hashes, err := CollectCatalogHashes(ctx, stratum0URL, repoName, tempDir, nil, obs)
	switch {
	case err == nil:
		// Full catalog walk completed.
		obs.Logger.Info("dedup: Bloom filter seeded from catalog tree",
			"repo", repoName, "hashes", len(hashes))
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled):
		// Partial seed: seed with what we have; missed objects fall back to CAS.Exists.
		obs.Logger.Warn("dedup: catalog walk interrupted — filter seeded with partial contents",
			"repo", repoName, "seeded", len(hashes), "error", err)
		span.RecordError(err)
	default:
		// Network or parse error — fall back to CAS walk rather than blocking startup.
		obs.Logger.Warn("dedup: catalog seed failed — falling back to CAS filesystem walk",
			"repo", repoName, "error", err)
		span.RecordError(err)
		return newWithConfig(ctx, casBackend, cfg, obs)
	}

	// Size the filter AFTER the catalog walk so we know the actual entry count.
	//
	// Creating the filter before the walk (old behaviour) caused saturation for
	// large repositories: a repo with 3M objects would add 3M hashes into a 1M-
	// capacity filter, driving FPR to ~100%.  With a saturated filter every
	// dedup.Check() triggers a cas.Exists() stat syscall — including for new
	// objects that cannot possibly be in CAS — negating the Bloom filter benefit
	// entirely and slowing jobs proportionally to publish size.
	//
	// When SharedFilter.Enabled the capacity MUST match across all nodes (the bit
	// array dimensions must be identical for filters to be merge-compatible).  In
	// that case we honour the configured value exactly.  When running without
	// shared snapshots (the common case) we auto-size to
	// max(configuredCapacity, len(hashes)*3/2) so the filter is never saturated
	// at startup regardless of repository size.
	capacity := cfg.filterCapacity()
	if !cfg.Enabled {
		// Apply 50% growth headroom above the seeded count so the shared checker
		// can absorb objects added via Add() during this service run before the
		// FPR starts to climb.
		if needed := uint(float64(len(hashes)) * 1.5); needed > capacity {
			capacity = needed
			obs.Logger.Info("dedup: auto-sized Bloom filter to avoid saturation",
				"catalog_hashes", len(hashes), "capacity", capacity)
		}
	}
	filter := bloom.NewWithEstimates(capacity, cfg.filterFPRate())

	for _, h := range hashes {
		filter.AddString(h)
	}

	return &Checker{
		filter: filter,
		cas:    casBackend,
		obs:    obs,
	}, nil
}

func newWithConfig(ctx context.Context, casBackend cas.Backend, cfg SharedFilterConfig, obs *observe.Provider) (*Checker, error) {
	ctx, span := obs.Tracer.Start(ctx, "dedup.new")
	defer span.End()

	filter := bloom.NewWithEstimates(cfg.filterCapacity(), cfg.filterFPRate())

	// Apply a hard timeout for the CAS listing so a large CAS (millions of
	// objects) cannot block pipeline startup indefinitely.  LocalFS.List
	// checks ctx.Done() on each directory entry and returns early with a
	// context error, also returning the hashes enumerated so far.
	//
	// A partial seed is acceptable: objects missing from the filter are treated
	// as cache misses and confirmed with a CAS.Exists round-trip before being
	// re-uploaded (idempotent CAS.Put).  The only cost is a minor dedup-miss
	// rate for objects not reached before the deadline.
	listCtx, cancel := context.WithTimeout(ctx, listTimeout)
	defer cancel()

	hashes, listErr := casBackend.List(listCtx)
	if listErr != nil {
		if errors.Is(listErr, context.DeadlineExceeded) || errors.Is(listErr, context.Canceled) {
			// Partial seed: log a warning but continue with what we got.
			obs.Logger.Warn("dedup: CAS listing timed out — Bloom filter seeded with partial contents; dedup effectiveness reduced",
				"seeded", len(hashes), "timeout", listTimeout)
			span.RecordError(listErr)
			// Fall through to seed the filter with the partial hashes list.
		} else {
			span.RecordError(listErr)
			return nil, fmt.Errorf("listing CAS for dedup seed: %w", listErr)
		}
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
