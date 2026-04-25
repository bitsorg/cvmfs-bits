package receiver

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
)

const (
	// defaultInventoryCapacity is the estimated number of distinct CAS objects
	// a Tier-1 node will hold.  5 million objects ≈ a large CVMFS repository
	// with several years of publishes.
	defaultInventoryCapacity uint    = 5_000_000
	defaultInventoryFPRate   float64 = 0.001 // 0.1 % false-positive rate

	// casHashLen is the expected length of a CAS object filename (SHA-256 hex,
	// 64 chars, optionally followed by a compression suffix like "C").
	casHashMinLen = 64
)

// inventory is a thread-safe Bloom filter that tracks which content-addressed
// objects are present in the receiver's local CAS directory.
//
// It is populated once at receiver startup by walking the CAS tree
// (populateFromCAS), then updated incrementally on every successful PUT
// (add).  The filter is served over HTTP at GET /api/v1/bloom so that the
// distributor can compute a per-node delta before pushing.
//
// Bloom filters admit false positives (the filter may report an object as
// present when it is not) but never false negatives (if the filter reports an
// object as absent, it is definitely absent).  For the delta-push use case
// this is the conservative direction: false positives cause the distributor to
// skip an object the node does not actually have, resulting in a wasted
// round-trip to fetch it later; false negatives would never occur.
type inventory struct {
	mu     sync.RWMutex
	filter *bloom.BloomFilter
	size   uint64 // approximate count of distinct objects added
	ready  bool   // true once populateFromCAS has finished
}

// newInventory creates an empty inventory filter with the given parameters.
// capacity is the expected number of distinct objects; fpRate is the desired
// false-positive probability (0 < fpRate < 1).
//
// All nodes in a HepCDN deployment should use identical capacity and fpRate
// values so that filter snapshots remain mergeable.
func newInventory(capacity uint, fpRate float64) *inventory {
	if capacity == 0 {
		capacity = defaultInventoryCapacity
	}
	if fpRate <= 0 || fpRate >= 1 {
		fpRate = defaultInventoryFPRate
	}
	return &inventory{
		filter: bloom.NewWithEstimates(capacity, fpRate),
	}
}

// add inserts hash into the inventory.  Safe for concurrent use; called from
// putObjectHandler after every successful CAS write.
func (inv *inventory) add(hash string) {
	inv.mu.Lock()
	inv.filter.AddString(hash)
	inv.size++
	inv.mu.Unlock()
}

// contains reports whether hash is probably present in the inventory.
// A false return is definitive (the object is absent); a true return means
// probably present (the object may have been evicted from the CAS).
func (inv *inventory) contains(hash string) bool {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	return inv.filter.TestString(hash)
}

// writeTo serialises the current filter state to w.
// The format is the binary encoding produced by bloom.BloomFilter.WriteTo,
// which the distributor reads back with bloom.BloomFilter.ReadFrom.
// Returns the number of bytes written.
//
// Implementation note: the filter is first captured into an in-memory buffer
// under the read lock (a fast bit-copy, typically ~9 MB for the default 5 M
// capacity filter), then the lock is released and the buffer is streamed to w.
// This bounds the lock hold time to the serialisation step only — not the full
// network write — so concurrent add() calls from PUT handlers are not stalled
// while a slow bloom-filter client is downloading the snapshot.
func (inv *inventory) writeTo(w io.Writer) (int64, error) {
	var buf bytes.Buffer
	inv.mu.RLock()
	_, err := inv.filter.WriteTo(&buf)
	inv.mu.RUnlock()
	if err != nil {
		return 0, fmt.Errorf("inventory: serialising bloom filter: %w", err)
	}
	n, err := buf.WriteTo(w)
	if err != nil {
		return n, fmt.Errorf("inventory: writing bloom filter to response: %w", err)
	}
	return n, nil
}

// isReady reports whether the initial CAS walk has completed.
func (inv *inventory) isReady() bool {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	return inv.ready
}

// approximateSize returns the number of distinct objects the filter believes
// it contains (updated on each add() call).
func (inv *inventory) approximateSize() uint64 {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	return inv.size
}

// populateFromCAS walks the two-level CAS directory tree rooted at casRoot and
// inserts every object hash it finds into the filter.  It is intended to run
// as a background goroutine at receiver startup; callers should not block on it.
//
// CAS layout:  {casRoot}/{aa}/{aabbcc…}[C]
//   - First level: 2-character hex directory prefix.
//   - Second level: full hash filename, optionally suffixed with "C"
//     (compressed) or other single-character flags.
//
// populateFromCAS is designed to be crash-safe: if the receiver restarts
// mid-walk, the filter starts empty and the walk runs again from scratch.
// The filter converges to a correct state within seconds on typical hardware
// (a 5 M object CAS walks in < 30 s on a local NVMe).
//
// Errors reading individual directories or files are logged and skipped so
// that a single corrupt entry never aborts the entire walk.
// populateFromCAS walks the two-level CAS directory tree rooted at casRoot and
// inserts every object hash it finds into the filter.  It is intended to run
// as a background goroutine at receiver startup; callers should not block on it.
//
// CAS layout:  {casRoot}/{aa}/{aabbcc…}[C]
//   - First level: 2-character hex directory prefix.
//   - Second level: full hash filename, optionally suffixed with "C"
//     (compressed) or other single-character flags.
//
// populateFromCAS is designed to be crash-safe: if the receiver restarts
// mid-walk, the filter starts empty and the walk runs again from scratch.
// The filter converges to a correct state within seconds on typical hardware
// (a 5 M object CAS walks in < 30 s on a local NVMe).
//
// Errors reading individual directories or files are logged and skipped so
// that a single corrupt entry never aborts the entire walk.
//
// The walk is cancelled when ctx is done (e.g. on receiver Shutdown).  Even
// on cancellation, isReady() is set to true so the bloom endpoint does not
// permanently return 503 — a partial filter is conservative (false positives
// only), never incorrect in the dangerous direction.
func (inv *inventory) populateFromCAS(ctx context.Context, casRoot string, logFn func(msg string, args ...any)) error {
	logFn("inventory: starting CAS walk", "root", casRoot)

	prefixEntries, err := os.ReadDir(casRoot)
	if err != nil {
		// Mark ready in both the not-exist and the general-error case.
		// An empty filter is the safe (conservative) result: the distributor will
		// push every object and the receiver will deduplicate via the idempotent
		// PUT path.  Never leaving ready=false avoids a permanent 503.
		inv.mu.Lock()
		inv.ready = true
		inv.mu.Unlock()
		if os.IsNotExist(err) {
			logFn("inventory: CAS root does not exist yet, starting with empty filter", "root", casRoot)
			return nil
		}
		logFn("inventory: failed to read CAS root, starting with empty filter",
			"root", casRoot, "error", err)
		return fmt.Errorf("inventory: reading CAS root %q: %w", casRoot, err)
	}

	// Walk the CAS tree and bulk-insert per prefix directory.
	//
	// Memory strategy: collect one prefix dir's hashes into a local []string,
	// then lock-bulk-add-unlock before moving to the next prefix.  A 2-char hex
	// prefix dir holds at most len(CAS)/256 objects; for a 5 M object CAS that
	// is ~20 K entries ≈ 1.3 MB per batch.  This is O(1) in CAS size (bounded
	// by the largest single subdir), unlike collecting all hashes globally which
	// would allocate ~400 MB for 5 M objects or 4 GB for 50 M objects.
	//
	// Lock strategy: one lock acquisition per prefix dir (256 total) rather than
	// one per object (5 M total) or one for the whole walk (blocks all add()
	// calls for potentially seconds).  Each lock hold covers only the fast
	// bloom-insert loop for that subdir.
	var totalCount uint64
	for _, prefixEntry := range prefixEntries {
		// Respect context cancellation between prefix directories.  This allows
		// Shutdown() to interrupt the walk on a stalling filesystem without waiting
		// for all 256 prefix dirs to be scanned.  Mark ready before returning so
		// the bloom endpoint does not serve 503 indefinitely.
		select {
		case <-ctx.Done():
			logFn("inventory: CAS walk cancelled", "root", casRoot, "objects_so_far", totalCount)
			inv.mu.Lock()
			inv.ready = true
			inv.mu.Unlock()
			return ctx.Err()
		default:
		}

		if !prefixEntry.IsDir() {
			continue
		}
		prefix := prefixEntry.Name()
		if len(prefix) != 2 {
			continue // skip non-CAS directories (e.g. lost+found)
		}

		subDir := filepath.Join(casRoot, prefix)
		hashEntries, err := os.ReadDir(subDir)
		if err != nil {
			logFn("inventory: skipping unreadable CAS subdir",
				"dir", subDir, "error", err)
			continue
		}

		// Collect valid hashes for this prefix into a stack-local slice.
		// Capacity hint: a balanced CAS has ~CAS_size/256 objects per prefix.
		batch := make([]string, 0, len(hashEntries))
		for _, hashEntry := range hashEntries {
			if hashEntry.IsDir() {
				continue
			}
			name := hashEntry.Name()

			// Skip orphaned .tmp files left by PUT handlers interrupted between
			// os.OpenFile and os.Rename.  A tmp filename looks like
			// "{64hexchars}C.{8hexchars}.tmp"; taking name[:casHashMinLen] yields
			// the 64-char CAS hash which passes isHexString — producing a false
			// positive in the bloom filter.  A false positive causes the
			// distributor to skip pushing an object the receiver does not hold.
			if strings.HasSuffix(name, ".tmp") {
				continue
			}

			// Strip optional suffix (e.g. "C" for compressed) to normalise to
			// the 64-character hex key stored in the filter.
			hash := name
			if len(name) > casHashMinLen {
				hash = name[:casHashMinLen]
			}
			if len(hash) < casHashMinLen {
				continue // skip files that don't look like CAS objects
			}
			// Validate: must be lowercase hex.
			if !isHexString(hash) {
				continue
			}
			batch = append(batch, hash)
		}

		if len(batch) == 0 {
			continue
		}

		// Bulk-insert this prefix's hashes under a single lock acquisition.
		// Concurrent add() calls only block for the duration of this tight loop,
		// not for the entire walk.  The bloom filter is idempotent so double-
		// adding a hash that a concurrent PUT also added is harmless; size may
		// be slightly over-estimated, which is acceptable for a gauge metric.
		inv.mu.Lock()
		for _, h := range batch {
			inv.filter.AddString(h)
		}
		inv.size += uint64(len(batch))
		inv.mu.Unlock()

		totalCount += uint64(len(batch))
	}

	inv.mu.Lock()
	inv.ready = true
	inv.mu.Unlock()

	logFn("inventory: CAS walk complete", "objects", totalCount)
	return nil
}

// isHexString reports whether s consists entirely of lowercase hex digits.
func isHexString(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return len(s) > 0
}

// filterAbsent returns the subset of hashes that are definitely not in the
// filter.  Used by the distributor to compute the delta before pushing.
func filterAbsent(hashes []string, bf *bloom.BloomFilter) []string {
	out := make([]string, 0, len(hashes))
	for _, h := range hashes {
		if !bf.TestString(h) {
			out = append(out, h)
		}
	}
	return out
}

// filterAbsentTrimSuffix is like filterAbsent but strips any suffix beyond
// casHashMinLen characters before testing, to handle CAS filenames that carry
// a compression suffix (e.g. "aabbcc…C" → test "aabbcc…").
//
// Note: strings.TrimRight must NOT be used here because it strips all
// matching trailing characters, not just one.  A suffix like "CC" would strip
// two characters, producing a 62-char key that is never in the filter.  A
// fixed slice to casHashMinLen is always correct regardless of suffix length.
func filterAbsentTrimSuffix(hashes []string, bf *bloom.BloomFilter) []string {
	out := make([]string, 0, len(hashes))
	for _, h := range hashes {
		key := h
		if len(h) > casHashMinLen {
			key = h[:casHashMinLen]
		}
		if !bf.TestString(key) {
			out = append(out, h)
		}
	}
	return out
}
