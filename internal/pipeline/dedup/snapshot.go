package dedup

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bits-and-blooms/bloom/v3"

	"cvmfs.io/prepub/pkg/observe"
)

const (
	snapshotExt    = ".bloom"
	snapshotTmpExt = ".bloom.tmp"

	// defaultMaxSnapshotAge is the cutoff beyond which a peer snapshot is
	// considered stale and is ignored during merge.  A node that has not
	// published for longer than this interval is assumed decommissioned.
	defaultMaxSnapshotAge = 24 * time.Hour
)

// SharedFilterConfig controls cross-node Bloom filter snapshot sharing.
//
// When Enabled is false (the default) the dedup behaviour is identical to the
// single-node case: each pipeline run seeds its filter by scanning the shared
// CAS at startup.
//
// When Enabled is true, each pipeline run additionally:
//   - loads all peer snapshot files from Dir at job start and merges them
//     (bitwise OR) into its local filter;
//   - writes its own snapshot to Dir/{NodeID}.bloom after the job completes.
//
// All build nodes MUST be configured with the same filter parameters (capacity
// and false-positive rate) because Bloom filters can only be merged when they
// have identical m (bit-width) and k (hash-function count).  The default
// parameters (1 000 000 items, 1 % FPR) are used if left at zero.
//
// The Dir must be readable and writable by every build node — a shared NFS,
// CephFS, or equivalent mount is the typical deployment.
type SharedFilterConfig struct {
	// Enabled activates snapshot sharing. Off by default.
	Enabled bool

	// Dir is the directory where snapshots are stored.
	// All build nodes must be able to read and write this directory.
	// If empty, snapshot sharing is a no-op even when Enabled is true.
	Dir string

	// NodeID is a unique, stable identifier for this build node.
	// Defaults to os.Hostname() if empty.
	// It becomes the filename stem: {Dir}/{NodeID}.bloom.
	NodeID string

	// MaxSnapshotAge is the maximum age of a peer snapshot considered for
	// merging.  Snapshots older than this are skipped — the originating node
	// is assumed decommissioned.  Defaults to defaultMaxSnapshotAge (24 h).
	MaxSnapshotAge time.Duration

	// Capacity is the estimated number of distinct objects the filter will
	// hold.  Must be the same value on every node.  Defaults to 1 000 000.
	Capacity uint

	// FPRate is the desired false-positive probability (0 < FPRate < 1).
	// Must be the same value on every node.  Defaults to 0.01 (1 %).
	FPRate float64
}

// filterCapacity returns the configured capacity with a safe default.
func (c SharedFilterConfig) filterCapacity() uint {
	if c.Capacity > 0 {
		return c.Capacity
	}
	return 1_000_000
}

// filterFPRate returns the configured false-positive rate with a safe default.
func (c SharedFilterConfig) filterFPRate() float64 {
	if c.FPRate > 0 && c.FPRate < 1 {
		return c.FPRate
	}
	return 0.01
}

// resolvedNodeID returns the configured NodeID, falling back to the OS hostname.
func (c SharedFilterConfig) resolvedNodeID() string {
	if c.NodeID != "" {
		return c.NodeID
	}
	h, err := os.Hostname()
	if err == nil && h != "" {
		return h
	}
	return "unknown-node"
}

// maxAge returns the configured snapshot age cutoff with a safe default.
func (c SharedFilterConfig) maxAge() time.Duration {
	if c.MaxSnapshotAge > 0 {
		return c.MaxSnapshotAge
	}
	return defaultMaxSnapshotAge
}

// LoadPeerSnapshots reads every {nodeID}.bloom file in cfg.Dir (except the
// file belonging to this node, which is already reflected in the in-memory
// filter) and merges each one into the Checker's filter via bitwise OR.
//
// Snapshots that are too old, corrupt, or have incompatible filter parameters
// are skipped with a Warn log rather than returning an error, so a single bad
// peer file never prevents a job from running.
//
// Call this once at the beginning of each pipeline run so the current job
// benefits from objects already pushed by peer nodes.
func (c *Checker) LoadPeerSnapshots(cfg SharedFilterConfig, obs *observe.Provider) error {
	if !cfg.Enabled || cfg.Dir == "" {
		return nil
	}

	entries, err := os.ReadDir(cfg.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // no snapshots yet — first node to run
		}
		return fmt.Errorf("shared bloom: reading snapshot dir %q: %w", cfg.Dir, err)
	}

	nodeID := cfg.resolvedNodeID()
	ownFile := nodeID + snapshotExt
	cutoff := time.Now().Add(-cfg.maxAge())
	merged := 0

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, snapshotExt) {
			continue
		}
		// Our own snapshot is already in the in-memory filter — skip it.
		if name == ownFile {
			continue
		}
		// Skip temp files from an in-progress write by another node.
		if strings.HasSuffix(name, snapshotTmpExt) {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			obs.Logger.Warn("shared bloom: stat failed", "file", name, "error", err)
			continue
		}
		if info.ModTime().Before(cutoff) {
			obs.Logger.Info("shared bloom: skipping stale snapshot",
				"file", name,
				"age", time.Since(info.ModTime()).Round(time.Minute))
			continue
		}

		peer, err := loadSnapshotFile(filepath.Join(cfg.Dir, name))
		if err != nil {
			obs.Logger.Warn("shared bloom: failed to load snapshot",
				"file", name, "error", err)
			continue
		}

		// Merge requires identical (m, k) parameters.  Incompatible parameters
		// mean the nodes were started with different capacity/FPRate settings —
		// log at Warn so the operator can fix the configuration mismatch.
		c.mu.Lock()
		if err := c.filter.Merge(peer); err != nil {
			c.mu.Unlock()
			obs.Logger.Warn("shared bloom: incompatible snapshot parameters — "+
				"ensure all nodes use the same filter capacity and FPRate",
				"file", name, "error", err)
			continue
		}
		c.mu.Unlock()

		merged++
	}

	if merged > 0 {
		obs.Logger.Info("shared bloom: merged peer snapshots", "count", merged)
	}
	return nil
}

// SaveSnapshot serializes the Checker's current Bloom filter to
// cfg.Dir/{nodeID}.bloom using a write-to-temp-then-rename sequence so a
// crash mid-write never leaves a corrupt snapshot visible to peers.
//
// Call this after each successful pipeline run so peer nodes can benefit from
// the objects added during this run on their next job start.
//
// Errors are non-fatal: log at Warn and continue.
func (c *Checker) SaveSnapshot(cfg SharedFilterConfig, obs *observe.Provider) error {
	if !cfg.Enabled || cfg.Dir == "" {
		return nil
	}

	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return fmt.Errorf("shared bloom: creating snapshot dir %q: %w", cfg.Dir, err)
	}

	nodeID := cfg.resolvedNodeID()
	finalPath := filepath.Join(cfg.Dir, nodeID+snapshotExt)
	tmpPath := filepath.Join(cfg.Dir, nodeID+snapshotTmpExt)

	// Fix: 0640 (owner rw, group r, no world read) instead of 0644.
	// Snapshots reveal CAS object hashes; they must not be readable by
	// arbitrary local users.  0640 still allows build nodes in the same Unix
	// group to load each other's snapshots from the shared NFS/CephFS mount —
	// the typical deployment has all nodes running under the same group.
	// If nodes run as completely different UIDs without a shared group, the
	// admin must set a setgid bit on the snapshot directory.
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0640)
	if err != nil {
		return fmt.Errorf("shared bloom: creating snapshot temp file: %w", err)
	}

	// Hold only a read lock while serialising — the filter content is stable
	// for the duration of a single WriteTo call.
	c.mu.RLock()
	_, writeErr := c.filter.WriteTo(f)
	c.mu.RUnlock()

	if syncErr := f.Sync(); syncErr != nil && writeErr == nil {
		writeErr = fmt.Errorf("shared bloom: syncing snapshot: %w", syncErr)
	}
	f.Close()

	if writeErr != nil {
		os.Remove(tmpPath)
		return writeErr
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("shared bloom: renaming snapshot to final path: %w", err)
	}

	obs.Logger.Info("shared bloom: snapshot saved", "path", finalPath)
	return nil
}

// loadSnapshotFile deserialises a Bloom filter from a single snapshot file.
func loadSnapshotFile(path string) (*bloom.BloomFilter, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening snapshot %q: %w", path, err)
	}
	defer f.Close()

	bf := new(bloom.BloomFilter)
	if _, err := bf.ReadFrom(f); err != nil {
		return nil, fmt.Errorf("deserialising snapshot %q: %w", path, err)
	}
	return bf, nil
}
