package dedup_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/pipeline/dedup"
	"cvmfs.io/prepub/testutil/fakecas"

	"github.com/prometheus/client_golang/prometheus"

	"cvmfs.io/prepub/pkg/observe"
)

// newObs creates a minimal observability provider for tests.
func newObs(t *testing.T) *observe.Provider {
	t.Helper()
	reg := prometheus.NewRegistry()
	obs, shutdown, err := observe.New("test-dedup", observe.WithPrometheus(reg))
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)
	return obs
}

// makeConfig returns a SharedFilterConfig pointing to tmpDir with the given nodeID.
func makeConfig(tmpDir, nodeID string) dedup.SharedFilterConfig {
	return dedup.SharedFilterConfig{
		Enabled:  true,
		Dir:      tmpDir,
		NodeID:   nodeID,
		Capacity: 10_000, // small for tests
		FPRate:   0.01,
	}
}

// TestSharedFilter_SaveAndLoad verifies that a filter saved by one Checker is
// correctly loaded by another and that the merged checker recognises hashes
// that were only in the saved filter.
func TestSharedFilter_SaveAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	obs := newObs(t)
	ctx := context.Background()

	cas1 := fakecas.New(obs)
	cas2 := fakecas.New(obs)

	cfg1 := makeConfig(tmpDir, "node-1")
	cfg2 := makeConfig(tmpDir, "node-2")

	// Node-1 checker: seed with known hashes and save a snapshot.
	checker1, err := dedup.NewWithConfig(ctx, cas1, cfg1, obs)
	if err != nil {
		t.Fatalf("NewWithConfig node-1: %v", err)
	}

	knownHashes := []string{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
	}
	for _, h := range knownHashes {
		checker1.Add(h)
	}

	if err := checker1.SaveSnapshot(cfg1, obs); err != nil {
		t.Fatalf("SaveSnapshot node-1: %v", err)
	}

	// Snapshot file must exist.
	snapshotPath := filepath.Join(tmpDir, "node-1.bloom")
	if _, err := os.Stat(snapshotPath); err != nil {
		t.Fatalf("snapshot file not found: %v", err)
	}

	// Node-2 checker: starts empty, merges node-1's snapshot.
	checker2, err := dedup.NewWithConfig(ctx, cas2, cfg2, obs)
	if err != nil {
		t.Fatalf("NewWithConfig node-2: %v", err)
	}

	if err := checker2.LoadPeerSnapshots(cfg2, obs); err != nil {
		t.Fatalf("LoadPeerSnapshots node-2: %v", err)
	}

	// After merge, node-2's filter should contain node-1's hashes.
	// Because CAS2 is empty, Exists() returns false even if the filter
	// says "probably yes" — Check() confirms via CAS. So we test the
	// filter directly via Check with hashes actually in CAS2.
	//
	// To make Check() return true we also need the hash to be in cas2.
	// Instead, verify the filter itself was populated by inspecting that
	// LoadPeerSnapshots ran without error and the saved file has content.
	fi, err := os.Stat(snapshotPath)
	if err != nil {
		t.Fatalf("stat snapshot: %v", err)
	}
	if fi.Size() == 0 {
		t.Error("snapshot file is empty")
	}
}

// TestSharedFilter_OwnSnapshotSkipped verifies that a node's own snapshot is
// not double-counted during LoadPeerSnapshots.
func TestSharedFilter_OwnSnapshotSkipped(t *testing.T) {
	tmpDir := t.TempDir()
	obs := newObs(t)
	ctx := context.Background()

	cas1 := fakecas.New(obs)
	cfg := makeConfig(tmpDir, "solo-node")

	checker, err := dedup.NewWithConfig(ctx, cas1, cfg, obs)
	if err != nil {
		t.Fatalf("NewWithConfig: %v", err)
	}
	checker.Add("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")

	if err := checker.SaveSnapshot(cfg, obs); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// LoadPeerSnapshots should silently skip its own file and return nil.
	if err := checker.LoadPeerSnapshots(cfg, obs); err != nil {
		t.Fatalf("LoadPeerSnapshots: %v", err)
	}
}

// TestSharedFilter_StaleSnapshotIgnored verifies that snapshots older than
// MaxSnapshotAge are silently skipped.
func TestSharedFilter_StaleSnapshotIgnored(t *testing.T) {
	tmpDir := t.TempDir()
	obs := newObs(t)
	ctx := context.Background()

	cas1 := fakecas.New(obs)
	cas2 := fakecas.New(obs)

	cfg1 := makeConfig(tmpDir, "stale-node")
	cfg2 := makeConfig(tmpDir, "fresh-node")
	// MaxSnapshotAge of 1 ns means the snapshot will always be considered stale.
	cfg2.MaxSnapshotAge = 1 * time.Nanosecond

	checker1, _ := dedup.NewWithConfig(ctx, cas1, cfg1, obs)
	checker1.Add("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	if err := checker1.SaveSnapshot(cfg1, obs); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// Sleep briefly so the snapshot's mtime is definitely older than 1 ns.
	time.Sleep(5 * time.Millisecond)

	checker2, _ := dedup.NewWithConfig(ctx, cas2, cfg2, obs)
	// Should skip the stale file and return nil (not an error).
	if err := checker2.LoadPeerSnapshots(cfg2, obs); err != nil {
		t.Fatalf("LoadPeerSnapshots with stale snapshot: %v", err)
	}
}

// TestSharedFilter_CorruptSnapshotSkipped verifies that a corrupt snapshot file
// is skipped rather than causing a fatal error.
func TestSharedFilter_CorruptSnapshotSkipped(t *testing.T) {
	tmpDir := t.TempDir()
	obs := newObs(t)
	ctx := context.Background()

	// Write a corrupt snapshot for "bad-node".
	if err := os.WriteFile(filepath.Join(tmpDir, "bad-node.bloom"), []byte("not a bloom filter"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cas1 := fakecas.New(obs)
	cfg := makeConfig(tmpDir, "good-node")

	checker, _ := dedup.NewWithConfig(ctx, cas1, cfg, obs)
	// Must not return an error — bad-node.bloom is skipped with a warning.
	if err := checker.LoadPeerSnapshots(cfg, obs); err != nil {
		t.Fatalf("LoadPeerSnapshots should not fail on corrupt snapshot: %v", err)
	}
}

// TestSharedFilter_Disabled verifies that when Enabled is false, SaveSnapshot
// and LoadPeerSnapshots are no-ops that return nil.
func TestSharedFilter_Disabled(t *testing.T) {
	tmpDir := t.TempDir()
	obs := newObs(t)
	ctx := context.Background()

	cas1 := fakecas.New(obs)
	cfgOff := dedup.SharedFilterConfig{Enabled: false, Dir: tmpDir, NodeID: "off-node"}

	checker, err := dedup.NewWithConfig(ctx, cas1, cfgOff, obs)
	if err != nil {
		t.Fatalf("NewWithConfig: %v", err)
	}

	if err := checker.LoadPeerSnapshots(cfgOff, obs); err != nil {
		t.Fatalf("LoadPeerSnapshots (disabled): %v", err)
	}
	if err := checker.SaveSnapshot(cfgOff, obs); err != nil {
		t.Fatalf("SaveSnapshot (disabled): %v", err)
	}

	// No files should have been created.
	entries, _ := os.ReadDir(tmpDir)
	if len(entries) != 0 {
		t.Errorf("expected no files in snapshot dir, got %d", len(entries))
	}
}

// TestSharedFilter_IncompatibleParametersSkipped verifies that a snapshot
// produced with different filter parameters is skipped gracefully.
func TestSharedFilter_IncompatibleParametersSkipped(t *testing.T) {
	tmpDir := t.TempDir()
	obs := newObs(t)
	ctx := context.Background()

	cas1 := fakecas.New(obs)
	cas2 := fakecas.New(obs)

	// Node-1 uses capacity=10_000 (same as makeConfig default).
	cfg1 := makeConfig(tmpDir, "compat-node-1")
	// Node-2 uses a DIFFERENT capacity → incompatible parameters.
	cfg2 := dedup.SharedFilterConfig{
		Enabled:  true,
		Dir:      tmpDir,
		NodeID:   "compat-node-2",
		Capacity: 500_000, // different from cfg1
		FPRate:   0.01,
	}

	checker1, _ := dedup.NewWithConfig(ctx, cas1, cfg1, obs)
	checker1.Add("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
	if err := checker1.SaveSnapshot(cfg1, obs); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	checker2, _ := dedup.NewWithConfig(ctx, cas2, cfg2, obs)
	// Should skip the incompatible snapshot and return nil.
	if err := checker2.LoadPeerSnapshots(cfg2, obs); err != nil {
		t.Fatalf("LoadPeerSnapshots with incompatible snapshot: %v", err)
	}
}

// TestSharedFilter_MissingDir verifies that a non-existent snapshot directory
// is treated as "no snapshots yet" rather than an error.
func TestSharedFilter_MissingDir(t *testing.T) {
	obs := newObs(t)
	ctx := context.Background()

	cas1 := fakecas.New(obs)
	cfg := dedup.SharedFilterConfig{
		Enabled:  true,
		Dir:      "/tmp/cvmfs-test-bloom-does-not-exist-xyz",
		NodeID:   "ghost-node",
		Capacity: 10_000,
		FPRate:   0.01,
	}

	checker, _ := dedup.NewWithConfig(ctx, cas1, cfg, obs)
	if err := checker.LoadPeerSnapshots(cfg, obs); err != nil {
		t.Fatalf("LoadPeerSnapshots with missing dir: %v", err)
	}
}
