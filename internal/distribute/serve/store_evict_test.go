// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package serve

import (
	"context"
	"testing"

	"cvmfs.io/prepub/internal/distribute/manifest"
)

func mkManifest(id string) *manifest.Manifest {
	return &manifest.Manifest{
		TransactionID:  id,
		Repo:           "r.cern.ch",
		TargetRootHash: "deadbeef",
		BaseURLs:       []string{"http://s0/cvmfs/r.cern.ch/data"},
		Generator:      manifest.GeneratorPipeline,
		Auth:           manifest.AuthPublic,
	}
}

// TestMemManifestStoreEviction verifies the bounded store evicts oldest entries
// (R-DoS: no unbounded growth) and Delete works.
func TestMemManifestStoreEviction(t *testing.T) {
	s := NewMemManifestStore()
	s.max = 3
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		if err := s.Put(ctx, mkManifest(id)); err != nil {
			t.Fatalf("Put(%s): %v", id, err)
		}
	}
	if len(s.byTxn) != 3 {
		t.Fatalf("want 3 entries after cap, got %d", len(s.byTxn))
	}
	for _, gone := range []string{"a", "b"} {
		if _, ok, _ := s.Manifest(ctx, gone); ok {
			t.Errorf("%s should have been evicted", gone)
		}
	}
	for _, kept := range []string{"c", "d", "e"} {
		if _, ok, _ := s.Manifest(ctx, kept); !ok {
			t.Errorf("%s should be present", kept)
		}
	}
	// Re-Put an existing key must not grow the store or evict.
	if err := s.Put(ctx, mkManifest("e")); err != nil {
		t.Fatal(err)
	}
	if len(s.byTxn) != 3 {
		t.Fatalf("re-Put grew store to %d", len(s.byTxn))
	}
	s.Delete("e")
	if _, ok, _ := s.Manifest(ctx, "e"); ok {
		t.Error("e should be deleted")
	}
}
