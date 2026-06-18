// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package serve

import (
	"context"
	"testing"

	"cvmfs.io/prepub/internal/distribute/manifest"
)

func mkSpoolManifest(txn string) *manifest.Manifest {
	return &manifest.Manifest{
		TransactionID:  txn,
		Repo:           "test.cvmfs.io",
		TargetRootHash: txn,
		BaseURLs:       []string{"http://s0/cvmfs/test.cvmfs.io/data"},
		Generator:      manifest.GeneratorPipeline,
		Auth:           manifest.AuthPublic,
		Objects:        []manifest.ObjRef{{Hash: "abc"}},
	}
}

func TestSpoolManifestStoreDurability(t *testing.T) {
	dir := t.TempDir()
	s, err := NewSpoolManifestStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, id := range []string{"t1", "t2", "t3"} {
		if err := s.Put(ctx, mkSpoolManifest(id)); err != nil {
			t.Fatal(err)
		}
	}
	// Reopen on the same dir: manifests must survive (durability / restart).
	s2, err := NewSpoolManifestStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"t1", "t2", "t3"} {
		if _, ok, _ := s2.Manifest(ctx, id); !ok {
			t.Errorf("manifest %s did not survive reopen", id)
		}
	}
	// Delete removes from index AND disk; a fresh reopen must not see it.
	s2.Delete("t2")
	if _, ok, _ := s2.Manifest(ctx, "t2"); ok {
		t.Error("t2 still present after Delete")
	}
	s3, _ := NewSpoolManifestStore(dir)
	if _, ok, _ := s3.Manifest(ctx, "t2"); ok {
		t.Error("t2 file not removed from disk on Delete")
	}
}

func TestSpoolManifestStoreEvictionBounded(t *testing.T) {
	s, err := NewSpoolManifestStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s.max = 4
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = s.Put(ctx, mkSpoolManifest(string(rune('A'+i%26))+string(rune('0'+i/26))))
	}
	s.mu.RLock()
	n := len(s.byTxn)
	ord := s.order.Len()
	s.mu.RUnlock()
	if n > 4 || ord > 4 {
		t.Errorf("store exceeded bound: byTxn=%d order=%d (max 4)", n, ord)
	}
}

func TestMemManifestStoreNoLeakAfterDelete(t *testing.T) {
	s := NewMemManifestStore()
	ctx := context.Background()
	// Publish-then-delete many times (the warm-quorum pattern). The eviction
	// list must not grow unboundedly past the live set.
	for i := 0; i < 10000; i++ {
		id := "txn-" + string(rune('0'+i%10)) + "-" + string(rune('a'+i%26))
		_ = s.Put(ctx, mkSpoolManifest(id))
		s.Delete(id)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.order.Len() != 0 || len(s.byTxn) != 0 {
		t.Errorf("leak: order=%d byTxn=%d after publish+delete cycles (want 0/0)", s.order.Len(), len(s.byTxn))
	}
}
