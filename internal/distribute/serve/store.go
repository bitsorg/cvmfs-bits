// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package serve

import (
	"context"
	"sync"

	"cvmfs.io/prepub/internal/distribute/manifest"
)

// ManifestStore holds transaction manifests for serving to receivers. It has two
// producers — the prepub publish pipeline (via BuildManifest) and the CVMFS
// gateway (via ManifestIngestHandler) — and one consumer, ManifestHandler.
type ManifestStore interface {
	ManifestSource
	// Put validates and stores m under m.TransactionID, replacing any existing
	// manifest for that transaction.
	Put(ctx context.Context, m *manifest.Manifest) error
}

// MemManifestStore is a concurrency-safe in-memory ManifestStore. Durable
// storage (so manifests survive a prepub restart) is wired with the transaction
// journal in P3.
type MemManifestStore struct {
	mu    sync.RWMutex
	byTxn map[string]*manifest.Manifest
}

// NewMemManifestStore returns an empty store.
func NewMemManifestStore() *MemManifestStore {
	return &MemManifestStore{byTxn: map[string]*manifest.Manifest{}}
}

func (s *MemManifestStore) Put(_ context.Context, m *manifest.Manifest) error {
	if err := m.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byTxn[m.TransactionID] = m
	return nil
}

func (s *MemManifestStore) Manifest(_ context.Context, txn string) (*manifest.Manifest, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.byTxn[txn]
	return m, ok, nil
}

var _ ManifestStore = (*MemManifestStore)(nil)
