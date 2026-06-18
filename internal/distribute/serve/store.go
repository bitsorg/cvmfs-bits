// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package serve

import (
	"container/list"
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

const defaultMaxManifests = 4096

// MemManifestStore is a concurrency-safe, bounded in-memory ManifestStore.
// Eviction uses a doubly linked list keyed by an index map, so insertion,
// lookup, and Delete are all O(1) and the bookkeeping never grows beyond the
// live set (a previous slice-based version leaked because Delete did not trim
// the order slice). For durability across restarts use SpoolManifestStore.
type MemManifestStore struct {
	mu    sync.RWMutex
	byTxn map[string]*list.Element // txn -> element holding *memEntry
	order *list.List               // front = oldest
	max   int
}

type memEntry struct {
	txn string
	m   *manifest.Manifest
}

// NewMemManifestStore returns an empty store.
func NewMemManifestStore() *MemManifestStore {
	return &MemManifestStore{byTxn: map[string]*list.Element{}, order: list.New(), max: defaultMaxManifests}
}

func (s *MemManifestStore) Put(_ context.Context, m *manifest.Manifest) error {
	if err := m.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.byTxn[m.TransactionID]; ok {
		el.Value.(*memEntry).m = m
		s.order.MoveToBack(el)
		return nil
	}
	for s.max > 0 && len(s.byTxn) >= s.max {
		front := s.order.Front()
		if front == nil {
			break
		}
		old := front.Value.(*memEntry).txn
		s.order.Remove(front)
		delete(s.byTxn, old)
	}
	s.byTxn[m.TransactionID] = s.order.PushBack(&memEntry{txn: m.TransactionID, m: m})
	return nil
}

// Delete drops a manifest (e.g. once its warm quorum is reached). O(1); fully
// removes the entry from both the index and the eviction list.
func (s *MemManifestStore) Delete(txn string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.byTxn[txn]; ok {
		s.order.Remove(el)
		delete(s.byTxn, txn)
	}
}

func (s *MemManifestStore) Manifest(_ context.Context, txn string) (*manifest.Manifest, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	el, ok := s.byTxn[txn]
	if !ok {
		return nil, false, nil
	}
	return el.Value.(*memEntry).m, true, nil
}

var _ ManifestStore = (*MemManifestStore)(nil)
