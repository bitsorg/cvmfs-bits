// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package serve

import (
	"container/list"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"cvmfs.io/prepub/internal/distribute/manifest"
)

const defaultMaxSpoolManifests = 8192

// SpoolManifestStore is a durable ManifestStore: every manifest is written to a
// JSON file under dir (atomic temp+rename) and an in-memory index serves reads.
// On startup existing files are loaded, so manifests survive a publisher
// restart and stay consistent with the durable distribution queue — a receiver
// triggered after a restart can still GET /s1/{txn}/manifest. Eviction is the
// same O(1) linked-list scheme as MemManifestStore, and Delete removes the file,
// so neither memory nor disk grows beyond the live set.
type SpoolManifestStore struct {
	mu    sync.RWMutex
	dir   string
	byTxn map[string]*list.Element
	order *list.List
	max   int
}

// NewSpoolManifestStore creates (mkdir -p) the directory and loads any manifests
// already present.
func NewSpoolManifestStore(dir string) (*SpoolManifestStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &SpoolManifestStore{dir: dir, byTxn: map[string]*list.Element{}, order: list.New(), max: defaultMaxSpoolManifests}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

// spoolFileName maps a transaction id to a safe filename (defence in depth
// against path traversal; real ids are UUIDs/hashes).
func spoolFileName(txn string) string {
	var b strings.Builder
	for _, r := range txn {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String() + ".json"
}

func (s *SpoolManifestStore) path(txn string) string { return filepath.Join(s.dir, spoolFileName(txn)) }

func (s *SpoolManifestStore) load() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(s.dir, e.Name()))
		if err != nil {
			continue
		}
		var m manifest.Manifest
		if json.Unmarshal(b, &m) != nil || m.TransactionID == "" {
			continue
		}
		mm := m
		s.byTxn[m.TransactionID] = s.order.PushBack(&memEntry{txn: m.TransactionID, m: &mm})
	}
	return nil
}

func (s *SpoolManifestStore) Put(_ context.Context, m *manifest.Manifest) error {
	if err := m.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.byTxn[m.TransactionID]; ok {
		el.Value.(*memEntry).m = m
		s.order.MoveToBack(el)
		return s.writeFile(m)
	}
	for s.max > 0 && len(s.byTxn) >= s.max {
		front := s.order.Front()
		if front == nil {
			break
		}
		old := front.Value.(*memEntry).txn
		s.order.Remove(front)
		delete(s.byTxn, old)
		_ = os.Remove(s.path(old))
	}
	s.byTxn[m.TransactionID] = s.order.PushBack(&memEntry{txn: m.TransactionID, m: m})
	return s.writeFile(m)
}

func (s *SpoolManifestStore) writeFile(m *manifest.Manifest) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tmp := s.path(m.TransactionID) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path(m.TransactionID))
}

func (s *SpoolManifestStore) Delete(txn string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.byTxn[txn]; ok {
		s.order.Remove(el)
		delete(s.byTxn, txn)
	}
	_ = os.Remove(s.path(txn))
}

func (s *SpoolManifestStore) Manifest(_ context.Context, txn string) (*manifest.Manifest, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	el, ok := s.byTxn[txn]
	if !ok {
		return nil, false, nil
	}
	return el.Value.(*memEntry).m, true, nil
}

var _ ManifestStore = (*SpoolManifestStore)(nil)
