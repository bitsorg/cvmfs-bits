// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package commit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"cvmfs.io/prepub/internal/distribute/manifest"
)

// Journal is an append-only, fsync'd record of distribution-transaction phase
// transitions (ADR-0001 R1). On restart the publisher reads it and reconciles
// any transaction left in a non-terminal phase (resume / commit / abort), so a
// crash during the three-phase commit cannot lose objects or strand a GC pin.
type Journal struct {
	mu   sync.Mutex
	path string
}

// OpenJournal returns a Journal backed by path (created on first Append).
func OpenJournal(path string) *Journal { return &Journal{path: path} }

// Append durably records one transaction phase transition.
func (j *Journal) Append(rec manifest.TxnRecord) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	f, err := os.OpenFile(j.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("journal: open: %w", err)
	}
	defer f.Close()
	b, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("journal: marshal: %w", err)
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("journal: write: %w", err)
	}
	return f.Sync()
}

// Records returns all journalled records in chronological (append) order. A
// missing journal reads as empty.
//
// Crash tolerance: Append writes a whole record followed by '\n' and fsyncs, so
// every durably-committed record is newline-terminated. A crash mid-Append can
// therefore only leave a torn segment after the final newline. Such a malformed
// trailing segment is skipped (it is a half-written record that was never
// acknowledged) rather than failing the whole read — otherwise one torn line
// would block restart reconcile. Corruption of any interior line still errors,
// since that indicates real damage, not an interrupted append.
func (j *Journal) Records() ([]manifest.TxnRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	data, err := os.ReadFile(j.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("journal: read: %w", err)
	}
	// Split on '\n'. A well-formed file ends with '\n', so the final element is
	// empty; any non-empty final element is an unterminated (torn) tail.
	lines := bytes.Split(data, []byte{'\n'})
	var recs []manifest.TxnRecord
	for i, line := range lines {
		if len(line) == 0 {
			continue
		}
		var r manifest.TxnRecord
		if err := json.Unmarshal(line, &r); err != nil {
			if i == len(lines)-1 {
				// Unterminated final segment: a torn write from a crash mid-Append.
				// It was never durably committed, so tolerate and stop.
				break
			}
			return nil, fmt.Errorf("journal: corrupt record at line %d: %w", i+1, err)
		}
		recs = append(recs, r)
	}
	return recs, nil
}

// Reconcile returns the latest record of every transaction left in a
// non-terminal phase (Prepare or Warm) — those that crashed mid-flight and must
// be resumed or aborted on restart (ADR R1). Transactions whose last record is
// Commit or Abort are terminal and omitted.
func Reconcile(records []manifest.TxnRecord) []manifest.TxnRecord {
	latest := make(map[string]manifest.TxnRecord, len(records))
	order := make([]string, 0, len(records))
	for _, r := range records {
		if _, seen := latest[r.TxnID]; !seen {
			order = append(order, r.TxnID)
		}
		latest[r.TxnID] = r // append order is chronological → last wins
	}
	var incomplete []manifest.TxnRecord
	for _, id := range order {
		switch latest[id].Phase {
		case manifest.PhasePrepare, manifest.PhaseWarm:
			incomplete = append(incomplete, latest[id])
		}
	}
	return incomplete
}
