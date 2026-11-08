// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/lease"
	"cvmfs.io/prepub/internal/measure"
)

// batchFake is an ingest-shaped backend that records single and batch commits.
type batchFake struct {
	noopBackend
	mu      sync.Mutex
	singles []string
	batches []string // "base: rel,rel"
	fail    bool
}

func (b *batchFake) Commit(_ context.Context, req lease.CommitRequest) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.singles = append(b.singles, req.CVMFSDir[strings.Index(req.CVMFSDir, "x86_64"):])
	return nil
}

func (b *batchFake) CommitBatch(_ context.Context, req lease.BatchCommitRequest) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var rels []string
	for _, m := range req.Members {
		rels = append(rels, m.RelPath)
	}
	sort.Strings(rels)
	b.batches = append(b.batches, req.Base+": "+strings.Join(rels, ","))
	if b.fail {
		return errors.New("ingest refused the batch")
	}
	n, objs := int64(300), 9
	req.Stats.Backend, req.Stats.TarBytes, req.Stats.Objects = 2*time.Second, &n, &objs
	return nil
}

func (b *batchFake) seen() ([]string, []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := append([]string(nil), b.singles...)
	sort.Strings(s)
	return s, append([]string(nil), b.batches...)
}

// runBatched submits ingest jobs for paths at once under build "b1" and
// returns the backend and the records, after every job has finished.
func runBatched(t *testing.T, cfg IngestBatchConfig, fail bool, extra map[string]map[string]string,
	paths ...string) (*batchFake, []measure.Record) {
	return runOnPath(t, IngestBatchPublishPath, cfg, fail, extra, paths...)
}

func runOnPath(t *testing.T, publishPath string, cfg IngestBatchConfig, fail bool,
	extra map[string]map[string]string, paths ...string) (*batchFake, []measure.Record) {
	t.Helper()
	srv, _, orch := newTestServer(t)
	w := withMeasurements(t, orch)
	be := &batchFake{fail: fail}
	orch.PublishPaths = map[string]lease.Backend{IngestPublishPath: be, IngestBatchPublishPath: be}
	orch.IngestBatch = cfg
	if extra != nil {
		orch.Stratum0URL = "http://stratum0.test"
	}
	for _, p := range paths {
		fields := map[string]string{"repo": "software.cern.ch", "path": p,
			"publish_path": publishPath, "build_id": "b1"}
		for k, v := range extra[p] {
			fields[k] = v
		}
		submitOne(t, srv, fields)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	recs, err := w.Read("b1")
	if err != nil {
		t.Fatal(err)
	}
	return be, recs
}

const pkgs = "x86_64-el9/Packages/"

func TestIngestBatch_OneCommitForSeveralPackages(t *testing.T) {
	be, recs := runBatched(t, IngestBatchConfig{Max: 3, Linger: 500 * time.Millisecond}, false, nil,
		pkgs+"A/1-1", pkgs+"B/1-1", pkgs+"C/1-1")
	singles, batches := be.seen()
	if len(singles) != 0 || len(batches) != 1 || batches[0] != "x86_64-el9/Packages: A/1-1,B/1-1,C/1-1" {
		t.Fatalf("singles %v, batches %v; want one batch of A, B, C at the Packages directory", singles, batches)
	}
	ids := map[string]bool{}
	for _, r := range recs {
		if r.Outcome != "published" || r.BatchSize != 3 || r.BatchBytes == nil || *r.BatchBytes != 300 ||
			r.BackendS == nil || *r.BackendS != 2 || r.Objects == nil || *r.Objects != 9 {
			t.Errorf("record %+v: want published in a batch of 3 with its numbers", r)
		}
		if r.TarBytes == nil || *r.TarBytes == 300 {
			t.Errorf("record %s: tar_bytes %v, want the package's own", r.Path, r.TarBytes)
		}
		ids[r.BatchID] = true
	}
	if len(recs) != 3 || len(ids) != 1 || ids[""] {
		t.Errorf("%d records with batch ids %v, want 3 sharing one", len(recs), ids)
	}
	if s := measure.Summarise(recs); s.Backend.N != 1 || s.Batches != 1 {
		t.Errorf("summary backend n=%d batches=%d, want the batch counted once", s.Backend.N, s.Batches)
	}
}

// The batch takes at most Max; a package left alone publishes as today.
func TestIngestBatch_MaxSplitsAndALoneOneGoesAsToday(t *testing.T) {
	be, recs := runBatched(t, IngestBatchConfig{Max: 2, Linger: 500 * time.Millisecond}, false, nil,
		pkgs+"A/1-1", pkgs+"B/1-1", pkgs+"C/1-1")
	singles, batches := be.seen()
	if len(batches) != 1 || len(singles) != 1 || len(recs) != 3 {
		t.Fatalf("singles %v, batches %v, %d records; want one batch of 2 and one alone", singles, batches, len(recs))
	}
	for _, r := range recs {
		if r.Outcome != "published" {
			t.Errorf("%s: %s", r.Path, r.Outcome)
		}
	}
}

// A failed batch publishes its packages one by one, and they do not batch again.
func TestIngestBatch_FailedBatchFallsBack(t *testing.T) {
	be, recs := runBatched(t, IngestBatchConfig{Max: 3, Linger: 500 * time.Millisecond}, true, nil,
		pkgs+"A/1-1", pkgs+"B/1-1", pkgs+"C/1-1")
	singles, batches := be.seen()
	if len(batches) != 1 || len(singles) != 3 {
		t.Fatalf("singles %v, batches %v; want one failed batch, then three alone", singles, batches)
	}
	for _, r := range recs {
		if r.Outcome != "published" || r.BatchID != "" {
			t.Errorf("record %+v: want published alone", r)
		}
	}
}

// Under the lock each member is checked: one already published ends as such,
// one whose directory exists goes alone, the last left goes alone too.
func TestIngestBatch_ChecksEachMember(t *testing.T) {
	stubExistsSet(t, pkgs+"B/1-1/.cvmfscatalog", pkgs+"C/1-1")
	be, recs := runBatched(t, IngestBatchConfig{Max: 3, Linger: 500 * time.Millisecond}, false,
		map[string]map[string]string{pkgs + "C/1-1": {"identity_path": pkgs + "C/1-1"}, pkgs + "A/1-1": {},
			pkgs + "B/1-1": {}},
		pkgs+"A/1-1", pkgs+"B/1-1", pkgs+"C/1-1")
	singles, batches := be.seen()
	if len(batches) != 0 || strings.Join(singles, " ") != pkgs+"A/1-1 "+pkgs+"B/1-1" {
		t.Fatalf("singles %v, batches %v; want A and B alone, C not committed", singles, batches)
	}
	outcomes := map[string]string{}
	for _, r := range recs {
		outcomes[r.Path] = r.Outcome
	}
	if outcomes[pkgs+"C/1-1"] != "already_published" || outcomes[pkgs+"A/1-1"] != "published" {
		t.Errorf("outcomes %v", outcomes)
	}
}

// A package inside another one of the batch waits for a later commit rather
// than failing this one.
func TestIngestBatch_OverlappingPathWaits(t *testing.T) {
	be, recs := runBatched(t, IngestBatchConfig{Max: 3, Linger: 500 * time.Millisecond}, false, nil,
		pkgs+"A/1-1", pkgs+"A/1-1/x", pkgs+"B/1-1")
	singles, batches := be.seen()
	if len(batches) != 1 || batches[0] != "x86_64-el9/Packages: A/1-1,B/1-1" ||
		len(singles) != 1 || singles[0] != pkgs+"A/1-1/x" {
		t.Fatalf("singles %v, batches %v; want A and B together, A/1-1/x after", singles, batches)
	}
	for _, r := range recs {
		if r.Outcome != "published" {
			t.Errorf("%s: %s", r.Path, r.Outcome)
		}
	}
}

// The plain ingest path never batches, whatever the node's settings.
func TestIngestBatch_PlainIngestIsUnchanged(t *testing.T) {
	be, recs := runOnPath(t, IngestPublishPath, IngestBatchConfig{Max: 3, Linger: 500 * time.Millisecond},
		false, nil, pkgs+"A/1-1", pkgs+"B/1-1")
	if singles, batches := be.seen(); len(batches) != 0 || len(singles) != 2 {
		t.Fatalf("singles %v, batches %v; want both alone", singles, batches)
	}
	for _, r := range recs {
		if r.PublishPath != IngestPublishPath || r.BatchID != "" {
			t.Errorf("record %+v", r)
		}
	}
}

func TestIngestBatch_OffIsToday(t *testing.T) {
	be, _ := runBatched(t, IngestBatchConfig{Max: 1, Linger: time.Second}, false, nil,
		pkgs+"A/1-1", pkgs+"B/1-1")
	if singles, batches := be.seen(); len(batches) != 0 || len(singles) != 2 {
		t.Fatalf("singles %v, batches %v; want both alone", singles, batches)
	}
}

func TestBatchBase(t *testing.T) {
	for _, c := range []struct {
		paths []string
		base  string
		ok    bool
	}{
		{[]string{"a/P/x/1", "a/P/y/1"}, "a/P", true},
		{[]string{"a/P/x/1", "a/M/x"}, "a", true},
		{[]string{"a/x", "b/y"}, "", true},
		{[]string{"a/P", "a/P/x"}, "", false},
		{[]string{"a/Px/1", "a/Py/1"}, "a", true},
	} {
		base, ok := batchBase(c.paths)
		if base != c.base || ok != c.ok {
			t.Errorf("batchBase(%v) = %q, %v; want %q, %v", c.paths, base, ok, c.base, c.ok)
		}
	}
}
