// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package commit

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/distribute/manifest"
)

func TestAdmissionConcurrencyAndPerNode(t *testing.T) {
	a := NewAdmission(Options{MaxConcurrent: 2, MaxPerNode: 1})

	l1, ok := a.Grant("n1", "txn")
	if !ok {
		t.Fatal("first grant should succeed")
	}
	if _, ok := a.Grant("n1", "txn"); ok {
		t.Fatal("per-node cap should deny a second grant to n1")
	}
	if _, ok := a.Grant("n2", "txn"); !ok {
		t.Fatal("n2 should get the second global slot")
	}
	if _, ok := a.Grant("n3", "txn"); ok {
		t.Fatal("global cap (2) should deny n3")
	}
	a.Release(l1.ID)
	if _, ok := a.Grant("n3", "txn"); !ok {
		t.Fatal("after release, a slot should be free for n3")
	}
}

func TestAdmissionSweepExpires(t *testing.T) {
	a := NewAdmission(Options{TTL: time.Nanosecond})
	l, _ := a.Grant("n", "txn")
	if a.Active() != 1 {
		t.Fatalf("active = %d, want 1", a.Active())
	}
	if n := a.Sweep(time.Now().Add(time.Millisecond)); n != 1 {
		t.Fatalf("sweep released %d, want 1", n)
	}
	if a.Renew(l.ID) {
		t.Fatal("renew of a swept lease should fail")
	}
}

func TestWarmGateQuorum(t *testing.T) {
	g := NewWarmGate([]string{"a", "b", "c"}, 2) // quorum 2 of 3 authoritative
	ctx := context.Background()

	// Non-authoritative ack does not count.
	g.Ack("t", "stranger")
	if g.Reached("t") {
		t.Fatal("stranger ack must not reach quorum")
	}

	go func() {
		g.Ack("t", "a")
		g.Ack("t", "b")
	}()
	if !g.WaitQuorum(ctx, "t", time.Second) {
		t.Fatal("quorum should be reached after a+b ack")
	}
	if acked, req := g.Warmth("t"); acked < 2 || req != 2 {
		t.Fatalf("warmth = %d/%d, want >=2/2", acked, req)
	}
	g.Forget("t")
	if acked, _ := g.Warmth("t"); acked != 0 {
		t.Fatalf("after Forget, acked = %d, want 0", acked)
	}
}

func TestWarmGateTimeout(t *testing.T) {
	g := NewWarmGate([]string{"a", "b"}, 0) // default quorum = all (2)
	if g.WaitQuorum(context.Background(), "t", 50*time.Millisecond) {
		t.Fatal("WaitQuorum should time out with no acks")
	}
}

func TestJournalAppendAndReconcile(t *testing.T) {
	jp := filepath.Join(t.TempDir(), "txn.jsonl")
	j := OpenJournal(jp)

	// empty journal
	if recs, err := j.Records(); err != nil || len(recs) != 0 {
		t.Fatalf("empty journal: recs=%v err=%v", recs, err)
	}

	now := time.Now()
	must := func(r manifest.TxnRecord) {
		if err := j.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	// txn-1: prepare -> warm -> commit (terminal)
	must(manifest.TxnRecord{TxnID: "txn-1", Repo: "r", Phase: manifest.PhasePrepare, At: now})
	must(manifest.TxnRecord{TxnID: "txn-1", Repo: "r", Phase: manifest.PhaseWarm, At: now})
	must(manifest.TxnRecord{TxnID: "txn-1", Repo: "r", Phase: manifest.PhaseCommit, At: now})
	// txn-2: prepare -> warm  (crashed mid-flight)
	must(manifest.TxnRecord{TxnID: "txn-2", Repo: "r", Phase: manifest.PhasePrepare, GCPin: "pin-2", At: now})
	must(manifest.TxnRecord{TxnID: "txn-2", Repo: "r", Phase: manifest.PhaseWarm, GCPin: "pin-2", At: now})
	// txn-3: prepare -> abort (terminal)
	must(manifest.TxnRecord{TxnID: "txn-3", Repo: "r", Phase: manifest.PhasePrepare, At: now})
	must(manifest.TxnRecord{TxnID: "txn-3", Repo: "r", Phase: manifest.PhaseAbort, At: now})

	recs, err := j.Records()
	if err != nil || len(recs) != 7 {
		t.Fatalf("records: n=%d err=%v", len(recs), err)
	}

	inc := Reconcile(recs)
	if len(inc) != 1 || inc[0].TxnID != "txn-2" || inc[0].GCPin != "pin-2" {
		t.Fatalf("reconcile should yield only txn-2 (warm, pinned): %+v", inc)
	}
}

// TestJournalTornTrailingLine simulates a crash mid-Append that left a partial,
// unterminated final record. Records must skip the torn tail (not fail the whole
// read) so restart reconcile still works on the intact prefix.
func TestJournalTornTrailingLine(t *testing.T) {
	jp := filepath.Join(t.TempDir(), "txn.jsonl")
	j := OpenJournal(jp)

	now := time.Now()
	if err := j.Append(manifest.TxnRecord{TxnID: "txn-1", Repo: "r", Phase: manifest.PhasePrepare, GCPin: "pin-1", At: now}); err != nil {
		t.Fatal(err)
	}
	// Append a torn, unterminated record as a crash would leave behind.
	f, err := os.OpenFile(jp, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"TxnID":"txn-2","Phase":"war`); err != nil {
		t.Fatal(err)
	}
	f.Close()

	recs, err := j.Records()
	if err != nil {
		t.Fatalf("torn tail must be tolerated, got err=%v", err)
	}
	if len(recs) != 1 || recs[0].TxnID != "txn-1" {
		t.Fatalf("want only the intact txn-1, got %+v", recs)
	}
	if inc := Reconcile(recs); len(inc) != 1 || inc[0].TxnID != "txn-1" {
		t.Fatalf("reconcile should still surface txn-1: %+v", inc)
	}
}

// TestJournalInteriorCorruptionErrors verifies that corruption of a non-trailing
// line is NOT silently tolerated — only a torn final segment is.
func TestJournalInteriorCorruptionErrors(t *testing.T) {
	jp := filepath.Join(t.TempDir(), "txn.jsonl")
	if err := os.WriteFile(jp, []byte("{bad json}\n{\"TxnID\":\"ok\",\"Phase\":\"prepare\"}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenJournal(jp).Records(); err == nil {
		t.Fatal("interior corruption should error")
	}
}
