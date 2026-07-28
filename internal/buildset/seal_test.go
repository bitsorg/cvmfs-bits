// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

package buildset

// Tests for the deferred-finalize control files: the declared member count, the
// member tally, and the single-winner finalize claim.

import (
	"sync"
	"testing"
)

func TestExpectRoundTrip(t *testing.T) {
	root := t.TempDir()

	if got := Expect(root, "b1"); got != 0 {
		t.Errorf("undeclared build: want 0, got %d", got)
	}
	if err := SetExpect(root, "b1", 87); err != nil {
		t.Fatalf("SetExpect: %v", err)
	}
	if got := Expect(root, "b1"); got != 87 {
		t.Errorf("want 87, got %d", got)
	}
	// A corrected count wins — the producer may re-seal.
	if err := SetExpect(root, "b1", 90); err != nil {
		t.Fatalf("SetExpect (update): %v", err)
	}
	if got := Expect(root, "b1"); got != 90 {
		t.Errorf("want 90, got %d", got)
	}
	// Zero clears the declaration; auto-finalize must then stay off.
	if err := SetExpect(root, "b1", 0); err != nil {
		t.Fatalf("SetExpect (clear): %v", err)
	}
	if got := Expect(root, "b1"); got != 0 {
		t.Errorf("after clear: want 0, got %d", got)
	}
	// Clearing an already-clear build is not an error.
	if err := SetExpect(root, "never-seen", 0); err != nil {
		t.Errorf("clearing unknown build: %v", err)
	}
}

// TestCountIgnoresControlFiles pins the naming contract: the control files sit
// in the same directory as the members, and neither Count nor Load may see them.
func TestCountIgnoresControlFiles(t *testing.T) {
	root := t.TempDir()

	for _, id := range []string{"job-a", "job-b"} {
		if err := Record(root, "b1", Member{JobID: id, Repo: "r", Path: "p/" + id}); err != nil {
			t.Fatalf("Record %s: %v", id, err)
		}
	}
	if err := SetExpect(root, "b1", 2); err != nil {
		t.Fatalf("SetExpect: %v", err)
	}
	if _, err := ClaimFinalize(root, "b1"); err != nil {
		t.Fatalf("ClaimFinalize: %v", err)
	}

	if got := Count(root, "b1"); got != 2 {
		t.Errorf("Count: want 2, got %d", got)
	}
	members, err := Load(root, "b1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(members) != 2 {
		t.Errorf("Load: want 2 members, got %d", len(members))
	}
}

// TestClaimFinalizeIsExclusive verifies that when several job goroutines see the
// build complete at the same moment, exactly one of them finalizes it.
func TestClaimFinalizeIsExclusive(t *testing.T) {
	root := t.TempDir()

	const goroutines = 16
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		won  int
		errs []error
	)
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ok, err := ClaimFinalize(root, "b1")
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			if ok {
				won++
			}
		}()
	}
	close(start)
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("ClaimFinalize errors: %v", errs)
	}
	if won != 1 {
		t.Errorf("want exactly 1 winner, got %d", won)
	}
	if !Finalizing(root, "b1") {
		t.Error("Finalizing should report true after a claim")
	}

	// Releasing lets a later attempt through — used when the finalize failed
	// before touching repository state.
	if err := ReleaseFinalize(root, "b1"); err != nil {
		t.Fatalf("ReleaseFinalize: %v", err)
	}
	if Finalizing(root, "b1") {
		t.Error("Finalizing should report false after release")
	}
	ok, err := ClaimFinalize(root, "b1")
	if err != nil || !ok {
		t.Errorf("re-claim after release: ok=%v err=%v", ok, err)
	}
	// Releasing twice is harmless.
	if err := ReleaseFinalize(root, "unknown-build"); err != nil {
		t.Errorf("release of unknown build: %v", err)
	}
}

// TestFailuresCountTowardsTerminal pins the property that makes a sealed build
// decidable: a package that FAILS still lets the build reach its declared
// count, so the finalize logic runs (and refuses) instead of the build waiting
// forever for a member that will never arrive.
func TestFailuresCountTowardsTerminal(t *testing.T) {
	root := t.TempDir()

	if err := Record(root, "b1", Member{JobID: "ok-1", Repo: "r", Path: "p/1"}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if err := MarkFailed(root, "b1", "bad-1", "pipeline_error"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}

	if got := Count(root, "b1"); got != 1 {
		t.Errorf("Count: want 1 (members only), got %d", got)
	}
	if got := Terminal(root, "b1"); got != 2 {
		t.Errorf("Terminal: want 2 (members + failures), got %d", got)
	}
	failures := Failures(root, "b1")
	if len(failures) != 1 || failures[0] != "bad-1" {
		t.Errorf("Failures: want [bad-1], got %v", failures)
	}
	// The failure marker must not be mistaken for a member.
	members, err := Load(root, "b1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(members) != 1 || members[0].JobID != "ok-1" {
		t.Errorf("Load: want just ok-1, got %+v", members)
	}
}

// TestResultSurvivesRemove verifies that the finalize outcome outlives the
// accumulator directory, which a successful finalize deletes.
func TestResultSurvivesRemove(t *testing.T) {
	root := t.TempDir()

	if err := Record(root, "b1", Member{JobID: "j1", Repo: "r", Path: "p"}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if err := WriteResult(root, Result{BuildID: "b1", Repo: "r", Packages: 1, Published: 1}); err != nil {
		t.Fatalf("WriteResult: %v", err)
	}
	if err := Remove(root, "b1"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	res := ReadResult(root, "b1")
	if res == nil {
		t.Fatal("result lost when the accumulator was removed")
	}
	if res.Published != 1 || res.Repo != "r" {
		t.Errorf("unexpected result: %+v", res)
	}
	if ReadResult(root, "never-finalized") != nil {
		t.Error("want nil result for an unknown build")
	}

	st := GetStatus(root, "b1")
	if st.Accumulated != 0 || st.Result == nil || st.Result.Published != 1 {
		t.Errorf("unexpected status: %+v", st)
	}
}
