// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestIngestBackend_AcquireQueues verifies the property that distinguishes this
// backend from LocalBackend: a second publisher for the same repository WAITS
// rather than being refused. A relay exists to absorb a burst from the producer
// and feed the gateway steadily, so queuing is the point.
func TestIngestBackend_AcquireQueues(t *testing.T) {
	b := NewIngestBackend(IngestOptions{}, newTestObs(t))

	first, err := b.Acquire(context.Background(), "repo.example.org", "")
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	// Second acquire must block while the first holds the slot.
	got := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := b.Acquire(ctx, "repo.example.org", "")
		got <- err
	}()

	select {
	case err := <-got:
		t.Fatalf("second Acquire returned early (err=%v); it must wait for the slot", err)
	case <-time.After(100 * time.Millisecond):
	}

	// Releasing lets it through — with the HOLDER's token, which is what the
	// orchestrator passes to Abort.
	_ = b.Abort(context.Background(), first)
	select {
	case err := <-got:
		if err != nil {
			t.Errorf("second Acquire after release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("second Acquire did not proceed after the slot was released")
	}
}

// TestIngestBackend_AcquireHonoursContext verifies the wait is bounded by the
// caller's context rather than being unbounded.
func TestIngestBackend_AcquireHonoursContext(t *testing.T) {
	b := NewIngestBackend(IngestOptions{}, newTestObs(t))
	if _, err := b.Acquire(context.Background(), "repo.example.org", ""); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := b.Acquire(ctx, "repo.example.org", ""); err == nil {
		t.Error("want an error when the context expires while queued")
	}
}

// TestIngestBackend_DifferentReposAreParallel verifies the slot is per
// repository, not global — one busy repository must not stall the others.
func TestIngestBackend_DifferentReposAreParallel(t *testing.T) {
	b := NewIngestBackend(IngestOptions{}, newTestObs(t))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if _, err := b.Acquire(ctx, "a.example.org", ""); err != nil {
		t.Fatalf("Acquire a: %v", err)
	}
	if _, err := b.Acquire(ctx, "b.example.org", ""); err != nil {
		t.Fatalf("Acquire b (different repo must not queue behind a): %v", err)
	}
}

// TestIngestBackend_CommitRejectsForeignTarget pins the guard that stops a
// mis-derived CVMFSDir from publishing into the wrong repository. `ingest -b`
// takes an absolute /cvmfs/<repo>/<path> and derives the repository from it, so
// a path that does not match the lease token would silently publish elsewhere.
func TestIngestBackend_CommitRejectsForeignTarget(t *testing.T) {
	b := NewIngestBackend(IngestOptions{CVMFSMount: "/cvmfs"}, newTestObs(t))

	err := b.Commit(context.Background(), CommitRequest{
		Token:    "mine.example.org",
		TarPath:  "/spool/payload.tar",
		CVMFSDir: "/cvmfs/other.example.org/pkg/1.0",
	})
	if err == nil {
		t.Fatal("want an error when the target is under a different repository")
	}
	if !strings.Contains(err.Error(), "not under") {
		t.Errorf("unexpected error: %v", err)
	}
	// The slot must still be released, or the repository would be wedged.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, aerr := b.Acquire(ctx, "mine.example.org", ""); aerr != nil {
		t.Errorf("slot not released after a rejected commit: %v", aerr)
	}
}

// TestIngestBackend_CommitRequiresPayload covers the finalize-shaped request
// (no tar), which this backend cannot serve.
func TestIngestBackend_CommitRequiresPayload(t *testing.T) {
	b := NewIngestBackend(IngestOptions{}, newTestObs(t))
	err := b.Commit(context.Background(), CommitRequest{
		Token:    "repo.example.org",
		CVMFSDir: "/cvmfs/repo.example.org/pkg",
	})
	if err == nil || !strings.Contains(err.Error(), "no tar payload") {
		t.Errorf("want a missing-payload error, got %v", err)
	}
}

func TestIngestBackend_NeedsPipeline(t *testing.T) {
	b := NewIngestBackend(IngestOptions{}, newTestObs(t))
	if b.NeedsPipeline() {
		t.Error("the ingest path hands over the raw tar; it must not require the pipeline")
	}
}

// TestIngestBackend_ReleaseIsHolderScoped is the important concurrency test.
// One job can call BOTH Commit and Abort (Commit fails, the orchestrator then
// aborts), so a release keyed only on the repository would free whichever job
// had taken the slot in between — putting two `cvmfs_server ingest` runs on one
// repository, which is exactly what the slot exists to prevent.
func TestIngestBackend_ReleaseIsHolderScoped(t *testing.T) {
	b := NewIngestBackend(IngestOptions{}, newTestObs(t))
	b.release("never-acquired.example.org") // must not block or panic

	// Job A takes the slot, then lets it go (as a failed Commit would).
	tokenA, err := b.Acquire(context.Background(), "repo.example.org", "")
	if err != nil {
		t.Fatalf("Acquire A: %v", err)
	}
	b.release(tokenA)

	// Job B now holds it.
	tokenB, err := b.Acquire(context.Background(), "repo.example.org", "")
	if err != nil {
		t.Fatalf("Acquire B: %v", err)
	}
	if tokenA == tokenB {
		t.Fatal("tokens must be unique per acquisition")
	}

	// Job A's late Abort must NOT free job B's slot.
	if aerr := b.Abort(context.Background(), tokenA); aerr != nil {
		t.Fatalf("Abort A: %v", aerr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, cerr := b.Acquire(ctx, "repo.example.org", ""); cerr == nil {
		t.Error("a stale release freed a slot that another job legitimately held")
	}

	// B's own release does work.
	b.release(tokenB)
	if _, aerr := b.Acquire(context.Background(), "repo.example.org", ""); aerr != nil {
		t.Errorf("Acquire after the holder released: %v", aerr)
	}
}

// TestIngestBackend_CommitArgsPutRepoLast pins an ordering constraint enforced
// by a shell script in another project: cvmfs_server's option loop is
// `while [ "$2" != "" ]` and then takes `$1` as the repository, so the
// repository must be the FINAL argument. Put it anywhere else and the last flag
// is swallowed as the repository name — with no owner configured, "-c" would
// become the repo and every publish would die in load_repo_config.
func TestIngestBackend_CommitArgsPutRepoLast(t *testing.T) {
	for _, tc := range []struct {
		name string
		opt  IngestOptions
		want []string
	}{
		{
			name: "nested catalog, no owner",
			opt:  IngestOptions{NestedCatalog: true},
			want: []string{"ingest", "-t", "/spool/p.tar", "-b", "pkg/1.0", "-c", "repo.example.org"},
		},
		{
			name: "nested catalog and owner",
			opt:  IngestOptions{NestedCatalog: true, Owner: "cvmfs"},
			want: []string{"ingest", "-t", "/spool/p.tar", "-b", "pkg/1.0", "-c", "-u", "cvmfs", "repo.example.org"},
		},
		{
			name: "bare",
			opt:  IngestOptions{},
			want: []string{"ingest", "-t", "/spool/p.tar", "-b", "pkg/1.0", "repo.example.org"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := NewIngestBackend(tc.opt, newTestObs(t))
			got := b.commitArgs("repo.example.org", "pkg/1.0", "/spool/p.tar", false, false)
			if strings.Join(got, " ") != strings.Join(tc.want, " ") {
				t.Errorf("commitArgs =\n  %v\nwant\n  %v", got, tc.want)
			}
			if got[len(got)-1] != "repo.example.org" {
				t.Error("the repository must be the final argument")
			}
		})
	}
}

// TestIngestBackend_AbortLeavesTransactionsAlone documents why Abort does not
// run `cvmfs_server abort`: this backend holds no transaction, and the
// repository may have one open belonging to another job — or to the prepub path
// on a node serving both. The test asserts Abort is a pure slot release.
func TestIngestBackend_AbortLeavesTransactionsAlone(t *testing.T) {
	b := NewIngestBackend(IngestOptions{}, newTestObs(t))
	token, err := b.Acquire(context.Background(), "repo.example.org", "")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	// cvmfs_server is not installed in the test environment; an Abort that
	// shelled out would surface that as an error or a long timeout.
	done := make(chan error, 1)
	go func() { done <- b.Abort(context.Background(), token) }()
	select {
	case aerr := <-done:
		if aerr != nil {
			t.Errorf("Abort must not fail: %v", aerr)
		}
	case <-time.After(time.Second):
		t.Fatal("Abort blocked — it should not be running a subprocess")
	}
}

// TestIngestBackend_ConcurrentQueueForIsSafe exercises the lazily created
// per-repo queues from several goroutines at once.
func TestIngestBackend_ConcurrentQueueForIsSafe(t *testing.T) {
	b := NewIngestBackend(IngestOptions{}, newTestObs(t))
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.queueFor("same.example.org")
		}()
	}
	wg.Wait()
}
