// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

// The tar prefetch (pipeline phase 0) deliberately runs BEFORE a job competes
// for a concurrency slot, so that its compress workers can start the moment it
// gets one. That makes it the one expensive thing the job semaphore does not
// bound — and StartPrefetch is called once per job from submitJob, at
// submission time.
//
// A producer that uploads a whole build in one burst therefore started one tar
// scan per package simultaneously. On a 174-package build that meant 174
// concurrent scans, each reading an archive and spilling its large entries to
// the spool. Observed effect: a publisher at 0% CPU with every job in I/O wait,
// spending four minutes on sixteen seconds of pipeline work, and degrading
// further the more work it was given.

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/lease"
)

// pipelineBackend is a noopBackend that DOES want the pipeline — StartPrefetch
// returns immediately otherwise, and the test would silently exercise nothing.
type pipelineBackend struct{ noopBackend }

func (p *pipelineBackend) NeedsPipeline() bool { return true }

var _ lease.Backend = (*pipelineBackend)(nil)

// writeTar puts a minimal, valid tar where the job expects its payload.
func writeTar(t *testing.T, dir string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	p := filepath.Join(dir, "payload.tar")
	// two zero blocks = a valid empty archive
	if err := os.WriteFile(p, make([]byte, 1024), 0o644); err != nil {
		t.Fatalf("write tar: %v", err)
	}
	return p
}

// TestStartPrefetch_IsBounded is the regression. Submitting far more jobs than
// the limit must not start more than `limit` scans at once.
func TestStartPrefetch_IsBounded(t *testing.T) {
	_, _, orch := newTestServer(t)
	orch.Lease = &pipelineBackend{}

	const limit = 4
	orch.SetPrefetchLimit(limit)

	// Hold every acquired slot open so concurrency is observable: the scans
	// block on a tar we only finish writing when the test says so.
	var live, peak int64
	var mu sync.Mutex
	release := make(chan struct{})
	orch.prefetchHook = func() {
		n := atomic.AddInt64(&live, 1)
		mu.Lock()
		if n > peak {
			peak = n
		}
		mu.Unlock()
		<-release
		atomic.AddInt64(&live, -1)
	}

	const submitted = 40
	for i := 0; i < submitted; i++ {
		j := &job.Job{ID: "job-" + string(rune('a'+i%26)) + string(rune('0'+i/26)), Repo: "r"}
		dir := filepath.Join(t.TempDir(), j.ID)
		j.TarPath = writeTar(t, dir)
		orch.StartPrefetch(context.Background(), j)
	}

	// Give the goroutines a moment to pile up if they are going to.
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	got := peak
	mu.Unlock()
	close(release)

	if got > limit {
		t.Errorf("%d concurrent tar scans with a limit of %d — the prefetch is unbounded, "+
			"so a burst of submissions puts every job into I/O wait", got, limit)
	}
	if got == 0 {
		t.Fatal("no scans started at all; the test is not exercising the path")
	}
}

// TestStartPrefetch_SkipsRatherThanQueues: over the limit, a job must fall
// through to the inline phase-0 path instead of waiting behind other scans.
// Queueing would just relocate the contention and delay the job that is
// actually holding a concurrency slot.
func TestStartPrefetch_SkipsRatherThanQueues(t *testing.T) {
	_, _, orch := newTestServer(t)
	orch.Lease = &pipelineBackend{}
	orch.SetPrefetchLimit(1)

	release := make(chan struct{})
	defer close(release)
	started := make(chan struct{}, 1)
	orch.prefetchHook = func() {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
	}

	first := &job.Job{ID: "first", Repo: "r"}
	first.TarPath = writeTar(t, filepath.Join(t.TempDir(), "first"))
	orch.StartPrefetch(context.Background(), first)
	<-started // the single slot is now held

	second := &job.Job{ID: "second", Repo: "r"}
	second.TarPath = writeTar(t, filepath.Join(t.TempDir(), "second"))

	done := make(chan struct{})
	go func() { orch.StartPrefetch(context.Background(), second); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("StartPrefetch blocked waiting for a slot; it must skip instead")
	}

	// Skipped means no result was stored, which is what makes takePrefetch
	// return nil and the pipeline do phase 0 inline.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if got := orch.takePrefetch(ctx, second.ID); got != nil {
		t.Error("a skipped prefetch must leave no result, so Run falls back to the inline scan")
	}
}

// TestSetPrefetchLimit_Defaults guards against a zero limit disabling the bound.
func TestSetPrefetchLimit_Defaults(t *testing.T) {
	_, _, orch := newTestServer(t)
	for _, n := range []int{0, -1} {
		orch.SetPrefetchLimit(n)
		if orch.prefetchLimit != defaultPrefetchLimit {
			t.Errorf("SetPrefetchLimit(%d) = %d, want the default %d",
				n, orch.prefetchLimit, defaultPrefetchLimit)
		}
	}
}
