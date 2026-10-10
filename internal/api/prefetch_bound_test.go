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
	"strconv"
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
func writeTar(t *testing.T, dir string) string { return writeTarOfSize(t, dir, 1024) }

// writeTarOfSize writes a sparse file of the requested size, so a "large
// package" costs the test a few microseconds rather than hundreds of megabytes.
func writeTarOfSize(t *testing.T, dir string, size int64) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	p := filepath.Join(dir, "payload.tar")
	f, err := os.Create(p)
	if err != nil {
		t.Fatalf("create tar: %v", err)
	}
	if err := f.Truncate(size); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
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

// TestSetPrefetchLimit_Defaults guards against an unset limit silently
// removing the bound.
func TestSetPrefetchLimit_Defaults(t *testing.T) {
	_, _, orch := newTestServer(t)
	orch.SetPrefetchLimit(0)
	if orch.prefetchLimit != defaultPrefetchLimit {
		t.Errorf("SetPrefetchLimit(0) = %d, want the default %d",
			orch.prefetchLimit, defaultPrefetchLimit)
	}
	if !orch.PrefetchEnabled() {
		t.Error("an unset limit must not disable the look-ahead")
	}
}

// ── Size-weighted budget ─────────────────────────────────────────────────────
//
// A flat per-scan count is the wrong meter: what a scan consumes is disk
// bandwidth and memory, and both scale with the archive. A limit tuned so that
// ordinary packages flow freely admits far too much work the moment several
// large ones coincide — observed directly with a limit of 4, which ran well
// until the big tars arrived together.

func TestPrefetchWeight(t *testing.T) {
	const limit = 4
	for _, tc := range []struct {
		name string
		size int64
		want int64
	}{
		{"empty", 0, 1},
		{"tiny modulefile tar", 4 << 10, 1},
		{"just under one unit", prefetchUnitBytes - 1, 1},
		{"exactly one unit", prefetchUnitBytes, 1},
		{"just over one unit", prefetchUnitBytes + 1, 2},
		{"two units", 2 * prefetchUnitBytes, 2},
		{"the whole budget", limit * prefetchUnitBytes, limit},
		// Clamped: an archive bigger than the entire budget must still be
		// admissible when idle, or the largest packages could NEVER prefetch.
		{"larger than the budget", 100 * prefetchUnitBytes, limit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := prefetchWeight(tc.size, limit); got != tc.want {
				t.Errorf("prefetchWeight(%d, %d) = %d, want %d", tc.size, limit, got, tc.want)
			}
		})
	}
}

// TestStartPrefetch_LargeTarsDoNotAllRunAtOnce is the behaviour asked for: with
// a budget of 4, four large archives must NOT scan concurrently the way four
// small ones may.
func TestStartPrefetch_LargeTarsDoNotAllRunAtOnce(t *testing.T) {
	_, _, orch := newTestServer(t)
	orch.Lease = &pipelineBackend{}

	const limit = 4
	orch.SetPrefetchLimit(limit)

	var live, peak int64
	var mu sync.Mutex
	release := make(chan struct{})
	defer close(release)
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

	// Each of these weighs the entire budget, so they must serialise.
	for i := 0; i < 6; i++ {
		j := &job.Job{ID: "big-" + strconv.Itoa(i), Repo: "r"}
		j.TarPath = writeTarOfSize(t, filepath.Join(t.TempDir(), j.ID), limit*prefetchUnitBytes)
		orch.StartPrefetch(context.Background(), j)
	}
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	got := peak
	mu.Unlock()
	if got > 1 {
		t.Errorf("%d large tars scanned at once; each weighs the whole budget, so only "+
			"one may run — this is the case a flat per-scan count got wrong", got)
	}
}

// TestStartPrefetch_SmallTarsStillRunConcurrently is the other half: weighting
// must not throttle ordinary packages, which is the common case by far.
func TestStartPrefetch_SmallTarsStillRunConcurrently(t *testing.T) {
	_, _, orch := newTestServer(t)
	orch.Lease = &pipelineBackend{}

	const limit = 4
	orch.SetPrefetchLimit(limit)

	var live, peak int64
	var mu sync.Mutex
	release := make(chan struct{})
	defer close(release)
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

	for i := 0; i < 10; i++ {
		j := &job.Job{ID: "small-" + strconv.Itoa(i), Repo: "r"}
		j.TarPath = writeTar(t, filepath.Join(t.TempDir(), j.ID))
		orch.StartPrefetch(context.Background(), j)
	}
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	got := peak
	mu.Unlock()
	if got != limit {
		t.Errorf("peak concurrency %d with a budget of %d — small packages must still "+
			"fill the budget, or weighting has made the common case slower", got, limit)
	}
}

// ── Disabling the look-ahead entirely ────────────────────────────────────────
//
// The prefetch reads a whole tar and spills the unpacked entries to disk; the
// pipeline then reads the spill. On fast storage that is a good trade, because
// phase 0 overlaps the wait for a concurrency slot. On a volume measured at
// 4.4 MB/s it roughly doubles the I/O on the one saturated resource to buy
// overlap nothing is waiting for. Off, each archive is read exactly once.

func TestSetPrefetchEnabled_FalseSkipsTheScan(t *testing.T) {
	_, _, orch := newTestServer(t)
	orch.Lease = &pipelineBackend{}
	orch.SetPrefetchEnabled(false)

	if orch.PrefetchEnabled() {
		t.Fatal("SetPrefetchEnabled(false) must disable the look-ahead")
	}

	ran := make(chan struct{}, 1)
	orch.prefetchHook = func() { ran <- struct{}{} }

	j := &job.Job{ID: "disabled", Repo: "r"}
	j.TarPath = writeTar(t, filepath.Join(t.TempDir(), "disabled"))
	orch.StartPrefetch(context.Background(), j)

	select {
	case <-ran:
		t.Error("a scan started even though the prefetch is disabled")
	case <-time.After(150 * time.Millisecond):
	}

	// No stored result is what makes the pipeline do phase 0 inline.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if got := orch.takePrefetch(ctx, j.ID); got != nil {
		t.Error("disabled prefetch must leave no result, so Run falls back to the inline scan")
	}
}

// TestSetPrefetchLimit_IsIndependentOfEnabled: "how much" and "whether" are
// separate questions, so no value of the budget may turn the look-ahead off.
func TestSetPrefetchLimit_IsIndependentOfEnabled(t *testing.T) {
	_, _, orch := newTestServer(t)
	for _, n := range []int{-1, 0, 1, 64} {
		orch.SetPrefetchLimit(n)
		if !orch.PrefetchEnabled() {
			t.Errorf("SetPrefetchLimit(%d) disabled the look-ahead; only "+
				"SetPrefetchEnabled(false) may do that", n)
		}
	}
}

// TestSetPrefetchLimit_ReEnables covers going back the other way, so the
// disabled flag cannot latch.
func TestSetPrefetchEnabled_ReEnables(t *testing.T) {
	_, _, orch := newTestServer(t)
	orch.SetPrefetchEnabled(false)
	orch.SetPrefetchEnabled(true)
	if !orch.PrefetchEnabled() {
		t.Error("the disabled flag latched; it must be settable both ways")
	}
}
