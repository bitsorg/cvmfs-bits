// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// nopLogger returns a discard logger for use in tests.
func nopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(nopWriter{}, nil))
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

// TestAcquire_FastPath verifies that Acquire succeeds immediately when slots
// are available and no one is queued.
func TestAcquire_FastPath(t *testing.T) {
	ds := NewDynamicSemaphore(2, 2, nopLogger())
	defer ds.Stop()

	ctx := context.Background()
	if _, err := ds.Acquire(ctx, 0); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	if _, err := ds.Acquire(ctx, 0); err != nil {
		t.Fatalf("second Acquire: %v", err)
	}
}

// TestAcquire_BlocksWhenFull verifies that Acquire blocks when all slots are
// taken and is unblocked by Release().
func TestAcquire_BlocksWhenFull(t *testing.T) {
	ds := NewDynamicSemaphore(1, 1, nopLogger())
	defer ds.Stop()

	ctx := context.Background()
	if _, err := ds.Acquire(ctx, 0); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	acquired := make(chan struct{})
	go func() {
		if _, err := ds.Acquire(ctx, 0); err == nil {
			close(acquired)
		}
	}()

	// Verify it is blocked.
	select {
	case <-acquired:
		t.Fatal("second Acquire returned before Release")
	case <-time.After(50 * time.Millisecond):
		// Good — still blocked.
	}

	ds.Release(1)

	select {
	case <-acquired:
		// Good — unblocked after Release.
	case <-time.After(time.Second):
		t.Fatal("second Acquire did not unblock within 1s after Release")
	}
}

// TestAcquire_CancellationUnblocks verifies that a cancelled context causes a
// blocked Acquire to return ctx.Err() without hanging.
func TestAcquire_CancellationUnblocks(t *testing.T) {
	ds := NewDynamicSemaphore(1, 1, nopLogger())
	defer ds.Stop()

	ctx := context.Background()
	// Fill the one slot.
	if _, err := ds.Acquire(ctx, 0); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		errCh <- func() error { _, e := ds.Acquire(cancelCtx, 0); return e }()
	}()

	// Give the goroutine time to park on the channel.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("cancelled Acquire returned nil error")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled Acquire did not return within 1s")
	}
}

// TestAcquire_LargestJobGetsSlotFirst is the core regression test for the
// priority scheduling fix.
//
// Setup: 1 slot.  The slot is held.  Three jobs with sizes 100, 500, 200 queue
// simultaneously.  When the slot is released, the job with size 500 must be
// dispatched next, then 200, then 100.
func TestAcquire_LargestJobGetsSlotFirst(t *testing.T) {
	ds := NewDynamicSemaphore(1, 1, nopLogger())
	defer ds.Stop()

	ctx := context.Background()
	// Hold the only slot.
	if _, err := ds.Acquire(ctx, 0); err != nil {
		t.Fatalf("holder Acquire: %v", err)
	}

	// Wait until all three goroutines are parked in Acquire before releasing.
	var parked atomic.Int32
	type result struct {
		priority int64
		order    int
	}
	orderCh := make(chan result, 3)

	sizes := []int64{100, 500, 200}
	var started sync.WaitGroup
	started.Add(len(sizes))

	var orderCounter atomic.Int32

	for _, sz := range sizes {
		sz := sz
		go func() {
			started.Done()                    // signal that goroutine is running
			time.Sleep(10 * time.Millisecond) // give all goroutines time to start
			parked.Add(1)
			if _, err := ds.Acquire(ctx, sz); err != nil {
				t.Errorf("Acquire(priority=%d): %v", sz, err)
				return
			}
			// Record the order in which slots were granted.
			ord := int(orderCounter.Add(1))
			orderCh <- result{priority: sz, order: ord}
			ds.Release(1)
		}()
	}

	// Wait for all goroutines to launch, then give them time to call Acquire.
	started.Wait()
	time.Sleep(80 * time.Millisecond) // let all three park in Acquire

	// Release the held slot — this should wake the highest-priority waiter.
	ds.Release(1)

	// Collect all three results with a generous timeout.
	results := make([]result, 0, 3)
	timeout := time.After(5 * time.Second)
	for len(results) < 3 {
		select {
		case r := <-orderCh:
			results = append(results, r)
		case <-timeout:
			t.Fatalf("timed out waiting for all Acquire calls; got %d/3", len(results))
		}
	}

	// Find the order of the priority-500 job.
	var order500 int
	for _, r := range results {
		if r.priority == 500 {
			order500 = r.order
		}
	}
	if order500 != 1 {
		t.Errorf("job with priority 500 was dispatched #%d; want #1 (first)", order500)
	}

	// Also verify priority-200 before priority-100.
	var order200, order100 int
	for _, r := range results {
		switch r.priority {
		case 200:
			order200 = r.order
		case 100:
			order100 = r.order
		}
	}
	if order200 >= order100 {
		t.Errorf("priority 200 dispatched #%d but priority 100 dispatched #%d; want 200 before 100",
			order200, order100)
	}
}

// TestRelease_SkipsCancelledWaiters verifies that Release() skips over
// cancelled waiters and grants the slot to the first live one.
func TestRelease_SkipsCancelledWaiters(t *testing.T) {
	ds := NewDynamicSemaphore(1, 1, nopLogger())
	defer ds.Stop()

	ctx := context.Background()
	// Hold the slot.
	if _, err := ds.Acquire(ctx, 0); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	// Queue a high-priority waiter that will be cancelled.
	cancelCtx, cancel := context.WithCancel(ctx)
	cancelledCh := make(chan error, 1)
	go func() {
		cancelledCh <- func() error { _, e := ds.Acquire(cancelCtx, 1000); return e }() // highest priority — will be cancelled
	}()
	time.Sleep(20 * time.Millisecond)

	// Queue a lower-priority live waiter.
	liveCh := make(chan error, 1)
	go func() {
		liveCh <- func() error { _, e := ds.Acquire(ctx, 50); return e }() // lower priority but not cancelled
	}()
	time.Sleep(20 * time.Millisecond)

	// Cancel the high-priority waiter.
	cancel()
	select {
	case err := <-cancelledCh:
		if err == nil {
			t.Fatal("cancelled waiter returned nil error")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled waiter did not return")
	}

	// Release — should skip the (now-cancelled) high-priority entry and grant to live waiter.
	ds.Release(1)
	select {
	case err := <-liveCh:
		if err != nil {
			t.Fatalf("live waiter returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("live waiter was not granted slot after Release")
	}
}

// TestAcquire_NoSlotsLeakedOnCancellation verifies that cancelling a queued
// Acquire does not leak slot counts — after the cancelled goroutine exits, the
// semaphore remains consistent and a subsequent Acquire succeeds normally.
func TestAcquire_NoSlotsLeakedOnCancellation(t *testing.T) {
	ds := NewDynamicSemaphore(1, 1, nopLogger())
	defer ds.Stop()

	ctx := context.Background()
	// Hold the slot.
	if _, err := ds.Acquire(ctx, 0); err != nil {
		t.Fatalf("holder Acquire: %v", err)
	}

	// Cancel a waiter.
	cancelCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		errCh <- func() error { _, e := ds.Acquire(cancelCtx, 100); return e }()
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-errCh

	// Release the held slot — should be available for a new Acquire.
	ds.Release(1)

	done := make(chan error, 1)
	go func() {
		done <- func() error { _, e := ds.Acquire(ctx, 0); return e }()
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("post-cancel Acquire: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("post-cancel Acquire timed out — slot was leaked")
	}
}

// TestAcquire_ZeroPriorityInteroperates verifies that calls with zero priority
// (e.g. recovery paths that do not have a TarSize) still work correctly and
// are dispatched last relative to jobs with positive priority.
func TestAcquire_ZeroPriorityInteroperates(t *testing.T) {
	ds := NewDynamicSemaphore(1, 1, nopLogger())
	defer ds.Stop()

	ctx := context.Background()
	// Hold the slot.
	if _, err := ds.Acquire(ctx, 0); err != nil {
		t.Fatalf("holder Acquire: %v", err)
	}

	type result struct {
		priority int64
		order    int
	}
	orderCh := make(chan result, 2)
	var orderCounter atomic.Int32

	for _, sz := range []int64{0, 999} {
		sz := sz
		go func() {
			time.Sleep(10 * time.Millisecond)
			if _, err := ds.Acquire(ctx, sz); err != nil {
				t.Errorf("Acquire(%d): %v", sz, err)
				return
			}
			ord := int(orderCounter.Add(1))
			orderCh <- result{priority: sz, order: ord}
			ds.Release(1)
		}()
	}

	time.Sleep(80 * time.Millisecond)
	ds.Release(1)

	results := make([]result, 0, 2)
	timeout := time.After(3 * time.Second)
	for len(results) < 2 {
		select {
		case r := <-orderCh:
			results = append(results, r)
		case <-timeout:
			t.Fatalf("timed out; got %d/2", len(results))
		}
	}

	var ord999 int
	for _, r := range results {
		if r.priority == 999 {
			ord999 = r.order
		}
	}
	if ord999 != 1 {
		t.Errorf("priority-999 job was dispatched #%d; want #1", ord999)
	}
}

// ── Size-weighted admission ──────────────────────────────────────────────────
//
// A slot used to mean "one job", pricing a 4 KiB modulefile and a 5.2 GB tar
// identically. On a spool volume delivering single-digit MB/s that admitted six
// multi-gigabyte packages together; each got a sixth of the device and took six
// times longer than it would have alone. A seek-limited disk does not go faster
// when more readers ask, so the concurrency bought nothing and multiplied every
// job's latency.

func TestJobWeight(t *testing.T) {
	const budget = 16
	for _, tc := range []struct {
		name  string
		bytes int64
		want  int
	}{
		{"empty", 0, 1},
		{"modulefile tar", 4 << 10, 1},
		{"just under a unit", jobWeightUnitBytes - 1, 1},
		{"exactly one unit", jobWeightUnitBytes, 1},
		{"just over a unit", jobWeightUnitBytes + 1, 2},
		{"944 MiB", 944 << 20, 8},
		// 20 units uncapped, but the budget is 16 — so at this slot count
		// anything from ~2 GiB up already runs alone, which is the intent.
		{"2.5 GiB, clamped by a 16 budget", 2560 << 20, budget},
		// Clamped: a tar bigger than the whole budget must still be admissible,
		// or the largest package in a build could never run at all.
		{"5.2 GiB against a 16 budget", 5325 << 20, budget},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := jobWeight(tc.bytes, budget); got != tc.want {
				t.Errorf("jobWeight(%d, %d) = %d, want %d", tc.bytes, budget, got, tc.want)
			}
		})
	}
}

// TestDynaSem_LargeJobsSerialise is the property asked for: big packages run one
// at a time even though the slot count is high.
func TestDynaSem_LargeJobsSerialise(t *testing.T) {
	ds := NewDynamicSemaphore(16, 16, nopLogger())
	defer ds.Stop()
	ctx := context.Background()

	const huge = int64(6) << 30 // weighs the whole budget

	w1, err := ds.Acquire(ctx, huge)
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	second := make(chan int, 1)
	go func() {
		w, aerr := ds.Acquire(ctx, huge)
		if aerr != nil {
			t.Errorf("second Acquire: %v", aerr)
		}
		second <- w
	}()

	select {
	case <-second:
		t.Fatal("two whole-budget jobs ran at once; large jobs must serialise")
	case <-time.After(150 * time.Millisecond):
	}

	ds.Release(w1)
	select {
	case <-second:
	case <-time.After(2 * time.Second):
		t.Error("the second large job never started after the first released")
	}
}

// TestDynaSem_SmallJobsStillRunConcurrently is the other half: weighting must
// not throttle ordinary packages, which are the overwhelming majority.
func TestDynaSem_SmallJobsStillRunConcurrently(t *testing.T) {
	const slots = 16
	ds := NewDynamicSemaphore(slots, slots, nopLogger())
	defer ds.Stop()
	ctx := context.Background()

	for i := 0; i < slots; i++ {
		if _, err := ds.Acquire(ctx, 4<<10); err != nil {
			t.Fatalf("small Acquire %d: %v", i, err)
		}
	}
	// The budget is now full; the next one must wait.
	tight, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := ds.Acquire(tight, 4<<10); err == nil {
		t.Error("admitted more small jobs than the budget allows")
	}
}

// TestDynaSem_OversizedJobRunsAloneRatherThanNever covers the escape hatch: the
// effective limit can shrink under load after a weight was computed, and a
// waiter whose weight then exceeds the limit must not wait forever.
func TestDynaSem_OversizedJobRunsAloneRatherThanNever(t *testing.T) {
	ds := NewDynamicSemaphore(1, 1, nopLogger())
	defer ds.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w, err := ds.Acquire(ctx, 100<<30) // vastly over any budget
	if err != nil {
		t.Fatalf("an oversized job must still be admitted when idle: %v", err)
	}
	ds.Release(w)
}
