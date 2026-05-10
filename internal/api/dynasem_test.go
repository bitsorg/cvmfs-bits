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
	if err := ds.Acquire(ctx, 0); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	if err := ds.Acquire(ctx, 0); err != nil {
		t.Fatalf("second Acquire: %v", err)
	}
}

// TestAcquire_BlocksWhenFull verifies that Acquire blocks when all slots are
// taken and is unblocked by Release().
func TestAcquire_BlocksWhenFull(t *testing.T) {
	ds := NewDynamicSemaphore(1, 1, nopLogger())
	defer ds.Stop()

	ctx := context.Background()
	if err := ds.Acquire(ctx, 0); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	acquired := make(chan struct{})
	go func() {
		if err := ds.Acquire(ctx, 0); err == nil {
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

	ds.Release()

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
	if err := ds.Acquire(ctx, 0); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		errCh <- ds.Acquire(cancelCtx, 0)
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
	if err := ds.Acquire(ctx, 0); err != nil {
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
			started.Done()                         // signal that goroutine is running
			time.Sleep(10 * time.Millisecond)      // give all goroutines time to start
			parked.Add(1)
			if err := ds.Acquire(ctx, sz); err != nil {
				t.Errorf("Acquire(priority=%d): %v", sz, err)
				return
			}
			// Record the order in which slots were granted.
			ord := int(orderCounter.Add(1))
			orderCh <- result{priority: sz, order: ord}
			ds.Release()
		}()
	}

	// Wait for all goroutines to launch, then give them time to call Acquire.
	started.Wait()
	time.Sleep(80 * time.Millisecond) // let all three park in Acquire

	// Release the held slot — this should wake the highest-priority waiter.
	ds.Release()

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
	if err := ds.Acquire(ctx, 0); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	// Queue a high-priority waiter that will be cancelled.
	cancelCtx, cancel := context.WithCancel(ctx)
	cancelledCh := make(chan error, 1)
	go func() {
		cancelledCh <- ds.Acquire(cancelCtx, 1000) // highest priority — will be cancelled
	}()
	time.Sleep(20 * time.Millisecond)

	// Queue a lower-priority live waiter.
	liveCh := make(chan error, 1)
	go func() {
		liveCh <- ds.Acquire(ctx, 50) // lower priority but not cancelled
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
	ds.Release()
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
	if err := ds.Acquire(ctx, 0); err != nil {
		t.Fatalf("holder Acquire: %v", err)
	}

	// Cancel a waiter.
	cancelCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		errCh <- ds.Acquire(cancelCtx, 100)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-errCh

	// Release the held slot — should be available for a new Acquire.
	ds.Release()

	done := make(chan error, 1)
	go func() {
		done <- ds.Acquire(ctx, 0)
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
	if err := ds.Acquire(ctx, 0); err != nil {
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
			if err := ds.Acquire(ctx, sz); err != nil {
				t.Errorf("Acquire(%d): %v", sz, err)
				return
			}
			ord := int(orderCounter.Add(1))
			orderCh <- result{priority: sz, order: ord}
			ds.Release()
		}()
	}

	time.Sleep(80 * time.Millisecond)
	ds.Release()

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
