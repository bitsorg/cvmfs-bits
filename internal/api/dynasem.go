// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Package api — dynasem.go: load-aware dynamic concurrency semaphore with
// priority scheduling.
//
// DynamicSemaphore limits the number of concurrently active jobs based on the
// current machine load.  The effective slot count is recomputed every ~5 s:
//
//	effectiveSlots = max(MinSlots, MaxSlots - int(load1min))
//
// where load1min is the 1-minute load average obtained via readLoadAvg()
// (implemented in dynasem_linux.go on Linux; returns 0 on other platforms) and
// MaxSlots defaults to runtime.NumCPU().  As load drops, the highest-priority
// waiters are woken first.
//
// Priority scheduling (Fix #priority): when multiple jobs are queued waiting
// for a slot, the job with the largest TarSize is dispatched first.  This
// ensures long-running jobs overlap with shorter ones rather than being pushed
// to the tail, reducing overall makespan.
//
// Implementation: instead of sync.Cond (which broadcasts to all waiters and
// causes a thundering-herd race), each Acquire call parks on a private
// chan struct{} stored in a max-heap keyed by priority.  Release() pops the
// top waiter and closes its channel, granting exactly one slot to the
// highest-priority caller.
//
// Context cancellation (abort, shutdown) is respected: Acquire returns
// ctx.Err() as soon as the context is done.  Cancelled waiters are lazily
// removed from the heap on the next Release() call.
//
// Platform note: load-aware throttling requires /proc/loadavg and therefore
// only works on Linux.  On other platforms readLoadAvg always returns 0 so
// the semaphore acts as a static pool capped at MaxSlots.  See
// dynasem_linux.go / dynasem_other.go for the platform split.
package api

import (
	"container/heap"
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"
)

const (
	// loadPollInterval is how often the background goroutine re-reads /proc/loadavg.
	loadPollInterval = 5 * time.Second
)

// jobWeightUnitBytes is one unit of admission budget.
//
// A slot used to mean "one job", which prices a 4 KiB modulefile and a 5.2 GB
// tar identically. On a spool volume delivering single-digit MB/s that is not a
// rounding error: six multi-gigabyte packages admitted together each got a sixth
// of the device, so all six took six times longer than any one of them would
// have alone. Aggregate throughput was unchanged — a seek-limited disk does not
// go faster when more readers ask — while per-job latency multiplied, which is
// what actually breaks things downstream.
//
// Charging by size makes the common case unchanged (small packages weigh 1 and
// still run tens at a time) and serialises the heavy ones, which is the whole
// point: one big job at a time, finishing quickly, rather than six crawling.
const jobWeightUnitBytes = 128 << 20

// jobWeight converts a job's tar size into admission units, clamped to [1, budget].
//
// The upper clamp matters: a tar larger than the entire budget must still be
// admissible, or it could never run at all. It simply runs alone.
func jobWeight(tarBytes int64, budget int) int {
	if budget < 1 {
		budget = 1
	}
	units := int((tarBytes + jobWeightUnitBytes - 1) / jobWeightUnitBytes) // ceil
	if units < 1 {
		units = 1
	}
	if units > budget {
		units = budget
	}
	return units
}

// waiter represents a single Acquire call blocked waiting for a slot.
type waiter struct {
	priority int64         // TarSize in bytes — larger = higher priority
	weight   int           // admission units this job costs (see jobWeight)
	index    int           // position in the heap (maintained by heap.Interface)
	ready    chan struct{} // closed by Release() when the slot is granted
	ctx      context.Context
}

// waiterHeap implements heap.Interface as a max-heap ordered by priority.
type waiterHeap []*waiter

func (h waiterHeap) Len() int           { return len(h) }
func (h waiterHeap) Less(i, j int) bool { return h[i].priority > h[j].priority } // max-heap
func (h waiterHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *waiterHeap) Push(x any) {
	w := x.(*waiter)
	w.index = len(*h)
	*h = append(*h, w)
}
func (h *waiterHeap) Pop() any {
	old := *h
	n := len(old)
	w := old[n-1]
	old[n-1] = nil // prevent memory leak
	*h = old[:n-1]
	w.index = -1
	return w
}

// DynamicSemaphore is a concurrency gate whose effective slot count tracks the
// current system load.  It dispatches waiting Acquire calls in descending
// priority (TarSize) order so large jobs start first.
type DynamicSemaphore struct {
	// minSlots is the floor for the effective slot count (never goes below this).
	minSlots int
	// maxSlots is the ceiling (defaults to runtime.NumCPU() at construction).
	maxSlots int

	mu      sync.Mutex
	waiters waiterHeap // max-heap of pending Acquire calls
	// inFlight is the sum of granted WEIGHTS, not a job count — see jobWeight.
	inFlight int
	// inFlightJobs is the number of jobs holding a grant, for reporting only.
	inFlightJobs int
	load         float64 // most recent 1-min load average

	logger *slog.Logger

	stopOnce sync.Once
	done     chan struct{} // closed by Stop()
}

// NewDynamicSemaphore constructs a DynamicSemaphore and starts the background
// load-poller goroutine.  minSlots is the guaranteed minimum concurrency (≥1).
// maxSlots is the upper bound; pass 0 to use runtime.NumCPU().
func NewDynamicSemaphore(minSlots, maxSlots int, logger *slog.Logger) *DynamicSemaphore {
	if minSlots < 1 {
		minSlots = 1
	}
	if maxSlots <= 0 {
		maxSlots = runtime.NumCPU()
	}
	if maxSlots < minSlots {
		maxSlots = minSlots
	}

	ds := &DynamicSemaphore{
		minSlots: minSlots,
		maxSlots: maxSlots,
		logger:   logger,
		done:     make(chan struct{}),
	}
	heap.Init(&ds.waiters)

	// Seed the initial load so the first Acquire does not start at 0.
	if l, err := readLoadAvg(); err == nil {
		ds.load = l
	}

	go ds.loadPoller()
	return ds
}

// effectiveSlotsLocked returns the current slot limit.  Must be called with
// ds.mu held.
func (ds *DynamicSemaphore) effectiveSlotsLocked() int {
	free := ds.maxSlots - int(ds.load)
	if free < ds.minSlots {
		return ds.minSlots
	}
	return free
}

// Acquire waits until there is a free slot or ctx is cancelled.  priority is
// the job's TarSize in bytes — higher values are dispatched first when multiple
// jobs are waiting.  Returns nil when a slot is granted, ctx.Err() on
// cancellation.
//
// Race safety: when a slot is granted concurrently with ctx cancellation,
// both w.ready and ctx.Done() may be readable at the same time.  We resolve
// this under the lock — the same technique used by golang.org/x/sync/semaphore:
// in the ctx.Done() branch, do a non-blocking receive on w.ready; if it
// succeeds, return nil (the grant wins) so the slot is never leaked.
func (ds *DynamicSemaphore) Acquire(ctx context.Context, tarBytes int64) (int, error) {
	ds.mu.Lock()

	weight := jobWeight(tarBytes, ds.effectiveSlotsLocked())

	// Fast path — take the slots immediately if they are available and no one is
	// already queued (to avoid skipping ahead of higher-priority waiters).
	if ds.inFlight+weight <= ds.effectiveSlotsLocked() && ds.waiters.Len() == 0 {
		ds.inFlight += weight
		ds.inFlightJobs++
		ds.mu.Unlock()
		return weight, nil
	}

	// Slow path — park on a private channel until Release() grants us the slots
	// or our context is cancelled.
	w := &waiter{
		priority: tarBytes,
		weight:   weight,
		ready:    make(chan struct{}),
		ctx:      ctx,
	}
	heap.Push(&ds.waiters, w)
	ds.mu.Unlock()

	select {
	case <-w.ready:
		// Release() closed the channel — the slots are ours.
		return w.weight, nil

	case <-ctx.Done():
		// Cancelled while waiting.  Take the lock so we can check atomically
		// whether Release() raced to grant us a slot at the same instant.
		ds.mu.Lock()
		defer ds.mu.Unlock()

		select {
		case <-w.ready:
			// Release() granted us a slot concurrently — accept it.
			// The caller owns the slot and must call Release() when done.
			// (The job will fail quickly anyway because its runCtx is also
			// derived from abortCtx which is now cancelled.)
			return w.weight, nil
		default:
			// Not granted yet.  Remove from the heap so Release() never
			// grants us a slot after we return.
			if w.index >= 0 && w.index < ds.waiters.Len() {
				heap.Remove(&ds.waiters, w.index)
			}
			return 0, ctx.Err()
		}
	}
}

// Release returns a slot.  It pops the highest-priority non-cancelled waiter
// and grants the slot directly to that goroutine, bypassing the thundering-
// herd issue of sync.Cond.Broadcast().
func (ds *DynamicSemaphore) Release(weight int) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if weight < 1 {
		weight = 1
	}
	ds.inFlight -= weight
	if ds.inFlight < 0 {
		ds.inFlight = 0
	}
	if ds.inFlightJobs > 0 {
		ds.inFlightJobs--
	}
	ds.grantLocked()
}

// grantLocked hands out slots to waiting jobs in priority order. Callers hold
// ds.mu.
//
// It stops at the first waiter that does not fit rather than skipping to a
// smaller one. That is deliberate head-of-line blocking: waiters are ordered by
// tar size, so the one at the front is the largest, and letting small jobs
// overtake it indefinitely is exactly how a big package never runs. Idling a
// few units until it fits is the cheaper mistake — and on storage where the big
// job is the bottleneck, running it sooner is what shortens the whole build.
func (ds *DynamicSemaphore) grantLocked() {
	limit := ds.effectiveSlotsLocked()
	for ds.waiters.Len() > 0 {
		if ds.waiters[0].ctx.Err() != nil {
			heap.Pop(&ds.waiters) // cancelled — drop and continue
			continue
		}
		w := ds.waiters[0]
		if ds.inFlight+w.weight > limit {
			// Escape hatch: a job whose weight exceeds the CURRENT limit (the
			// limit can shrink under load after the weight was computed) would
			// otherwise wait forever. When nothing else is running, let it
			// through — it runs alone, which is what it would have done anyway.
			if ds.inFlight > 0 {
				return
			}
		}
		heap.Pop(&ds.waiters)
		ds.inFlight += w.weight
		ds.inFlightJobs++
		close(w.ready)
	}
}

// Stop shuts down the background load-poller.  Subsequent Acquire calls still
// work (using the last observed load value) but the effective limit will no
// longer be updated.
func (ds *DynamicSemaphore) Stop() {
	ds.stopOnce.Do(func() { close(ds.done) })
}

// loadPoller runs in a background goroutine; it reads /proc/loadavg every
// loadPollInterval and, when the effective slot count increases, wakes
// additional top-priority waiters proportional to the newly available capacity.
func (ds *DynamicSemaphore) loadPoller() {
	ticker := time.NewTicker(loadPollInterval)
	defer ticker.Stop()

	ds.mu.Lock()
	prevLimit := ds.effectiveSlotsLocked()
	ds.mu.Unlock()

	for {
		select {
		case <-ds.done:
			return
		case <-ticker.C:
			newLoad, err := readLoadAvg()
			if err != nil {
				// /proc/loadavg unavailable (non-Linux, container quirk, …).
				// Keep previous load; don't spam the log.
				continue
			}

			ds.mu.Lock()
			oldLoad := ds.load
			ds.load = newLoad
			newLimit := ds.effectiveSlotsLocked()

			if newLimit > prevLimit {
				// More budget: hand it out by weight, same rule as Release.
				ds.grantLocked()
			}
			inFlightJobs := ds.inFlightJobs
			inFlightWeight := ds.inFlight
			ds.mu.Unlock()

			if newLimit != prevLimit {
				ds.logger.Info("dynamic concurrency limit updated",
					"prev_limit", prevLimit,
					"new_limit", newLimit,
					"load_avg_1min", fmt.Sprintf("%.2f", newLoad),
					"in_flight", inFlightJobs,
					"in_flight_weight", inFlightWeight,
					"min_slots", ds.minSlots,
					"max_slots", ds.maxSlots,
				)
				prevLimit = newLimit
			} else if newLoad != oldLoad {
				ds.logger.Debug("load updated (limit unchanged)",
					"load_avg_1min", fmt.Sprintf("%.2f", newLoad),
					"effective_limit", newLimit,
				)
			}
		}
	}
}

// readLoadAvg is defined in dynasem_linux.go (Linux) and dynasem_other.go
// (all other platforms).  The Linux implementation reads /proc/loadavg; the
// stub always returns 0 so the semaphore acts as a static pool on non-Linux.
