// Package api — dynasem.go: load-aware dynamic concurrency semaphore.
//
// DynamicSemaphore limits the number of concurrently active jobs based on the
// current machine load.  The effective slot count is recomputed every ~5 s:
//
//	effectiveSlots = max(MinSlots, MaxSlots - int(load1min))
//
// where load1min is the 1-minute load average read from /proc/loadavg and
// MaxSlots defaults to runtime.NumCPU().  As load drops, waiters are woken
// immediately via sync.Cond so they can re-evaluate the limit.
//
// Context cancellation (abort, shutdown) is respected: Acquire returns
// ctx.Err() as soon as the context is done, without holding any lock.
package api

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// loadPollInterval is how often the background goroutine re-reads /proc/loadavg.
	loadPollInterval = 5 * time.Second
)

// DynamicSemaphore is a concurrency gate whose effective slot count tracks the
// current system load.  It replaces the static semaphore.Weighted used for
// MaxConcurrentJobs.
type DynamicSemaphore struct {
	// MinSlots is the floor for the effective slot count (never goes below this).
	minSlots int
	// MaxSlots is the ceiling (defaults to runtime.NumCPU() at construction).
	maxSlots int

	mu       sync.Mutex
	cond     *sync.Cond
	inFlight int     // number of acquired (unreleased) slots
	load     float64 // most recent 1-min load average

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
	ds.cond = sync.NewCond(&ds.mu)

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

// Acquire waits until there is a free slot or ctx is cancelled.
// Returns nil when a slot is granted, ctx.Err() on cancellation.
func (ds *DynamicSemaphore) Acquire(ctx context.Context) error {
	// Fast path — try without waiting.
	ds.mu.Lock()
	if ds.inFlight < ds.effectiveSlotsLocked() {
		ds.inFlight++
		ds.mu.Unlock()
		return nil
	}
	ds.mu.Unlock()

	// Slow path — park on cond until either a slot opens or ctx is cancelled.
	// We spin a small watcher goroutine that broadcasts on ctx.Done so that
	// the Cond.Wait below wakes up promptly.
	wakeCtx, wakeCancel := context.WithCancel(ctx)
	defer wakeCancel()
	go func() {
		<-wakeCtx.Done()
		ds.cond.Broadcast() // wake all waiters so they can check ctx.Err()
	}()

	ds.mu.Lock()
	defer ds.mu.Unlock()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if ds.inFlight < ds.effectiveSlotsLocked() {
			ds.inFlight++
			return nil
		}
		ds.cond.Wait()
	}
}

// Release returns a slot and wakes waiting Acquire callers.
func (ds *DynamicSemaphore) Release() {
	ds.mu.Lock()
	if ds.inFlight > 0 {
		ds.inFlight--
	}
	ds.mu.Unlock()
	ds.cond.Broadcast()
}

// Stop shuts down the background load-poller.  Subsequent Acquire calls still
// work (using the last observed load value) but the effective limit will no
// longer be updated.
func (ds *DynamicSemaphore) Stop() {
	ds.stopOnce.Do(func() { close(ds.done) })
}

// loadPoller runs in a background goroutine; it reads /proc/loadavg every
// loadPollInterval and broadcasts to all Acquire waiters when the limit
// increases (more slots available) or when callers are stuck at the floor.
func (ds *DynamicSemaphore) loadPoller() {
	ticker := time.NewTicker(loadPollInterval)
	defer ticker.Stop()
	prevLimit := ds.effectiveSlotsLocked() // safe — poller has not started yet

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
			ds.mu.Unlock()

			if newLimit != prevLimit {
				ds.logger.Info("dynamic concurrency limit updated",
					"prev_limit", prevLimit,
					"new_limit", newLimit,
					"load_avg_1min", fmt.Sprintf("%.2f", newLoad),
					"in_flight", ds.inFlight,
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

			// Always broadcast — if limit increased, waiting Acquires can proceed.
			ds.cond.Broadcast()
		}
	}
}

// readLoadAvg reads the 1-minute load average from /proc/loadavg.
// Returns an error on non-Linux systems or if the file is unreadable.
func readLoadAvg() (float64, error) {
	f, err := os.Open("/proc/loadavg")
	if err != nil {
		return 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	if !scanner.Scan() {
		return 0, fmt.Errorf("empty /proc/loadavg")
	}
	fields := strings.Fields(scanner.Text())
	if len(fields) < 1 {
		return 0, fmt.Errorf("unexpected /proc/loadavg format")
	}
	return strconv.ParseFloat(fields[0], 64)
}
