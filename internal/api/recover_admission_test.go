// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/lease"
	"cvmfs.io/prepub/internal/notify"
	"cvmfs.io/prepub/internal/spool"
	"cvmfs.io/prepub/pkg/observe"
)

// peakBackend records how many commits ran at the same time. In local mode the
// concurrency slot is held through Commit, so this is the admission count.
type peakBackend struct {
	noopBackend
	mu           sync.Mutex
	active, peak int
}

func (b *peakBackend) Commit(_ context.Context, _ lease.CommitRequest) error {
	b.mu.Lock()
	b.active++
	b.peak = max(b.peak, b.active)
	b.mu.Unlock()
	time.Sleep(30 * time.Millisecond)
	b.mu.Lock()
	b.active--
	b.mu.Unlock()
	return nil
}

// Jobs recovered at startup must queue for a concurrency slot like new
// submissions do; previously each ran at once, whatever the limit.
func TestRecoverJob_UsesTheConcurrencyLimit(t *testing.T) {
	dir := t.TempDir()
	obs, shutdown, err := observe.New("test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shutdown)
	sp, err := spool.New(dir, obs)
	if err != nil {
		t.Fatal(err)
	}
	nb := notify.NewBus()
	be := &peakBackend{}
	orch := &Orchestrator{Spool: sp, Notify: nb, Obs: obs, Lease: be}
	srv := New(obs, "", orch, sp, nb, dir, "", 1 /*min*/, 1 /*max: one slot*/)
	t.Cleanup(func() { srv.jobWg.Wait(); srv.dynaSem.Stop() })

	var ids []string
	for i := range 4 {
		j := &job.Job{ID: fmt.Sprintf("rec-%d", i), Repo: "software.cern.ch", Path: "p",
			State: job.StateIncoming, CreatedAt: time.Now()}
		if err := sp.WriteManifest(j); err != nil {
			t.Fatal(err)
		}
		j.TarPath = filepath.Join(sp.JobDir(j), "payload.tar")
		if err := os.WriteFile(j.TarPath, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := srv.RecoverJob(context.Background(), j, true); err != nil {
			t.Fatalf("RecoverJob: %v", err)
		}
		ids = append(ids, j.ID)
	}
	for _, id := range ids {
		if j := waitTerminal(t, sp, id); j.State != job.StatePublished {
			t.Fatalf("job %s ended %s: %s", id, j.State, j.Error)
		}
	}
	be.mu.Lock()
	defer be.mu.Unlock()
	if be.peak != 1 {
		t.Errorf("recovered jobs ran %d at once; the limit is 1", be.peak)
	}
}

// A recovered job queued for a concurrency slot must not hold up Shutdown,
// and must be left in incoming (not aborted) so it recovers on the next
// start. A job launched after Shutdown is left there too.
func TestLaunch_ShutdownEndsSlotWait(t *testing.T) {
	dir := t.TempDir()
	obs, shutdown, err := observe.New("test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shutdown)
	sp, err := spool.New(dir, obs)
	if err != nil {
		t.Fatal(err)
	}
	nb := notify.NewBus()
	orch := &Orchestrator{Spool: sp, Notify: nb, Obs: obs, Lease: &noopBackend{}}
	srv := New(obs, "", orch, sp, nb, dir, "", 1, 1)

	// Hold the only slot so the recovered job has to queue.
	held, err := srv.dynaSem.Acquire(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.dynaSem.Release(held)

	newJob := func(id string) *job.Job {
		j := &job.Job{ID: id, Repo: "software.cern.ch", Path: "p", State: job.StateIncoming, CreatedAt: time.Now()}
		if err := sp.WriteManifest(j); err != nil {
			t.Fatal(err)
		}
		j.TarPath = filepath.Join(sp.JobDir(j), "payload.tar")
		if err := os.WriteFile(j.TarPath, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		return j
	}
	if err := srv.RecoverJob(context.Background(), newJob("queued"), true); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); ; {
		srv.dynaSem.mu.Lock()
		n := srv.dynaSem.waiters.Len()
		srv.dynaSem.mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("job never queued for the slot")
		}
		time.Sleep(5 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown with a queued job: %v", err)
	}
	srv.launch(newJob("late")) // after Shutdown: not started

	for _, id := range []string{"queued", "late"} {
		got, err := sp.FindJob(id)
		if err != nil {
			t.Fatal(err)
		}
		if got.State != job.StateIncoming || got.Error != "" {
			t.Errorf("%s: state %s error %q; want incoming, no error", id, got.State, got.Error)
		}
	}
}
