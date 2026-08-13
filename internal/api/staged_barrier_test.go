// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

// The serialize-until-published barrier for staged jobs.
//
// The barrier holds the per-repo commit lock until stratum0 reflects the
// commit, so the next publish grafts onto a base that already contains it.
// Its gate was `subtreeResult != nil` -- the PIPELINE's catalog build -- which
// excluded the one publish kind that always grafts. Two staged publishes in a
// row would then read old_root_hash from a stratum0 that had not caught up, and
// the second graft fails with the spurious merge_error the barrier prevents.
//
// The barrier also records j.NewRootHash. Without it the post-commit MQTT
// broadcast is skipped and Stratum 1 receivers only learn from the backstop
// poll, so asserting the hash is asserting the notification too.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/lease"
)

// stratum0 serves .cvmfspublished and models the one thing that matters here:
// the published root advances when a COMMIT happens, and only after a lag.
//
// An earlier version advanced once and then held that value forever. That made
// every barrier after the first poll to the context deadline -- the content
// commit's base equalled the current root, so "advanced past the base" was
// never true -- and the tests still passed, 20 s each, on the value the barrier
// returns when it gives up. Modelling the commit is what makes the barrier
// observable rather than merely slow.
type stratum0 struct {
	mu      sync.Mutex
	reads   int
	root    string
	pending int // reads remaining before a committed change becomes visible
	lag     int
	gen     int
	url     string
}

func newStratum0(t *testing.T, lag int) *stratum0 {
	t.Helper()
	s := &stratum0{root: strings.Repeat("a", 40), lag: lag}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/.cvmfspublished") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		s.mu.Lock()
		s.reads++
		if s.pending > 0 {
			s.pending--
			if s.pending == 0 {
				s.gen++
				s.root = strings.Repeat(string(rune('b'+s.gen)), 40)
			}
		}
		root := s.root
		s.mu.Unlock()
		w.Write([]byte("C" + root + "\nNsoftware.cern.ch\n"))
	}))
	t.Cleanup(srv.Close)
	s.url = srv.URL
	return s
}

// committed is called by the fake backend: the repository has moved, and
// stratum0 will show it after `lag` more reads.
func (s *stratum0) committed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = s.lag
}

func (s *stratum0) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

func (s *stratum0) currentRoot() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.root
}

// committingBackend tells the fake stratum0 that the repository moved.
type committingBackend struct {
	noopBackend
	mu      sync.Mutex
	s0      *stratum0
	commits []lease.CommitRequest
}

func (c *committingBackend) Commit(_ context.Context, req lease.CommitRequest) error {
	c.mu.Lock()
	c.commits = append(c.commits, req)
	c.mu.Unlock()
	c.s0.committed()
	return nil
}
func (c *committingBackend) snapshot() []lease.CommitRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]lease.CommitRequest(nil), c.commits...)
}

// A staged publish waits for its commit to appear on stratum0, and records the
// resulting root hash.
//
// NEGATIVE CONTROL: restore the gate to `subtreeResult != nil` and the barrier
// is skipped — j.NewRootHash stays empty and stratum0 is read once (the
// old_root_hash fetch) rather than polled. Verified.
func TestRun_StagedPublishWaitsForPropagation(t *testing.T) {
	_, _, orch := newTestServer(t)
	s0 := newStratum0(t, 2) // the commit takes two reads to become visible

	cb := &committingBackend{s0: s0}
	orch.Lease = cb
	orch.PublishPaths = map[string]lease.Backend{StagedPublishPath: cb}
	orch.CAS = newFakeCAS(stagedCatalog)
	orch.Stratum0URL = s0.url

	j := stagedJob(t, orch, "staging/host7/job-1")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := orch.Run(ctx, j, nil); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The barrier polls: one read for old_root_hash, then at least one more
	// before the manifest advances. A skipped barrier reads exactly once.
	if n := s0.readCount(); n < 2 {
		t.Errorf("stratum0 was read %d times; the barrier did not poll", n)
	}
	// The root the receiver produced, recorded for S1 propagation tracking. An
	// empty value here is what silences the post-commit MQTT broadcast.
	if j.NewRootHash == "" {
		t.Error("j.NewRootHash is empty: the barrier did not record the new root, " +
			"so Stratum 1 receivers are never told about this publish")
	}
	if strings.HasSuffix(j.NewRootHash, "C") {
		t.Errorf("j.NewRootHash = %q must be plain hex, with no content-type suffix",
			j.NewRootHash)
	}
	if want := s0.currentRoot(); j.NewRootHash != want {
		t.Errorf("j.NewRootHash = %q, want the ADVANCED root %q — the barrier "+
			"returned before stratum0 caught up", j.NewRootHash, want)
	}
}

// Two staged publishes to one repository, in sequence. The second must read a
// base that already contains the first, which is the whole purpose of holding
// the lock across the barrier.
func TestRun_SecondStagedPublishSeesTheFirst(t *testing.T) {
	_, _, orch := newTestServer(t)
	s0 := newStratum0(t, 1)

	cb := &committingBackend{s0: s0}
	orch.Lease = cb
	orch.PublishPaths = map[string]lease.Backend{StagedPublishPath: cb}
	orch.CAS = newFakeCAS(stagedCatalog)
	orch.Stratum0URL = s0.url

	for i, id := range []string{"job-a", "job-b"} {
		j := job.NewJob(id, "software.cern.ch", "", "")
		// Single-component paths: a deeper one would also drive a parent-dir
		// commit through this same backend, and the count below is about the
		// content commits.
		j.Path = "pkg-" + string(rune('a'+i))
		j.PublishPath = StagedPublishPath
		j.StagingPrefix = "staging/host7/" + id
		j.CatalogHash = stagedCatalog
		if err := orch.Spool.WriteManifest(j); err != nil {
			t.Fatalf("WriteManifest: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		err := orch.Run(ctx, j, nil)
		cancel()
		if err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		if j.NewRootHash == "" {
			t.Errorf("%s recorded no new root hash", id)
		}
	}

	// Both grafted, and the second's base was read after the first's barrier
	// released the lock.
	commits := cb.snapshot()
	if len(commits) != 2 {
		t.Fatalf("want 2 commits, got %d", len(commits))
	}
	for i, req := range commits {
		if !req.DirectGraft {
			t.Errorf("commit %d was not a graft", i)
		}
	}
	if commits[1].OldRootHash == "" {
		t.Fatal("the second publish committed against an empty base")
	}
	// The point of the barrier: the second job's base is the root the FIRST
	// job's commit produced, not the one that predated it.
	if commits[1].OldRootHash == commits[0].OldRootHash {
		t.Errorf("both publishes committed against the same base %q — the second "+
			"did not wait for the first to appear on stratum0", commits[0].OldRootHash)
	}
}

// The control: a job with no staging prefix and no subtree result must not
// start waiting on stratum0 — that would add the barrier's latency to publish
// kinds that never grafted anything.
func TestRun_UnstagedJobDoesNotEnterTheStagedBarrier(t *testing.T) {
	_, sp, orch := newTestServer(t)
	s0 := newStratum0(t, 1)

	cb := &committingBackend{s0: s0}
	orch.Lease = cb
	orch.PublishPaths = map[string]lease.Backend{"ingest": cb}
	orch.CAS = newFakeCAS()
	orch.Stratum0URL = s0.url

	j := job.NewJob("job-plain", "software.cern.ch", "", "")
	j.Path = "pkg/1.0"
	j.PublishPath = "ingest"
	if err := sp.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := orch.Run(ctx, j, nil); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if j.NewRootHash != "" {
		t.Errorf("an ingest job recorded NewRootHash = %q; it did not graft a subtree",
			j.NewRootHash)
	}
	if n := s0.readCount(); n != 0 {
		t.Errorf("an ingest job read stratum0 %d times, want 0", n)
	}
}
