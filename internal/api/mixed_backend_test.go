// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

// A node may be started with more than one publish path — `--publish-mode local
// --ingest-publish` is the combination used for testing both against one
// service. Each individual publish still uses exactly one path, but two jobs on
// one repository could pick different ones, and each backend only knows about
// its OWN concurrency control (LocalBackend's fail-fast per-repo semaphore, the
// ingest backend's per-repo slot queue). Neither can see the other.
//
// What actually keeps them apart is the orchestrator's per-repo commit lock,
// which is backend-agnostic and now covers no-pipeline jobs too. These tests
// pin that: two jobs on one repository never publish concurrently regardless of
// which paths they chose, and two jobs on DIFFERENT repositories still do.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/lease"
)

// trackingBackend records how many Commits are in flight at once, and the
// maximum ever observed. NeedsPipeline is false so it takes the same code path
// as LocalBackend and IngestBackend.
type trackingBackend struct {
	noopBackend
	shared *commitTracker
	name   string
}

type commitTracker struct {
	mu       sync.Mutex
	inFlight map[string]int // repo → concurrent commits
	maxSeen  map[string]int // repo → high-water mark
	order    []string       // backend names, in commit order
}

func newCommitTracker() *commitTracker {
	return &commitTracker{
		inFlight: map[string]int{},
		maxSeen:  map[string]int{},
	}
}

func (c *commitTracker) enter(repo, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inFlight[repo]++
	if c.inFlight[repo] > c.maxSeen[repo] {
		c.maxSeen[repo] = c.inFlight[repo]
	}
	c.order = append(c.order, name)
}

func (c *commitTracker) leave(repo string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inFlight[repo]--
}

func (c *commitTracker) max(repo string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxSeen[repo]
}

// Acquire returns the repository as the token, as LocalBackend and
// IngestBackend both do — Commit then knows which repository it is publishing.
func (t *trackingBackend) Acquire(_ context.Context, repo, _ string) (string, error) {
	return repo, nil
}

func (t *trackingBackend) Commit(_ context.Context, req lease.CommitRequest) error {
	repo := req.Token
	t.shared.enter(repo, t.name)
	// Long enough that a second job would overlap if nothing serialised them.
	time.Sleep(80 * time.Millisecond)
	t.shared.leave(repo)
	return nil
}

// submitOne posts a job and returns its id.
func submitOne(t *testing.T, srv *Server, fields map[string]string) string {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.submitJob(rec, newMultipartRequest(t, fields, []byte("payload")))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("submit %v: want 202, got %d: %s", fields, rec.Code, rec.Body.String())
	}
	var resp struct {
		JobID string `json:"job_id"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return resp.JobID
}

// TestMixedPublishPaths_SerialisePerRepo is the test that makes
// `--publish-mode local --ingest-publish` safe to run: two jobs on the same
// repository taking DIFFERENT publish paths must not commit at the same time,
// even though neither backend can see the other's lock.
func TestMixedPublishPaths_SerialisePerRepo(t *testing.T) {
	srv, _, orch := newTestServer(t)
	tracker := newCommitTracker()
	orch.Lease = &trackingBackend{shared: tracker, name: "prepub"}
	orch.PublishPaths = map[string]lease.Backend{
		"ingest": &trackingBackend{shared: tracker, name: "ingest"},
	}

	const repo = "software.cern.ch"
	var wg sync.WaitGroup
	for _, path := range []string{"", "ingest"} {
		wg.Add(1)
		go func(publishPath string) {
			defer wg.Done()
			fields := map[string]string{"repo": repo, "path": "x86_64-el9/pkg/1.0"}
			if publishPath != "" {
				fields["publish_path"] = publishPath
				fields["path"] = "x86_64-el9/other/1.0"
			}
			submitOne(t, srv, fields)
		}(path)
	}
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	t.Logf("max concurrent commits on %s = %d (order %v)", repo, tracker.max(repo), tracker.order)
	if got := tracker.max(repo); got > 1 {
		t.Errorf("%d concurrent commits on %s; publishes on one repository must "+
			"serialise across publish paths (the per-repo commit lock is what "+
			"stops a local publish and an ingest publish colliding)", got, repo)
	}
	if len(tracker.order) != 2 {
		t.Errorf("expected both jobs to commit, saw %v", tracker.order)
	}
}

// TestMixedPublishPaths_DifferentReposStillParallel guards the other direction:
// the serialisation must be per repository, not a global publish lock, or one
// community's build would stall every other community's.
func TestMixedPublishPaths_DifferentReposStillParallel(t *testing.T) {
	srv, _, orch := newTestServer(t)

	var started atomic.Int32
	release := make(chan struct{})
	blocking := &blockingBackend{started: &started, release: release}
	orch.Lease = blocking
	orch.PublishPaths = map[string]lease.Backend{"ingest": blocking}

	submitOne(t, srv, map[string]string{"repo": "a.cern.ch", "path": "p/1"})
	submitOne(t, srv, map[string]string{"repo": "b.cern.ch", "path": "p/1", "publish_path": "ingest"})

	// Both must reach Commit without either finishing: a global lock would let
	// only one in.
	deadline := time.After(10 * time.Second)
	for started.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("only %d of 2 jobs reached commit; publishes on different "+
				"repositories must proceed in parallel", started.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}
	close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// blockingBackend parks in Commit until released, so a test can observe how
// many jobs are inside Commit at once.
type blockingBackend struct {
	noopBackend
	started *atomic.Int32
	release chan struct{}
}

func (b *blockingBackend) Commit(ctx context.Context, _ lease.CommitRequest) error {
	b.started.Add(1)
	select {
	case <-b.release:
	case <-ctx.Done():
	}
	return nil
}
