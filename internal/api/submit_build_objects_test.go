// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

package api

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/lease"
	"cvmfs.io/prepub/internal/pipeline"
	"cvmfs.io/prepub/pkg/observe"
)

// gatewayStub is a lease.Backend that also implements payloadSubmitter, so it
// exercises the same code path as the real gateway client.
type gatewayStub struct {
	mu sync.Mutex

	acquired   []string // paths leased
	submitted  []string // object hashes pushed to the repository
	catalogArg string   // catalogHash passed to SubmitPayload
	aborts     int      // Release(commit=false)
	commits    int      // must stay 0: the finalize owns the commit
	submitErr  error
}

func (g *gatewayStub) Acquire(_ context.Context, _, path string) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.acquired = append(g.acquired, path)
	return "stub-token", nil
}

func (g *gatewayStub) Heartbeat(_ context.Context, _ string, _ time.Duration, _ context.CancelFunc) func() {
	return func() {}
}

func (g *gatewayStub) Commit(_ context.Context, _ lease.CommitRequest) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.commits++
	return nil
}

func (g *gatewayStub) Abort(_ context.Context, _ string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.aborts++
	return nil
}

func (g *gatewayStub) NeedsPipeline() bool           { return true }
func (g *gatewayStub) Probe(_ context.Context) error { return nil }

func (g *gatewayStub) SubmitPayload(_ context.Context, _, catalogHash string,
	objectHashes []string, _ lease.ObjectReader) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.submitErr != nil {
		return g.submitErr
	}
	g.catalogArg = catalogHash
	g.submitted = append(g.submitted, objectHashes...)
	return nil
}

func testObs(t *testing.T) *observe.Provider {
	t.Helper()
	obs, shutdown, err := observe.New("test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)
	return obs
}

func testJob() *job.Job {
	return &job.Job{ID: "job-1", Repo: "test.cvmfs.io", Path: "x86_64-el9/Packages/ROOT/v1",
		BuildID: "build-42"}
}

// The coarse-publish path must push objects to the REPOSITORY as each package
// arrives. Staging alone leaves them in the prepub's own CAS, and the deferred
// finalize would then commit catalogs referencing content the repository never
// received — every client read failing with EIO.
func TestSubmitBuildObjects_PushesObjectsWithoutCommitting(t *testing.T) {
	g := &gatewayStub{}
	o := &Orchestrator{Lease: g, Obs: testObs(t)}
	res := &pipeline.Result{NewObjectHashes: []string{"aaa", "bbbP", "ccc"}}

	if err := o.submitBuildObjects(context.Background(), testJob(), res); err != nil {
		t.Fatalf("submitBuildObjects: %v", err)
	}

	if len(g.submitted) != 3 {
		t.Errorf("submitted %d objects, want 3 (%v)", len(g.submitted), g.submitted)
	}
	if g.catalogArg != "" {
		t.Errorf("catalogHash=%q, want empty — the finalize builds the catalog", g.catalogArg)
	}
	if g.commits != 0 {
		t.Errorf("commits=%d, want 0 — the single commit belongs to the finalize", g.commits)
	}
	if g.aborts != 1 {
		t.Errorf("aborts=%d, want 1 — the lease must be released without commit", g.aborts)
	}
	// The lease must be scoped to this package's own path so concurrent
	// packages take disjoint leases and keep uploading in parallel.
	if len(g.acquired) != 1 || g.acquired[0] != "x86_64-el9/Packages/ROOT/v1" {
		t.Errorf("acquired=%v, want one lease on the package path", g.acquired)
	}
}

func TestSubmitBuildObjects_NoNewObjectsSkipsLease(t *testing.T) {
	g := &gatewayStub{}
	o := &Orchestrator{Lease: g, Obs: testObs(t)}

	// Everything deduplicated: nothing to push, so no lease at all.
	if err := o.submitBuildObjects(context.Background(), testJob(),
		&pipeline.Result{}); err != nil {
		t.Fatalf("submitBuildObjects: %v", err)
	}
	if len(g.acquired) != 0 || len(g.submitted) != 0 {
		t.Errorf("expected no lease/submission, got acquired=%v submitted=%v",
			g.acquired, g.submitted)
	}
}

func TestSubmitBuildObjects_SubmitFailureReleasesLease(t *testing.T) {
	g := &gatewayStub{submitErr: errors.New("gateway down")}
	o := &Orchestrator{Lease: g, Obs: testObs(t)}

	err := o.submitBuildObjects(context.Background(), testJob(),
		&pipeline.Result{NewObjectHashes: []string{"aaa"}})
	if err == nil {
		t.Fatal("expected the submission error to propagate")
	}
	if g.aborts != 1 {
		t.Errorf("aborts=%d, want 1 — the lease must be released on failure too", g.aborts)
	}
	if g.commits != 0 {
		t.Errorf("commits=%d, want 0", g.commits)
	}
}

// Local/dev backends do not implement payloadSubmitter and need no push.
func TestSubmitBuildObjects_LocalBackendIsNoop(t *testing.T) {
	o := &Orchestrator{Lease: &noopBackend{}, Obs: testObs(t)}
	if err := o.submitBuildObjects(context.Background(), testJob(),
		&pipeline.Result{NewObjectHashes: []string{"aaa"}}); err != nil {
		t.Fatalf("local backend must be a no-op, got: %v", err)
	}
}
