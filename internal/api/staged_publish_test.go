// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

// The staged publish path through Run: a job naming a staging prefix has its
// objects promoted into the CAS and its catalog grafted, with no tar anywhere.
//
// What these tests are really guarding is the ORDER and the CONTENT of the two
// calls. Grafting before promoting, or grafting the wrong hash, both produce a
// gateway that accepts the commit and a repository that serves EIO later, so
// the failure would surface a long way from the cause.

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/lease"
)

// capturingBackend records the CommitRequest instead of publishing it.
type capturingBackend struct {
	noopBackend
	mu        sync.Mutex
	committed []lease.CommitRequest
	// promotedBy is read at Commit time to prove promotion happened FIRST.
	promotedBy func() int
	promotedAt int
}

func (c *capturingBackend) Commit(_ context.Context, req lease.CommitRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.promotedBy != nil {
		c.promotedAt = c.promotedBy()
	}
	c.committed = append(c.committed, req)
	return nil
}

func (c *capturingBackend) only(t *testing.T) lease.CommitRequest {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.committed) != 1 {
		t.Fatalf("want exactly one commit, got %d", len(c.committed))
	}
	return c.committed[0]
}

// fakeCAS satisfies cas.Backend and the promoter interface. It models the one
// thing the staged path cares about: which objects are IN the store. PromoteFrom
// puts `adds` there, and Exists answers from it — so a test can express "the
// promotion ran but did not bring the catalog", which is the case the object
// counters cannot distinguish.
type fakeCAS struct {
	mu        sync.Mutex
	calls     []string // staging aliases, in order
	result    cas.PromoteResult
	err       error
	promotes  int
	adds      []string        // what a successful promotion lands in the store
	present   map[string]bool // the store
	existsErr error
}

func newFakeCAS(adds ...string) *fakeCAS {
	return &fakeCAS{adds: adds, present: map[string]bool{},
		result: cas.PromoteResult{Copied: len(adds), Bytes: 4096}}
}

func (f *fakeCAS) PromoteFrom(_ context.Context, alias string, _ int) (cas.PromoteResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, alias)
	f.promotes++
	if f.err != nil {
		return cas.PromoteResult{}, f.err
	}
	for _, h := range f.adds {
		f.present[h] = true
	}
	return f.result, nil
}
func (f *fakeCAS) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.promotes
}

func (f *fakeCAS) Exists(_ context.Context, hash string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.existsErr != nil {
		return false, f.existsErr
	}
	return f.present[hash], nil
}
func (f *fakeCAS) Put(context.Context, string, io.Reader, int64) error {
	return errors.New("fakeCAS: Put must not be called on the staged path")
}
func (f *fakeCAS) Get(context.Context, string) (io.ReadCloser, error) { return nil, nil }
func (f *fakeCAS) Size(context.Context, string) (int64, error)        { return 0, nil }
func (f *fakeCAS) Delete(context.Context, string) error               { return nil }
func (f *fakeCAS) List(context.Context) ([]string, error)             { return nil, nil }

// plainCAS is a Backend that CANNOT promote — the local/filesystem case.
type plainCAS struct{ fakeCAS }

// shadow PromoteFrom so plainCAS does not satisfy promoter.
func (p *plainCAS) PromoteFrom() {}

const stagedCatalog = "abcdef0123456789abcdef0123456789abcdef01C"

// stagedJob builds a job in the shape submitJob produces for a staged publish:
// the two fields set, the staged publish path, and NO tar.
func stagedJob(t *testing.T, orch *Orchestrator, prefix string) *job.Job {
	t.Helper()
	j := job.NewJob("job-staged", "software.cern.ch", "", "")
	j.Path = "x86_64-el9/pkg/1.0"
	j.PublishPath = StagedPublishPath
	j.StagingPrefix = prefix
	j.CatalogHash = stagedCatalog
	if err := orch.Spool.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	return j
}

func runStaged(t *testing.T, orch *Orchestrator, j *job.Job) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return orch.Run(ctx, j, nil)
}

// The whole point of the feature: promote, then graft the producer's catalog.
//
// NEGATIVE CONTROL: drop `|| j.StagingPrefix != ""` from the DirectGraft
// expression and the DirectGraft assertion fails; drop the
// NewRootHashSuffixed assignment and the hash assertion fails. Both verified.
func TestRun_StagedPublishPromotesThenGrafts(t *testing.T) {
	_, _, orch := newTestServer(t)
	fc := newFakeCAS(stagedCatalog)
	cb := &capturingBackend{promotedBy: fc.count}
	orch.CAS = fc
	orch.Lease = &noopBackend{}
	orch.PublishPaths = map[string]lease.Backend{StagedPublishPath: cb, "ingest": cb}

	j := stagedJob(t, orch, "staging/host7/job-1")
	if err := runStaged(t, orch, j); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := fc.calls; len(got) != 1 || got[0] != "staging/host7/job-1" {
		t.Fatalf("PromoteFrom calls = %v, want one for the job's prefix", got)
	}

	req := cb.only(t)
	if !req.DirectGraft {
		t.Error("a staged commit must set DirectGraft; the producer already built the catalog")
	}
	if req.NewRootHashSuffixed != stagedCatalog {
		t.Errorf("NewRootHashSuffixed = %q, want the job's catalog hash %q",
			req.NewRootHashSuffixed, stagedCatalog)
	}
	if req.TarPath != "" {
		t.Errorf("a staged commit must carry no tar, got TarPath = %q", req.TarPath)
	}
	// Ordering: the receiver downloads the catalog by hash during the commit, so
	// a commit that ran before the promotion would fetch an object that is not
	// there. Asserting both happened is not enough.
	if cb.promotedAt != 1 {
		t.Errorf("promotion had run %d times when Commit was called, want 1 — "+
			"the objects must be in the CAS before the graft", cb.promotedAt)
	}
}

// A prefix that is merely wrong lists nothing and copies nothing WITHOUT
// erroring. Grafting anyway publishes a catalog whose objects were never
// promoted, and the repository serves EIO for content that appears published.
//
// NEGATIVE CONTROL: remove the Exists check and this test fails with a commit
// having been issued. Verified.
func TestRun_StagedPublishRefusesAnEmptyPrefix(t *testing.T) {
	_, _, orch := newTestServer(t)
	fc := newFakeCAS() // promotion brings nothing back
	fc.result = cas.PromoteResult{Rejected: 3}
	cb := &capturingBackend{}
	orch.CAS = fc
	orch.Lease = &noopBackend{}
	orch.PublishPaths = map[string]lease.Backend{StagedPublishPath: cb, "ingest": cb}

	err := runStaged(t, orch, stagedJob(t, orch, "staging/host7/typo"))
	if err == nil {
		t.Fatal("a prefix holding no CAS objects must fail the job, not graft")
	}
	if !strings.Contains(err.Error(), "nothing to graft") {
		t.Errorf("error should name the cause, got: %v", err)
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if len(cb.committed) != 0 {
		t.Error("no commit may be issued when nothing was promoted")
	}
}

// The case object counters cannot see: the promotion moved things, but not the
// catalog the job names. Counting promoted objects passes this and grafts a
// catalog the receiver cannot fetch; asking whether the catalog is there does
// not.
//
// NEGATIVE CONTROL: replace the Exists check with `res.Copied+res.Skipped == 0`
// and this test fails with a commit having been issued. Verified.
func TestRun_StagedPublishRefusesWhenTheCatalogIsMissing(t *testing.T) {
	_, _, orch := newTestServer(t)
	// Something WAS promoted — just not the catalog.
	fc := newFakeCAS("0000000000000000000000000000000000000000")
	cb := &capturingBackend{}
	orch.CAS = fc
	orch.Lease = &noopBackend{}
	orch.PublishPaths = map[string]lease.Backend{StagedPublishPath: cb, "ingest": cb}

	err := runStaged(t, orch, stagedJob(t, orch, "staging/host7/job-1"))
	if err == nil {
		t.Fatal("a promotion that did not bring the catalog must fail the job")
	}
	if !strings.Contains(err.Error(), stagedCatalog) {
		t.Errorf("error should name the missing catalog, got: %v", err)
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if len(cb.committed) != 0 {
		t.Error("no commit may be issued when the catalog is not in the store")
	}
}

// A retry whose producer has since cleaned up its staging prefix promotes
// nothing — but every object is already in the store, so the graft is still
// correct. The counter-based guard failed this; the Exists check passes it.
func TestRun_StagedPublishSucceedsWhenAlreadyPromoted(t *testing.T) {
	_, _, orch := newTestServer(t)
	fc := newFakeCAS()
	fc.present[stagedCatalog] = true // a previous attempt promoted it
	fc.result = cas.PromoteResult{}  // this attempt finds an empty prefix
	cb := &capturingBackend{}
	orch.CAS = fc
	orch.Lease = &noopBackend{}
	orch.PublishPaths = map[string]lease.Backend{StagedPublishPath: cb, "ingest": cb}

	if err := runStaged(t, orch, stagedJob(t, orch, "staging/host7/job-1")); err != nil {
		t.Fatalf("a re-run whose objects are already in the store must succeed: %v", err)
	}
	if req := cb.only(t); req.NewRootHashSuffixed != stagedCatalog {
		t.Errorf("NewRootHashSuffixed = %q, want %q", req.NewRootHashSuffixed, stagedCatalog)
	}
}

// A promotion error must fail the job rather than graft against a partly
// populated CAS.
func TestRun_StagedPublishFailsWhenPromotionFails(t *testing.T) {
	_, _, orch := newTestServer(t)
	fc := newFakeCAS()
	fc.err = errors.New("AccessDenied")
	cb := &capturingBackend{}
	orch.CAS = fc
	orch.Lease = &noopBackend{}
	orch.PublishPaths = map[string]lease.Backend{StagedPublishPath: cb, "ingest": cb}

	err := runStaged(t, orch, stagedJob(t, orch, "staging/host7/job-1"))
	if err == nil {
		t.Fatal("a failed promotion must fail the job")
	}
	if !strings.Contains(err.Error(), "AccessDenied") {
		t.Errorf("error should carry the cause, got: %v", err)
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if len(cb.committed) != 0 {
		t.Error("no commit may be issued after a failed promotion")
	}
}

// A CAS that cannot promote is a misconfiguration, and it has to say so. The
// alternative is a nil-ish failure much deeper in, or worse, a silent skip that
// grafts against objects nobody moved.
func TestRun_StagedPublishRefusesANonPromotingCAS(t *testing.T) {
	_, _, orch := newTestServer(t)
	cb := &capturingBackend{}
	orch.CAS = &plainCAS{}
	orch.Lease = &noopBackend{}
	orch.PublishPaths = map[string]lease.Backend{StagedPublishPath: cb, "ingest": cb}

	err := runStaged(t, orch, stagedJob(t, orch, "staging/host7/job-1"))
	if err == nil {
		t.Fatal("a CAS that cannot promote must fail the job")
	}
	if !strings.Contains(err.Error(), "promote a staging prefix") {
		t.Errorf("error should name the missing capability, got: %v", err)
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if len(cb.committed) != 0 {
		t.Error("no commit may be issued when the CAS cannot promote")
	}
}

// The ordinary ingest path must be untouched: no promotion, no graft. This is
// the control for everyone NOT using the feature, which is everyone today.
func TestRun_UnstagedJobNeitherPromotesNorGrafts(t *testing.T) {
	_, sp, orch := newTestServer(t)
	fc := newFakeCAS()
	cb := &capturingBackend{}
	orch.CAS = fc
	orch.Lease = &noopBackend{}
	orch.PublishPaths = map[string]lease.Backend{StagedPublishPath: cb, "ingest": cb}

	j := job.NewJob("job-plain", "software.cern.ch", "", "")
	j.Path = "x86_64-el9/pkg/1.0"
	j.PublishPath = "ingest"
	if err := sp.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	_ = runStaged(t, orch, j) // may fail later for want of a tar; irrelevant here

	if n := fc.count(); n != 0 {
		t.Errorf("PromoteFrom called %d times for a job with no staging prefix", n)
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	for _, req := range cb.committed {
		if req.DirectGraft {
			t.Error("an ordinary ingest job must not be grafted")
		}
	}
}
