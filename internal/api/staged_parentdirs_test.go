// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

// Parent directories for a staged publish.
//
// cvmfs_receiver grafts a subtree at the exact lease path and does not create
// the intermediate directory entries leading to it. Without them the FUSE
// client returns ENOENT traversing to content that was published successfully.
// ensureParentDirs has solved this since 36e88c2; staged jobs simply did not
// reach it, because both it and its call site sit inside the pipeline branch.
//
// Two things are asserted, and the second is the one an earlier version of this
// file claimed but never checked: the directory-only catalog is built moments
// before it is committed, so it must be UPLOADED. A staged job's own backend
// deliberately skips SubmitPayload -- right for its own content, which is
// already in the store, wrong for a catalog built here seconds ago. So the
// mkdir commit has to go through the default backend, carrying an ObjectStore.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/lease"
)

// recordingLease is the DEFAULT backend: it performs the mkdir commit.
type recordingLease struct {
	noopBackend
	mu      sync.Mutex
	commits []lease.CommitRequest
}

func (r *recordingLease) Commit(_ context.Context, req lease.CommitRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commits = append(r.commits, req)
	return nil
}
func (r *recordingLease) snapshot() []lease.CommitRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]lease.CommitRequest(nil), r.commits...)
}

// stratum0Serving answers .cvmfspublished so FetchManifestRootHash succeeds and
// the mkdir step runs to completion. A 404 makes it return ("", nil), which is
// also fine, but then nothing distinguishes "fetched" from "not configured".
//
// The root hash ADVANCES after the first read. ensureParentDirs ends with
// waitForManifestPropagation, which polls until the manifest moves past the
// root it committed against; a fixed hash makes that poll run to the context
// deadline and the test spends its whole timeout in a barrier rather than
// asserting anything.
func stratum0Serving(t *testing.T) string {
	t.Helper()
	var reads atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/.cvmfspublished") {
			fill := "f"
			if reads.Add(1) > 1 {
				fill = "e" // "the commit propagated"
			}
			// C is the root hash and N the repository name; the parser requires
			// both and ignores the rest of the manifest.
			w.Write([]byte("C" + strings.Repeat(fill, 40) + "\nNsoftware.cern.ch\n"))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

type parentDirFixture struct {
	orch    *Orchestrator
	mkdir   *recordingLease
	content *capturingBackend
	cas     *fakeCAS
}

func newParentDirFixture(t *testing.T) *parentDirFixture {
	t.Helper()
	_, _, orch := newTestServer(t)
	f := &parentDirFixture{
		orch:    orch,
		mkdir:   &recordingLease{},
		content: &capturingBackend{},
		cas:     newFakeCAS(stagedCatalog),
	}
	orch.Lease = f.mkdir
	orch.PublishPaths = map[string]lease.Backend{StagedPublishPath: f.content, "ingest": f.content}
	orch.CAS = f.cas
	orch.Stratum0URL = stratum0Serving(t)
	return f
}

func (f *parentDirFixture) run(t *testing.T, j *job.Job) error {
	t.Helper()
	if err := f.orch.Spool.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return f.orch.Run(ctx, j, nil)
}

func deepStagedJob(id string) *job.Job {
	j := job.NewJob(id, "software.cern.ch", "", "")
	j.Path = "releases/ROOT/v6-36-04/el9-x86_64" // three missing ancestors
	j.PublishPath = StagedPublishPath
	j.StagingPrefix = "staging/host7/job-1"
	j.CatalogHash = stagedCatalog
	return j
}

// A staged publish to a deep path creates its ancestors, through the default
// backend, carrying an ObjectStore so the freshly built catalog is uploaded.
//
// NEGATIVE CONTROL: restore the guard to `!o.leaseFor(j).NeedsPipeline()` with
// no StagingPrefix clause and no mkdir commit is issued — this fails on
// len(commits) == 0. Verified.
func TestRun_StagedPublishCreatesParentDirs(t *testing.T) {
	f := newParentDirFixture(t)
	if err := f.run(t, deepStagedJob("job-deep")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	commits := f.mkdir.snapshot()
	if len(commits) == 0 {
		t.Fatal("a staged publish to a deep path issued no parent-dir commit")
	}
	// The routing claim: the mkdir catalog was built moments ago and has to be
	// uploaded, which is what an ObjectStore on the request drives.
	if commits[0].ObjectStore == nil {
		t.Error("the mkdir commit carried no ObjectStore: the directory catalog " +
			"was just built and would never reach the gateway")
	}
	if n := f.cas.putCount(); n == 0 {
		t.Error("the directory catalog was never Put into the CAS")
	}
	// And the content commit is still the graft, through the staged backend.
	if req := f.content.only(t); !req.DirectGraft {
		t.Error("the content commit must still be a graft")
	}
}

// The bug this pairs with: on a node run with --gateway-direct-graft=false, a
// staged job STILL grafts, so the mkdir catalog must not pre-create the leaf as
// a plain directory. Doing so grafts a nested catalog into an existing
// directory, which fails as a merge_error and is then misreported as
// "already published" by the PathExists check.
//
// NEGATIVE CONTROL: change graftsAt back to `o.DirectGraft` alone and the leaf
// entry reappears. Verified by asserting the built catalog's entries below.
func TestGraftsAt_StagedAlwaysGraftsRegardlessOfNodeSetting(t *testing.T) {
	_, _, orch := newTestServer(t)

	staged := &job.Job{StagingPrefix: "staging/host7/job-1"}
	plain := &job.Job{}

	for _, tc := range []struct {
		name        string
		directGraft bool
		j           *job.Job
		want        bool
	}{
		{"staged job, node default on", true, staged, true},
		{"staged job, node default OFF", false, staged, true},
		{"ordinary job follows the node", true, plain, true},
		{"ordinary job follows the node", false, plain, false},
		{"nil job follows the node", false, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orch.DirectGraft = tc.directGraft
			if got := orch.graftsAt(tc.j); got != tc.want {
				t.Errorf("graftsAt = %v, want %v", got, tc.want)
			}
		})
	}
}

// A staged publish at a top-level path has no ancestors to create.
func TestRun_StagedPublishAtTopLevelSkipsParentDirs(t *testing.T) {
	f := newParentDirFixture(t)
	j := deepStagedJob("job-top")
	j.Path = "toplevel" // one component: no intermediate dirs

	if err := f.run(t, j); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := len(f.mkdir.snapshot()); n != 0 {
		t.Errorf("a top-level staged publish issued %d parent-dir commits, want 0", n)
	}
}

// The control for everyone else. Asserting only "no mkdir commit" would pass
// even if the guard wrongly admitted ingest jobs and they failed earlier, so
// this asserts the job SUCCEEDS and touched neither the mkdir backend nor the
// CAS — the ingest backend creates ancestors itself via cvmfs_server.
func TestRun_UnstagedJobUnaffectedByStagedParentDirs(t *testing.T) {
	f := newParentDirFixture(t)

	j := job.NewJob("job-plain", "software.cern.ch", "", "")
	j.Path = "releases/ROOT/v6-36-04/el9-x86_64"
	j.PublishPath = "ingest"

	if err := f.run(t, j); err != nil {
		t.Fatalf("an ordinary ingest job must be unaffected, got: %v", err)
	}
	if n := len(f.mkdir.snapshot()); n != 0 {
		t.Errorf("an ingest job issued %d parent-dir commits, want 0", n)
	}
	if n := f.cas.putCount(); n != 0 {
		t.Errorf("an ingest job issued %d CAS Puts, want 0 — it never entered mkdir-p", n)
	}
}
