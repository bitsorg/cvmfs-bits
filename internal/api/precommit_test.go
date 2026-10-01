// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/lease"
	"cvmfs.io/prepub/internal/spool"
)

// stubExistsSet answers PathExists from a fixed set, restoring the seam after.
func stubExistsSet(t *testing.T, present ...string) {
	t.Helper()
	real := pathExistsFn
	set := map[string]bool{}
	for _, p := range present {
		set[p] = true
	}
	pathExistsFn = func(_ context.Context, _ *http.Client, _, _, p string) (bool, error) {
		return set[p], nil
	}
	t.Cleanup(func() { pathExistsFn = real })
}

// stubHash answers publishedPackageHash with a fixed hash, restoring after.
func stubHash(t *testing.T, hash string, found bool) {
	t.Helper()
	real := publishedHashFn
	publishedHashFn = func(_ context.Context, _, _, _ string) (string, bool, error) {
		return hash, found, nil
	}
	t.Cleanup(func() { publishedHashFn = real })
}

func TestPreCommitChecks(t *testing.T) {
	const pkg = "lcg/arch/Packages/ROOT/6.36-1"
	const mods = "lcg/arch/Modules/modulefiles/ROOT"
	for _, tc := range []struct {
		name         string
		path, id     string
		idHash       string
		published    string // hash in the published .meta.json ("" = none)
		present      []string
		ingest       bool
		replace      bool
		wantSkip     bool
		wantErr      bool
		wantBaseUsed bool
	}{
		{name: "identity published since submission", path: pkg, id: pkg, present: []string{pkg}, wantSkip: true},
		{name: "same build hash", path: pkg, id: pkg, idHash: "h1", published: "h1", present: []string{pkg}, wantSkip: true},
		{name: "another build's hash", path: pkg, id: pkg, idHash: "h1", published: "h2", present: []string{pkg}, wantErr: true},
		{name: "no .meta.json to compare", path: pkg, id: pkg, idHash: "h1", present: []string{pkg}, wantErr: true},
		{name: "identity not there yet", path: pkg, id: pkg},
		{name: "no identity sent", path: pkg, present: []string{pkg}},
		{name: "replace_on_conflict overwrites", path: pkg, id: pkg, present: []string{pkg}, replace: true},
		{name: "new modulefile in an existing modules dir", path: mods, id: mods + "/6.36-1",
			present: []string{mods, mods + "/.cvmfscatalog"}, ingest: true, wantBaseUsed: true},
		{name: "existing plain directory keeps -c", path: mods, present: []string{mods}, ingest: true},
		{name: "first modulefile of a package", path: mods, id: mods + "/6.36-1", ingest: true},
		{name: "existing base, not the ingest path", path: mods, present: []string{mods, mods + "/.cvmfscatalog"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var b lease.Backend = &replBackend{}
			if tc.ingest {
				b = lease.NewIngestBackend(lease.IngestOptions{}, newOrchTestObs(t))
			}
			o := replOrch(t, b, tc.replace)
			stubExistsSet(t, tc.present...)
			stubHash(t, tc.published, tc.published != "")
			j := &job.Job{ID: "j", Repo: "r.example.org", Path: tc.path, IdentityPath: tc.id, IdentityHash: tc.idHash}
			var req lease.CommitRequest

			skip, err := o.preCommitChecks(context.Background(), j, &req, o.Obs.Logger)
			if skip != tc.wantSkip || (err != nil) != tc.wantErr || req.BaseExists != tc.wantBaseUsed {
				t.Errorf("skip=%v err=%v BaseExists=%v, want %v %v %v",
					skip, err, req.BaseExists, tc.wantSkip, tc.wantErr, tc.wantBaseUsed)
			}
		})
	}
}

// End to end: a job whose identity is already published finishes as
// published without its backend ever committing, and releases its lease.
func TestRun_SkipsAlreadyPublished(t *testing.T) {
	srv, sp, orch := newTestServer(t)
	b := &replBackend{}
	orch.Lease = b
	orch.Stratum0URL = "http://stratum0.test"
	stubExistsSet(t, "x86_64-el9/pkg/1.0")
	req := newMultipartRequest(t, map[string]string{
		"repo": "software.cern.ch", "path": "x86_64-el9/pkg/1.0", "identity_path": "x86_64-el9/pkg/1.0",
	}, []byte("payload"))
	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		JobID string `json:"job_id"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	j := waitTerminal(t, sp, body.JobID)
	if j.State != job.StatePublished {
		t.Fatalf("state = %s (%s), want published", j.State, j.Error)
	}
	for _, c := range b.calls {
		if strings.HasPrefix(c, "commit:") {
			t.Errorf("backend committed (%v); the job should have skipped", b.calls)
		}
	}
	if len(b.calls) == 0 || !strings.HasPrefix(b.calls[len(b.calls)-1], "abort:") {
		t.Errorf("lease not released: calls %v", b.calls)
	}
}

// waitTerminal polls the spool until the job reaches a terminal state.
func waitTerminal(t *testing.T, sp *spool.Spool, id string) *job.Job {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if j, err := sp.FindJob(id); err == nil && job.IsTerminal(j.State) {
			return j
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s did not finish", id)
	return nil
}

func TestValidateIdentityPath(t *testing.T) {
	for _, tc := range []struct {
		path, id string
		ok       bool
	}{
		{"a/b", "", true},
		{"a/b", "a/b", true},
		{"a/b", "a/b/c", true},
		{"a/b", "a/bc", false},
		{"a/b", "a/b/../c", false},
		{"a/b", "/a/b", false},
		{"a/b", "x/y", false},
	} {
		if err := validateIdentityPath(tc.path, tc.id); (err == nil) != tc.ok {
			t.Errorf("validateIdentityPath(%q, %q) = %v, want ok=%v", tc.path, tc.id, err, tc.ok)
		}
	}
}

// identity_path is taken from the submission, and refused outside the job's path.
func TestSubmitJob_IdentityPath(t *testing.T) {
	for _, tc := range []struct {
		id   string
		want int
	}{
		{"x86_64-el9/pkg/1.0", http.StatusAccepted},
		{"x86_64-el9/other/1.0", http.StatusBadRequest},
	} {
		srv, sp, orch := newTestServer(t)
		orch.Lease = &noopBackend{}
		req := newMultipartRequest(t, map[string]string{
			"repo": "software.cern.ch", "path": "x86_64-el9/pkg/1.0", "identity_path": tc.id,
		}, []byte("payload"))
		rec := httptest.NewRecorder()
		srv.submitJob(rec, req)
		if rec.Code != tc.want {
			t.Fatalf("identity %q: want %d, got %d: %s", tc.id, tc.want, rec.Code, rec.Body.String())
		}
		if tc.want == http.StatusAccepted {
			var body struct {
				JobID string `json:"job_id"`
			}
			_ = json.Unmarshal(rec.Body.Bytes(), &body)
			// The job moves between state dirs as it runs; retry a lookup
			// that lands mid-rename.
			var j *job.Job
			var err error
			for i := 0; i < 100; i++ {
				if j, err = sp.FindJob(body.JobID); err == nil {
					break
				}
				time.Sleep(2 * time.Millisecond)
			}
			if err != nil || j.IdentityPath != tc.id {
				t.Errorf("stored identity: %+v, %v", j, err)
			}
		}
	}
}
