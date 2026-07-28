// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

// Tests for per-job publish-path selection and cache pre-warming.
//
// The invariant worth protecting: a job is published the way the producer asked
// for, or not at all. Falling back to a different path would produce a build
// that looks identical while having different dedup, pre-warming and
// commit-granularity behaviour.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/lease"
)

// altBackend is a second, distinguishable lease.Backend for resolution tests.
type altBackend struct{ noopBackend }

func TestLeaseFor_ResolvesRegisteredPath(t *testing.T) {
	_, _, orch := newTestServer(t)
	def := &noopBackend{}
	alt := &altBackend{}
	orch.Lease = def
	orch.PublishPaths = map[string]lease.Backend{"ingest": alt}

	cases := []struct {
		name string
		path string
		want lease.Backend
	}{
		{"unset uses the default", "", def},
		{"explicit default", DefaultPublishPath, def},
		{"registered alternative", "ingest", alt},
		// An unknown path must not panic on the failure path: abortJob has to be
		// able to release a lease for a job whose configuration changed under it.
		{"unknown falls back rather than panicking", "does-not-exist", def},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := orch.leaseFor(&job.Job{PublishPath: tc.path})
			if got != tc.want {
				t.Errorf("leaseFor(%q) resolved to the wrong backend", tc.path)
			}
		})
	}
	if got := orch.leaseFor(nil); got != def {
		t.Error("leaseFor(nil) must resolve to the default backend")
	}
}

func TestHasPublishPath(t *testing.T) {
	_, _, orch := newTestServer(t)
	orch.Lease = &noopBackend{}
	orch.PublishPaths = map[string]lease.Backend{"ingest": &altBackend{}}

	for _, tc := range []struct {
		path string
		want bool
	}{
		{"", true},
		{DefaultPublishPath, true},
		{"ingest", true},
		{"local", false},
		{"nonsense", false},
	} {
		if got := orch.HasPublishPath(tc.path); got != tc.want {
			t.Errorf("HasPublishPath(%q) = %v; want %v", tc.path, got, tc.want)
		}
	}

	names := strings.Join(orch.PublishPathNames(), ",")
	if names != "ingest,prepub" {
		t.Errorf("PublishPathNames() = %q; want sorted ingest,prepub", names)
	}
}

// TestHasPublishPath_NilEntryIsNotAvailable guards against a registry entry
// that was declared but never constructed.
func TestHasPublishPath_NilEntryIsNotAvailable(t *testing.T) {
	_, _, orch := newTestServer(t)
	orch.Lease = &noopBackend{}
	orch.PublishPaths = map[string]lease.Backend{"ingest": nil}

	if orch.HasPublishPath("ingest") {
		t.Error("a nil backend must not count as an available publish path")
	}
	if got := strings.Join(orch.PublishPathNames(), ","); got != "prepub" {
		t.Errorf("PublishPathNames() = %q; want prepub", got)
	}
}

func TestSubmitJob_RejectsUnavailablePublishPath(t *testing.T) {
	srv, sp, orch := newTestServer(t)
	orch.Lease = &noopBackend{}
	// No alternative paths configured — the default deployment.

	req := newMultipartRequest(t, map[string]string{
		"repo":         "software.cern.ch",
		"path":         "x86_64-el9/pkg/1.0",
		"publish_path": "ingest",
	}, []byte("dummy"))

	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "not configured") {
		t.Errorf("the error should say the path is unavailable, got %s", rec.Body.String())
	}
	if p := findSpooledTar(t, sp.Root); p != "" {
		t.Errorf("rejected submission left a payload behind: %s", p)
	}
}

func TestSubmitJob_AcceptsConfiguredPublishPath(t *testing.T) {
	srv, _, orch := newTestServer(t)
	orch.Lease = &noopBackend{}
	orch.PublishPaths = map[string]lease.Backend{"ingest": &altBackend{}}

	req := newMultipartRequest(t, map[string]string{
		"repo":         "software.cern.ch",
		"path":         "x86_64-el9/pkg/1.0",
		"publish_path": "ingest",
	}, []byte("dummy"))

	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestSubmitJob_RejectsPreWarmOnAlternativePath: the ingest path commits
// through the gateway, so there is no window in which the objects exist and the
// catalog has not yet flipped. Accepting the request and ignoring it would be
// worse than refusing it.
func TestSubmitJob_RejectsPreWarmOnAlternativePath(t *testing.T) {
	srv, _, orch := newTestServer(t)
	orch.Lease = &noopBackend{}
	orch.PublishPaths = map[string]lease.Backend{"ingest": &altBackend{}}

	req := newMultipartRequest(t, map[string]string{
		"repo":         "software.cern.ch",
		"path":         "x86_64-el9/pkg/1.0",
		"publish_path": "ingest",
		"prewarm":      "true",
	}, []byte("dummy"))

	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "pre-warm") {
		t.Errorf("unexpected error body: %s", rec.Body.String())
	}
}

// TestSubmitJob_RejectsBuildIDOnAlternativePath: an alternative path commits
// each package on arrival, so it can never accumulate into a coarse build.
// Dropping build_id silently would leave the producer sealing a build that can
// never complete.
func TestSubmitJob_RejectsBuildIDOnAlternativePath(t *testing.T) {
	srv, _, orch := newTestServer(t)
	orch.Lease = &noopBackend{}
	orch.PublishPaths = map[string]lease.Backend{"ingest": &altBackend{}}

	req := newMultipartRequest(t, map[string]string{
		"repo":         "software.cern.ch",
		"path":         "x86_64-el9/pkg/1.0",
		"publish_path": "ingest",
		"build_id":     "pipeline-1",
	}, []byte("dummy"))

	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "coarse build") {
		t.Errorf("unexpected error body: %s", rec.Body.String())
	}
}

func TestSubmitJob_RejectsMalformedPreWarm(t *testing.T) {
	srv, _, orch := newTestServer(t)
	orch.Lease = &noopBackend{}

	req := newMultipartRequest(t, map[string]string{
		"repo":    "software.cern.ch",
		"prewarm": "yes-please",
	}, []byte("dummy"))

	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestPreWarmFor covers the tri-state: a job that says nothing inherits the
// node default, and a job that does say something overrides it in both
// directions.
func TestPreWarmFor(t *testing.T) {
	_, _, orch := newTestServer(t)
	yes, no := true, false

	for _, tc := range []struct {
		name        string
		nodeDefault bool
		job         *bool
		want        bool
	}{
		{"unset job, node off", false, nil, false},
		{"unset job, node on", true, nil, true},
		{"job opts in over an off node", false, &yes, true},
		{"job opts out of an on node", true, &no, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orch.PreWarm = tc.nodeDefault
			if got := orch.preWarmFor(&job.Job{PreWarm: tc.job}); got != tc.want {
				t.Errorf("preWarmFor = %v; want %v", got, tc.want)
			}
		})
	}

	orch.PreWarm = true
	if !orch.preWarmFor(nil) {
		t.Error("a nil job must fall back to the node default")
	}
}

// TestRun_FailsWhenPublishPathDisappeared covers recovery of a job whose
// configured path is gone — the job must fail rather than be published a
// different way than it asked for.
func TestRun_FailsWhenPublishPathDisappeared(t *testing.T) {
	_, sp, orch := newTestServer(t)
	orch.Lease = &noopBackend{}
	orch.PublishPaths = nil // the deployment no longer offers alternatives

	j := job.NewJob("job-1", "software.cern.ch", "", "")
	j.Path = "x86_64-el9/pkg/1.0"
	j.PublishPath = "ingest"
	if err := sp.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := orch.Run(ctx, j, nil)
	if err == nil {
		t.Fatal("want an error when the job's publish path is not configured")
	}
	if !strings.Contains(err.Error(), "not configured") {
		t.Errorf("unexpected error: %v", err)
	}
}
