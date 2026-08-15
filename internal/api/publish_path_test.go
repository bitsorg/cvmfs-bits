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
	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/internal/pipeline"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

// An alternative path commits each package on arrival, so it cannot take part
// in a coarse build -- but it MUST still accept the build id, which is the CI
// pipeline identity every job of a run carries (the same one the views and the
// signed common manifest use, and the key its measurement records are filed
// under). Refusing the id forced producers to send none at all on these paths,
// which left their records unattributable.
//
// NEGATIVE CONTROL: restore the old `if buildID != ""` rejection and the first
// case fails with 400.
func TestSubmitJob_AcceptsBuildIDButRefusesCoarseOnAlternativePath(t *testing.T) {
	for name, tc := range map[string]struct {
		fields   map[string]string
		wantCode int
	}{
		"identity only is accepted": {
			fields: map[string]string{
				"repo": "software.cern.ch", "path": "x86_64-el9/pkg/1.0",
				"publish_path": "ingest", "build_id": "pipeline-1",
			},
			wantCode: http.StatusAccepted,
		},
		"an explicit coarse request is refused": {
			fields: map[string]string{
				"repo": "software.cern.ch", "path": "x86_64-el9/pkg/2.0",
				"publish_path": "ingest", "build_id": "pipeline-1", "coarse": "true",
			},
			wantCode: http.StatusBadRequest,
		},
	} {
		t.Run(name, func(t *testing.T) {
			srv, _, orch := newTestServer(t)
			orch.Lease = &noopBackend{}
			orch.PublishPaths = map[string]lease.Backend{"ingest": &altBackend{}}

			rec := httptest.NewRecorder()
			srv.submitJob(rec, newMultipartRequest(t, tc.fields, []byte("dummy")))

			if rec.Code != tc.wantCode {
				t.Fatalf("want %d, got %d: %s", tc.wantCode, rec.Code, rec.Body.String())
			}
			if tc.wantCode == http.StatusBadRequest &&
				!strings.Contains(rec.Body.String(), "coarse") {
				t.Errorf("error should name the coarse request: %s", rec.Body.String())
			}
		})
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

// The invariant that makes the build id safe to carry everywhere: on a
// per-package path it is IDENTITY ONLY. It must not declare an expectation,
// because nothing will ever accumulate against it and the build would wait
// forever for packages that already committed on arrival.
//
// NEGATIVE CONTROL: gate SetExpect on `buildID != ""` again (the old code) and
// this fails — a builds/ directory appears for the pipeline.
func TestSubmitJob_BuildIDOnAlternativePathDeclaresNoBuild(t *testing.T) {
	srv, sp, orch := newTestServer(t)
	orch.Lease = &noopBackend{}
	orch.PublishPaths = map[string]lease.Backend{"ingest": &altBackend{}}

	rec := httptest.NewRecorder()
	srv.submitJob(rec, newMultipartRequest(t, map[string]string{
		"repo": "software.cern.ch", "path": "x86_64-el9/pkg/1.0",
		"publish_path": "ingest",
		"build_id":     "pipeline-77",
		"build_expect": "170",
	}, []byte("dummy")))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d: %s", rec.Code, rec.Body.String())
	}

	if _, err := os.Stat(filepath.Join(sp.Root, "builds", "pipeline-77")); !os.IsNotExist(err) {
		t.Errorf("a coarse build was declared for a per-package publish (err=%v)", err)
	}
}

// The default path keeps its historical behaviour with no `coarse` field at
// all: a build id there still means accumulate. Old producers are unaffected.
func TestSubmitJob_DefaultPathStillInfersCoarseFromBuildID(t *testing.T) {
	srv, sp, orch := newTestServer(t)
	orch.Lease = &noopBackend{}

	rec := httptest.NewRecorder()
	srv.submitJob(rec, newMultipartRequest(t, map[string]string{
		"repo": "software.cern.ch", "path": "x86_64-el9/pkg/1.0",
		"build_id":     "pipeline-88",
		"build_expect": "3",
	}, []byte("dummy")))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d: %s", rec.Code, rec.Body.String())
	}
	if _, err := os.Stat(filepath.Join(sp.Root, "builds", "pipeline-88")); err != nil {
		t.Errorf("the default path stopped declaring a coarse build: %v", err)
	}
}

// Links the coarse DECISION to what Run does with it. The previous version of
// this test set Coarse AND BuildID and asserted only the accumulate case, so
// reverting the gate to `j.BuildID != ""` still satisfied it — a test whose
// comment claimed a control it did not have. The discriminating cases are the
// ones where the decision and the build id disagree.
//
// NEGATIVE CONTROL: revert the accumulate gate (orchestrator.go) to
// `j.BuildID != ""` and the "explicitly not coarse" case fails, because a job
// carrying a build id would accumulate against the producer's decision.
func TestSubmitToRun_CoarseDecisionDrivesAccumulation(t *testing.T) {
	yes, no := true, false
	for name, tc := range map[string]struct {
		coarse      *bool
		publishPath string
		wantState   job.State
	}{
		"explicitly coarse accumulates": {
			coarse: &yes, wantState: job.StateAccumulated,
		},
		"explicitly not coarse commits on arrival": {
			coarse: &no, wantState: job.StatePublished,
		},
		"not stated on the default path infers coarse": {
			coarse: nil, wantState: job.StateAccumulated,
		},
	} {
		t.Run(name, func(t *testing.T) {
			backend := &mockBackend{needsPipeline: true}
			o, sp := minimalOrch(t, backend)
			cs, err := cas.NewLocalFS(t.TempDir())
			if err != nil {
				t.Fatalf("cas.NewLocalFS: %v", err)
			}
			o.CAS = cs
			o.Pipeline = pipeline.Config{
				Workers: 1, UploadConc: 1, CompressLevel: 1,
				ChunkMin: 1 << 20, ChunkAvg: 1 << 22, ChunkMax: 1 << 23,
				CAS: cs, SpoolDir: t.TempDir(), Obs: o.Obs,
			}

			j := newIncomingJob(t, sp)
			j.BuildID = "pipeline-run" // identity: present in EVERY case
			j.Coarse = tc.coarse
			if tc.publishPath != "" {
				j.PublishPath = tc.publishPath
			}
			tarPath := filepath.Join(sp.JobDir(j), "payload.tar")
			if err := os.WriteFile(tarPath, make([]byte, 10240), 0o644); err != nil {
				t.Fatalf("write payload: %v", err)
			}
			j.TarPath = tarPath

			if err := o.Run(context.Background(), j, nil); err != nil {
				t.Fatalf("Run: %v", err)
			}
			if j.State != tc.wantState {
				t.Errorf("state = %v, want %v (build id present in all cases, so only "+
					"the coarse decision can distinguish them)", j.State, tc.wantState)
			}
		})
	}
}

// A job whose manifest predates the coarse field (nil) must still accumulate,
// or a build interrupted by a prepub restart strands every remaining package.
func TestIsCoarse_NilFallsBackToTheOldInference(t *testing.T) {
	for name, tc := range map[string]struct {
		j    job.Job
		want bool
	}{
		"old manifest, default path, has build id": {job.Job{BuildID: "p1"}, true},
		"old manifest, explicit prepub path":       {job.Job{BuildID: "p1", PublishPath: "prepub"}, true},
		"old manifest, ingest path":                {job.Job{BuildID: "p1", PublishPath: "ingest"}, false},
		"old manifest, no build id":                {job.Job{}, false},
		"finalize never accumulates":               {job.Job{BuildID: "p1", Finalize: true}, false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := tc.j.IsCoarse(); got != tc.want {
				t.Errorf("IsCoarse() = %v, want %v", got, tc.want)
			}
		})
	}
	// An explicit value always wins over the inference.
	no := false
	if (&job.Job{BuildID: "p1", Coarse: &no}).IsCoarse() {
		t.Error("an explicit coarse=false was overridden by the inference")
	}
}

// The duplicated constant must not drift from the one it mirrors.
func TestDefaultPublishPathNamesAgree(t *testing.T) {
	if job.DefaultPublishPathName != DefaultPublishPath {
		t.Errorf("job.DefaultPublishPathName=%q but api.DefaultPublishPath=%q",
			job.DefaultPublishPathName, DefaultPublishPath)
	}
}

// Pins the ingress assignment itself. The default path + a build id INFERS
// coarse, so an explicit coarse=false is the only input where the stored
// decision differs from the fallback — and therefore the only one that can
// catch `j.Coarse = &coarse` going missing.
//
// NEGATIVE CONTROL: delete that assignment and this fails: Coarse comes back
// nil, IsCoarse() infers true from the build id, and the job would accumulate
// into a build the producer explicitly declined.
func TestSubmitJob_ExplicitCoarseFalseIsStoredNotInferred(t *testing.T) {
	srv, sp, orch := newTestServer(t)
	orch.Lease = &noopBackend{}

	rec := httptest.NewRecorder()
	srv.submitJob(rec, newMultipartRequest(t, map[string]string{
		"repo": "software.cern.ch", "path": "x86_64-el9/pkg/1.0",
		"build_id": "pipeline-55",
		"coarse":   "false",
	}, []byte("dummy")))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d: %s", rec.Code, rec.Body.String())
	}

	var id struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &id); err != nil || id.JobID == "" {
		t.Fatalf("no job id in %s (err %v)", rec.Body.String(), err)
	}
	j, err := sp.FindJob(id.JobID)
	if err != nil {
		t.Fatalf("FindJob: %v", err)
	}
	if j.Coarse == nil {
		t.Fatal("Coarse was not stored: it is nil, so IsCoarse() would infer true from the build id")
	}
	if *j.Coarse {
		t.Error("an explicit coarse=false was stored as true")
	}
	if j.IsCoarse() {
		t.Error("IsCoarse() ignored the explicit decision")
	}
}

// A malformed boolean must be refused, not silently read as false — the same
// rule the sibling knobs on this handler follow.
func TestSubmitJob_RejectsMalformedCoarse(t *testing.T) {
	srv, _, orch := newTestServer(t)
	orch.Lease = &noopBackend{}
	rec := httptest.NewRecorder()
	srv.submitJob(rec, newMultipartRequest(t, map[string]string{
		"repo": "software.cern.ch", "path": "x86_64-el9/pkg/1.0",
		"coarse": "maybe",
	}, []byte("dummy")))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("want 400 for coarse=maybe, got %d: %s", rec.Code, rec.Body.String())
	}
}
