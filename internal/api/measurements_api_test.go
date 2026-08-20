// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/lease"
	"cvmfs.io/prepub/internal/measure"
	"cvmfs.io/prepub/internal/pipeline"
)

// withMeasurements gives the test server a writer and returns it.
func withMeasurements(t *testing.T, orch *Orchestrator) *measure.Writer {
	t.Helper()
	w, err := measure.NewWriter(t.TempDir())
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	orch.Measurements = w
	return w
}

func getJSON(t *testing.T, srv *Server, url string, into any) int {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.router.ServeHTTP(rec, httptest.NewRequest("GET", url, nil))
	if into != nil && rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), into); err != nil {
			t.Fatalf("decoding %s: %v (body: %s)", url, err, rec.Body.String())
		}
	}
	return rec.Code
}

// The measurements endpoint is public by design: a CI run or a person fetches
// per-build stats with no token. This pins that — and, as a negative control,
// that a protected route on the SAME server is still 401 without a token, so
// the exemption is deliberate, not a disabled auth mode.
func TestMeasurementsAPI_PublicWithoutToken(t *testing.T) {
	srv, orch := authTestServer(t, AuthBearer) // auth enforced on /api/v1/jobs
	w := withMeasurements(t, orch)
	if err := w.Append(measure.Record{
		BuildID: "b1", JobID: "j1", PublishPath: "staged", Outcome: "published", TotalS: 1,
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	var recs []measure.Record
	if code := getJSON(t, srv, "/api/v1/measurements/b1", &recs); code != http.StatusOK {
		t.Fatalf("measurements must be reachable without a token, got %d", code)
	}
	if len(recs) != 1 {
		t.Errorf("want 1 record, got %d", len(recs))
	}
	rec := httptest.NewRecorder()
	srv.router.ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/jobs", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("protected route should be 401 without a token, got %d", rec.Code)
	}
}

func TestMeasurementsAPI_ReturnsRecordsAndFilters(t *testing.T) {
	srv, _, orch := newTestServer(t)
	w := withMeasurements(t, orch)
	for _, r := range []measure.Record{
		{BuildID: "b1", JobID: "j1", PublishPath: "ingest", Outcome: "published", TotalS: 1},
		{BuildID: "b1", JobID: "j2", PublishPath: "staged", Outcome: "published", TotalS: 2},
		{BuildID: "b1", JobID: "j3", PublishPath: "ingest", Outcome: "failed", TotalS: 3},
	} {
		if err := w.Append(r); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	var all []measure.Record
	if code := getJSON(t, srv, "/api/v1/measurements/b1", &all); code != 200 {
		t.Fatalf("status %d", code)
	}
	if len(all) != 3 {
		t.Errorf("want 3 records, got %d", len(all))
	}

	var one []measure.Record
	getJSON(t, srv, "/api/v1/measurements/b1?job=j2", &one)
	if len(one) != 1 || one[0].JobID != "j2" {
		t.Errorf("job filter returned %+v", one)
	}

	var ingest []measure.Record
	getJSON(t, srv, "/api/v1/measurements/b1?path=ingest", &ingest)
	if len(ingest) != 2 {
		t.Errorf("path filter returned %d records, want 2", len(ingest))
	}
}

func TestMeasurementsAPI_SummaryAndLatest(t *testing.T) {
	srv, _, orch := newTestServer(t)
	w := withMeasurements(t, orch)
	_ = w.Append(measure.Record{BuildID: "b9", JobID: "j1", PublishPath: "ingest",
		Outcome: "published", TotalS: 1, BackendS: measure.Secs(2 * time.Second),
		Conflicted: true, Replaced: true})
	_ = w.Append(measure.Record{BuildID: "b9", JobID: "j2", PublishPath: "ingest",
		Outcome: "failed", TotalS: 1})

	var s measure.Summary
	if code := getJSON(t, srv, "/api/v1/measurements/b9?summary=1", &s); code != 200 {
		t.Fatalf("status %d", code)
	}
	if s.Jobs != 2 || s.Published != 1 || s.Failed != 1 || s.Replaced != 1 {
		t.Errorf("summary = %+v", s)
	}
	if s.Backend.Max != 2 {
		t.Errorf("backend max = %v, want 2", s.Backend.Max)
	}

	// "latest" resolves without the caller knowing the pipeline id.
	var viaLatest measure.Summary
	if code := getJSON(t, srv, "/api/v1/measurements/latest?summary=1", &viaLatest); code != 200 {
		t.Fatalf("latest: status %d", code)
	}
	if viaLatest.Jobs != 2 {
		t.Errorf("latest resolved to the wrong build: %+v", viaLatest)
	}

	var builds []string
	getJSON(t, srv, "/api/v1/measurements", &builds)
	if len(builds) != 1 || builds[0] != "b9" {
		t.Errorf("build listing = %v", builds)
	}
}

func TestMeasurementsAPI_UnknownBuildAndDisabled(t *testing.T) {
	srv, _, orch := newTestServer(t)
	withMeasurements(t, orch)
	if code := getJSON(t, srv, "/api/v1/measurements/nope", nil); code != http.StatusNotFound {
		t.Errorf("unknown build: status %d, want 404", code)
	}

	// Disabled deployment: 404 with an explanation, not a panic on a nil writer.
	srv2, _, orch2 := newTestServer(t)
	orch2.Measurements = nil
	if code := getJSON(t, srv2, "/api/v1/measurements/anything", nil); code != http.StatusNotFound {
		t.Errorf("disabled: status %d, want 404", code)
	}
}

// ── wiring: the orchestrator must actually produce records ──────────────────

// A publish that succeeds writes one record carrying the publish path and the
// backend's own duration.
//
// NEGATIVE CONTROL: remove the measFinish call from the success path and this
// fails with "want 1 record, got 0".
func TestOrchestrator_WritesARecordOnSuccess(t *testing.T) {
	backend := &mockBackend{}
	o, sp := minimalOrch(t, backend)
	// Register the ingest path so the job is not rejected before it publishes:
	// the point of this test is the record a real publish leaves behind.
	o.PublishPaths = map[string]lease.Backend{"ingest": backend}
	w := withMeasurements(t, o)

	j := newIncomingJob(t, sp)
	j.PublishPath = "ingest"
	j.BuildID = "b-success"
	if err := o.Run(context.Background(), j, nil); err != nil {
		t.Fatalf("Run: %v", err)
	}

	recs, err := w.Read("b-success")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("want 1 record, got %d", len(recs))
	}
	if recs[0].Outcome != "published" || recs[0].PublishPath != "ingest" {
		t.Errorf("record = %+v", recs[0])
	}
	if recs[0].JobID != j.ID || recs[0].TotalS <= 0 {
		t.Errorf("identity/timing wrong: %+v", recs[0])
	}
}

// A failed publish is recorded too, with the REAL cause — not the generic
// operator-facing string that replaces j.Error immediately afterwards.
//
// NEGATIVE CONTROL: move measFinish below the `j.Error = "job processing
// failed…"` assignment and the error assertion fails.
func TestOrchestrator_WritesARecordOnFailureWithTheRealCause(t *testing.T) {
	backend := &mockBackend{commitErr: errors.New("UNIQUE constraint failed: catalog.md5path_1")}
	o, sp := minimalOrch(t, backend)
	o.PublishPaths = map[string]lease.Backend{"ingest": backend}
	w := withMeasurements(t, o)

	j := newIncomingJob(t, sp)
	j.PublishPath = "ingest"
	j.BuildID = "b-fail"
	_ = o.Run(context.Background(), j, nil)

	recs, err := w.Read("b-fail")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("want 1 record, got %d", len(recs))
	}
	if recs[0].Outcome != "failed" {
		t.Errorf("outcome = %q", recs[0].Outcome)
	}
	if !strings.Contains(recs[0].Error, "UNIQUE constraint") {
		t.Errorf("record lost the real cause: %q", recs[0].Error)
	}
}

// One job must never produce two records, however it ends.
func TestOrchestrator_RecordsExactlyOncePerJob(t *testing.T) {
	backend := &mockBackend{}
	o, sp := minimalOrch(t, backend)
	w := withMeasurements(t, o)

	j := newIncomingJob(t, sp)
	j.BuildID = "b-once"
	_ = o.Run(context.Background(), j, nil)
	// A second terminal call (the shape crash recovery produces) must be a
	// no-op: the accumulator is consumed by the first.
	o.measFinish(j, "failed", errors.New("late"))

	recs, _ := w.Read("b-once")
	if len(recs) != 1 {
		t.Fatalf("want exactly 1 record, got %d: %+v", len(recs), recs)
	}
}

// Measurements disabled must not disturb a publish, and must not panic on the
// nil writer or the absent accumulator.
func TestOrchestrator_DisabledMeasurementsChangeNothing(t *testing.T) {
	backend := &mockBackend{}
	o, sp := minimalOrch(t, backend)
	o.Measurements = nil

	j := newIncomingJob(t, sp)
	if err := o.Run(context.Background(), j, nil); err != nil {
		t.Fatalf("Run with measurements disabled: %v", err)
	}
	if j.State != job.StatePublished {
		t.Errorf("state = %v, want published", j.State)
	}
}

// The coarse-publish accumulate path (BuildID set + a pipeline backend) ends
// in StateAccumulated and returns without success or abort. It is the DEFAULT
// path, and it used to record nothing while leaking one accumulator per job.
//
// NEGATIVE CONTROL: remove `defer o.measSweep(j)` from Run and this fails
// both assertions — no record, and the accumulator still in the map.
func TestOrchestrator_AccumulatePathIsRecordedAndReleased(t *testing.T) {
	backend := &mockBackend{needsPipeline: true}
	o, sp := minimalOrch(t, backend)
	// The pipeline path needs a CAS; without one Run stops at a
	// misconfiguration long before the accumulate branch.
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
	w := withMeasurements(t, o)

	j := newIncomingJob(t, sp)
	j.BuildID = "b-accum"
	coarse := true
	j.Coarse = &coarse // accumulate: what "join this build's one commit" means
	// The pipeline reads <jobdir>/payload.tar; the spool rename carries it
	// along as the job advances. An empty archive is enough — the accumulate
	// branch is about where Run returns, not about content.
	tarPath := filepath.Join(sp.JobDir(j), "payload.tar")
	if err := os.WriteFile(tarPath, make([]byte, 10240), 0o644); err != nil {
		t.Fatalf("writing payload.tar: %v", err)
	}
	j.TarPath = tarPath
	if runErr := o.Run(context.Background(), j, nil); runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}
	if j.State != job.StateAccumulated {
		t.Fatalf("job reached %v, not StateAccumulated — this test must drive the accumulate path", j.State)
	}

	recs, err := w.Read("b-accum")
	if err != nil || len(recs) != 1 {
		t.Fatalf("accumulate path left no record: %d records, err %v", len(recs), err)
	}
	if !strings.HasPrefix(recs[0].Outcome, "incomplete:") {
		t.Errorf("outcome = %q, want an incomplete:<state> marker", recs[0].Outcome)
	}
	if _, leaked := o.measAcc.Load(j.ID); leaked {
		t.Error("accumulator leaked: the sync.Map still holds this job")
	}
}

// Whatever exit Run takes, nothing may be left in the map — that leak is
// unbounded growth in a long-lived service.
func TestOrchestrator_NoAccumulatorSurvivesRun(t *testing.T) {
	for name, backend := range map[string]*mockBackend{
		"success":        {},
		"commit fails":   {commitErr: errors.New("boom")},
		"needs pipeline": {needsPipeline: true},
	} {
		t.Run(name, func(t *testing.T) {
			o, sp := minimalOrch(t, backend)
			withMeasurements(t, o)
			j := newIncomingJob(t, sp)
			j.BuildID = "b-" + strings.ReplaceAll(name, " ", "-")
			_ = o.Run(context.Background(), j, nil)

			n := 0
			o.measAcc.Range(func(_, _ any) bool { n++; return true })
			if n != 0 {
				t.Errorf("%d accumulator(s) left after Run", n)
			}
		})
	}
}
