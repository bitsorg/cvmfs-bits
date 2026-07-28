// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

package api

// Tests for the seal / auto-finalize control surface.
//
// These exercise the decision logic only — none of them reach ingestsql, which
// requires a configured gateway.  The behaviours pinned here are the ones whose
// failure modes are silent: a build that never finalizes, a build that
// finalizes a subset, and a seal that shrinks a build.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cvmfs.io/prepub/internal/buildset"

	"github.com/gorilla/mux"
)

// sealRequest builds a POST /api/v1/builds/{id}/seal with mux vars populated,
// since the handlers are called directly rather than through the router.
func sealRequest(buildID, body string) *http.Request {
	req := httptest.NewRequest("POST", "/api/v1/builds/"+buildID+"/seal", strings.NewReader(body))
	return mux.SetURLVars(req, map[string]string{"id": buildID})
}

func TestSealBuild_RejectsNonPositive(t *testing.T) {
	srv, _, _ := newTestServer(t)

	for _, body := range []string{`{"expect":0}`, `{"expect":-3}`, `{}`} {
		rec := httptest.NewRecorder()
		srv.sealBuild(rec, sealRequest("b1", body))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("body %s: want 400, got %d (%s)", body, rec.Code, rec.Body.String())
		}
	}
}

// TestSealBuild_CannotShrinkBuild is the important one: sealing below what has
// already finished would finalize a subset and then remove the accumulator, so
// members still in flight would be dropped without trace.
func TestSealBuild_CannotShrinkBuild(t *testing.T) {
	srv, sp, _ := newTestServer(t)

	for _, id := range []string{"j1", "j2", "j3"} {
		if err := buildset.Record(sp.Root, "b1", buildset.Member{
			JobID: id, Repo: "software.cern.ch", Path: "p/" + id,
		}); err != nil {
			t.Fatalf("Record %s: %v", id, err)
		}
	}

	rec := httptest.NewRecorder()
	srv.sealBuild(rec, sealRequest("b1", `{"expect":2}`))
	if rec.Code != http.StatusConflict {
		t.Fatalf("want 409, got %d: %s", rec.Code, rec.Body.String())
	}
	if got := buildset.Expect(sp.Root, "b1"); got != 0 {
		t.Errorf("rejected seal must not record an expectation, got %d", got)
	}

	// Sealing at or above the current count is accepted.
	rec = httptest.NewRecorder()
	srv.sealBuild(rec, sealRequest("b1", `{"expect":4}`))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d: %s", rec.Code, rec.Body.String())
	}
	if got := buildset.Expect(sp.Root, "b1"); got != 4 {
		t.Errorf("want expect=4, got %d", got)
	}

	// ... and a later seal may not lower it again.
	rec = httptest.NewRecorder()
	srv.sealBuild(rec, sealRequest("b1", `{"expect":3}`))
	if rec.Code != http.StatusConflict {
		t.Errorf("lowering a declared expectation: want 409, got %d", rec.Code)
	}
}

// TestMaybeAutoFinalize_RefusesWhenAJobFailed verifies that a build whose
// packages did not all succeed is NOT published, and that it is still resolved
// (claim taken, result recorded) rather than left waiting for a member that
// will never arrive.
func TestMaybeAutoFinalize_RefusesWhenAJobFailed(t *testing.T) {
	_, sp, orch := newTestServer(t)
	// Non-empty so that a finalize attempt would be possible if the failure
	// were ignored — the test would then see a different error.
	orch.IngestConfigPrefix = "/nonexistent"

	if err := buildset.Record(sp.Root, "b1", buildset.Member{
		JobID: "ok-1", Repo: "software.cern.ch", Path: "p/1",
	}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if err := buildset.MarkFailed(sp.Root, "b1", "bad-1", "pipeline_error"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	if err := buildset.SetExpect(sp.Root, "b1", 2); err != nil {
		t.Fatalf("SetExpect: %v", err)
	}

	orch.maybeAutoFinalize("b1")
	orch.finalizeWg.Wait()

	res := buildset.ReadResult(sp.Root, "b1")
	if res == nil {
		t.Fatal("no result recorded — the build would wait forever with nobody watching")
	}
	if res.Published != 0 {
		t.Errorf("nothing may be published when a job failed, got published=%d", res.Published)
	}
	if !strings.Contains(res.Error, "bad-1") {
		t.Errorf("result should name the failed job, got %q", res.Error)
	}
	// The accumulator is kept so an operator can inspect it and, if wanted,
	// force the partial publish.
	if buildset.Count(sp.Root, "b1") != 1 {
		t.Error("accumulator must be preserved for inspection")
	}
}

// TestMaybeAutoFinalize_WaitsBelowExpect verifies no premature finalize.
func TestMaybeAutoFinalize_WaitsBelowExpect(t *testing.T) {
	_, sp, orch := newTestServer(t)
	orch.IngestConfigPrefix = "/nonexistent"

	if err := buildset.Record(sp.Root, "b1", buildset.Member{
		JobID: "ok-1", Repo: "software.cern.ch", Path: "p/1",
	}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if err := buildset.SetExpect(sp.Root, "b1", 5); err != nil {
		t.Fatalf("SetExpect: %v", err)
	}

	orch.maybeAutoFinalize("b1")
	orch.finalizeWg.Wait()

	if buildset.Finalizing(sp.Root, "b1") {
		t.Error("finalize claimed while the build is still incomplete")
	}
	if buildset.ReadResult(sp.Root, "b1") != nil {
		t.Error("result recorded while the build is still incomplete")
	}
}

// TestMaybeAutoFinalize_NoDeclarationIsInert verifies that builds submitted
// without a seal keep the previous behaviour: prepub waits to be asked.
func TestMaybeAutoFinalize_NoDeclarationIsInert(t *testing.T) {
	_, sp, orch := newTestServer(t)
	orch.IngestConfigPrefix = "/nonexistent"

	if err := buildset.Record(sp.Root, "b1", buildset.Member{
		JobID: "ok-1", Repo: "software.cern.ch", Path: "p/1",
	}); err != nil {
		t.Fatalf("Record: %v", err)
	}

	orch.maybeAutoFinalize("b1")
	orch.finalizeWg.Wait()

	if buildset.Finalizing(sp.Root, "b1") {
		t.Error("an unsealed build must not auto-finalize")
	}
}

func TestBuildStatus(t *testing.T) {
	srv, sp, _ := newTestServer(t)

	if err := buildset.Record(sp.Root, "b1", buildset.Member{
		JobID: "ok-1", Repo: "software.cern.ch", Path: "p/1",
	}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if err := buildset.MarkFailed(sp.Root, "b1", "bad-1", "pipeline_error"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	if err := buildset.SetExpect(sp.Root, "b1", 2); err != nil {
		t.Fatalf("SetExpect: %v", err)
	}

	req := mux.SetURLVars(httptest.NewRequest("GET", "/api/v1/builds/b1", nil),
		map[string]string{"id": "b1"})
	rec := httptest.NewRecorder()
	srv.buildStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	var st buildset.Status
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatalf("decode: %v (%s)", err, rec.Body.String())
	}
	if st.Expect != 2 || st.Accumulated != 1 || len(st.Failed) != 1 {
		t.Errorf("unexpected status: %+v", st)
	}
}
