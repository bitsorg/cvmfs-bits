// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/lease"
)

// In local mode nothing accumulates: a build id (inferred or with an explicit
// coarse=true) yields a per-package job that keeps its retries and declares no
// build that could wait forever for a finalize.
func TestLocalMode_BuildIDIsNotCoarse(t *testing.T) {
	for name, extra := range map[string]map[string]string{
		"inferred":        {},
		"explicit coarse": {"coarse": "true"},
	} {
		t.Run(name, func(t *testing.T) {
			srv, sp, orch := newTestServer(t)
			orch.Lease = &noopBackend{} // NeedsPipeline() == false: local mode
			fields := map[string]string{"repo": "software.cern.ch", "path": "x86_64/pkg/1.0",
				"build_id": "pipeline-9", "build_expect": "2"}
			for k, v := range extra {
				fields[k] = v
			}
			rec := httptest.NewRecorder()
			srv.submitJob(rec, newMultipartRequest(t, fields, []byte("dummy")))
			if rec.Code != http.StatusAccepted {
				t.Fatalf("want 202, got %d: %s", rec.Code, rec.Body.String())
			}
			var resp struct {
				JobID string `json:"job_id"`
			}
			_ = json.Unmarshal(rec.Body.Bytes(), &resp)
			j := waitTerminal(t, sp, resp.JobID)
			if j.Coarse == nil || *j.Coarse || j.State != job.StatePublished || j.BuildID != "pipeline-9" {
				t.Errorf("coarse=%v state=%s build_id=%q; want a published per-package job carrying the build id",
					j.Coarse, j.State, j.BuildID)
			}
			if _, err := os.Stat(filepath.Join(sp.Root, "builds", "pipeline-9")); !os.IsNotExist(err) {
				t.Errorf("a coarse build was declared in local mode (err=%v)", err)
			}
		})
	}
}

// A job recorded as coarse (an old manifest, or a mode change) is still
// retried when its backend cannot accumulate.
func TestLocalMode_CoarseRecordStillRetries(t *testing.T) {
	o, _ := minimalOrch(t, &noopBackend{})
	o.RetryWindow = time.Hour
	c := true
	j := &job.Job{ID: "j", BuildID: "b", Coarse: &c, State: job.StateCommitting, CreatedAt: time.Now()}
	if _, ok := o.retryAt(j, errors.New("gateway: 503 service unavailable")); !ok {
		t.Error("a local-mode job marked coarse lost its retries")
	}
}

// Seal, build status and health agree that local mode has no coarse builds.
func TestLocalMode_SealStatusAndHealth(t *testing.T) {
	for _, tc := range []struct {
		name     string
		be       lease.Backend
		sealCode int
		perPkg   bool
		ready    bool
	}{
		{"local", &noopBackend{}, http.StatusOK, true, false},
		{"gateway", &pipelineBackend{}, http.StatusAccepted, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, _, orch := newTestServer(t)
			orch.Lease = tc.be
			orch.IngestConfigPrefix = "/etc/cvmfs-prepub/ingest"

			rec := httptest.NewRecorder()
			srv.sealBuild(rec, sealRequest("b1", `{"expect":2}`))
			if rec.Code != tc.sealCode {
				t.Errorf("seal: got %d, want %d (%s)", rec.Code, tc.sealCode, rec.Body.String())
			}
			var sealed struct {
				Expect     int  `json:"expect"`
				PerPackage bool `json:"per_package"`
			}
			_ = json.Unmarshal(rec.Body.Bytes(), &sealed)
			if sealed.PerPackage != tc.perPkg {
				t.Errorf("seal per_package = %v, want %v", sealed.PerPackage, tc.perPkg)
			}
			if tc.perPkg && sealed.Expect != 0 {
				t.Errorf("a local-mode seal recorded expect=%d; it must be a no-op", sealed.Expect)
			}

			rec = httptest.NewRecorder()
			srv.buildStatus(rec, sealRequest("b1", ""))
			var st struct {
				PerPackage bool `json:"per_package"`
			}
			_ = json.Unmarshal(rec.Body.Bytes(), &st)
			if st.PerPackage != tc.perPkg {
				t.Errorf("build status per_package = %v, want %v", st.PerPackage, tc.perPkg)
			}

			rec = httptest.NewRecorder()
			srv.health(rec, httptest.NewRequest("GET", "/api/v1/health", nil))
			var h struct {
				FinalizeReady bool `json:"finalize_ready"`
			}
			_ = json.Unmarshal(rec.Body.Bytes(), &h)
			if h.FinalizeReady != tc.ready {
				t.Errorf("health finalize_ready = %v, want %v", h.FinalizeReady, tc.ready)
			}
		})
	}
}

// isCoarse must not panic when the job's backend is missing.
func TestIsCoarse_NilBackend(t *testing.T) {
	coarse := true
	if (&Orchestrator{}).isCoarse(&job.Job{BuildID: "b", Coarse: &coarse}) {
		t.Error("a job with no backend is not coarse")
	}
}
