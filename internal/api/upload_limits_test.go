// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func limitFields() map[string]string {
	return map[string]string{"repo": "software.cern.ch", "path": "x86_64-el9/pkg/1.0"}
}

// A declared size over the limit is refused before any byte is stored, with
// a body that names the setting.
func TestSubmitJob_TooLargeRefusedUpFront(t *testing.T) {
	srv, sp, orch := newTestServer(t)
	orch.Lease = &noopBackend{}
	srv.SetUploadLimits(16, 0)
	// The form overhead allowance is added to the limit, so the payload must
	// exceed both.
	req := newMultipartRequest(t, limitFields(), make([]byte, maxFormFieldSize*maxMultipartParts+64))

	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("want 413, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "max_tar_size_gib") {
		t.Errorf("error does not name the setting: %s", rec.Body.String())
	}
	if p := findSpooledTar(t, sp.Root); p != "" {
		t.Errorf("refused upload left a payload behind: %s", p)
	}
}

// Without a declared size the streamed limit still applies.
func TestSubmitJob_TooLargeStreamed(t *testing.T) {
	srv, sp, orch := newTestServer(t)
	orch.Lease = &noopBackend{}
	srv.SetUploadLimits(16, 0)
	req := newMultipartRequest(t, limitFields(), make([]byte, 64))
	req.ContentLength = -1

	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("want 413, got %d: %s", rec.Code, rec.Body.String())
	}
	if p := findSpooledTar(t, sp.Root); p != "" {
		t.Errorf("refused upload left a payload behind: %s", p)
	}
}

// An upload that would leave the spool below its free-space floor is refused.
func TestSubmitJob_SpoolFloor(t *testing.T) {
	srv, _, orch := newTestServer(t)
	orch.Lease = &noopBackend{}
	srv.SetUploadLimits(0, 1<<62) // more than any test disk has
	req := newMultipartRequest(t, limitFields(), []byte("small"))

	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)

	if rec.Code != http.StatusInsufficientStorage {
		t.Fatalf("want 507, got %d: %s", rec.Code, rec.Body.String())
	}
}

// The limit is advertised so a producer can refuse an oversized package itself.
func TestHealth_AdvertisesMaxTarSize(t *testing.T) {
	srv, _, _ := newTestServer(t)
	srv.SetUploadLimits(32<<30, 0)
	rec := httptest.NewRecorder()
	srv.health(rec, httptest.NewRequest("GET", "/api/v1/health", nil))

	var body struct {
		MaxTarSize int64 `json:"max_tar_size"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.MaxTarSize != 32<<30 {
		t.Errorf("max_tar_size = %d, want %d", body.MaxTarSize, int64(32<<30))
	}
}
