// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

// Tests for the streamed multipart submission path (submitJob).
//
// submitJob reads the multipart body part by part instead of calling
// ParseMultipartForm, which means:
//   - the payload is written to the spool exactly once, and
//   - form fields are only known once their part has arrived, so field order is
//     the client's choice and must not change the outcome.
//
// The tests below pin that order-independence, the payload integrity check, and
// the explicit limits that replaced ParseMultipartForm's implicit ones.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// newTarFirstRequest builds a multipart body in which the "tar" part precedes
// every form field — the opposite of newMultipartRequest's ordering.
func newTarFirstRequest(t *testing.T, fields map[string]string, tarContent []byte) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)

	fw, err := mw.CreateFormFile("tar", "payload.tar")
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := fw.Write(tarContent); err != nil {
		t.Fatalf("write tar: %v", err)
	}
	for k, v := range fields {
		if err := mw.WriteField(k, v); err != nil {
			t.Fatalf("WriteField %q: %v", k, err)
		}
	}
	mw.Close()

	req := httptest.NewRequest("POST", "/api/v1/jobs", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return req
}

// findSpooledTar locates the payload written for the single job in the spool.
func findSpooledTar(t *testing.T, spoolRoot string) string {
	t.Helper()
	var found string
	err := filepath.WalkDir(spoolRoot, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && d.Name() == "payload.tar" {
			found = p
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk spool: %v", err)
	}
	return found
}

// TestSubmitJob_TarBeforeFields verifies that a client which sends the payload
// before the form fields is accepted, and that tar_sha256 is still verified —
// the hash cannot be computed lazily once the payload has streamed past, so the
// handler must hash unconditionally.
func TestSubmitJob_TarBeforeFields(t *testing.T) {
	srv, sp, orch := newTestServer(t)
	orch.Lease = &noopBackend{}

	content := []byte("tar-payload-sent-before-the-fields")
	sum := sha256.Sum256(content)

	req := newTarFirstRequest(t, map[string]string{
		"repo":       "software.cern.ch",
		"path":       "x86_64-el9/pkg/1.0",
		"tar_sha256": hex.EncodeToString(sum[:]),
	}, content)

	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d: %s", rec.Code, rec.Body.String())
	}

	tarPath := findSpooledTar(t, sp.Root)
	if tarPath == "" {
		t.Fatal("payload.tar not found in spool")
	}
	got, err := os.ReadFile(tarPath)
	if err != nil {
		t.Fatalf("read spooled tar: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("spooled payload mismatch: got %q want %q", got, content)
	}
}

// TestSubmitJob_TarBeforeFields_BadSHA verifies the checksum is still enforced
// when tar_sha256 arrives after the payload, and that the rejected job leaves
// nothing behind in the spool.
func TestSubmitJob_TarBeforeFields_BadSHA(t *testing.T) {
	srv, sp, orch := newTestServer(t)
	orch.Lease = &noopBackend{}

	req := newTarFirstRequest(t, map[string]string{
		"repo":       "software.cern.ch",
		"tar_sha256": strings.Repeat("0", 64),
	}, []byte("content-that-does-not-match"))

	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if p := findSpooledTar(t, sp.Root); p != "" {
		t.Errorf("rejected submission left a payload behind: %s", p)
	}
}

// TestSubmitJob_MissingTar verifies that a non-finalize submission without a
// payload part is rejected.  ParseMultipartForm used to surface this through
// FormFile; the streamed path has to notice the absent part itself.
func TestSubmitJob_MissingTar(t *testing.T) {
	srv, _, orch := newTestServer(t)
	orch.Lease = &noopBackend{}

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("repo", "software.cern.ch")
	mw.Close()
	req := httptest.NewRequest("POST", "/api/v1/jobs", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())

	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "tar field is required") {
		t.Errorf("unexpected error body: %s", rec.Body.String())
	}
}

// TestSubmitJob_OversizedFormField verifies the explicit per-field cap that
// replaced ParseMultipartForm's implicit memory bound.
func TestSubmitJob_OversizedFormField(t *testing.T) {
	srv, _, orch := newTestServer(t)
	orch.Lease = &noopBackend{}

	req := newMultipartRequest(t, map[string]string{
		"repo": "software.cern.ch",
		"path": strings.Repeat("x", maxFormFieldSize+1),
	}, []byte("dummy"))

	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("want 413, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestSubmitJob_BadBuildExpect verifies that a malformed build_expect is
// rejected rather than silently ignored — a producer that mistypes the count
// would otherwise wait forever for a finalize that never triggers.
func TestSubmitJob_BadBuildExpect(t *testing.T) {
	srv, _, orch := newTestServer(t)
	orch.Lease = &noopBackend{}

	req := newMultipartRequest(t, map[string]string{
		"repo":         "software.cern.ch",
		"build_id":     "build-1",
		"build_expect": "not-a-number",
	}, []byte("dummy"))

	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", rec.Code, rec.Body.String())
	}
}
