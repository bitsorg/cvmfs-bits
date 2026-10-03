// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// stageTar writes a tar into the staging directory and returns its path and
// digest.
func stageTar(t *testing.T, stagingRoot string) (string, string) {
	t.Helper()
	dir := filepath.Join(stagingRoot, "drop")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	content := []byte("staged tar content")
	p := filepath.Join(dir, "pkg-1.0.tar")
	if err := os.WriteFile(p, content, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(content)
	return p, hex.EncodeToString(sum[:])
}

func tarPathSubmit(t *testing.T, srv *Server, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/api/v1/jobs", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)
	return rec
}

// A tar_path submission refused by ANY check — including the ones that run
// after the shared path/containment/publish-path validation — must leave the
// producer's file where it was. Previously it had already been moved into the
// spool, and the rejection deleted it.
func TestSubmitJob_TarPathRejectedLateKeepsFile(t *testing.T) {
	for name, override := range map[string]map[string]any{
		"malformed path":       {"path": "../escape"},
		"unavailable path":     {"publish_path": "nosuch"},
		"coarse sans build_id": {"coarse": true},
		"digest mismatch":      {"tar_sha256": strings.Repeat("0", 64)},
	} {
		t.Run(name, func(t *testing.T) {
			srv, sp, orch := newTestServer(t)
			orch.Lease = &noopBackend{}
			tarPath, sum := stageTar(t, sp.Root)
			body := map[string]any{"repo": "software.cern.ch", "path": "x86_64/pkg/1.0",
				"tar_path": tarPath, "tar_sha256": sum}
			for k, v := range override {
				body[k] = v
			}
			rec := tarPathSubmit(t, srv, body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("want 400, got %d: %s", rec.Code, rec.Body.String())
			}
			if _, err := os.Stat(tarPath); err != nil {
				t.Errorf("the producer's tar was consumed by a rejected submission: %v", err)
			}
			if left, _ := os.ReadDir(filepath.Join(sp.Root, "incoming")); len(left) != 0 {
				t.Errorf("rejected submission left a job directory: %v", left)
			}
		})
	}
}

// An accepted tar_path submission moves the file into the spool and records
// the original file name.
func TestSubmitJob_TarPathAcceptedMovesFileAndKeepsName(t *testing.T) {
	srv, sp, orch := newTestServer(t)
	orch.Lease = &noopBackend{}
	tarPath, sum := stageTar(t, sp.Root)
	rec := tarPathSubmit(t, srv, map[string]any{"repo": "software.cern.ch", "path": "x86_64/pkg/1.0",
		"tar_path": tarPath, "tar_sha256": sum})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d: %s", rec.Code, rec.Body.String())
	}
	if _, err := os.Stat(tarPath); !os.IsNotExist(err) {
		t.Errorf("accepted tar still in staging (err=%v)", err)
	}
	var resp struct {
		JobID string `json:"job_id"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if j := waitTerminal(t, sp, resp.JobID); j.TarName != "pkg-1.0.tar" {
		t.Errorf("tar_name = %q, want the original file name pkg-1.0.tar", j.TarName)
	}
}

// A multipart upload records the part's file name rather than the spool's
// internal payload.tar.
func TestSubmitJob_MultipartRecordsUploadedFileName(t *testing.T) {
	srv, sp, orch := newTestServer(t)
	orch.Lease = &noopBackend{}
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("repo", "software.cern.ch")
	_ = mw.WriteField("path", "x86_64/pkg/1.0")
	fw, _ := mw.CreateFormFile("tar", "ROOT-6.30 el9.tar")
	_, _ = fw.Write([]byte("dummy"))
	mw.Close()
	req := httptest.NewRequest("POST", "/api/v1/jobs", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		JobID string `json:"job_id"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if j := waitTerminal(t, sp, resp.JobID); j.TarName != "ROOT-6.30 el9.tar" {
		t.Errorf("tar_name = %q, want the uploaded file name", j.TarName)
	}
}

func TestSanitizeTarName(t *testing.T) {
	for in, want := range map[string]string{
		"pkg.tar":                "pkg.tar",
		"/staging/atlas/pkg.tar": "pkg.tar",
		`C:\builds\pkg.tar`:      "pkg.tar",
		"bad\x00na\x1bme\n.tar":  "badname.tar",
		"  spaced.tar ":          "spaced.tar",
		"..":                     "",
		"":                       "",
		strings.Repeat("é", 200): strings.Repeat("é", 127),
	} {
		if got := sanitizeTarName(in); got != want {
			t.Errorf("sanitizeTarName(%q) = %q, want %q", in, got, want)
		}
	}
}
