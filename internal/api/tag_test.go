package api

// Tag-feature integration tests for submitJob.
//
// Strategy:
//   - Invalid tag names (containing spaces or slashes, or exceeding 255 chars)
//     return 400 before any background goroutine is spawned — these tests are
//     purely synchronous and race-free.
//   - Valid tag tests submit a real multipart request and verify (a) the HTTP
//     response is 202 Accepted, (b) the job manifest written to the spool
//     carries the correct TagName and TagDescription.  A noopBackend is wired
//     into the orchestrator so the background goroutine completes cleanly
//     without touching the network.

import (
	"bytes"
	"context"
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
	"time"

	"cvmfs.io/prepub/internal/lease"
	"cvmfs.io/prepub/internal/notify"
	"cvmfs.io/prepub/internal/spool"
	"cvmfs.io/prepub/pkg/observe"
)

// noopBackend is a lease.Backend that succeeds immediately without touching
// the network.  NeedsPipeline returns false so the orchestrator takes the local
// mode fast-path and skips all CAS and distribution logic.
type noopBackend struct{}

func (n *noopBackend) Acquire(_ context.Context, _, _ string) (string, error) {
	return "noop-token", nil
}
func (n *noopBackend) Heartbeat(_ context.Context, _ string, _ time.Duration, _ context.CancelFunc) func() {
	return func() {}
}
func (n *noopBackend) Commit(_ context.Context, _ lease.CommitRequest) error { return nil }
func (n *noopBackend) Abort(_ context.Context, _ string) error               { return nil }
func (n *noopBackend) NeedsPipeline() bool                                   { return false }
func (n *noopBackend) Probe(_ context.Context) error                         { return nil }

// newMultipartRequest builds a multipart/form-data POST to /api/v1/jobs with
// the given form fields.  tarContent is written as the "tar" file part.
func newMultipartRequest(t *testing.T, fields map[string]string, tarContent []byte) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)

	for k, v := range fields {
		if err := mw.WriteField(k, v); err != nil {
			t.Fatalf("WriteField %q: %v", k, err)
		}
	}

	fw, err := mw.CreateFormFile("tar", "payload.tar")
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := fw.Write(tarContent); err != nil {
		t.Fatalf("write tar: %v", err)
	}
	mw.Close()

	req := httptest.NewRequest("POST", "/api/v1/jobs", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return req
}

// ── Invalid tag rejection ─────────────────────────────────────────────────────

// TestSubmitJob_InvalidTagName_SpaceMultipart verifies that a tag name
// containing a space is rejected with 400 in multipart mode.
func TestSubmitJob_InvalidTagName_SpaceMultipart(t *testing.T) {
	srv, _, orch := newTestServer(t)
	orch.Lease = &noopBackend{}

	req := newMultipartRequest(t, map[string]string{
		"repo":     "software.cern.ch",
		"tag_name": "invalid tag",
	}, []byte("dummy-tar-content"))

	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestSubmitJob_InvalidTagName_SlashMultipart verifies that a tag name
// containing a forward slash is rejected with 400 in multipart mode.
func TestSubmitJob_InvalidTagName_SlashMultipart(t *testing.T) {
	srv, _, orch := newTestServer(t)
	orch.Lease = &noopBackend{}

	req := newMultipartRequest(t, map[string]string{
		"repo":     "software.cern.ch",
		"tag_name": "v1/0/0",
	}, []byte("dummy-tar-content"))

	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestSubmitJob_InvalidTagName_TooLongMultipart verifies that a tag name
// exceeding 255 chars is rejected with 400 in multipart mode.
func TestSubmitJob_InvalidTagName_TooLongMultipart(t *testing.T) {
	srv, _, orch := newTestServer(t)
	orch.Lease = &noopBackend{}

	req := newMultipartRequest(t, map[string]string{
		"repo":     "software.cern.ch",
		"tag_name": strings.Repeat("a", 256),
	}, []byte("dummy-tar-content"))

	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── Valid tag acceptance ──────────────────────────────────────────────────────

// TestSubmitJob_ValidTag_MultipartAccepted submits a multipart job with a
// well-formed tag name and verifies (a) 202 Accepted, (b) the spool manifest
// records the correct TagName and TagDescription.
func TestSubmitJob_ValidTag_MultipartAccepted(t *testing.T) {
	const (
		wantTag  = "v1.2.3"
		wantDesc = "stable release"
	)

	srv, sp, orch := newTestServer(t)
	orch.Lease = &noopBackend{}

	req := newMultipartRequest(t, map[string]string{
		"repo":            "software.cern.ch",
		"tag_name":        wantTag,
		"tag_description": wantDesc,
	}, []byte("dummy-tar-content"))

	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d: %s", rec.Code, rec.Body.String())
	}

	// Extract job_id from response body.
	var resp struct {
		JobID string `json:"job_id"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.JobID == "" {
		t.Fatal("response missing job_id")
	}

	// Wait for the background goroutine to finish so we can read the final
	// manifest without data races.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)

	j, err := sp.FindJob(resp.JobID)
	if err != nil {
		t.Fatalf("FindJob(%q): %v", resp.JobID, err)
	}
	if j.TagName != wantTag {
		t.Errorf("job.TagName = %q; want %q", j.TagName, wantTag)
	}
	if j.TagDescription != wantDesc {
		t.Errorf("job.TagDescription = %q; want %q", j.TagDescription, wantDesc)
	}
}

// TestSubmitJob_EmptyTag_MultipartAccepted verifies that omitting tag_name is
// valid and results in an empty TagName in the spool manifest.
func TestSubmitJob_EmptyTag_MultipartAccepted(t *testing.T) {
	srv, sp, orch := newTestServer(t)
	orch.Lease = &noopBackend{}

	req := newMultipartRequest(t, map[string]string{
		"repo": "software.cern.ch",
		// no tag_name / tag_description
	}, []byte("dummy-tar-content"))

	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		JobID string `json:"job_id"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)

	j, err := sp.FindJob(resp.JobID)
	if err != nil {
		t.Fatalf("FindJob(%q): %v", resp.JobID, err)
	}
	if j.TagName != "" {
		t.Errorf("job.TagName = %q; want empty string", j.TagName)
	}
}

// TestSubmitJob_TagErrorMessage_ContainsReason verifies that the 400 error
// body returned for an invalid tag name contains a human-readable reason.
// TestSubmitJob_InvalidTagName_JSON verifies that a tag name containing a
// forward slash is rejected with 400 in JSON (tar_path) mode.  Critically,
// validation must fire BEFORE moveOrLink so the staging tar is not consumed.
func TestSubmitJob_InvalidTagName_JSON(t *testing.T) {
	stagingDir := t.TempDir()
	spoolDir := t.TempDir()

	obs, shutdown, err := observe.New("test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)

	sp, err := spool.New(spoolDir, obs)
	if err != nil {
		t.Fatalf("spool.New: %v", err)
	}
	nb := notify.NewBus()
	orch := &Orchestrator{Spool: sp, Notify: nb, Obs: obs, Lease: &noopBackend{}}
	srv := New(obs, "", orch, sp, nb, spoolDir, stagingDir)

	// Write a dummy tar to the staging dir and compute its SHA-256.
	tarContent := []byte("dummy tar content for JSON tag rejection test")
	sum := sha256.Sum256(tarContent)
	tarSHA256 := hex.EncodeToString(sum[:])
	tarPath := filepath.Join(stagingDir, "payload.tar")
	if err := os.WriteFile(tarPath, tarContent, 0600); err != nil {
		t.Fatalf("write staging tar: %v", err)
	}

	reqBody, _ := json.Marshal(map[string]interface{}{
		"repo":        "software.cern.ch",
		"tar_path":    tarPath,
		"tar_sha256":  tarSHA256,
		"tag_name":    "v1/bad", // slash → invalid
	})
	req := httptest.NewRequest("POST", "/api/v1/jobs", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d: %s", rec.Code, rec.Body.String())
	}

	// Staging tar must still exist — tag validation fired before moveOrLink.
	if _, statErr := os.Stat(tarPath); os.IsNotExist(statErr) {
		t.Error("staging tar was consumed even though tag validation should have aborted early")
	}
}

func TestSubmitJob_TagErrorMessage_ContainsReason(t *testing.T) {
	srv, _, orch := newTestServer(t)
	orch.Lease = &noopBackend{}

	req := newMultipartRequest(t, map[string]string{
		"repo":     "software.cern.ch",
		"tag_name": "a b", // space → invalid
	}, []byte("dummy"))

	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
	body := rec.Body.String()
	// The error message should mention the allowlist or invalid characters.
	if !strings.Contains(body, "invalid") && !strings.Contains(body, "allowed") && !strings.Contains(body, "must not") {
		t.Errorf("error body %q does not mention the reason for rejection", body)
	}
}
