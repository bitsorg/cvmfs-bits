package spool

import (
	"os"
	"path/filepath"
	"testing"

	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/pkg/observe"
)

// newTestSpool creates a Spool rooted at a temp directory backed by a real
// (but minimal) observe.Provider so tracing calls do not panic.
func newTestSpool(t *testing.T) *Spool {
	t.Helper()
	obs, shutdown, err := observe.New("test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)
	s, err := New(t.TempDir(), obs)
	if err != nil {
		t.Fatalf("spool.New: %v", err)
	}
	return s
}

// TestWriteManifest_RoundTrip verifies that WriteManifest produces a file that
// ReadManifest can decode back to an equivalent Job.
func TestWriteManifest_RoundTrip(t *testing.T) {
	s := newTestSpool(t)
	j := &job.Job{
		ID:    "test-job-1",
		Repo:  "testrepo.example.com",
		Path:  "/data/set1",
		State: job.StateIncoming,
	}

	if err := s.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	got, err := s.ReadManifest(s.JobDir(j))
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if got.ID != j.ID {
		t.Errorf("ID: got %q, want %q", got.ID, j.ID)
	}
	if got.Repo != j.Repo {
		t.Errorf("Repo: got %q, want %q", got.Repo, j.Repo)
	}
	if got.Path != j.Path {
		t.Errorf("Path: got %q, want %q", got.Path, j.Path)
	}
}

// TestWriteManifest_Atomic verifies that a crash between temp-file write and
// rename cannot leave a partial manifest: the temp file is always renamed to
// the final path, never left behind.
func TestWriteManifest_Atomic(t *testing.T) {
	s := newTestSpool(t)
	j := &job.Job{
		ID:    "atomic-job",
		State: job.StateIncoming,
	}

	if err := s.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	jobDir := s.JobDir(j)
	manifestPath := filepath.Join(jobDir, "manifest.json")
	tmpPath := manifestPath + ".tmp"

	// The final manifest must exist.
	if _, err := os.Stat(manifestPath); err != nil {
		t.Errorf("manifest.json missing after WriteManifest: %v", err)
	}

	// The .tmp file must be cleaned up (renamed away).
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf(".tmp file still exists after WriteManifest (err=%v)", err)
	}
}

// TestWriteManifest_FileMode verifies that the manifest is written with mode
// 0600 — readable only by the owner (Fix #12).
func TestWriteManifest_FileMode(t *testing.T) {
	s := newTestSpool(t)
	j := &job.Job{ID: "perm-job", State: job.StateIncoming}

	if err := s.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	manifestPath := filepath.Join(s.JobDir(j), "manifest.json")
	info, err := os.Stat(manifestPath)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("manifest.json has mode %04o, want 0600", perm)
	}
}

// TestWriteManifest_DirFsync verifies that WriteManifest succeeds and that the
// parent directory entry points to the final manifest — not the tmp file.
// This exercises the Fix #6 code path (fsync parent dir after rename).
//
// We cannot easily intercept the fsync syscall in a unit test, but we can at
// least verify that:
// (a) WriteManifest does not return an error when os.Open on the dir succeeds, and
// (b) the manifest is readable immediately after the call (not left as .tmp).
func TestWriteManifest_DirFsync(t *testing.T) {
	s := newTestSpool(t)
	j := &job.Job{ID: "fsync-job", State: job.StateIncoming}

	if err := s.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	// Parent directory must be openable (Fix #6 opens it for Sync).
	jobDir := s.JobDir(j)
	f, err := os.Open(jobDir)
	if err != nil {
		t.Fatalf("os.Open(jobDir): %v", err)
	}
	f.Close()

	// Manifest must be readable and decode correctly.
	got, err := s.ReadManifest(jobDir)
	if err != nil {
		t.Fatalf("ReadManifest after WriteManifest: %v", err)
	}
	if got.ID != j.ID {
		t.Errorf("ID: got %q, want %q", got.ID, j.ID)
	}
}

// TestWriteManifest_OverwriteIsAtomic verifies that a second WriteManifest call
// (with updated fields) replaces the first cleanly with no .tmp residue.
func TestWriteManifest_OverwriteIsAtomic(t *testing.T) {
	s := newTestSpool(t)
	j := &job.Job{ID: "overwrite-job", State: job.StateIncoming, NObjects: 0}

	if err := s.WriteManifest(j); err != nil {
		t.Fatalf("first WriteManifest: %v", err)
	}

	j.NObjects = 42
	if err := s.WriteManifest(j); err != nil {
		t.Fatalf("second WriteManifest: %v", err)
	}

	got, err := s.ReadManifest(s.JobDir(j))
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if got.NObjects != 42 {
		t.Errorf("NObjects: got %d, want 42", got.NObjects)
	}

	// No .tmp residue.
	tmpPath := filepath.Join(s.JobDir(j), "manifest.json.tmp")
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf(".tmp file left behind after overwrite (err=%v)", err)
	}
}

// TestFindJob_NotFound verifies that FindJob returns a wrapped os.ErrNotExist
// when no job with the given ID exists in any state directory.
func TestFindJob_NotFound(t *testing.T) {
	s := newTestSpool(t)
	_, err := s.FindJob("no-such-job")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !isNotExist(err) {
		t.Errorf("expected os.ErrNotExist-wrapped error, got: %v", err)
	}
}

// TestFindJob_Found verifies that FindJob locates a job that exists in a state
// directory.
func TestFindJob_Found(t *testing.T) {
	s := newTestSpool(t)
	j := &job.Job{ID: "find-me", Repo: "repo.example", State: job.StateIncoming}
	if err := s.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	got, err := s.FindJob("find-me")
	if err != nil {
		t.Fatalf("FindJob: %v", err)
	}
	if got.ID != "find-me" {
		t.Errorf("ID: got %q, want %q", got.ID, "find-me")
	}
}

// isNotExist is a helper that wraps errors.Is(err, os.ErrNotExist) so the test
// reads naturally without importing "errors" directly.
func isNotExist(err error) bool {
	return os.IsNotExist(err) || func() bool {
		type unwrapper interface{ Unwrap() error }
		for err != nil {
			if err == os.ErrNotExist {
				return true
			}
			u, ok := err.(unwrapper)
			if !ok {
				break
			}
			err = u.Unwrap()
		}
		return false
	}()
}
