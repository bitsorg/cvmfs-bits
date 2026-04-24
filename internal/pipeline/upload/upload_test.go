package upload

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// ── Fix #7: per-object retry ──────────────────────────────────────────────────

// flakyBackend succeeds after failCount failures.
type flakyBackend struct {
	calls     atomic.Int32
	failCount int32
}

func (f *flakyBackend) Put(ctx context.Context, hash string, r io.Reader, size int64) error {
	n := f.calls.Add(1)
	if n <= f.failCount {
		return fmt.Errorf("transient error #%d", n)
	}
	return nil
}

func (f *flakyBackend) Get(ctx context.Context, hash string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(nil)), nil
}

func (f *flakyBackend) Exists(ctx context.Context, hash string) (bool, error) { return true, nil }

func (f *flakyBackend) Delete(ctx context.Context, hash string) error { return nil }

func (f *flakyBackend) List(ctx context.Context) ([]string, error) { return nil, nil }

// TestUploadWithRetry_SucceedsAfterTransientFailure verifies that a transient
// CAS error is retried and the upload eventually succeeds (Fix #7).
func TestUploadWithRetry_SucceedsAfterTransientFailure(t *testing.T) {
	// Fail 2 times, succeed on attempt 3 — within maxUploadAttempts.
	backend := &flakyBackend{failCount: 2}

	err := PutWithRetry(context.Background(), backend, "hash1", []byte("data"), 4)
	if err != nil {
		t.Fatalf("expected success after retries, got: %v", err)
	}
	if calls := backend.calls.Load(); calls != 3 {
		t.Errorf("expected 3 calls (2 failures + 1 success), got %d", calls)
	}
}

// TestUploadWithRetry_FailsAfterMaxAttempts verifies that a persistently
// failing backend is abandoned after maxUploadAttempts tries.
func TestUploadWithRetry_FailsAfterMaxAttempts(t *testing.T) {
	// Always fail.
	backend := &flakyBackend{failCount: 100}

	err := PutWithRetry(context.Background(), backend, "hash2", []byte("x"), 1)
	if err == nil {
		t.Fatal("expected error after exhausting retries, got nil")
	}
	if calls := backend.calls.Load(); calls != int32(maxUploadAttempts) {
		t.Errorf("expected %d calls, got %d", maxUploadAttempts, calls)
	}
}

// TestUploadWithRetry_ContextCancellation verifies that a cancelled context
// stops retries immediately without exhausting all attempts.
func TestUploadWithRetry_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	backend := &flakyBackend{failCount: 100}
	err := PutWithRetry(ctx, backend, "hash3", []byte("y"), 1)
	if err == nil {
		t.Fatal("expected error on cancelled context, got nil")
	}
	// Should have bailed on the first attempt without retrying.
	if calls := backend.calls.Load(); calls > 1 {
		t.Errorf("expected at most 1 call on cancelled ctx, got %d", calls)
	}
}

func TestRecord_CreatesFileWithRestrictedPermissions(t *testing.T) {
	dir := t.TempDir()
	log := OpenUploadLog(filepath.Join(dir, "upload.log"))

	if err := log.Record("aabbcc"); err != nil {
		t.Fatalf("Record: %v", err)
	}

	info, err := os.Stat(filepath.Join(dir, "upload.log"))
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	// File must not be readable by group or others.
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("expected mode 0600, got %04o", perm)
	}
}

func TestReadHashes_Empty(t *testing.T) {
	dir := t.TempDir()
	log := OpenUploadLog(filepath.Join(dir, "upload.log"))

	hashes, err := log.ReadHashes()
	if err != nil {
		t.Fatalf("ReadHashes on missing file: %v", err)
	}
	if len(hashes) != 0 {
		t.Errorf("expected empty slice, got %v", hashes)
	}
}

func TestReadHashes_RecordsRoundtrip(t *testing.T) {
	dir := t.TempDir()
	log := OpenUploadLog(filepath.Join(dir, "upload.log"))

	want := []string{"hash1", "hash2", "hash3"}
	for _, h := range want {
		if err := log.Record(h); err != nil {
			t.Fatalf("Record(%q): %v", h, err)
		}
	}

	got, err := log.ReadHashes()
	if err != nil {
		t.Fatalf("ReadHashes: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d hashes, got %d: %v", len(want), len(got), got)
	}
	for i, h := range want {
		if got[i] != h {
			t.Errorf("index %d: expected %q, got %q", i, h, got[i])
		}
	}
}

func TestReadHashes_Deduplicates(t *testing.T) {
	dir := t.TempDir()
	log := OpenUploadLog(filepath.Join(dir, "upload.log"))

	// Record the same hash three times (e.g. from concurrent workers).
	for i := 0; i < 3; i++ {
		if err := log.Record("deadbeef"); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}
	if err := log.Record("cafebabe"); err != nil {
		t.Fatalf("Record: %v", err)
	}

	got, err := log.ReadHashes()
	if err != nil {
		t.Fatalf("ReadHashes: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 deduplicated hashes, got %d: %v", len(got), got)
	}
	if got[0] != "deadbeef" || got[1] != "cafebabe" {
		t.Errorf("unexpected hashes: %v", got)
	}
}

func TestContains_PresentAndAbsent(t *testing.T) {
	dir := t.TempDir()
	log := OpenUploadLog(filepath.Join(dir, "upload.log"))

	if err := log.Record("present"); err != nil {
		t.Fatalf("Record: %v", err)
	}

	ok, err := log.Contains("present")
	if err != nil {
		t.Fatalf("Contains: %v", err)
	}
	if !ok {
		t.Error("expected Contains to return true for recorded hash")
	}

	ok, err = log.Contains("absent")
	if err != nil {
		t.Fatalf("Contains: %v", err)
	}
	if ok {
		t.Error("expected Contains to return false for unrecorded hash")
	}
}

// TestContains_NoFalsePositiveOnSubstring verifies that searching for a hash
// that is a prefix of a recorded hash does NOT return true.
func TestContains_NoFalsePositiveOnSubstring(t *testing.T) {
	dir := t.TempDir()
	log := OpenUploadLog(dir + "/upload.log")

	// Record a long hash and search for its prefix.
	full := "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	prefix := full[:32]

	if err := log.Record(full); err != nil {
		t.Fatalf("Record: %v", err)
	}

	ok, err := log.Contains(prefix)
	if err != nil {
		t.Fatalf("Contains: %v", err)
	}
	if ok {
		t.Errorf("Contains(%q) returned true — prefix of a recorded hash must not match", prefix)
	}

	// Full hash must still be found.
	ok, err = log.Contains(full)
	if err != nil {
		t.Fatalf("Contains(full): %v", err)
	}
	if !ok {
		t.Error("Contains(full) returned false — exact hash must be found")
	}
}

func TestContains_MissingFile(t *testing.T) {
	dir := t.TempDir()
	log := OpenUploadLog(filepath.Join(dir, "does-not-exist.log"))

	ok, err := log.Contains("anything")
	if err != nil {
		t.Fatalf("Contains on missing file: %v", err)
	}
	if ok {
		t.Error("expected false for missing log file")
	}
}
