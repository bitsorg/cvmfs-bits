package cas

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"cvmfs.io/prepub/pkg/cvmfshash"
)

// TestLocalFS_ObjectFileMode verifies that CAS objects written by Put are
// created with mode 0600 (owner read/write only), not world-readable 0644
// (Fix #6).
func TestLocalFS_ObjectFileMode(t *testing.T) {
	dir := t.TempDir()
	lf, err := NewLocalFS(dir)
	if err != nil {
		t.Fatalf("NewLocalFS: %v", err)
	}

	data := []byte("test content for file-mode check")
	// Compute hash so it can be used as the CAS key.
	hash, _, err := cvmfshash.HashReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("HashReader: %v", err)
	}

	if err := lf.Put(context.Background(), hash, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Walk the data directory and check every stored file has mode 0600.
	dataDir := filepath.Join(dir, "data")
	found := false
	err = filepath.Walk(dataDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		found = true
		if perm := info.Mode().Perm(); perm != 0600 {
			t.Errorf("CAS object %q has mode %04o, want 0600", filepath.Base(path), perm)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking data dir: %v", err)
	}
	if !found {
		t.Error("no CAS object files found after Put — check ObjectPath logic")
	}
}

// TestLocalFS_IdempotentPut verifies that a second Put for the same hash is a
// no-op and does not return an error.
func TestLocalFS_IdempotentPut(t *testing.T) {
	dir := t.TempDir()
	lf, err := NewLocalFS(dir)
	if err != nil {
		t.Fatalf("NewLocalFS: %v", err)
	}

	data := []byte("idempotent content")
	hash, _, _ := cvmfshash.HashReader(bytes.NewReader(data))

	ctx := context.Background()
	if err := lf.Put(ctx, hash, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	if err := lf.Put(ctx, hash, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("second Put (idempotent): %v", err)
	}
}

// TestLocalFS_GetRoundtrip verifies Put+Get returns the original bytes.
func TestLocalFS_GetRoundtrip(t *testing.T) {
	dir := t.TempDir()
	lf, err := NewLocalFS(dir)
	if err != nil {
		t.Fatalf("NewLocalFS: %v", err)
	}

	data := []byte("round-trip test data")
	hash, _, _ := cvmfshash.HashReader(bytes.NewReader(data))

	ctx := context.Background()
	if err := lf.Put(ctx, hash, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("Put: %v", err)
	}

	rc, err := lf.Get(ctx, hash)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()

	var buf bytes.Buffer
	buf.ReadFrom(rc)
	if !bytes.Equal(buf.Bytes(), data) {
		t.Errorf("Get returned %q, want %q", buf.Bytes(), data)
	}
}
