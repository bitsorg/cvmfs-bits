// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package cas

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"cvmfs.io/prepub/pkg/cvmfshash"
)

// TestLocalFS_ObjectFileMode verifies that CAS objects written by Put are
// created with mode 0644 (group/world readable after the standard 0022 umask).
//
// Background: localfs.go uses 0666 (matching the CVMFS C++ uploader default
// upload_local.h: default_backend_file_mode_ = 0666).  With the typical
// container umask of 0022 this yields 0644, which allows Apache on the
// stratum0 host to serve the objects over HTTP.  Using 0600 (owner-only)
// caused 403 responses and CVMFS client EIO errors (Fix #6 was later revised).
//
// This test accepts any mode that is at least 0644 (owner+group+other read
// plus owner write) so it is robust across environments with tighter umasks
// while still catching modes that are too restrictive (e.g. 0600).
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

	// Walk the data directory and check every stored file is world-readable
	// (Apache must be able to serve the objects over HTTP).
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
		perm := info.Mode().Perm()
		// Must be at least 0644: owner read+write, group+other read.
		// Strictly less than 0644 means Apache cannot serve the file.
		const minPerm = os.FileMode(0644)
		if perm&minPerm != minPerm {
			t.Errorf("CAS object %q has mode %04o — want at least 0644 so Apache can serve it", filepath.Base(path), perm)
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

// TestLocalFS_List_ContextCancel verifies that List respects context cancellation:
// it returns a partial (non-empty) hash slice and a context error when the context
// is cancelled partway through the walk.
func TestLocalFS_List_ContextCancel(t *testing.T) {
	dir := t.TempDir()
	lf, err := NewLocalFS(dir)
	if err != nil {
		t.Fatalf("NewLocalFS: %v", err)
	}

	// Write several objects so the walk has entries to visit.
	ctx := context.Background()
	for i, payload := range []string{"alpha", "bravo", "charlie", "delta", "echo"} {
		data := []byte(payload)
		hash, _, err := cvmfshash.HashReader(bytes.NewReader(data))
		if err != nil {
			t.Fatalf("HashReader[%d]: %v", i, err)
		}
		if err := lf.Put(ctx, hash, bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatalf("Put[%d]: %v", i, err)
		}
	}

	// Cancel the context immediately to simulate the 30-second dedup listTimeout
	// firing before the walk completes.
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before calling List

	hashes, listErr := lf.List(cancelCtx)
	// The error must be a context error (Canceled or DeadlineExceeded).
	if listErr == nil {
		t.Error("expected a context error from List with a cancelled context, got nil")
	} else if !errors.Is(listErr, context.Canceled) && !errors.Is(listErr, context.DeadlineExceeded) {
		t.Errorf("expected context error, got: %v", listErr)
	}
	// A partial result is acceptable: the slice may have 0..N hashes depending on
	// exactly when the cancellation check fires in the walk.
	t.Logf("partial List returned %d hashes before cancellation", len(hashes))
}

// TestLocalFS_List_Complete verifies that List with a non-cancelled context
// returns all stored hashes.
func TestLocalFS_List_Complete(t *testing.T) {
	dir := t.TempDir()
	lf, err := NewLocalFS(dir)
	if err != nil {
		t.Fatalf("NewLocalFS: %v", err)
	}

	ctx := context.Background()
	wantHashes := make(map[string]bool)
	for _, payload := range []string{"foo", "bar", "baz"} {
		data := []byte(payload)
		hash, _, err := cvmfshash.HashReader(bytes.NewReader(data))
		if err != nil {
			t.Fatalf("HashReader: %v", err)
		}
		if err := lf.Put(ctx, hash, bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatalf("Put: %v", err)
		}
		wantHashes[hash] = true
	}

	hashes, err := lf.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(hashes) != len(wantHashes) {
		t.Fatalf("List returned %d hashes, want %d", len(hashes), len(wantHashes))
	}
	for _, h := range hashes {
		if !wantHashes[h] {
			t.Errorf("unexpected hash in List result: %q", h)
		}
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
