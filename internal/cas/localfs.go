// SPDX-FileCopyrightText: 2026 CERN (European Organization for Nuclear Research)
// SPDX-License-Identifier: Apache-2.0

// Package cas defines the backend interface for content-addressable storage
// and provides a filesystem-based implementation.
package cas

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"cvmfs.io/prepub/pkg/cvmfshash"
)

// LocalFS is a filesystem-based content-addressable storage backend.
// Objects are stored in a data/XX/... directory hierarchy based on their hash.
type LocalFS struct {
	// Root is the root directory for all CAS objects.
	Root string
}

// NewLocalFS creates a new filesystem CAS, initializing the root directory with mode 0755.
// All 256 possible two-hex-digit shard directories (data/00 … data/ff) are
// pre-created so that Put never has to call os.MkdirAll on the hot path.
func NewLocalFS(root string) (*LocalFS, error) {
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, fmt.Errorf("creating CAS root: %w", err)
	}
	// Pre-create all 256 shard directories once so Put never calls MkdirAll
	// per-object.  The CAS layout is data/XX/<hash> where XX is the first two
	// hex digits of the hash.  After this loop all shards exist; MkdirAll in
	// Put degrades to a no-op stat without the allocation/syscall overhead of
	// actually creating directories.
	const hexChars = "0123456789abcdef"
	for _, hi := range hexChars {
		for _, lo := range hexChars {
			shardDir := filepath.Join(root, "data", string([]rune{hi, lo}))
			if err := os.MkdirAll(shardDir, 0755); err != nil {
				return nil, fmt.Errorf("creating CAS shard directory %s: %w", shardDir, err)
			}
		}
	}
	return &LocalFS{Root: root}, nil
}

// ExistsIsNative implements cas.NativeExistsChecker.
// LocalFS.Exists is a single os.Stat call (~1 µs on a local filesystem) so
// a Bloom-filter pre-check would add overhead rather than save CAS calls.
func (lf *LocalFS) ExistsIsNative() bool { return true }

// Exists checks if an object with the given hash exists in the store.
func (lf *LocalFS) Exists(ctx context.Context, hash string) (bool, error) {
	path := filepath.Join(lf.Root, cvmfshash.ObjectPath(hash))
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// Size returns the on-disk size in bytes of the stored object.
func (lf *LocalFS) Size(_ context.Context, hash string) (int64, error) {
	path := filepath.Join(lf.Root, cvmfshash.ObjectPath(hash))
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// smallFileSizeThreshold is the maximum object size (bytes) for which Put uses
// the fast direct-write path instead of the temp-file+rename path.
//
// At or below this threshold the entire object fits in a single write(2) call,
// which is atomic with respect to concurrent readers on Linux (POSIX §2.9.7).
// Using O_EXCL instead of a temp file eliminates the CreateTemp + Chmod +
// Rename syscalls (6–7 total) and replaces them with open + write + close
// (3 syscalls).  For archives with hundreds of thousands of small objects
// (icons, headers, scripts) this roughly halves the syscall overhead for the
// CAS upload phase.
const smallFileSizeThreshold = 4096

// Put stores an object atomically.
//
// Small objects (≤ smallFileSizeThreshold bytes): a single open(O_CREAT|O_EXCL)
// + write + close sequence.  O_EXCL makes the open fail with EEXIST when a
// concurrent goroutine has already created the file, in which case Put returns
// nil (idempotent).  A single write(2) for ≤ 4 KiB is atomic on Linux so no
// temp file is needed.
//
// Large objects: write to a per-upload temp file (avoiding the fixed-name
// concurrency hazard) then rename into the final CAS path atomically.
// io.Copy validates every byte written; os.Rename is atomic, so a concurrent
// goroutine racing on the same hash safely overwrites an identical file.
//
// No per-object fsync is issued: the overall publish transaction (CVMFS commit)
// provides the durability boundary.  A crash mid-publish leaves an orphaned
// partial publish regardless of per-object fsync discipline.
//
// Permissions: 0666 before umask, matching the standard CVMFS local uploader
// (upload_local.h: default_backend_file_mode_ = 0666) so that Apache in the
// stratum0 container can serve the object over HTTP.  With a typical container
// umask of 0022 this yields 0644; 0600 causes 403 responses and client EIO.
//
// The operation is idempotent: if the object already exists, Put returns nil.
func (lf *LocalFS) Put(ctx context.Context, hash string, r io.Reader, size int64) error {
	path := filepath.Join(lf.Root, cvmfshash.ObjectPath(hash))

	// Fast path for small objects — O_EXCL open + single write + close.
	// This avoids the temp-file dance (CreateTemp, Chmod, Rename) and cuts
	// the per-object syscall count from 6–7 down to 3.
	if size >= 0 && size <= smallFileSizeThreshold {
		data, err := io.ReadAll(r)
		if err != nil {
			return fmt.Errorf("reading small object: %w", err)
		}

		// O_EXCL: fail with EEXIST if another goroutine already wrote this hash —
		// both wrote identical bytes, so the result is correct either way.
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0666)
		if err != nil {
			if errors.Is(err, os.ErrExist) {
				return nil // already in CAS — idempotent
			}
			return fmt.Errorf("creating CAS object: %w", err)
		}
		if _, err := f.Write(data); err != nil {
			f.Close()
			os.Remove(path)
			return fmt.Errorf("writing small object: %w", err)
		}
		if err := f.Close(); err != nil {
			os.Remove(path)
			return fmt.Errorf("closing small object: %w", err)
		}
		return nil
	}

	// Slow path for large objects: temp-file + atomic rename.

	// If the object already exists skip the write (idempotent).
	if _, err := os.Stat(path); err == nil {
		return nil
	}

	// Create directory if needed.
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("creating CAS directory: %w", err)
	}

	// Using a fixed name (path + ".tmp") is a concurrency hazard: if two
	// goroutines upload the same hash simultaneously (common when concurrent jobs
	// share library files), the second os.O_TRUNC truncates the first goroutine's
	// partial write, producing a garbled CAS object that is silently renamed into
	// place.  The resulting file has the correct hash in its name but wrong
	// content, causing all CVMFS clients to receive corrupted data.
	//
	// os.CreateTemp gives each upload attempt its own uniquely named temp file.
	// Concurrent uploads of the same hash now race only on the final os.Rename,
	// which is atomic — the loser's rename overwrites an identical file safely.
	f, err := os.CreateTemp(dir, ".prepub-")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := f.Name()
	if err := f.Chmod(0666); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("setting temp file permissions: %w", err)
	}

	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("writing to temp file: %w", err)
	}

	// Close before rename — required on all platforms.
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("closing temp file: %w", err)
	}

	// Atomic rename.  Two goroutines racing on the same hash both produce
	// identical bytes, so whichever rename wins leaves the correct content in
	// place; the loser's rename is a harmless overwrite of an identical file.
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("renaming to final path: %w", err)
	}

	return nil
}

// Get retrieves an object by hash and returns a readable file handle.
// The caller must close the handle.
func (lf *LocalFS) Get(ctx context.Context, hash string) (io.ReadCloser, error) {
	path := filepath.Join(lf.Root, cvmfshash.ObjectPath(hash))
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening object: %w", err)
	}
	return f, nil
}

// Delete removes an object from the store.
func (lf *LocalFS) Delete(ctx context.Context, hash string) error {
	path := filepath.Join(lf.Root, cvmfshash.ObjectPath(hash))
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("deleting object: %w", err)
	}
	return nil
}

// List returns all object hashes currently in the store by walking the data/ directory tree.
// The walk is cancelled if ctx is done (e.g. when the dedup seed listTimeout fires).
func (lf *LocalFS) List(ctx context.Context) ([]string, error) {
	var hashes []string
	dataDir := filepath.Join(lf.Root, "data")

	err := filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		// Respect context cancellation so the dedup seed timeout (30 s) is honoured
		// even when the CAS contains millions of objects.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		// Extract hash from path: data/XX/XXXX...
		rel, err := filepath.Rel(dataDir, path)
		if err != nil {
			return err
		}
		// Remove the XX/ prefix
		if len(rel) > 3 && rel[2] == '/' {
			hash := rel[:2] + rel[3:]
			hashes = append(hashes, hash)
		}
		return nil
	})

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return hashes, nil
		}
		// Propagate context errors so callers can distinguish a deadline-truncated
		// walk (partial hashes returned) from a genuine I/O error (empty result).
		return hashes, fmt.Errorf("listing CAS: %w", err)
	}

	return hashes, nil
}
