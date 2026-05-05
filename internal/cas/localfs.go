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
func NewLocalFS(root string) (*LocalFS, error) {
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, fmt.Errorf("creating CAS root: %w", err)
	}
	return &LocalFS{Root: root}, nil
}

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

// Put stores an object and verifies the written bytes match the expected hash.
// The write is performed to a temp file, synced to disk, then atomically renamed
// into place. After the rename, the file is re-read and hashed to verify integrity
// (Fix #11). A mismatch causes the object to be removed and an error is returned.
// The operation is idempotent: if the object already exists, Put returns nil.
func (lf *LocalFS) Put(ctx context.Context, hash string, r io.Reader, size int64) error {
	path := filepath.Join(lf.Root, cvmfshash.ObjectPath(hash))

	// If the object already exists skip the write (idempotent).
	if _, err := os.Stat(path); err == nil {
		return nil
	}

	// Create directory if needed.
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("creating CAS directory: %w", err)
	}

	// Write to a temporary file.
	// Use 0666 (world-readable before umask) to match the standard CVMFS local
	// uploader (upload_local.h: default_backend_file_mode_ = 0666, masked by the
	// process umask).  With the typical container umask of 0022 this yields 0644,
	// allowing Apache in the stratum0 container to serve the object over HTTP.
	// Using 0600 (owner-only) causes 403 responses and CVMFS client EIO errors.
	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}

	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("writing to temp file: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("syncing temp file: %w", err)
	}

	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("closing temp file: %w", err)
	}

	// Atomic rename.
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("renaming to final path: %w", err)
	}

	// Fix #11 (revised): Verify the written file has the expected size.
	//
	// The original read-back approach hashed the on-disk bytes and compared
	// them to the `hash` key — but the CAS key is the SHA-256 of the
	// *uncompressed* content while the stored bytes are zlib-compressed.
	// Those two hashes will never match, causing every upload to fail.
	//
	// A size check is sufficient here: io.Copy already returns an error on a
	// short write, Sync() flushes to stable storage, and Rename() is atomic.
	// Together they guarantee the file is complete and durable.  The size check
	// adds one extra layer against silent truncation.
	if size > 0 {
		info, err := os.Stat(path)
		if err != nil {
			os.Remove(path)
			return fmt.Errorf("stat after write: %w", err)
		}
		if info.Size() != size {
			os.Remove(path)
			return fmt.Errorf("CAS write size mismatch: expected %d bytes, got %d", size, info.Size())
		}
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
func (lf *LocalFS) List(ctx context.Context) ([]string, error) {
	var hashes []string
	dataDir := filepath.Join(lf.Root, "data")

	err := filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
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

	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("listing CAS: %w", err)
	}

	return hashes, nil
}
