// Package cas provides the interface and filesystem implementation for
// content-addressable storage used by the CVMFS pre-publisher.
package cas

import (
	"context"
	"io"
)

// NativeExistsChecker is an optional interface that CAS backends implement
// when their Exists() call is inherently cheap — for example a single
// os.Stat for a local-disk store, or a single S3 HEAD request — so that
// adding a Bloom-filter pre-check would increase overhead rather than
// reduce it.
//
// When a Backend implements this interface the pipeline and the service
// startup suppress Bloom-filter construction and use, regardless of any
// --bloom-filter / --bloom-snapshot-dir flags, and fall back to calling
// Exists() directly for every candidate object.
type NativeExistsChecker interface {
	Backend
	// ExistsIsNative reports true when Exists() is cheap enough that a
	// Bloom-filter pre-check is counterproductive.
	ExistsIsNative() bool
}

// Backend is the interface for content-addressable storage.
// All implementations must be thread-safe.
type Backend interface {
	// Exists checks if a hash exists in the store.
	Exists(ctx context.Context, hash string) (bool, error)

	// Put stores data with the given hash.
	// Implementations may be idempotent (return nil if already present).
	Put(ctx context.Context, hash string, r io.Reader, size int64) error

	// Get retrieves data with the given hash.
	// Returns an io.ReadCloser that must be closed by the caller.
	Get(ctx context.Context, hash string) (io.ReadCloser, error)

	// Size returns the stored size in bytes of the object with the given hash.
	// Returns an error if the object does not exist.
	Size(ctx context.Context, hash string) (int64, error)

	// Delete removes data with the given hash.
	Delete(ctx context.Context, hash string) error

	// List returns all hashes in the store.
	List(ctx context.Context) ([]string, error)
}
