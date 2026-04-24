// Package cas provides the interface and filesystem implementation for
// content-addressable storage used by the CVMFS pre-publisher.
package cas

import (
	"context"
	"io"
)

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

	// Delete removes data with the given hash.
	Delete(ctx context.Context, hash string) error

	// List returns all hashes in the store.
	List(ctx context.Context) ([]string, error)
}
