// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package distribute

import (
	"context"
	"io"

	"cvmfs.io/prepub/internal/distribute/manifest"
)

// Fetcher opens a stream of a single CAS object from a base URL. Per-object
// HTTP GET (puller.HTTPFetcher) is the implementation.
//
// A Fetcher must not assume the bytes it transfers are trustworthy: the caller
// verifies the content hash before the object is installed, so the
// data channel can safely traverse untrusted proxies.
type Fetcher interface {
	// Fetch opens a stream of obj's bytes from base. The caller verifies the
	// content hash against obj.Hash and must Close the returned stream.
	Fetch(ctx context.Context, base string, obj manifest.ObjRef) (io.ReadCloser, error)
}
