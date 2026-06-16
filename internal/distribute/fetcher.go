// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package distribute

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"

	"cvmfs.io/prepub/internal/distribute/manifest"
)

// Fetcher transfers a single CAS object from a base URL into w and returns the
// number of bytes written. It is the pluggable data-plane transport of ADR-0001
// (D5): per-object HTTP GET is the default implementation (added in P2), with
// bundled/archived transports as optional, benchmark-gated alternatives (P-A).
//
// A Fetcher must not assume the bytes it transfers are trustworthy: the caller
// verifies the content hash before the object is installed (ADR R3), so the
// data channel can safely traverse untrusted proxies.
type Fetcher interface {
	// Name is the stable registry key for this transport (e.g. "object-http").
	Name() string
	// Fetch opens a stream of obj's bytes from base. The caller verifies the
	// content hash against obj.Hash and must Close the returned stream.
	Fetch(ctx context.Context, base string, obj manifest.ObjRef) (io.ReadCloser, error)
}

var (
	fetcherMu       sync.RWMutex
	fetcherRegistry = map[string]Fetcher{}
)

// RegisterFetcher records f under its Name for later lookup. It panics on a
// duplicate name, mirroring the standard-library registration pattern.
func RegisterFetcher(f Fetcher) {
	fetcherMu.Lock()
	defer fetcherMu.Unlock()
	name := f.Name()
	if _, dup := fetcherRegistry[name]; dup {
		panic(fmt.Sprintf("distribute: duplicate Fetcher %q", name))
	}
	fetcherRegistry[name] = f
}

// GetFetcher returns the Fetcher registered under name.
func GetFetcher(name string) (Fetcher, bool) {
	fetcherMu.RLock()
	defer fetcherMu.RUnlock()
	f, ok := fetcherRegistry[name]
	return f, ok
}

// FetcherNames lists the registered Fetcher names, sorted.
func FetcherNames() []string {
	fetcherMu.RLock()
	defer fetcherMu.RUnlock()
	names := make([]string, 0, len(fetcherRegistry))
	for n := range fetcherRegistry {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
