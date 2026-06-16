// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Package puller is the Stratum-1 receiver side of pull-based distribution
// (ADR-0001 P2): on a notification it fetches a transaction manifest, computes
// the objects it is missing locally, fetches them over HTTP, verifies each
// content hash, and installs them atomically into the local CAS.
package puller

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"cvmfs.io/prepub/internal/distribute"
	"cvmfs.io/prepub/internal/distribute/manifest"
	"cvmfs.io/prepub/pkg/cvmfshash"
)

// HTTPFetcher fetches one content-addressed object per GET from the CVMFS data
// layout (ADR D5, the default transport). Objects are cacheable and the bytes
// are hash-verified by the caller, so the request may traverse forward proxies.
type HTTPFetcher struct {
	Client *http.Client
}

func (f *HTTPFetcher) Name() string { return "object-http" }

// Fetch GETs obj from base, where base is an object root ending in ".../data"
// (a Manifest.BaseURLs entry). The returned body must be closed by the caller.
func (f *HTTPFetcher) Fetch(ctx context.Context, base string, obj manifest.ObjRef) (io.ReadCloser, error) {
	// ObjectPath returns "data/xx/rest"; base already ends in the data root.
	u := strings.TrimRight(base, "/") + "/" + cvmfshash.ObjectPath(obj.Hash)[len("data/"):]
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	client := f.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("GET %s: status %d", u, resp.StatusCode)
	}
	return resp.Body, nil
}

var _ distribute.Fetcher = (*HTTPFetcher)(nil)
