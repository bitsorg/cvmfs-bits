// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package puller

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"cvmfs.io/prepub/internal/distribute/manifest"
)

// defaultMaxManifestBytes bounds the manifest body the receiver will read, so a
// compromised or buggy Stratum 0 cannot OOM the receiver with a giant document.
const defaultMaxManifestBytes = 256 << 20 // 256 MiB

// Coordinator turns a transaction notification into a pull: it fetches the
// transaction manifest from Stratum 0 (the cvmfs-prepub endpoint) and runs the
// Puller. It is the receiver-side glue invoked by the control plane
// (announce/published) when the receiver runs in pull mode (ADR-0001 D1/D3).
type Coordinator struct {
	// ManifestBase is the base URL where manifests are served (the cvmfs-prepub
	// endpoint). The manifest for a transaction is at
	// ManifestBase + "/s1/{txn}/manifest". Object locations come from the
	// manifest's own BaseURLs.
	ManifestBase string
	// BundleBase is the base URL for the chunked-bundle endpoint (POST
	// {BundleBase}/s1/bundle). Empty defaults to ManifestBase.
	BundleBase string
	Client     *http.Client
	Puller     *Puller
	// MaxManifestBytes caps the manifest body read (0 = 256 MiB default).
	MaxManifestBytes int64
	// CatchupBase is the base URL for the cumulative catch-up endpoint
	// (GET /s1/catchup). Empty falls back to ManifestBase (they are the same S0
	// endpoint in the default deployment).
	CatchupBase string
	// TokenSource, when set, supplies a bearer token attached to catch-up
	// requests (data-plane auth). Satisfied by *credential.Client.Token. A nil
	// source sends no Authorization header (open deployments).
	TokenSource func(ctx context.Context) (string, error)
}

// OnTransaction fetches the manifest for txnID and pulls the objects the local
// CAS is missing. It returns the Puller Result; a non-nil error means the
// receiver is not warm for this transaction (the caller must not ack).
func (c *Coordinator) OnTransaction(ctx context.Context, txnID string) (Result, error) {
	if c.Puller == nil {
		return Result{}, fmt.Errorf("coordinator: Puller is required")
	}
	if c.ManifestBase == "" {
		return Result{}, fmt.Errorf("coordinator: ManifestBase is required")
	}
	url := strings.TrimRight(c.ManifestBase, "/") + "/s1/" + txnID + "/manifest"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Result{}, err
	}
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return Result{}, fmt.Errorf("coordinator: fetch manifest: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Result{}, fmt.Errorf("coordinator: manifest %s: status %d", url, resp.StatusCode)
	}
	maxBytes := c.MaxManifestBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxManifestBytes
	}
	m, err := manifest.Decode(io.LimitReader(resp.Body, maxBytes))
	if err != nil {
		return Result{}, fmt.Errorf("coordinator: decode manifest: %w", err)
	}
	if err := m.Validate(); err != nil {
		return Result{}, fmt.Errorf("coordinator: %w", err)
	}
	// Chunked-bundle path when the receiver is tuned for >1 file per request;
	// otherwise the per-object path.
	if c.Puller != nil && c.Puller.FilesPerRequest > 1 {
		base := c.BundleBase
		if base == "" {
			base = c.ManifestBase
		}
		bundleURL := strings.TrimRight(base, "/") + "/s1/bundle"
		return c.Puller.PullChunked(ctx, bundleURL, m)
	}
	return c.Puller.Pull(ctx, m)
}
