// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package puller

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"cvmfs.io/prepub/internal/distribute/manifest"
)

// PullStream brings the local CAS up to a streamed NDJSON manifest (ADR-0001 D4 /
// P4 catch-up). Unlike Pull it never materialises the object set: it decodes the
// header, then for every object record checks the local store and dispatches the
// missing ones to a bounded worker pool as they arrive — so memory stays flat no
// matter how large the catch-up diff is.
//
// It deliberately does NOT advance the synced root: the caller (Coordinator.
// Catchup) must first confirm the stream completed (the X-Catchup-Complete
// trailer), since a truncated stream looks like a clean EOF at this layer. The
// decoded header is returned so the caller knows the target root to record.
func (p *Puller) PullStream(ctx context.Context, r io.Reader) (Result, *manifest.Manifest, error) {
	if p.Store == nil || p.Fetcher == nil {
		return Result{}, nil, fmt.Errorf("puller: Store and Fetcher are required")
	}

	slots := p.Slots
	if slots <= 0 {
		slots = 4
	}
	sem := make(chan struct{}, slots)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var res Result
	var bases []string

	onHeader := func(m *manifest.Manifest) error {
		if err := m.Validate(); err != nil {
			return err
		}
		bases = m.BaseURLs
		return nil
	}
	onObj := func(o manifest.ObjRef) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !isHexName(o.Hash) { // path-safety: hash flows into the object URL
			mu.Lock()
			res.Total++
			res.Failed++
			res.Errors = append(res.Errors, fmt.Errorf("object %q: invalid hash", o.Hash))
			mu.Unlock()
			return nil
		}
		mu.Lock()
		res.Total++
		mu.Unlock()
		if ok, err := p.Store.Exists(ctx, o.Hash); err == nil && ok {
			mu.Lock()
			res.Skipped++
			mu.Unlock()
			return nil
		}
		wg.Add(1)
		select {
		case sem <- struct{}{}: // bounded in-flight; blocks the decode loop (backpressure)
		case <-ctx.Done():
			wg.Done()
			return ctx.Err()
		}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			err := p.fetchOne(ctx, bases, o)
			mu.Lock()
			if err != nil {
				res.Failed++
				res.Errors = append(res.Errors, err)
			} else {
				res.Fetched++
			}
			mu.Unlock()
		}()
		return nil
	}

	hdr, decErr := manifest.DecodeNDJSONStream(r, onHeader, onObj)
	wg.Wait() // always drain in-flight workers before returning
	if decErr != nil {
		return res, hdr, decErr
	}
	if err := ctx.Err(); err != nil {
		return res, hdr, err
	}
	if res.Failed > 0 {
		return res, hdr, fmt.Errorf("puller: %d objects failed during catch-up", res.Failed)
	}
	return res, hdr, nil
}

// Catchup fetches the cumulative catch-up manifest for repo (from the receiver's
// persisted last-synced root up to targetRoot) and pulls every missing object
// (ADR-0001 D4). It advances the synced root ONLY when the diff stream both
// completed (X-Catchup-Complete trailer) and every object was installed — so an
// interrupted catch-up is safely retried from the same baseline next time.
func (c *Coordinator) Catchup(ctx context.Context, repo, targetRoot string) (Result, error) {
	if c.Puller == nil {
		return Result{}, fmt.Errorf("coordinator: Puller is required")
	}
	if repo == "" || targetRoot == "" {
		return Result{}, fmt.Errorf("coordinator: repo and targetRoot are required")
	}
	base := c.CatchupBase
	if base == "" {
		base = c.ManifestBase
	}
	if base == "" {
		return Result{}, fmt.Errorf("coordinator: CatchupBase/ManifestBase is required")
	}

	var from string
	if c.Puller.State != nil {
		v, err := c.Puller.State.Get(repo)
		if err != nil {
			return Result{}, fmt.Errorf("coordinator: read synced root: %w", err)
		}
		from = v
	}

	q := url.Values{}
	q.Set("repo", repo)
	q.Set("to", targetRoot)
	if from != "" {
		q.Set("from", from)
	}
	u := strings.TrimRight(base, "/") + "/s1/catchup?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return Result{}, err
	}
	if c.TokenSource != nil {
		tok, terr := c.TokenSource(ctx)
		if terr != nil {
			return Result{}, fmt.Errorf("coordinator: obtain catch-up token: %w", terr)
		}
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return Result{}, fmt.Errorf("coordinator: catch-up GET: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Result{}, fmt.Errorf("coordinator: catch-up %s: status %d", u, resp.StatusCode)
	}

	res, hdr, err := c.Puller.PullStream(ctx, resp.Body)
	if err != nil {
		return res, fmt.Errorf("coordinator: catch-up pull: %w", err)
	}
	// Trailer is only populated after the body is fully read, which PullStream did.
	if resp.Trailer.Get("X-Catchup-Complete") != "1" {
		return res, fmt.Errorf("coordinator: catch-up stream incomplete (server did not signal completion)")
	}
	// Stream complete and every object installed → safe to advance the baseline.
	if c.Puller.State != nil {
		root := targetRoot
		if hdr != nil && hdr.TargetRootHash != "" {
			root = hdr.TargetRootHash
		}
		if err := c.Puller.State.Set(repo, root); err != nil {
			return res, fmt.Errorf("coordinator: record synced root: %w", err)
		}
	}
	return res, nil
}

// isHexName reports whether s is a safe CVMFS object name (alphanumeric, ≥3
// chars) so it cannot encode path traversal when turned into an object URL.
func isHexName(s string) bool {
	if len(s) < 3 {
		return false
	}
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		default:
			return false
		}
	}
	return true
}
