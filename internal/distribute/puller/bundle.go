// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package puller

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"cvmfs.io/prepub/internal/distribute/manifest"
)

// PullBundle brings the local CAS up to a manifest using the single-request
// bundle endpoint (ADR-0001 P-A): it computes the locally-missing set, asks the
// publisher for all of them in one POST, and installs each object from the
// streamed, self-delimiting response — one round-trip instead of one per object.
// Each object is still hash-verified, so a corrupt or short body is rejected and
// counted as failed (to be re-fetched individually). bundleURL is the publisher's
// POST /s1/bundle endpoint.
func (p *Puller) PullBundle(ctx context.Context, bundleURL string, m *manifest.Manifest) (Result, error) {
	if p.Store == nil {
		return Result{}, fmt.Errorf("puller: Store is required")
	}
	missing := m.Missing(func(h string) bool {
		ok, err := p.Store.Exists(ctx, h)
		return err == nil && ok
	})
	res := Result{Total: len(m.Objects), Skipped: len(m.Objects) - len(missing)}
	if len(missing) == 0 {
		if p.State != nil && m.TargetRootHash != "" {
			if err := p.State.Set(m.Repo, m.TargetRootHash); err != nil {
				return res, err
			}
		}
		return res, nil
	}

	hashes := make([]string, len(missing))
	for i, o := range missing {
		hashes[i] = o.Hash
	}
	body, _ := json.Marshal(struct {
		Repo   string   `json:"repo"`
		Hashes []string `json:"hashes"`
	}{Repo: m.Repo, Hashes: hashes})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, bundleURL, bytes.NewReader(body))
	if err != nil {
		return res, err
	}
	req.Header.Set("Content-Type", "application/json")
	client := http.DefaultClient
	resp, err := client.Do(req)
	if err != nil {
		return res, fmt.Errorf("puller: bundle request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return res, fmt.Errorf("puller: bundle status %d", resp.StatusCode)
	}

	br := bufio.NewReader(resp.Body)
	for {
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		header, err := br.ReadString('\n')
		if header == "" && err == io.EOF {
			break // clean end of stream
		}
		if err != nil && err != io.EOF {
			return res, fmt.Errorf("puller: bundle frame: %w", err)
		}
		hash, size, perr := parseBundleHeader(header)
		if perr != nil {
			return res, fmt.Errorf("puller: bundle header %q: %w", strings.TrimSpace(header), perr)
		}
		if size < 0 {
			// Server reported the object absent; count as failed so the caller does
			// not advance state and re-tries (per-object) later.
			res.Failed++
			res.Errors = append(res.Errors, fmt.Errorf("object %s: absent at source", hash))
			if header == "" || err == io.EOF {
				break
			}
			continue
		}
		// Read exactly size body bytes, verifying the hash as we install.
		lr := io.LimitReader(br, size)
		vr := newVerifyReader(lr, hexPrefix(hash))
		putErr := p.Store.Put(ctx, hash, vr, size)
		// Drain any unread remainder so the next frame header is correctly aligned
		// even if Put aborted early (e.g. on a hash mismatch).
		_, _ = io.Copy(io.Discard, lr)
		if putErr != nil {
			res.Failed++
			res.Errors = append(res.Errors, fmt.Errorf("object %s: %w", hash, putErr))
		} else {
			res.Fetched++
		}
		if err == io.EOF {
			break
		}
	}

	if res.Failed > 0 {
		return res, fmt.Errorf("puller: %d of %d bundled objects failed", res.Failed, len(missing))
	}
	if p.State != nil && m.TargetRootHash != "" {
		if err := p.State.Set(m.Repo, m.TargetRootHash); err != nil {
			return res, fmt.Errorf("puller: recording synced root: %w", err)
		}
	}
	return res, nil
}

// parseBundleHeader parses a "<hash> <size>" frame header line.
func parseBundleHeader(line string) (hash string, size int64, err error) {
	fields := strings.Fields(line)
	if len(fields) != 2 {
		return "", 0, fmt.Errorf("malformed frame header")
	}
	size, err = strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("bad size")
	}
	if size >= 0 && !isHexName(fields[0]) {
		return "", 0, fmt.Errorf("unsafe object name")
	}
	return fields[0], size, nil
}
