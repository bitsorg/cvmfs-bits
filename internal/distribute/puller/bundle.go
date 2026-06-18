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
	"sync"

	"cvmfs.io/prepub/internal/distribute/manifest"
)

// PullBundle brings the local CAS up to a manifest by asking the publisher for
// the entire locally-missing set in a single POST /s1/bundle request (ADR-0001
// P-A): one round-trip instead of one per object. Each object is still
// hash-verified. For latency-tuned transfers use PullChunked.
func (p *Puller) PullBundle(ctx context.Context, bundleURL string, m *manifest.Manifest) (Result, error) {
	if p.Store == nil {
		return Result{}, fmt.Errorf("puller: Store is required")
	}
	missing := p.missing(ctx, m)
	res := Result{Total: len(m.Objects), Skipped: len(m.Objects) - len(missing)}
	if len(missing) == 0 {
		return p.bundleDone(res, m)
	}
	f, fa, errs := p.fetchBundle(ctx, bundleURL, m.Repo, missing)
	res.Fetched += f
	res.Failed += fa
	res.Errors = append(res.Errors, errs...)
	if res.Failed > 0 {
		return res, fmt.Errorf("puller: %d of %d bundled objects failed for txn %s", res.Failed, len(missing), m.TransactionID)
	}
	return p.bundleDone(res, m)
}

// PullChunked brings the local CAS up to a manifest using latency-tuned chunked
// bundles: the locally-missing set is split into chunks of FilesPerRequest
// objects, each fetched in one POST /s1/bundle request, with up to Slots
// requests in flight. This lets a receiver trade round-trips for parallelism
// according to its RTT to Stratum 0 (high RTT → larger chunks; low RTT → smaller
// chunks with more streams). Each object is hash-verified.
func (p *Puller) PullChunked(ctx context.Context, bundleURL string, m *manifest.Manifest) (Result, error) {
	if p.Store == nil {
		return Result{}, fmt.Errorf("puller: Store is required")
	}
	missing := p.missing(ctx, m)
	res := Result{Total: len(m.Objects), Skipped: len(m.Objects) - len(missing)}
	if len(missing) == 0 {
		return p.bundleDone(res, m)
	}

	k := p.FilesPerRequest
	if k < 1 {
		k = 1
	}
	slots := p.Slots
	if slots <= 0 {
		slots = defaultSlots
	}

	sem := make(chan struct{}, slots)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < len(missing); i += k {
		if ctx.Err() != nil {
			break
		}
		end := i + k
		if end > len(missing) {
			end = len(missing)
		}
		chunk := missing[i:end]
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			f, fa, errs := p.fetchBundle(ctx, bundleURL, m.Repo, chunk)
			mu.Lock()
			res.Fetched += f
			res.Failed += fa
			res.Errors = append(res.Errors, errs...)
			mu.Unlock()
		}()
	}
	wg.Wait()

	if err := ctx.Err(); err != nil {
		return res, err
	}
	if res.Failed > 0 {
		return res, fmt.Errorf("puller: %d of %d chunked-bundle objects failed for txn %s", res.Failed, len(missing), m.TransactionID)
	}
	return p.bundleDone(res, m)
}

// missing returns the manifest objects not already present in the local CAS.
func (p *Puller) missing(ctx context.Context, m *manifest.Manifest) []manifest.ObjRef {
	return m.Missing(func(h string) bool {
		ok, err := p.Store.Exists(ctx, h)
		return err == nil && ok
	})
}

// bundleDone records the synced root on a fully successful, non-provisional pull.
func (p *Puller) bundleDone(res Result, m *manifest.Manifest) (Result, error) {
	if p.State != nil && !m.Provisional && m.TargetRootHash != "" {
		if err := p.State.Set(m.Repo, m.TargetRootHash); err != nil {
			return res, fmt.Errorf("puller: recording synced root: %w", err)
		}
	}
	return res, nil
}

// fetchBundle pulls a specific set of objects in one POST /s1/bundle request,
// installing each from the streamed, self-delimiting response (hash-verified).
func (p *Puller) fetchBundle(ctx context.Context, bundleURL, repo string, objs []manifest.ObjRef) (fetched, failed int, errs []error) {
	if len(objs) == 0 {
		return 0, 0, nil
	}
	hashes := make([]string, len(objs))
	for i, o := range objs {
		hashes[i] = o.Hash
	}
	body, _ := json.Marshal(struct {
		Repo   string   `json:"repo"`
		Hashes []string `json:"hashes"`
	}{Repo: repo, Hashes: hashes})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, bundleURL, bytes.NewReader(body))
	if err != nil {
		return 0, len(objs), []error{err}
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.bundleHTTPClient().Do(req)
	if err != nil {
		return 0, len(objs), []error{fmt.Errorf("puller: bundle request: %w", err)}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, len(objs), []error{fmt.Errorf("puller: bundle status %d", resp.StatusCode)}
	}

	br := bufio.NewReader(resp.Body)
	for {
		if ctx.Err() != nil {
			return fetched, failed, append(errs, ctx.Err())
		}
		header, rerr := br.ReadString('\n')
		if header == "" && rerr == io.EOF {
			break
		}
		if rerr != nil && rerr != io.EOF {
			return fetched, failed, append(errs, fmt.Errorf("puller: bundle frame: %w", rerr))
		}
		hash, size, perr := parseBundleHeader(header)
		if perr != nil {
			return fetched, failed, append(errs, fmt.Errorf("puller: bundle header %q: %w", strings.TrimSpace(header), perr))
		}
		if size < 0 {
			failed++
			errs = append(errs, fmt.Errorf("object %s: absent at source", hash))
			if rerr == io.EOF {
				break
			}
			continue
		}
		lr := io.LimitReader(br, size)
		vr := newVerifyReader(lr, hexPrefix(hash))
		putErr := p.Store.Put(ctx, hash, vr, size)
		_, _ = io.Copy(io.Discard, lr)
		if putErr != nil {
			failed++
			errs = append(errs, fmt.Errorf("object %s: %w", hash, putErr))
		} else {
			fetched++
		}
		if rerr == io.EOF {
			break
		}
	}
	return fetched, failed, errs
}

func (p *Puller) bundleHTTPClient() *http.Client {
	if p.Client != nil {
		return p.Client
	}
	return http.DefaultClient
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
