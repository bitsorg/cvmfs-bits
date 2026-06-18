// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package puller

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"

	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/internal/distribute"
	"cvmfs.io/prepub/internal/distribute/manifest"
)

// defaultSlots is the parallel-transfer default when Slots is unset; chosen to
// match the cvmfs_server snapshot norm rather than a timid handful.
const defaultSlots = 16

// Puller fetches a transaction's objects into the local CAS (ADR-0001 D1/D3/R3).
// It computes the missing set locally (manifest − localstore), so no per-receiver
// hash list is sent upstream; each object is verified against its hash before
// being installed.
type Puller struct {
	// Store is the local CAS the receiver fills.
	Store cas.Backend
	// Fetcher transfers individual objects (default: HTTPFetcher).
	Fetcher distribute.Fetcher
	// Slots bounds concurrent object fetches / bundle requests (default 16).
	Slots int
	// FilesPerRequest, when > 1, switches to chunked-bundle transfers: the
	// missing set is split into chunks of this many objects, each fetched in one
	// POST /s1/bundle request, with up to Slots requests in flight. 1 (default)
	// keeps the per-object path.
	FilesPerRequest int
	// Client is used for chunked-bundle requests (nil -> http.DefaultClient).
	Client *http.Client
	// State, when set, records the last-synced root on a fully successful pull (R4).
	State *State
}

// Result summarises one Pull.
type Result struct {
	Total   int     // objects in the manifest
	Skipped int     // already present locally
	Fetched int     // newly fetched and installed
	Failed  int     // failed to fetch/verify/install
	Errors  []error // per-object errors (Failed entries)
}

// Pull brings the local CAS up to the manifest. It returns a non-nil error if
// any object failed (the catalog must not be flipped on this receiver until a
// later Pull completes cleanly — the caller gates the ack on this).
func (p *Puller) Pull(ctx context.Context, m *manifest.Manifest) (Result, error) {
	if p.Store == nil || p.Fetcher == nil {
		return Result{}, fmt.Errorf("puller: Store and Fetcher are required")
	}
	if len(m.BaseURLs) == 0 {
		return Result{}, fmt.Errorf("puller: manifest %s has no base_urls", m.TransactionID)
	}

	missing := m.Missing(func(h string) bool {
		ok, err := p.Store.Exists(ctx, h)
		return err == nil && ok // a stat error is treated as "absent" → re-fetch
	})

	res := Result{Total: len(m.Objects), Skipped: len(m.Objects) - len(missing)}

	slots := p.Slots
	if slots <= 0 {
		slots = defaultSlots
	}
	sem := make(chan struct{}, slots)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, obj := range missing {
		if ctx.Err() != nil {
			break
		}
		obj := obj
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			err := p.fetchOne(ctx, m.BaseURLs, obj)
			mu.Lock()
			if err != nil {
				res.Failed++
				res.Errors = append(res.Errors, err)
			} else {
				res.Fetched++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	if err := ctx.Err(); err != nil {
		return res, err
	}
	if res.Failed > 0 {
		return res, fmt.Errorf("puller: %d of %d objects failed for txn %s", res.Failed, len(missing), m.TransactionID)
	}
	if p.State != nil && !m.Provisional {
		if err := p.State.Set(m.Repo, m.TargetRootHash); err != nil {
			return res, fmt.Errorf("puller: recording synced root: %w", err)
		}
	}
	return res, nil
}

// fetchOne fetches, verifies and installs a single object, trying base URLs in
// order (a different base / proxy may succeed where one returned bad bytes).
func (p *Puller) fetchOne(ctx context.Context, bases []string, obj manifest.ObjRef) error {
	var lastErr error
	for _, base := range bases {
		rc, err := p.Fetcher.Fetch(ctx, base, obj)
		if err != nil {
			lastErr = err
			continue
		}
		err = p.storeVerified(ctx, obj, rc)
		rc.Close()
		if err == nil {
			return nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no base urls")
	}
	return fmt.Errorf("object %s: %w", obj.Hash, lastErr)
}

// storeVerified streams the object through a hash-verifying reader into the CAS.
// If the digest mismatches, the verifying reader errors at EOF and the CAS Put
// aborts before installing (no partial / poisoned object lands).
func (p *Puller) storeVerified(ctx context.Context, obj manifest.ObjRef, rc io.Reader) error {
	size := obj.Size
	if size <= 0 {
		size = -1 // unknown: force the streaming path (avoid buffering large objects)
	}
	vr := newVerifyReader(rc, hexPrefix(obj.Hash))
	return p.Store.Put(ctx, obj.Hash, vr, size)
}
