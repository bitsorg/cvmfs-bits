// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Package serve implements the Stratum-0 HTTP serving side of pull-based
// distribution (ADR-0001 P1): the content-addressed object endpoint, the
// transaction-manifest endpoint, the signed .cvmfsbits discovery document, and
// the GC pin registry. Handlers are framework-agnostic http.Handlers so they can
// be mounted on the existing gorilla/mux router when the pull path is activated.
package serve

import (
	"context"
	"io"
	"net/http"
	"strconv"
)

// ObjectStore is the read subset of cas.Backend needed to serve objects.
type ObjectStore interface {
	Exists(ctx context.Context, hash string) (bool, error)
	Get(ctx context.Context, hash string) (io.ReadCloser, error)
	Size(ctx context.Context, hash string) (int64, error)
}

// ObjectHandler serves content-addressed CAS objects for S1 pull (ADR D5). The
// URL mirrors the CVMFS data layout:
//
//	GET /cvmfs/{repo}/data/{xx}/{rest}
//
// Objects are immutable, so responses are cacheable (default auth: public, D8).
// The client verifies the hash (R3); a 404 means "not yet present" (GC race /
// ordering) and the client should retry.
type ObjectHandler struct {
	Store ObjectStore
}

func (h *ObjectHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	hash, ok := hashFromDataPath(r.URL.Path)
	if !ok {
		http.Error(w, "bad object path", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	exists, err := h.Store.Exists(ctx, hash)
	if err != nil {
		http.Error(w, "store error", http.StatusInternalServerError)
		return
	}
	if !exists {
		http.NotFound(w, r)
		return
	}

	// setMeta sets the response headers; called only once we are committed to
	// emitting a 200, so an error before that point never leaves a stale
	// Content-Length on the (different-length) error response.
	setMeta := func() {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		w.Header().Set("ETag", `"`+hash+`"`)
		if size, err := h.Store.Size(ctx, hash); err == nil && size >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		}
	}

	if r.Method == http.MethodHead {
		setMeta()
		w.WriteHeader(http.StatusOK)
		return
	}

	rc, err := h.Store.Get(ctx, hash)
	if err != nil {
		// Object vanished between Exists and Get (e.g. GC raced the read). No
		// body headers were set yet, so the error response is clean.
		http.Error(w, "store error", http.StatusInternalServerError)
		return
	}
	defer rc.Close()
	setMeta()
	_, _ = io.Copy(w, rc)
}
