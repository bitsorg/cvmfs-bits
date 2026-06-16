// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package serve

import (
	"context"
	"net/http"
	"strings"

	"cvmfs.io/prepub/internal/distribute/manifest"
)

// ManifestSource provides the manifest for a transaction id.
type ManifestSource interface {
	Manifest(ctx context.Context, txn string) (*manifest.Manifest, bool, error)
}

// ManifestHandler serves GET /s1/{txn}/manifest (ADR D3/D4). It returns a single
// JSON document by default, or streamed NDJSON (for large cold-start deltas)
// when the client sends Accept: application/x-ndjson or ?stream=1.
type ManifestHandler struct {
	Source ManifestSource
}

func (h *ManifestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	txn, ok := segmentBetween(r.URL.Path, "/s1/", "/manifest")
	if !ok {
		http.Error(w, "bad manifest path", http.StatusBadRequest)
		return
	}
	m, found, err := h.Source.Manifest(r.Context(), txn)
	if err != nil {
		http.Error(w, "manifest error", http.StatusInternalServerError)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}
	if wantsNDJSON(r) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_ = m.EncodeNDJSON(w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = m.Encode(w)
}

func wantsNDJSON(r *http.Request) bool {
	return r.URL.Query().Get("stream") == "1" ||
		strings.Contains(r.Header.Get("Accept"), "application/x-ndjson")
}
