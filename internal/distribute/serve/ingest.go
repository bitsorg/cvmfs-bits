// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package serve

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"cvmfs.io/prepub/internal/distribute/manifest"
)

// ManifestPutter is the write half of ManifestStore.
type ManifestPutter interface {
	Put(ctx context.Context, m *manifest.Manifest) error
}

// ManifestIngestHandler accepts a transaction manifest submitted by a trusted
// producer — typically the CVMFS gateway — and stores it for receivers to pull
// (ADR-0001 D3; gateway-submitted manifests). It is a *separate, authenticated*
// endpoint from the receiver-facing GET, e.g. mounted under the existing
// authenticated API subrouter:
//
//	POST /api/v1/distribute/manifests
//	Content-Type: application/json        (single document), or
//	Content-Type: application/x-ndjson    (streamed header + object-per-line)
//
// The transaction id is taken from the manifest body (TransactionID).
type ManifestIngestHandler struct {
	Store ManifestPutter
	// MaxBytes caps the request body for both the JSON and NDJSON paths
	// (0 = 256 MiB default). The cap bounds server memory: the NDJSON path
	// accumulates objects in memory in P1, so an uncapped stream could OOM the
	// server. The durable, truly-streaming ingest store lands in P4.
	MaxBytes int64
}

func (h *ManifestIngestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		w.Header().Set("Allow", "POST, PUT")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	maxBytes := h.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 256 << 20
	}
	body := http.MaxBytesReader(w, r.Body, maxBytes)

	var (
		m   *manifest.Manifest
		err error
	)
	if strings.Contains(r.Header.Get("Content-Type"), "application/x-ndjson") {
		var objs []manifest.ObjRef
		m, err = manifest.DecodeNDJSON(body, func(o manifest.ObjRef) error {
			objs = append(objs, o)
			return nil
		})
		if err == nil {
			m.Objects = objs
		}
	} else {
		m, err = manifest.Decode(body)
	}
	if err != nil {
		http.Error(w, "bad manifest: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := m.Validate(); err != nil {
		http.Error(w, "invalid manifest: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := h.Store.Put(r.Context(), m); err != nil {
		http.Error(w, "store error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]string{"transaction_id": m.TransactionID})
}
