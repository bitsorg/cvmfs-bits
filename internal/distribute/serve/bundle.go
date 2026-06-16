// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package serve

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// maxBundleHashes caps how many objects one bundle request may ask for, bounding
// the work a single request can trigger.
const maxBundleHashes = 100000

// BundleHandler serves many CAS objects in a single streamed response, so a
// receiver fetching a large delta of small objects pays one request/round-trip
// instead of one per object (ADR-0001 P-A, the "archiving on top" option). It is
// an optimisation over the per-object ObjectHandler, not a replacement: objects
// are still content-addressed and individually hash-verified by the receiver.
//
//	POST /s1/bundle   {"repo":"…","hashes":["…",…]}
//	→ application/x-cvmfs-bundle, a stream of frames:
//	      "<hash> <size>\n" <size bytes>     (object present)
//	      "<hash> -1\n"                       (object absent — receiver retries it)
//
// The framing is self-delimiting and streamed both ways, so neither side buffers
// the whole bundle.
type BundleHandler struct {
	Store ObjectStore
}

type bundleReq struct {
	Repo   string   `json:"repo"`
	Hashes []string `json:"hashes"`
}

func (h *BundleHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req bundleReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20)).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if len(req.Hashes) == 0 {
		http.Error(w, "no hashes requested", http.StatusBadRequest)
		return
	}
	if len(req.Hashes) > maxBundleHashes {
		http.Error(w, "too many hashes", http.StatusRequestEntityTooLarge)
		return
	}

	w.Header().Set("Content-Type", "application/x-cvmfs-bundle")
	w.WriteHeader(http.StatusOK)
	ctx := r.Context()
	for _, hash := range req.Hashes {
		if ctx.Err() != nil {
			return
		}
		if !validObjectName(hash) {
			// A malformed hash cannot index the CAS; report absent and continue
			// rather than aborting the whole bundle.
			fmt.Fprintf(w, "%s -1\n", "invalid")
			continue
		}
		size, err := h.Store.Size(ctx, hash)
		if err != nil || size < 0 {
			fmt.Fprintf(w, "%s -1\n", hash)
			continue
		}
		rc, err := h.Store.Get(ctx, hash)
		if err != nil {
			fmt.Fprintf(w, "%s -1\n", hash)
			continue
		}
		if _, err := fmt.Fprintf(w, "%s %d\n", hash, size); err != nil {
			rc.Close()
			return
		}
		// Copy exactly the object body; if it ends short the receiver's hash check
		// fails and it re-fetches that object individually.
		_, copyErr := io.Copy(w, rc)
		rc.Close()
		if copyErr != nil {
			return
		}
	}
}
