// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package serve

import (
	"encoding/json"
	"net/http"

	"cvmfs.io/prepub/internal/distribute/commit"
)

// LeaseGranter issues pull leases; implemented by *commit.Admission (ADR D6).
type LeaseGranter interface {
	Grant(node, txn string) (commit.Lease, bool)
}

// LeaseHandler serves POST /s1/{txn}/lease — admission control for a receiver's
// pull. On success it returns the lease; when the concurrency cap is reached it
// returns 429 so the receiver backs off and retries.
//
// Security: the node identity is taken from the mTLS client-certificate CN when
// one is presented — it cannot be spoofed, so the per-node cap and admission
// accounting are sound. The X-Node-Id header is honoured only as a fallback when
// no client certificate is present (development / non-mTLS), and never overrides
// a certificate. This endpoint MUST be served over mTLS in production.
type LeaseHandler struct {
	Admission LeaseGranter
}

// nodeIdentity returns the caller's node id: the mTLS client-certificate CN if a
// client cert was presented, otherwise the X-Node-Id header (dev fallback).
func nodeIdentity(r *http.Request) string {
	if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
		if cn := r.TLS.PeerCertificates[0].Subject.CommonName; cn != "" {
			return cn
		}
	}
	return r.Header.Get("X-Node-Id")
}

type leaseResponse struct {
	LeaseID        string `json:"lease_id"`
	ExpiresUnix    int64  `json:"expires_unix"`
	MaxBytesPerSec int64  `json:"max_bytes_per_sec,omitempty"`
	Slots          int    `json:"slots,omitempty"`
}

func (h *LeaseHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	txn, ok := segmentBetween(r.URL.Path, "/s1/", "/lease")
	if !ok {
		http.Error(w, "bad lease path", http.StatusBadRequest)
		return
	}
	node := nodeIdentity(r)
	if node == "" {
		http.Error(w, "missing node identity (mTLS client cert or X-Node-Id)", http.StatusBadRequest)
		return
	}
	lease, granted := h.Admission.Grant(node, txn)
	if !granted {
		w.Header().Set("Retry-After", "5")
		http.Error(w, "admission: concurrency limit reached, retry", http.StatusTooManyRequests)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(leaseResponse{
		LeaseID:        lease.ID,
		ExpiresUnix:    lease.Expires.Unix(),
		MaxBytesPerSec: lease.Budget.MaxBytesPerSec,
		Slots:          lease.Budget.Slots,
	})
}
