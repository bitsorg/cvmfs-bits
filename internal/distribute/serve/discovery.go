// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package serve

import (
	"context"
	"encoding/json"
	"net/http"
)

// ControlPlaneRef names the control-plane transport and endpoint an S1 should
// connect to (ADR D7/D10).
type ControlPlaneRef struct {
	Type string `json:"type"` // "mqtt" | "sse"
	URL  string `json:"url"`
}

// Discovery is the signed bootstrap document served at
// GET /cvmfs/{repo}/.cvmfsbits (ADR D10). An S1's only required configuration is
// its Stratum 0 URL; it fetches this to learn where the control plane is and
// which repos this S0 serves.
type Discovery struct {
	Repos        []string        `json:"repos"`
	ControlPlane ControlPlaneRef `json:"control_plane"`
	CA           string          `json:"ca,omitempty"`         // PEM trust anchor for the control plane
	EnrollURL    string          `json:"enroll_url,omitempty"` // HTTPS base for challenge/enroll (token confidentiality)
	Signature    string          `json:"signature,omitempty"`  // detached signature over the doc with this field empty
}

// Signer produces a detached signature over the canonical (signature-empty)
// document bytes. In production this reuses the repository signing key (the
// .cvmfswhitelist trust path); a nil Signer leaves the document unsigned
// (dev/test only) — see Sign.
type Signer func(payload []byte) (string, error)

// Sign returns a copy of d with Signature filled from s, computed over the
// canonical signature-empty encoding. A nil Signer returns d unchanged.
func (d Discovery) Sign(s Signer) (Discovery, error) {
	if s == nil {
		return d, nil
	}
	unsigned := d
	unsigned.Signature = ""
	payload, err := json.Marshal(unsigned)
	if err != nil {
		return d, err
	}
	sig, err := s(payload)
	if err != nil {
		return d, err
	}
	d.Signature = sig
	return d, nil
}

// Verify recomputes the signature over the canonical (signature-empty) encoding
// and reports whether it matches d.Signature. A nil verify accepts any document
// (dev). verify receives the canonical payload and the detached signature and
// must compare them in constant time.
func (d Discovery) Verify(verify func(payload []byte, sig string) bool) bool {
	if verify == nil {
		return true
	}
	unsigned := d
	unsigned.Signature = ""
	payload, err := json.Marshal(unsigned)
	if err != nil {
		return false
	}
	return verify(payload, d.Signature)
}

// DiscoverySource returns the Discovery document for a repo.
type DiscoverySource interface {
	Discovery(ctx context.Context, repo string) (Discovery, bool, error)
}

// DiscoveryHandler serves GET /cvmfs/{repo}/.cvmfsbits.
type DiscoveryHandler struct {
	Source DiscoverySource
}

func (h *DiscoveryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	repo, ok := segmentBetween(r.URL.Path, "/cvmfs/", "/.cvmfsbits")
	if !ok {
		http.Error(w, "bad discovery path", http.StatusBadRequest)
		return
	}
	d, found, err := h.Source.Discovery(r.Context(), repo)
	if err != nil {
		http.Error(w, "discovery error", http.StatusInternalServerError)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache")
	_ = json.NewEncoder(w).Encode(d)
}
