// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package serve

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cvmfs.io/prepub/internal/distribute/commit"
)

func TestLeaseHandler(t *testing.T) {
	h := &LeaseHandler{Admission: commit.NewAdmission(commit.Options{MaxConcurrent: 1})}

	// Missing node header → 400.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/s1/t/lease", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing node header: code=%d", rec.Code)
	}

	// Granted → 200 with a lease id.
	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/s1/t/lease", nil)
	req.Header.Set("X-Node-Id", "n1")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"lease_id"`) {
		t.Fatalf("grant: code=%d body=%q", rec.Code, rec.Body.String())
	}

	// Over the cap (MaxConcurrent=1) → 429.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/s1/t/lease", nil)
	req.Header.Set("X-Node-Id", "n2")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("over cap: code=%d", rec.Code)
	}
}

// TestLeaseHandlerCertOverridesHeader proves the node identity comes from the
// mTLS client certificate CN, not the spoofable X-Node-Id header: two requests
// carrying the same client cert but different header values must be accounted to
// the same node, so the per-node cap (1) denies the second.
func TestLeaseHandlerCertOverridesHeader(t *testing.T) {
	h := &LeaseHandler{Admission: commit.NewAdmission(commit.Options{MaxPerNode: 1})}

	withCert := func(cn, hdr string) *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/s1/t/lease", nil)
		if hdr != "" {
			req.Header.Set("X-Node-Id", hdr)
		}
		req.TLS = &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{{Subject: pkix.Name{CommonName: cn}}},
		}
		return req
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, withCert("s1.example", "spoof-a"))
	if rec.Code != http.StatusOK {
		t.Fatalf("cert grant: code=%d", rec.Code)
	}
	// Same cert, different header → still node "s1.example" → per-node cap → 429.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, withCert("s1.example", "spoof-b"))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("header must not bypass per-node cap when a cert is present: code=%d", rec.Code)
	}
}
