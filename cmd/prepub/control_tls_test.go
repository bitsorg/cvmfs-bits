// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/distribute/credential"
	"cvmfs.io/prepub/pkg/observe"
)

func discardObs() *observe.Provider {
	return &observe.Provider{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

// TestRevokeHandlerAuth locks in the admin gate: only a publisher-minted token
// may revoke; no token or a receiver token is forbidden; the publisher cannot
// be revoked.
func TestRevokeHandlerAuth(t *testing.T) {
	secret := []byte("test-secret-at-least-32-bytes-long!!")
	m := credential.NewMinter(secret)
	revoc := newRevocation()
	h := revokeHandler(credential.NewVerifier(secret), revoc, nil, nil, discardObs())

	post := func(tok, body string) int {
		r := httptest.NewRequest(http.MethodPost, "/control/revoke", strings.NewReader(body))
		if tok != "" {
			r.Header.Set("Authorization", "Bearer "+tok)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}

	pubTok, _, _ := m.Mint("publisher", "control", "n1", time.Minute)
	recvTok, _, _ := m.Mint("stratum1-a", "control", "n2", time.Minute)

	if code := post("", `{"node":"stratum1-a"}`); code != http.StatusForbidden {
		t.Errorf("no token => want 403, got %d", code)
	}
	if code := post(recvTok, `{"node":"stratum1-b"}`); code != http.StatusForbidden {
		t.Errorf("receiver token must not revoke => want 403, got %d", code)
	}
	if revoc.IsRevoked("stratum1-a") || revoc.IsRevoked("stratum1-b") {
		t.Fatal("forbidden requests must not revoke anything")
	}
	if code := post(pubTok, `{"node":"stratum1-a"}`); code != http.StatusOK {
		t.Errorf("publisher token => want 200, got %d", code)
	}
	if !revoc.IsRevoked("stratum1-a") {
		t.Error("stratum1-a must be revoked after a valid admin revoke")
	}
	if code := post(pubTok, `{"node":"publisher"}`); code != http.StatusBadRequest {
		t.Errorf("revoking the publisher must be rejected => want 400, got %d", code)
	}
}
