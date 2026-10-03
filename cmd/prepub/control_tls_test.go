// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/api"
	"cvmfs.io/prepub/internal/distribute/credential"
	"cvmfs.io/prepub/internal/httpsig"
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
	h := adminOnly(credential.NewVerifier(secret), revokeCore(revoc, nil, nil, discardObs(), false))

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

// TestRevokeViaAPIRequest: `prepub revoke --api-url` sends a request the API
// auth accepts (HMAC-signed with PREPUB_API_TOKEN, body bound) to RevokePath.
func TestRevokeViaAPIRequest(t *testing.T) {
	t.Setenv("PREPUB_API_TOKEN", "api-token")
	req, err := buildRevokeRequest("stratum1-a", false, "", "http://s0:8080/")
	if err != nil {
		t.Fatal(err)
	}
	if req.URL.Path != api.RevokePath || req.Header.Get("Authorization") != "" {
		t.Fatalf("path %q, Authorization %q", req.URL.Path, req.Header.Get("Authorization"))
	}
	sig, err := httpsig.Parse(req.Header.Get(httpsig.HeaderName))
	if err != nil {
		t.Fatal(err)
	}
	if err := sig.Verify([]byte("api-token"), req.Method, req.URL.RequestURI(), time.Now(), httpsig.DefaultSkew); err != nil {
		t.Fatalf("signature: %v", err)
	}
	body, _ := io.ReadAll(req.Body)
	if sig.KeyID != api.SigningKeyID || sig.BodyHash != httpsig.BodyDigest(body) || !strings.Contains(string(body), "stratum1-a") {
		t.Errorf("key %q, body %q not bound by the signature", sig.KeyID, body)
	}

	t.Setenv("PREPUB_API_TOKEN", "")
	if _, err := buildRevokeRequest("stratum1-a", false, "", "http://s0:8080"); err == nil {
		t.Error("--api-url without PREPUB_API_TOKEN must fail")
	}
}

// TestRevokeCoreSharedDenylist: the API route's handler revokes in the same
// persisted denylist as the TLS endpoint.
func TestRevokeCoreSharedDenylist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "revoked-nodes.json")
	revoc, err := loadRevocation(path)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	revokeCore(revoc, nil, nil, discardObs(), false).ServeHTTP(rec,
		httptest.NewRequest(http.MethodPost, api.RevokePath, strings.NewReader(`{"node":"stratum1-a"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("got %d: %s", rec.Code, rec.Body.String())
	}
	reloaded, err := loadRevocation(path)
	if err != nil || !reloaded.IsRevoked("stratum1-a") {
		t.Errorf("revocation not persisted: %v", err)
	}
}

// TestRevokeCoreUndoAndValidation: a node name that is not a valid node id or
// a body with other fields is refused, and the unrevoke handler lifts a
// persisted revocation.
func TestRevokeCoreUndoAndValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "revoked-nodes.json")
	revoc, err := loadRevocation(path)
	if err != nil {
		t.Fatal(err)
	}
	post := func(undo bool, body string) int {
		rec := httptest.NewRecorder()
		revokeCore(revoc, nil, nil, discardObs(), undo).ServeHTTP(rec,
			httptest.NewRequest(http.MethodPost, api.RevokePath, strings.NewReader(body)))
		return rec.Code
	}
	for _, body := range []string{`{"node":"a/b"}`, `{"node":"x","nodes":["y","z"]}`, `{"node":"x","undo":true}`} {
		if code := post(false, body); code != http.StatusBadRequest {
			t.Errorf("%s: code %d, want 400", body, code)
		}
	}
	if revoc.IsRevoked("a/b") || revoc.IsRevoked("x") {
		t.Error("a refused request revoked a node")
	}
	if code := post(false, `{"node":"stratum1-a"}`); code != http.StatusOK {
		t.Fatalf("revoke: %d", code)
	}
	if code := post(true, `{"node":"stratum1-a"}`); code != http.StatusOK {
		t.Fatalf("unrevoke: %d", code)
	}
	reloaded, err := loadRevocation(path)
	if err != nil || reloaded.IsRevoked("stratum1-a") || revoc.IsRevoked("stratum1-a") {
		t.Errorf("unrevoke not applied and persisted: %v", err)
	}
}

// TestUnrevokeRequestAndResponse: --undo targets the unrevoke routes, and the
// CLI accepts only a response that confirms the un-revoke -- an older
// publisher's 404, or a body confirming something else, is a failure.
func TestUnrevokeRequestAndResponse(t *testing.T) {
	t.Setenv("PREPUB_API_TOKEN", "x")
	t.Setenv("PREPUB_HMAC_SECRET", "0123456789abcdef0123")
	req, err := buildRevokeRequest("stratum1-a", true, "", "http://s0:8080")
	if err != nil || req.URL.Path != api.UnrevokePath {
		t.Fatalf("api: %v %v", req, err)
	}
	if req, err = buildRevokeRequest("stratum1-a", true, "https://s0:8443", ""); err != nil || req.URL.Path != "/control/unrevoke" {
		t.Fatalf("tls: %v %v", req, err)
	}

	for _, tc := range []struct {
		status int
		body   string
		undo   bool
		ok     bool
	}{
		{200, `{"unrevoked":"stratum1-a"}`, true, true},
		{200, `{"revoked":"stratum1-a","sessions_dropped":0}`, true, false},
		{404, `404 page not found`, true, false},
		{200, `{"unrevoked":"other"}`, true, false},
		{200, `{"revoked":"stratum1-a","sessions_dropped":1}`, false, true},
		{200, `not json`, false, false},
	} {
		if err := checkRevokeResponse(tc.status, []byte(tc.body), "stratum1-a", tc.undo); (err == nil) != tc.ok {
			t.Errorf("%d %s undo=%v: err=%v, want ok=%v", tc.status, tc.body, tc.undo, err, tc.ok)
		}
	}
}
