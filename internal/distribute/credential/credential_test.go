// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package credential

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTokenMintVerify(t *testing.T) {
	m := NewMinter([]byte("server-secret-0123456789abcdef"))
	v := NewVerifier([]byte("server-secret-0123456789abcdef"))

	tok, _, err := m.Mint("s1-a", "catchup", "n1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	c, err := v.Verify(tok, "catchup")
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if c.Node != "s1-a" || c.Scope != "catchup" {
		t.Fatalf("claims wrong: %+v", c)
	}

	// Wrong scope rejected.
	if _, err := v.Verify(tok, "admin"); err == nil {
		t.Fatal("wrong scope must be rejected")
	}
	// Wrong secret rejected (forgery).
	if _, err := NewVerifier([]byte("different-secret")).Verify(tok, "catchup"); err == nil {
		t.Fatal("token from a different secret must be rejected")
	}
	// Tampered payload rejected.
	if _, err := v.Verify("x"+tok, "catchup"); err == nil {
		t.Fatal("tampered token must be rejected")
	}
}

func TestTokenExpiry(t *testing.T) {
	m := NewMinter([]byte("k"))
	v := &Verifier{secret: []byte("k")} // zero leeway
	tok, _, _ := m.Mint("s1", "catchup", "n", -time.Second)
	if _, err := v.Verify(tok, "catchup"); err == nil {
		t.Fatal("expired token must be rejected")
	}
}

func enrollHarness(t *testing.T) (*httptest.Server, *MapEnrollStore, *Verifier) {
	t.Helper()
	secret := []byte("server-signing-secret-aaaaaaaaaaaa")
	store := NewMapEnrollStore()
	es := NewEnrollServer(store, NewMinter(secret), nil)
	srv := httptest.NewServer(es.Handler())
	t.Cleanup(srv.Close)
	return srv, store, NewVerifier(secret)
}

func TestEnrollChallengeResponseSuccess(t *testing.T) {
	srv, store, v := enrollHarness(t)
	key := []byte("out-of-band-node-key-for-s1-a")
	store.Add("s1-a", key)

	cl := &Client{Base: srv.URL, HTTP: srv.Client(), Node: "s1-a", Key: key}
	tok, err := cl.Token(context.Background())
	if err != nil {
		t.Fatalf("enroll: %v", err)
	}
	claims, err := v.Verify(tok, "catchup")
	if err != nil || claims.Node != "s1-a" {
		t.Fatalf("verify enrolled token: err=%v claims=%+v", err, claims)
	}

	// Cached: a second Token call returns the same token without re-enrolling.
	tok2, _ := cl.Token(context.Background())
	if tok2 != tok {
		t.Fatal("token should be cached until near expiry")
	}
}

func TestEnrollWrongKeyRejected(t *testing.T) {
	srv, store, _ := enrollHarness(t)
	store.Add("s1-a", []byte("the-real-key"))

	cl := &Client{Base: srv.URL, HTTP: srv.Client(), Node: "s1-a", Key: []byte("attacker-guess")}
	if _, err := cl.Token(context.Background()); err == nil {
		t.Fatal("enrollment with the wrong key must fail")
	}
}

func TestEnrollUnknownNodeRejected(t *testing.T) {
	srv, _, _ := enrollHarness(t)
	cl := &Client{Base: srv.URL, HTTP: srv.Client(), Node: "ghost", Key: []byte("whatever")}
	if _, err := cl.Token(context.Background()); err == nil {
		t.Fatal("enrollment of an unprovisioned node must fail")
	}
}

func TestEnrollNonceIsOneTimeAndBound(t *testing.T) {
	srv, store, _ := enrollHarness(t)
	key := []byte("k-s1-a")
	store.Add("s1-a", key)

	// Manually drive the protocol so we can replay the same nonce.
	cresp, err := srv.Client().Get(srv.URL + "/control/challenge?node=s1-a")
	if err != nil {
		t.Fatal(err)
	}
	var ch challengeResp
	if err := json.NewDecoder(cresp.Body).Decode(&ch); err != nil {
		t.Fatal(err)
	}
	cresp.Body.Close()

	post := func(node, nonce, mac string) int {
		body := `{"node":"` + node + `","nonce":"` + nonce + `","mac":"` + mac + `"}`
		resp, err := srv.Client().Post(srv.URL+"/control/enroll", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	mac := EnrollMACHex(key, "s1-a", ch.Nonce)
	if code := post("s1-a", ch.Nonce, mac); code != http.StatusOK {
		t.Fatalf("first enroll should succeed, got %d", code)
	}
	// Replay the same nonce → rejected (consumed).
	if code := post("s1-a", ch.Nonce, mac); code != http.StatusUnauthorized {
		t.Fatalf("nonce replay must be 401, got %d", code)
	}
}

func TestRequireTokenMiddleware(t *testing.T) {
	m := NewMinter([]byte("sek"))
	v := NewVerifier([]byte("sek"))
	protected := RequireToken(v, "catchup")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, ok := ClaimsFrom(r.Context())
		if !ok || c.Node == "" {
			t.Error("claims not propagated to handler")
		}
		w.WriteHeader(http.StatusOK)
	}))
	srv := httptest.NewServer(protected)
	defer srv.Close()

	// No token → 401.
	resp, _ := srv.Client().Get(srv.URL)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("missing token: %d", resp.StatusCode)
	}
	// Valid token → 200.
	tok, _, _ := m.Mint("s1", "catchup", "n", time.Minute)
	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp2, _ := srv.Client().Do(req)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("valid token: %d", resp2.StatusCode)
	}

	// nil verifier disables the gate (handler reached without a token).
	open := RequireToken(nil, "catchup")(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	osrv := httptest.NewServer(open)
	defer osrv.Close()
	r3, _ := osrv.Client().Get(osrv.URL)
	r3.Body.Close()
	if r3.StatusCode != http.StatusOK {
		t.Fatalf("nil verifier should pass through: %d", r3.StatusCode)
	}
}
