// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"cvmfs.io/prepub/internal/distribute/serve"
)

func writeEd25519Keys(t *testing.T) (privPath, pubPath string) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	privDER, _ := x509.MarshalPKCS8PrivateKey(priv)
	pubDER, _ := x509.MarshalPKIXPublicKey(pub)
	privPath = filepath.Join(dir, "k.key")
	pubPath = filepath.Join(dir, "k.pub")
	_ = os.WriteFile(privPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER}), 0o600)
	_ = os.WriteFile(pubPath, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}), 0o644)
	return privPath, pubPath
}

func TestEd25519DiscoverySignVerify(t *testing.T) {
	privPath, pubPath := writeEd25519Keys(t)
	signer, err := ed25519SignerFromFile(privPath)
	if err != nil {
		t.Fatal(err)
	}
	verify, err := ed25519VerifierFromFile(pubPath)
	if err != nil {
		t.Fatal(err)
	}
	doc := serve.Discovery{Repos: []string{"r"}, ControlPlane: serve.ControlPlaneRef{Type: "mqtt", URL: "wss://s0:1882"}, EnrollURL: "https://s0:8443"}
	signed, err := doc.Sign(signer)
	if err != nil {
		t.Fatal(err)
	}
	if !signed.Verify(verify) {
		t.Fatal("valid signature must verify")
	}
	// Tamper: a changed field must fail verification.
	tampered := signed
	tampered.ControlPlane.URL = "wss://evil:1882"
	if tampered.Verify(verify) {
		t.Error("tampered document must NOT verify (MITM)")
	}
	// A different key must not verify.
	_, otherPub := writeEd25519Keys(t)
	otherVerify, _ := ed25519VerifierFromFile(otherPub)
	if signed.Verify(otherVerify) {
		t.Error("signature must not verify under a different public key")
	}
}

// TestReceiverDiscoveryVerification: --broker-auth needs a verify key, and a
// configured key is enforced whether or not --broker-auth is set.
func TestReceiverDiscoveryVerification(t *testing.T) {
	if checkReceiverAuthConfig(true, "") == nil {
		t.Error("--broker-auth without --discovery-verify-key must be a startup error")
	}
	if checkReceiverAuthConfig(true, "k.pub") != nil || checkReceiverAuthConfig(false, "") != nil {
		t.Error("valid combinations rejected")
	}

	privPath, pubPath := writeEd25519Keys(t)
	signer, err := ed25519SignerFromFile(privPath)
	if err != nil {
		t.Fatal(err)
	}
	doc := serve.Discovery{Repos: []string{"r"}, ControlPlane: serve.ControlPlaneRef{Type: "mqtt", URL: "wss://s0:1882"}}
	signed, _ := doc.Sign(signer)
	if err := verifyDiscovery(signed, pubPath); err != nil {
		t.Errorf("valid document rejected: %v", err)
	}
	tampered := signed
	tampered.ControlPlane.URL = "wss://evil:1882"
	if verifyDiscovery(tampered, pubPath) == nil {
		t.Error("tampered document accepted")
	}
	if verifyDiscovery(doc, pubPath) == nil {
		t.Error("unsigned document accepted although a verify key is set")
	}
	if verifyDiscovery(tampered, "") != nil {
		t.Error("no verify key configured: the document is not checked")
	}
}

// TestFetchDiscoveryUsesBrokerCA: with --broker-ca-cert the discovery GET
// trusts that CA; without it the system pool rejects a private CA.
func TestFetchDiscoveryUsesBrokerCA(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"repos":["r.cern.ch"],"control_plane":{"type":"mqtt","url":"wss://s0:1882"}}`))
	}))
	srv.Config.ErrorLog = log.New(io.Discard, "", 0) // the rejected handshake is expected
	srv.StartTLS()
	defer srv.Close()
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(caPath, pemBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	withCA, err := discoveryHTTPClient(caPath)
	if err != nil {
		t.Fatal(err)
	}
	d, err := fetchDiscovery(context.Background(), withCA, srv.URL, "r.cern.ch")
	if err != nil || d.ControlPlane.URL != "wss://s0:1882" {
		t.Fatalf("fetch with broker CA: %v %+v", err, d)
	}
	system, _ := discoveryHTTPClient("")
	if _, err := fetchDiscovery(context.Background(), system, srv.URL, "r.cern.ch"); err == nil {
		t.Error("system pool must not trust the test CA")
	}
}

// TestDiscoveryHTTPClientPoolAndProxy: with --broker-ca-cert the discovery
// client trusts the system pool plus that CA, and still honours the proxy
// environment; the enroll/revoke client trusts only the CA.
func TestDiscoveryHTTPClientPoolAndProxy(t *testing.T) {
	srv := httptest.NewTLSServer(http.NotFoundHandler())
	defer srv.Close()
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(caPath, pemBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	want, err := x509.SystemCertPool()
	if err != nil {
		want = x509.NewCertPool()
	}
	want.AppendCertsFromPEM(pemBytes)
	caOnly := x509.NewCertPool()
	caOnly.AppendCertsFromPEM(pemBytes)

	for name, tc := range map[string]struct {
		mk   func(string) (*http.Client, error)
		pool *x509.CertPool
	}{
		"discovery": {discoveryHTTPClient, want},
		"ca-only":   {caHTTPClient, caOnly},
	} {
		c, err := tc.mk(caPath)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		tr := c.Transport.(*http.Transport)
		if tr.Proxy == nil {
			t.Errorf("%s: transport ignores the proxy environment", name)
		}
		if !tr.TLSClientConfig.RootCAs.Equal(tc.pool) {
			t.Errorf("%s: unexpected root pool", name)
		}
	}
}

// TestStaticDiscoveryRejectsInvalidRepo: no document is signed for a name that
// is not a valid repository name.
func TestStaticDiscoveryRejectsInvalidRepo(t *testing.T) {
	d := &staticDiscovery{cp: serve.ControlPlaneRef{Type: "mqtt", URL: "wss://s0:1882"}}
	if _, found, _ := d.Discovery(context.Background(), "a..b"); found {
		t.Error("discovery answered for an invalid repository name")
	}
	if _, found, _ := d.Discovery(context.Background(), "r.cern.ch"); !found {
		t.Error("discovery refused a valid repository name")
	}
}
