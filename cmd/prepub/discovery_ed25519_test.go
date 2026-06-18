// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
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
