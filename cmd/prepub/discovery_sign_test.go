// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"testing"

	"cvmfs.io/prepub/internal/distribute/serve"
)

func TestDiscoverySignVerify(t *testing.T) {
	secret := []byte("discovery-secret-at-least-32-bytes!")
	sd := &staticDiscovery{
		repos:  []string{"r.cern.ch"},
		cp:     serve.ControlPlaneRef{Type: "mqtt", URL: "wss://s0:1882"},
		signer: hmacDiscoverySigner(secret),
	}
	doc, ok, err := sd.Discovery(context.Background(), "r.cern.ch")
	if err != nil || !ok {
		t.Fatalf("Discovery: ok=%v err=%v", ok, err)
	}
	if doc.Signature == "" {
		t.Fatal("signed doc must carry a signature")
	}
	if !doc.Verify(hmacDiscoveryVerify(secret)) {
		t.Error("a validly-signed doc must verify")
	}
	// Tampering with the advertised URL must invalidate the signature.
	bad := doc
	bad.ControlPlane.URL = "wss://attacker:1882"
	if bad.Verify(hmacDiscoveryVerify(secret)) {
		t.Error("tampered doc must NOT verify (MITM defense)")
	}
	// A wrong secret must not verify.
	if doc.Verify(hmacDiscoveryVerify([]byte("the-wrong-secret-32-bytes-padding!"))) {
		t.Error("wrong secret must NOT verify")
	}
	// nil verifier (dev) accepts anything.
	if !doc.Verify(nil) {
		t.Error("nil verifier should accept (dev)")
	}
}
