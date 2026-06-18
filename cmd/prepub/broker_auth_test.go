// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"
	"time"

	"cvmfs.io/prepub/internal/distribute/credential"
)

func TestBrokerAuthHook(t *testing.T) {
	secret := []byte("test-secret-at-least-32-bytes-long!!")
	m := credential.NewMinter(secret)
	h := newBrokerAuthHook(credential.NewVerifier(secret), "publisher", newRevocation(), nil)

	pubTok, _, _ := m.Mint("publisher", "control", "n1", time.Minute)
	recvTok, _, _ := m.Mint("stratum1-a", "control", "n2", time.Minute)
	expTok, _, _ := m.Mint("stratum1-b", "control", "n3", -time.Minute) // already expired

	// Authentication
	if n, ok := h.authNode(pubTok); !ok || n != "publisher" {
		t.Fatalf("publisher token should authenticate, got %q ok=%v", n, ok)
	}
	if n, ok := h.authNode(recvTok); !ok || n != "stratum1-a" {
		t.Fatalf("receiver token should authenticate, got %q ok=%v", n, ok)
	}
	if _, ok := h.authNode(expTok); ok {
		t.Error("expired token must be rejected")
	}
	if _, ok := h.authNode("garbage.token"); ok {
		t.Error("garbage token must be rejected")
	}

	// Revocation
	h.Revoke("stratum1-a")
	if _, ok := h.authNode(recvTok); ok {
		t.Error("revoked node must be rejected even with a valid token")
	}

	// Authorization
	if !aclAllowed("publisher", "publisher", "cvmfs/repos/r/announce", true) {
		t.Error("publisher must be allowed to publish announce")
	}
	if aclAllowed("stratum1-a", "publisher", "cvmfs/repos/r/announce", true) {
		t.Error("receiver must NOT be allowed to publish announce")
	}
	if aclAllowed("stratum1-a", "publisher", "cvmfs/repos/r/published", true) {
		t.Error("receiver must NOT be allowed to publish published")
	}
	if !aclAllowed("stratum1-a", "publisher", "cvmfs/receivers/stratum1-a/ready", true) {
		t.Error("receiver must be allowed to publish its ready")
	}
	if !aclAllowed("stratum1-a", "publisher", "cvmfs/receivers/stratum1-a/presence", true) {
		t.Error("receiver must be allowed to publish its presence")
	}
	if !aclAllowed("stratum1-a", "publisher", "cvmfs/repos/r/announce", false) {
		t.Error("receiver must be allowed to subscribe announce")
	}
}
