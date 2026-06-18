// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"
	"time"

	mqttbroker "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

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

// TestBrokerAuthHookConnectionIdentity locks in the fix for the announce-ACL
// race: the authenticated node must be derived from the verified token and
// stored on the connection (cl.Properties.Username), NOT from a client-supplied
// username and NOT from a separate map that OnDisconnect can clear out from
// under an inflight publish on a reconnecting client.
func TestBrokerAuthHookConnectionIdentity(t *testing.T) {
	secret := []byte("test-secret-at-least-32-bytes-long!!")
	m := credential.NewMinter(secret)
	h := newBrokerAuthHook(credential.NewVerifier(secret), "publisher", newRevocation(), nil)

	recvTok, _, _ := m.Mint("stratum1-a", "control", "n1", time.Minute)
	pubTok, _, _ := m.Mint("publisher", "control", "n2", time.Minute)

	// A receiver connects but FORGES the CONNECT username as "publisher".
	recvCl := &mqttbroker.Client{}
	recvCl.ID = "recv-1"
	recvCl.Properties.Username = []byte("publisher") // forged; must be overwritten
	var recvPk packets.Packet
	recvPk.Connect.Password = []byte(recvTok)
	if ok := h.OnConnectAuthenticate(recvCl, recvPk); !ok {
		t.Fatal("valid receiver token must authenticate")
	}
	if got := string(recvCl.Properties.Username); got != "stratum1-a" {
		t.Fatalf("verified node must overwrite forged username: got %q", got)
	}
	// Despite the forged "publisher" username, the receiver must NOT be able to
	// publish announce, and CAN publish its own ready.
	if h.OnACLCheck(recvCl, "cvmfs/repos/r/announce", true) {
		t.Error("receiver (forged username) must NOT be authorized to publish announce")
	}
	if !h.OnACLCheck(recvCl, "cvmfs/receivers/stratum1-a/ready", true) {
		t.Error("receiver must be authorized to publish its ready")
	}

	// The real publisher authenticates and CAN publish announce — and this still
	// holds after an OnDisconnect of a *different* client id (the old map-race
	// would have cleared shared state).
	pubCl := &mqttbroker.Client{}
	pubCl.ID = "pub-1"
	var pubPk packets.Packet
	pubPk.Connect.Password = []byte(pubTok)
	if ok := h.OnConnectAuthenticate(pubCl, pubPk); !ok {
		t.Fatal("valid publisher token must authenticate")
	}
	h.OnDisconnect(recvCl, nil, true) // unrelated disconnect must not affect pubCl
	if !h.OnACLCheck(pubCl, "cvmfs/repos/r/announce", true) {
		t.Error("publisher must remain authorized to publish announce after an unrelated disconnect")
	}
}
