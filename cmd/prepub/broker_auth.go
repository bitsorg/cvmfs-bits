// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"

	mqttbroker "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"cvmfs.io/prepub/internal/distribute/credential"
	"cvmfs.io/prepub/pkg/observe"
)

// brokerAuthHook authenticates and authorizes control-plane MQTT clients with
// the credential token scheme — no certificates. A client presents its scoped
// bearer token (obtained via the challenge/response enrollment) as the MQTT
// CONNECT password; the hook verifies the HMAC signature + expiry, records the
// node identity, and enforces per-role topic ACLs. A revocation denylist plus
// active disconnect (in-process broker) gives immediate cut-off (H3).
type brokerAuthHook struct {
	mqttbroker.HookBase
	verifier      *credential.Verifier
	publisherNode string // node id granted publish rights to control topics (S0)
	obs           *observe.Provider

	mu      sync.RWMutex
	clients map[string]string // mqtt client-id -> authenticated node id
	revoc   *revocation       // shared revocation denylist
}

func newBrokerAuthHook(v *credential.Verifier, publisherNode string, revoc *revocation, obs *observe.Provider) *brokerAuthHook {
	return &brokerAuthHook{
		verifier: v, publisherNode: publisherNode, obs: obs,
		clients: map[string]string{}, revoc: revoc,
	}
}

func (h *brokerAuthHook) ID() string { return "cvmfs-control-auth" }

func (h *brokerAuthHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqttbroker.OnConnectAuthenticate,
		mqttbroker.OnACLCheck,
		mqttbroker.OnDisconnect,
	}, []byte{b})
}

// authNode verifies a token and returns the authenticated node id. It is the
// pure, testable core of OnConnectAuthenticate.
func (h *brokerAuthHook) authNode(token string) (string, bool) {
	claims, err := h.verifier.Verify(token, "") // scope-agnostic: any valid, unexpired token
	if err != nil {
		return "", false
	}
	if h.revoc.IsRevoked(claims.Node) {
		return "", false
	}
	return claims.Node, true
}

// aclAllowed is the pure, testable authorization rule. The publisher may do
// anything; receivers may SUBSCRIBE freely but may only PUBLISH to their own
// ready/presence topics — they cannot publish announce/published (which would
// let a forged warm/ready ack push the publisher toward a premature commit).
func aclAllowed(node, publisherNode, topic string, write bool) bool {
	if node != "" && node == publisherNode {
		return true
	}
	if !write {
		return true
	}
	return strings.Contains(topic, "/ready") || strings.Contains(topic, "/presence")
}

func (h *brokerAuthHook) OnConnectAuthenticate(cl *mqttbroker.Client, pk packets.Packet) bool {
	node, ok := h.authNode(string(pk.Connect.Password))
	if !ok {
		h.obs.Logger.Warn("broker: connection rejected (bad/expired/revoked token)", "client", cl.ID)
		return false
	}
	h.mu.Lock()
	h.clients[cl.ID] = node
	h.mu.Unlock()
	return true
}

func (h *brokerAuthHook) OnACLCheck(cl *mqttbroker.Client, topic string, write bool) bool {
	h.mu.RLock()
	node := h.clients[cl.ID]
	h.mu.RUnlock()
	return aclAllowed(node, h.publisherNode, topic, write)
}

func (h *brokerAuthHook) OnDisconnect(cl *mqttbroker.Client, _ error, _ bool) {
	h.mu.Lock()
	delete(h.clients, cl.ID)
	h.mu.Unlock()
}

// Revoke marks a node revoked (future connects refused). Pair with active
// disconnect of live sessions for immediate cut-off.
func (h *brokerAuthHook) Revoke(node string) { h.revoc.Revoke(node) }

// clientsForNode returns the mqtt client-ids currently authenticated as node
// (used by the revoke command to actively disconnect live sessions).
func (h *brokerAuthHook) clientsForNode(node string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var ids []string
	for cid, n := range h.clients {
		if n == node {
			ids = append(ids, cid)
		}
	}
	return ids
}

// revocation is a shared denylist used by both the enroll key store (refuse new
// enrollments) and the broker auth hook (refuse new connects).
type revocation struct {
	mu  sync.RWMutex
	set map[string]bool
}

func newRevocation() *revocation { return &revocation{set: map[string]bool{}} }

func (r *revocation) Revoke(node string) {
	r.mu.Lock()
	r.set[node] = true
	r.mu.Unlock()
}

func (r *revocation) IsRevoked(node string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.set[node]
}

// derivedEnrollStore implements credential.EnrollKeyStore by deriving each
// node's enrollment key as HMAC-SHA256(secret, node). This gives per-node keys
// (granular revocation via the shared denylist) with ZERO key distribution —
// both sides derive the same key from the one shared secret. "publisher" is
// reserved (the publisher mints its own token directly; nobody may enroll as it).
type derivedEnrollStore struct {
	secret []byte
	revoc  *revocation
}

func (d *derivedEnrollStore) Key(node string) ([]byte, bool) {
	if node == "" || node == "publisher" || d.revoc.IsRevoked(node) {
		return nil, false
	}
	mac := hmac.New(sha256.New, d.secret)
	mac.Write([]byte(node))
	return mac.Sum(nil), true
}

// randNonce returns a random 128-bit hex nonce (token jti / uniqueness).
func randNonce() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
