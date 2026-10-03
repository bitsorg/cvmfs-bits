// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"

	mqttbroker "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"cvmfs.io/prepub/internal/broker"
	"cvmfs.io/prepub/internal/distribute/credential"
	"cvmfs.io/prepub/pkg/observe"
)

// brokerAuthHook authenticates and authorizes control-plane MQTT clients with
// the credential token scheme — no certificates. A client presents its scoped
// bearer token (obtained via the challenge/response enrollment) as the MQTT
// CONNECT password; the hook verifies the HMAC signature + expiry, records the
// node identity, and enforces per-role topic ACLs. A revocation denylist plus
// active disconnect (in-process broker) gives immediate cut-off.
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
// presence topic — not announce/published, nor another node's presence.
func aclAllowed(node, publisherNode, topic string, write bool) bool {
	if node != "" && node == publisherNode {
		return true
	}
	if !write {
		return true
	}
	if node == "" || broker.ValidateNodeID(node) != nil {
		return false
	}
	return topic == broker.PresenceTopic(node)
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
	// Stash the TOKEN-VERIFIED node on the client connection object itself, so
	// authorization is tied to the connection's own lifetime. This overwrites
	// any client-supplied CONNECT username (which is untrusted) with the node
	// proven by the bearer token, so it cannot be forged. OnACLCheck reads it
	// from here rather than from a separate map, which a reconnect/takeover plus
	// OnDisconnect could clear out from under an inflight QoS-1 publish.
	cl.Properties.Username = []byte(node)
	return true
}

func (h *brokerAuthHook) OnACLCheck(cl *mqttbroker.Client, topic string, write bool) bool {
	// The authenticated node is stored on the connection (set from the verified
	// token in OnConnectAuthenticate), so it is correct for the whole connection
	// lifetime and immune to the clients-map / OnDisconnect reconnect race.
	node := string(cl.Properties.Username)
	return aclAllowed(node, h.publisherNode, topic, write)
}

func (h *brokerAuthHook) OnDisconnect(cl *mqttbroker.Client, _ error, _ bool) {
	h.mu.Lock()
	delete(h.clients, cl.ID)
	h.mu.Unlock()
}

// Revoke marks a node revoked (future connects refused). Pair with active
// disconnect of live sessions for immediate cut-off.
func (h *brokerAuthHook) Revoke(node string) error { return h.revoc.Revoke(node) }

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
// enrollments) and the broker auth hook (refuse new connects). With a path it
// is persisted there, so a revocation survives a publisher restart.
type revocation struct {
	mu     sync.RWMutex
	set    map[string]bool
	path   string       // "" => in memory only
	logger *slog.Logger // nil => slog.Default()
}

func newRevocation() *revocation { return &revocation{set: map[string]bool{}} }

// loadRevocation reads the denylist persisted at path; a missing file is an
// empty list. An unreadable or corrupt file is an error, so startup fails
// closed rather than silently re-admitting revoked nodes.
func loadRevocation(path string) (*revocation, error) {
	r := &revocation{set: map[string]bool{}, path: path}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return r, nil
	}
	if err != nil {
		return nil, err
	}
	var nodes []string
	if err := json.Unmarshal(b, &nodes); err != nil {
		return nil, fmt.Errorf("revocation list %s: %w", path, err)
	}
	for _, n := range nodes {
		r.set[n] = true
	}
	return r, nil
}

// Revoke denies node at once. The in-memory entry is kept even when saving
// fails; the error then means the revocation would not survive a restart.
func (r *revocation) Revoke(node string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.set[node] = true
	return r.saveLocked()
}

// Unrevoke lifts a revocation. When saving fails the node stays revoked (fail
// closed), so memory and disk agree.
func (r *revocation) Unrevoke(node string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.set[node] {
		return nil
	}
	delete(r.set, node)
	if err := r.saveLocked(); err != nil {
		r.set[node] = true
		return err
	}
	return nil
}

// saveLocked persists the list; the caller holds r.mu.
func (r *revocation) saveLocked() error {
	if r.path == "" {
		return nil
	}
	nodes := make([]string, 0, len(r.set))
	for n := range r.set {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	b, err := json.Marshal(nodes)
	if err != nil {
		return err
	}
	logger := r.logger
	if logger == nil {
		logger = slog.Default()
	}
	return writeFileAtomic(r.path, b, logger)
}

// writeFileAtomic writes data to path (mode 0600) via a temp file and rename,
// so a crash leaves either the old or the new list, never a torn one.
func writeFileAtomic(path string, data []byte, logger *slog.Logger) error {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp) // no-op after a successful rename
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	// Make the rename durable. The new list is already in place, so a failure
	// here is only logged: reporting it as an error would claim the save failed.
	if d, err := os.Open(filepath.Dir(path)); err != nil {
		logger.Warn("fsync of directory after rename failed", "path", path, "error", err)
	} else {
		if err := d.Sync(); err != nil {
			logger.Warn("fsync of directory after rename failed", "path", path, "error", err)
		}
		d.Close()
	}
	return nil
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
	return deriveNodeKey(d.secret, node), true
}

// deriveNodeKey derives a node's enrollment key as HMAC-SHA256(secret, node).
// Both the publisher (derivedEnrollStore) and the receiver compute it, so no
// per-node key is ever transmitted.
func deriveNodeKey(secret []byte, node string) []byte {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(node))
	return mac.Sum(nil)
}

// randNonce returns a random 128-bit hex nonce (token jti / uniqueness).
func randNonce() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
