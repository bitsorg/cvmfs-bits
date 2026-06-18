// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package credential

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// EnrollKeyStore resolves a node's out-of-band enrollment key — the per-node
// secret an operator provisions on Stratum 0 (and hands to the S1 admin over a
// separate channel) when the node joins the distribution network. Lookups for an
// unknown node return ok=false; the server then fails enrollment with a generic
// error so the endpoint cannot be used to enumerate enrolled nodes.
type EnrollKeyStore interface {
	Key(node string) (key []byte, ok bool)
}

// MapEnrollStore is an in-memory EnrollKeyStore (operator-provisioned at startup).
type MapEnrollStore struct {
	mu   sync.RWMutex
	keys map[string][]byte
}

// NewMapEnrollStore returns an empty store.
func NewMapEnrollStore() *MapEnrollStore { return &MapEnrollStore{keys: map[string][]byte{}} }

// Add provisions node's enrollment key.
func (s *MapEnrollStore) Add(node string, key []byte) {
	s.mu.Lock()
	s.keys[node] = append([]byte(nil), key...)
	s.mu.Unlock()
}

func (s *MapEnrollStore) Key(node string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	k, ok := s.keys[node]
	return k, ok
}

// EnrollServer runs the two-step challenge–response enrollment and mints scoped
// tokens (ADR-0001 data-plane auth):
//
//	GET  /control/challenge?node={id}   → {"nonce": "..."}
//	POST /control/enroll  {node,nonce,mac=hex(HMAC-SHA256(enrollKey, node|nonce))}
//	                                    → {"token": "...", "exp_unix": N, "scope": "..."}
//
// The MAC proves the node holds the enrollment key without ever transmitting it,
// and the server-issued one-time nonce makes a captured MAC unreplayable. On
// success the node receives a short-lived bearer token (default scope "catchup")
// to present on the data plane.
type EnrollServer struct {
	Keys     EnrollKeyStore
	Minter   *Minter
	Scope    string        // token scope to grant (default "catchup")
	TokenTTL time.Duration // token lifetime (default 10m)
	NonceTTL time.Duration // challenge lifetime (default 2m)

	log func(string, ...any)

	// nonceKey signs the stateless challenge nonce (HMAC), generated per process.
	// The server stores NO nonce, so flooding /control/challenge costs only an
	// HMAC and zero memory (R-DoS).
	nonceKey []byte

	mu            sync.Mutex
	consumed      map[string]struct{} // recently-redeemed nonces (replay guard)
	consumedOrder []string            // FIFO for bounded eviction
}

// NewEnrollServer builds an EnrollServer. log may be nil.
func NewEnrollServer(keys EnrollKeyStore, minter *Minter, log func(string, ...any)) *EnrollServer {
	nk := make([]byte, 32)
	_, _ = rand.Read(nk)
	return &EnrollServer{
		Keys:     keys,
		Minter:   minter,
		Scope:    "catchup",
		TokenTTL: 10 * time.Minute,
		NonceTTL: 2 * time.Minute,
		log:      log,
		nonceKey: nk,
		consumed: map[string]struct{}{},
	}
}

func (s *EnrollServer) scope() string {
	if s.Scope != "" {
		return s.Scope
	}
	return "catchup"
}

func (s *EnrollServer) tokenTTL() time.Duration {
	if s.TokenTTL > 0 {
		return s.TokenTTL
	}
	return 10 * time.Minute
}

func (s *EnrollServer) nonceTTL() time.Duration {
	if s.NonceTTL > 0 {
		return s.NonceTTL
	}
	return 2 * time.Minute
}

// Handler returns the HTTP handler exposing the challenge and enroll endpoints.
func (s *EnrollServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/control/challenge", s.serveChallenge)
	mux.HandleFunc("/control/enroll", s.serveEnroll)
	return mux
}

type challengeResp struct {
	Nonce string `json:"nonce"`
}

func (s *EnrollServer) serveChallenge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	node := r.URL.Query().Get("node")
	if node == "" {
		http.Error(w, "missing node", http.StatusBadRequest)
		return
	}
	// A challenge is issued for ANY node id (no existence check) so the endpoint
	// does not reveal which nodes are enrolled; enrollment still fails later
	// unless the MAC matches a provisioned key.
	writeJSON(w, http.StatusOK, challengeResp{Nonce: s.makeNonce(node, time.Now())})
}

type enrollReq struct {
	Node  string `json:"node"`
	Nonce string `json:"nonce"`
	MAC   string `json:"mac"` // hex(HMAC-SHA256(enrollKey, node|nonce))
}

// EnrollResp is the token grant returned to a successfully-enrolled node.
type EnrollResp struct {
	Token   string `json:"token"`
	ExpUnix int64  `json:"exp_unix"`
	Scope   string `json:"scope"`
}

func (s *EnrollServer) serveEnroll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req enrollReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.Node == "" || req.Nonce == "" || req.MAC == "" {
		http.Error(w, "missing fields", http.StatusBadRequest)
		return
	}

	// Verify the stateless nonce (HMAC over node+timestamp) is authentic and
	// unexpired before doing any work.
	if !s.checkNonce(req.Node, req.Nonce, time.Now()) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	key, known := s.Keys.Key(req.Node)
	gotMAC, decErr := hex.DecodeString(req.MAC)
	// Always compute an expected MAC (even for an unknown node, against a throwaway
	// key) so the response time does not reveal whether the node is enrolled.
	if !known {
		key = []byte("\x00unknown-node-placeholder-key\x00")
	}
	expected := computeEnrollMAC(key, req.Node, req.Nonce)
	if decErr != nil || !known || !hmac.Equal(gotMAC, expected) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	// Replay guard: an authenticated (nonce, MAC) pair is redeemable once within
	// its window. Only verified enrolls reach this bounded set, so a challenge
	// flood can never grow it.
	if !s.consumeNonce(req.Nonce) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	nonce, err := randomHex(16)
	if err != nil {
		http.Error(w, "entropy failure", http.StatusInternalServerError)
		return
	}
	token, exp, err := s.Minter.Mint(req.Node, s.scope(), nonce, s.tokenTTL())
	if err != nil {
		http.Error(w, "mint failed", http.StatusInternalServerError)
		return
	}
	if s.log != nil {
		s.log("credential: node enrolled", "node", req.Node, "scope", s.scope(), "exp", exp)
	}
	writeJSON(w, http.StatusOK, EnrollResp{Token: token, ExpUnix: exp.Unix(), Scope: s.scope()})
}

const maxConsumedNonces = 8192

// makeNonce returns a stateless, self-authenticating nonce:
// hex(ts8 || HMAC(nonceKey, ts8||node)[:16]). The server stores nothing.
func (s *EnrollServer) makeNonce(node string, t time.Time) string {
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(t.Unix()))
	mac := hmac.New(sha256.New, s.nonceKey)
	mac.Write(ts[:])
	mac.Write([]byte(node))
	return hex.EncodeToString(append(ts[:], mac.Sum(nil)[:16]...))
}

// checkNonce verifies a nonce was issued by this server for node and is fresh.
func (s *EnrollServer) checkNonce(node, nonce string, now time.Time) bool {
	raw, err := hex.DecodeString(nonce)
	if err != nil || len(raw) != 24 {
		return false
	}
	ts, gotMAC := raw[:8], raw[8:24]
	mac := hmac.New(sha256.New, s.nonceKey)
	mac.Write(ts)
	mac.Write([]byte(node))
	if !hmac.Equal(gotMAC, mac.Sum(nil)[:16]) {
		return false
	}
	issued := time.Unix(int64(binary.BigEndian.Uint64(ts)), 0)
	age := now.Sub(issued)
	return age >= -30*time.Second && age <= s.nonceTTL()
}

// consumeNonce records a nonce as redeemed; false if already used. Bounded (FIFO).
func (s *EnrollServer) consumeNonce(nonce string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.consumed[nonce]; dup {
		return false
	}
	if len(s.consumed) >= maxConsumedNonces {
		old := s.consumedOrder[0]
		s.consumedOrder = s.consumedOrder[1:]
		delete(s.consumed, old)
	}
	s.consumed[nonce] = struct{}{}
	s.consumedOrder = append(s.consumedOrder, nonce)
	return true
}

// computeEnrollMAC is the proof a node computes: HMAC-SHA256(enrollKey, node|nonce).
func computeEnrollMAC(key []byte, node, nonce string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(node))
	mac.Write([]byte{'|'})
	mac.Write([]byte(nonce))
	return mac.Sum(nil)
}

// EnrollMACHex is the helper a receiver uses to build the enroll proof.
func EnrollMACHex(key []byte, node, nonce string) string {
	return hex.EncodeToString(computeEnrollMAC(key, node, nonce))
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
