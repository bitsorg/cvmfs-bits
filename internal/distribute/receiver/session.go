package receiver

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// session represents an active pre-transfer negotiation between the sender and
// this receiver.  It is created by a successful announce request and authorises
// subsequent object PUTs on the data channel for the duration of its TTL.
type session struct {
	// token is the opaque bearer credential sent in PUT Authorization headers.
	token string
	// payloadID is the sender-supplied job identifier used for idempotent
	// re-announce: a second announce with the same payloadID returns the
	// existing token rather than creating a new session.
	payloadID string
	// expiresAt is the wall-clock time after which the session is invalid.
	expiresAt time.Time
}

// expired returns true if the session TTL has elapsed.
func (s *session) expired() bool {
	return time.Now().After(s.expiresAt)
}

// maxSessions is the hard upper limit on concurrent live sessions.  This caps
// memory use on receivers that handle many concurrent payloads and prevents a
// compromised sender (one that knows the HMAC secret) from exhausting heap by
// flooding the announce endpoint with unique payloadIDs.
//
// 10,000 sessions at ~250 bytes each ≈ 2.5 MB — well within normal headroom.
const maxSessions = 10_000

// sessionStore is a thread-safe in-memory registry of active sessions.
// It is indexed by both session token and payload ID to support O(1) lookup
// from either direction.
type sessionStore struct {
	mu        sync.Mutex
	byToken   map[string]*session
	byPayload map[string]*session
}

func newSessionStore() *sessionStore {
	return &sessionStore{
		byToken:   make(map[string]*session),
		byPayload: make(map[string]*session),
	}
}

// create generates a new session for payloadID with the given TTL and registers
// it in the store.  The caller must check getByPayload first to ensure
// idempotency.
//
// Returns (session, true) on success, or (nil, false) when the store is at
// capacity even after pruning expired entries.  The caller should respond with
// HTTP 429 in the false case.
func (ss *sessionStore) create(payloadID string, ttl time.Duration) (*session, bool) {
	token := randomToken()
	s := &session{
		token:     token,
		payloadID: payloadID,
		expiresAt: time.Now().Add(ttl),
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()

	// Enforce the capacity limit.  On first breach, purge all expired entries
	// inline (same work the background cleanup would do) before giving up.
	if len(ss.byToken) >= maxSessions {
		now := time.Now()
		for tok, existing := range ss.byToken {
			if now.After(existing.expiresAt) {
				delete(ss.byToken, tok)
				delete(ss.byPayload, existing.payloadID)
			}
		}
		// If still at capacity after pruning, reject the request.
		if len(ss.byToken) >= maxSessions {
			return nil, false
		}
	}

	ss.byToken[token] = s
	ss.byPayload[payloadID] = s
	return s, true
}

// get looks up a session by bearer token.  Returns (session, true) if the token
// exists and has not expired; (nil, false) otherwise.
// Expired entries are removed inline to prevent unbounded accumulation between
// periodic cleanup sweeps (important when TTL is long or announce rate is high).
func (ss *sessionStore) get(token string) (*session, bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	s, ok := ss.byToken[token]
	if !ok {
		return nil, false
	}
	if s.expired() {
		delete(ss.byToken, token)
		delete(ss.byPayload, s.payloadID)
		return nil, false
	}
	return s, true
}

// getByPayload looks up a live session by payload ID.  Used by the announce
// handler to return the existing session on a duplicate announce without
// creating a new token.
// Expired entries are removed inline (same rationale as get).
func (ss *sessionStore) getByPayload(payloadID string) (*session, bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	s, ok := ss.byPayload[payloadID]
	if !ok {
		return nil, false
	}
	if s.expired() {
		delete(ss.byToken, s.token)
		delete(ss.byPayload, payloadID)
		return nil, false
	}
	return s, true
}

// cleanup removes all expired sessions from both indexes.  It should be called
// periodically (e.g. every minute) by a background goroutine to bound memory
// use on receivers that handle many payloads.
func (ss *sessionStore) cleanup() {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	for token, s := range ss.byToken {
		if s.expired() {
			delete(ss.byToken, token)
			delete(ss.byPayload, s.payloadID)
		}
	}
}

// randomToken generates a 32-byte cryptographically random bearer token
// encoded as a 64-character hex string.
func randomToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		// rand.Read only fails if the OS entropy source is broken, which is a
		// fatal condition for any security-sensitive process.
		panic("receiver: crypto/rand.Read failed: " + err.Error())
	}
	return hex.EncodeToString(b)
}
