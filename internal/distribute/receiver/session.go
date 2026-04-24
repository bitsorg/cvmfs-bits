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
func (ss *sessionStore) create(payloadID string, ttl time.Duration) *session {
	token := randomToken()
	s := &session{
		token:     token,
		payloadID: payloadID,
		expiresAt: time.Now().Add(ttl),
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.byToken[token] = s
	ss.byPayload[payloadID] = s
	return s
}

// get looks up a session by bearer token.  Returns (session, true) if the token
// exists and has not expired; (nil, false) otherwise.
func (ss *sessionStore) get(token string) (*session, bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	s, ok := ss.byToken[token]
	if !ok || s.expired() {
		return nil, false
	}
	return s, true
}

// getByPayload looks up a live session by payload ID.  Used by the announce
// handler to return the existing session on a duplicate announce without
// creating a new token.
func (ss *sessionStore) getByPayload(payloadID string) (*session, bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	s, ok := ss.byPayload[payloadID]
	if !ok || s.expired() {
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
