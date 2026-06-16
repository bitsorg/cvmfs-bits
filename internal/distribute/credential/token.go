// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Package credential implements the data-plane authentication of ADR-0001: a
// Stratum 1 enrols with the publisher over the (mutually authenticated) control
// plane using an out-of-band per-node key, and in return receives a short-lived,
// scoped bearer token that it presents on critical data-plane endpoints such as
// GET /s1/catchup. The token is a stateless HMAC-signed assertion, so the data
// plane verifies it without a database lookup or shared session state.
//
// Token wire format (compact, URL-safe):
//
//	base64url(payload) "." base64url(HMAC-SHA256(secret, base64url(payload)))
//
// where payload is the JSON Claims. The signature covers the already-encoded
// payload string, so verification re-encodes nothing and is unambiguous.
package credential

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Claims are the assertions carried by a token.
type Claims struct {
	Node  string `json:"node"`  // the Stratum 1 node the token was issued to
	Scope string `json:"scope"` // capability, e.g. "catchup"
	Exp   int64  `json:"exp"`   // expiry, unix seconds
	Nonce string `json:"jti"`   // unique id (binds the token to one issuance)
}

// ErrToken is returned (wrapped) for every verification failure. Callers should
// treat any non-nil error as "reject" and not distinguish causes to a client.
var ErrToken = errors.New("credential: invalid token")

var b64 = base64.RawURLEncoding

// Minter issues tokens signed with a server secret. The same secret backs the
// Verifier on the data plane; keep it on Stratum 0 only.
type Minter struct {
	secret []byte
}

// NewMinter returns a Minter. The secret must be high-entropy (>= 32 bytes
// recommended) and is never transmitted.
func NewMinter(secret []byte) *Minter { return &Minter{secret: append([]byte(nil), secret...)} }

// Mint returns a signed token for node with the given scope and TTL, plus its
// expiry. nonce uniquely identifies the issuance (use a random value).
func (m *Minter) Mint(node, scope, nonce string, ttl time.Duration) (string, time.Time, error) {
	if node == "" || scope == "" {
		return "", time.Time{}, fmt.Errorf("credential: node and scope are required")
	}
	exp := time.Now().Add(ttl)
	payload, err := json.Marshal(Claims{Node: node, Scope: scope, Exp: exp.Unix(), Nonce: nonce})
	if err != nil {
		return "", time.Time{}, err
	}
	p := b64.EncodeToString(payload)
	sig := b64.EncodeToString(sign(m.secret, p))
	return p + "." + sig, exp, nil
}

// Verifier validates tokens minted with the matching secret.
type Verifier struct {
	secret []byte
	leeway time.Duration // clock-skew tolerance on expiry
}

// NewVerifier returns a Verifier with a 30s clock-skew leeway.
func NewVerifier(secret []byte) *Verifier {
	return &Verifier{secret: append([]byte(nil), secret...), leeway: 30 * time.Second}
}

// Verify checks the signature and expiry and, when requiredScope is non-empty,
// that the token carries that scope. It returns the claims on success. The
// signature is checked with a constant-time comparison BEFORE the payload is
// parsed, so a forged token is rejected without interpreting attacker-controlled
// fields.
func (v *Verifier) Verify(token, requiredScope string) (*Claims, error) {
	dot := strings.IndexByte(token, '.')
	if dot <= 0 || dot == len(token)-1 {
		return nil, fmt.Errorf("%w: malformed", ErrToken)
	}
	p, sigStr := token[:dot], token[dot+1:]
	sig, err := b64.DecodeString(sigStr)
	if err != nil {
		return nil, fmt.Errorf("%w: bad signature encoding", ErrToken)
	}
	if !hmac.Equal(sig, sign(v.secret, p)) {
		return nil, fmt.Errorf("%w: signature mismatch", ErrToken)
	}
	payload, err := b64.DecodeString(p)
	if err != nil {
		return nil, fmt.Errorf("%w: bad payload encoding", ErrToken)
	}
	var c Claims
	if err := json.Unmarshal(payload, &c); err != nil {
		return nil, fmt.Errorf("%w: bad payload", ErrToken)
	}
	if time.Now().After(time.Unix(c.Exp, 0).Add(v.leeway)) {
		return nil, fmt.Errorf("%w: expired", ErrToken)
	}
	if requiredScope != "" && c.Scope != requiredScope {
		return nil, fmt.Errorf("%w: wrong scope %q", ErrToken, c.Scope)
	}
	return &c, nil
}

func sign(secret []byte, msg string) []byte {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(msg))
	return mac.Sum(nil)
}
