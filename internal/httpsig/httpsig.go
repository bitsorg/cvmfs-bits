// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Package httpsig implements the body-bound request signing used between build
// runners and the cvmfs-prepub API (ADR-0008 D3, option T1).
//
// # Why not just send the token
//
// A bearer token must TRAVEL to be used, so anyone who observes one request —
// a packet capture left running, a proxy log, a mirrored switch port — holds
// publish rights to a production repository until the token is rotated, and
// nothing in the next request tells the server the difference. Signing instead
// keeps the shared secret on both ends and puts only a per-request MAC on the
// wire. An observer learns a signature that is bound to one request, is useless
// for any other, and expires.
//
// # What this does NOT provide
//
// Confidentiality (the payload is still readable) and server authenticity (an
// on-path attacker can still forge a RESPONSE, e.g. a fake job_id). Those need
// transport encryption; this composes with TLS or WireGuard rather than
// replacing them. See ADR-0008 D3.
//
// # The scheme
//
//	X-Bits-Auth: v1 key_id=<id> ts=<unix> nonce=<hex> fd=<hex> bh=<hex> mac=<hex>
//
//	mac = HMAC-SHA256(secret, canonical) where canonical is
//
//	    bits-hmac-v1 \n
//	    <METHOD>      \n
//	    <request URI> \n      path AND query — an unsigned query string is a
//	                          way to set fields the server reads
//	    <fd>          \n      digest of every non-payload form field
//	    <bh>          \n      SHA-256 of the payload, or "-" when there is none
//	    <ts>          \n
//	    <nonce>
//
// Binding is in two stages, because the payload is a multi-gigabyte stream the
// server deliberately does not buffer. `fd` and `bh` are carried IN the header
// and covered by the MAC, so the signature can be checked before a byte of body
// is read; the handler then recomputes the field digest from the fields it
// actually parsed and verifies the payload against `bh`. A signature that
// verifies therefore commits the client to exactly the fields and bytes the
// server went on to act upon — provided the caller performs both stages.
//
// Everything that changes the effect of a request must be inside `fd`, which is
// why it digests ALL non-payload fields rather than a hand-picked list: a field
// added later is bound automatically instead of silently becoming unauthenticated.
package httpsig

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// HeaderName is the request header carrying the signature.
const HeaderName = "X-Bits-Auth"

// Scheme is the version prefix. It is part of the signed canonical string, so
// a future v2 cannot be down-negotiated to v1 by an attacker rewriting the
// header: the MAC would not verify.
const Scheme = "v1"

const canonicalPrefix = "bits-hmac-v1"

// NoBody is the placeholder used for `bh` when a request carries no payload.
const NoBody = "-"

// NoFields is the digest of an empty field set — SHA-256 of the empty string.
// JSON requests use it: their body is small enough to hash whole, so `bh`
// covers everything and there is nothing left for `fd` to bind. Only the
// streamed multipart submission needs the two-part split.
//
// A constant rather than FieldsDigest(nil): this value is the sole gate on the
// JSON binding path, and a package-level var could be reassigned by anything in
// the process. TestNoFieldsMatchesDigest keeps it honest.
const NoFields = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// BodyDigest is the hex SHA-256 of a request body, for the JSON endpoints
// where the whole body is read into memory anyway.
func BodyDigest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// DefaultSkew bounds how far a request's timestamp may be from the server's
// clock in either direction. Two minutes tolerates ordinary NTP drift between a
// runner and the service while keeping the replay window short.
const DefaultSkew = 2 * time.Minute

// maxFutureSkew is how far AHEAD of the server a client's clock may be. Small
// on purpose: see Verify.
const maxFutureSkew = 15 * time.Second

var (
	// ErrMissing indicates no signature header was present.
	ErrMissing = errors.New("no signature header")
	// ErrMalformed indicates the header could not be parsed.
	ErrMalformed = errors.New("malformed signature header")
	// ErrUnsupportedScheme indicates a version this build does not implement.
	ErrUnsupportedScheme = errors.New("unsupported signature scheme")
	// ErrExpired indicates the timestamp is outside the accepted skew window.
	ErrExpired = errors.New("signature timestamp outside the accepted window")
	// ErrReplay indicates the nonce has already been used.
	ErrReplay = errors.New("signature nonce already used")
	// ErrBadMAC indicates the signature did not verify.
	ErrBadMAC = errors.New("signature does not verify")
	// ErrBindingMismatch indicates the request's actual fields or payload do
	// not match what the signature committed to.
	ErrBindingMismatch = errors.New("request does not match its signature")
)

// Signature is a parsed X-Bits-Auth header.
type Signature struct {
	KeyID      string
	Timestamp  time.Time
	Nonce      string
	FieldsHash string // hex; digest of the non-payload fields
	BodyHash   string // hex SHA-256 of the payload, or NoBody
	MAC        string // hex
}

// FieldsDigest returns the digest that `fd` must carry for a given set of
// non-payload fields.
//
// Encoding is length-prefixed — "<len(k)>:<k>=<len(v)>:<v>\n" per field, sorted
// by key — so that no combination of separators inside a key or value can make
// two different field sets produce the same digest. A tag description
// containing a newline, or a value containing "=", is unambiguous. That
// property is the whole point: canonicalisation ambiguity is the classic way
// request-signing schemes get broken.
func FieldsDigest(fields map[string]string) string {
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := sha256.New()
	for _, k := range keys {
		v := fields[k]
		fmt.Fprintf(h, "%d:%s=%d:%s\n", len(k), k, len(v), v)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Canonical builds the string that is MAC'd. Exported so that a client in
// another language (the CI signer) can be tested against it.
//
// `uri` must be the full request URI including any query string
// (http.Request.URL.RequestURI()), never just the path: a handler that reads a
// query parameter would otherwise honour one an attacker appended to a
// captured request, with the MAC still verifying.
func Canonical(method, uri, fieldsHash, bodyHash string, ts int64, nonce string) string {
	return strings.Join([]string{
		canonicalPrefix,
		strings.ToUpper(method),
		uri,
		fieldsHash,
		bodyHash,
		strconv.FormatInt(ts, 10),
		nonce,
	}, "\n")
}

// Sign produces an X-Bits-Auth header value.
func Sign(secret []byte, keyID, method, uri, fieldsHash, bodyHash string, ts time.Time, nonce string) string {
	if bodyHash == "" {
		bodyHash = NoBody
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(Canonical(method, uri, fieldsHash, bodyHash, ts.Unix(), nonce)))
	return fmt.Sprintf("%s key_id=%s ts=%d nonce=%s fd=%s bh=%s mac=%s",
		Scheme, keyID, ts.Unix(), nonce, fieldsHash, bodyHash,
		hex.EncodeToString(mac.Sum(nil)))
}

// Parse reads an X-Bits-Auth header value.
func Parse(header string) (*Signature, error) {
	header = strings.TrimSpace(header)
	if header == "" {
		return nil, ErrMissing
	}
	scheme, rest, ok := strings.Cut(header, " ")
	if !ok {
		return nil, ErrMalformed
	}
	if scheme != Scheme {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedScheme, scheme)
	}

	kv := map[string]string{}
	for _, part := range strings.Fields(rest) {
		k, v, found := strings.Cut(part, "=")
		if !found {
			return nil, ErrMalformed
		}
		if _, dup := kv[k]; dup {
			// A duplicated parameter is the kind of ambiguity that lets a
			// client and a server read the same header differently.
			return nil, fmt.Errorf("%w: duplicate parameter %q", ErrMalformed, k)
		}
		kv[k] = v
	}

	for _, required := range []string{"key_id", "ts", "nonce", "fd", "bh", "mac"} {
		if kv[required] == "" {
			return nil, fmt.Errorf("%w: missing %s", ErrMalformed, required)
		}
	}
	tsUnix, err := strconv.ParseInt(kv["ts"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: bad ts", ErrMalformed)
	}

	return &Signature{
		KeyID:      kv["key_id"],
		Timestamp:  time.Unix(tsUnix, 0),
		Nonce:      kv["nonce"],
		FieldsHash: kv["fd"],
		BodyHash:   kv["bh"],
		MAC:        kv["mac"],
	}, nil
}

// Verify checks the MAC and the timestamp window. It does NOT check the nonce
// (see NonceCache) or the field/payload binding (see Signature.Bound), because
// those belong to different stages of request handling.
func (s *Signature) Verify(secret []byte, method, uri string, now time.Time, skew time.Duration) error {
	if skew <= 0 {
		skew = DefaultSkew
	}
	// Asymmetric window. A legitimate client's clock is occasionally behind
	// ours and almost never far ahead, so allowing the full skew in both
	// directions would double the replay window (2×skew) for no practical
	// tolerance gain. Future timestamps get a small allowance only.
	d := now.Sub(s.Timestamp)
	switch {
	case d > skew:
		return fmt.Errorf("%w: %s old", ErrExpired, d.Round(time.Second))
	case d < -maxFutureSkew:
		return fmt.Errorf("%w: %s in the future (check the client clock)",
			ErrExpired, (-d).Round(time.Second))
	}

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(Canonical(method, uri, s.FieldsHash, s.BodyHash, s.Timestamp.Unix(), s.Nonce)))
	want := mac.Sum(nil)

	got, err := hex.DecodeString(s.MAC)
	if err != nil {
		return ErrBadMAC
	}
	if subtle.ConstantTimeCompare(got, want) != 1 {
		return ErrBadMAC
	}
	return nil
}

// Bound reports whether the fields and payload the server actually received
// are the ones the signature committed to. Call it once both are known.
//
// bodyHash must be the hex SHA-256 of the payload as received, or NoBody when
// the request carried none. Comparison is case-insensitive on the hex.
func (s *Signature) Bound(fields map[string]string, bodyHash string) error {
	if fd := FieldsDigest(fields); !strings.EqualFold(fd, s.FieldsHash) {
		return fmt.Errorf("%w: form fields differ from the signed set", ErrBindingMismatch)
	}
	if bodyHash == "" {
		bodyHash = NoBody
	}
	if !strings.EqualFold(bodyHash, s.BodyHash) {
		return fmt.Errorf("%w: payload differs from the signed digest", ErrBindingMismatch)
	}
	return nil
}

// cacheKey identifies one signature for replay purposes. The nonce alone would
// do if clients always generated it well; including the MAC means a client with
// a weak nonce source still cannot have two DIFFERENT requests collide.
func (s *Signature) cacheKey() string {
	return s.Nonce + "|" + s.MAC
}
