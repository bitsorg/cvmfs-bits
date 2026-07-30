// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

// Request authentication for the publisher API.
//
// Two credentials are understood, and which are accepted is deployment policy
// (ADR-0008 D3):
//
//   - a bearer token, which must travel on every request, so observing one
//     request yields publish rights until the token is rotated;
//   - an HMAC signature over a canonical form of the request, which keeps the
//     shared secret on both ends and puts only a per-request, expiring,
//     single-use MAC on the wire.
//
// The signature is verified in two stages because the payload is a stream the
// server deliberately does not buffer: this middleware checks the MAC, the
// clock window and the nonce before any body is read, and submitJob later
// confirms that the fields it parsed and the bytes it stored are the ones the
// signature committed to. Both halves are required; neither alone binds the
// request.

import (
	"bytes"
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"cvmfs.io/prepub/internal/httpsig"
)

// AuthMode selects which credentials the API accepts.
type AuthMode string

const (
	// AuthBearer accepts only the legacy bearer token.
	AuthBearer AuthMode = "bearer"
	// AuthBoth accepts either. This is the migration setting: it lets signed
	// and unsigned publishers coexist while the CI is rolled out.
	AuthBoth AuthMode = "both"
	// AuthHMAC accepts only signed requests, so the shared secret never
	// travels. This is the end state; rotate the token once after switching,
	// since until then it has been on the wire.
	AuthHMAC AuthMode = "hmac"
)

// ParseAuthMode validates a configured mode.
func ParseAuthMode(s string) (AuthMode, error) {
	switch AuthMode(strings.ToLower(strings.TrimSpace(s))) {
	case "", AuthBoth:
		return AuthBoth, nil
	case AuthBearer:
		return AuthBearer, nil
	case AuthHMAC:
		return AuthHMAC, nil
	default:
		return "", fmt.Errorf("unknown auth mode %q (want bearer, both or hmac)", s)
	}
}

// SetAuthMode configures which credentials are accepted. Called at startup.
func (s *Server) SetAuthMode(m AuthMode) { s.authMode = m }

// SetSignatureSkew overrides the accepted clock difference for signed requests.
// Called at startup, before the listener is up.
//
// The replay cache is rebuilt to match. Its retention and the skew are one
// setting wearing two hats: a nonce must be remembered for at least as long as
// a signature bearing it can still be inside the clock window, or the cache
// forgets first and the replay it exists to stop succeeds. Widening the skew
// without widening the retention reopens exactly that gap, silently.
func (s *Server) SetSignatureSkew(d time.Duration) {
	if d <= 0 {
		return
	}
	s.signSkew = d
	if s.stopNonceSweeper != nil {
		s.stopNonceSweeper()
	}
	s.nonces = httpsig.NewNonceCache(2*d, 0)
	s.nonces.SetPressureHook(s.noncePressure)
	s.stopNonceSweeper = s.nonces.StartSweeper()
}

// signatureContextKey is the request-context key under which a verified
// signature is stored for the handler's binding check.
type signatureContextKey struct{}

// withSignature stores a verified signature on the request context.
func withSignature(r *http.Request, sig *httpsig.Signature) context.Context {
	return context.WithValue(r.Context(), signatureContextKey{}, sig)
}

// signatureFrom returns the verified signature for a request, if it was signed.
func signatureFrom(r *http.Request) *httpsig.Signature {
	sig, _ := r.Context().Value(signatureContextKey{}).(*httpsig.Signature)
	return sig
}

func (s *Server) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.apiToken == "" {
			next.ServeHTTP(w, r) // auth disabled (dev)
			return
		}

		if raw := r.Header.Get(httpsig.HeaderName); raw != "" {
			if s.authMode == AuthBearer {
				s.rejectAuth(w, r, "signed requests are not accepted by this deployment (auth_mode=bearer)")
				return
			}
			sig, err := s.verifySignature(r, raw)
			if err != nil {
				s.rejectAuth(w, r, "signature rejected: "+err.Error())
				return
			}
			if err := s.bindNonStreamingBody(r, sig); err != nil {
				s.rejectAuth(w, r, "signature rejected: "+err.Error())
				return
			}
			st := &bindingState{deferred: isStreamingRoute(r)}
			ctx := context.WithValue(withSignature(r, sig), bindingStateKey{}, st)
			// The status is recorded so the backstop below can tell "the
			// handler forgot to bind" from "the handler rejected the request
			// before it got that far", which submitJob does on a dozen ordinary
			// paths — a missing --staging-root, a malformed part, a client that
			// drops mid-upload. Logging those at ERROR would make the warning
			// routine, and a routine warning is one nobody reads.
			sw := &statusWriter{ResponseWriter: w}
			next.ServeHTTP(sw, r.WithContext(ctx))
			// Backstop for the one case the allowlist cannot prevent: a new
			// streaming route that never binds. The response has already been
			// written so this cannot be turned into a 401, but a SUCCESSFUL
			// request that was never bound must not pass silently — the
			// signature authenticated nothing.
			if st.deferred && !st.done && sw.status < 300 {
				s.obs.Logger.Error("BUG: signed request succeeded without binding its body — "+
					"the signature authenticated nothing. Add the binding call to this handler.",
					"method", r.Method, "path", r.URL.Path, "status", sw.status)
			}
			return
		}

		if s.authMode == AuthHMAC {
			s.rejectAuth(w, r, "this deployment requires a signed request ("+
				httpsig.HeaderName+"); a bearer token is no longer accepted")
			return
		}

		authHeader := r.Header.Get("Authorization")
		token := strings.TrimPrefix(authHeader, "Bearer ")
		if token == authHeader || token == "" {
			s.rejectAuth(w, r, "missing or malformed Authorization header")
			return
		}
		if subtle.ConstantTimeCompare([]byte(token), []byte(s.apiToken)) != 1 {
			s.rejectAuth(w, r, "invalid token")
			return
		}

		next.ServeHTTP(w, r)
	})
}

// verifySignature performs the pre-body half of the check: the MAC itself, the
// clock window, and single use of the nonce.
//
// Order matters. The nonce is consumed only AFTER the MAC verifies, so an
// unauthenticated caller cannot fill the replay cache with nonces it never had
// to sign — which would otherwise be a cheap way to push the cache to its cap
// and, once there, start getting legitimate requests rejected.
func (s *Server) verifySignature(r *http.Request, raw string) (*httpsig.Signature, error) {
	sig, err := httpsig.Parse(raw)
	if err != nil {
		return nil, err
	}
	// Only one key is configured today; key_id exists so that rotation can
	// introduce a second without a flag day. Reject an unknown one explicitly
	// rather than silently trying the only key we have.
	if sig.KeyID != s.signingKeyID() {
		return nil, fmt.Errorf("unknown key_id %q", sig.KeyID)
	}
	// RequestURI, not Path: the query string is part of what the client signed,
	// so appending one to a captured request breaks the MAC instead of quietly
	// changing what the handler does.
	if err := sig.Verify([]byte(s.apiToken), r.Method, r.URL.RequestURI(), time.Now(), s.signSkew); err != nil {
		return nil, err
	}
	if err := s.nonces.Use(sig, time.Now()); err != nil {
		return nil, err
	}
	return sig, nil
}

// bindNonStreamingBody buffers and binds the body of a signed request that is
// NOT a streamed upload, then restores it for the handler.
//
// Doing this in the middleware rather than in each handler is the point: a
// handler that forgets the binding check does not become "less strict", it
// becomes unauthenticated, and forgetting is silent. Every route except the
// multipart submission has a small body, so the server can hash the whole thing
// here and no handler has to remember anything. The multipart route is the one
// exception — its body is the multi-gigabyte tar the server deliberately
// streams — and it does its own binding in submitJob.
func (s *Server) bindNonStreamingBody(r *http.Request, sig *httpsig.Signature) error {
	if isStreamingRoute(r) {
		return nil // bound by submitJob, which sees the parsed fields and payload
	}
	limit := maxSignedBody(r)
	raw, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	r.Body.Close()
	if err != nil {
		return fmt.Errorf("reading request body: %w", err)
	}
	if int64(len(raw)) > limit {
		return fmt.Errorf("request body exceeds %d bytes", limit)
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	r.ContentLength = int64(len(raw))

	if !strings.EqualFold(sig.FieldsHash, httpsig.NoFields) {
		return fmt.Errorf("%w: a non-multipart request binds its whole body, not a field set",
			httpsig.ErrBindingMismatch)
	}
	// A GET carries nothing to hash, so its signature says "no body" rather
	// than the digest of the empty string. Accept that ONLY when there really
	// is no body: a non-empty body can never digest to the marker, so this
	// cannot be used to attach a payload to a request signed without one.
	if len(raw) == 0 && strings.EqualFold(sig.BodyHash, httpsig.NoBody) {
		return nil
	}
	if !strings.EqualFold(sig.BodyHash, httpsig.BodyDigest(raw)) {
		return fmt.Errorf("%w: request body differs from the signed digest",
			httpsig.ErrBindingMismatch)
	}
	return nil
}

// requireSignedJSONBody binds a signed request on a STREAMING route whose body
// turned out to be small JSON after all — the tar_path submission, which shares
// POST /api/v1/jobs with the multipart upload. The middleware deferred binding
// for the whole route, so this branch has to do it.
func requireSignedJSONBody(r *http.Request, raw []byte) error {
	sig := signatureFrom(r)
	if sig == nil {
		return nil
	}
	if !strings.EqualFold(sig.FieldsHash, httpsig.NoFields) {
		return fmt.Errorf("%w: a JSON submission binds its whole body, not a field set",
			httpsig.ErrBindingMismatch)
	}
	if !strings.EqualFold(sig.BodyHash, httpsig.BodyDigest(raw)) {
		return fmt.Errorf("%w: request body differs from the signed digest",
			httpsig.ErrBindingMismatch)
	}
	if st := bindingStateFrom(r); st != nil {
		st.done = true
	}
	return nil
}

// maxSignedBodySize caps a buffered, signed non-streaming body. Almost every
// such endpoint takes a small JSON document; the largest is a job submission by
// tar_path.
const maxSignedBodySize = 1 << 20

// maxSignedManifestSize is the cap for the distribution manifest ingest, whose
// handler accepts up to 256 MiB of NDJSON — a manifest for a large build runs
// to thousands of object references and is nowhere near "a small JSON
// document". Binding it means buffering it, and buffering 256 MiB per request
// would be a fine denial of service if anyone could ask for it; only a holder
// of the signing key can, because the MAC is verified first, and such a caller
// can already submit a 10 GiB tar. Left at the handler's own limit so that a
// manifest which the handler would accept is never refused by the auth layer
// instead — which is what happened before, as an unexplained 401.
const maxSignedManifestSize = 256 << 20

// maxSignedBody returns the buffered-body cap for a route.
func maxSignedBody(r *http.Request) int64 {
	if strings.HasPrefix(r.URL.Path, "/api/v1/distribute/manifests") {
		return maxSignedManifestSize
	}
	return maxSignedBodySize
}

// isStreamingRoute reports whether a request goes to the one route whose body
// the server refuses to buffer — the job submission, whose payload is a
// multi-gigabyte tar — and which therefore binds its own body in the handler.
//
// Every other authenticated route, including the distribution manifest ingest
// (which is large but bounded — see maxSignedManifestSize), is buffered and
// bound by the middleware.
//
// This is deliberately a METHOD+PATH allowlist and not a Content-Type test.
// An earlier version skipped the middleware binding for anything whose
// Content-Type began with "multipart/", which is a client-supplied header that
// is NOT part of the canonical string: rewriting it on a captured request made
// the middleware skip its binding while the handler (which only binds on the
// submit route) never ran one, leaving the body entirely unauthenticated with
// the MAC still verifying. The exemption must derive from something the server
// decides, i.e. which handler is about to run.
//
// Adding another streaming route means adding it here AND binding it in its
// handler; the deferred-binding check below shouts if only the first is done.
func isStreamingRoute(r *http.Request) bool {
	return r.Method == http.MethodPost && r.URL.Path == "/api/v1/jobs"
}

// statusWriter records the status code so the deferred-binding backstop can
// distinguish a handler that forgot to bind from one that rejected the request.
// It deliberately implements nothing else: it wraps only the streaming submit
// route, which neither flushes nor hijacks.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK // implicit 200 on first write
	}
	return w.ResponseWriter.Write(b)
}

// bindingState tracks, for a signed request whose binding was deferred to the
// handler, whether the handler actually performed it.
type bindingState struct{ deferred, done bool }

type bindingStateKey struct{}

func bindingStateFrom(r *http.Request) *bindingState {
	st, _ := r.Context().Value(bindingStateKey{}).(*bindingState)
	return st
}

// signingKeyID is the key identifier this deployment expects. A fixed value is
// enough while there is one shared secret; the field exists so that adding a
// second key later does not change the wire format.
func (s *Server) signingKeyID() string { return "prepub" }

// rejectAuth logs and returns 401 with a body that says what to fix. The
// distinction between "no credential", "wrong credential" and "wrong KIND of
// credential" is useful to an operator and useless to an attacker, who can
// determine it by trying anyway.
func (s *Server) rejectAuth(w http.ResponseWriter, r *http.Request, reason string) {
	s.obs.Logger.Warn("rejected unauthenticated request",
		"remote_addr", r.RemoteAddr,
		"method", r.Method,
		"path", r.URL.Path,
		"reason", reason,
	)
	// http.Error would label this text/plain, so every client that parses the
	// error field has to sniff the body instead of trusting the header.
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusUnauthorized)
	fmt.Fprintf(w, "{\"error\":%q}\n", reason)
}

// requireSignatureBinding is the post-parse half of the check: the fields the
// handler parsed, and the payload it stored, must be the ones the signature
// committed to. Unsigned requests pass through unchanged.
//
// Without this, a signature would attest only to a header an attacker could
// keep while replacing the entire body — so a handler that forgets to call it
// is not "less strict", it is unauthenticated.
func requireSignatureBinding(r *http.Request, fields map[string]string, bodyHash string) error {
	sig := signatureFrom(r)
	if sig == nil {
		return nil
	}
	if err := sig.Bound(fields, bodyHash); err != nil {
		return err
	}
	if st := bindingStateFrom(r); st != nil {
		st.done = true
	}
	return nil
}

var errSignedWithoutDigest = errors.New(
	"a signed submission must carry tar_sha256 so the signature binds the payload")

// noncePressure is called when the replay cache passes its high-water mark.
//
// The cache fails CLOSED at its cap — a request whose nonce cannot be
// remembered is refused, because admitting it would silently stop preventing
// replays. That is the right call and a bad first symptom: the operator would
// see legitimate publishers getting 401s with no warning. So the approach is
// announced while raising the cap is still a calm decision.
//
// Reaching it is not normal. A 100-package build makes a few hundred requests
// over its lifetime; filling 50 000 entries inside the retention window means
// either a much larger fleet than this cap was sized for, or someone minting
// nonces with a secret they should not have.
func (s *Server) noncePressure(entries, maxSize int) {
	s.obs.Logger.Warn("replay cache is filling up; at capacity, signed requests are REFUSED",
		"entries", entries, "max", maxSize,
		"retention", (2 * s.signSkew).String(),
		"action", "raise the cap, shorten the signature skew, or find out who is minting nonces")
}
