// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package credential

import (
	"context"
	"net/http"
	"strings"
)

type ctxKey int

const claimsKey ctxKey = 0

// RequireToken returns middleware that admits a request only if it carries a
// valid `Authorization: Bearer <token>` with the required scope. Failures return
// 401 with a WWW-Authenticate challenge and no detail (so the endpoint reveals
// nothing about why). On success the verified Claims are stored in the request
// context (see ClaimsFrom).
//
// A nil Verifier disables the check (the handler is returned unwrapped) so a
// deployment can opt out of data-plane auth without code changes.
func RequireToken(v *Verifier, scope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if v == nil {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tok, ok := bearer(r)
			if !ok {
				unauthorized(w)
				return
			}
			claims, err := v.Verify(tok, scope)
			if err != nil {
				unauthorized(w)
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), claimsKey, claims)))
		})
	}
}

// ClaimsFrom returns the verified claims attached by RequireToken, if any.
func ClaimsFrom(ctx context.Context) (*Claims, bool) {
	c, ok := ctx.Value(claimsKey).(*Claims)
	return c, ok
}

func bearer(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	const pfx = "Bearer "
	if len(h) <= len(pfx) || !strings.EqualFold(h[:len(pfx)], pfx) {
		return "", false
	}
	return strings.TrimSpace(h[len(pfx):]), true
}

func unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="cvmfs-distribute"`)
	http.Error(w, "unauthorized", http.StatusUnauthorized)
}
