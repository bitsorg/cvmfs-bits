// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package serve

import (
	"context"
	"net/http"
	"time"

	"cvmfs.io/prepub/internal/distribute/manifest"
)

// DiffSource computes the cumulative set of CAS objects added to a repository
// between two root-catalog states (ADR-0001 D4). It streams each object to emit
// so an arbitrarily large catch-up never materialises the whole set on Stratum 0.
// In production it is backed by `cvmfs_server diff` (or a catalog walk) over the
// two roots; it is an interface so the HTTP layer is testable without cvmfs.
//
// fromRoot may be empty, meaning "from the beginning" (a cold-start receiver):
// the implementation then emits every object reachable from toRoot.
type DiffSource interface {
	Diff(ctx context.Context, repo, fromRoot, toRoot string, emit func(manifest.ObjRef) error) error
}

// CatchupHandler serves the cumulative catch-up manifest (ADR-0001 D4 / P4):
//
//	GET /s1/catchup?repo={repo}&to={root}[&from={root}]
//
// It streams an NDJSON diff manifest — a header line followed by one object per
// line — describing every object a receiver currently at root `from` must fetch
// to reach root `to`. `to` is required (the receiver learns it from the published
// notification or .cvmfspublished); `from` is the receiver's persisted
// last-synced root and may be empty for a cold start.
//
// The response is always NDJSON and is generated on the fly, so neither side
// buffers the whole set. Because the 200 status and header are committed before
// the diff runs, completion is signalled with the X-Catchup-Complete HTTP
// trailer: a receiver MUST treat a missing or non-"1" trailer as a truncated
// stream and NOT advance its last-synced root.
type CatchupHandler struct {
	Source   DiffSource
	BaseURLs []string      // object base URL(s) advertised to receivers
	Auth     manifest.Auth // object-channel auth policy (default public)
}

func (h *CatchupHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.Source == nil || len(h.BaseURLs) == 0 {
		http.Error(w, "catch-up not configured", http.StatusServiceUnavailable)
		return
	}
	q := r.URL.Query()
	repo := q.Get("repo")
	to := q.Get("to")
	from := q.Get("from")
	// repo flows into the (possibly cvmfs_server-backed) diff source; constrain it
	// to a safe repository-name charset so it can never carry shell metacharacters
	// or path traversal, regardless of the DiffSource implementation.
	if !validRepoName(repo) {
		http.Error(w, "missing or invalid repo", http.StatusBadRequest)
		return
	}
	// from/to are catalog root hashes that flow into the (possibly shell-backed)
	// diff source; constrain them to safe object-name characters so they can never
	// carry path-traversal or argument-injection payloads.
	if to == "" || !validObjectName(to) {
		http.Error(w, "missing or invalid 'to' root", http.StatusBadRequest)
		return
	}
	if from != "" && !validObjectName(from) {
		http.Error(w, "invalid 'from' root", http.StatusBadRequest)
		return
	}

	auth := h.Auth
	if auth == "" {
		auth = manifest.AuthPublic
	}
	header := &manifest.Manifest{
		TransactionID:  "catchup-" + to,
		Repo:           repo,
		BaseRootHash:   from,
		TargetRootHash: to,
		BaseURLs:       h.BaseURLs,
		Generator:      manifest.GeneratorDiff,
		Auth:           auth,
		CreatedAt:      time.Now().UTC(),
	}

	// Declare the completion trailer before writing the body (Go sends declared
	// trailers after the streamed body).
	w.Header().Set("Trailer", "X-Catchup-Complete")
	w.Header().Set("Content-Type", "application/x-ndjson")

	if err := manifest.EncodeNDJSONHeader(w, header); err != nil {
		// Nothing streamed yet beyond (maybe) part of the header; signal incomplete.
		w.Header().Set("X-Catchup-Complete", "0")
		return
	}
	emit := func(o manifest.ObjRef) error { return manifest.EncodeNDJSONObject(w, &o) }
	if err := h.Source.Diff(r.Context(), repo, from, to, emit); err != nil {
		// 200 + header already committed; the only way to signal failure is the
		// trailer. The receiver will see Complete != "1" and discard the catch-up.
		w.Header().Set("X-Catchup-Complete", "0")
		return
	}
	w.Header().Set("X-Catchup-Complete", "1")
}

// validRepoName reports whether s is a safe CVMFS repository name: non-empty and
// composed only of alphanumerics and the punctuation legal in a repo fqrn
// ('.', '-', '_'). It excludes path separators and shell metacharacters so the
// name can be passed to a diff backend (including a shell-invoking one) safely.
func validRepoName(s string) bool {
	if s == "" || len(s) > 255 {
		return false
	}
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z',
			c == '.', c == '-', c == '_':
		default:
			return false
		}
	}
	return true
}
