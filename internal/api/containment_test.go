// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestPublishAuthorized exercises the namespace containment predicate directly.
func TestPublishAuthorized(t *testing.T) {
	srv, _, _ := newTestServer(t)

	// No prefixes configured ⇒ check disabled ⇒ everything allowed.
	if !srv.publishAuthorized("repo.cern.ch", "some/other/group/x") {
		t.Fatal("empty allowlist should allow any target")
	}

	srv.SetAllowedPublishPrefixes([]string{
		"/cvmfs/repo.cern.ch/lcg",
		"  /cvmfs/repo.cern.ch/cms/  ", // trailing slash + whitespace: normalized
		"",                             // dropped
	})

	cases := []struct {
		name    string
		repo    string
		subPath string
		want    bool
	}{
		{"inside lcg root", "repo.cern.ch", "lcg/releases/main/ROOT/x", true},
		{"lcg root exactly", "repo.cern.ch", "lcg", true},
		{"inside cms root (normalized)", "repo.cern.ch", "cms/releases/1/y", true},
		{"sibling of an allowed root", "repo.cern.ch", "lhcb/releases/x", false},
		{"prefix-string false friend", "repo.cern.ch", "lcg-evil/x", false},
		{"other repo entirely", "other.cern.ch", "lcg/x", false},
		{"traversal escape is cleaned", "repo.cern.ch", "lcg/../lhcb/x", false},
		{"empty subpath is repo root", "repo.cern.ch", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := srv.publishAuthorized(c.repo, c.subPath); got != c.want {
				t.Errorf("publishAuthorized(%q,%q)=%v want %v", c.repo, c.subPath, got, c.want)
			}
		})
	}
}

// TestReserveHandler_ContainmentRejects verifies the reserve endpoint returns 403
// for a target outside the authorized namespace, and passes an in-namespace target
// through (204, since the test server has no gateway lease client).
func TestReserveHandler_ContainmentRejects(t *testing.T) {
	srv, _, _ := newTestServer(t)
	srv.SetAllowedPublishPrefixes([]string{"/cvmfs/repo.cern.ch/lcg"})

	post := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "/api/v1/reserve", strings.NewReader(body))
		rec := httptest.NewRecorder()
		srv.reserveHandler(rec, req)
		return rec
	}

	if rec := post(`{"repo":"repo.cern.ch","path":"cms/releases/1/x"}`); rec.Code != http.StatusForbidden {
		t.Errorf("out-of-namespace reserve: want 403, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec := post(`{"repo":"repo.cern.ch","path":"lcg/releases/main/ROOT/x"}`); rec.Code != http.StatusNoContent {
		t.Errorf("in-namespace reserve: want 204, got %d: %s", rec.Code, rec.Body.String())
	}
}
