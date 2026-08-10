// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestValidateSubPath pins the shapes a job path may take.
func TestValidateSubPath(t *testing.T) {
	for _, tc := range []struct {
		name    string
		path    string
		wantErr string // substring; "" = must be accepted
	}{
		{"ordinary", "alice/el9-x86_64/Packages/ROOT/v6-36-10", ""},
		{"root publish", "", ""},
		{"single segment", "pkg", ""},
		{"trailing slash tolerated", "a/b/", ""},

		{"absolute", "/cvmfs/test.cvmfs.io/a/b", "repository-relative"},
		{"absolute plain", "/a/b", "repository-relative"},
		{"traversal", "../other-repo/x", "escapes the repository"},

		// The production case: a full path from ANOTHER repository, submitted
		// relative. Both filepath.Join and the containment check absorb it.
		{"repo-qualified", "cvmfs/bits.cern.ch/alice/el9-x86_64/Packages/ROOT/v1", "cvmfs"},
		{"bare cvmfs", "cvmfs", "cvmfs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSubPath(tc.path)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validateSubPath(%q) = %v, want accepted", tc.path, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateSubPath(%q) accepted, want rejected", tc.path)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("validateSubPath(%q) = %q, want mention of %q", tc.path, err, tc.wantErr)
			}
		})
	}
}

// TestJoinAbsorbsAbsolutePath documents WHY validateSubPath exists, so nobody
// later decides the check is redundant with the containment test.
//
// filepath.Join does not reject an absolute component — it concatenates it. The
// result is still under /cvmfs/<repo>, so publishAuthorized sees a legitimate
// in-namespace path and every downstream prefix check agrees. The job then
// publishes to a real but absurd location instead of being refused.
func TestJoinAbsorbsAbsolutePath(t *testing.T) {
	const mount, repo = "/cvmfs", "test.cvmfs.io"
	got := filepath.Join(mount, repo, "/cvmfs/bits.cern.ch/alice/x")
	const want = "/cvmfs/test.cvmfs.io/cvmfs/bits.cern.ch/alice/x"
	if got != want {
		t.Fatalf("filepath.Join gave %q, want %q — if this changed, re-check "+
			"whether validateSubPath is still needed", got, want)
	}
	// And it still looks contained, which is the trap.
	if !strings.HasPrefix(got, filepath.Join(mount, repo)+"/") {
		t.Errorf("expected the mangled path to still appear inside the repository root")
	}
}
