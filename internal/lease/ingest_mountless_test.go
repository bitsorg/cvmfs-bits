// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// Regression: a registration that was once mounted leaves an empty
// /cvmfs/<repo>. That directory is not a mount, so no local transaction may be
// opened (it cannot mount); the parents are left to the gateway.
func TestEnsureAncestors_EmptyMountpointIsMountless(t *testing.T) {
	if _, err := os.Stat("/proc/self/mountinfo"); err != nil {
		t.Skip("no /proc/self/mountinfo: a directory then counts as mounted")
	}
	calls := fakeCvmfsServer(t, "")
	mount := t.TempDir()
	repo := "bits.cern.ch"
	if err := os.MkdirAll(filepath.Join(mount, repo), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	b := NewIngestBackend(IngestOptions{CVMFSMount: mount}, newTestObs(t))
	for _, pkg := range []string{"a/b/pkg1", "a/b/pkg2"} {
		if err := b.ensureAncestors(context.Background(), repo,
			filepath.Join(mount, repo, pkg)); err != nil {
			t.Fatalf("ensureAncestors: %v", err)
		}
	}
	if got := calls(); len(got) != 0 {
		t.Errorf("must not open a local transaction, got %v", got)
	}
	if !b.warned[repo] {
		t.Error("the mountless case must be reported (once)")
	}
}

func TestIsMountPoint(t *testing.T) {
	if _, err := os.Stat("/proc/self/mountinfo"); err != nil {
		t.Skip("no /proc/self/mountinfo")
	}
	if !isMountPoint("/") {
		t.Error(`"/" must be a mount point`)
	}
	if isMountPoint(t.TempDir()) {
		t.Error("a fresh temp dir must not be a mount point")
	}
	if got := unescapeMountinfo(`/a\040b`); got != "/a b" {
		t.Errorf("unescape = %q", got)
	}
}
