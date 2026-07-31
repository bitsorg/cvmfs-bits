// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeCvmfsServer puts a recording stub named `cvmfs_server` at the front of
// PATH and returns a function yielding the argument vectors it was called with,
// one invocation per line.
//
// The stub is how these tests can assert on the real code path: ensureAncestors
// shells out, so anything less would only be testing a mock of itself. `exit 1`
// via failOn lets a specific subcommand fail without disturbing the others.
func fakeCvmfsServer(t *testing.T, failOn string) func() []string {
	t.Helper()
	dir := t.TempDir()
	log := filepath.Join(dir, "calls.log")
	script := "#!/bin/sh\necho \"$@\" >> " + log + "\n"
	if failOn != "" {
		script += "if [ \"$1\" = \"" + failOn + "\" ]; then echo 'stub failure' >&2; exit 1; fi\n"
	}
	script += "exit 0\n"
	if err := os.WriteFile(filepath.Join(dir, "cvmfs_server"), []byte(script), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return func() []string {
		raw, err := os.ReadFile(log)
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			t.Fatalf("read stub log: %v", err)
		}
		return strings.Split(strings.TrimSpace(string(raw)), "\n")
	}
}

// newAncestorBackend builds a backend rooted at a temp dir standing in for
// /cvmfs, with <mount>/<repo> present the way a mounted repository would be.
func newAncestorBackend(t *testing.T, repo string) (*IngestBackend, string) {
	t.Helper()
	mount := t.TempDir()
	if err := os.MkdirAll(filepath.Join(mount, repo), 0o755); err != nil {
		t.Fatalf("mkdir repo root: %v", err)
	}
	return NewIngestBackend(IngestOptions{CVMFSMount: mount}, newTestObs(t)), mount
}

// TestEnsureAncestors_CreatesMissingChain is the regression test for the
// gateway panic that failed a 174-job O2 publish: every payload uploaded and
// every commit died with "failed to graft nested catalog" / "catalog for
// directory ... cannot be found", because ingest creates its base directory but
// not the directories above it.
func TestEnsureAncestors_CreatesMissingChain(t *testing.T) {
	calls := fakeCvmfsServer(t, "")
	repo := "bits.cern.ch"
	b, mount := newAncestorBackend(t, repo)

	target := filepath.Join(mount, repo, "alice/el9-x86_64/Modules/modulefiles/probe")
	if err := b.ensureAncestors(context.Background(), repo, target); err != nil {
		t.Fatalf("ensureAncestors: %v", err)
	}

	parent := filepath.Dir(target)
	if !isDir(parent) {
		t.Errorf("parent chain %q was not created", parent)
	}
	got := calls()
	if len(got) != 2 {
		t.Fatalf("want transaction+publish, got %d calls: %v", len(got), got)
	}
	// Nothing under the repo exists, so the narrowest possible lease is the
	// repository root itself.
	if got[0] != "transaction "+repo {
		t.Errorf("lease target: got %q, want %q", got[0], "transaction "+repo)
	}
	if got[1] != "publish "+repo {
		t.Errorf("publish: got %q", got[1])
	}
}

// TestEnsureAncestors_LeasesDeepestExisting checks the blast-radius property:
// when part of the chain is already there, the transaction is taken on the
// deepest existing directory, not on the repository root, so creating one leaf
// prefix does not lock out every other path in the repository.
func TestEnsureAncestors_LeasesDeepestExisting(t *testing.T) {
	calls := fakeCvmfsServer(t, "")
	repo := "bits.cern.ch"
	b, mount := newAncestorBackend(t, repo)

	if err := os.MkdirAll(filepath.Join(mount, repo, "alice/el9-x86_64"), 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	target := filepath.Join(mount, repo, "alice/el9-x86_64/Modules/modulefiles/probe")
	if err := b.ensureAncestors(context.Background(), repo, target); err != nil {
		t.Fatalf("ensureAncestors: %v", err)
	}

	want := "transaction " + repo + "/alice/el9-x86_64"
	if got := calls(); len(got) == 0 || got[0] != want {
		t.Errorf("lease target: got %v, want %q", got, want)
	}
}

// TestEnsureAncestors_NoopWhenParentExists guards against a transaction per
// publish. The overwhelmingly common case is an existing prefix, and paying a
// gateway lease for it would make this fix more expensive than the bug.
func TestEnsureAncestors_NoopWhenParentExists(t *testing.T) {
	calls := fakeCvmfsServer(t, "")
	repo := "bits.cern.ch"
	b, mount := newAncestorBackend(t, repo)

	if err := os.MkdirAll(filepath.Join(mount, repo, "alice/el9"), 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	target := filepath.Join(mount, repo, "alice/el9/pkg")
	if err := b.ensureAncestors(context.Background(), repo, target); err != nil {
		t.Fatalf("ensureAncestors: %v", err)
	}
	if got := calls(); len(got) != 0 {
		t.Errorf("expected no cvmfs_server calls, got %v", got)
	}
}

// TestEnsureAncestors_NoopAtRepositoryRoot covers `-b /`: the repository root
// always exists, and path.Dir of the root would otherwise walk outside it.
func TestEnsureAncestors_NoopAtRepositoryRoot(t *testing.T) {
	calls := fakeCvmfsServer(t, "")
	repo := "bits.cern.ch"
	b, mount := newAncestorBackend(t, repo)

	if err := b.ensureAncestors(context.Background(), repo,
		filepath.Join(mount, repo, "pkg")); err != nil {
		t.Fatalf("ensureAncestors: %v", err)
	}
	if got := calls(); len(got) != 0 {
		t.Errorf("expected no cvmfs_server calls, got %v", got)
	}
}

// TestEnsureAncestors_MissingMountIsAnError distinguishes "this prefix does not
// exist yet" from "this repository is not mounted here". Walking up from an
// unmounted repository would otherwise try to create the mount point.
func TestEnsureAncestors_MissingMountIsAnError(t *testing.T) {
	calls := fakeCvmfsServer(t, "")
	mount := t.TempDir() // deliberately no <mount>/<repo>
	b := NewIngestBackend(IngestOptions{CVMFSMount: mount}, newTestObs(t))

	err := b.ensureAncestors(context.Background(), "bits.cern.ch",
		filepath.Join(mount, "bits.cern.ch", "a/b/pkg"))
	if err == nil {
		t.Fatal("want error for unmounted repository, got nil")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("error should name the missing mount, got: %v", err)
	}
	if got := calls(); len(got) != 0 {
		t.Errorf("must not open a transaction against an unmounted repo, got %v", got)
	}
}

// TestEnsureAncestors_AbortsOwnTransactionOnPublishFailure verifies the
// distinction ensureAncestors draws from Abort(): Abort refuses to abort
// because it cannot know whose transaction is open, but here the backend just
// opened this one, so leaving it dangling would block the repository until the
// gateway expired the lease.
func TestEnsureAncestors_AbortsOwnTransactionOnPublishFailure(t *testing.T) {
	calls := fakeCvmfsServer(t, "publish")
	repo := "bits.cern.ch"
	b, mount := newAncestorBackend(t, repo)

	err := b.ensureAncestors(context.Background(), repo,
		filepath.Join(mount, repo, "a/b/pkg"))
	if err == nil {
		t.Fatal("want error when publish fails, got nil")
	}
	got := calls()
	if len(got) != 3 {
		t.Fatalf("want transaction+publish+abort, got %d: %v", len(got), got)
	}
	if got[2] != "abort -f "+repo {
		t.Errorf("abort call: got %q, want %q", got[2], "abort -f "+repo)
	}
}

// TestEnsureAncestors_SkipOption keeps the escape hatch honest.
func TestEnsureAncestors_SkipOption(t *testing.T) {
	calls := fakeCvmfsServer(t, "")
	repo := "bits.cern.ch"
	mount := t.TempDir()
	if err := os.MkdirAll(filepath.Join(mount, repo), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	b := NewIngestBackend(
		IngestOptions{CVMFSMount: mount, SkipAncestorDirs: true}, newTestObs(t))

	if err := b.ensureAncestors(context.Background(), repo,
		filepath.Join(mount, repo, "a/b/pkg")); err != nil {
		t.Fatalf("ensureAncestors: %v", err)
	}
	if got := calls(); len(got) != 0 {
		t.Errorf("skip option must make no calls, got %v", got)
	}
}

// TestDeepestExisting pins the walk itself, including the case where dir is
// already root and the case where it escapes the repository.
func TestDeepestExisting(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "a/b"), 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	for _, tc := range []struct{ name, dir, want string }{
		{"deep miss lands on existing", filepath.Join(root, "a/b/c/d"), filepath.Join(root, "a/b")},
		{"exact hit", filepath.Join(root, "a"), filepath.Join(root, "a")},
		{"all missing falls back to root", filepath.Join(root, "x/y/z"), root},
		{"root itself", root, root},
		{"outside the repository", "/elsewhere/x", root},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := deepestExisting(root, tc.dir); got != tc.want {
				t.Errorf("deepestExisting(%q) = %q, want %q", tc.dir, got, tc.want)
			}
		})
	}
}
