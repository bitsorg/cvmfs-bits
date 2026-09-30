// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"archive/tar"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// fakeIngestStub is fakeCvmfsServer plus a copy of the tar each ingest was
// given (the backend removes its temp tar after the call).
func fakeIngestStub(t *testing.T) (calls func() []string, tarCopy string) {
	t.Helper()
	dir := t.TempDir()
	log := filepath.Join(dir, "calls.log")
	tarCopy = filepath.Join(dir, "chain.tar")
	script := "#!/bin/sh\necho \"$@\" >> " + log + "\n" +
		"if [ \"$1\" = ingest ]; then cp \"$3\" " + tarCopy + "; fi\nexit 0\n"
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
	}, tarCopy
}

// mountlessBackend: repository not mounted, published paths given by exists.
func mountlessBackend(t *testing.T, exists map[string]bool, lookups *[]string) (*IngestBackend, string) {
	t.Helper()
	mount := t.TempDir()
	b := NewIngestBackend(IngestOptions{CVMFSMount: mount, Owner: "cvbits"}, newTestObs(t))
	b.mounted = func(string) bool { return false }
	b.pathExists = func(_ context.Context, _, rel string) (bool, error) {
		*lookups = append(*lookups, rel)
		return exists[rel], nil
	}
	return b, mount
}

func tarNames(t *testing.T, p string) []string {
	t.Helper()
	f, err := os.Open(p)
	if err != nil {
		t.Fatalf("open tar: %v", err)
	}
	defer f.Close()
	var names []string
	tr := tar.NewReader(f)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return names
		}
		if err != nil {
			t.Fatalf("read tar: %v", err)
		}
		if h.Typeflag != tar.TypeDir {
			t.Errorf("%s: not a directory entry", h.Name)
		}
		names = append(names, h.Name)
	}
}

// A mountless host creates the missing chain through the gateway: one ingest
// at the deepest published ancestor, holding only the missing directories.
func TestEnsureAncestorsMountless_IngestsMissingChain(t *testing.T) {
	calls, tarCopy := fakeIngestStub(t)
	var lookups []string
	b, mount := mountlessBackend(t, map[string]bool{"key4hep": true}, &lookups)
	repo := "bits.cern.ch"
	target := filepath.Join(mount, repo, "key4hep/x86_64-el9-gcc15-opt/Packages/ROOT")

	if err := b.ensureAncestors(context.Background(), repo, target); err != nil {
		t.Fatalf("ensureAncestors: %v", err)
	}
	wantLookups := []string{"key4hep/x86_64-el9-gcc15-opt/Packages", "key4hep/x86_64-el9-gcc15-opt", "key4hep"}
	if !reflect.DeepEqual(lookups, wantLookups) {
		t.Errorf("lookups = %v, want %v", lookups, wantLookups)
	}
	got := calls()
	if len(got) != 1 || !strings.HasPrefix(got[0], "ingest -t ") ||
		!strings.HasSuffix(got[0], " -b key4hep -u cvbits "+repo) {
		t.Fatalf("want one ingest at key4hep, got %v", got)
	}
	if strings.Contains(got[0], " -c") {
		t.Errorf("ancestor ingest must not create a nested catalog: %q", got[0])
	}
	want := []string{"x86_64-el9-gcc15-opt/", "x86_64-el9-gcc15-opt/Packages/"}
	if names := tarNames(t, tarCopy); !reflect.DeepEqual(names, want) {
		t.Errorf("tar = %v, want %v", names, want)
	}

	// Remembered: the next package under the same parent asks nothing.
	lookups = nil
	if err := b.ensureAncestors(context.Background(), repo,
		filepath.Join(mount, repo, "key4hep/x86_64-el9-gcc15-opt/Packages/CMake")); err != nil {
		t.Fatalf("second ensureAncestors: %v", err)
	}
	if len(lookups) != 0 || len(calls()) != 1 {
		t.Errorf("second call: lookups %v, calls %v", lookups, calls())
	}
}

// Nothing published yet: the chain is ingested at the repository root.
func TestEnsureAncestorsMountless_EmptyRepository(t *testing.T) {
	calls, tarCopy := fakeIngestStub(t)
	var lookups []string
	b, mount := mountlessBackend(t, map[string]bool{}, &lookups)
	repo := "bits.cern.ch"
	if err := b.ensureAncestors(context.Background(), repo,
		filepath.Join(mount, repo, "lhcb/x86_64-el9-gcc14-opt/Packages/ROOT")); err != nil {
		t.Fatalf("ensureAncestors: %v", err)
	}
	got := calls()
	if len(got) != 1 || !strings.Contains(got[0], " -b / ") {
		t.Fatalf("want one ingest at /, got %v", got)
	}
	want := []string{"lhcb/", "lhcb/x86_64-el9-gcc14-opt/", "lhcb/x86_64-el9-gcc14-opt/Packages/"}
	if names := tarNames(t, tarCopy); !reflect.DeepEqual(names, want) {
		t.Errorf("tar = %v, want %v", names, want)
	}
}

// The common case: the parent is published, one lookup and no ingest.
func TestEnsureAncestorsMountless_ParentPublished(t *testing.T) {
	calls, _ := fakeIngestStub(t)
	var lookups []string
	b, mount := mountlessBackend(t, map[string]bool{"a/b": true}, &lookups)
	if err := b.ensureAncestors(context.Background(), "r.example.org",
		filepath.Join(mount, "r.example.org", "a/b/pkg")); err != nil {
		t.Fatalf("ensureAncestors: %v", err)
	}
	if len(lookups) != 1 || len(calls()) != 0 {
		t.Errorf("lookups %v, calls %v", lookups, calls())
	}
}

// A lookup that fails is logged and the publish proceeds, as before.
func TestEnsureAncestorsMountless_LookupErrorProceeds(t *testing.T) {
	calls, _ := fakeIngestStub(t)
	mount := t.TempDir()
	b := NewIngestBackend(IngestOptions{CVMFSMount: mount}, newTestObs(t))
	b.mounted = func(string) bool { return false }
	b.pathExists = func(context.Context, string, string) (bool, error) {
		return false, errors.New("stratum0 down")
	}
	if err := b.ensureAncestors(context.Background(), "r.example.org",
		filepath.Join(mount, "r.example.org", "a/b/pkg")); err != nil {
		t.Fatalf("lookup failure must not fail the publish: %v", err)
	}
	if got := calls(); len(got) != 0 {
		t.Errorf("no cvmfs_server call expected, got %v", got)
	}
}

// Regression: a registration that was once mounted leaves an empty
// /cvmfs/<repo>. That directory is not a mount, so the mountless path must be
// taken instead of a local transaction that cannot mount.
func TestEnsureAncestors_EmptyMountpointIsMountless(t *testing.T) {
	calls, _ := fakeIngestStub(t)
	mount := t.TempDir()
	repo := "bits.cern.ch"
	if err := os.MkdirAll(filepath.Join(mount, repo), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	b := NewIngestBackend(IngestOptions{CVMFSMount: mount, Stratum0URL: "http://unused"}, newTestObs(t))
	b.pathExists = func(context.Context, string, string) (bool, error) { return true, nil }
	if err := b.ensureAncestors(context.Background(), repo,
		filepath.Join(mount, repo, "a/b/pkg")); err != nil {
		t.Fatalf("ensureAncestors: %v", err)
	}
	if got := calls(); len(got) != 0 {
		t.Errorf("must not open a local transaction, got %v", got)
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

// A failed ingest forgets what was known about that repository only.
func TestForgetKnown(t *testing.T) {
	b := NewIngestBackend(IngestOptions{}, newTestObs(t))
	b.remember("a.org", "x/y")
	b.remember("b.org", "x/y")
	b.forgetKnown("a.org")
	if b.isKnown("a.org", "x/y") || !b.isKnown("b.org", "x/y") {
		t.Error("forgetKnown must drop only the given repository")
	}
}

// Inside Commit the ancestor ingest runs first, and a failed main ingest drops
// what was remembered about the repository.
func TestCommitMountless_AncestorsFirstAndForgetOnFailure(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "calls.log")
	script := "#!/bin/sh\necho \"$@\" >> " + log + "\n" +
		"case \"$3\" in *payload.tar) echo 'ingest failed' >&2; exit 1;; esac\nexit 0\n"
	if err := os.WriteFile(filepath.Join(dir, "cvmfs_server"), []byte(script), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	var lookups []string
	b, mount := mountlessBackend(t, map[string]bool{"key4hep": true}, &lookups)
	repo := "bits.cern.ch"
	err := b.Commit(context.Background(), CommitRequest{
		Token:    repo,
		TarPath:  filepath.Join(dir, "payload.tar"),
		CVMFSDir: filepath.Join(mount, repo, "key4hep/arch/Packages/ROOT/1-1"),
	})
	if err == nil {
		t.Fatal("want the main ingest failure")
	}
	raw, _ := os.ReadFile(log)
	got := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(got) != 2 || !strings.Contains(got[0], " -b key4hep ") ||
		!strings.Contains(got[1], "payload.tar") {
		t.Fatalf("want ancestor ingest then main ingest, got %v", got)
	}
	if b.isKnown(repo, "key4hep/arch/Packages/ROOT") {
		t.Error("a failed ingest must forget the repository's known parents")
	}
}

// A mounted host keeps the transaction path even when a stratum0 is configured.
func TestEnsureAncestors_MountedIgnoresStratum0(t *testing.T) {
	calls := fakeCvmfsServer(t, "")
	repo := "bits.cern.ch"
	b, mount := newAncestorBackend(t, repo)
	b.pathExists = func(context.Context, string, string) (bool, error) {
		t.Error("a mounted host must not ask the stratum0")
		return false, nil
	}
	if err := b.ensureAncestors(context.Background(), repo,
		filepath.Join(mount, repo, "a/b/pkg")); err != nil {
		t.Fatalf("ensureAncestors: %v", err)
	}
	if got := calls(); len(got) != 2 || !strings.HasPrefix(got[0], "transaction ") {
		t.Errorf("want transaction+publish, got %v", got)
	}
}
