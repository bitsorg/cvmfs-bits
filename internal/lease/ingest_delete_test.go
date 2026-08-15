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

// The stub outputs below are DERIVED FROM THE REAL cvmfs_server, observed on
// the testbed on 2026-08-15 (MEASUREMENTS.md §25): the success tail
// "Changes submitted to repository gateway", and the exit-0 refusal
// "[WARNING] '<path>' cannot be deleted. Unrecognized file type." that an
// unfixed server (or a non-nested-catalog target) produces while committing an
// EMPTY revision. A stub that invents its own shapes tests only itself.

// fakeCvmfsServerWithOutput is fakeCvmfsServer plus a fixed stdout payload and
// exit code, so DeleteSubtree's output inspection sees what the real tool
// prints.
func fakeCvmfsServerWithOutput(t *testing.T, output string, exit int) func() []string {
	t.Helper()
	dir := t.TempDir()
	log := filepath.Join(dir, "calls.log")
	script := "#!/bin/sh\necho \"$@\" >> " + log + "\n" +
		"cat <<'CVMFS_STUB_EOF'\n" + output + "\nCVMFS_STUB_EOF\n" +
		"exit " + map[bool]string{true: "0", false: "1"}[exit == 0] + "\n"
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

func TestDeleteSubtree_InvokesFastDeleteRepoLast(t *testing.T) {
	calls := fakeCvmfsServerWithOutput(t,
		"Changes submitted to repository gateway", 0)
	b := NewIngestBackend(IngestOptions{}, newTestObs(t))

	err := b.DeleteSubtree(context.Background(), "test.cvmfs.io",
		"el9-x86_64/Packages/alibuild-recipe-tools/v0.3.0-3")
	if err != nil {
		t.Fatalf("DeleteSubtree: %v", err)
	}
	got := calls()
	if len(got) != 1 {
		t.Fatalf("want exactly one cvmfs_server call, got %d: %v", len(got), got)
	}
	// Repository LAST: cvmfs_server's option loop takes $1 as the repository
	// once the flags run out — anything after it is silently consumed.
	want := "ingest -f el9-x86_64/Packages/alibuild-recipe-tools/v0.3.0-3 test.cvmfs.io"
	if got[0] != want {
		t.Errorf("argv = %q, want %q", got[0], want)
	}
}

// The real failure mode this guards: on a server without the mountless
// fast-delete fix the deletion is refused with a warning, the transaction
// commits EMPTY, and the exit status is 0. Trusting exit status would retry
// the publish straight into the same UNIQUE-constraint crash.
func TestDeleteSubtree_RefusalWarningIsAnErrorDespiteExitZero(t *testing.T) {
	fakeCvmfsServerWithOutput(t,
		"[WARNING] 'el9-x86_64/Packages/alibuild-recipe-tools/v0.3.0-3' cannot "+
			"be deleted. Unrecognized file type.\n"+
			"Changes submitted to repository gateway", 0)
	b := NewIngestBackend(IngestOptions{}, newTestObs(t))

	err := b.DeleteSubtree(context.Background(), "test.cvmfs.io",
		"el9-x86_64/Packages/alibuild-recipe-tools/v0.3.0-3")
	if err == nil {
		t.Fatal("exit-0 refusal was treated as success")
	}
	if !strings.Contains(err.Error(), "fast-delete fix") &&
		!strings.Contains(err.Error(), "nested-catalog mountpoint") {
		t.Errorf("error %q does not explain the refusal", err)
	}
}

func TestDeleteSubtree_NonzeroExitIsAnError(t *testing.T) {
	fakeCvmfsServerWithOutput(t, "Gateway reply: error", 1)
	b := NewIngestBackend(IngestOptions{}, newTestObs(t))

	if err := b.DeleteSubtree(context.Background(), "test.cvmfs.io",
		"el9-x86_64/Packages/x/v1"); err == nil {
		t.Fatal("nonzero exit was treated as success")
	}
}

func TestDeleteSubtree_RefusesTheRepositoryRoot(t *testing.T) {
	calls := fakeCvmfsServerWithOutput(t, "", 0)
	b := NewIngestBackend(IngestOptions{}, newTestObs(t))

	for _, p := range []string{"", "/", "//"} {
		if err := b.DeleteSubtree(context.Background(), "test.cvmfs.io", p); err == nil {
			t.Errorf("path %q: deleting the repository root was allowed", p)
		}
	}
	if got := calls(); got != nil {
		t.Errorf("cvmfs_server was invoked for a root delete: %v", got)
	}
}

// The slot must be released on every path, or the next publish to the
// repository deadlocks. Acquire after each DeleteSubtree outcome proves it.
func TestDeleteSubtree_ReleasesTheSlot(t *testing.T) {
	fakeCvmfsServerWithOutput(t, "Changes submitted to repository gateway", 0)
	b := NewIngestBackend(IngestOptions{}, newTestObs(t))

	if err := b.DeleteSubtree(context.Background(), "test.cvmfs.io", "a/b"); err != nil {
		t.Fatalf("DeleteSubtree: %v", err)
	}
	tok, err := b.Acquire(context.Background(), "test.cvmfs.io", "a/b")
	if err != nil {
		t.Fatalf("slot not released after successful delete: %v", err)
	}
	b.release(tok)
}

// The measurement records are only as good as what the backend reports, and
// nothing else in the suite exercises the PublishStats sink: a mock backend
// that ignores it would keep every other test green while the numbers went
// missing (the review found exactly this hole).
//
// NEGATIVE CONTROL: delete the `req.Stats` assignments in Commit and this
// fails on all three assertions.
func TestCommit_FillsPublishStats(t *testing.T) {
	fakeCvmfsServerWithOutput(t, "Changes submitted to repository gateway", 0)
	b := NewIngestBackend(IngestOptions{CVMFSMount: "/cvmfs", SkipAncestorDirs: true},
		newTestObs(t))

	tar := filepath.Join(t.TempDir(), "payload.tar")
	if err := os.WriteFile(tar, make([]byte, 4096), 0o644); err != nil {
		t.Fatalf("write tar: %v", err)
	}
	token, err := b.Acquire(context.Background(), "test.cvmfs.io", "a/b")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	var stats PublishStats
	err = b.Commit(context.Background(), CommitRequest{
		Token:    token,
		TarPath:  tar,
		CVMFSDir: "/cvmfs/test.cvmfs.io/a/b",
		Stats:    &stats,
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if stats.Backend <= 0 {
		t.Error("backend duration not reported")
	}
	if stats.TarBytes == nil || *stats.TarBytes != 4096 {
		t.Errorf("tar bytes = %v, want 4096", stats.TarBytes)
	}
	// Without --object-list nothing counts objects, and the sink must stay
	// empty rather than claim zero.
	if stats.Objects != nil {
		t.Errorf("objects reported without an object list: %v", *stats.Objects)
	}
}

// A nil sink is the disabled case and must not panic or change the publish.
func TestCommit_NilStatsSinkIsFine(t *testing.T) {
	fakeCvmfsServerWithOutput(t, "Changes submitted to repository gateway", 0)
	b := NewIngestBackend(IngestOptions{CVMFSMount: "/cvmfs", SkipAncestorDirs: true},
		newTestObs(t))
	tar := filepath.Join(t.TempDir(), "payload.tar")
	_ = os.WriteFile(tar, make([]byte, 16), 0o644)
	token, _ := b.Acquire(context.Background(), "test.cvmfs.io", "a/b")
	if err := b.Commit(context.Background(), CommitRequest{
		Token: token, TarPath: tar, CVMFSDir: "/cvmfs/test.cvmfs.io/a/b", Stats: nil,
	}); err != nil {
		t.Fatalf("Commit with a nil stats sink: %v", err)
	}
}
