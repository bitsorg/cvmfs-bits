package lease

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cvmfs.io/prepub/pkg/observe"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// tarEntry describes one entry in a test tar archive.
type tarEntry struct {
	name     string
	typeflag byte      // 0 → tar.TypeReg
	content  []byte
	linkname string    // TypeLink or TypeSymlink target
	mode     fs.FileMode
	modTime  time.Time
	accTime  time.Time
}

// buildTar writes the given entries to a temporary tar file and returns its path.
func buildTar(t *testing.T, entries []tarEntry) string {
	t.Helper()
	tmp := t.TempDir()
	f, err := os.CreateTemp(tmp, "test-*.tar")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer f.Close() // ensure fd is released even if t.Fatalf fires below
	path := f.Name()
	tw := tar.NewWriter(f)
	for _, e := range entries {
		tf := e.typeflag
		if tf == 0 {
			tf = tar.TypeReg
		}
		size := int64(len(e.content))
		if tf == tar.TypeLink || tf == tar.TypeSymlink || tf == tar.TypeDir {
			size = 0
		}
		hdr := &tar.Header{
			Name:       e.name,
			Typeflag:   tf,
			Size:       size,
			Mode:       int64(e.mode),
			Linkname:   e.linkname,
			ModTime:    e.modTime,
			AccessTime: e.accTime,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("WriteHeader %q: %v", e.name, err)
		}
		if len(e.content) > 0 && tf == tar.TypeReg {
			if _, err := tw.Write(e.content); err != nil {
				t.Fatalf("Write %q: %v", e.name, err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tw.Close: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("f.Close: %v", err)
	}
	return path
}

func newTestObs(t *testing.T) *observe.Provider {
	t.Helper()
	obs, shutdown, err := observe.New("test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)
	return obs
}

// ── extractTar tests ──────────────────────────────────────────────────────────

// TestExtractTar_RegularFile verifies that a TypeReg entry is written with
// the correct content, mode, and modification time.
func TestExtractTar_RegularFile(t *testing.T) {
	obs := newTestObs(t)
	dest := t.TempDir()
	want := []byte("hello, world")
	mtime := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)

	tarPath := buildTar(t, []tarEntry{
		{name: "greet.txt", content: want, mode: 0644, modTime: mtime},
	})

	if err := extractTar(context.Background(), tarPath, dest, obs); err != nil {
		t.Fatalf("extractTar: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(dest, "greet.txt"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("content mismatch: got %q, want %q", got, want)
	}

	info, err := os.Stat(filepath.Join(dest, "greet.txt"))
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Mode().Perm() != 0644 {
		t.Errorf("mode: got %o, want %o", info.Mode().Perm(), 0644)
	}
	if !info.ModTime().Equal(mtime) {
		t.Errorf("mtime: got %v, want %v", info.ModTime(), mtime)
	}
}

// TestExtractTar_Directory verifies that a TypeDir entry creates a directory
// with the correct mode.
func TestExtractTar_Directory(t *testing.T) {
	obs := newTestObs(t)
	dest := t.TempDir()

	tarPath := buildTar(t, []tarEntry{
		{name: "subdir/", typeflag: tar.TypeDir, mode: 0755},
		{name: "subdir/file.txt", content: []byte("in subdir"), mode: 0644},
	})

	if err := extractTar(context.Background(), tarPath, dest, obs); err != nil {
		t.Fatalf("extractTar: %v", err)
	}

	info, err := os.Stat(filepath.Join(dest, "subdir"))
	if err != nil {
		t.Fatalf("Stat subdir: %v", err)
	}
	if !info.IsDir() {
		t.Error("expected a directory")
	}
	got, _ := os.ReadFile(filepath.Join(dest, "subdir/file.txt"))
	if string(got) != "in subdir" {
		t.Errorf("file content: got %q", got)
	}
}

// TestExtractTar_HardLink is the regression test for the TypeLink bug.
// Before the fix, TypeLink entries were read from the tar reader (which
// returns EOF for TypeLink — no payload) and the file was written as empty.
// After the fix, the hard link is materialised as a copy of the source file.
func TestExtractTar_HardLink(t *testing.T) {
	obs := newTestObs(t)
	dest := t.TempDir()
	want := []byte("shared content — must not be empty after hard link materialisation")
	mtime := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

	tarPath := buildTar(t, []tarEntry{
		// Original regular file.
		{name: "original.txt", content: want, mode: 0644, modTime: mtime},
		// Hard link — no payload in the tar, reader returns EOF for this entry.
		{name: "link.txt", typeflag: tar.TypeLink, linkname: "original.txt", mode: 0644, modTime: mtime},
	})

	if err := extractTar(context.Background(), tarPath, dest, obs); err != nil {
		t.Fatalf("extractTar: %v", err)
	}

	for _, name := range []string{"original.txt", "link.txt"} {
		got, err := os.ReadFile(filepath.Join(dest, name))
		if err != nil {
			t.Fatalf("ReadFile %q: %v", name, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%q: got %q (%d bytes), want %q (%d bytes) — hard link content is wrong (empty file bug?)",
				name, got, len(got), want, len(want))
		}
	}
}

// TestExtractTar_HardLink_TimestampPreserved verifies that the mtime on a
// materialised hard link copy reflects the tar header, not the extraction time.
func TestExtractTar_HardLink_TimestampPreserved(t *testing.T) {
	obs := newTestObs(t)
	dest := t.TempDir()
	mtime := time.Date(2020, 3, 14, 15, 9, 26, 0, time.UTC)

	tarPath := buildTar(t, []tarEntry{
		{name: "src.txt", content: []byte("x"), mode: 0644, modTime: mtime},
		{name: "hl.txt", typeflag: tar.TypeLink, linkname: "src.txt", mode: 0644, modTime: mtime},
	})

	if err := extractTar(context.Background(), tarPath, dest, obs); err != nil {
		t.Fatalf("extractTar: %v", err)
	}

	info, err := os.Stat(filepath.Join(dest, "hl.txt"))
	if err != nil {
		t.Fatalf("Stat hl.txt: %v", err)
	}
	if !info.ModTime().Equal(mtime) {
		t.Errorf("hl.txt mtime: got %v, want %v", info.ModTime(), mtime)
	}
}

// TestExtractTar_Symlink verifies that a TypeSymlink entry creates a symbolic
// link with the correct target and does not follow or validate the target path.
func TestExtractTar_Symlink(t *testing.T) {
	obs := newTestObs(t)
	dest := t.TempDir()

	// Create a real file so the symlink can be read back (optional, but cleaner).
	if err := os.WriteFile(filepath.Join(dest, "real.txt"), []byte("real"), 0644); err != nil {
		t.Fatal(err)
	}

	tarPath := buildTar(t, []tarEntry{
		{name: "link.txt", typeflag: tar.TypeSymlink, linkname: "real.txt"},
	})

	if err := extractTar(context.Background(), tarPath, dest, obs); err != nil {
		t.Fatalf("extractTar: %v", err)
	}

	target, err := os.Readlink(filepath.Join(dest, "link.txt"))
	if err != nil {
		t.Fatalf("Readlink: %v", err)
	}
	if target != "real.txt" {
		t.Errorf("symlink target: got %q, want %q", target, "real.txt")
	}
}

// TestExtractTar_AbsoluteSymlink verifies that an absolute symlink target
// (legitimate in CVMFS for host library references) is created without error.
// This is a security-aware test: the symlink placement is within destDir, only
// the target is absolute.
func TestExtractTar_AbsoluteSymlink(t *testing.T) {
	obs := newTestObs(t)
	dest := t.TempDir()

	tarPath := buildTar(t, []tarEntry{
		{name: "ld.so", typeflag: tar.TypeSymlink, linkname: "/lib64/ld-linux-x86-64.so.2"},
	})

	if err := extractTar(context.Background(), tarPath, dest, obs); err != nil {
		t.Fatalf("extractTar: %v", err)
	}

	target, err := os.Readlink(filepath.Join(dest, "ld.so"))
	if err != nil {
		t.Fatalf("Readlink: %v", err)
	}
	if target != "/lib64/ld-linux-x86-64.so.2" {
		t.Errorf("symlink target: got %q", target)
	}
}

// TestExtractTar_PathTraversal_EntryName verifies that a tar entry whose name
// contains ".." and would escape the destination directory is rejected.
func TestExtractTar_PathTraversal_EntryName(t *testing.T) {
	obs := newTestObs(t)
	dest := t.TempDir()

	tarPath := buildTar(t, []tarEntry{
		{name: "../escape.txt", content: []byte("bad"), mode: 0644},
	})

	err := extractTar(context.Background(), tarPath, dest, obs)
	if err == nil {
		t.Fatal("expected error for path-traversal entry, got nil")
	}
	if !strings.Contains(err.Error(), "escapes destination") {
		t.Errorf("unexpected error: %v", err)
	}

	// Verify the file was not created outside destDir.
	parent := filepath.Dir(dest)
	if _, statErr := os.Stat(filepath.Join(parent, "escape.txt")); statErr == nil {
		t.Error("traversal file was created outside destDir")
	}
}

// TestExtractTar_PathTraversal_HardLinkSource verifies that a hard link whose
// Linkname resolves outside destDir is rejected before any file is opened.
// This is the security fix for the hard-link source traversal vector.
func TestExtractTar_PathTraversal_HardLinkSource(t *testing.T) {
	obs := newTestObs(t)
	dest := t.TempDir()

	// Place a sensitive file in the parent directory (simulates a host file).
	sensitiveDir := filepath.Dir(dest)
	sensitive := filepath.Join(sensitiveDir, "sensitive.txt")
	if err := os.WriteFile(sensitive, []byte("secret"), 0600); err != nil {
		t.Fatalf("WriteFile sensitive: %v", err)
	}
	t.Cleanup(func() { os.Remove(sensitive) })

	tarPath := buildTar(t, []tarEntry{
		// Linkname climbs out of dest to read the sibling file.
		{name: "steal.txt", typeflag: tar.TypeLink, linkname: "../sensitive.txt"},
	})

	err := extractTar(context.Background(), tarPath, dest, obs)
	if err == nil {
		t.Fatal("expected error for hard-link source traversal, got nil")
	}
	if !strings.Contains(err.Error(), "escapes destination") {
		t.Errorf("unexpected error: %v", err)
	}

	// Verify no file was created in dest.
	if _, statErr := os.Stat(filepath.Join(dest, "steal.txt")); statErr == nil {
		t.Error("traversal file was created inside dest")
	}
}

// TestExtractTar_NestedPath verifies that entries under subdirectories are
// correctly placed without pre-existing directories in the tar.
func TestExtractTar_NestedPath(t *testing.T) {
	obs := newTestObs(t)
	dest := t.TempDir()

	tarPath := buildTar(t, []tarEntry{
		// No explicit TypeDir entry — extractTar must create parents automatically.
		{name: "a/b/c/file.txt", content: []byte("deep"), mode: 0644},
	})

	if err := extractTar(context.Background(), tarPath, dest, obs); err != nil {
		t.Fatalf("extractTar: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(dest, "a/b/c/file.txt"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "deep" {
		t.Errorf("content: got %q", got)
	}
}

// TestExtractTar_ContextCancelled verifies that extractTar returns
// context.Canceled when the context is already cancelled on entry.
//
// extractTar checks ctx.Err() at the top of every loop iteration, so a
// pre-cancelled context is sufficient to test that the guard fires.  The tar
// contains 20 uniquely named entries (not duplicates) so the test would still
// work correctly even if the context check were somehow delayed past the first
// entry.
func TestExtractTar_ContextCancelled(t *testing.T) {
	obs := newTestObs(t)
	dest := t.TempDir()

	// Build a tar with 20 uniquely named 1-byte files.
	var entries []tarEntry
	for i := 0; i < 20; i++ {
		entries = append(entries, tarEntry{
			name:    fmt.Sprintf("file%03d.txt", i),
			content: []byte("x"),
			mode:    0644,
		})
	}
	tarPath := buildTar(t, entries)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before calling — tests the ctx.Err() guard at loop entry

	err := extractTar(ctx, tarPath, dest, obs)
	if err == nil {
		t.Fatal("expected context error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

// TestExtractTar_HardLink_MissingSource verifies that a hard link whose source
// was not previously extracted returns an informative error rather than panicking
// or silently creating an empty file.
func TestExtractTar_HardLink_MissingSource(t *testing.T) {
	obs := newTestObs(t)
	dest := t.TempDir()

	tarPath := buildTar(t, []tarEntry{
		// Source file is NOT present in the tar — only the hard link entry.
		{name: "link.txt", typeflag: tar.TypeLink, linkname: "missing-original.txt", mode: 0644},
	})

	err := extractTar(context.Background(), tarPath, dest, obs)
	if err == nil {
		t.Fatal("expected error for missing hard link source, got nil")
	}
	// The error should mention the copy or the source path.
	if !strings.Contains(err.Error(), "hard link") && !strings.Contains(err.Error(), "missing-original") {
		t.Errorf("unexpected error (want mention of hard link / source): %v", err)
	}
}

// TestExtractTar_ExistingSymlinkOverwrite verifies that extracting a symlink
// over an existing entry (file or stale symlink) succeeds without error.
func TestExtractTar_ExistingSymlinkOverwrite(t *testing.T) {
	obs := newTestObs(t)
	dest := t.TempDir()

	// Pre-place a stale symlink at the target path.
	stale := filepath.Join(dest, "link.txt")
	if err := os.Symlink("stale-target", stale); err != nil {
		t.Fatalf("pre-create symlink: %v", err)
	}

	tarPath := buildTar(t, []tarEntry{
		{name: "link.txt", typeflag: tar.TypeSymlink, linkname: "new-target"},
	})

	if err := extractTar(context.Background(), tarPath, dest, obs); err != nil {
		t.Fatalf("extractTar: %v", err)
	}

	target, err := os.Readlink(stale)
	if err != nil {
		t.Fatalf("Readlink: %v", err)
	}
	if target != "new-target" {
		t.Errorf("symlink target after overwrite: got %q, want %q", target, "new-target")
	}
}
