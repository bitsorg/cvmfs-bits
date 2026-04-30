package catalog

import (
	"context"
	"io/fs"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/pipeline/unpack"
	"cvmfs.io/prepub/pkg/cvmfscatalog"
	"cvmfs.io/prepub/pkg/observe"
)

func newObs(t *testing.T) *observe.Provider {
	t.Helper()
	obs, shutdown, err := observe.New("catalog-test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)
	return obs
}

// TestNew verifies that New returns a non-nil builder with an empty entry list.
func TestNew(t *testing.T) {
	obs := newObs(t)
	b, err := New("/unused/path", obs)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if b == nil {
		t.Fatal("New returned nil builder")
	}
	if entries := b.Entries(); len(entries) != 0 {
		t.Errorf("New: Entries() = %d; want 0", len(entries))
	}
}

// TestAdd_RegularFile verifies that a regular file entry is collected correctly.
func TestAdd_RegularFile(t *testing.T) {
	obs := newObs(t)
	b, _ := New("", obs)

	e := unpack.FileEntry{
		Path:    "usr/bin/foo",
		Mode:    0o755,
		Size:    1024,
		ModTime: time.Unix(1700000000, 0),
		UID:     1000,
		GID:     1000,
	}
	if err := b.Add(context.Background(), e, "abc123"); err != nil {
		t.Fatalf("Add: %v", err)
	}

	entries := b.Entries()
	if len(entries) != 1 {
		t.Fatalf("Entries() len = %d; want 1", len(entries))
	}
	got := entries[0]
	if got.FullPath != "usr/bin/foo" {
		t.Errorf("FullPath = %q; want usr/bin/foo", got.FullPath)
	}
	if got.Name != "foo" {
		t.Errorf("Name = %q; want foo", got.Name)
	}
	if got.Mode != 0o755 {
		t.Errorf("Mode = %v; want 0755", got.Mode)
	}
	if got.Size != 1024 {
		t.Errorf("Size = %d; want 1024", got.Size)
	}
	if got.UID != 1000 || got.GID != 1000 {
		t.Errorf("UID/GID = %d/%d; want 1000/1000", got.UID, got.GID)
	}
	if got.Mtime != 1700000000 {
		t.Errorf("Mtime = %d; want 1700000000", got.Mtime)
	}
	// Regular file with a hash should have algo fields set.
	if got.HashAlgo != cvmfscatalog.HashSha1 {
		t.Errorf("HashAlgo = %v; want HashSha1", got.HashAlgo)
	}
}

// TestAdd_Directory verifies that a directory entry is collected with the correct mode.
func TestAdd_Directory(t *testing.T) {
	obs := newObs(t)
	b, _ := New("", obs)

	e := unpack.FileEntry{
		Path:    "usr/lib",
		Mode:    fs.ModeDir | 0o755,
		ModTime: time.Now(),
	}
	if err := b.Add(context.Background(), e, ""); err != nil {
		t.Fatalf("Add dir: %v", err)
	}
	entries := b.Entries()
	if len(entries) != 1 {
		t.Fatalf("Entries len = %d; want 1", len(entries))
	}
	if entries[0].Name != "lib" {
		t.Errorf("Name = %q; want lib", entries[0].Name)
	}
	if entries[0].Mode&fs.ModeDir == 0 {
		t.Errorf("Mode %v does not have ModeDir set", entries[0].Mode)
	}
}

// TestAdd_Symlink verifies that the symlink target is recorded in the entry.
func TestAdd_Symlink(t *testing.T) {
	obs := newObs(t)
	b, _ := New("", obs)

	e := unpack.FileEntry{
		Path:       "usr/lib/libfoo.so",
		Mode:       fs.ModeSymlink | 0o777,
		LinkTarget: "libfoo.so.1",
		ModTime:    time.Now(),
	}
	if err := b.Add(context.Background(), e, ""); err != nil {
		t.Fatalf("Add symlink: %v", err)
	}
	entries := b.Entries()
	if len(entries) != 1 {
		t.Fatalf("Entries len = %d; want 1", len(entries))
	}
	if entries[0].Symlink != "libfoo.so.1" {
		t.Errorf("Symlink = %q; want libfoo.so.1", entries[0].Symlink)
	}
}

// TestAdd_Xattrs verifies that extended attributes from the tar entry are
// copied into the catalog entry.
func TestAdd_Xattrs(t *testing.T) {
	obs := newObs(t)
	b, _ := New("", obs)

	e := unpack.FileEntry{
		Path:    "etc/security/policy",
		Mode:    0o644,
		ModTime: time.Now(),
		Xattrs: map[string][]byte{
			"user.myapp.version": []byte("2"),
			"user.myapp.tag":     []byte("stable"),
		},
	}
	if err := b.Add(context.Background(), e, ""); err != nil {
		t.Fatalf("Add xattr: %v", err)
	}
	entry := b.Entries()[0]
	if len(entry.Xattr) != 2 {
		t.Fatalf("Xattr len = %d; want 2", len(entry.Xattr))
	}
	if string(entry.Xattr["user.myapp.version"]) != "2" {
		t.Errorf("xattr user.myapp.version = %q; want 2", entry.Xattr["user.myapp.version"])
	}
	if string(entry.Xattr["user.myapp.tag"]) != "stable" {
		t.Errorf("xattr user.myapp.tag = %q; want stable", entry.Xattr["user.myapp.tag"])
	}
}

// TestAdd_NoXattrs verifies that entries with no xattrs have a nil Xattr map.
func TestAdd_NoXattrs(t *testing.T) {
	obs := newObs(t)
	b, _ := New("", obs)

	e := unpack.FileEntry{
		Path:    "bin/sh",
		Mode:    0o755,
		ModTime: time.Now(),
	}
	if err := b.Add(context.Background(), e, ""); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if b.Entries()[0].Xattr != nil {
		t.Errorf("Xattr = %v; want nil for entry without xattrs", b.Entries()[0].Xattr)
	}
}

// TestEntries_Multiple verifies that multiple Add calls accumulate entries in
// insertion order and Entries() returns a stable slice.
func TestEntries_Multiple(t *testing.T) {
	obs := newObs(t)
	b, _ := New("", obs)

	paths := []string{"a/b", "a/c", "a/d/e"}
	for _, p := range paths {
		e := unpack.FileEntry{Path: p, Mode: 0o644, ModTime: time.Now()}
		if err := b.Add(context.Background(), e, ""); err != nil {
			t.Fatalf("Add %q: %v", p, err)
		}
	}

	entries := b.Entries()
	if len(entries) != len(paths) {
		t.Fatalf("Entries() len = %d; want %d", len(entries), len(paths))
	}
	for i, want := range paths {
		if entries[i].FullPath != want {
			t.Errorf("entries[%d].FullPath = %q; want %q", i, entries[i].FullPath, want)
		}
	}
}

// TestFinalize_ReturnsDeprecationError verifies that Finalize always returns
// an error and never succeeds (it is deprecated).
func TestFinalize_ReturnsDeprecationError(t *testing.T) {
	obs := newObs(t)
	b, _ := New("", obs)

	compPath, dbHash, err := b.Finalize(context.Background())
	if err == nil {
		t.Fatal("Finalize returned nil error; want deprecation error")
	}
	if compPath != "" {
		t.Errorf("compPath = %q; want empty string", compPath)
	}
	if dbHash != "" {
		t.Errorf("dbHash = %q; want empty string", dbHash)
	}
}

// TestAdd_RootPath verifies that adding an entry with path "/" or ""
// results in an empty Name (the root directory has no base name).
func TestAdd_RootPath(t *testing.T) {
	obs := newObs(t)
	b, _ := New("", obs)

	for _, p := range []string{"", "/"} {
		b2, _ := New("", obs)
		e := unpack.FileEntry{Path: p, Mode: fs.ModeDir | 0o755, ModTime: time.Now()}
		if err := b2.Add(context.Background(), e, ""); err != nil {
			t.Fatalf("Add(%q): %v", p, err)
		}
		entry := b2.Entries()[0]
		if entry.Name != "" {
			t.Errorf("Add(%q): Name = %q; want empty string for root", p, entry.Name)
		}
	}
	_ = b
}
