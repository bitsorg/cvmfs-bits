// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package cvmfscatalog

import (
	"bytes"
	"compress/zlib"
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cvmfs.io/prepub/pkg/cvmfshash"
)

// decompressCatalogCAS decompresses a zlib-compressed CVMFS CAS catalog file
// and writes the raw SQLite bytes to destPath so it can be opened with Open().
func decompressCatalogCAS(casPath, destPath string) error {
	f, err := os.Open(casPath)
	if err != nil {
		return err
	}
	defer f.Close()

	zr, err := zlib.NewReader(f)
	if err != nil {
		return err
	}
	defer zr.Close()

	out, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, zr)
	return err
}

// TestBuildSubtreeBasic verifies that BuildSubtree creates a catalog CAS file
// and returns a non-empty, self-consistent SubtreeResult.
func TestBuildSubtreeBasic(t *testing.T) {
	tmpdir := t.TempDir()
	now := time.Now().Unix()

	entries := []Entry{
		{FullPath: ".", Mode: fs.ModeDir | 0o755, Size: 4096, Mtime: now, LinkCount: 2},
		{FullPath: "hello.txt", Name: "hello.txt", Mode: 0o100644, Size: 5, Mtime: now, LinkCount: 1},
	}

	result, err := BuildSubtree(context.Background(), SubtreeConfig{
		LeasePath: "test/smoke",
		TempDir:   tmpdir,
	}, entries)
	if err != nil {
		t.Fatalf("BuildSubtree failed: %v", err)
	}

	if result.CatalogHash == "" {
		t.Fatal("CatalogHash is empty")
	}
	if result.CatalogHashSuffixed != result.CatalogHash+"C" {
		t.Errorf("CatalogHashSuffixed %q != CatalogHash+C %q",
			result.CatalogHashSuffixed, result.CatalogHash+"C")
	}
	if len(result.AllCatalogHashes) == 0 {
		t.Fatal("AllCatalogHashes is empty")
	}
	// Last element must be the subtree root catalog.
	last := result.AllCatalogHashes[len(result.AllCatalogHashes)-1]
	if last != result.CatalogHash {
		t.Errorf("last AllCatalogHashes element %q != CatalogHash %q", last, result.CatalogHash)
	}

	// CAS file must exist at the expected path.
	casFile := filepath.Join(tmpdir, cvmfshash.ObjectPath(result.CatalogHash)+"C")
	if _, statErr := os.Stat(casFile); statErr != nil {
		t.Errorf("CAS catalog file not found at %s: %v", casFile, statErr)
	}
}

// TestBuildSubtreeRootPrefix verifies that the top-level lease catalog has
// root_prefix="" in its SQLite properties.
//
// Why ""?  cvmfs_receiver loads the submitted catalog via SimpleCatalogManager,
// which hard-codes mountpoint="" in GetNewRootCatalogContext().  Catalog::Open
// computes is_regular_mountpoint_ = (mountpoint == root_prefix).  If
// root_prefix were non-empty (e.g. "/atlas/24.0"), is_regular_mountpoint_ would
// be false and NormalizePath would compute MD5(root_prefix+path) instead of
// MD5(path), so every Listing() call during DiffRec returns empty and no files
// are committed.  Setting root_prefix="" makes is_regular_mountpoint_=true and
// NormalizePath=MD5(path), while entries remain stored at the correct absolute
// MD5 keys (e.g. MD5("/atlas/24.0") for the root entry).
//
// Split sub-catalogs are unaffected: they keep their real root_prefix because
// GraftNestedCatalog loads them via LoadFreeCatalog(mountpoint=actual_path),
// giving is_regular_mountpoint_=true automatically, and panics if
// new_catalog->root_prefix() != nested_root_ps.
func TestBuildSubtreeRootPrefix(t *testing.T) {
	tmpdir := t.TempDir()
	now := time.Now().Unix()

	entries := []Entry{
		{FullPath: ".", Mode: fs.ModeDir | 0o755, Size: 4096, Mtime: now, LinkCount: 2},
		{FullPath: "libfoo.so", Name: "libfoo.so", Mode: 0o100755, Size: 1024, Mtime: now, LinkCount: 1},
	}

	result, err := BuildSubtree(context.Background(), SubtreeConfig{
		LeasePath: "atlas/24.0",
		TempDir:   tmpdir,
	}, entries)
	if err != nil {
		t.Fatalf("BuildSubtree failed: %v", err)
	}

	// Decompress and open the catalog to inspect its properties.
	casFile := filepath.Join(tmpdir, cvmfshash.ObjectPath(result.CatalogHash)+"C")
	dbPath := filepath.Join(tmpdir, "verify.db")
	if err := decompressCatalogCAS(casFile, dbPath); err != nil {
		t.Fatalf("decompress catalog: %v", err)
	}
	cat, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open catalog: %v", err)
	}
	defer cat.Close()

	// The top-level lease catalog must have root_prefix="" so that
	// SimpleCatalogManager (mountpoint="") sets is_regular_mountpoint_=true
	// and NormalizePath returns MD5(path) unchanged during DiffRec.
	wantPrefix := ""
	if cat.rootPrefix != wantPrefix {
		t.Errorf("root_prefix = %q; want %q", cat.rootPrefix, wantPrefix)
	}
}

// TestBuildSubtreeWithSplits verifies that .cvmfscatalog markers produce
// split sub-catalogs and that every CAS file is written to disk.
func TestBuildSubtreeWithSplits(t *testing.T) {
	tmpdir := t.TempDir()
	now := time.Now().Unix()

	entries := []Entry{
		{FullPath: ".", Mode: fs.ModeDir | 0o755, Size: 4096, Mtime: now, LinkCount: 2},
		{FullPath: "sub", Name: "sub", Mode: fs.ModeDir | 0o755, Size: 4096, Mtime: now, LinkCount: 2},
		// .cvmfscatalog marker → "sub" becomes a split point.
		{FullPath: "sub/.cvmfscatalog", Name: ".cvmfscatalog", Mode: 0o100644, Size: 0, Mtime: now, LinkCount: 1},
		{FullPath: "sub/file.txt", Name: "file.txt", Mode: 0o100644, Size: 10, Mtime: now, LinkCount: 1},
	}

	result, err := BuildSubtree(context.Background(), SubtreeConfig{
		LeasePath: "pkg/v1",
		TempDir:   tmpdir,
	}, entries)
	if err != nil {
		t.Fatalf("BuildSubtree with splits failed: %v", err)
	}

	// Expect at least 2 catalog hashes: the split catalog + the subtree root.
	if len(result.AllCatalogHashes) < 2 {
		t.Errorf("expected ≥2 catalog hashes for split, got %d", len(result.AllCatalogHashes))
	}

	// All CAS files must exist on disk.
	for _, h := range result.AllCatalogHashes {
		casFile := filepath.Join(tmpdir, cvmfshash.ObjectPath(h)+"C")
		if _, statErr := os.Stat(casFile); statErr != nil {
			t.Errorf("CAS file for split catalog %s not found: %v", h, statErr)
		}
	}
}

// TestNestedCatalogSizeIsUncompressed pins the CVMFS invariant that a parent
// catalog's nested_catalogs.size holds the size of the child's UNCOMPRESSED
// SQLite database.
//
// cvmfs_swissknife check downloads the child object, decompresses it, and
// compares GetFileSize(decompressed) against that column
// (swissknife_check.cc:726-741). Recording the compressed object size instead
// made every nested catalog fail with "catalog file size mismatch, expected
// 1822, got 53248" and, because the checker then cannot walk those subtrees,
// produced a cascade of "statistics counter mismatch" errors at the root —
// reproducible on a clean testbed with `make test` alone.
func TestNestedCatalogSizeIsUncompressed(t *testing.T) {
	tmpdir := t.TempDir()
	now := time.Now().Unix()

	entries := []Entry{
		{FullPath: ".", Mode: fs.ModeDir | 0o755, Size: 4096, Mtime: now, LinkCount: 2},
		{FullPath: "sub", Name: "sub", Mode: fs.ModeDir | 0o755, Size: 4096, Mtime: now, LinkCount: 2},
		{FullPath: "sub/.cvmfscatalog", Name: ".cvmfscatalog", Mode: 0o100644, Size: 0, Mtime: now, LinkCount: 1},
		{FullPath: "sub/file.txt", Name: "file.txt", Mode: 0o100644, Size: 10, Mtime: now, LinkCount: 1},
	}

	result, err := BuildSubtree(context.Background(), SubtreeConfig{
		LeasePath: "pkg/v1",
		TempDir:   tmpdir,
	}, entries)
	if err != nil {
		t.Fatalf("BuildSubtree: %v", err)
	}

	// Open the subtree root catalog and read its nested_catalogs row.
	rootRaw := filepath.Join(tmpdir, "root-decompressed.db")
	if derr := decompressCatalogCAS(
		filepath.Join(tmpdir, cvmfshash.ObjectPath(result.CatalogHash)+"C"), rootRaw); derr != nil {
		t.Fatalf("decompress root catalog: %v", derr)
	}
	rootCat, err := Open(rootRaw)
	if err != nil {
		t.Fatalf("open root catalog: %v", err)
	}
	defer rootCat.Close()

	childHash, recordedSize, found, err := rootCat.FindNestedMount("/pkg/v1/sub")
	if err != nil || !found {
		t.Fatalf("nested mount /pkg/v1/sub not found (found=%v, err=%v)", found, err)
	}

	// What the checker measures: the decompressed child database.
	childCAS := filepath.Join(tmpdir, cvmfshash.ObjectPath(childHash)+"C")
	childRaw := filepath.Join(tmpdir, "child-decompressed.db")
	if derr := decompressCatalogCAS(childCAS, childRaw); derr != nil {
		t.Fatalf("decompress child catalog: %v", derr)
	}
	rawFI, err := os.Stat(childRaw)
	if err != nil {
		t.Fatalf("stat decompressed child: %v", err)
	}
	compFI, err := os.Stat(childCAS)
	if err != nil {
		t.Fatalf("stat compressed child: %v", err)
	}

	if recordedSize != rawFI.Size() {
		t.Errorf("nested_catalogs.size = %d, want the UNCOMPRESSED db size %d "+
			"(compressed object is %d bytes)",
			recordedSize, rawFI.Size(), compFI.Size())
	}
	// Guard against the regression returning: the two sizes must differ here,
	// otherwise the assertion above would pass for the wrong reason.
	if compFI.Size() >= rawFI.Size() {
		t.Skipf("catalog did not compress (%d >= %d); assertion not discriminating",
			compFI.Size(), rawFI.Size())
	}
}

// TestBuildSubtreeContextCancel verifies that a cancelled context returns an error.
func TestBuildSubtreeContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before calling

	_, err := BuildSubtree(ctx, SubtreeConfig{
		LeasePath: "test/path",
		TempDir:   t.TempDir(),
	}, nil)
	if err == nil {
		t.Error("expected error from cancelled context, got nil")
	}
}

// TestBuildSubtreeRootLevel verifies that LeasePath="" produces a root catalog
// (root_prefix = "") — required for initial or replace-all root-level publishes.
func TestBuildSubtreeRootLevel(t *testing.T) {
	tmpdir := t.TempDir()
	now := time.Now().Unix()

	entries := []Entry{
		{FullPath: ".", Mode: fs.ModeDir | 0o755, Size: 4096, Mtime: now, LinkCount: 2},
		{FullPath: "top.txt", Name: "top.txt", Mode: 0o100644, Size: 3, Mtime: now, LinkCount: 1},
	}

	result, err := BuildSubtree(context.Background(), SubtreeConfig{
		LeasePath: "",
		TempDir:   tmpdir,
	}, entries)
	if err != nil {
		t.Fatalf("BuildSubtree root-level failed: %v", err)
	}

	casFile := filepath.Join(tmpdir, cvmfshash.ObjectPath(result.CatalogHash)+"C")
	dbPath := filepath.Join(tmpdir, "verify_root.db")
	if err := decompressCatalogCAS(casFile, dbPath); err != nil {
		t.Fatalf("decompress: %v", err)
	}
	cat, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cat.Close()

	if cat.rootPrefix != "" {
		t.Errorf("root_prefix = %q; want empty string for root-level catalog", cat.rootPrefix)
	}
}

// TestDirLinkCountsMatchCheckRule pins the rule cvmfs_swissknife check
// enforces at swissknife_check.cc:652 —
//
//	linkcount(dir) == 2 + number of immediate subdirectories
//
// Producers used to hardcode LinkCount: 1, which made check report EVERY
// directory in the repository ("wrong linkcount for /test/smoke.0/simple;
// expected 2, got 1" — 212 occurrences in one `make test` run).
func TestDirLinkCountsMatchCheckRule(t *testing.T) {
	tmpdir := t.TempDir()
	now := time.Now().Unix()

	// pkg/v1/          → 2 subdirs (bin, share)     → linkcount 4
	// pkg/v1/bin       → 0 subdirs                  → linkcount 2
	// pkg/v1/share     → 1 subdir  (share/doc)      → linkcount 3
	// pkg/v1/share/doc → 0 subdirs                  → linkcount 2
	entries := []Entry{
		{FullPath: ".", Mode: fs.ModeDir | 0o755, Mtime: now, LinkCount: 1},
		{FullPath: "bin", Name: "bin", Mode: fs.ModeDir | 0o755, Mtime: now, LinkCount: 1},
		{FullPath: "bin/tool", Name: "tool", Mode: 0o755, Size: 10, Mtime: now, LinkCount: 1},
		{FullPath: "share", Name: "share", Mode: fs.ModeDir | 0o755, Mtime: now, LinkCount: 1},
		{FullPath: "share/doc", Name: "doc", Mode: fs.ModeDir | 0o755, Mtime: now, LinkCount: 1},
	}

	result, err := BuildSubtree(context.Background(), SubtreeConfig{
		LeasePath: "pkg/v1",
		TempDir:   tmpdir,
	}, entries)
	if err != nil {
		t.Fatalf("BuildSubtree: %v", err)
	}

	raw := filepath.Join(tmpdir, "linkcount.db")
	if derr := decompressCatalogCAS(
		filepath.Join(tmpdir, cvmfshash.ObjectPath(result.CatalogHash)+"C"), raw); derr != nil {
		t.Fatalf("decompress: %v", derr)
	}
	cat, err := Open(raw)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cat.Close()

	for path, want := range map[string]int64{
		"/pkg/v1":           4,
		"/pkg/v1/bin":       2,
		"/pkg/v1/share":     3,
		"/pkg/v1/share/doc": 2,
	} {
		p1, p2 := MD5Path(path)
		var hardlinks int64
		if qerr := cat.db.QueryRow(
			"SELECT hardlinks FROM catalog WHERE md5path_1=? AND md5path_2=?",
			p1, p2).Scan(&hardlinks); qerr != nil {
			t.Errorf("%s: query: %v", path, qerr)
			continue
		}
		// Low 32 bits of the hardlinks column carry the link count.
		if got := hardlinks & 0xFFFFFFFF; got != want {
			t.Errorf("%s: linkcount = %d, want %d (2 + #subdirs)", path, got, want)
		}
	}
}

// TestNestedRootHasMarker pins swissknife_check.cc:643-649: every nested
// catalog root must contain a .cvmfscatalog file, or check reports "nested
// catalog without marker at <path>" (28 occurrences in one `make test` run,
// all at lease-path roots, which tars do not carry a marker for).
func TestNestedRootHasMarker(t *testing.T) {
	tmpdir := t.TempDir()
	now := time.Now().Unix()

	entries := []Entry{
		{FullPath: ".", Mode: fs.ModeDir | 0o755, Mtime: now, LinkCount: 1},
		{FullPath: "file.txt", Name: "file.txt", Mode: 0o644, Size: 4, Mtime: now, LinkCount: 1},
	}

	result, err := BuildSubtree(context.Background(), SubtreeConfig{
		LeasePath: "pkg/v1",
		TempDir:   tmpdir,
	}, entries)
	if err != nil {
		t.Fatalf("BuildSubtree: %v", err)
	}
	if !result.NeedsMarkerObject {
		t.Error("NeedsMarkerObject = false; the caller would never store the marker object")
	}

	raw := filepath.Join(tmpdir, "marker.db")
	if derr := decompressCatalogCAS(
		filepath.Join(tmpdir, cvmfshash.ObjectPath(result.CatalogHash)+"C"), raw); derr != nil {
		t.Fatalf("decompress: %v", derr)
	}
	cat, err := Open(raw)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cat.Close()

	p1, p2 := MD5Path("/pkg/v1/" + NestedMarkerName)
	var size int64
	var hash []byte
	if qerr := cat.db.QueryRow(
		"SELECT size, hash FROM catalog WHERE md5path_1=? AND md5path_2=?",
		p1, p2).Scan(&size, &hash); qerr != nil {
		t.Fatalf("marker entry missing from the nested root catalog: %v", qerr)
	}
	if size != 0 {
		t.Errorf("marker size = %d, want 0", size)
	}
	// The marker must reference a real object, not a null hash: check verifies
	// content availability for regular files with -c.
	_, wantRaw, _ := NestedMarkerObject()
	if !bytes.Equal(hash, wantRaw) {
		t.Errorf("marker hash = %x, want the empty-file object hash %x", hash, wantRaw)
	}
}

// A marker already present in the tar must not be duplicated.
func TestExistingMarkerNotDuplicated(t *testing.T) {
	tmpdir := t.TempDir()
	now := time.Now().Unix()

	entries := []Entry{
		{FullPath: ".", Mode: fs.ModeDir | 0o755, Mtime: now, LinkCount: 1},
		{FullPath: NestedMarkerName, Name: NestedMarkerName, Mode: 0o644, Size: 0,
			Mtime: now, LinkCount: 1, Hash: []byte("existing-marker-hash-bytes--------------")[:20]},
	}

	result, err := BuildSubtree(context.Background(), SubtreeConfig{
		LeasePath: "pkg/v1",
		TempDir:   tmpdir,
	}, entries)
	if err != nil {
		t.Fatalf("BuildSubtree: %v", err)
	}
	if result.NeedsMarkerObject {
		t.Error("NeedsMarkerObject = true although the tar already provided the marker")
	}
}
