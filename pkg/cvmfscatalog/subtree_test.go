// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package cvmfscatalog

import (
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
