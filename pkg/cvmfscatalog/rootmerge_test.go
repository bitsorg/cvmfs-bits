// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package cvmfscatalog

import (
	"bytes"
	"compress/zlib"
	"context"
	"database/sql"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"cvmfs.io/prepub/pkg/cvmfshash"
)

// openFinalizedCatalog decompresses a finalized catalog CAS object (data/XY/hashC)
// back into a temporary SQLite file and opens it read-only so a test can inspect
// the entries that BuildFromRoot wrote.
func openFinalizedCatalog(t *testing.T, dir, hashHex string) *sql.DB {
	t.Helper()
	casFile := filepath.Join(dir, cvmfshash.ObjectPath(hashHex)+"C")
	raw, err := os.ReadFile(casFile)
	if err != nil {
		t.Fatalf("reading CAS catalog %s: %v", hashHex, err)
	}
	zr, err := zlib.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("zlib reader: %v", err)
	}
	defer zr.Close()
	plain, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	dbPath := filepath.Join(t.TempDir(), "inspect_"+hashHex+".db")
	if err := os.WriteFile(dbPath, plain, 0o644); err != nil {
		t.Fatalf("write db: %v", err)
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	return db
}

// entryFlags returns (flags, found) for the absolute path in the catalog DB.
func entryFlags(t *testing.T, db *sql.DB, absPath string) (int, bool) {
	t.Helper()
	p1, p2 := MD5Path(absPath)
	var flags int
	err := db.QueryRow(
		"SELECT flags FROM catalog WHERE md5path_1 = ? AND md5path_2 = ?", p1, p2,
	).Scan(&flags)
	switch err {
	case nil:
		return flags, true
	case sql.ErrNoRows:
		return 0, false
	default:
		t.Fatalf("querying %q: %v", absPath, err)
		return 0, false
	}
}

// TestBuildFromRootDeepChain verifies that BuildFromRoot, given a deep lease path
// "a/b/c" with a single leaf file, produces a single root-anchored catalog that
// contains directory entries for every ancestor component (/a, /a/b, /a/b/c) with
// FlagDir set, plus the leaf content entry with FlagFile set.
func TestBuildFromRootDeepChain(t *testing.T) {
	tmp := t.TempDir()

	leaf := Entry{
		FullPath: "leaf",
		Name:     "leaf",
		Hash:     []byte("0123456789abcdef0123456789abcdef"),
		HashAlgo: HashSha256,
		CompAlgo: CompZlib,
		Size:     42,
		Mode:     0o644,
		Mtime:    1000,
	}

	res, err := BuildFromRoot(context.Background(), SubtreeConfig{
		LeasePath: "a/b/c",
		TempDir:   tmp,
	}, []Entry{leaf})
	if err != nil {
		t.Fatalf("BuildFromRoot: %v", err)
	}

	// A flat ancestor chain + leaf needs no nested catalogs: exactly one catalog.
	if len(res.AllCatalogHashes) != 1 {
		t.Fatalf("AllCatalogHashes = %d catalogs; want 1 (no splits)", len(res.AllCatalogHashes))
	}
	if res.CatalogHash == "" || res.CatalogHashSuffixed != res.CatalogHash+"C" {
		t.Fatalf("bad CatalogHash/Suffixed: %q / %q", res.CatalogHash, res.CatalogHashSuffixed)
	}

	db := openFinalizedCatalog(t, tmp, res.CatalogHash)
	defer db.Close()

	// Ancestor directory chain: /a, /a/b, /a/b/c must each be present as dirs.
	for _, dir := range []string{"/a", "/a/b", "/a/b/c"} {
		flags, ok := entryFlags(t, db, dir)
		if !ok {
			t.Errorf("ancestor dir %q missing from catalog", dir)
			continue
		}
		if flags&FlagDir == 0 {
			t.Errorf("ancestor %q flags=%d; FlagDir not set", dir, flags)
		}
		if flags&FlagFile != 0 {
			t.Errorf("ancestor %q unexpectedly has FlagFile (flags=%d)", dir, flags)
		}
	}

	// Leaf content entry must be present under the lease path as a regular file.
	leafFlags, ok := entryFlags(t, db, "/a/b/c/leaf")
	if !ok {
		t.Fatalf("leaf entry /a/b/c/leaf missing from catalog")
	}
	if leafFlags&FlagFile == 0 {
		t.Errorf("leaf flags=%d; FlagFile not set", leafFlags)
	}
	if leafFlags&FlagDir != 0 {
		t.Errorf("leaf flags=%d; unexpectedly has FlagDir", leafFlags)
	}

	// Root entry "" exists (inserted by Create) and is a directory.
	if rootFlags, ok := entryFlags(t, db, ""); !ok {
		t.Errorf("root entry \"\" missing")
	} else if rootFlags&FlagDir == 0 {
		t.Errorf("root entry flags=%d; FlagDir not set", rootFlags)
	}

	// root_prefix must be "" (root-anchored) so the gateway merge against the
	// existing repository root computes MD5(path) directly.
	var rp string
	if err := db.QueryRow("SELECT value FROM properties WHERE key='root_prefix'").Scan(&rp); err != nil {
		t.Fatalf("reading root_prefix: %v", err)
	}
	if rp != "" {
		t.Errorf("root_prefix = %q; want \"\" (root-anchored catalog)", rp)
	}
}

// TestBuildFromRootRejectsEmptyLease confirms BuildFromRoot refuses a root-level
// publish (LeasePath == ""), which has no ancestor chain.
func TestBuildFromRootRejectsEmptyLease(t *testing.T) {
	_, err := BuildFromRoot(context.Background(), SubtreeConfig{
		LeasePath: "",
		TempDir:   t.TempDir(),
	}, []Entry{{FullPath: ".", Mode: fs.ModeDir | 0o755}})
	if err == nil {
		t.Fatal("BuildFromRoot(LeasePath=\"\") = nil error; want rejection")
	}
}
