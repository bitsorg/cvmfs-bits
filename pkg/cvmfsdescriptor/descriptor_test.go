// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

package cvmfsdescriptor

import (
	"database/sql"
	"encoding/hex"
	"io/fs"
	"path/filepath"
	"testing"

	"cvmfs.io/prepub/pkg/cvmfscatalog"

	_ "modernc.org/sqlite"
)

func h(b byte) []byte {
	out := make([]byte, 32) // SHA-256 width
	for i := range out {
		out[i] = b
	}
	return out
}

func TestWriteDescriptor(t *testing.T) {
	entries := []cvmfscatalog.Entry{
		{FullPath: "pkg/foo/1.0", Mode: fs.ModeDir | 0o755, Mtime: 100, UID: 0, GID: 0, IsNestedRoot: true},
		{FullPath: "pkg/foo/1.0/bin", Mode: fs.ModeDir | 0o755, Mtime: 100},
		{FullPath: "pkg/foo/1.0/bin/foo", Mode: 0o755, Size: 12, Mtime: 100,
			Hash: h(0xaa), HashAlgo: cvmfscatalog.HashSha256, CompAlgo: cvmfscatalog.CompZlib},
		{FullPath: "pkg/foo/1.0/README", Mode: 0o644, Size: 5, Mtime: 100,
			Hash: h(0xbb), HashAlgo: cvmfscatalog.HashSha256, CompAlgo: cvmfscatalog.CompNone},
		{FullPath: "pkg/foo/1.0/bin/foo-link", Mode: fs.ModeSymlink | 0o777, Mtime: 100, Symlink: "foo"},
		{FullPath: "pkg/foo/1.0/big", Mode: 0o644, Size: ExternalChunkSize + 1, Mtime: 100,
			CompAlgo: cvmfscatalog.CompZlib,
			Chunks: []cvmfscatalog.ChunkRecord{
				{Offset: 0, Size: ExternalChunkSize, Hash: h(0x01)},
				{Offset: ExternalChunkSize, Size: 1, Hash: h(0x02)},
			}},
	}

	dbPath := filepath.Join(t.TempDir(), "descriptor.db")
	if err := Write(dbPath, entries); err != nil {
		t.Fatalf("Write: %v", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// schema_revision
	var rev string
	if err := db.QueryRow("SELECT value FROM properties WHERE key='schema_revision'").Scan(&rev); err != nil || rev != "4" {
		t.Fatalf("schema_revision = %q, %v; want 4", rev, err)
	}

	// counts
	assertCount(t, db, "dirs", 2)
	assertCount(t, db, "files", 3)
	assertCount(t, db, "links", 1)

	// nested flag: the package root is nested, the subdir is not
	var nested int
	db.QueryRow("SELECT nested FROM dirs WHERE name='pkg/foo/1.0'").Scan(&nested)
	if nested != 1 {
		t.Errorf("pkg root nested=%d, want 1", nested)
	}
	db.QueryRow("SELECT nested FROM dirs WHERE name='pkg/foo/1.0/bin'").Scan(&nested)
	if nested != 0 {
		t.Errorf("subdir nested=%d, want 0", nested)
	}
	// dir mode is decimal POSIX perm
	var mode int
	db.QueryRow("SELECT mode FROM dirs WHERE name='pkg/foo/1.0'").Scan(&mode)
	if mode != 0o755 {
		t.Errorf("dir mode=%d, want %d", mode, 0o755)
	}

	// single-blob file: one unsuffixed hex hash, compressed=1 (CompZlib)
	var hashes string
	var compressed int
	db.QueryRow("SELECT hashes,compressed FROM files WHERE name='pkg/foo/1.0/bin/foo'").Scan(&hashes, &compressed)
	if hashes != hex.EncodeToString(h(0xaa)) {
		t.Errorf("foo hashes=%q", hashes)
	}
	if compressed != 1 {
		t.Errorf("foo compressed=%d, want 1 (CompZlib)", compressed)
	}
	// verbatim file: compressed=0 (CompNone)
	db.QueryRow("SELECT compressed FROM files WHERE name='pkg/foo/1.0/README'").Scan(&compressed)
	if compressed != 0 {
		t.Errorf("README compressed=%d, want 0 (CompNone)", compressed)
	}

	// chunked file: two comma-joined hashes in order
	db.QueryRow("SELECT hashes FROM files WHERE name='pkg/foo/1.0/big'").Scan(&hashes)
	want := hex.EncodeToString(h(0x01)) + "," + hex.EncodeToString(h(0x02))
	if hashes != want {
		t.Errorf("big hashes=%q, want %q", hashes, want)
	}

	// symlink target
	var target string
	db.QueryRow("SELECT target FROM links WHERE name='pkg/foo/1.0/bin/foo-link'").Scan(&target)
	if target != "foo" {
		t.Errorf("symlink target=%q, want foo", target)
	}
}

// A file larger than one chunk but given a single whole-file hash must be
// rejected — this is content-defined (variable) chunking reaching the ingestsql
// path, which ingestsql cannot express.
func TestWriteRejectsMismatchedChunking(t *testing.T) {
	entries := []cvmfscatalog.Entry{
		{FullPath: "pkg/x", Mode: 0o644, Size: ExternalChunkSize + 1,
			Hash: h(0xcc), HashAlgo: cvmfscatalog.HashSha256, CompAlgo: cvmfscatalog.CompZlib},
	}
	err := Write(filepath.Join(t.TempDir(), "bad.db"), entries)
	if err == nil {
		t.Fatal("expected error for size>chunk with a single hash, got nil")
	}
}

func assertCount(t *testing.T, db *sql.DB, table string, want int) {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	if n != want {
		t.Errorf("%s count=%d, want %d", table, n, want)
	}
}
