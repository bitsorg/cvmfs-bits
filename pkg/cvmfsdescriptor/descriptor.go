// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

// Package cvmfsdescriptor writes the cvmfs `ingestsql` SQLite descriptor
// (schema_revision 4) from prepub catalog entries.
//
// It is the producer side of ADR-0007 Variant A: instead of the prepub authoring
// a CVMFS catalog itself (pkg/cvmfscatalog), it emits a flat description of the
// files/dirs/symlinks to register — content referenced by hash, objects already
// in the store — and the canonical `cvmfs_swissknife ingestsql` builds and
// commits the catalog. The schema here is copied verbatim from cvmfs
// swissknife_ingestsql.cc::create_empty_database.
package cvmfsdescriptor

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"io/fs"
	"math"
	"strings"

	"cvmfs.io/prepub/pkg/cvmfscatalog"

	_ "modernc.org/sqlite"
)

// ExternalChunkSize / InternalChunkSize mirror cvmfs swissknife_ingestsql.cc:
// ingestsql derives chunk offsets as i*chunkSize and requires exactly
// ceil(size/chunkSize) hashes for a file. The prepub's ingestsql path must
// therefore chunk large files at this fixed size (ADR-0007 decision: align the
// prepub rather than extend ingestsql). Files <= ExternalChunkSize are a single
// blob (one hash) and are unaffected.
const (
	ExternalChunkSize = 24 * 1024 * 1024
	InternalChunkSize = 6 * 1024 * 1024
)

// schema is the exact descriptor DDL from ingestsql (schema_revision 4).
var schema = []string{
	`CREATE TABLE IF NOT EXISTS dirs (
		name  TEXT    PRIMARY KEY,
		mode  INTEGER NOT NULL DEFAULT 493,
		mtime INTEGER NOT NULL DEFAULT 0,
		owner INTEGER NOT NULL DEFAULT 0,
		grp   INTEGER NOT NULL DEFAULT 0,
		acl   TEXT    NOT NULL DEFAULT '',
		nested INTEGER DEFAULT 1);`,
	`CREATE TABLE IF NOT EXISTS files (
		name   TEXT    PRIMARY KEY,
		mode   INTEGER NOT NULL DEFAULT 420,
		mtime  INTEGER NOT NULL DEFAULT 0,
		owner  INTEGER NOT NULL DEFAULT 0,
		grp    INTEGER NOT NULL DEFAULT 0,
		size   INTEGER NOT NULL DEFAULT 0,
		hashes TEXT    NOT NULL DEFAULT '',
		internal INTEGER NOT NULL DEFAULT 0,
		compressed INTEGER NOT NULL DEFAULT 0);`,
	`CREATE TABLE IF NOT EXISTS links (
		name   TEXT    PRIMARY KEY,
		target TEXT    NOT NULL DEFAULT '',
		mtime  INTEGER NOT NULL DEFAULT 0,
		owner  INTEGER NOT NULL DEFAULT 0,
		grp    INTEGER NOT NULL DEFAULT 0,
		skip_if_file_or_dir INTEGER NOT NULL DEFAULT 0);`,
	`CREATE TABLE IF NOT EXISTS deletions (
		name      TEXT PRIMARY KEY,
		directory INTEGER NOT NULL DEFAULT 0,
		file      INTEGER NOT NULL DEFAULT 0,
		link      INTEGER NOT NULL DEFAULT 0);`,
	`CREATE TABLE IF NOT EXISTS properties (
		key   TEXT PRIMARY KEY,
		value TEXT NOT NULL);`,
	`INSERT INTO properties VALUES ('schema_revision', '4') ON CONFLICT DO NOTHING;`,
}

// Write builds the ingestsql descriptor at dbPath from entries. Each entry is
// classified by mode into the dirs / files / links tables. Regular files
// reference content by hash (Entry.Hash, or the ordered Entry.Chunks hashes).
//
// Assumptions, guaranteed by the prepub pipeline (ADR-0007):
//   - No file xattrs and no hardlinks (hardlinks are converted to symlinks
//     upstream); only dir POSIX ACLs would be representable, and bits has none,
//     so acl is always empty.
//   - Chunked files use fixed ExternalChunkSize boundaries; Write returns an
//     error if a file's hash count does not match ceil(size/ExternalChunkSize),
//     which catches content-defined (variable) chunking reaching this path.
func Write(dbPath string, entries []cvmfscatalog.Entry) (err error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("open descriptor db: %w", err)
	}
	defer func() {
		if cerr := db.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	if _, err = db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		return fmt.Errorf("set journal mode: %w", err)
	}
	for _, stmt := range schema {
		if _, err = db.Exec(stmt); err != nil {
			return fmt.Errorf("create schema: %w", err)
		}
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	for i := range entries {
		if err = insertEntry(tx, &entries[i]); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

func insertEntry(tx *sql.Tx, e *cvmfscatalog.Entry) error {
	name := strings.TrimPrefix(e.FullPath, "/")
	if name == "" {
		return nil // subtree root; the lease path root is not a descriptor row
	}
	if e.IsDelete {
		return insertDeletion(tx, name, e)
	}
	switch {
	case e.Mode&fs.ModeSymlink != 0:
		_, err := tx.Exec(
			"INSERT INTO links(name,target,mtime,owner,grp,skip_if_file_or_dir) VALUES(?,?,?,?,?,0)",
			name, e.Symlink, e.Mtime, e.UID, e.GID)
		return err
	case e.Mode.IsDir():
		nested := 0
		if e.IsNestedRoot {
			nested = 1
		}
		_, err := tx.Exec(
			"INSERT INTO dirs(name,mode,mtime,owner,grp,acl,nested) VALUES(?,?,?,?,?,'',?)",
			name, posixMode(e.Mode), e.Mtime, e.UID, e.GID, nested)
		return err
	default:
		hashes, err := fileHashes(e)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		compressed := 0
		if e.CompAlgo == cvmfscatalog.CompZlib {
			compressed = 1
		}
		_, err = tx.Exec(
			"INSERT INTO files(name,mode,mtime,owner,grp,size,hashes,internal,compressed) VALUES(?,?,?,?,?,?,?,0,?)",
			name, posixMode(e.Mode), e.Mtime, e.UID, e.GID, e.Size, hashes, compressed)
		return err
	}
}

func insertDeletion(tx *sql.Tx, name string, e *cvmfscatalog.Entry) error {
	dir, file, link := 0, 0, 0
	switch {
	case e.Mode&fs.ModeSymlink != 0:
		link = 1
	case e.Mode.IsDir():
		dir = 1
	default:
		file = 1
	}
	_, err := tx.Exec(
		"INSERT INTO deletions(name,directory,file,link) VALUES(?,?,?,?)",
		name, dir, file, link)
	return err
}

// fileHashes returns the comma-separated unsuffixed hex content hash(es) for a
// regular file and validates the count against ingestsql's fixed-chunk rule.
func fileHashes(e *cvmfscatalog.Entry) (string, error) {
	var parts []string
	if len(e.Chunks) > 0 {
		parts = make([]string, len(e.Chunks))
		for i, c := range e.Chunks {
			parts[i] = hex.EncodeToString(c.Hash)
		}
	} else {
		if len(e.Hash) == 0 {
			return "", fmt.Errorf("regular file has no content hash")
		}
		parts = []string{hex.EncodeToString(e.Hash)}
	}
	if want := expectedChunks(e.Size); len(parts) != want {
		return "", fmt.Errorf(
			"hash count %d != ceil(size/%dMiB)=%d — ingestsql needs fixed %dMiB chunks",
			len(parts), ExternalChunkSize/(1024*1024), want, ExternalChunkSize/(1024*1024))
	}
	return strings.Join(parts, ","), nil
}

// expectedChunks mirrors ingestsql: ceil(size/ExternalChunkSize), minimum 1.
func expectedChunks(size int64) int {
	if size <= 0 {
		return 1
	}
	return int(math.Ceil(float64(size) / float64(ExternalChunkSize)))
}

// posixMode converts a Go fs.FileMode to the 12-bit POSIX permission mode
// (rwx + setuid/setgid/sticky) ingestsql expects; type bits are implied by the
// table the entry lands in.
func posixMode(m fs.FileMode) uint32 {
	out := uint32(m.Perm())
	if m&fs.ModeSetuid != 0 {
		out |= 0o4000
	}
	if m&fs.ModeSetgid != 0 {
		out |= 0o2000
	}
	if m&fs.ModeSticky != 0 {
		out |= 0o1000
	}
	return out
}
