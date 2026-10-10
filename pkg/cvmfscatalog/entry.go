// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Package cvmfscatalog creates and manages CVMFS catalogs (SQLite databases).
//
// A CVMFS catalog is a content-addressed SQLite database that records the
// filesystem namespace for one path scope in a CVMFS repository.  Each
// catalog covers one "lease path" subtree; cvmfs_receiver grafts them
// together at commit time.
//
// Primary API:
//   - [Create]: build a fresh catalog for a given root prefix.
//   - [Upsert] / [BatchUpsert]: insert or replace file, directory, or symlink entries.
//   - [Remove]: delete an entry and its associated chunk records.
//   - [AddNestedMount]: register a sub-catalog's graft point in the parent.
//   - [Finalize]: flush statistics, zlib-compress, compute SHA-1, and write
//     the catalog to the CAS layout (data/XY/hashC).
//   - [BuildSubtree]: orchestrate Create → Upsert → Finalize for a full lease path.
//
// Entry flag bits, hash algorithm IDs, and compression IDs are defined in
// this file (entry.go) and match the layout in the CVMFS source file
// cvmfs/catalog_sql.h.  See CATALOG.md in the repository root for a complete
// description of the catalog schema, flag bit layout, and CAS conventions.
package cvmfscatalog

import (
	"crypto/md5"
	"encoding/binary"
	"io/fs"
)

// HashAlgo is a content hash algorithm ID matching CVMFS shash::Algorithms
// (cvmfs/crypto/hash.h: kMd5=0, kSha1, kRmd160, kShake128).  The zero value
// means "no hash" (directories, symlinks); MD5 is never used for content.
type HashAlgo int

const (
	HashSha1      HashAlgo = 1 // kSha1; flag bits 8-10 = 0
	HashRipeMD160 HashAlgo = 2 // kRmd160; flag bits 8-10 = 1
	HashShake128  HashAlgo = 3 // kShake128 (160 output bits); flag bits 8-10 = 2
)

// Compression algorithm IDs matching CVMFS zlib::Algorithms in compression.h.
//
// CVMFS stores the compression algorithm in catalog flag bits 11-13 as the
// raw enum value (no offset adjustment, unlike hash algo which subtracts 1).
//
//	kZlibDefault  = 0  → bits 11-13 = 000  (zlib-compressed; the default)
//	kNoCompression = 1  → bits 11-13 = 001  (stored verbatim, no decompression)
//
// WARNING: Do NOT swap these values.  CompZlib MUST be 0 so that zlib-
// compressed files set bits 11-13 = 000 in the flags column, which the CVMFS
// client interprets as kZlibDefault and correctly decompresses on read.
// Assigning CompZlib = 1 (kNoCompression) causes the client to skip
// decompression, then fail a size check (compressed size ≠ catalog size) and
// quarantine the object, returning EIO on every subsequent read.
type CompAlgo int

const (
	CompZlib CompAlgo = 0 // kZlibDefault = 0  — compressed with zlib (CVMFS default)
	CompNone CompAlgo = 1 // kNoCompression = 1 — stored verbatim, no compression
)

// Catalog entry flag bits (from cvmfs/catalog_sql.h)
const (
	FlagDir            = 1
	FlagDirNestedMount = 2
	FlagFile           = 4
	FlagLink           = 8
	FlagFileSpecial    = 16
	FlagDirNestedRoot  = 32
	FlagFileChunk      = 64
	FlagFileExternal   = 128
	// FlagXattr is an INTERNAL prepub flag used only for in-memory statistics
	// tracking (SelfXattr delta).  It is NEVER written to the SQLite flags
	// column: CVMFS has no xattr flag bit — xattr presence is determined by
	// whether the xattr BLOB is NULL.  CVMFS uses bits 0-7 for entry types,
	// 8-10 hash algo, 11-13 compression, 14 bind-mountpoint, 15 hidden,
	// 16 direct-I/O and 17 bundle trigger, so this sits well above them.
	FlagXattr  = 1 << 30
	FlagHidden = 0x8000
)

// Bit-field shift positions for hash and compression algorithm IDs packed into
// the flags column.  Both are unexported because they are implementation
// details of Flags() and HashAlgoFromFlags(); callers never need to shift by
// hand.
const (
	flagHashShift = 8  // bits 8-10: hash algorithm (stored as HashAlgo-1)
	flagCompShift = 11 // bits 11-13: compression algorithm (stored as CompAlgo)
)

// ChunkRecord represents a single chunk of a chunked file.
type ChunkRecord struct {
	Offset int64  // byte offset in the uncompressed file
	Size   int64  // uncompressed size of this chunk
	Hash   []byte // raw SHA-1 digest of the compressed chunk (= CAS key)
}

// Entry represents a single catalog entry.
type Entry struct {
	FullPath      string // absolute path e.g. "/foo/bar"; "" for repo root
	Name          string // filename only; "" for repo root
	Hash          []byte // raw bytes; nil for dirs/symlinks
	HashAlgo      HashAlgo
	CompAlgo      CompAlgo
	Size          int64
	Mode          fs.FileMode // Go fs.FileMode
	Mtime         int64       // Unix seconds
	MtimeNs       int32
	UID, GID      uint32
	Symlink       string
	HardlinkGroup uint32
	LinkCount     uint32 // 1 for normal non-hardlinked files/dirs
	IsHidden      bool
	IsNestedRoot  bool // set on root entry of a nested catalog
	// IsDelete marks this entry as an explicit deletion request.  When true,
	// BuildSubtree removes the path from the catalog instead of upserting it.
	// Prefer setting this field over relying on nil Hash to signal deletion —
	// the nil-Hash convention is fragile: a regular file with a missing hash
	// is indistinguishable from an intentional deletion.
	IsDelete bool          `json:"is_delete,omitempty"`
	Chunks   []ChunkRecord // for chunked files
	// Xattr holds extended attributes to store in the catalog xattr BLOB.
	// A nil map means no xattrs; FlagXattr is set in the flags column when
	// this map is non-empty.  User xattrs (from the source tar PAX headers)
	// and synthetic xattrs (user.cvmfs.hash, user.cvmfs.compression,
	// user.cvmfs.chunk_list) are merged here before the entry is written.
	Xattr map[string][]byte
}

// MD5Path returns (md5path_1, md5path_2) for the given absolute CVMFS path.
// Root directory uses absPath=""; all others use the full path with leading "/".
func MD5Path(absPath string) (int64, int64) {
	sum := md5.Sum([]byte(absPath))
	p1 := int64(binary.LittleEndian.Uint64(sum[0:8]))
	p2 := int64(binary.LittleEndian.Uint64(sum[8:16]))
	return p1, p2
}

// ParentAbsPath returns the absolute path of the parent directory.
// For root ("") returns ("", false). For "/foo" returns ("", true).
func ParentAbsPath(absPath string) (string, bool) {
	if absPath == "" {
		return "", false
	}
	for i := len(absPath) - 1; i > 0; i-- {
		if absPath[i] == '/' {
			return absPath[:i], true
		}
	}
	return "", true // "/foo" → parent is root ""
}

// UnixMode converts Go fs.FileMode to the Unix mode integer stored in the
// catalog.  CVMFS stores st_mode verbatim and classifies entries with the
// S_IS* macros on it, so special files need their real S_IF* type bits.
func UnixMode(m fs.FileMode) int64 {
	var t int64
	switch {
	case m.IsDir():
		t = 0o040000
	case m&fs.ModeSymlink != 0:
		t = 0o120000
	case m&fs.ModeNamedPipe != 0:
		t = 0o010000 // S_IFIFO
	case m&fs.ModeSocket != 0:
		t = 0o140000 // S_IFSOCK
	case m&fs.ModeCharDevice != 0:
		t = 0o020000 // S_IFCHR (Go sets ModeDevice too)
	case m&fs.ModeDevice != 0:
		t = 0o060000 // S_IFBLK
	default:
		t = 0o100000 // S_IFREG
	}
	perm := int64(m.Perm())
	if m&fs.ModeSetuid != 0 {
		perm |= 0o4000
	}
	if m&fs.ModeSetgid != 0 {
		perm |= 0o2000
	}
	if m&fs.ModeSticky != 0 {
		perm |= 0o1000
	}
	return t | perm
}

// Flags computes the integer flags column value.
func (e *Entry) Flags() int {
	var f int
	switch {
	case e.Mode.IsDir():
		f = FlagDir
		if e.IsNestedRoot {
			f |= FlagDirNestedRoot
		}
	case e.Mode&fs.ModeSymlink != 0:
		f = FlagFile | FlagLink // as CVMFS writes symlinks (catalog_sql.cc)
	case e.Mode.IsRegular():
		f = FlagFile
		if len(e.Chunks) > 0 {
			f |= FlagFileChunk
		}
	default:
		f = FlagFile | FlagFileSpecial
	}
	if e.HashAlgo >= HashSha1 {
		f |= (int(e.HashAlgo) - 1) << flagHashShift
	}
	f |= int(e.CompAlgo) << flagCompShift
	if e.IsHidden {
		f |= FlagHidden
	}
	if len(e.Xattr) > 0 {
		f |= FlagXattr
	}
	return f
}

// Hardlinks encodes the hardlinks column: high 32 bits = group, low 32 bits = count.
func (e *Entry) Hardlinks() int64 {
	lc := e.LinkCount
	if lc == 0 {
		lc = 1
	}
	return int64(e.HardlinkGroup)<<32 | int64(lc)
}

// HashSuffix returns the CVMFS algorithm suffix string for a given HashAlgo.
//
//	SHA-1      → ""   (no suffix — the default and most common case)
//	RIPEMD-160 → "-rmd160"
//	SHAKE-128  → "-shake128"
//
// These match shash::kAlgorithmIds in cvmfs/crypto/hash.cc.  The suffix
// follows the hex digest in hash strings and CAS object paths, before any
// content-type suffix (e.g. "<hex>-rmd160C" for a catalog).
func HashSuffix(algo HashAlgo) string {
	switch algo {
	case HashRipeMD160:
		return "-rmd160"
	case HashShake128:
		return "-shake128"
	default:
		return ""
	}
}

// HashAlgoFromFlags extracts the hash algorithm from a flags value.
func HashAlgoFromFlags(flags int) HashAlgo {
	return HashAlgo(((flags >> flagHashShift) & 7) + 1)
}
