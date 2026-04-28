// Package cvmfscatalog provides CVMFS-compatible catalog operations.
package cvmfscatalog

import (
	"crypto/md5"
	"encoding/binary"
	"io/fs"
)

// Hash algorithm IDs (matching CVMFS shash::Algorithms)
type HashAlgo int

const (
	HashSha1      HashAlgo = 1
	HashSha256    HashAlgo = 2
	HashRipeMD160 HashAlgo = 3
)

// Compression algorithm IDs
type CompAlgo int

const (
	CompNone CompAlgo = 0
	CompZlib CompAlgo = 1
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
	FlagHidden         = 0x8000
	FlagPosHash        = 8
	FlagPosComp        = 11
)

// ChunkRecord represents a single chunk of a chunked file.
type ChunkRecord struct {
	Offset int64  // byte offset in the uncompressed file
	Size   int64  // uncompressed size of this chunk
	Hash   []byte // raw SHA-256 bytes (= CAS key)
}

// Entry represents a single catalog entry.
type Entry struct {
	FullPath       string         // absolute path e.g. "/foo/bar"; "" for repo root
	Name           string         // filename only; "" for repo root
	Hash           []byte         // raw bytes; nil for dirs/symlinks
	HashAlgo       HashAlgo
	CompAlgo       CompAlgo
	Size           int64
	Mode           fs.FileMode // Go fs.FileMode
	Mtime          int64       // Unix seconds
	MtimeNs        int32
	UID, GID       uint32
	Symlink        string
	HardlinkGroup  uint32
	LinkCount      uint32 // 1 for normal non-hardlinked files/dirs
	IsHidden       bool
	IsNestedRoot   bool // set on root entry of a nested catalog
	Chunks         []ChunkRecord // for chunked files
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

// UnixMode converts Go fs.FileMode to the Unix mode integer stored in the catalog.
func UnixMode(m fs.FileMode) int64 {
	var t int64
	switch {
	case m.IsDir():
		t = 0o040000
	case m&fs.ModeSymlink != 0:
		t = 0o120000
	case m.IsRegular():
		t = 0o100000
	default:
		t = 0o100000
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
		f = FlagLink
	case e.Mode.IsRegular():
		f = FlagFile
		if len(e.Chunks) > 0 {
			f |= FlagFileChunk
		}
	default:
		f = FlagFile | FlagFileSpecial
	}
	if e.HashAlgo >= HashSha1 {
		f |= (int(e.HashAlgo) - 1) << FlagPosHash
	}
	f |= int(e.CompAlgo) << FlagPosComp
	if e.IsHidden {
		f |= FlagHidden
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

// HashAlgoFromFlags extracts the hash algorithm from a flags value.
func HashAlgoFromFlags(flags int) HashAlgo {
	return HashAlgo(((flags>>FlagPosHash)&7) + 1)
}
