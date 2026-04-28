package cvmfscatalog

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// SyntheticAttrs returns the synthetic CVMFS xattrs for a catalog entry.
//
// Synthetic xattrs are attributes that are derived from the pipeline's own
// computation (hash, compression, chunking) rather than read from the source
// filesystem.  They can only be generated after the compress stage has run,
// which is why injection happens in the pipeline's post-wait patch loop rather
// than in the catalog builder stage.
//
// Generated keys (regular files only):
//
//   - user.cvmfs.hash — hex content hash with CVMFS algorithm suffix
//     (e.g. "abc123…-" for SHA-256, "abc123…" for SHA-1).
//   - user.cvmfs.compression — compression algorithm name: "zlib" or "none".
//   - user.cvmfs.chunk_list — (chunked files only) newline-separated list of
//     "offset:size:hash" records, one per chunk.
//
// Returns nil for directories and symlinks, which carry no content hash.
func SyntheticAttrs(e *Entry) map[string][]byte {
	if !e.Mode.IsRegular() || len(e.Hash) == 0 {
		return nil
	}

	m := make(map[string][]byte, 3)

	// user.cvmfs.hash — hex content hash with algorithm suffix.
	m["user.cvmfs.hash"] = []byte(hex.EncodeToString(e.Hash) + HashSuffix(e.HashAlgo))

	// user.cvmfs.compression — human-readable algorithm name.
	switch e.CompAlgo {
	case CompZlib:
		m["user.cvmfs.compression"] = []byte("zlib")
	case CompNone:
		m["user.cvmfs.compression"] = []byte("none")
	default:
		m["user.cvmfs.compression"] = []byte(fmt.Sprintf("algo%d", int(e.CompAlgo)))
	}

	// user.cvmfs.chunk_list — present only for chunked files.
	// Format per line: "offset:uncompressed_size:hex_hash_with_suffix"
	if len(e.Chunks) > 0 {
		var sb strings.Builder
		for i, ch := range e.Chunks {
			if i > 0 {
				sb.WriteByte('\n')
			}
			fmt.Fprintf(&sb, "%d:%d:%s",
				ch.Offset, ch.Size,
				hex.EncodeToString(ch.Hash)+HashSuffix(e.HashAlgo))
		}
		m["user.cvmfs.chunk_list"] = []byte(sb.String())
	}

	return m
}
