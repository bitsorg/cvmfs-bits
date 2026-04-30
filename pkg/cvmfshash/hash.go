// Package cvmfshash provides CVMFS-compatible hashing and path utilities.
package cvmfshash

import (
	"crypto/sha1" //nolint:gosec // CVMFS wire protocol requires SHA-1; see payload.go
	"encoding/hex"
	"fmt"
	"io"
)

// ObjectPath returns the CAS path for a hash matching CVMFS directory structure.
// Format: "data/XX/XXXXXXXXXXX..." where XX is the first two hex characters of the hash.
// The hash may include a CVMFS content-type suffix (e.g., "C" for catalogs) as the
// 41st character; this is preserved as-is so the path maps directly to the on-disk layout.
// This 2-level directory structure balances fanout for fast lookups.
func ObjectPath(hash string) string {
	if len(hash) < 2 {
		return "data/00/" + hash
	}
	return "data/" + hash[:2] + "/" + hash
}

// HashReader reads all data from r, computes its SHA-1 hash, and returns
// the hex-encoded hash string and total bytes read.
//
// CVMFS CAS key convention: SHA-1 of the zlib-compressed object bytes.
// The C++ receiver (shash::MkFromHexPtr / MkFromSuffixedHexPtr) only recognises
// SHA-1 (40 hex chars), RIPEMD-160 (47 hex), and SHAKE-128 (49 hex).
// SHA-256 (64 hex chars) is NOT in the CVMFS hash enum and causes a PANIC in the
// C++ receiver, so all CAS keys must be computed with SHA-1 here.
func HashReader(r io.Reader) (hash string, n int64, err error) {
	h := sha1.New() //nolint:gosec // required by CVMFS CAS convention
	n, err = io.Copy(h, r)
	if err != nil {
		return "", n, fmt.Errorf("hashing reader: %w", err)
	}
	hash = hex.EncodeToString(h.Sum(nil))
	return hash, n, nil
}
