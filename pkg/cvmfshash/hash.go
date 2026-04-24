// Package cvmfshash provides CVMFS-compatible hashing and path utilities.
package cvmfshash

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
)

// ObjectPath returns the CAS path for a hash matching CVMFS directory structure.
// Format: "data/XX/XXXXXXXXXXX...C" where XX is the first two hex characters of the hash.
// This 2-level directory structure balances fanout for fast lookups.
func ObjectPath(hash string) string {
	if len(hash) < 2 {
		return "data/00/" + hash
	}
	return "data/" + hash[:2] + "/" + hash
}

// HashReader reads all data from r, computes its SHA256 hash, and returns
// the hex-encoded hash string and total bytes read.
func HashReader(r io.Reader) (hash string, n int64, err error) {
	h := sha256.New()
	n, err = io.Copy(h, r)
	if err != nil {
		return "", n, fmt.Errorf("hashing reader: %w", err)
	}
	hash = hex.EncodeToString(h.Sum(nil))
	return hash, n, nil
}
