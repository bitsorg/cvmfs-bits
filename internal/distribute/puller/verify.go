// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package puller

import (
	"crypto/sha1" //nolint:gosec // CVMFS CAS keys are SHA-1; see pkg/cvmfshash
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"strings"
)

// verifyReader streams src while computing its SHA-1, and at EOF returns an
// error instead of io.EOF if the digest does not match expected (the hex part of
// the object hash). Feeding this to cas.Backend.Put makes the store abort before
// renaming, so a corrupted or substituted object is never installed
// (matching pkg/cvmfshash: CAS key = SHA-1 of the compressed object bytes).
type verifyReader struct {
	src      io.Reader
	h        hash.Hash
	expected string
	checked  bool
}

func newVerifyReader(src io.Reader, expectedHexHash string) *verifyReader {
	return &verifyReader{src: src, h: sha1.New(), expected: expectedHexHash} //nolint:gosec
}

func (v *verifyReader) Read(p []byte) (int, error) {
	n, err := v.src.Read(p)
	if n > 0 {
		v.h.Write(p[:n])
	}
	if err == io.EOF && !v.checked {
		v.checked = true
		got := hex.EncodeToString(v.h.Sum(nil))
		if !strings.EqualFold(got, v.expected) {
			return n, fmt.Errorf("hash mismatch: want %s got %s", v.expected, got)
		}
	}
	return n, err
}

// hexPrefix returns the leading lowercase-hex run of a CVMFS object hash,
// dropping any content-type suffix (an uppercase letter, e.g. "C" on a catalog,
// which is itself a hex digit and so must not count). The digest is the SHA-1
// hex of the compressed bytes; the suffix is not part of it.
func hexPrefix(s string) string {
	i := 0
	for i < len(s) && isHex(s[i]) {
		i++
	}
	return s[:i]
}

func isHex(b byte) bool {
	return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f')
}

// isHexName reports whether s is a safe CVMFS object name (alphanumeric, ≥3
// chars) so it cannot encode path traversal when turned into an object URL.
func isHexName(s string) bool {
	if len(s) < 3 {
		return false
	}
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		default:
			return false
		}
	}
	return true
}
