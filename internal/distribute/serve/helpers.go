// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package serve

import "strings"

// segmentBetween returns the single path segment between the first occurrence of
// pre and the following suf. It rejects an empty segment or one containing a
// slash (so a one-segment id like a repo or transaction id is matched exactly).
func segmentBetween(p, pre, suf string) (string, bool) {
	i := strings.Index(p, pre)
	if i < 0 {
		return "", false
	}
	rest := p[i+len(pre):]
	j := strings.Index(rest, suf)
	if j < 0 {
		return "", false
	}
	seg := rest[:j]
	if seg == "" || strings.Contains(seg, "/") {
		return "", false
	}
	return seg, true
}

// hashFromDataPath extracts the object hash from a ".../data/{xx}/{rest}" path.
func hashFromDataPath(p string) (string, bool) {
	i := strings.LastIndex(p, "/data/")
	if i < 0 {
		return "", false
	}
	rest := p[i+len("/data/"):]
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 || len(parts[0]) != 2 || parts[1] == "" || strings.Contains(parts[1], "/") {
		return "", false
	}
	hash := parts[0] + parts[1]
	if !validObjectName(hash) {
		return "", false
	}
	return hash, true
}

// validObjectName guards against path traversal: a hash (optionally with a CVMFS
// suffix letter) is alphanumeric only — no dots or slashes.
func validObjectName(s string) bool {
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
