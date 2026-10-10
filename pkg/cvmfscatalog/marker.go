// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package cvmfscatalog

import (
	"bytes"
	"compress/zlib"
	"crypto/sha1" //nolint:gosec // CVMFS CAS key = SHA-1(zlib(content)); see pkg/cvmfshash
	"encoding/hex"
	"io/fs"
	"path"
	"sync"
)

// NestedMarkerName is the file CVMFS uses to mark a nested catalog root.
const NestedMarkerName = ".cvmfscatalog"

var (
	markerOnce    sync.Once
	markerHashHex string
	markerObject  []byte
	markerHashRaw []byte
)

// NestedMarkerObject returns the CAS object for an EMPTY file — the content of
// a .cvmfscatalog marker — as (hex hash, raw hash, compressed bytes).
//
// The key is SHA-1 over the zlib-compressed content, the same convention the
// compress pipeline uses for every other object (hash of the stored bytes), so
// the marker is fetched and verified by clients like any ordinary empty file.
// Computed once: the value is a constant.
func NestedMarkerObject() (hashHex string, hashRaw []byte, compressed []byte) {
	markerOnce.Do(func() {
		var buf bytes.Buffer
		zw := zlib.NewWriter(&buf)
		// Empty content: nothing to write.
		_ = zw.Close()
		markerObject = buf.Bytes()
		sum := sha1.Sum(markerObject) //nolint:gosec // CVMFS CAS convention
		markerHashRaw = sum[:]
		markerHashHex = hex.EncodeToString(sum[:])
	})
	return markerHashHex, markerHashRaw, markerObject
}

// nestedMarkerEntry builds the catalog entry for the marker file inside
// dirAbsPath.
//
// cvmfs_swissknife check requires every nested catalog root directory to
// contain this file (swissknife_check.cc:643-649, "nested catalog without
// marker at %s"); conversely a marker in a directory that is NOT a nested root
// is reported as "abandoned" (:394), so callers must add it only at real split
// points. It is an ordinary empty regular file — zero size, real content hash.
func nestedMarkerEntry(dirAbsPath string, mtime int64) Entry {
	hashHex, hashRaw, _ := NestedMarkerObject()
	_ = hashHex
	full := path.Join(dirAbsPath, NestedMarkerName)
	if dirAbsPath == "" {
		full = "/" + NestedMarkerName
	}
	return Entry{
		FullPath:  full,
		Name:      NestedMarkerName,
		Mode:      0o644, // regular file
		Size:      0,
		Mtime:     mtime,
		Hash:      hashRaw,
		HashAlgo:  HashSha1,
		CompAlgo:  CompZlib,
		LinkCount: 1,
	}
}

// hasMarkerIn reports whether entries already contain a .cvmfscatalog file
// directly inside dirAbsPath.
func hasMarkerIn(entries []Entry, dirAbsPath string) bool {
	for i := range entries {
		if entries[i].Mode&fs.ModeDir != 0 || entries[i].Mode&fs.ModeSymlink != 0 {
			continue
		}
		if path.Base(entries[i].FullPath) != NestedMarkerName {
			continue
		}
		if parent, ok := ParentAbsPath(entries[i].FullPath); ok && parent == dirAbsPath {
			return true
		}
	}
	return false
}
