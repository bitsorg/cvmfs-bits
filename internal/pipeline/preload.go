// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Package pipeline — preload file generation.
//
// buildPreloadEntry compresses a list of CAS hashes into a CVMFS preload file,
// uploads it to CAS as a regular content object, and returns a synthetic
// catalog entry together with its CAS key.  The caller (runFromSortedEntries)
// appends both to the pipeline Result so the gateway commit includes the file.
//
// File naming convention (mirrors the CVMFS client expectation):
//
//	exe = "sw/ROOT/v6-24-06-4/bin/root"
//	→   "sw/ROOT/v6-24-06-4/bin/.root.cvmfspreload"
package pipeline

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1" //nolint:gosec // CVMFS CAS key = SHA-1(zlib(content))
	"encoding/hex"
	"fmt"
	"io/fs"
	"path"
	"strings"
	"time"

	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/internal/pipeline/upload"
	"cvmfs.io/prepub/pkg/cvmfscatalog"
)

// buildPreloadEntry compresses hashes into a preload file, uploads it to CAS,
// and returns a catalog Entry (to be appended to the pipeline result) and the
// CAS key string.
//
// hashes must be non-empty; the caller is responsible for ensuring this.
// Each hash is a lower-case hex SHA-1 string as produced by the compress stage.
func buildPreloadEntry(
	ctx context.Context,
	exe string,
	hashes []string,
	casBackend cas.Backend,
) (*cvmfscatalog.Entry, string, error) {
	// Build the raw file content: one hash per line, newline-terminated.
	content := []byte(strings.Join(hashes, "\n") + "\n")

	// Compress with zlib — CAS objects are always zlib-compressed.
	var buf bytes.Buffer
	zw, err := zlib.NewWriterLevel(&buf, zlib.DefaultCompression)
	if err != nil {
		// zlib.NewWriterLevel only errors for invalid level; DefaultCompression is always valid.
		return nil, "", fmt.Errorf("creating zlib writer: %w", err)
	}
	if _, err := zw.Write(content); err != nil {
		return nil, "", fmt.Errorf("compressing preload content: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, "", fmt.Errorf("closing zlib writer: %w", err)
	}
	compressed := buf.Bytes()

	// CAS key = hex(SHA-1(compressed)).
	//nolint:gosec // CVMFS mandates SHA-1 for all CAS keys.
	h := sha1.New()
	h.Write(compressed)
	casKey := hex.EncodeToString(h.Sum(nil))
	hashBytes, err := hex.DecodeString(casKey)
	if err != nil {
		return nil, "", fmt.Errorf("decoding cas key: %w", err)
	}

	// Upload to CAS (with retry).
	if err := upload.PutWithRetry(ctx, casBackend, casKey, compressed, int64(len(compressed))); err != nil {
		return nil, "", fmt.Errorf("uploading preload file: %w", err)
	}

	// Derive the preload file path from the executable path.
	// exe is a tar-relative path (no leading slash), e.g. "sw/ROOT/v6/bin/root".
	dir := path.Dir(exe)
	base := path.Base(exe)
	preloadName := "." + base + ".cvmfspreload"
	var fullPath string
	if dir == "." {
		fullPath = preloadName
	} else {
		fullPath = dir + "/" + preloadName
	}

	entry := &cvmfscatalog.Entry{
		FullPath:  fullPath,
		Name:      preloadName,
		Hash:      hashBytes,
		HashAlgo:  cvmfscatalog.HashSha1,
		CompAlgo:  cvmfscatalog.CompZlib,
		Size:      int64(len(content)), // uncompressed size, as CVMFS catalog expects
		Mode:      fs.FileMode(0o644),
		Mtime:     time.Now().Unix(),
		LinkCount: 1,
	}
	return entry, casKey, nil
}
