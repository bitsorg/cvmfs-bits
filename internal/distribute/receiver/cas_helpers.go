// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package receiver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// casPath constructs the CVMFS CAS filesystem path for a given hash.
// Objects are stored at {root}/{hash[0:2]}/{hash}C where 'C' denotes a
// compressed (zlib) object — the standard CVMFS on-disk layout.
// hash must be at least 2 characters long; the caller must validate before calling.
func casPath(root, hash string) string {
	if len(hash) < 2 {
		// This should never happen if the caller validates the hash first;
		// this is a defensive check.
		return filepath.Join(root, hash, hash+"C")
	}
	return filepath.Join(root, hash[:2], hash+"C")
}

// sweepTmpFiles removes orphaned ".tmp" files left under the CAS by object
// writes (puller fetches) that were interrupted by a previous crash. It walks
// the two-char prefix subdirectories of casRoot and unlinks any stale temp file.
func sweepTmpFiles(ctx context.Context, casRoot string, logFn func(msg string, args ...any)) error {
	prefixEntries, err := os.ReadDir(casRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // CAS not initialised yet — nothing to sweep
		}
		return fmt.Errorf("sweepTmpFiles: reading CAS root %q: %w", casRoot, err)
	}
	var removed int
	for _, prefixEntry := range prefixEntries {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if !prefixEntry.IsDir() || len(prefixEntry.Name()) != 2 {
			continue
		}
		subDir := filepath.Join(casRoot, prefixEntry.Name())
		entries, err := os.ReadDir(subDir)
		if err != nil {
			logFn("receiver: sweepTmpFiles skipping unreadable subdir",
				"dir", subDir, "error", err)
			continue
		}
		for _, e := range entries {
			if !strings.HasSuffix(e.Name(), ".tmp") {
				continue
			}
			tmpPath := filepath.Join(subDir, e.Name())
			if err := os.Remove(tmpPath); err != nil && !os.IsNotExist(err) {
				logFn("receiver: sweepTmpFiles failed to remove", "path", tmpPath, "error", err)
			} else {
				removed++
			}
		}
	}
	if removed > 0 {
		logFn("receiver: removed orphaned .tmp files", "count", removed)
	}
	return nil
}

// randomToken returns a 32-hex-char (128-bit) random token, used to name the
// temporary file an object fetch streams into before the atomic rename.
func randomToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("receiver: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
