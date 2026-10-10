// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package receiver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cvmfs.io/prepub/internal/cas"
)

// sweepMinAge is how long a temp file must be idle before it counts as orphaned.
const sweepMinAge = 15 * time.Minute

// sweepTmpFiles removes temp files the CAS store (cas.LocalFS) left under
// {casRoot}/data/XX/ when a Put was interrupted by a crash. Only files last
// modified more than sweepMinAge before cutoff are removed, so neither a Put of
// this process nor one of another writer sharing the CAS root is touched.
func sweepTmpFiles(ctx context.Context, casRoot string, cutoff time.Time, logFn func(msg string, args ...any)) error {
	dataDir := filepath.Join(casRoot, "data")
	shards, err := os.ReadDir(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // CAS not initialised yet — nothing to sweep
		}
		return fmt.Errorf("sweepTmpFiles: reading %q: %w", dataDir, err)
	}
	var removed int
	for _, shard := range shards {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !shard.IsDir() || len(shard.Name()) != 2 {
			continue
		}
		subDir := filepath.Join(dataDir, shard.Name())
		entries, err := os.ReadDir(subDir)
		if err != nil {
			logFn("receiver: sweepTmpFiles skipping unreadable subdir", "dir", subDir, "error", err)
			continue
		}
		for _, e := range entries {
			if !strings.HasPrefix(e.Name(), cas.TempPrefix) {
				continue
			}
			info, err := e.Info()
			if err != nil || !info.ModTime().Before(cutoff.Add(-sweepMinAge)) {
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
		logFn("receiver: removed orphaned CAS temp files", "count", removed)
	}
	return nil
}
