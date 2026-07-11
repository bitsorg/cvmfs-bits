// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package cvmfscatalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// HasEntry reports whether a catalog entry (of any kind) exists at absPath in
// this catalog. absPath is the CVMFS absolute path with a leading "/" (the same
// form MD5Path expects); the repository root is "".
func (c *Catalog) HasEntry(absPath string) (bool, error) {
	p1, p2 := MD5Path(absPath)
	var one int
	err := c.db.QueryRow(
		"SELECT 1 FROM catalog WHERE md5path_1 = ? AND md5path_2 = ?", p1, p2,
	).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("looking up entry %q: %w", absPath, err)
	}
	return true, nil
}

// longestNestedAncestor returns the nested_catalogs row whose path is absPath
// itself or the longest proper ancestor of absPath registered in THIS catalog.
// A catalog only records its direct child nested mounts, so this is used to
// decide whether to descend into a child catalog (proper ancestor) or whether
// absPath is itself a mount point (exact match). found=false means neither.
func (c *Catalog) longestNestedAncestor(absPath string) (mount, hashHex string, found bool, err error) {
	// Candidate set: absPath and every ancestor down to "/x" (root "" is never
	// a nested mount). Ordered longest-first by the SQL query below.
	var cands []string
	for p := absPath; p != ""; {
		cands = append(cands, p)
		parent, ok := ParentAbsPath(p)
		if !ok {
			break
		}
		p = parent
	}
	if len(cands) == 0 {
		return "", "", false, nil
	}

	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(cands)), ",")
	args := make([]interface{}, len(cands))
	for i, v := range cands {
		args[i] = v
	}
	row := c.db.QueryRow(
		"SELECT path, sha1 FROM nested_catalogs WHERE path IN ("+placeholders+
			") ORDER BY length(path) DESC LIMIT 1", args...)
	var mp, sha string
	if scanErr := row.Scan(&mp, &sha); scanErr != nil {
		if errors.Is(scanErr, sql.ErrNoRows) {
			return "", "", false, nil
		}
		return "", "", false, fmt.Errorf("querying nested ancestor of %q: %w", absPath, scanErr)
	}
	return mp, sha, true, nil
}

// PathExists reports whether leasePath is already present in the published
// repository, walking nested catalogs from the current root as needed.
//
// leasePath is a repo-relative publish path (e.g. "releases/x86_64-el8/Packages/
// ROOT/v6.38.00-3"); it is normalised to a CVMFS absolute path internally. A
// package/version directory is published as a nested-catalog mountpoint, so the
// walk descends through ancestor mounts until it can answer authoritatively.
//
// Returns (false, nil) when the repository has never been published (no manifest)
// — nothing exists yet. client may be nil (http.DefaultClient is used).
//
// This is a best-effort fast-path check for fail-fast reservation: it downloads
// the root catalog (and any ancestor nested catalogs on the path), so callers
// should treat an error as "could not determine" and proceed rather than block.
func PathExists(ctx context.Context, client *http.Client, stratum0URL, repo, leasePath string) (bool, error) {
	abs := normalizeLeasePathForNested(leasePath)
	if abs == "" {
		return true, nil // repository root always exists
	}

	rootSuffixed, err := FetchManifestRootHash(ctx, client, stratum0URL, repo)
	if err != nil {
		return false, fmt.Errorf("fetching manifest root hash: %w", err)
	}
	if rootSuffixed == "" {
		return false, nil // repo never published — nothing exists yet
	}
	curHash := strings.TrimSuffix(rootSuffixed, "C")

	tmpDir, err := os.MkdirTemp("", "cvmfs-exists-*")
	if err != nil {
		return false, fmt.Errorf("creating temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	// Bound the descent so a pathological/looping nested chain cannot spin
	// forever; the depth of any real publish path is small.
	for depth := 0; depth < 64; depth++ {
		dbPath := filepath.Join(tmpDir, curHash+".db")
		if dlErr := DownloadCatalog(ctx, client, stratum0URL, repo, curHash, dbPath); dlErr != nil {
			if errors.Is(dlErr, ErrCatalogNotFound) {
				return false, nil
			}
			return false, fmt.Errorf("downloading catalog %s: %w", curHash, dlErr)
		}
		cat, openErr := Open(dbPath)
		if openErr != nil {
			return false, fmt.Errorf("opening catalog %s: %w", curHash, openErr)
		}

		mount, childHash, found, ancErr := cat.longestNestedAncestor(abs)
		if ancErr != nil {
			cat.Close()
			return false, ancErr
		}
		if found && mount == abs {
			cat.Close()
			return true, nil // absPath is itself a nested-catalog mountpoint
		}
		if found {
			// A proper ancestor of absPath is a nested mount — descend into it.
			cat.Close()
			_ = os.Remove(dbPath)
			curHash = childHash
			continue
		}
		// No child nested mount on the path in this catalog: absPath, if it
		// exists at all, is a plain entry owned here.
		has, hasErr := cat.HasEntry(abs)
		cat.Close()
		return has, hasErr
	}
	return false, fmt.Errorf("nested-catalog walk exceeded max depth for %q", abs)
}
