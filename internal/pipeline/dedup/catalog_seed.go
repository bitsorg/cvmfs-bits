// SPDX-FileCopyrightText: 2026 CERN (European Organization for Nuclear Research)
// SPDX-License-Identifier: Apache-2.0

package dedup

// catalog_seed.go — catalog-based Bloom filter seeding.
//
// Instead of walking the CAS filesystem (O(files) stat syscalls, gets slower
// as the CAS grows), this file seeds the dedup filter by fetching the CVMFS
// catalog tree from the Stratum 0 HTTP server and querying it with SQLite.
//
// Why this is faster:
//   - Each catalog is a compact (~1 MB) SQLite file; queries are memory-mapped.
//   - A repo with 10M objects and 5000 nested catalogs downloads ~5 GB total
//     catalog data but all queries are in-process — no per-file syscalls against
//     a potentially slow local filesystem.
//   - Catalog download (HTTP + zlib) is network-bound and can overlap with
//     service startup probe checks.
//
// Why this is more correct:
//   - Only objects referenced by the committed catalog are seeded.  Orphan
//     objects in the CAS from failed/aborted publishes are intentionally excluded
//     — the dedup filter should not claim those are "already present".
//   - The Bloom filter false-positive rate is bounded by the catalog entry count,
//     not the CAS object count (which includes stale objects).

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite" // sqlite driver: side-effect registration

	"cvmfs.io/prepub/pkg/cvmfscatalog"
	"cvmfs.io/prepub/pkg/observe"
)

// CollectCatalogHashes fetches the CVMFS catalog tree for repoName from
// stratum0URL and returns all content-object hashes referenced by any catalog
// in the tree as 40-char hex strings.
//
// The returned slice contains:
//   - Bulk file hashes (catalog.hash where entry is a regular file or chunked file)
//   - Per-chunk hashes (chunks.hash)
//
// Catalog hashes themselves are NOT included: they are generated fresh each
// publish and are never deduplication candidates.
//
// When stratum0URL is empty, CollectCatalogHashes returns nil immediately.
// When the manifest returns 404 (repository not yet initialised), it returns
// nil with no error — the filter starts empty, which is correct.
// Any other error is returned to the caller; callers should treat it as
// non-fatal and fall back to an empty filter rather than aborting startup.
func CollectCatalogHashes(ctx context.Context, stratum0URL, repoName, tempDir string, httpClient *http.Client, obs *observe.Provider) ([]string, error) {
	if stratum0URL == "" {
		return nil, nil
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	// Fetch the manifest to obtain the current root catalog hash.
	manifestURL := stratum0URL + "/" + repoName + "/.cvmfspublished"
	req, err := http.NewRequestWithContext(ctx, "GET", manifestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("catalog seed: building manifest request: %w", err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("catalog seed: fetching manifest from %s: %w", manifestURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// Repository has never been published — no catalog exists yet.
		obs.Logger.Info("dedup catalog seed: repository not yet published — starting with empty filter")
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("catalog seed: manifest HTTP %d: %s", resp.StatusCode, manifestURL)
	}

	manifestData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("catalog seed: reading manifest: %w", err)
	}
	manifest, err := cvmfscatalog.ParseManifest(manifestData)
	if err != nil {
		return nil, fmt.Errorf("catalog seed: parsing manifest: %w", err)
	}

	obs.Logger.Info("dedup catalog seed: starting catalog tree walk",
		"root_hash", manifest.RootHash[:8]+"...", "repo", repoName)

	var allHashes []string
	if err := walkCatalogTree(ctx, httpClient, stratum0URL, repoName, manifest.RootHash, tempDir, &allHashes, obs); err != nil {
		return allHashes, err // return partial results for context-cancelled walks
	}

	obs.Logger.Info("dedup catalog seed: catalog tree walk complete",
		"hashes_collected", len(allHashes), "repo", repoName)
	return allHashes, nil
}

// walkCatalogTree downloads the catalog identified by hashHex, collects all
// content-object hashes from it, then recursively processes nested catalogs.
// On context cancellation the partial hash list collected so far is preserved
// and the context error is returned, allowing the caller to seed a partial filter.
func walkCatalogTree(ctx context.Context, client *http.Client, stratum0URL, repoName, hashHex, tempDir string, hashes *[]string, obs *observe.Provider) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Download the compressed catalog and decompress it to a temp SQLite file.
	// Use a unique suffix per catalog hash so concurrent future callers are safe.
	tmpPath := filepath.Join(tempDir, "dedup-cat-"+hashHex+".db")
	defer os.Remove(tmpPath) // clean up immediately after we're done with this catalog

	if dlErr := cvmfscatalog.DownloadCatalog(ctx, client, stratum0URL, repoName, hashHex, tmpPath); dlErr != nil {
		if errors.Is(dlErr, cvmfscatalog.ErrCatalogNotFound) {
			obs.Logger.Warn("dedup catalog seed: catalog not found (skipping)", "hash", hashHex[:8])
			return nil // non-fatal: skip missing nested catalogs
		}
		return fmt.Errorf("catalog seed: downloading %s: %w", hashHex[:8], dlErr)
	}

	db, err := sql.Open("sqlite", tmpPath)
	if err != nil {
		return fmt.Errorf("catalog seed: opening catalog %s: %w", hashHex[:8], err)
	}
	defer db.Close()

	// ── File bulk hashes ────────────────────────────────────────────────────────
	// Query regular file entries: exclude directories (FlagDir = bit 0) and
	// symlinks (FlagLink = bit 3).  Chunked-file entries (FlagFileChunk = bit 6)
	// carry the bulk hash of the whole file in hash; their per-chunk hashes are
	// in the chunks table.  Both are content-addressed objects we care about.
	fileRows, err := db.QueryContext(ctx, `
		SELECT hash FROM catalog
		WHERE hash IS NOT NULL
		  AND length(hash) > 0
		  AND (flags & 1) = 0
		  AND (flags & 8) = 0
	`)
	if err != nil {
		return fmt.Errorf("catalog seed: querying file hashes in %s: %w", hashHex[:8], err)
	}
	if err := collectSHA1Blobs(fileRows, hashes); err != nil {
		return fmt.Errorf("catalog seed: scanning file hashes in %s: %w", hashHex[:8], err)
	}

	// ── Chunk hashes ────────────────────────────────────────────────────────────
	chunkRows, err := db.QueryContext(ctx, `
		SELECT hash FROM chunks
		WHERE hash IS NOT NULL AND length(hash) > 0
	`)
	if err != nil {
		return fmt.Errorf("catalog seed: querying chunk hashes in %s: %w", hashHex[:8], err)
	}
	if err := collectSHA1Blobs(chunkRows, hashes); err != nil {
		return fmt.Errorf("catalog seed: scanning chunk hashes in %s: %w", hashHex[:8], err)
	}

	// ── Nested catalog list ─────────────────────────────────────────────────────
	// Collect nested catalog hashes before closing the db so we can recurse
	// after the file handle is freed.
	nestedRows, err := db.QueryContext(ctx, `
		SELECT sha1 FROM nested_catalogs WHERE sha1 != ''
		UNION ALL
		SELECT sha1 FROM bind_mountpoints WHERE sha1 != ''
	`)
	if err != nil {
		// bind_mountpoints may not exist in older catalogs — fall back to
		// querying only nested_catalogs.
		nestedRows, err = db.QueryContext(ctx, `
			SELECT sha1 FROM nested_catalogs WHERE sha1 != ''
		`)
		if err != nil {
			return fmt.Errorf("catalog seed: querying nested catalogs in %s: %w", hashHex[:8], err)
		}
	}

	var nestedHashes []string
	for nestedRows.Next() {
		var sha1 string
		if scanErr := nestedRows.Scan(&sha1); scanErr != nil || sha1 == "" {
			continue
		}
		nestedHashes = append(nestedHashes, sha1)
	}
	if rowErr := nestedRows.Err(); rowErr != nil {
		obs.Logger.Warn("dedup catalog seed: error scanning nested_catalogs rows (continuing)",
			"hash", hashHex[:8], "error", rowErr)
	}
	nestedRows.Close()
	db.Close() // close the db now that we have everything we need from this catalog

	// ── Recurse into nested catalogs ────────────────────────────────────────────
	for _, nestedHash := range nestedHashes {
		if err := walkCatalogTree(ctx, client, stratum0URL, repoName, nestedHash, tempDir, hashes, obs); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err // propagate context errors so the caller can use partial results
			}
			// Non-fatal: a missing or corrupt nested catalog should not prevent
			// the rest of the tree from being walked.
			obs.Logger.Warn("dedup catalog seed: skipping nested catalog (non-fatal)",
				"parent_hash", hashHex[:8], "nested_hash", nestedHash[:8], "error", err)
		}
	}

	return nil
}

// collectSHA1Blobs drains a single-column BLOB result set, converts each
// 20-byte row (SHA-1) to a 40-char hex string, and appends it to dst.
// Rows whose BLOB is not exactly 20 bytes are silently skipped: they represent
// objects stored under a different hash algorithm (SHA-256, RipeMD-160) and
// are keyed differently in the CAS — the prepub pipeline always generates
// SHA-1 keys and would never match those objects in a dedup check.
func collectSHA1Blobs(rows *sql.Rows, dst *[]string) error {
	defer rows.Close()
	for rows.Next() {
		var blob []byte
		if err := rows.Scan(&blob); err != nil {
			continue // skip unreadable rows
		}
		if len(blob) == 20 { // SHA-1 is exactly 20 bytes
			*dst = append(*dst, hex.EncodeToString(blob))
		}
		// Non-SHA-1 blobs (e.g. SHA-256 = 32 bytes) are intentionally ignored:
		// prepub never queries the dedup filter with those keys.
	}
	return rows.Err()
}
