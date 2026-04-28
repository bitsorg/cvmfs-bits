package cvmfscatalog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"cvmfs.io/prepub/pkg/cvmfshash"
)

// MergeConfig holds configuration for the Merge operation.
type MergeConfig struct {
	Stratum0URL string
	RepoName    string
	LeasePath   string      // e.g. "atlas/24.0" (no leading/trailing slash)
	TempDir     string
	HTTPClient  *http.Client // nil = http.DefaultClient
}

// MergeResult holds the result of a merge operation.
type MergeResult struct {
	OldRootHash         string   // plain hex from manifest (no suffix)
	NewRootHash         string   // plain hex of new root catalog (no suffix)
	NewRootHashSuffixed string   // new root hash with CVMFS algorithm suffix (e.g. "abc-" for SHA-256)
	AllCatalogHashes    []string // hashes in order: leaf first, root last (no suffix)
}

// HashSuffix returns the CVMFS algorithm suffix string for a given algorithm.
// SHA-1 → "", SHA-256 → "-", RipeMD-160 → "~"
func HashSuffix(algo HashAlgo) string {
	switch algo {
	case HashSha1:
		return ""
	case HashSha256:
		return "-"
	case HashRipeMD160:
		return "~"
	default:
		return ""
	}
}

// catalogChainNode is one level in the root→leaf catalog hierarchy.
type catalogChainNode struct {
	cat  *Catalog
	path string // absolute root prefix for this catalog ("" for repo root)
}

// Merge fetches the current root catalog from stratum0, walks the nested
// catalog tree to find the deepest catalog covering LeasePath, applies the
// given entries to it, then re-finalises each catalog from leaf back up to
// root — updating each parent's nested_catalogs row with the new child hash.
//
// All modified catalog files are written as compressed CAS objects under
// TempDir (path format: data/XY/hashC).  The caller is responsible for
// uploading those files to the CAS backend before calling gateway Commit.
func Merge(ctx context.Context, cfg MergeConfig, entries []Entry) (*MergeResult, error) {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}

	// ── Step 1: Fetch the current manifest ──────────────────────────────────
	manifestURL := cfg.Stratum0URL + "/" + cfg.RepoName + "/.cvmfspublished"
	resp, err := cfg.HTTPClient.Get(manifestURL)
	if err != nil {
		return nil, fmt.Errorf("fetching manifest: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("manifest http %d: %s", resp.StatusCode, manifestURL)
	}
	manifestData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading manifest: %w", err)
	}
	manifest, err := ParseManifest(manifestData)
	if err != nil {
		return nil, fmt.Errorf("parsing manifest: %w", err)
	}

	result := &MergeResult{OldRootHash: manifest.RootHash}

	// Finalize() always SHA-256 hashes the compressed bytes, so the suffix on
	// new_root_hash is always "-" regardless of the old repository's algorithm.
	// hashAlgo below is kept for diagnostics / future per-repo algo selection.
	hashAlgo := manifest.HashAlgo
	if hashAlgo == 0 {
		hashAlgo = HashSha1
	}
	_ = hashAlgo

	// ── Step 2: Download and open root catalog ───────────────────────────────
	rootDBPath := filepath.Join(cfg.TempDir, "root.db")
	if err := DownloadCatalog(ctx, cfg.HTTPClient, cfg.Stratum0URL, cfg.RepoName,
		manifest.RootHash, rootDBPath); err != nil {
		return nil, fmt.Errorf("downloading root catalog: %w", err)
	}
	rootCat, err := Open(rootDBPath)
	if err != nil {
		return nil, fmt.Errorf("opening root catalog: %w", err)
	}

	// ── Step 3: Walk nested catalogs to find the target ──────────────────────
	// The chain is built root-first; we will walk it bottom-up during
	// finalization so that parent nested_catalogs rows are updated after their
	// child hashes are known.
	targetAbsPath := normalizeLeasePathForNested(cfg.LeasePath)
	chain, err := buildCatalogChain(ctx, cfg, rootCat, targetAbsPath)
	if err != nil {
		// buildCatalogChain closed any nested catalogs it opened; close root here.
		rootCat.Close()
		return nil, fmt.Errorf("walking nested catalogs: %w", err)
	}
	// Fix H1: ensure every catalog in the chain is closed when Merge returns,
	// regardless of whether subsequent steps succeed or fail.
	// Finalize calls c.db.Close() internally, but Close() is idempotent (no-op
	// if c.db is already nil), so the defers below are safe in all cases.
	for i := range chain {
		i := i
		defer chain[i].cat.Close()
	}

	// ── Step 4: Apply entries to the leaf catalog ────────────────────────────
	leafCat := chain[len(chain)-1].cat
	for _, entry := range entries {
		if isDeletion(entry) {
			rmErr := leafCat.Remove(entry.FullPath)
			if rmErr == nil {
				continue
			}
			// Fix N4: ErrNotFound means the entry was already absent.
			// Treat this as a no-op (idempotent deletion) rather than a failure,
			// because a retry of a partial publish should not break on entries
			// that were already removed in a prior attempt.
			if errors.Is(rmErr, ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("removing entry %q: %w", entry.FullPath, rmErr)
		}
		if uErr := leafCat.Upsert(entry); uErr != nil {
			return nil, fmt.Errorf("upserting entry %q: %w", entry.FullPath, uErr)
		}
	}

	// ── Step 5: Finalise bottom-up, updating parent nested_catalogs rows ─────
	// Invariants maintained by the loop:
	//   childHash / childSize / childPath — hash, compressed size, and absolute
	//   mount-point path of the catalog finalised in the previous iteration.
	//   childDelta — statistics changes from the child catalog.
	//   These are empty/zero on the first (leaf) iteration.
	var childHash string
	var childSize int64
	var childPath string
	var childDelta Statistics

	for i := len(chain) - 1; i >= 0; i-- {
		node := chain[i]

		// If there is a child (i.e. this is not the first iteration), update
		// the nested_catalogs row in the current catalog with the child's new hash.
		if childHash != "" {
			if updErr := node.cat.UpdateNestedMount(childPath, childHash, childSize); updErr != nil {
				return nil, fmt.Errorf("updating nested mount %q in catalog at %q: %w",
					childPath, node.path, updErr)
			}

			// Propagate child statistics: child's total change (self+subtree) becomes parent's subtree change
			node.cat.delta.SubtreeRegular += childDelta.SelfRegular + childDelta.SubtreeRegular
			node.cat.delta.SubtreeSymlink += childDelta.SelfSymlink + childDelta.SubtreeSymlink
			node.cat.delta.SubtreeDir += childDelta.SelfDir + childDelta.SubtreeDir
			node.cat.delta.SubtreeNested += childDelta.SelfNested + childDelta.SubtreeNested
			node.cat.delta.SubtreeXattr += childDelta.SelfXattr + childDelta.SubtreeXattr
			node.cat.delta.SubtreeExternal += childDelta.SelfExternal + childDelta.SubtreeExternal
			node.cat.delta.SubtreeSpecial += childDelta.SelfSpecial + childDelta.SubtreeSpecial
		}

		// Finalize: increment revision, compress, SHA-256 hash, write CAS file.
		hash, delta, finalErr := node.cat.Finalize(cfg.TempDir)
		if finalErr != nil {
			return nil, fmt.Errorf("finalizing catalog at %q: %w", node.path, finalErr)
		}

		// Stat the compressed catalog file to get the size for parent's row.
		casFile := filepath.Join(cfg.TempDir, cvmfshash.ObjectPath(hash)+"C")
		fi, statErr := os.Stat(casFile)
		if statErr != nil {
			return nil, fmt.Errorf("stat finalized catalog %s: %w", hash, statErr)
		}

		result.AllCatalogHashes = append(result.AllCatalogHashes, hash)
		childHash = hash
		childSize = fi.Size()
		childPath = node.path
		childDelta = delta
	}

	// After the loop: childHash holds the new root catalog hash.
	result.NewRootHash = childHash
	result.NewRootHashSuffixed = childHash + HashSuffix(HashSha256)

	return result, nil
}

// buildCatalogChain starts at rootCat and descends into nested catalogs as
// far as targetAbsPath allows.  It returns the chain root-first; the last
// element is the deepest catalog that covers targetAbsPath.
//
// For a targetAbsPath of "/atlas/24.0" the function checks whether "/atlas"
// is a nested mount point in the root catalog.  If so it downloads that
// catalog and checks whether "/atlas/24.0" is a nested mount point within it,
// and so on.  The first ancestor path that is NOT a mount point ends the walk;
// the catalog opened at that point is the target.
//
// Fix H1: any nested catalog opened before an error is returned is closed
// before this function returns, preventing DB handle leaks.  rootCat is NOT
// closed here — it is owned by the caller (Merge).
func buildCatalogChain(ctx context.Context, cfg MergeConfig,
	rootCat *Catalog, targetAbsPath string) ([]catalogChainNode, error) {

	chain := []catalogChainNode{{cat: rootCat, path: ""}}
	current := rootCat

	// closeOpened closes all nested catalogs (index 1+) opened so far.
	// rootCat at index 0 is excluded — it is owned by the caller.
	closeOpened := func() {
		for i := 1; i < len(chain); i++ {
			chain[i].cat.Close()
		}
	}

	for _, ancestor := range ancestorPaths(targetAbsPath) {
		hashHex, _, found, err := current.FindNestedMount(ancestor)
		if err != nil {
			closeOpened()
			return nil, fmt.Errorf("looking up nested mount %q: %w", ancestor, err)
		}
		if !found {
			// No nested catalog at this ancestor; current catalog covers the target.
			break
		}

		// Download the nested catalog.  Use the hash as the filename so that
		// multiple levels with the same path component never collide.
		nestedDBPath := filepath.Join(cfg.TempDir, "nested_"+hashHex+".db")
		if dlErr := DownloadCatalog(ctx, cfg.HTTPClient, cfg.Stratum0URL, cfg.RepoName,
			hashHex, nestedDBPath); dlErr != nil {
			closeOpened()
			return nil, fmt.Errorf("downloading nested catalog at %q (hash %s): %w",
				ancestor, hashHex, dlErr)
		}
		nestedCat, openErr := Open(nestedDBPath)
		if openErr != nil {
			closeOpened()
			return nil, fmt.Errorf("opening nested catalog at %q: %w", ancestor, openErr)
		}

		chain = append(chain, catalogChainNode{cat: nestedCat, path: ancestor})
		current = nestedCat
	}

	return chain, nil
}

// ancestorPaths returns the absolute ancestor paths of absPath from
// shallowest to deepest, including absPath itself if non-empty.
//
//	""             → []
//	"/atlas"       → ["/atlas"]
//	"/atlas/24.0"  → ["/atlas", "/atlas/24.0"]
func ancestorPaths(absPath string) []string {
	if absPath == "" {
		return nil
	}
	parts := strings.Split(strings.TrimPrefix(absPath, "/"), "/")
	out := make([]string, 0, len(parts))
	for i := range parts {
		if parts[i] == "" {
			continue
		}
		out = append(out, "/"+strings.Join(parts[:i+1], "/"))
	}
	return out
}

// isDeletion returns true when entry should be treated as a catalog removal
// rather than an upsert.  An entry is a deletion when it has no content hash,
// is not a directory, and is not a symlink (symlinks have no hash by design).
func isDeletion(entry Entry) bool {
	return (entry.Hash == nil || len(entry.Hash) == 0) &&
		!entry.Mode.IsDir() &&
		entry.Mode&fs.ModeSymlink == 0
}

// normalizeLeasePathForNested converts a lease path (e.g. "atlas/24.0") to
// the CVMFS absolute path ("/atlas/24.0").  An empty lease path stays empty
// (root-level publish).
func normalizeLeasePathForNested(leasePath string) string {
	leasePath = strings.TrimPrefix(leasePath, "/")
	leasePath = strings.TrimSuffix(leasePath, "/")
	if leasePath == "" {
		return ""
	}
	return "/" + leasePath
}
