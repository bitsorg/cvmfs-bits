package cvmfscatalog

// subtree.go — fast subtree-only catalog build for the bits pipeline.
//
// BuildSubtree creates a fresh CVMFS catalog covering exactly the lease path
// (and any split sub-catalogs within it) without downloading or modifying
// any existing repository catalog.  The result is suitable for submission to
// the gateway commit endpoint; cvmfs_receiver grafts the subtree into the
// existing repository at LeasePath, exactly as it does for cvmfs_server ingest.
//
// Behavioural contract
// ────────────────────
// BuildSubtree performs a REPLACE-ALL publish for the lease path: the new
// catalog will contain only the entries present in the supplied slice.  Any
// files previously published under LeasePath that are absent from the current
// publish are silently removed.  This matches cvmfs_server ingest semantics
// and is correct for complete-version software publishing.
//
// Both subtree paths (LeasePath != "") and root-level publishes (LeasePath == "")
// use BuildSubtree.  The gateway (cvmfs_receiver) grafts the resulting catalog
// into the existing repository at LeasePath during the commit step, so this
// function never needs to download or modify the existing repository catalog.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"cvmfs.io/prepub/pkg/cvmfsdirtab"
	"cvmfs.io/prepub/pkg/cvmfshash"
)

// SubtreeConfig holds configuration for the BuildSubtree operation.
type SubtreeConfig struct {
	// LeasePath is the gateway lease path (e.g. "atlas/24.0"); no leading or
	// trailing slash.  An empty string means a root-level publish (LeasePath==""),
	// producing a catalog rooted at "/".  The gateway grafts it into the existing
	// repository exactly as it does for subtree paths.
	LeasePath string
	// TempDir is a writable directory for temporary catalog SQLite files.
	TempDir string
	// DirtabContent is the raw .cvmfsdirtab content to apply when computing
	// catalog split points.  nil/empty means no dirtab rules (only
	// .cvmfscatalog marker files determine splits).
	DirtabContent []byte
}

// SubtreeResult holds the catalog hashes produced by BuildSubtree.
type SubtreeResult struct {
	// CatalogHash is the SHA-1 hex hash of the subtree root catalog (no suffix).
	// Use CatalogHashSuffixed as new_root_hash in the gateway commit body so
	// cvmfs_receiver can locate the object in CAS.
	CatalogHash string
	// CatalogHashSuffixed is CatalogHash + "C" (CVMFS catalog content-type suffix).
	// Pass this as CommitRequest.NewRootHashSuffixed.
	CatalogHashSuffixed string
	// AllCatalogHashes contains every catalog hash produced: split (child)
	// catalogs in deepest-first order, followed by the subtree root catalog.
	// Each hash is plain hex without the 'C' suffix.  The caller must append
	// "C" when uploading these objects to the CAS or the gateway.
	AllCatalogHashes []string
}

// BuildSubtree builds the new subtree catalog for LeasePath and all split
// sub-catalogs within it.
//
// It performs path-rewriting, split planning, entry routing, and finalization
// entirely from the supplied entry list, without fetching the HTTP manifest,
// downloading the existing root catalog, or walking a catalog chain.
//
// # Caller responsibilities
//
//  1. Upload every hash in SubtreeResult.AllCatalogHashes+"C" to the local
//     CAS (o.CAS.Put) before invoking the gateway commit.
//  2. Pass SubtreeResult.CatalogHashSuffixed as CommitRequest.NewRootHashSuffixed.
//  3. Independently fetch old_root_hash via FetchManifestRootHash and pass it
//     as CommitRequest.OldRootHash.
//
// The catalog files are written to:
//
//	TempDir/data/XY/hashC   (CVMFS standard CAS layout, one file per catalog)
//
// # Receiver interaction contract
//
// cvmfs_receiver requires two invariants that BuildSubtree upholds:
//
//  1. root_prefix must be set for every nested catalog.  When loading a nested
//     catalog via AttachFreely / LoadFreeCatalog, the receiver calls
//     LookupPath(rootPrefix) to locate the catalog's root directory entry.
//     An absent or incorrect root_prefix causes the receiver to fail the
//     lookup and reject the catalog.
//
//  2. The root directory entry of a nested catalog must be stored at
//     MD5Path(rootPrefix), not at MD5Path("").  The receiver's NormalizePath
//     for is_regular_mountpoint_ returns the path unchanged, so
//     LookupPath("/atlas/24.0") computes MD5("/atlas/24.0") and must find an
//     entry there.  BuildSubtree satisfies this by passing rootPrefix as
//     Entry.FullPath when creating the root entry inside Create().
func BuildSubtree(ctx context.Context, cfg SubtreeConfig, entries []Entry) (*SubtreeResult, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	targetAbsPath := normalizeLeasePathForNested(cfg.LeasePath)

	// Parse dirtab content if provided.
	var dt *cvmfsdirtab.Dirtab
	if len(cfg.DirtabContent) > 0 {
		dt, _ = cvmfsdirtab.Parse(cfg.DirtabContent)
		// parse error → dt == nil → no dirtab rules applied
	}

	// ── Create the fresh subtree catalog ─────────────────────────────────────
	// This catalog will have root_prefix=targetAbsPath (or "" for root-level).
	// cvmfs_receiver uses root_prefix to determine the graft point.
	catDBPath := filepath.Join(cfg.TempDir, "subtree_"+safePathID(targetAbsPath)+".db")
	leaseCat, err := Create(catDBPath, targetAbsPath)
	if err != nil {
		return nil, fmt.Errorf("creating subtree catalog at %q: %w", targetAbsPath, err)
	}
	defer leaseCat.Close()

	// The "chain" is exactly one node: the fresh subtree catalog.
	chain := []catalogChainNode{{cat: leaseCat, path: targetAbsPath}}

	// ── Rewrite entry paths from relative to absolute ─────────────────────────
	// Callers supply tar-relative paths (e.g. "usr/lib/foo.so").  Prepend the
	// lease prefix so every FullPath is an absolute CVMFS path
	// (e.g. "/atlas/24.0/usr/lib/foo.so") before the entries are routed into
	// the SQLite catalog.
	prefix := targetAbsPath
	for i := range entries {
		rel := entries[i].FullPath
		switch {
		case rel == "." || rel == "":
			// Tar root entry → the lease directory itself.
			entries[i].FullPath = prefix
			if prefix != "" {
				// Mark as nested-catalog root so GraftNestedCatalog can
				// validate the catalog root entry.
				entries[i].IsNestedRoot = true
			}
		case strings.HasPrefix(rel, "/"):
			// Already absolute — pass through unchanged.
		case prefix != "":
			entries[i].FullPath = prefix + "/" + rel
		default:
			entries[i].FullPath = "/" + rel
		}
		if entries[i].FullPath == "" || entries[i].FullPath == "/" {
			entries[i].Name = ""
		} else {
			entries[i].Name = path.Base(entries[i].FullPath)
		}
	}

	// ── Plan catalog split points ─────────────────────────────────────────────
	// Inspect the entry list for .cvmfscatalog marker files and dirtab glob
	// rules.  Every matching directory within the lease boundary becomes a
	// nested-catalog split point, rooted in its own fresh SQLite database.
	splitPaths := planSplits(entries, targetAbsPath, dt)

	// Create a fresh child catalog for each split point.
	//
	// Each catalog is closed by its Finalize() call in the finalization loop
	// below (Finalize calls c.db.Close()).  On the error path, closeAllSplits
	// closes any handles that have not yet been finalized.
	type newCatNode struct {
		cat  *Catalog
		path string
	}
	newCats := make(map[string]*newCatNode, len(splitPaths))
	closeAllSplits := func() {
		for _, n := range newCats {
			n.cat.Close() // idempotent via closeOnce
		}
	}
	for _, sp := range splitPaths {
		splitDBPath := filepath.Join(cfg.TempDir, "split_"+safePathID(sp)+".db")
		newCat, createErr := Create(splitDBPath, sp)
		if createErr != nil {
			closeAllSplits()
			return nil, fmt.Errorf("creating new catalog for %q: %w", sp, createErr)
		}
		newCats[sp] = &newCatNode{cat: newCat, path: sp}
	}

	// ── Route entries to the correct catalog ──────────────────────────────────
	// Group non-deletion entries by target catalog for BatchUpsert so all
	// inserts for a given catalog share a single SQLite transaction (O(1)
	// BEGIN/COMMIT overhead instead of O(entries)).  Deletions are still
	// executed immediately so they flush any pending batch first.
	leafCat := chain[len(chain)-1].cat // = leaseCat (single chain element)
	batchMap := make(map[*Catalog][]Entry, len(newCats)+1)
	for _, entry := range entries {
		owner := findOwner(splitPaths, entry.FullPath)
		var targetCat *Catalog
		if owner != "" {
			targetCat = newCats[owner].cat
		} else {
			targetCat = leafCat
		}

		if isDeletion(entry) {
			// Flush any pending batch for this catalog before the deletion so
			// that the Remove operates on a consistent catalog state.
			if pending := batchMap[targetCat]; len(pending) > 0 {
				if batchErr := targetCat.BatchUpsert(pending); batchErr != nil {
					return nil, fmt.Errorf("batch upsert before deletion of %q: %w", entry.FullPath, batchErr)
				}
				delete(batchMap, targetCat)
			}
			rmErr := targetCat.Remove(entry.FullPath)
			if rmErr == nil {
				continue
			}
			if errors.Is(rmErr, ErrNotFound) {
				continue // idempotent deletion
			}
			return nil, fmt.Errorf("removing entry %q: %w", entry.FullPath, rmErr)
		}
		batchMap[targetCat] = append(batchMap[targetCat], entry)
	}
	// Flush remaining batches.
	for targetCat, pending := range batchMap {
		if batchErr := targetCat.BatchUpsert(pending); batchErr != nil {
			return nil, fmt.Errorf("batch upsert for catalog entries: %w", batchErr)
		}
	}

	result := &SubtreeResult{}

	// ── Finalise split catalogs deepest-first ─────────────────────────────────
	// Sort split paths by descending length so the deepest nested catalogs are
	// finalised first.  A child must be finalised (and its hash known) before
	// its parent registers it via AddNestedMount.
	sortedSplits := make([]string, len(splitPaths))
	copy(sortedSplits, splitPaths)
	sort.Slice(sortedSplits, func(i, j int) bool {
		return len(sortedSplits[i]) > len(sortedSplits[j])
	})

	for _, sp := range sortedSplits {
		node := newCats[sp]

		hash, delta, finalErr := node.cat.Finalize(cfg.TempDir)
		if finalErr != nil {
			closeAllSplits()
			return nil, fmt.Errorf("finalizing split catalog at %q: %w", sp, finalErr)
		}
		// node.cat is now closed by Finalize; no further Close needed for this node.

		casFile := filepath.Join(cfg.TempDir, cvmfshash.ObjectPath(hash)+"C")
		fi, statErr := os.Stat(casFile)
		if statErr != nil {
			closeAllSplits()
			return nil, fmt.Errorf("stat split catalog %s: %w", hash, statErr)
		}
		result.AllCatalogHashes = append(result.AllCatalogHashes, hash)

		// Register in its parent (another split or the subtree root).
		parentOwner := findOwner(splitPaths, sp)
		var parentCat *Catalog
		if parentOwner != "" {
			parentCat = newCats[parentOwner].cat
		} else {
			parentCat = leafCat
		}
		if addErr := parentCat.AddNestedMount(sp, hash, fi.Size()); addErr != nil {
			closeAllSplits()
			return nil, fmt.Errorf("adding nested mount %q to parent catalog: %w", sp, addErr)
		}

		// Propagate child statistics into parent delta.
		parentCat.delta.SubtreeRegular  += delta.SelfRegular  + delta.SubtreeRegular
		parentCat.delta.SubtreeSymlink  += delta.SelfSymlink  + delta.SubtreeSymlink
		parentCat.delta.SubtreeDir      += delta.SelfDir      + delta.SubtreeDir
		parentCat.delta.SubtreeNested   += delta.SelfNested   + delta.SubtreeNested
		parentCat.delta.SubtreeXattr    += delta.SelfXattr    + delta.SubtreeXattr
		parentCat.delta.SubtreeExternal += delta.SelfExternal + delta.SubtreeExternal
		parentCat.delta.SubtreeSpecial  += delta.SelfSpecial  + delta.SubtreeSpecial
		// Chunked-file and size counters (task #12).
		parentCat.delta.SubtreeChunked          += delta.SelfChunked          + delta.SubtreeChunked
		parentCat.delta.SubtreeChunks           += delta.SelfChunks           + delta.SubtreeChunks
		parentCat.delta.SubtreeFileSize         += delta.SelfFileSize         + delta.SubtreeFileSize
		parentCat.delta.SubtreeChunkedSize      += delta.SelfChunkedSize      + delta.SubtreeChunkedSize
		parentCat.delta.SubtreeExternalFileSize += delta.SelfExternalFileSize + delta.SubtreeExternalFileSize
	}

	// ── Finalise the subtree root catalog ────────────────────────────────────
	// chain always has exactly one element (the lease root catalog created
	// above).  A multi-level chain (downloading the existing root catalog and
	// walking it) was an earlier design that was replaced by this fresh-subtree
	// approach.  If that path is ever reintroduced, restore the loop and the
	// child-registration block that previously lived here.
	//
	// TODO: if root-level publishes ever need to update an existing root
	// catalog rather than build a fresh subtree, extend chain here and
	// re-introduce the parent-registration loop.
	rootNode := chain[0]
	rootHash, _, finalErr := rootNode.cat.Finalize(cfg.TempDir)
	if finalErr != nil {
		return nil, fmt.Errorf("finalizing subtree root catalog at %q: %w", rootNode.path, finalErr)
	}

	casFile := filepath.Join(cfg.TempDir, cvmfshash.ObjectPath(rootHash)+"C")
	if _, statErr := os.Stat(casFile); statErr != nil {
		return nil, fmt.Errorf("stat finalized root catalog %s: %w", rootHash, statErr)
	}

	result.AllCatalogHashes = append(result.AllCatalogHashes, rootHash)
	result.CatalogHash = rootHash
	result.CatalogHashSuffixed = rootHash + "C"

	return result, nil
}
