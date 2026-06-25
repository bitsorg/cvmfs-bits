// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package cvmfscatalog

// rootmerge.go — root-anchored catalog build for the optional A/B "root-merge"
// commit path (mirrors subtree.go's BuildSubtree).
//
// BuildFromRoot creates a fresh CVMFS catalog anchored at the repository ROOT
// (rootPrefix == "") that contains:
//
//   - a directory-entry chain for every intermediate path component from the
//     repository root down to the parent of the lease path
//     (e.g. for LeasePath "a/b/c": "/a", "/a/b", "/a/b/c"); and
//   - the leaf content entries, rewritten to absolute paths under the lease
//     path (e.g. tar-relative "leaf" → "/a/b/c/leaf").
//
// The resulting catalog is submitted to the gateway commit endpoint with
// direct_graft=false and the current HEAD root hash as old_root_hash, so that
// cvmfs_receiver performs its standard 3-way DiffRec merge against the existing
// repository root.  Unlike BuildSubtree (which grafts a leaf subtree and relies
// on a separate mkdir-p pass to pre-create the ancestor directories), the root
// catalog already carries the full ancestor chain, so no ensureParentDirs pass
// is needed.
//
// This path is OFF by default and exists for A/B comparison and integrity
// verification against the graft + mkdir-p path.  See the orchestrator's
// RootMerge config field.
//
// Known item to validate on the testbed (do NOT assume correct here): the
// gateway 3-way merge consumes this root-anchored catalog as the "new" tree and
// diffs it against the current repository root.  Because BuildFromRoot emits
// ONLY the ancestor chain plus the lease-path content, any sibling entries that
// exist in the live repository root but are not represented here are seen by the
// merge as the "new" side of the diff for those directories.  Whether the
// gateway's DiffRec correctly treats unrepresented siblings as unchanged (rather
// than as deletions), and whether the intermediate directory entries need to
// carry specific statistics/flags for the merge, must be confirmed on the
// testbed before this path is promoted past A/B.

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"cvmfs.io/prepub/pkg/cvmfsdirtab"
	"cvmfs.io/prepub/pkg/cvmfshash"
)

// BuildFromRoot builds a fresh root-anchored catalog (rootPrefix == "") covering
// the intermediate directory chain from the repository root down to LeasePath
// plus the supplied leaf content entries, together with any split sub-catalogs
// within the leaf content.
//
// It mirrors BuildSubtree's structure (same Create/route/Finalize machinery,
// same statistics handling, same flag conventions) but anchors the produced
// catalog at the repository root and pre-materialises the ancestor directory
// entries inside that same catalog instead of grafting a leaf subtree.
//
// The leaf content entries are supplied tar-relative (e.g. "usr/lib/foo.so")
// exactly as for BuildSubtree; they are rewritten to absolute paths under
// LeasePath.  An empty LeasePath is rejected: a root-level publish has no
// ancestor chain and is already handled by BuildSubtree(LeasePath="").
//
// Caller responsibilities mirror BuildSubtree:
//
//  1. Upload every hash in SubtreeResult.AllCatalogHashes+"C" to the local CAS.
//  2. Pass SubtreeResult.CatalogHashSuffixed as CommitRequest.NewRootHashSuffixed.
//  3. Fetch old_root_hash via FetchManifestRootHash and pass it as
//     CommitRequest.OldRootHash, and commit with DirectGraft=false.
func BuildFromRoot(ctx context.Context, cfg SubtreeConfig, entries []Entry) (*SubtreeResult, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	leaseAbsPath := normalizeLeasePathForNested(cfg.LeasePath)
	if leaseAbsPath == "" {
		return nil, fmt.Errorf("BuildFromRoot requires a non-empty LeasePath (root-level publishes use BuildSubtree)")
	}

	// Parse dirtab content if provided (same as BuildSubtree).
	var dt *cvmfsdirtab.Dirtab
	if len(cfg.DirtabContent) > 0 {
		dt, _ = cvmfsdirtab.Parse(cfg.DirtabContent)
	}

	// ── Create the fresh ROOT catalog (rootPrefix == "") ─────────────────────
	// Anchored at the repository root so the gateway 3-way merge diffs it
	// against the existing repository root.  Create() inserts the root "" entry.
	catDBPath := filepath.Join(cfg.TempDir, "rootmerge_"+safePathID(leaseAbsPath)+".db")
	rootCat, err := Create(catDBPath, "")
	if err != nil {
		return nil, fmt.Errorf("creating root-merge catalog for lease %q: %w", leaseAbsPath, err)
	}
	defer rootCat.Close()

	// ── Build the intermediate directory chain ───────────────────────────────
	// Absolute path components from the repository root down to (and including)
	// the lease path itself: for leaseAbsPath "/a/b/c" → "/a", "/a/b", "/a/b/c".
	// The deepest component ("/a/b/c") is the lease directory; the leaf content
	// entries are rewritten to live underneath it.  We materialise each as a
	// plain directory entry (FlagDir) so the FUSE client can traverse to the
	// lease path.  The lease-directory entry itself is overridden below if the
	// tar carries a "." entry with real metadata.
	now := time.Now().Unix()
	chainParts := strings.Split(strings.Trim(leaseAbsPath, "/"), "/")
	dirChain := make([]Entry, 0, len(chainParts))
	for i := 1; i <= len(chainParts); i++ {
		abs := "/" + strings.Join(chainParts[:i], "/")
		dirChain = append(dirChain, Entry{
			FullPath:  abs,
			Name:      chainParts[i-1],
			Mode:      fs.ModeDir | 0o755,
			Size:      4096,
			Mtime:     now,
			LinkCount: 2,
		})
	}

	// ── Rewrite leaf content entries from tar-relative to absolute ───────────
	// Prepend the lease prefix so every FullPath is an absolute CVMFS path under
	// the lease directory.  The tar "." entry maps onto the lease directory
	// itself; it is diverted so it overrides the dirChain entry for the lease
	// path rather than producing a duplicate.
	prefix := leaseAbsPath
	contentEntries := make([]Entry, 0, len(entries))
	var leaseDirEntry *Entry // tar "." entry rewritten onto the lease directory
	for i := range entries {
		e := entries[i]
		rel := e.FullPath
		switch {
		case rel == "." || rel == "":
			e.FullPath = prefix
		case strings.HasPrefix(rel, "/"):
			// Already absolute — pass through unchanged.
		default:
			e.FullPath = prefix + "/" + rel
		}
		if e.FullPath == "" || e.FullPath == "/" {
			e.Name = ""
		} else {
			e.Name = path.Base(e.FullPath)
		}
		if e.FullPath == prefix {
			ee := e
			leaseDirEntry = &ee
			continue
		}
		contentEntries = append(contentEntries, e)
	}

	// Override the lease-directory dirChain entry with the tar "." metadata when
	// present (uid/gid/mtime), keeping it a directory entry.
	if leaseDirEntry != nil {
		for i := range dirChain {
			if dirChain[i].FullPath == prefix {
				dirChain[i] = *leaseDirEntry
				break
			}
		}
	}

	// Combined absolute-path entry set: ancestor/lease directory chain followed
	// by the leaf content.
	allEntries := make([]Entry, 0, len(dirChain)+len(contentEntries))
	allEntries = append(allEntries, dirChain...)
	allEntries = append(allEntries, contentEntries...)

	// ── Plan catalog split points within the leaf content ────────────────────
	// targetAbsPath "" means the whole tree is in scope; planSplits inspects the
	// (now absolute) entries for .cvmfscatalog markers and dirtab matches.
	splitPaths := planSplits(allEntries, "", dt)

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
		splitDBPath := filepath.Join(cfg.TempDir, "rootsplit_"+safePathID(sp)+".db")
		newCat, createErr := Create(splitDBPath, sp)
		if createErr != nil {
			closeAllSplits()
			return nil, fmt.Errorf("creating split catalog for %q: %w", sp, createErr)
		}
		newCats[sp] = &newCatNode{cat: newCat, path: sp}
	}

	// ── Route entries to the correct catalog ─────────────────────────────────
	// Same approach as BuildSubtree: batch non-deletion entries per catalog so
	// each catalog's inserts share one SQLite transaction.  The root catalog's
	// own "" placeholder is pre-inserted by Create(); none of allEntries map to
	// "" (they are all under the lease path), so there is no root-placeholder
	// collision to divert here.
	batchMap := make(map[*Catalog][]Entry, len(newCats)+1)
	for _, entry := range allEntries {
		owner := findOwner(splitPaths, entry.FullPath)
		var targetCat *Catalog
		if owner != "" {
			targetCat = newCats[owner].cat
		} else {
			targetCat = rootCat
		}

		if isDeletion(entry) {
			if pending := batchMap[targetCat]; len(pending) > 0 {
				if batchErr := targetCat.BatchInsert(pending); batchErr != nil {
					return nil, fmt.Errorf("batch insert before deletion of %q: %w", entry.FullPath, batchErr)
				}
				delete(batchMap, targetCat)
			}
			rmErr := targetCat.Remove(entry.FullPath)
			if rmErr == nil || rmErr == ErrNotFound {
				continue
			}
			return nil, fmt.Errorf("removing entry %q: %w", entry.FullPath, rmErr)
		}

		batchMap[targetCat] = append(batchMap[targetCat], entry)
	}
	for targetCat, pending := range batchMap {
		if batchErr := targetCat.BatchInsert(pending); batchErr != nil {
			return nil, fmt.Errorf("batch insert for root-merge catalog entries: %w", batchErr)
		}
	}

	result := &SubtreeResult{}

	// ── Finalise split catalogs deepest-first ────────────────────────────────
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

		casFile := filepath.Join(cfg.TempDir, cvmfshash.ObjectPath(hash)+"C")
		fi, statErr := os.Stat(casFile)
		if statErr != nil {
			closeAllSplits()
			return nil, fmt.Errorf("stat split catalog %s: %w", hash, statErr)
		}
		result.AllCatalogHashes = append(result.AllCatalogHashes, hash)

		parentOwner := findOwner(splitPaths, sp)
		var parentCat *Catalog
		if parentOwner != "" {
			parentCat = newCats[parentOwner].cat
		} else {
			parentCat = rootCat
		}
		if addErr := parentCat.AddNestedMount(sp, hash, fi.Size()); addErr != nil {
			closeAllSplits()
			return nil, fmt.Errorf("adding nested mount %q to parent catalog: %w", sp, addErr)
		}

		// Propagate child statistics into parent delta (same as BuildSubtree).
		parentCat.delta.SubtreeRegular += delta.SelfRegular + delta.SubtreeRegular
		parentCat.delta.SubtreeSymlink += delta.SelfSymlink + delta.SubtreeSymlink
		parentCat.delta.SubtreeDir += delta.SelfDir + delta.SubtreeDir
		parentCat.delta.SubtreeNested += delta.SelfNested + delta.SubtreeNested
		parentCat.delta.SubtreeXattr += delta.SelfXattr + delta.SubtreeXattr
		parentCat.delta.SubtreeExternal += delta.SelfExternal + delta.SubtreeExternal
		parentCat.delta.SubtreeSpecial += delta.SelfSpecial + delta.SubtreeSpecial
		parentCat.delta.SubtreeChunked += delta.SelfChunked + delta.SubtreeChunked
		parentCat.delta.SubtreeChunks += delta.SelfChunks + delta.SubtreeChunks
		parentCat.delta.SubtreeFileSize += delta.SelfFileSize + delta.SubtreeFileSize
		parentCat.delta.SubtreeChunkedSize += delta.SelfChunkedSize + delta.SubtreeChunkedSize
		parentCat.delta.SubtreeExternalFileSize += delta.SelfExternalFileSize + delta.SubtreeExternalFileSize
	}

	// ── Finalise the root catalog ────────────────────────────────────────────
	rootHash, _, finalErr := rootCat.Finalize(cfg.TempDir)
	if finalErr != nil {
		return nil, fmt.Errorf("finalizing root-merge catalog: %w", finalErr)
	}

	casFile := filepath.Join(cfg.TempDir, cvmfshash.ObjectPath(rootHash)+"C")
	if _, statErr := os.Stat(casFile); statErr != nil {
		return nil, fmt.Errorf("stat finalized root-merge catalog %s: %w", rootHash, statErr)
	}

	result.AllCatalogHashes = append(result.AllCatalogHashes, rootHash)
	result.CatalogHash = rootHash
	result.CatalogHashSuffixed = rootHash + "C"

	return result, nil
}
