package cvmfscatalog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"cvmfs.io/prepub/pkg/cvmfsdirtab"
	"cvmfs.io/prepub/pkg/cvmfshash"
)

// MergeConfig holds configuration for the Merge operation.
type MergeConfig struct {
	Stratum0URL string
	RepoName    string
	LeasePath   string      // e.g. "atlas/24.0" (no leading/trailing slash)
	TempDir     string
	HTTPClient  *http.Client // nil = http.DefaultClient
	// DirtabContent is the raw text of the .cvmfsdirtab file found in the
	// current tar payload.  When non-empty it takes precedence over any
	// .cvmfsdirtab already present in the repository.  When nil/empty, Merge
	// fetches the repository's existing .cvmfsdirtab (if any) via the stratum0
	// CAS so that split rules apply even for incremental publishes that don't
	// resend the dirtab file itself.
	DirtabContent []byte
}

// MergeResult holds the result of a merge operation.
type MergeResult struct {
	OldRootHash         string   // SHA-1 hex + 'C' catalog content-type suffix (e.g. "abc123...C"); empty on first publish
	NewRootHash         string   // plain hex of new root catalog (no suffix)
	NewRootHashSuffixed string   // new root hash with CVMFS algorithm suffix (e.g. "abc-" for SHA-256)
	AllCatalogHashes    []string // hashes in order: leaf/new-children first, root last (no suffix)
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
	cat   *Catalog
	path  string // absolute root prefix for this catalog ("" for repo root)
	isNew bool   // true when freshly created for this publish (not downloaded from stratum0)
}

// Merge fetches the current root catalog from stratum0, walks the nested
// catalog tree to find the deepest catalog covering LeasePath, applies the
// given entries to it (routing entries under new split points into freshly
// created child catalogs), then re-finalises each catalog from leaf back up to
// root — updating each parent's nested_catalogs row with the new child hash.
//
// Split points are derived from two sources:
//  1. .cvmfscatalog marker files in the entries: the parent directory of each
//     such file becomes a nested-catalog root.
//  2. .cvmfsdirtab glob rules: directories matched by the dirtab file get
//     their own nested catalog.  The dirtab content is taken from
//     cfg.DirtabContent (present in the tar payload) or, if absent, fetched
//     from the repository's existing .cvmfsdirtab via the stratum0 CAS.
//
// Both .cvmfscatalog and .cvmfsdirtab are stored as regular file entries in
// the catalog exactly as any other file — they are never suppressed.
//
// All modified catalog files are written as compressed CAS objects under
// TempDir (path format: data/XY/hashC).  The caller is responsible for
// uploading those files to the CAS backend before calling gateway Commit.
func Merge(ctx context.Context, cfg MergeConfig, entries []Entry) (*MergeResult, error) {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}

	// ── Step 1: Fetch the current manifest ──────────────────────────────────
	// A 404 on .cvmfspublished means the repository has never been published
	// (first publish ever).  We treat this identically to "start from empty".
	manifestURL := cfg.Stratum0URL + "/" + cfg.RepoName + "/.cvmfspublished"
	resp, err := cfg.HTTPClient.Get(manifestURL)
	if err != nil {
		return nil, fmt.Errorf("fetching manifest: %w", err)
	}
	defer resp.Body.Close()

	// emptyRoot is set when there is no existing catalog to merge into.
	emptyRoot := false
	var manifest *Manifest

	if resp.StatusCode == http.StatusNotFound {
		// First publish: no manifest yet — start from an empty root catalog.
		emptyRoot = true
	} else if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("manifest http %d: %s", resp.StatusCode, manifestURL)
	} else {
		manifestData, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("reading manifest: %w", err)
		}
		manifest, err = ParseManifest(manifestData)
		if err != nil {
			return nil, fmt.Errorf("parsing manifest: %w", err)
		}
	}

	result := &MergeResult{}
	if manifest != nil && manifest.RootHash != "" {
		// OldRootHash must carry the 'C' catalog content-type suffix so that
		// the receiver's LoadCatalogByHash assertion
		//   assert(shash::kSuffixCatalog == effective_hash.suffix)
		// is satisfied.  manifest.RootHash is the plain 40-char SHA-1 (algorithm
		// suffixes like "-rmd160" are stripped by ParseManifest); we add 'C'
		// here to produce the 41-char suffixed form the receiver expects.
		result.OldRootHash = manifest.RootHash + "C"
	}

	// Finalize() always SHA-256 hashes the compressed bytes, so the suffix on
	// new_root_hash is always "-" regardless of the old repository's algorithm.
	hashAlgo := HashSha1
	if manifest != nil && manifest.HashAlgo != 0 {
		hashAlgo = manifest.HashAlgo
	}
	_ = hashAlgo

	// ── Step 2: Download and open root catalog ───────────────────────────────
	// If the manifest doesn't exist (first publish) or the catalog object is
	// missing (state corruption from a previously crashed receiver commit),
	// create a fresh empty root catalog and continue.  In both cases the
	// new publish will produce a fully self-consistent catalog tree.
	rootDBPath := filepath.Join(cfg.TempDir, "root.db")
	var rootCat *Catalog
	if !emptyRoot && manifest != nil {
		dlErr := DownloadCatalog(ctx, cfg.HTTPClient, cfg.Stratum0URL, cfg.RepoName,
			manifest.RootHash, rootDBPath)
		if dlErr != nil && !errors.Is(dlErr, ErrCatalogNotFound) {
			return nil, fmt.Errorf("downloading root catalog: %w", dlErr)
		}
		if errors.Is(dlErr, ErrCatalogNotFound) {
			// Catalog object missing despite manifest existing — treat as empty.
			emptyRoot = true
		}
	}
	if emptyRoot {
		rootCat, err = Create(rootDBPath, "")
		if err != nil {
			return nil, fmt.Errorf("creating empty root catalog: %w", err)
		}
	} else {
		rootCat, err = Open(rootDBPath)
		if err != nil {
			return nil, fmt.Errorf("opening root catalog: %w", err)
		}
	}

	// ── Step 2.5: Resolve dirtab content ────────────────────────────────────
	// Prefer content from the new tar payload; fall back to the repository's
	// existing .cvmfsdirtab (fetched from CAS).  A fetch failure is non-fatal:
	// we proceed with whatever we have.
	dirtabContent := cfg.DirtabContent
	if len(dirtabContent) == 0 && cfg.Stratum0URL != "" {
		fetched, fetchErr := fetchExistingDirtab(ctx, cfg, rootCat)
		if fetchErr == nil {
			dirtabContent = fetched
		}
		// fetchErr intentionally ignored — proceed without dirtab on failure
	}

	var dt *cvmfsdirtab.Dirtab
	if len(dirtabContent) > 0 {
		dt, _ = cvmfsdirtab.Parse(dirtabContent)
		// parse error → dt == nil → no dirtab rules applied
	}

	// ── Step 3: Walk nested catalogs to find the target ──────────────────────
	targetAbsPath := normalizeLeasePathForNested(cfg.LeasePath)
	chain, err := buildCatalogChain(ctx, cfg, rootCat, targetAbsPath)
	if err != nil {
		rootCat.Close()
		return nil, fmt.Errorf("walking nested catalogs: %w", err)
	}
	// Fix H1: ensure every catalog in the chain is closed when Merge returns.
	for i := range chain {
		i := i
		defer chain[i].cat.Close()
	}

	// ── Step 3.3: Ensure the chain ends exactly at the lease path ─────────────
	// The CVMFS receiver (catalog_merge_tool_impl.h) calls GraftNestedCatalog /
	// SwapNestedCatalog for the entry at targetAbsPath and PANICs if no matching
	// catalog object exists in the commit payload.  If the existing chain already
	// ends at targetAbsPath (a prior publish created a nested catalog there),
	// no action is needed.
	if targetAbsPath != "" && chain[len(chain)-1].path != targetAbsPath {
		leaseCatDBPath := filepath.Join(cfg.TempDir, "lease_"+safePathID(targetAbsPath)+".db")
		leaseCat, createErr := Create(leaseCatDBPath, targetAbsPath)
		if createErr != nil {
			return nil, fmt.Errorf("creating lease catalog at %q: %w", targetAbsPath, createErr)
		}
		defer leaseCat.Close()
		chain = append(chain, catalogChainNode{cat: leaseCat, path: targetAbsPath, isNew: true})
	}

	// ── Step 3.5: Rewrite entry paths from relative to absolute ─────────────
	// The pipeline produces paths relative to the tar root, e.g.
	// "usr/share/test-pkg/hello.txt". CVMFS requires absolute paths with the
	// full lease prefix, e.g. "/test/smoke/usr/share/test-pkg/hello.txt".
	// The tar root entry "." maps to targetAbsPath itself (or "" for a
	// root-level lease that publishes directly into "/").
	prefix := targetAbsPath
	for i := range entries {
		rel := entries[i].FullPath
		switch {
		case rel == "." || rel == "":
			// Tar root entry → the lease directory itself.
			entries[i].FullPath = prefix
			if prefix != "" {
				// This entry becomes the root of the lease nested catalog.
				// FlagDirNestedRoot must be set so GraftNestedCatalog /
				// SwapNestedCatalog can validate the catalog root entry.
				entries[i].IsNestedRoot = true
			}
		case strings.HasPrefix(rel, "/"):
			// Already absolute — pipeline has already prefixed this entry.
			// Pass through unchanged to avoid double-prefixing (e.g. if the
			// caller supplies paths like /cvmfs/repo/test/smoke/... we must
			// not prepend prefix a second time).
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

	// ── Step 3.6: Plan split points from entries + dirtab ───────────────────
	// splitPaths is the sorted list of absolute paths that should become new
	// nested-catalog roots within this publish.  Splits must be strictly inside
	// the LeasePath so we don't touch parts of the repo we don't own.
	splitPaths := planSplits(entries, targetAbsPath, dt)

	// Create a new child catalog for each split point.
	// All new catalogs are deferred-closed (idempotent after Finalize).
	type newCatNode struct {
		cat  *Catalog
		path string
	}
	newCats := make(map[string]*newCatNode, len(splitPaths))
	for _, sp := range splitPaths {
		catDBPath := filepath.Join(cfg.TempDir, "split_"+safePathID(sp)+".db")
		newCat, createErr := Create(catDBPath, sp)
		if createErr != nil {
			for _, n := range newCats {
				n.cat.Close()
			}
			// chain defers will close existing chain catalogs
			return nil, fmt.Errorf("creating new catalog for %q: %w", sp, createErr)
		}
		sp2 := sp
		nc := newCat
		defer func() { nc.Close() }()
		newCats[sp2] = &newCatNode{cat: newCat, path: sp}
	}

	// ── Step 3.9: Synthesize missing ancestor directories ────────────────────
	// For a lease at "/test/smoke", the tar payload only contains entries at or
	// below that path.  The root catalog needs a "/test" directory entry so that
	// the CVMFS client can traverse into the subtree.  We insert any missing
	// intermediate path components (everything between "/" and targetAbsPath,
	// exclusive) into the root catalog using Upsert (idempotent on re-publish).
	if targetAbsPath != "" {
		ancestors := ancestorPaths(targetAbsPath)
		if len(ancestors) > 1 {
			now := time.Now().Unix()
			for _, ancestor := range ancestors[:len(ancestors)-1] {
				syntheticDir := Entry{
					FullPath: ancestor,
					Name:     path.Base(ancestor),
					Mode:     fs.ModeDir | 0o755,
					Size:     4096,
					Mtime:    now,
					UID:      0,
					GID:      0,
				}
				if uErr := chain[0].cat.Upsert(syntheticDir); uErr != nil {
					return nil, fmt.Errorf("synthesizing ancestor dir %q: %w", ancestor, uErr)
				}
			}
		}
	}

	// ── Step 4: Route entries to the correct catalog ──────────────────────────
	// Entries whose path falls strictly under a split point go to that split's
	// new child catalog.  All other entries go to the leaf of the existing chain.
	// The .cvmfscatalog file itself goes into the child catalog that owns the
	// subtree it lives in (i.e. the directory that was split).
	leafCat := chain[len(chain)-1].cat
	for _, entry := range entries {
		owner := findOwner(splitPaths, entry.FullPath)
		var targetCat *Catalog
		if owner != "" {
			targetCat = newCats[owner].cat
		} else {
			targetCat = leafCat
		}

		if isDeletion(entry) {
			rmErr := targetCat.Remove(entry.FullPath)
			if rmErr == nil {
				continue
			}
			// Fix N4: ErrNotFound → idempotent deletion
			if errors.Is(rmErr, ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("removing entry %q: %w", entry.FullPath, rmErr)
		}
		if uErr := targetCat.Upsert(entry); uErr != nil {
			return nil, fmt.Errorf("upserting entry %q: %w", entry.FullPath, uErr)
		}
	}

	// ── Step 5: Finalise new child catalogs deepest-first ────────────────────
	// Sort split paths by descending length (= depth) so children are finalised
	// before their parents, allowing AddNestedMount to be called on each parent
	// while it is still open.
	sortedSplits := make([]string, len(splitPaths))
	copy(sortedSplits, splitPaths)
	sort.Slice(sortedSplits, func(i, j int) bool {
		return len(sortedSplits[i]) > len(sortedSplits[j])
	})

	for _, sp := range sortedSplits {
		node := newCats[sp]

		hash, delta, finalErr := node.cat.Finalize(cfg.TempDir)
		if finalErr != nil {
			return nil, fmt.Errorf("finalizing new catalog at %q: %w", sp, finalErr)
		}

		casFile := filepath.Join(cfg.TempDir, cvmfshash.ObjectPath(hash)+"C")
		fi, statErr := os.Stat(casFile)
		if statErr != nil {
			return nil, fmt.Errorf("stat new catalog %s: %w", hash, statErr)
		}
		result.AllCatalogHashes = append(result.AllCatalogHashes, hash)

		// Register the new nested catalog in its parent.
		// The parent is the deepest OTHER split path that owns sp, or the leaf.
		parentOwner := findOwner(splitPaths, sp)
		var parentCat *Catalog
		if parentOwner != "" {
			parentCat = newCats[parentOwner].cat
		} else {
			parentCat = leafCat
		}
		if addErr := parentCat.AddNestedMount(sp, hash, fi.Size()); addErr != nil {
			return nil, fmt.Errorf("adding nested mount %q to parent catalog: %w", sp, addErr)
		}

		// Propagate child statistics into parent's delta so subtree counts
		// are correct when the parent is later finalised.
		parentCat.delta.SubtreeRegular += delta.SelfRegular + delta.SubtreeRegular
		parentCat.delta.SubtreeSymlink += delta.SelfSymlink + delta.SubtreeSymlink
		parentCat.delta.SubtreeDir += delta.SelfDir + delta.SubtreeDir
		parentCat.delta.SubtreeNested += delta.SelfNested + delta.SubtreeNested
		parentCat.delta.SubtreeXattr += delta.SelfXattr + delta.SubtreeXattr
		parentCat.delta.SubtreeExternal += delta.SelfExternal + delta.SubtreeExternal
		parentCat.delta.SubtreeSpecial += delta.SelfSpecial + delta.SubtreeSpecial
	}

	// ── Step 6: Finalise the existing chain bottom-up ─────────────────────────
	// childHash / childSize / childPath / childDelta track the previously-
	// finalised child so we can register it in its parent's nested_catalogs.
	// childIsNew distinguishes a freshly-created catalog (Step 3.3) from one
	// that was downloaded from stratum0: new catalogs need AddNestedMount
	// (INSERT into nested_catalogs + guarantee dir entry), while existing ones
	// need UpdateNestedMount (UPDATE the existing nested_catalogs row).
	var childHash string
	var childSize int64
	var childPath string
	var childDelta Statistics
	var childIsNew bool

	for i := len(chain) - 1; i >= 0; i-- {
		node := chain[i]

		if childHash != "" {
			var nestErr error
			if childIsNew {
				nestErr = node.cat.AddNestedMount(childPath, childHash, childSize)
			} else {
				nestErr = node.cat.UpdateNestedMount(childPath, childHash, childSize)
			}
			if nestErr != nil {
				return nil, fmt.Errorf("registering nested mount %q in catalog at %q: %w",
					childPath, node.path, nestErr)
			}

			node.cat.delta.SubtreeRegular += childDelta.SelfRegular + childDelta.SubtreeRegular
			node.cat.delta.SubtreeSymlink += childDelta.SelfSymlink + childDelta.SubtreeSymlink
			node.cat.delta.SubtreeDir += childDelta.SelfDir + childDelta.SubtreeDir
			node.cat.delta.SubtreeNested += childDelta.SelfNested + childDelta.SubtreeNested
			node.cat.delta.SubtreeXattr += childDelta.SelfXattr + childDelta.SubtreeXattr
			node.cat.delta.SubtreeExternal += childDelta.SelfExternal + childDelta.SubtreeExternal
			node.cat.delta.SubtreeSpecial += childDelta.SelfSpecial + childDelta.SubtreeSpecial
		}

		hash, delta, finalErr := node.cat.Finalize(cfg.TempDir)
		if finalErr != nil {
			return nil, fmt.Errorf("finalizing catalog at %q: %w", node.path, finalErr)
		}

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
		childIsNew = node.isNew
	}

	// After the loop: childHash holds the new root catalog hash (SHA-1 of its
	// compressed bytes).  NewRootHashSuffixed appends the CVMFS catalog
	// content-type suffix 'C' so the receiver stores it at data/XY/hashC and
	// CommitProcessor can fetch it at the same content-typed path.
	result.NewRootHash = childHash
	result.NewRootHashSuffixed = childHash + "C"

	return result, nil
}

// ── Catalog split helpers ────────────────────────────────────────────────────

// planSplits returns a sorted, deduplicated list of absolute CVMFS paths that
// should become new nested-catalog roots based on:
//
//  1. .cvmfscatalog marker files: the parent directory of each such file
//     becomes a split point.
//  2. Directory entries that match the dirtab rules (dt may be nil).
//
// Only paths strictly under targetAbsPath (or anywhere when targetAbsPath=="")
// are included, so we never create catalogs outside the lease boundary.
func planSplits(entries []Entry, targetAbsPath string, dt *cvmfsdirtab.Dirtab) []string {
	seen := make(map[string]bool)

	for _, e := range entries {
		// ── .cvmfscatalog trigger ──────────────────────────────────────────
		if e.Mode.IsRegular() && path.Base(e.FullPath) == ".cvmfscatalog" {
			parent := path.Dir(e.FullPath)
			// path.Dir("/foo/.cvmfscatalog") == "/foo"
			// path.Dir("/.cvmfscatalog") == "/" — skip repo root
			if parent != "/" && parent != "." && parent != "" {
				if isUnderLease(parent, targetAbsPath) {
					seen[parent] = true
				}
			}
		}

		// ── .cvmfsdirtab rules on directories ─────────────────────────────
		if e.Mode.IsDir() && dt != nil && dt.Matches(e.FullPath) {
			if isUnderLease(e.FullPath, targetAbsPath) {
				seen[e.FullPath] = true
			}
		}
	}

	result := make([]string, 0, len(seen))
	for p := range seen {
		result = append(result, p)
	}
	sort.Strings(result)
	return result
}

// isUnderLease reports whether absPath is strictly under (or equal to) the
// lease boundary targetAbsPath.  When targetAbsPath is "" (root-level lease)
// all paths are in scope.  The split path must not BE the target itself
// (splitting the lease root is a no-op).
func isUnderLease(absPath, targetAbsPath string) bool {
	if targetAbsPath == "" {
		return true
	}
	// absPath must start with targetAbsPath+"/" — it must be a proper descendant.
	return strings.HasPrefix(absPath, targetAbsPath+"/")
}

// findOwner returns the deepest path in splitPaths that is a strict proper
// prefix of entryPath (i.e. entryPath starts with splitPath+"/").
// Returns "" when no split path owns entryPath (entry belongs to the leaf
// catalog of the existing chain).
func findOwner(splitPaths []string, entryPath string) string {
	owner := ""
	for _, s := range splitPaths {
		if len(s) <= len(owner) {
			continue // can't be deeper than current best
		}
		if strings.HasPrefix(entryPath, s+"/") {
			owner = s
		}
	}
	return owner
}

// fetchExistingDirtab looks up the .cvmfsdirtab file in the root catalog and,
// if present, downloads and decompresses it from the stratum0 CAS.
// Returns (nil, nil) when the file does not exist in the catalog.
// Fetch / decompress errors are returned so the caller can decide whether to
// treat them as fatal or non-fatal.
func fetchExistingDirtab(ctx context.Context, cfg MergeConfig, rootCat *Catalog) ([]byte, error) {
	hashHex, algo, found, err := rootCat.LookupFileHash("/.cvmfsdirtab")
	if err != nil {
		return nil, fmt.Errorf("looking up /.cvmfsdirtab in root catalog: %w", err)
	}
	if !found {
		return nil, nil
	}
	client := cfg.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	data, err := DownloadObject(ctx, client, cfg.Stratum0URL, cfg.RepoName, hashHex, algo)
	if err != nil {
		return nil, fmt.Errorf("downloading existing .cvmfsdirtab: %w", err)
	}
	return data, nil
}

// safePathID converts an absolute path like "/atlas/24.0/EventDisplay" into a
// collision-free filesystem-safe identifier for use in temp file names.
// A short hash suffix ensures two different paths that share a simplified form
// (e.g. "/a/b_c" and "/a_b/c") still produce distinct file names.
func safePathID(absPath string) string {
	p1, _ := MD5Path(absPath) // first half of MD5 is sufficient for uniqueness
	s := strings.TrimPrefix(absPath, "/")
	s = strings.ReplaceAll(s, "/", "_")
	if s == "" {
		s = "root"
	}
	// Append 8 hex chars (32-bit) of the path MD5 to guarantee uniqueness.
	return fmt.Sprintf("%s_%08x", s, uint64(p1))
}

// ── Existing helpers (unchanged) ─────────────────────────────────────────────

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
