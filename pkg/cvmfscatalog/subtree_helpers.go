package cvmfscatalog

// subtree_helpers.go — helpers used by BuildSubtree (subtree.go).
//
// These were previously co-located in merge.go alongside the now-deleted Merge
// function.  They are preserved here because BuildSubtree uses them directly.

import (
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"

	"cvmfs.io/prepub/pkg/cvmfsdirtab"
)

// catalogChainNode is a single catalog in the chain that BuildSubtree writes
// into — one node for the lease root catalog, plus one for every split point.
type catalogChainNode struct {
	cat   *Catalog
	path  string // absolute root prefix for this catalog ("" for repo root)
	isNew bool   // true when freshly created for this publish (not downloaded from stratum0)
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

// planSplits inspects entries and the optional dirtab to determine which
// sub-paths within targetAbsPath need their own nested catalog.  Returns a
// sorted slice of absolute split-point paths.
func planSplits(entries []Entry, targetAbsPath string, dt *cvmfsdirtab.Dirtab) []string {
	seen := make(map[string]bool)

	for _, e := range entries {
		// ── .cvmfscatalog trigger ──────────────────────────────────────────
		if e.Mode.IsRegular() && path.Base(e.FullPath) == ".cvmfscatalog" {
			parent := path.Dir(e.FullPath)
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
// all paths are in scope.
func isUnderLease(absPath, targetAbsPath string) bool {
	if targetAbsPath == "" {
		return true
	}
	return strings.HasPrefix(absPath, targetAbsPath+"/")
}

// findOwner returns the deepest path in splitPaths that is a strict proper
// prefix of entryPath (i.e. entryPath starts with splitPath+"/").
// Returns "" when no split path owns entryPath (entry belongs to the lease
// root catalog).
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

// isDeletion reports whether entry represents a removal rather than an upsert.
// An entry is a deletion when it has no content hash, is not a directory, and
// is not a symlink (symlinks have no hash by design).
func isDeletion(entry Entry) bool {
	return (entry.Hash == nil || len(entry.Hash) == 0) &&
		!entry.Mode.IsDir() &&
		entry.Mode&fs.ModeSymlink == 0
}
