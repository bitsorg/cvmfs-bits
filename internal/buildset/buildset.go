// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

// Package buildset implements the coarse, publish-at-end-of-build model of
// ADR-0007: per-package publish jobs record their catalog entries (already
// content-hashed and uploaded to the store) into a build-scoped accumulator
// instead of committing individually. One end-of-build finalize step assembles
// all members into a single ingestsql descriptor and publishes them in one
// gateway commit.
//
// Each package's pipeline produces entries whose FullPath is RELATIVE to the
// package's publish path (as BuildSubtree would prefix them); Assemble applies
// that prefix so the merged descriptor carries repo-relative paths.
package buildset

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"cvmfs.io/prepub/pkg/cvmfscatalog"
)

// Member is one package's contribution to a build: its publish path, its bits
// identity fingerprint (used for dedup / conflict detection), and the catalog
// entries the pipeline produced (package-relative FullPaths).
type Member struct {
	JobID           string               `json:"job_id"`
	Repo            string               `json:"repo"`             // fully-qualified repo; all members of a build share one
	Path            string               `json:"path"`             // repo-relative publish path, e.g. "x86_64-el10/Packages/foo/1.0"
	BitsFingerprint string               `json:"bits_fingerprint"` // package identity (dedup key); the tar SHA-256 today
	Entries         []cvmfscatalog.Entry `json:"entries"`          // package-relative FullPaths
	Dirtab          string               `json:"dirtab,omitempty"`
}

// buildDir is the on-disk location for a build's accumulated members.
func buildDir(spoolRoot, buildID string) string {
	return filepath.Join(spoolRoot, "builds", sanitizeID(buildID))
}

// Record persists one member under the build, atomically (temp + rename). Safe
// to call concurrently for distinct jobs of the same build.
func Record(spoolRoot, buildID string, m Member) error {
	if buildID == "" || m.JobID == "" {
		return fmt.Errorf("buildset.Record: buildID and JobID are required")
	}
	dir := buildDir(spoolRoot, buildID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("buildset: mkdir %s: %w", dir, err)
	}
	data, err := json.Marshal(&m)
	if err != nil {
		return fmt.Errorf("buildset: marshal member: %w", err)
	}
	final := filepath.Join(dir, sanitizeID(m.JobID)+".json")
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("buildset: write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("buildset: rename %s: %w", final, err)
	}
	return nil
}

// Load returns all recorded members for a build, sorted by publish path for a
// deterministic descriptor.
func Load(spoolRoot, buildID string) ([]Member, error) {
	dir := buildDir(spoolRoot, buildID)
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("buildset: read %s: %w", dir, err)
	}
	var members []Member
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		data, rerr := os.ReadFile(filepath.Join(dir, e.Name()))
		if rerr != nil {
			return nil, fmt.Errorf("buildset: read member %s: %w", e.Name(), rerr)
		}
		var m Member
		if uerr := json.Unmarshal(data, &m); uerr != nil {
			return nil, fmt.Errorf("buildset: decode member %s: %w", e.Name(), uerr)
		}
		members = append(members, m)
	}
	sort.Slice(members, func(i, j int) bool { return members[i].Path < members[j].Path })
	return members, nil
}

// Remove deletes a build's accumulator directory (after a successful finalize).
func Remove(spoolRoot, buildID string) error {
	return os.RemoveAll(buildDir(spoolRoot, buildID))
}

// Conflict records a package excluded from the assembled build.
type Conflict struct {
	Path   string
	Reason string
}

// Assemble merges members into a single, repo-relative []Entry ready for the
// ingestsql descriptor, applying the ADR-0007 dedup/conflict rule keyed on the
// bits fingerprint:
//
//   - Two members at the SAME path with the SAME fingerprint  -> idempotent,
//     keep the first (shared dependency built more than once).
//   - Same path, DIFFERENT fingerprint -> genuine identity collision: exclude
//     it and report a Conflict (validate-then-commit partial success).
//
// Each kept member's entries are prefixed with its path; the package root is
// marked as a nested-catalog boundary (one catalog per package).
func Assemble(members []Member) (entries []cvmfscatalog.Entry, conflicts []Conflict) {
	byPath := map[string][]Member{}
	order := []string{}
	for _, m := range members {
		if _, seen := byPath[m.Path]; !seen {
			order = append(order, m.Path)
		}
		byPath[m.Path] = append(byPath[m.Path], m)
	}
	var kept []string // package paths actually included (for the lease-root prefix)
	for _, p := range order {
		group := byPath[p]
		fp := group[0].BitsFingerprint
		mixed := false
		for _, m := range group[1:] {
			if m.BitsFingerprint != fp {
				mixed = true
				break
			}
		}
		if mixed {
			conflicts = append(conflicts, Conflict{
				Path:   p,
				Reason: "same path published with differing bits fingerprints in one build",
			})
			continue
		}
		entries = append(entries, expand(group[0])...)
		kept = append(kept, group[0].Path)
	}
	// ingestsql does NOT reliably auto-create intermediate directories for a
	// branching multi-package tree (it panics "catalog for directory ... cannot
	// be found" when a package's ancestor dir is missing). Emit every ancestor
	// directory between the build's common root (the lease path) and each entry
	// so the descriptor is self-contained.
	entries = fillAncestorDirs(entries, commonDirPrefix(kept))
	return entries, conflicts
}

// fillAncestorDirs adds a directory entry for every ancestor path (down to and
// including leaseRoot) that is not already present. Ancestors above leaseRoot are
// left out — they are the graft attach point and must already exist. Added dirs
// are ordinary (non-nested); package roots keep their nested marking from expand.
func fillAncestorDirs(entries []cvmfscatalog.Entry, leaseRoot string) []cvmfscatalog.Entry {
	leaseRoot = strings.Trim(leaseRoot, "/")
	have := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		have[strings.Trim(e.FullPath, "/")] = struct{}{}
	}
	needed := map[string]struct{}{}
	for _, e := range entries {
		p := strings.Trim(e.FullPath, "/")
		for p != "" && p != leaseRoot {
			i := strings.LastIndex(p, "/")
			if i < 0 {
				break
			}
			p = p[:i] // parent
			if leaseRoot != "" && p != leaseRoot && !strings.HasPrefix(p, leaseRoot+"/") {
				break // reached/above the lease root's ancestors
			}
			if _, ok := have[p]; !ok {
				needed[p] = struct{}{}
			}
		}
	}
	if leaseRoot != "" {
		if _, ok := have[leaseRoot]; !ok {
			needed[leaseRoot] = struct{}{}
		}
	}
	for p := range needed {
		entries = append(entries, cvmfscatalog.Entry{
			FullPath: p, Name: path.Base(p), Mode: fs.ModeDir | 0o755, LinkCount: 2,
		})
	}
	return entries
}

// commonDirPrefix returns the longest directory path that is an ancestor of (or
// equal to) every path in paths — the natural lease root for the build.
func commonDirPrefix(paths []string) string {
	if len(paths) == 0 {
		return ""
	}
	prefix := strings.Trim(paths[0], "/")
	for _, p := range paths[1:] {
		prefix = commonTwo(prefix, strings.Trim(p, "/"))
		if prefix == "" {
			return ""
		}
	}
	return prefix
}

func commonTwo(a, b string) string {
	as, bs := strings.Split(a, "/"), strings.Split(b, "/")
	n := len(as)
	if len(bs) < n {
		n = len(bs)
	}
	i := 0
	for i < n && as[i] == bs[i] {
		i++
	}
	return strings.Join(as[:i], "/")
}

// expand rewrites a member's package-relative entries to repo-relative paths and
// marks (or synthesises) the package-root directory as a nested-catalog root.
func expand(m Member) []cvmfscatalog.Entry {
	base := strings.Trim(m.Path, "/")
	out := make([]cvmfscatalog.Entry, 0, len(m.Entries)+1)
	haveRoot := false
	for _, e := range m.Entries {
		// Pipeline entries are package-relative; the package root is emitted as
		// "." (and paths may carry a leading "./"). Normalise both to the base.
		rel := strings.TrimPrefix(e.FullPath, "./")
		rel = strings.Trim(rel, "/")
		if rel == "" || rel == "." {
			e.FullPath = base
		} else {
			e.FullPath = base + "/" + rel
		}
		if e.FullPath == base {
			e.IsNestedRoot = true
			haveRoot = true
		}
		out = append(out, e)
	}
	if !haveRoot {
		out = append(out, cvmfscatalog.Entry{
			FullPath:     base,
			Name:         path.Base(base),
			Mode:         fs.ModeDir | 0o755,
			IsNestedRoot: true,
			LinkCount:    2,
		})
	}
	return out
}

// sanitizeID keeps build/job identifiers safe as a single path component.
func sanitizeID(id string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_', r == '.':
			return r
		default:
			return '_'
		}
	}, id)
}
