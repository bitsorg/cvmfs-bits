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
	"strconv"
	"strings"
	"time"

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

// ── Deferred finalize ────────────────────────────────────────────────────────
//
// A producer that must not block cannot poll every package job to a terminal
// state and only then request the finalize.  Instead it declares, on each
// package submission, how many packages the build will contain; prepub counts
// the accumulated members and runs the finalize itself when the last one lands.
//
// The control files (_expect, _finalizing, <job>.failed) live alongside the
// member JSONs and are named so that Load and Count — which only accept
// "*.json" — ignore them.  The finalize Result is deliberately NOT one of them:
// it is a sibling of the directory (see resultPath) so that it survives the
// Remove that a successful finalize performs.

const (
	expectFile     = "_expect"
	finalizingFile = "_finalizing"
	failedSuffix   = ".failed"
)

// MarkFailed records that a job belonging to this build reached a terminal
// failure.  Without it a build whose 87th package fails would never reach its
// declared member count, so the deferred finalize would never fire and the
// build would sit in the spool forever with nobody watching — the producer has
// already exited.  Counting failures as terminal outcomes lets the build reach
// a decision, and Failures() makes that decision "refuse to publish" rather
// than "publish the 86 that worked".
func MarkFailed(spoolRoot, buildID, jobID, reason string) error {
	if buildID == "" || jobID == "" {
		return fmt.Errorf("buildset.MarkFailed: buildID and JobID are required")
	}
	dir := buildDir(spoolRoot, buildID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("buildset: mkdir %s: %w", dir, err)
	}
	final := filepath.Join(dir, sanitizeID(jobID)+failedSuffix)
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, []byte(reason), 0o644); err != nil {
		return fmt.Errorf("buildset: write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("buildset: rename %s: %w", final, err)
	}
	return nil
}

// Failures returns the job IDs recorded as failed for this build.
func Failures(spoolRoot, buildID string) []string {
	ents, err := os.ReadDir(buildDir(spoolRoot, buildID))
	if err != nil {
		return nil
	}
	var ids []string
	for _, e := range ents {
		if !e.IsDir() && strings.HasSuffix(e.Name(), failedSuffix) {
			ids = append(ids, strings.TrimSuffix(e.Name(), failedSuffix))
		}
	}
	sort.Strings(ids)
	return ids
}

// Terminal returns how many of the build's jobs have finished, successfully or
// not.  This — not Count — is what the declared expectation is compared
// against, so that a failed package still lets the build reach a decision.
func Terminal(spoolRoot, buildID string) int {
	return Count(spoolRoot, buildID) + len(Failures(spoolRoot, buildID))
}

// SetExpect records how many members build buildID is expected to accumulate.
// It is written by every package submission of the build, so a late-arriving
// (or corrected) count wins; writes are atomic, and a count of zero or less is
// treated as "not declared" and clears any previous declaration.
func SetExpect(spoolRoot, buildID string, n int) error {
	if buildID == "" {
		return fmt.Errorf("buildset.SetExpect: buildID is required")
	}
	dir := buildDir(spoolRoot, buildID)
	if n <= 0 {
		err := os.Remove(filepath.Join(dir, expectFile))
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("buildset: clear expect: %w", err)
		}
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("buildset: mkdir %s: %w", dir, err)
	}
	final := filepath.Join(dir, expectFile)
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.Itoa(n)), 0o644); err != nil {
		return fmt.Errorf("buildset: write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("buildset: rename %s: %w", final, err)
	}
	return nil
}

// Expect returns the declared member count, or 0 when the build has no
// declaration (the caller then waits for an explicit finalize request).
func Expect(spoolRoot, buildID string) int {
	data, err := os.ReadFile(filepath.Join(buildDir(spoolRoot, buildID), expectFile))
	if err != nil {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// Count returns the number of members recorded for a build without decoding
// them — Load reads and unmarshals every member, which is wasteful when all we
// need is "have they all arrived yet?".
func Count(spoolRoot, buildID string) int {
	ents, err := os.ReadDir(buildDir(spoolRoot, buildID))
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range ents {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			n++
		}
	}
	return n
}

// ClaimFinalize atomically claims the right to finalize a build, returning
// false when another caller already holds the claim.  O_EXCL makes this safe
// across the concurrent job goroutines that may all observe the last member
// arriving at the same moment.
//
// The claim deliberately survives a crash: if prepub dies mid-finalize the
// marker remains, auto-finalize stays off for that build, and an operator
// resolves it with POST /builds/{id}/finalize (which does not consult the
// claim).  Silently re-running a half-finished ingestsql commit would be the
// more dangerous behaviour.
func ClaimFinalize(spoolRoot, buildID string) (bool, error) {
	dir := buildDir(spoolRoot, buildID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false, fmt.Errorf("buildset: mkdir %s: %w", dir, err)
	}
	f, err := os.OpenFile(filepath.Join(dir, finalizingFile),
		os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		if os.IsExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("buildset: claim finalize: %w", err)
	}
	_, _ = f.WriteString(time.Now().UTC().Format(time.RFC3339))
	if cerr := f.Close(); cerr != nil {
		return false, fmt.Errorf("buildset: claim finalize: %w", cerr)
	}
	return true, nil
}

// ReleaseFinalize drops the claim so that a later attempt can be made.  It is
// called only when the finalize failed *before* changing repository state; a
// failure during the commit keeps the claim.
func ReleaseFinalize(spoolRoot, buildID string) error {
	err := os.Remove(filepath.Join(buildDir(spoolRoot, buildID), finalizingFile))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Finalizing reports whether a build's finalize has been claimed.
func Finalizing(spoolRoot, buildID string) bool {
	_, err := os.Stat(filepath.Join(buildDir(spoolRoot, buildID), finalizingFile))
	return err == nil
}

// Status is the observable state of a build, for GET /builds/{id}.
type Status struct {
	BuildID     string   `json:"build_id"`
	Expect      int      `json:"expect"`      // 0 = no declaration; finalize must be requested
	Accumulated int      `json:"accumulated"` // members recorded so far
	Failed      []string `json:"failed,omitempty"`
	Finalizing  bool     `json:"finalizing"` // finalize claimed (running, done, or crashed)
	Result      *Result  `json:"result,omitempty"`
}

// Result is the outcome of a finalize, persisted outside the accumulator
// directory so that it survives Remove and can still be read afterwards.
type Result struct {
	BuildID   string    `json:"build_id"`
	Repo      string    `json:"repo,omitempty"`
	Packages  int       `json:"packages"`
	Published int       `json:"published"`
	Error     string    `json:"error,omitempty"`
	At        time.Time `json:"at"`
}

// resultPath is a sibling of the accumulator directory, so a successful
// finalize (which removes the directory) does not erase the record.
func resultPath(spoolRoot, buildID string) string {
	return buildDir(spoolRoot, buildID) + ".result.json"
}

// WriteResult persists the finalize outcome atomically.
func WriteResult(spoolRoot string, res Result) error {
	if res.BuildID == "" {
		return fmt.Errorf("buildset.WriteResult: BuildID is required")
	}
	data, err := json.Marshal(&res)
	if err != nil {
		return fmt.Errorf("buildset: marshal result: %w", err)
	}
	final := resultPath(spoolRoot, res.BuildID)
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return fmt.Errorf("buildset: mkdir %s: %w", filepath.Dir(final), err)
	}
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("buildset: write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("buildset: rename %s: %w", final, err)
	}
	return nil
}

// ReadResult returns the persisted finalize outcome, or nil when none exists.
func ReadResult(spoolRoot, buildID string) *Result {
	data, err := os.ReadFile(resultPath(spoolRoot, buildID))
	if err != nil {
		return nil
	}
	var res Result
	if err := json.Unmarshal(data, &res); err != nil {
		return nil
	}
	return &res
}

// GetStatus assembles the observable state of a build.
func GetStatus(spoolRoot, buildID string) Status {
	return Status{
		BuildID:     buildID,
		Expect:      Expect(spoolRoot, buildID),
		Accumulated: Count(spoolRoot, buildID),
		Failed:      Failures(spoolRoot, buildID),
		Finalizing:  Finalizing(spoolRoot, buildID),
		Result:      ReadResult(spoolRoot, buildID),
	}
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
	// directory between the lease root and each entry so the descriptor is
	// self-contained.
	//
	// The lease root is the FIRST path component of the common prefix, not the
	// full common prefix. ingestsql auto-detects the lease as the shallowest
	// descriptor path and grafts it into its parent — which must already exist in
	// the repo. Using the full common prefix breaks when it is deep and its
	// parent is absent (e.g. a build whose paths are all under Packages/* with no
	// modulefiles => common prefix ".../Packages", parent ".../" missing =>
	// ingestsql aborts). A top-level component's parent is always the repo root.
	entries = fillAncestorDirs(entries, firstComponent(commonDirPrefix(kept)))
	return entries, conflicts
}

// firstComponent returns the first path segment of p (e.g. "a/b/c" -> "a", "" -> "").
func firstComponent(p string) string {
	p = strings.Trim(p, "/")
	if i := strings.IndexByte(p, '/'); i >= 0 {
		return p[:i]
	}
	return p
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
// "." and ".." must not survive: sanitizeID("..") == ".." would make
// buildDir(spool, "..") resolve to the spool root itself — an authenticated
// publisher could then write member records into (and, on finalize,
// os.RemoveAll) the whole spool. Empty input is normalised for the same
// reason (filepath.Join drops it silently).
func sanitizeID(id string) string {
	s := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_', r == '.':
			return r
		default:
			return '_'
		}
	}, id)
	if s == "" || strings.Trim(s, ".") == "" {
		return "_invalid_"
	}
	return s
}
