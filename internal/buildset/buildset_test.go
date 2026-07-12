// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

package buildset

import (
	"io/fs"
	"testing"

	"cvmfs.io/prepub/pkg/cvmfscatalog"
)

func fileEntry(rel string) cvmfscatalog.Entry {
	h := make([]byte, 20)
	for i := range h {
		h[i] = 0xab
	}
	return cvmfscatalog.Entry{FullPath: rel, Mode: 0o644, Size: 3, Hash: h,
		HashAlgo: cvmfscatalog.HashSha1, CompAlgo: cvmfscatalog.CompZlib}
}

func TestRecordLoadRoundTrip(t *testing.T) {
	root := t.TempDir()
	b := "build-123"
	if err := Record(root, b, Member{JobID: "j2", Path: "arch/Packages/bar/2.0", BitsFingerprint: "fb", Entries: []cvmfscatalog.Entry{fileEntry("bin/bar")}}); err != nil {
		t.Fatal(err)
	}
	if err := Record(root, b, Member{JobID: "j1", Path: "arch/Packages/foo/1.0", BitsFingerprint: "fa", Entries: []cvmfscatalog.Entry{fileEntry("bin/foo")}}); err != nil {
		t.Fatal(err)
	}
	got, err := Load(root, b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("loaded %d members, want 2", len(got))
	}
	// sorted by path: bar before foo
	if got[0].Path != "arch/Packages/bar/2.0" || got[1].Path != "arch/Packages/foo/1.0" {
		t.Fatalf("members not sorted by path: %s, %s", got[0].Path, got[1].Path)
	}
	if got[0].Entries[0].FullPath != "bin/bar" {
		t.Fatalf("entry not round-tripped: %+v", got[0].Entries[0])
	}
}

func TestAssembleExpandsAndNests(t *testing.T) {
	m := Member{Path: "arch/Packages/foo/1.0", BitsFingerprint: "fa", Entries: []cvmfscatalog.Entry{
		fileEntry("bin/foo"),
		{FullPath: "bin", Mode: fs.ModeDir | 0o755}, // subdir
	}}
	entries, conflicts := Assemble([]Member{m})
	if len(conflicts) != 0 {
		t.Fatalf("unexpected conflicts: %v", conflicts)
	}
	byPath := map[string]cvmfscatalog.Entry{}
	for _, e := range entries {
		byPath[e.FullPath] = e
	}
	// file and subdir prefixed with the package path
	if _, ok := byPath["arch/Packages/foo/1.0/bin/foo"]; !ok {
		t.Fatalf("file not prefixed; got %v", keys(byPath))
	}
	if _, ok := byPath["arch/Packages/foo/1.0/bin"]; !ok {
		t.Fatalf("subdir not prefixed; got %v", keys(byPath))
	}
	// synthesised package root, marked nested
	root, ok := byPath["arch/Packages/foo/1.0"]
	if !ok || !root.IsNestedRoot || !root.Mode.IsDir() {
		t.Fatalf("package root missing/not nested: %+v", root)
	}
}

func TestAssembleDedupSameFingerprint(t *testing.T) {
	m := Member{Path: "arch/Packages/dep/1.0", BitsFingerprint: "same", Entries: []cvmfscatalog.Entry{fileEntry("lib/d")}}
	entries, conflicts := Assemble([]Member{m, m}) // same dep recorded twice
	if len(conflicts) != 0 {
		t.Fatalf("same-fingerprint dup should not conflict: %v", conflicts)
	}
	// included exactly once (root + file + no duplicate)
	n := 0
	for _, e := range entries {
		if e.FullPath == "arch/Packages/dep/1.0/lib/d" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("dep file included %d times, want 1", n)
	}
}

func TestAssembleConflictDifferentFingerprint(t *testing.T) {
	a := Member{Path: "arch/Packages/dep/1.0", BitsFingerprint: "A", Entries: []cvmfscatalog.Entry{fileEntry("lib/d")}}
	b := Member{Path: "arch/Packages/dep/1.0", BitsFingerprint: "B", Entries: []cvmfscatalog.Entry{fileEntry("lib/d")}}
	entries, conflicts := Assemble([]Member{a, b})
	if len(conflicts) != 1 || conflicts[0].Path != "arch/Packages/dep/1.0" {
		t.Fatalf("expected 1 conflict for the divergent package, got %v", conflicts)
	}
	if len(entries) != 0 {
		t.Fatalf("conflicting package must be excluded, got %d entries", len(entries))
	}
}

func keys(m map[string]cvmfscatalog.Entry) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
