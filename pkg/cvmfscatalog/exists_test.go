// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package cvmfscatalog

import (
	"io/fs"
	"path/filepath"
	"testing"
	"time"
)

// newTestCatalog creates an empty root catalog in a temp dir.
func newTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "catalog.db")
	cat, err := Create(dbPath, "")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	return cat
}

func addDir(t *testing.T, c *Catalog, absPath string) {
	t.Helper()
	if err := c.Upsert(Entry{
		FullPath:  absPath,
		Name:      filepath.Base(absPath),
		Mode:      fs.ModeDir | 0o755,
		Size:      4096,
		Mtime:     time.Now().Unix(),
		LinkCount: 2,
	}); err != nil {
		t.Fatalf("Upsert(%q) failed: %v", absPath, err)
	}
}

func TestHasEntry(t *testing.T) {
	cat := newTestCatalog(t)
	addDir(t, cat, "/releases")
	addDir(t, cat, "/releases/x86_64-el8")

	cases := map[string]bool{
		"/releases":              true,
		"/releases/x86_64-el8":   true,
		"/releases/aarch64":      false,
		"/nope":                  false,
		"/releases/x86_64-el8/x": false,
	}
	for p, want := range cases {
		got, err := cat.HasEntry(p)
		if err != nil {
			t.Fatalf("HasEntry(%q) error: %v", p, err)
		}
		if got != want {
			t.Errorf("HasEntry(%q) = %v, want %v", p, got, want)
		}
	}
}

func TestLongestNestedAncestor(t *testing.T) {
	cat := newTestCatalog(t)
	h := "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
	// Two nested mounts: a top-level one and a deep version dir under it.
	if err := cat.AddNestedMount("/releases", h, 100); err != nil {
		t.Fatalf("AddNestedMount /releases: %v", err)
	}
	deep := "/releases/x86_64-el8/Packages/ROOT/v6.38.00-3"
	if err := cat.AddNestedMount(deep, h, 200); err != nil {
		t.Fatalf("AddNestedMount deep: %v", err)
	}

	// Exact match on the deep mount.
	mount, _, found, err := cat.longestNestedAncestor(deep)
	if err != nil || !found || mount != deep {
		t.Fatalf("exact: got (%q,%v,%v) want (%q,true,nil)", mount, found, err, deep)
	}

	// A path *under* the deep mount resolves to the deep mount (proper ancestor),
	// choosing it over the shorter "/releases" ancestor (longest-first).
	under := deep + "/lib/libCore.so"
	mount, _, found, err = cat.longestNestedAncestor(under)
	if err != nil || !found || mount != deep {
		t.Fatalf("under: got (%q,%v,%v) want (%q,true,nil)", mount, found, err, deep)
	}

	// A sibling that only shares the "/releases" ancestor resolves to "/releases".
	sib := "/releases/aarch64/Packages/foo/1.0-1"
	mount, _, found, err = cat.longestNestedAncestor(sib)
	if err != nil || !found || mount != "/releases" {
		t.Fatalf("sibling: got (%q,%v,%v) want (\"/releases\",true,nil)", mount, found, err)
	}

	// A path outside any mount has no nested ancestor.
	_, _, found, err = cat.longestNestedAncestor("/other/thing")
	if err != nil || found {
		t.Fatalf("outside: got (found=%v,err=%v) want (false,nil)", found, err)
	}
}
