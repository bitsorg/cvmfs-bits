// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package cvmfscatalog

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/hex"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cvmfs.io/prepub/pkg/cvmfshash"
)

// publishedRepo serves a root catalog with one nested package catalog holding
// g/pkg/.meta.json, and counts the catalog downloads.
func publishedRepo(t *testing.T, content []byte) (url string, catalogGets *atomic.Int64) {
	t.Helper()
	repoDir := t.TempDir()
	var zb bytes.Buffer
	zw := zlib.NewWriter(&zb)
	zw.Write(content)
	zw.Close()
	objHash, _, err := cvmfshash.HashReader(bytes.NewReader(zb.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	objPath := filepath.Join(repoDir, cvmfshash.ObjectPath(objHash))
	os.MkdirAll(filepath.Dir(objPath), 0o755)
	os.WriteFile(objPath, zb.Bytes(), 0o644)
	raw, _ := hex.DecodeString(objHash)

	child, err := Create(filepath.Join(t.TempDir(), "child.db"), "/g/pkg")
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Upsert(Entry{FullPath: "/g/pkg/.meta.json", Name: ".meta.json",
		Hash: raw, HashAlgo: HashSha1, Size: int64(len(content)), Mode: 0o644,
		Mtime: time.Now().Unix(), LinkCount: 1}); err != nil {
		t.Fatal(err)
	}
	childHash, _, err := child.Finalize(repoDir)
	if err != nil {
		t.Fatal(err)
	}
	root := newTestCatalog(t)
	addDir(t, root, "/g")
	if err := root.Upsert(Entry{FullPath: "/g/pkg", Name: "pkg", Mode: fs.ModeDir | 0o755,
		Size: 4096, Mtime: time.Now().Unix(), LinkCount: 2}); err != nil {
		t.Fatal(err)
	}
	if err := root.AddNestedMount("/g/pkg", childHash, child.UncompressedSize()); err != nil {
		t.Fatal(err)
	}
	rootHash, _, err := root.Finalize(repoDir)
	if err != nil {
		t.Fatal(err)
	}

	catalogGets = &atomic.Int64{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/repo/.cvmfspublished" {
			w.Write([]byte("C" + rootHash + "\nNrepo\nS1\n--\n"))
			return
		}
		if strings.HasSuffix(r.URL.Path, "C") {
			catalogGets.Add(1)
		}
		http.StripPrefix("/repo/", http.FileServer(http.Dir(repoDir))).ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, catalogGets
}

func useCatalogCache(t *testing.T, maxBytes int64) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "catalogs")
	if err := SetCatalogCache(dir, maxBytes); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { SetCatalogCache("", 0) })
	return dir
}

func cachedFiles(t *testing.T, dir string) []string {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(dir, "*"))
	return m
}

// Each catalog is downloaded once; later lookups read it from the cache and
// still answer correctly, through both walks.
//
// NEGATIVE CONTROL: disable the cache (useCatalogCache not called) and the
// download count becomes 6.
func TestCatalogCache_DownloadsEachCatalogOnce(t *testing.T) {
	content := []byte(`{"package":{"hash":"abc123"}}`)
	url, gets := publishedRepo(t, content)
	dir := useCatalogCache(t, 1<<30)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		got, found, err := ReadPublishedFile(ctx, nil, url, "repo", "g/pkg/.meta.json")
		if err != nil || !found || !bytes.Equal(got, content) {
			t.Fatalf("read %d: (%q, %v, %v)", i, got, found, err)
		}
		if ok, err := PathExists(ctx, nil, url, "repo", "g/pkg"); err != nil || !ok {
			t.Fatalf("exists %d: (%v, %v)", i, ok, err)
		}
	}
	if ok, err := PathExists(ctx, nil, url, "repo", "g/other"); err != nil || ok {
		t.Errorf("absent path: (%v, %v)", ok, err)
	}
	if n := gets.Load(); n != 2 {
		t.Errorf("catalog downloads = %d, want 2 (root and package, once each)", n)
	}
	// Read-only: nothing but the two catalogs, no WAL or journal beside them.
	files := cachedFiles(t, dir)
	if len(files) != 2 {
		t.Fatalf("cache holds %v, want exactly the two catalogs", files)
	}
	cat, err := openReadOnly(files[0])
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	if _, err := cat.db.Exec("CREATE TABLE x (a)"); err == nil {
		t.Error("a cached catalog accepted a write; it must be opened read-only")
	}
}

// Over the limit, the least recently used catalog goes, never the one just
// added, and lookups keep answering.
func TestCatalogCache_EvictsDownToTheLimit(t *testing.T) {
	content := []byte(`{"package":{"hash":"abc123"}}`)
	url, _ := publishedRepo(t, content)
	dir := useCatalogCache(t, 1) // smaller than any catalog
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, found, err := ReadPublishedFile(ctx, nil, url, "repo", "g/pkg/.meta.json"); err != nil || !found {
			t.Fatalf("read %d: found=%v err=%v", i, found, err)
		}
	}
	if files := cachedFiles(t, dir); len(files) != 1 {
		t.Errorf("cache holds %v, want only the most recent catalog", files)
	}
}

// A download interrupted by a crash leaves a part file, removed at startup.
func TestCatalogCache_RemovesLeftoverDownloads(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "catalogs")
	os.MkdirAll(dir, 0o750)
	os.WriteFile(filepath.Join(dir, "ab-123"+partSuffix), []byte("x"), 0o640)
	if err := SetCatalogCache(dir, 1<<20); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { SetCatalogCache("", 0) })
	if files := cachedFiles(t, dir); len(files) != 0 {
		t.Errorf("leftovers kept: %v", files)
	}
}

// A repository whose root catalog is gone reads as "nothing published", as
// without the cache.
func TestCatalogCache_MissingCatalogIsNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/repo/.cvmfspublished" {
			w.Write([]byte("C" + strings.Repeat("ab", 20) + "\nNrepo\nS1\n--\n"))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	useCatalogCache(t, 1<<20)
	if ok, err := PathExists(context.Background(), nil, srv.URL, "repo", "g/pkg"); err != nil || ok {
		t.Errorf("(%v, %v), want (false, nil)", ok, err)
	}
}

// A cached catalog that cannot be opened (left damaged by a crash) is dropped
// and fetched again, rather than failing every later lookup.
func TestCatalogCache_DropsADamagedCatalog(t *testing.T) {
	content := []byte(`{"package":{"hash":"abc123"}}`)
	url, gets := publishedRepo(t, content)
	dir := useCatalogCache(t, 1<<30)
	ctx := context.Background()
	if ok, err := PathExists(ctx, nil, url, "repo", "g/pkg"); err != nil || !ok {
		t.Fatalf("(%v, %v)", ok, err)
	}
	for _, f := range cachedFiles(t, dir) {
		os.WriteFile(f, nil, 0o640) // truncated, as after a crash
	}
	if _, err := PathExists(ctx, nil, url, "repo", "g/pkg"); err == nil {
		t.Fatal("a damaged catalog was read without error")
	}
	if ok, err := PathExists(ctx, nil, url, "repo", "g/pkg"); err != nil || !ok {
		t.Fatalf("after the damaged file was dropped: (%v, %v)", ok, err)
	}
	if n := gets.Load(); n != 2 {
		t.Errorf("catalog downloads = %d, want 2 (first lookup, then the replacement)", n)
	}
}

// Characters that mean something in a URI do not break the cache directory.
func TestCatalogCache_DirectoryWithURICharacters(t *testing.T) {
	content := []byte(`{"package":{"hash":"abc123"}}`)
	url, _ := publishedRepo(t, content)
	dir := filepath.Join(t.TempDir(), "c#1?x%20")
	if err := SetCatalogCache(dir, 1<<30); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { SetCatalogCache("", 0) })
	for i := 0; i < 2; i++ {
		got, found, err := ReadPublishedFile(context.Background(), nil, url, "repo", "g/pkg/.meta.json")
		if err != nil || !found || !bytes.Equal(got, content) {
			t.Fatalf("read %d: (%q, %v, %v)", i, got, found, err)
		}
	}
}

// Concurrent lookups, including misses on the same catalogs and eviction of
// catalogs other lookups hold open, all answer correctly (run with -race).
func TestCatalogCache_Concurrent(t *testing.T) {
	content := []byte(`{"package":{"hash":"abc123"}}`)
	url, _ := publishedRepo(t, content)
	useCatalogCache(t, 1) // every insert evicts the other catalog
	ctx := context.Background()
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		go func() {
			got, found, err := ReadPublishedFile(ctx, nil, url, "repo", "g/pkg/.meta.json")
			if err == nil && (!found || !bytes.Equal(got, content)) {
				err = fmt.Errorf("got (%q, %v)", got, found)
			}
			errs <- err
		}()
	}
	for i := 0; i < 16; i++ {
		if err := <-errs; err != nil {
			t.Error(err)
		}
	}
}
