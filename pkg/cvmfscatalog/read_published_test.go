// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package cvmfscatalog

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/hex"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"cvmfs.io/prepub/pkg/cvmfshash"
)

// A package root published as a nested catalog; its .meta.json lives in the
// child catalog, so the read must descend into it.
func TestReadPublishedFile(t *testing.T) {
	srv, content, _, _ := newMetaRepo(t)
	ctx := context.Background()

	got, found, err := ReadPublishedFile(ctx, srv.Client(), srv.URL, "repo", "g/pkg/.meta.json")
	if err != nil || !found || !bytes.Equal(got, content) {
		t.Fatalf("got (%q, %v, %v), want the file content", got, found, err)
	}
	for _, p := range []string{"g/pkg/missing", "g/pkg", "g/other/.meta.json"} {
		if _, found, err := ReadPublishedFile(ctx, srv.Client(), srv.URL, "repo", p); err != nil || found {
			t.Errorf("%s: found=%v err=%v, want not found", p, found, err)
		}
	}
}

// Several files are read from one revision: the manifest is fetched once, and
// only the files that exist are returned.
func TestReadPublishedFiles(t *testing.T) {
	srv, content, manifests, _ := newMetaRepo(t)
	paths := []string{"g/pkg/.meta.json", "g/pkg/missing", "g/pkg", "g/other/.meta.json"}
	got, big, err := ReadPublishedFiles(context.Background(), srv.Client(), srv.URL, "repo", paths,
		ReadLimits{Workers: 3})
	if err != nil || len(big) != 0 {
		t.Fatal(err, big)
	}
	if len(got) != 1 || !bytes.Equal(got["g/pkg/.meta.json"], content) {
		t.Fatalf("got %q, want only g/pkg/.meta.json", got)
	}
	if n := manifests.Load(); n != 1 {
		t.Errorf("manifest read %d times, want once", n)
	}
}

// Sizes are checked from the catalog, before a download: a file over the
// per-file limit is reported, files over the total limit fail the call.
func TestReadPublishedFilesLimits(t *testing.T) {
	srv, content, _, objects := newMetaRepo(t)
	ctx := context.Background()
	n := int64(len(content))
	got, big, err := ReadPublishedFiles(ctx, srv.Client(), srv.URL, "repo",
		[]string{"g/pkg/.meta.json"}, ReadLimits{Workers: 2, MaxFileBytes: n - 1})
	if err != nil || len(got) != 0 || len(big) != 1 || big[0] != "g/pkg/.meta.json" {
		t.Fatalf("per-file limit: got %q %v %v", got, big, err)
	}
	_, _, err = ReadPublishedFiles(ctx, srv.Client(), srv.URL, "repo",
		[]string{"g/pkg/.meta.json"}, ReadLimits{Workers: 2, MaxTotalBytes: n - 1})
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("total limit: got %v, want ErrTooLarge", err)
	}
	if objects.Load() != 0 {
		t.Errorf("%d data objects downloaded, want none", objects.Load())
	}
}

// A failed download fails the whole call; no partial answer.
func TestReadPublishedFilesError(t *testing.T) {
	srv, _, _, _ := newMetaRepo(t)
	failObjects.Store(true)
	t.Cleanup(func() { failObjects.Store(false) })
	paths := []string{"g/pkg/.meta.json", "g/pkg/missing", "g/pkg/.meta.json"}
	if _, _, err := ReadPublishedFiles(context.Background(), srv.Client(), srv.URL, "repo",
		paths, ReadLimits{Workers: 2}); err == nil {
		t.Fatal("want the download error")
	}
}

// failObjects makes newMetaRepo's server fail data object downloads.
var failObjects atomic.Bool

// newMetaRepo serves a repository whose /g/pkg is a nested catalog holding
// .meta.json; it returns the server, that file's content and counts of the
// manifest reads and of the data objects served.
func newMetaRepo(t *testing.T) (*httptest.Server, []byte, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	repoDir := t.TempDir()
	content := []byte(`{"package":{"hash":"abc123"}}`)

	// Content object: zlib-compressed, keyed by SHA-1 of the compressed bytes.
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

	manifests, objects := &atomic.Int32{}, &atomic.Int32{}
	objURL := "/repo/" + cvmfshash.ObjectPath(objHash)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/repo/.cvmfspublished" {
			manifests.Add(1)
			w.Write([]byte("C" + rootHash + "\nNrepo\nS1\n--\n"))
			return
		}
		if r.URL.Path == objURL {
			objects.Add(1)
			if failObjects.Load() {
				http.Error(w, "boom", http.StatusInternalServerError)
				return
			}
		}
		http.StripPrefix("/repo/", http.FileServer(http.Dir(repoDir))).ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, content, manifests, objects
}

// A chunked file (NULL bulk hash) is read by concatenating its 'P' chunks.
func TestReadPublishedChunkedFile(t *testing.T) {
	repoDir := t.TempDir()
	parts := [][]byte{[]byte(`{"package":`), []byte(`{"hash":"abc"}}`)}
	var chunks []ChunkRecord
	var off int64
	for _, p := range parts {
		var zb bytes.Buffer
		zw := zlib.NewWriter(&zb)
		zw.Write(p)
		zw.Close()
		h, _, err := cvmfshash.HashReader(bytes.NewReader(zb.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		objPath := filepath.Join(repoDir, cvmfshash.ObjectPath(h)+"P")
		os.MkdirAll(filepath.Dir(objPath), 0o755)
		os.WriteFile(objPath, zb.Bytes(), 0o644)
		raw, _ := hex.DecodeString(h)
		chunks = append(chunks, ChunkRecord{Offset: off, Size: int64(len(p)), Hash: raw})
		off += int64(len(p))
	}

	root := newTestCatalog(t)
	if err := root.Upsert(Entry{FullPath: "/meta.json", Name: "meta.json", HashAlgo: HashSha1,
		Size: off, Mode: 0o644, Mtime: time.Now().Unix(), LinkCount: 1, Chunks: chunks}); err != nil {
		t.Fatal(err)
	}
	rootHash, _, err := root.Finalize(repoDir)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/repo/.cvmfspublished" {
			w.Write([]byte("C" + rootHash + "\nNrepo\nS1\n--\n"))
			return
		}
		http.StripPrefix("/repo/", http.FileServer(http.Dir(repoDir))).ServeHTTP(w, r)
	}))
	defer srv.Close()

	got, found, err := ReadPublishedFile(context.Background(), srv.Client(), srv.URL, "repo", "meta.json")
	want := append(append([]byte{}, parts[0]...), parts[1]...)
	if err != nil || !found || !bytes.Equal(got, want) {
		t.Fatalf("got (%q, %v, %v), want %q", got, found, err, want)
	}
}
