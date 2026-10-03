// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package cvmfscatalog

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/hex"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cvmfs.io/prepub/pkg/cvmfshash"
)

// A package root published as a nested catalog; its .meta.json lives in the
// child catalog, so the read must descend into it.
func TestReadPublishedFile(t *testing.T) {
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

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/repo/.cvmfspublished" {
			w.Write([]byte("C" + rootHash + "\nNrepo\nS1\n--\n"))
			return
		}
		http.StripPrefix("/repo/", http.FileServer(http.Dir(repoDir))).ServeHTTP(w, r)
	}))
	defer srv.Close()
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
