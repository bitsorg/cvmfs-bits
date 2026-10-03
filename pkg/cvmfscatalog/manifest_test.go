// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package cvmfscatalog

import (
	"bytes"
	"compress/zlib"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestParseManifest(t *testing.T) {
	// Real manifest content from guinea_pig_repo_20
	manifestContent := []byte(`C713ca8a74dd20682338da781e314ac2b8ce883e4
D3600
Ntestmigration.cern.ch
S2
--
signature and other stuff here
`)

	m, err := ParseManifest(manifestContent)
	if err != nil {
		t.Fatalf("ParseManifest failed: %v", err)
	}

	if m.RootHash != "713ca8a74dd20682338da781e314ac2b8ce883e4" {
		t.Errorf("Expected RootHash '713ca8a74dd20682338da781e314ac2b8ce883e4', got '%s'", m.RootHash)
	}
	if m.RepoName != "testmigration.cern.ch" {
		t.Errorf("Expected RepoName 'testmigration.cern.ch', got '%s'", m.RepoName)
	}
	if m.Revision != 2 {
		t.Errorf("Expected Revision 2, got %d", m.Revision)
	}
	if m.TTL != 3600 {
		t.Errorf("Expected TTL 3600, got %d", m.TTL)
	}
}

func TestDownloadCatalog(t *testing.T) {
	tmpdir := t.TempDir()

	// Create a simple test SQLite database and compress it
	testDBPath := filepath.Join(tmpdir, "test.db")
	if err := os.WriteFile(testDBPath, []byte("FAKE_SQLITE_DATA_12345"), 0o644); err != nil {
		t.Fatalf("Creating test DB failed: %v", err)
	}

	// Compress the fake DB
	dbData, err := os.ReadFile(testDBPath)
	if err != nil {
		t.Fatalf("Reading test DB failed: %v", err)
	}

	var buf bytes.Buffer
	w, err := zlib.NewWriterLevel(&buf, zlib.BestCompression)
	if err != nil {
		t.Fatalf("Creating zlib writer failed: %v", err)
	}
	if _, err := w.Write(dbData); err != nil {
		t.Fatalf("Compressing failed: %v", err)
	}
	w.Close()

	compressedData := buf.Bytes()

	// Create an httptest server that serves the compressed catalog
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Expected path: /testrepo/data/34/56789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01C
		if r.URL.Path == "/testrepo/data/34/56789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01C" {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Write(compressedData)
			return
		}
		http.Error(w, "Not Found", http.StatusNotFound)
	}))
	defer server.Close()

	// Download the catalog
	destPath := filepath.Join(tmpdir, "downloaded.db")
	hashHex := "3456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01"

	err = DownloadCatalog(context.Background(), server.Client(), server.URL, "testrepo", hashHex, destPath)
	if err != nil {
		t.Fatalf("DownloadCatalog failed: %v", err)
	}

	// Verify the decompressed data matches
	downloaded, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("Reading downloaded file failed: %v", err)
	}

	if !bytes.Equal(downloaded, dbData) {
		t.Errorf("Downloaded data doesn't match original")
	}
}

func TestDownloadCatalogHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Not Found", http.StatusNotFound)
	}))
	defer server.Close()

	tmpdir := t.TempDir()
	destPath := filepath.Join(tmpdir, "downloaded.db")

	err := DownloadCatalog(context.Background(), server.Client(), server.URL, "testrepo", "abc123", destPath)
	if err == nil {
		t.Errorf("Expected error for HTTP 404")
	}
}

// TestFetchObject_ShortName: a name too short for data/xx/rest is an error,
// not a slice panic, and makes no request.
func TestFetchObject_ShortName(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("request made for an invalid object name")
	}))
	defer srv.Close()
	for _, name := range []string{"", "a", "ab"} {
		if _, err := fetchObject(context.Background(), srv.Client(), srv.URL, "r", name); err == nil {
			t.Errorf("fetchObject(%q): want an error", name)
		}
	}
}
