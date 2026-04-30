package cvmfscatalog

import (
	"bytes"
	"compress/zlib"
	"context"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cvmfs.io/prepub/pkg/cvmfsdirtab"
	"cvmfs.io/prepub/pkg/cvmfshash"
)

func TestHashSuffix(t *testing.T) {
	tests := []struct {
		algo HashAlgo
		want string
	}{
		{HashSha1, ""},
		{HashSha256, "-"},
		{HashRipeMD160, "~"},
	}

	for _, tt := range tests {
		got := HashSuffix(tt.algo)
		if got != tt.want {
			t.Errorf("HashSuffix(%d) = %q; want %q", tt.algo, got, tt.want)
		}
	}
}

func TestMerge(t *testing.T) {
	tmpdir := t.TempDir()

	// Create a root catalog database
	rootCatalogPath := filepath.Join(tmpdir, "root.db")
	rootCat, err := Create(rootCatalogPath, "")
	if err != nil {
		t.Fatalf("Create root catalog failed: %v", err)
	}

	// Add an entry to the root catalog
	now := time.Now().Unix()
	rootCat.Upsert(Entry{
		FullPath:  "/oldfile.txt",
		Name:      "oldfile.txt",
		Mode:      0o100644,
		Size:      100,
		Mtime:     now,
		LinkCount: 1,
	})

	// Finalize it
	casTempDir := filepath.Join(tmpdir, "cas")
	os.MkdirAll(casTempDir, 0o755)
	oldHash, _, err := rootCat.Finalize(casTempDir)
	if err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}

	// Create a mock manifest
	manifestContent := []byte("C" + oldHash + "\nD3600\nNtestrepo.cern.ch\nS1\n--\n")

	// Create a compressed version of the root catalog for serving
	rootDBData, err := os.ReadFile(rootCatalogPath)
	if err != nil {
		t.Fatalf("Reading root catalog failed: %v", err)
	}

	var buf bytes.Buffer
	w, err := zlib.NewWriterLevel(&buf, zlib.BestCompression)
	if err != nil {
		t.Fatalf("Creating zlib writer failed: %v", err)
	}
	w.Write(rootDBData)
	w.Close()

	compressedRootCatalog := buf.Bytes()

	// Create a test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/testrepo/.cvmfspublished" {
			w.Write(manifestContent)
			return
		}

		// CAS path for root catalog
		if r.URL.Path == "/testrepo/data/"+oldHash[:2]+"/"+oldHash+"C" {
			w.Write(compressedRootCatalog)
			return
		}

		http.Error(w, "Not Found", http.StatusNotFound)
	}))
	defer server.Close()

	// Test merge with a new entry
	newEntry := Entry{
		FullPath:  "/newfile.txt",
		Name:      "newfile.txt",
		Mode:      0o100644,
		Size:      200,
		Mtime:     now,
		LinkCount: 1,
	}

	mergeDir := filepath.Join(tmpdir, "merge")
	os.MkdirAll(mergeDir, 0o755)

	mergeResult, err := Merge(context.Background(), MergeConfig{
		Stratum0URL: server.URL,
		RepoName:    "testrepo",
		LeasePath:   "",
		TempDir:     mergeDir,
		HTTPClient:  server.Client(),
	}, []Entry{newEntry})

	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	if mergeResult.OldRootHash != oldHash+"C" {
		t.Errorf("Expected OldRootHash %s, got %s", oldHash+"C", mergeResult.OldRootHash)
	}

	if mergeResult.NewRootHash == "" {
		t.Errorf("Expected non-empty NewRootHash")
	}

	if len(mergeResult.AllCatalogHashes) == 0 {
		t.Errorf("Expected at least one catalog hash")
	}
}

func TestMergeWithDeletion(t *testing.T) {
	tmpdir := t.TempDir()

	// Create a root catalog with an entry to be deleted
	rootCatalogPath := filepath.Join(tmpdir, "root.db")
	rootCat, err := Create(rootCatalogPath, "")
	if err != nil {
		t.Fatalf("Create root catalog failed: %v", err)
	}

	now := time.Now().Unix()
	rootCat.Upsert(Entry{
		FullPath:  "/todelete.txt",
		Name:      "todelete.txt",
		Mode:      0o100644,
		Size:      100,
		Mtime:     now,
		LinkCount: 1,
	})

	casTempDir := filepath.Join(tmpdir, "cas")
	os.MkdirAll(casTempDir, 0o755)
	oldHash, _, err := rootCat.Finalize(casTempDir)
	if err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}

	// Compress for serving
	rootDBData, err := os.ReadFile(rootCatalogPath)
	if err != nil {
		t.Fatalf("Reading root catalog failed: %v", err)
	}

	var buf bytes.Buffer
	w, err := zlib.NewWriterLevel(&buf, zlib.BestCompression)
	if err != nil {
		t.Fatalf("Creating zlib writer failed: %v", err)
	}
	w.Write(rootDBData)
	w.Close()

	compressedRootCatalog := buf.Bytes()

	// Server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/testrepo/.cvmfspublished" {
			manifestContent := []byte("C" + oldHash + "\nD3600\nNtestrepo.cern.ch\nS1\n--\n")
			w.Write(manifestContent)
			return
		}

		if r.URL.Path == "/testrepo/data/"+oldHash[:2]+"/"+oldHash+"C" {
			w.Write(compressedRootCatalog)
			return
		}

		http.Error(w, "Not Found", http.StatusNotFound)
	}))
	defer server.Close()

	// Merge with deletion (Hash=nil, not a directory)
	deleteEntry := Entry{
		FullPath: "/todelete.txt",
		Name:     "todelete.txt",
		Hash:     nil,
		Mode:     0o100644,
	}

	mergeDir := filepath.Join(tmpdir, "merge")
	os.MkdirAll(mergeDir, 0o755)

	mergeResult, err := Merge(context.Background(), MergeConfig{
		Stratum0URL: server.URL,
		RepoName:    "testrepo",
		LeasePath:   "",
		TempDir:     mergeDir,
		HTTPClient:  server.Client(),
	}, []Entry{deleteEntry})

	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	if mergeResult.NewRootHash == "" {
		t.Errorf("Expected non-empty NewRootHash")
	}
}

// compressCatalog reads a freshly-written catalog db file and returns its
// zlib-compressed bytes, exactly as Finalize would write to CAS.
func compressCatalog(t *testing.T, dbPath string) []byte {
	t.Helper()
	raw, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("compressCatalog: reading %s: %v", dbPath, err)
	}
	var buf bytes.Buffer
	w, err := zlib.NewWriterLevel(&buf, zlib.BestCompression)
	if err != nil {
		t.Fatalf("compressCatalog: zlib writer: %v", err)
	}
	if _, err := w.Write(raw); err != nil {
		t.Fatalf("compressCatalog: write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("compressCatalog: close: %v", err)
	}
	return buf.Bytes()
}

// TestMergeNestedCatalog verifies that Merge correctly:
//   - descends into a nested catalog when the lease path is covered by it,
//   - applies entries to the nested (leaf) catalog rather than the root,
//   - finalises both catalogs bottom-up, and
//   - updates the root's nested_catalogs row with the new leaf hash.
func TestMergeNestedCatalog(t *testing.T) {
	tmpdir := t.TempDir()
	casTempDir := filepath.Join(tmpdir, "cas") // used only during setup
	os.MkdirAll(casTempDir, 0o755)

	now := time.Now().Unix()

	// ── 1. Build nested catalog (rootPrefix = "/sub") ─────────────────────
	nestedDBPath := filepath.Join(tmpdir, "nested_setup.db")
	nestedCat, err := Create(nestedDBPath, "/sub")
	if err != nil {
		t.Fatalf("Create nested catalog: %v", err)
	}
	nestedCat.Upsert(Entry{
		FullPath:  "/sub/existing.txt",
		Name:      "existing.txt",
		Mode:      0o100644,
		Size:      50,
		Mtime:     now,
		LinkCount: 1,
	})
	nestedHash, _, err := nestedCat.Finalize(casTempDir)
	if err != nil {
		t.Fatalf("Finalize nested catalog: %v", err)
	}

	// compressed bytes for the HTTP server
	compressedNested := compressCatalog(t, nestedDBPath)

	// stat the CAS file to get the exact compressed size Finalize wrote
	nestedCASFilePath := filepath.Join(casTempDir, "data", nestedHash[:2], nestedHash+"C")
	nestedFI, err := os.Stat(nestedCASFilePath)
	if err != nil {
		t.Fatalf("stat nested CAS file: %v", err)
	}
	nestedSize := nestedFI.Size()

	// ── 2. Build root catalog with nested mount at /sub ───────────────────
	rootDBPath := filepath.Join(tmpdir, "root_setup.db")
	rootCat, err := Create(rootDBPath, "")
	if err != nil {
		t.Fatalf("Create root catalog: %v", err)
	}
	// /sub must exist as a directory entry before AddNestedMount flags it
	rootCat.Upsert(Entry{
		FullPath:  "/sub",
		Name:      "sub",
		Mode:      fs.ModeDir | 0o755,
		Size:      4096,
		Mtime:     now,
		LinkCount: 1,
	})
	if err := rootCat.AddNestedMount("/sub", nestedHash, nestedSize); err != nil {
		t.Fatalf("AddNestedMount: %v", err)
	}
	rootHash, _, err := rootCat.Finalize(casTempDir)
	if err != nil {
		t.Fatalf("Finalize root catalog: %v", err)
	}
	compressedRoot := compressCatalog(t, rootDBPath)

	// ── 3. HTTP server ─────────────────────────────────────────────────────
	// The manifest uses SHA-256 suffix "-" on the C line.
	manifest := []byte("C" + rootHash + "-\nD3600\nNtestrepo.cern.ch\nS5\n--\n")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/testrepo/.cvmfspublished":
			w.Write(manifest)
		case "/testrepo/data/" + rootHash[:2] + "/" + rootHash + "C":
			w.Write(compressedRoot)
		case "/testrepo/data/" + nestedHash[:2] + "/" + nestedHash + "C":
			w.Write(compressedNested)
		default:
			http.Error(w, "not found: "+r.URL.Path, http.StatusNotFound)
		}
	}))
	defer srv.Close()

	// ── 4. Run Merge targeting the nested catalog ──────────────────────────
	mergeDir := filepath.Join(tmpdir, "merge")
	os.MkdirAll(mergeDir, 0o755)

	result, err := Merge(context.Background(), MergeConfig{
		Stratum0URL: srv.URL,
		RepoName:    "testrepo",
		LeasePath:   "sub",
		TempDir:     mergeDir,
		HTTPClient:  srv.Client(),
	}, []Entry{{
		FullPath:  "/sub/newfile.txt",
		Name:      "newfile.txt",
		Mode:      0o100644,
		Size:      200,
		Mtime:     now,
		LinkCount: 1,
	}})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}

	// ── 5. Assertions ──────────────────────────────────────────────────────
	if result.OldRootHash != rootHash+"C" {
		t.Errorf("OldRootHash: got %s, want %s", result.OldRootHash, rootHash+"C")
	}
	if result.NewRootHash == "" {
		t.Error("NewRootHash is empty")
	}
	if result.NewRootHash == rootHash {
		t.Error("NewRootHash should differ from OldRootHash after mutation")
	}
	// NewRootHashSuffixed must carry the CVMFS catalog content-type suffix 'C'.
	if want := result.NewRootHash + "C"; result.NewRootHashSuffixed != want {
		t.Errorf("NewRootHashSuffixed: got %s, want %s", result.NewRootHashSuffixed, want)
	}
	// Chain: nested leaf first, root last → two hashes total
	if len(result.AllCatalogHashes) != 2 {
		t.Fatalf("AllCatalogHashes: got %d entries, want 2", len(result.AllCatalogHashes))
	}
	if result.AllCatalogHashes[1] != result.NewRootHash {
		t.Errorf("AllCatalogHashes[1] should equal NewRootHash")
	}
	// Both hashes should be non-empty and distinct
	if result.AllCatalogHashes[0] == result.AllCatalogHashes[1] {
		t.Error("Nested hash and root hash should differ")
	}
}

// TestMergeNestedCatalogChain verifies two-level nesting: root → /a → /a/b.
// The lease path targets /a/b, so the chain has three nodes and three
// AllCatalogHashes (deepest first).
func TestMergeNestedCatalogChain(t *testing.T) {
	tmpdir := t.TempDir()
	casTempDir := filepath.Join(tmpdir, "cas")
	os.MkdirAll(casTempDir, 0o755)
	now := time.Now().Unix()

	// ── 1. Deepest catalog: /a/b ───────────────────────────────────────────
	abDBPath := filepath.Join(tmpdir, "ab_setup.db")
	abCat, err := Create(abDBPath, "/a/b")
	if err != nil {
		t.Fatalf("Create /a/b catalog: %v", err)
	}
	abCat.Upsert(Entry{
		FullPath:  "/a/b/existing.dat",
		Name:      "existing.dat",
		Mode:      0o100644,
		Size:      10,
		Mtime:     now,
		LinkCount: 1,
	})
	abHash, _, err := abCat.Finalize(casTempDir)
	if err != nil {
		t.Fatalf("Finalize /a/b catalog: %v", err)
	}
	compressedAB := compressCatalog(t, abDBPath)
	abFI, err := os.Stat(filepath.Join(casTempDir, "data", abHash[:2], abHash+"C"))
	if err != nil {
		t.Fatalf("stat /a/b CAS file: %v", err)
	}

	// ── 2. Middle catalog: /a — contains nested mount at /a/b ─────────────
	aDBPath := filepath.Join(tmpdir, "a_setup.db")
	aCat, err := Create(aDBPath, "/a")
	if err != nil {
		t.Fatalf("Create /a catalog: %v", err)
	}
	aCat.Upsert(Entry{
		FullPath:  "/a/b",
		Name:      "b",
		Mode:      fs.ModeDir | 0o755,
		Size:      4096,
		Mtime:     now,
		LinkCount: 1,
	})
	if err := aCat.AddNestedMount("/a/b", abHash, abFI.Size()); err != nil {
		t.Fatalf("AddNestedMount /a/b in /a: %v", err)
	}
	aHash, _, err := aCat.Finalize(casTempDir)
	if err != nil {
		t.Fatalf("Finalize /a catalog: %v", err)
	}
	compressedA := compressCatalog(t, aDBPath)
	aFI, err := os.Stat(filepath.Join(casTempDir, "data", aHash[:2], aHash+"C"))
	if err != nil {
		t.Fatalf("stat /a CAS file: %v", err)
	}

	// ── 3. Root catalog — contains nested mount at /a ──────────────────────
	rootDBPath := filepath.Join(tmpdir, "root_setup.db")
	rootCat, err := Create(rootDBPath, "")
	if err != nil {
		t.Fatalf("Create root catalog: %v", err)
	}
	rootCat.Upsert(Entry{
		FullPath:  "/a",
		Name:      "a",
		Mode:      fs.ModeDir | 0o755,
		Size:      4096,
		Mtime:     now,
		LinkCount: 1,
	})
	if err := rootCat.AddNestedMount("/a", aHash, aFI.Size()); err != nil {
		t.Fatalf("AddNestedMount /a in root: %v", err)
	}
	rootHash, _, err := rootCat.Finalize(casTempDir)
	if err != nil {
		t.Fatalf("Finalize root catalog: %v", err)
	}
	compressedRoot := compressCatalog(t, rootDBPath)

	// ── 4. HTTP server ─────────────────────────────────────────────────────
	manifest := []byte("C" + rootHash + "-\nD3600\nNtestrepo.cern.ch\nS10\n--\n")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/testrepo/.cvmfspublished":
			w.Write(manifest)
		case "/testrepo/data/" + rootHash[:2] + "/" + rootHash + "C":
			w.Write(compressedRoot)
		case "/testrepo/data/" + aHash[:2] + "/" + aHash + "C":
			w.Write(compressedA)
		case "/testrepo/data/" + abHash[:2] + "/" + abHash + "C":
			w.Write(compressedAB)
		default:
			http.Error(w, "not found: "+r.URL.Path, http.StatusNotFound)
		}
	}))
	defer srv.Close()

	// ── 5. Merge: publish to /a/b ──────────────────────────────────────────
	mergeDir := filepath.Join(tmpdir, "merge")
	os.MkdirAll(mergeDir, 0o755)

	result, err := Merge(context.Background(), MergeConfig{
		Stratum0URL: srv.URL,
		RepoName:    "testrepo",
		LeasePath:   "a/b",
		TempDir:     mergeDir,
		HTTPClient:  srv.Client(),
	}, []Entry{{
		FullPath:  "/a/b/newfile.dat",
		Name:      "newfile.dat",
		Mode:      0o100644,
		Size:      99,
		Mtime:     now,
		LinkCount: 1,
	}})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}

	// ── 6. Assertions ──────────────────────────────────────────────────────
	if result.OldRootHash != rootHash+"C" {
		t.Errorf("OldRootHash: got %s, want %s", result.OldRootHash, rootHash+"C")
	}
	if result.NewRootHash == rootHash {
		t.Error("NewRootHash should differ from OldRootHash")
	}
	if want := result.NewRootHash + "C"; result.NewRootHashSuffixed != want {
		t.Errorf("NewRootHashSuffixed: got %s, want %s", result.NewRootHashSuffixed, want)
	}
	// Three levels → three hashes: /a/b leaf, /a middle, root
	if len(result.AllCatalogHashes) != 3 {
		t.Fatalf("AllCatalogHashes: got %d, want 3", len(result.AllCatalogHashes))
	}
	if result.AllCatalogHashes[2] != result.NewRootHash {
		t.Errorf("AllCatalogHashes[2] should equal NewRootHash")
	}
	// All three hashes must be distinct
	h := result.AllCatalogHashes
	if h[0] == h[1] || h[1] == h[2] || h[0] == h[2] {
		t.Errorf("Expected three distinct hashes, got %v", h)
	}
}

func TestMergeHTTPError(t *testing.T) {
	// A 404 on .cvmfspublished is intentionally treated as "first publish ever"
	// (no existing catalog) and must NOT return an error.
	t.Run("404_is_first_publish", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Not Found", http.StatusNotFound)
		}))
		defer server.Close()

		tmpdir := t.TempDir()
		_, err := Merge(context.Background(), MergeConfig{
			Stratum0URL: server.URL,
			RepoName:    "testrepo",
			LeasePath:   "",
			TempDir:     tmpdir,
			HTTPClient:  server.Client(),
		}, []Entry{})
		if err != nil {
			t.Errorf("404 should be treated as first-publish (no error), got: %v", err)
		}
	})

	// A 500 Internal Server Error must propagate as an error.
	t.Run("500_is_error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		}))
		defer server.Close()

		tmpdir := t.TempDir()
		_, err := Merge(context.Background(), MergeConfig{
			Stratum0URL: server.URL,
			RepoName:    "testrepo",
			LeasePath:   "",
			TempDir:     tmpdir,
			HTTPClient:  server.Client(),
		}, []Entry{})
		if err == nil {
			t.Errorf("Expected error when manifest fetch returns 500")
		}
	})
}

// TestMergeStatisticsPropagation verifies that statistics deltas are correctly
// flushed during Finalize. The test creates a simple catalog, applies entries,
// and verifies that Finalize returns the accumulated delta.
func TestMergeStatisticsPropagation(t *testing.T) {
	tmpdir := t.TempDir()

	// Create a new catalog
	rootDBPath := filepath.Join(tmpdir, "root.db")
	rootCat, err := Create(rootDBPath, "")
	if err != nil {
		t.Fatalf("Create root catalog: %v", err)
	}

	now := time.Now().Unix()

	// Add 3 entries: 2 files + 1 directory
	rootCat.Upsert(Entry{
		FullPath:  "/file1.txt",
		Name:      "file1.txt",
		Mode:      0o100644,
		Size:      100,
		Mtime:     now,
		LinkCount: 1,
	})
	rootCat.Upsert(Entry{
		FullPath:  "/file2.txt",
		Name:      "file2.txt",
		Mode:      0o100644,
		Size:      200,
		Mtime:     now,
		LinkCount: 1,
	})
	rootCat.Upsert(Entry{
		FullPath:  "/mydir",
		Name:      "mydir",
		Mode:      fs.ModeDir | 0o755,
		Size:      4096,
		Mtime:     now,
		LinkCount: 1,
	})

	casTempDir := filepath.Join(tmpdir, "cas")
	os.MkdirAll(casTempDir, 0o755)

	// Finalize and verify the returned delta
	hash, delta, err := rootCat.Finalize(casTempDir)
	if err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}

	if hash == "" {
		t.Errorf("Expected non-empty hash")
	}

	// Verify delta reflects all entries added:
	// Create: 1 root dir
	// Upsert: 2 files + 1 dir = 3 more entries
	// Total delta: SelfDir=2 (root+mydir), SelfRegular=2
	if delta.SelfRegular != 2 {
		t.Errorf("Expected delta.SelfRegular=2, got %d", delta.SelfRegular)
	}
	if delta.SelfDir != 2 {
		t.Errorf("Expected delta.SelfDir=2, got %d", delta.SelfDir)
	}

	// Reopen and verify statistics were persisted
	reopenedCat, err := Open(rootDBPath)
	if err != nil {
		t.Fatalf("Opening catalog: %v", err)
	}
	defer reopenedCat.db.Close()

	stats, err := reopenedCat.GetStatistics()
	if err != nil {
		t.Fatalf("GetStatistics failed: %v", err)
	}

	if stats.SelfRegular != 2 {
		t.Errorf("Expected persisted SelfRegular=2, got %d", stats.SelfRegular)
	}
	if stats.SelfDir != 2 {
		t.Errorf("Expected persisted SelfDir=2, got %d", stats.SelfDir)
	}
}

// TestMergeClosesChainOnNestedDownloadError verifies that when a nested catalog
// download fails, Merge returns an appropriate error and does not leak the
// already-opened catalogs (Fix H1).
//
// Observable: the HTTP handler for the nested catalog returns 404, so Merge
// must fail with an error describing the download failure.  We also verify that
// rootCat (which is opened inside Merge) is properly closed by checking that
// the deferred Close() on the chain is triggered — the proxy is that Merge
// returns a non-nil error rather than hanging or panicking.
func TestMergeClosesChainOnNestedDownloadError(t *testing.T) {
	tmpdir := t.TempDir()
	casTempDir := filepath.Join(tmpdir, "cas")
	os.MkdirAll(casTempDir, 0o755)

	now := time.Now().Unix()

	// ── 1. Build a nested catalog so we have a real hash ─────────────────
	nestedDBPath := filepath.Join(tmpdir, "nested_setup.db")
	nestedCat, err := Create(nestedDBPath, "/sub")
	if err != nil {
		t.Fatalf("Create nested catalog: %v", err)
	}
	nestedHash, _, err := nestedCat.Finalize(casTempDir)
	if err != nil {
		t.Fatalf("Finalize nested catalog: %v", err)
	}
	// Compressed bytes of nested catalog are intentionally NOT served below.

	// ── 2. Build root catalog with nested mount at /sub ───────────────────
	rootDBPath := filepath.Join(tmpdir, "root_setup.db")
	rootCat, err := Create(rootDBPath, "")
	if err != nil {
		t.Fatalf("Create root catalog: %v", err)
	}
	rootCat.Upsert(Entry{
		FullPath:  "/sub",
		Name:      "sub",
		Mode:      fs.ModeDir | 0o755,
		Size:      4096,
		Mtime:     now,
		LinkCount: 1,
	})
	// Stat the nested CAS file so AddNestedMount gets a real size.
	nestedCASPath := filepath.Join(casTempDir, "data", nestedHash[:2], nestedHash+"C")
	fi, err := os.Stat(nestedCASPath)
	if err != nil {
		t.Fatalf("stat nested CAS file: %v", err)
	}
	if err := rootCat.AddNestedMount("/sub", nestedHash, fi.Size()); err != nil {
		t.Fatalf("AddNestedMount: %v", err)
	}
	rootHash, _, err := rootCat.Finalize(casTempDir)
	if err != nil {
		t.Fatalf("Finalize root catalog: %v", err)
	}
	compressedRoot := compressCatalog(t, rootDBPath)

	// ── 3. HTTP server: serves root catalog but NOT the nested catalog ────
	manifest := []byte("C" + rootHash + "-\nD3600\nNtestrepo.cern.ch\nS5\n--\n")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/testrepo/.cvmfspublished":
			w.Write(manifest)
		case "/testrepo/data/" + rootHash[:2] + "/" + rootHash + "C":
			w.Write(compressedRoot)
		default:
			// Nested catalog download will hit here and get 404.
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
	defer srv.Close()

	// ── 4. Run Merge — expect failure when downloading the nested catalog ─
	mergeDir := filepath.Join(tmpdir, "merge")
	os.MkdirAll(mergeDir, 0o755)

	_, mergeErr := Merge(context.Background(), MergeConfig{
		Stratum0URL: srv.URL,
		RepoName:    "testrepo",
		LeasePath:   "sub",
		TempDir:     mergeDir,
		HTTPClient:  srv.Client(),
	}, []Entry{})

	if mergeErr == nil {
		t.Fatal("expected Merge to fail when nested catalog download returns 404")
	}
	// The error should mention the nested catalog download.
	errStr := mergeErr.Error()
	if !containsAny(errStr, "nested", "downloading") {
		t.Errorf("error %q should mention nested catalog download", errStr)
	}
}

// ── Catalog-split helper unit tests ──────────────────────────────────────────

func TestPlanSplits_CvmfsCatalogMarker(t *testing.T) {
	entries := []Entry{
		// marker file — parent /data/run1 should become a split
		{FullPath: "/data/run1/.cvmfscatalog", Mode: 0o100644},
		// regular files under the same parent (not triggers)
		{FullPath: "/data/run1/result.root", Mode: 0o100644},
		// marker at a deeper level
		{FullPath: "/data/run1/sub/.cvmfscatalog", Mode: 0o100644},
	}
	// targetAbsPath = "" → root lease, all paths are in scope
	got := planSplits(entries, "", nil)
	want := []string{"/data/run1", "/data/run1/sub"}
	if len(got) != len(want) {
		t.Fatalf("planSplits: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("planSplits[%d]: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestPlanSplits_CvmfsCatalogAtRoot(t *testing.T) {
	// /.cvmfscatalog at repo root — should be ignored (can't split root)
	entries := []Entry{
		{FullPath: "/.cvmfscatalog", Mode: 0o100644},
	}
	got := planSplits(entries, "", nil)
	if len(got) != 0 {
		t.Errorf("planSplits: expected no splits for root marker, got %v", got)
	}
}

func TestPlanSplits_LeaseBoundary(t *testing.T) {
	// Only paths strictly under targetAbsPath are included.
	entries := []Entry{
		{FullPath: "/other/dir/.cvmfscatalog", Mode: 0o100644}, // outside lease
		{FullPath: "/atlas/24.0/evtdisp/.cvmfscatalog", Mode: 0o100644}, // inside lease
	}
	got := planSplits(entries, "/atlas/24.0", nil)
	if len(got) != 1 || got[0] != "/atlas/24.0/evtdisp" {
		t.Errorf("planSplits with lease boundary: got %v, want [\"/atlas/24.0/evtdisp\"]", got)
	}
}

func TestPlanSplits_Dirtab(t *testing.T) {
	dt, err := parseDirtabForTest(t, "/data/*")
	if err != nil {
		t.Fatal(err)
	}
	entries := []Entry{
		{FullPath: "/data/run1", Mode: fs.ModeDir | 0o755},
		{FullPath: "/data/run2", Mode: fs.ModeDir | 0o755},
		{FullPath: "/data/run1/file.txt", Mode: 0o100644}, // not a dir, should be ignored
	}
	got := planSplits(entries, "", dt)
	if len(got) != 2 {
		t.Fatalf("planSplits with dirtab: got %v, want 2 entries", got)
	}
	if got[0] != "/data/run1" || got[1] != "/data/run2" {
		t.Errorf("planSplits with dirtab: got %v", got)
	}
}

func TestPlanSplits_DirtabNegation(t *testing.T) {
	// /data/* matches but *.svn is excluded via negation
	dt, err := parseDirtabForTest(t, "/data/*\n! *.svn")
	if err != nil {
		t.Fatal(err)
	}
	entries := []Entry{
		{FullPath: "/data/run1", Mode: fs.ModeDir | 0o755},
		{FullPath: "/data/repo.svn", Mode: fs.ModeDir | 0o755},
	}
	got := planSplits(entries, "", dt)
	if len(got) != 1 || got[0] != "/data/run1" {
		t.Errorf("planSplits with dirtab negation: got %v, want [\"/data/run1\"]", got)
	}
}

func TestFindOwner(t *testing.T) {
	// With splitPaths = ["/foo", "/foo/bar", "/foo/bar/baz"]:
	//  - Entries strictly UNDER a split path go to that split's catalog.
	//  - A path that IS a split point itself (e.g. "/foo/bar") is strictly
	//    under its parent split ("/foo"), so it goes to /foo's catalog.
	//    That catalog then receives AddNestedMount("/foo/bar", ...).
	//  - A path that has NO split-path strict prefix (e.g. "/foo" itself,
	//    since nothing in splitPaths is a prefix of "/foo") goes to the
	//    leaf catalog (return "").
	splitPaths := []string{"/foo", "/foo/bar", "/foo/bar/baz"}
	cases := []struct {
		entry string
		want  string
	}{
		{"/foo/file.txt", "/foo"},
		{"/foo/bar/file.txt", "/foo/bar"},
		{"/foo/bar/baz/deep.txt", "/foo/bar/baz"},
		// /foo/bar is itself a split point, but /foo is a strict prefix of it:
		// the /foo/bar directory entry is routed to /foo's catalog.
		{"/foo/bar", "/foo"},
		// /foo has no parent split → goes to leaf catalog.
		{"/foo", ""},
		{"/other/path", ""},
		{"", ""},
	}
	for _, tc := range cases {
		got := findOwner(splitPaths, tc.entry)
		if got != tc.want {
			t.Errorf("findOwner(%q) = %q, want %q", tc.entry, got, tc.want)
		}
	}
}

func TestSafePathID_Unique(t *testing.T) {
	// Two paths that share a simplified form must produce different IDs.
	id1 := safePathID("/a/b_c")
	id2 := safePathID("/a_b/c")
	if id1 == id2 {
		t.Errorf("safePathID collision: %q == %q for different paths", id1, id2)
	}
}

func TestSafePathID_Empty(t *testing.T) {
	id := safePathID("")
	if id == "" {
		t.Error("safePathID(\"\") must not return empty string")
	}
}

// TestMergeWithCvmfsCatalogSplit verifies end-to-end that:
//   - a .cvmfscatalog marker in the entries triggers a new nested catalog,
//   - files under the marker's parent directory are routed to the new catalog,
//   - AllCatalogHashes contains two entries (new child + root), and
//   - the new root hash differs from the old root hash.
func TestMergeWithCvmfsCatalogSplit(t *testing.T) {
	tmpdir := t.TempDir()
	casTmpDir := filepath.Join(tmpdir, "cas")
	os.MkdirAll(casTmpDir, 0o755)

	now := time.Now().Unix()

	// ── 1. Build a root catalog with a directory that will be split ───────
	rootCatalogPath := filepath.Join(tmpdir, "root_setup.db")
	rootCat, err := Create(rootCatalogPath, "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	rootCat.Upsert(Entry{
		FullPath:  "/data",
		Name:      "data",
		Mode:      fs.ModeDir | 0o755,
		Size:      4096,
		Mtime:     now,
		LinkCount: 1,
	})
	oldHash, _, err := rootCat.Finalize(casTmpDir)
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	compressedRoot := compressCatalog(t, rootCatalogPath)

	manifest := []byte("C" + oldHash + "-\nD3600\nNtestrepo.cern.ch\nS1\n--\n")

	// ── 2. HTTP server ─────────────────────────────────────────────────────
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/testrepo/.cvmfspublished":
			w.Write(manifest)
		case "/testrepo/data/" + oldHash[:2] + "/" + oldHash + "C":
			w.Write(compressedRoot)
		default:
			http.Error(w, "not found: "+r.URL.Path, http.StatusNotFound)
		}
	}))
	defer srv.Close()

	// ── 3. Merge with entries that include a .cvmfscatalog marker ─────────
	mergeDir := filepath.Join(tmpdir, "merge")
	os.MkdirAll(mergeDir, 0o755)

	entries := []Entry{
		// The directory that will become a split point
		{FullPath: "/data/run1", Name: "run1", Mode: fs.ModeDir | 0o755, Size: 4096, Mtime: now, LinkCount: 1},
		// The .cvmfscatalog marker — triggers split at /data/run1
		{FullPath: "/data/run1/.cvmfscatalog", Name: ".cvmfscatalog", Mode: 0o100644, Size: 0, Mtime: now, LinkCount: 1},
		// A file under the split — must go into the new nested catalog
		{FullPath: "/data/run1/result.root", Name: "result.root", Mode: 0o100644, Size: 1000, Mtime: now, LinkCount: 1},
	}

	result, err := Merge(context.Background(), MergeConfig{
		Stratum0URL: srv.URL,
		RepoName:    "testrepo",
		LeasePath:   "",
		TempDir:     mergeDir,
		HTTPClient:  srv.Client(),
	}, entries)
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}

	// ── 4. Assertions ──────────────────────────────────────────────────────
	if result.OldRootHash != oldHash+"C" {
		t.Errorf("OldRootHash: got %s, want %s", result.OldRootHash, oldHash+"C")
	}
	if result.NewRootHash == oldHash {
		t.Error("NewRootHash must differ from OldRootHash after split")
	}
	// Expect 2 hashes: the new nested catalog first, root last
	if len(result.AllCatalogHashes) != 2 {
		t.Fatalf("AllCatalogHashes: got %d, want 2 (new child + root)", len(result.AllCatalogHashes))
	}
	if result.AllCatalogHashes[1] != result.NewRootHash {
		t.Errorf("AllCatalogHashes[last] should equal NewRootHash")
	}
	// Child and root must be distinct
	if result.AllCatalogHashes[0] == result.AllCatalogHashes[1] {
		t.Error("child catalog hash must differ from root catalog hash")
	}

	// Verify the root catalog now has a nested_catalogs entry at /data/run1
	newRootDBPath := filepath.Join(mergeDir, cvmfshash.ObjectPath(result.NewRootHash)+"C")
	if _, statErr := os.Stat(newRootDBPath); statErr != nil {
		t.Fatalf("new root CAS file not found: %v", statErr)
	}
}

// TestMergeWithDirtabSplit verifies that a DirtabContent rule in MergeConfig
// causes matching directories to be split into new nested catalogs.
func TestMergeWithDirtabSplit(t *testing.T) {
	tmpdir := t.TempDir()
	casTmpDir := filepath.Join(tmpdir, "cas")
	os.MkdirAll(casTmpDir, 0o755)
	now := time.Now().Unix()

	// Minimal root catalog
	rootCatalogPath := filepath.Join(tmpdir, "root_setup.db")
	rootCat, err := Create(rootCatalogPath, "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	oldHash, _, err := rootCat.Finalize(casTmpDir)
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	compressedRoot := compressCatalog(t, rootCatalogPath)
	manifest := []byte("C" + oldHash + "-\nD3600\nNtestrepo.cern.ch\nS1\n--\n")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/testrepo/.cvmfspublished":
			w.Write(manifest)
		case "/testrepo/data/" + oldHash[:2] + "/" + oldHash + "C":
			w.Write(compressedRoot)
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
	defer srv.Close()

	mergeDir := filepath.Join(tmpdir, "merge")
	os.MkdirAll(mergeDir, 0o755)

	entries := []Entry{
		// Directory matched by dirtab /releases/*
		{FullPath: "/releases/v1.0", Name: "v1.0", Mode: fs.ModeDir | 0o755, Size: 4096, Mtime: now, LinkCount: 1},
		// File under the split directory — goes to the new nested catalog
		{FullPath: "/releases/v1.0/lib.so", Name: "lib.so", Mode: 0o100644, Size: 500, Mtime: now, LinkCount: 1},
	}

	result, err := Merge(context.Background(), MergeConfig{
		Stratum0URL:   srv.URL,
		RepoName:      "testrepo",
		LeasePath:     "",
		TempDir:       mergeDir,
		HTTPClient:    srv.Client(),
		DirtabContent: []byte("/releases/*\n"),
	}, entries)
	if err != nil {
		t.Fatalf("Merge with dirtab: %v", err)
	}

	// Expect 2 hashes: new child (/releases/v1.0) first, root last
	if len(result.AllCatalogHashes) != 2 {
		t.Fatalf("AllCatalogHashes: got %d, want 2 (dirtab split + root)", len(result.AllCatalogHashes))
	}
	if result.AllCatalogHashes[1] != result.NewRootHash {
		t.Errorf("AllCatalogHashes[last] should equal NewRootHash")
	}
}

// parseDirtabForTest is a test helper that parses a dirtab string.
func parseDirtabForTest(t *testing.T, content string) (*cvmfsdirtab.Dirtab, error) {
	t.Helper()
	return cvmfsdirtab.Parse([]byte(content))
}

// containsAny returns true if s contains at least one of the given substrings.
func containsAny(s string, substrings ...string) bool {
	for _, sub := range substrings {
		if len(sub) > 0 && len(s) >= len(sub) {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}

// ── N4 (merge side): idempotent deletion ─────────────────────────────────────

// TestMergeIdempotentDeletion verifies that Merge does not fail when a deletion
// entry targets a path that is absent from the catalog.  This can happen during
// a retry of a partial publish where the first attempt already removed the entry.
// After Fix N4 the call must succeed (Fix N4).
func TestMergeIdempotentDeletion(t *testing.T) {
	tmpdir := t.TempDir()

	// Build a minimal root catalog with one file.
	rootCatalogPath := filepath.Join(tmpdir, "root.db")
	rootCat, err := Create(rootCatalogPath, "")
	if err != nil {
		t.Fatalf("Create root catalog: %v", err)
	}
	now := time.Now().Unix()
	rootCat.Upsert(Entry{
		FullPath: "/existing.txt", Name: "existing.txt",
		Mode: 0o100644, Size: 10, Mtime: now, LinkCount: 1,
	})
	casTempDir := filepath.Join(tmpdir, "cas")
	os.MkdirAll(casTempDir, 0o755)
	oldHash, _, err := rootCat.Finalize(casTempDir)
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// Serve the root catalog over fake HTTP.
	rootDBData, err := os.ReadFile(rootCatalogPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var buf bytes.Buffer
	zw, _ := zlib.NewWriterLevel(&buf, zlib.BestCompression)
	zw.Write(rootDBData)
	zw.Close()
	compressedRoot := buf.Bytes()
	manifestContent := []byte("C" + oldHash + "\nD3600\nNtestrepo.cern.ch\nS1\n--\n")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/testrepo/.cvmfspublished":
			w.Write(manifestContent)
		case "/testrepo/data/" + oldHash[:2] + "/" + oldHash + "C":
			w.Write(compressedRoot)
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
	defer srv.Close()

	mergeDir := filepath.Join(tmpdir, "merge")
	os.MkdirAll(mergeDir, 0o755)

	// A deletion entry for a path that does NOT exist in the catalog.
	// Merge must treat this as a no-op (idempotent) and succeed.
	deletionEntry := Entry{
		FullPath: "/never/existed",
		Name:     "existed",
		Mode:     0o100644, // regular, no Hash → isDeletion returns true
	}

	_, err = Merge(context.Background(), MergeConfig{
		Stratum0URL: srv.URL,
		RepoName:    "testrepo",
		LeasePath:   "",
		TempDir:     mergeDir,
		HTTPClient:  srv.Client(),
	}, []Entry{deletionEntry})
	if err != nil {
		t.Fatalf("Merge with absent deletion entry: expected success, got %v", err)
	}
}
