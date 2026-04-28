package cvmfscatalog

import (
	"bytes"
	"encoding/hex"
	"io/fs"
	"strings"
	"testing"

	"cvmfs.io/prepub/pkg/cvmfsxattr"
)

// TestSyntheticAttrsRegularFile verifies that SyntheticAttrs produces the
// expected keys for a regular file with a known hash and compression algo.
func TestSyntheticAttrsRegularFile(t *testing.T) {
	hashBytes, _ := hex.DecodeString("abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")
	e := &Entry{
		Mode:     0o100644,
		Hash:     hashBytes,
		HashAlgo: HashSha256,
		CompAlgo: CompZlib,
	}

	m := SyntheticAttrs(e)
	if m == nil {
		t.Fatal("SyntheticAttrs returned nil for a regular file with a hash")
	}

	// user.cvmfs.hash must be hex hash with SHA-256 suffix "-".
	wantHash := hex.EncodeToString(hashBytes) + "-"
	if got := string(m["user.cvmfs.hash"]); got != wantHash {
		t.Errorf("user.cvmfs.hash: want %q, got %q", wantHash, got)
	}

	// user.cvmfs.compression must be "zlib".
	if got := string(m["user.cvmfs.compression"]); got != "zlib" {
		t.Errorf("user.cvmfs.compression: want %q, got %q", "zlib", got)
	}

	// user.cvmfs.chunk_list must be absent for a non-chunked file.
	if _, ok := m["user.cvmfs.chunk_list"]; ok {
		t.Error("user.cvmfs.chunk_list must be absent for non-chunked file")
	}
}

// TestSyntheticAttrsCompNone verifies that CompNone produces "none".
func TestSyntheticAttrsCompNone(t *testing.T) {
	e := &Entry{
		Mode:     0o100644,
		Hash:     []byte("12345678901234567890123456789012"),
		HashAlgo: HashSha256,
		CompAlgo: CompNone,
	}
	m := SyntheticAttrs(e)
	if got := string(m["user.cvmfs.compression"]); got != "none" {
		t.Errorf("user.cvmfs.compression: want %q, got %q", "none", got)
	}
}

// TestSyntheticAttrsChunkedFile verifies that user.cvmfs.chunk_list is
// present and correctly formatted for a chunked file.
func TestSyntheticAttrsChunkedFile(t *testing.T) {
	chunk0Hash, _ := hex.DecodeString("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	chunk1Hash, _ := hex.DecodeString("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

	e := &Entry{
		Mode:     0o100644,
		Hash:     []byte("bulk_hash_placeholder_32bytes!!!"),
		HashAlgo: HashSha256,
		CompAlgo: CompZlib,
		Chunks: []ChunkRecord{
			{Offset: 0, Size: 4096, Hash: chunk0Hash},
			{Offset: 4096, Size: 1024, Hash: chunk1Hash},
		},
	}

	m := SyntheticAttrs(e)
	cl, ok := m["user.cvmfs.chunk_list"]
	if !ok {
		t.Fatal("user.cvmfs.chunk_list missing for chunked file")
	}

	lines := strings.Split(string(cl), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 chunk lines, got %d: %q", len(lines), cl)
	}

	wantLine0 := "0:4096:" + hex.EncodeToString(chunk0Hash) + "-"
	if lines[0] != wantLine0 {
		t.Errorf("chunk line 0: want %q, got %q", wantLine0, lines[0])
	}

	wantLine1 := "4096:1024:" + hex.EncodeToString(chunk1Hash) + "-"
	if lines[1] != wantLine1 {
		t.Errorf("chunk line 1: want %q, got %q", wantLine1, lines[1])
	}
}

// TestSyntheticAttrsDirectory verifies that directories return nil (no hash).
func TestSyntheticAttrsDirectory(t *testing.T) {
	e := &Entry{Mode: fs.ModeDir | 0o755}
	if m := SyntheticAttrs(e); m != nil {
		t.Errorf("SyntheticAttrs should be nil for a directory, got %v", m)
	}
}

// TestSyntheticAttrsSymlink verifies that symlinks return nil (no hash).
func TestSyntheticAttrsSymlink(t *testing.T) {
	e := &Entry{Mode: fs.ModeSymlink | 0o777}
	if m := SyntheticAttrs(e); m != nil {
		t.Errorf("SyntheticAttrs should be nil for a symlink, got %v", m)
	}
}

// TestSyntheticAttrsNoHash verifies that a regular file with a nil hash
// returns nil (hash not yet populated — early builder stage).
func TestSyntheticAttrsNoHash(t *testing.T) {
	e := &Entry{Mode: 0o100644} // no Hash
	if m := SyntheticAttrs(e); m != nil {
		t.Errorf("SyntheticAttrs should be nil for file with no hash, got %v", m)
	}
}

// TestFlagsXattrBit verifies that FlagXattr is set in Flags() when Xattr is
// non-empty and cleared when the map is nil or empty.
func TestFlagsXattrBit(t *testing.T) {
	e := Entry{
		Mode:      0o100644,
		LinkCount: 1,
		Xattr:     map[string][]byte{"user.a": []byte("b")},
	}
	if e.Flags()&FlagXattr == 0 {
		t.Error("FlagXattr should be set when Xattr is non-empty")
	}

	e.Xattr = nil
	if e.Flags()&FlagXattr != 0 {
		t.Error("FlagXattr should be clear when Xattr is nil")
	}

	e.Xattr = map[string][]byte{} // empty but non-nil
	if e.Flags()&FlagXattr != 0 {
		t.Error("FlagXattr should be clear when Xattr is an empty map")
	}
}

// TestXattrRoundTripThroughCatalog verifies the full serialize→store→load
// round-trip: Upsert writes the BLOB, and reading it back via Unmarshal
// gives the original key-value pairs.
func TestXattrRoundTripThroughCatalog(t *testing.T) {
	import_cvmfsxattr_roundtrip(t)
}

// import_cvmfsxattr_roundtrip is the actual test body, kept as a helper to
// avoid import-cycle issues with the test binary: the cvmfscatalog package
// imports cvmfsxattr, so we can call cvmfsxattr.Unmarshal via the internal
// catalog.go wiring by reading the raw BLOB back from the DB.
func import_cvmfsxattr_roundtrip(t *testing.T) {
	t.Helper()

	tmpdir := t.TempDir()
	cat, err := Create(tmpdir+"/cat.db", "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer cat.Close()

	want := map[string][]byte{
		"user.app.name":    []byte("myapp"),
		"user.app.version": []byte("2.0"),
		"user.binary.val":  {0x00, 0xDE, 0xAD, 0xBE, 0xEF},
	}
	entry := Entry{
		FullPath:  "/annotated.bin",
		Name:      "annotated.bin",
		Mode:      0o100644,
		Size:      42,
		Mtime:     1234567890,
		LinkCount: 1,
		Xattr:     want,
	}
	if err := cat.Upsert(entry); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// Read the raw BLOB from the DB.
	var blob []byte
	if err := cat.db.QueryRow(
		"SELECT xattr FROM catalog WHERE name = 'annotated.bin'",
	).Scan(&blob); err != nil {
		t.Fatalf("SELECT xattr: %v", err)
	}
	if len(blob) == 0 {
		t.Fatal("xattr BLOB is empty")
	}

	// Unmarshal manually (we are inside the package so we can import cvmfsxattr
	// indirectly by calling the package-level helper decodeXattrBlob).
	got := decodeXattrBlobForTest(t, blob)

	for k, wantV := range want {
		gotV, ok := got[k]
		if !ok {
			t.Errorf("key %q missing after round-trip", k)
			continue
		}
		if !bytes.Equal(gotV, wantV) {
			t.Errorf("key %q: want %v, got %v", k, wantV, gotV)
		}
	}
	if len(got) != len(want) {
		t.Errorf("got %d keys, want %d", len(got), len(want))
	}
}

// decodeXattrBlobForTest is a thin shim that calls cvmfsxattr.Unmarshal so
// that catalog_test.go (which is in the same package) can verify BLOB content
// without importing cvmfsxattr directly in the test file.
func decodeXattrBlobForTest(t *testing.T, blob []byte) map[string][]byte {
	t.Helper()
	m, err := cvmfsxattr.Unmarshal(blob)
	if err != nil {
		t.Fatalf("cvmfsxattr.Unmarshal: %v", err)
	}
	return m
}
