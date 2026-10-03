// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package cvmfscatalog

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

// The IDs, flag bits and suffixes must match shash::Algorithms,
// SqlDirent::StoreHashAlgorithm and shash::kAlgorithmIds in CVMFS.
func TestHashAlgoMatchesCVMFS(t *testing.T) {
	tests := []struct {
		algo   HashAlgo
		id     int
		bits   int
		suffix string
	}{
		{HashSha1, 1, 0, ""},
		{HashRipeMD160, 2, 1, "-rmd160"},
		{HashShake128, 3, 2, "-shake128"},
	}
	for _, tt := range tests {
		if int(tt.algo) != tt.id {
			t.Errorf("algo %d: want CVMFS id %d", tt.algo, tt.id)
		}
		e := Entry{Mode: 0o644, Hash: make([]byte, 20), HashAlgo: tt.algo}
		if got := (e.Flags() >> flagHashShift) & 7; got != tt.bits {
			t.Errorf("algo %d: flag bits = %d, want %d", tt.algo, got, tt.bits)
		}
		if got := HashAlgoFromFlags(e.Flags()); got != tt.algo {
			t.Errorf("HashAlgoFromFlags round trip = %d, want %d", got, tt.algo)
		}
		if got := HashSuffix(tt.algo); got != tt.suffix {
			t.Errorf("HashSuffix(%d) = %q, want %q", tt.algo, got, tt.suffix)
		}
	}
	if FlagXattr&0x3ffff != 0 {
		t.Errorf("FlagXattr %#x overlaps a CVMFS flag bit", FlagXattr)
	}
}

// TestSha1CatalogRowsUnchanged pins the exact catalog row bytes for SHA-1
// content, which is all prepub produces.
func TestSha1CatalogRowsUnchanged(t *testing.T) {
	cat, err := Create(filepath.Join(t.TempDir(), "cat.db"), "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer cat.Close()

	bulk, _ := hex.DecodeString("713ca8a74dd20682338da781e314ac2b8ce883e4")
	c0, _ := hex.DecodeString("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	c1, _ := hex.DecodeString("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	entries := []Entry{
		{FullPath: "/plain", Name: "plain", Mode: 0o644, Size: 10, Mtime: 1, LinkCount: 1,
			Hash: bulk, HashAlgo: HashSha1, CompAlgo: CompZlib},
		{FullPath: "/raw", Name: "raw", Mode: 0o755, Size: 10, Mtime: 1, LinkCount: 1,
			Hash: bulk, HashAlgo: HashSha1, CompAlgo: CompNone},
		{FullPath: "/chunked", Name: "chunked", Mode: 0o644, Size: 20, Mtime: 1, LinkCount: 1,
			HashAlgo: HashSha1, CompAlgo: CompZlib,
			Chunks: []ChunkRecord{{Offset: 0, Size: 10, Hash: c0}, {Offset: 10, Size: 10, Hash: c1}}},
	}
	for _, e := range entries {
		if err := cat.Upsert(e); err != nil {
			t.Fatalf("Upsert %s: %v", e.FullPath, err)
		}
	}

	// A chunked file's bulk hash is NULL, as CVMFS writes it by default.
	want := map[string]struct {
		flags, mode int64
		hash        []byte
	}{
		"plain":   {FlagFile, 0o100644, bulk},
		"raw":     {FlagFile | 1<<flagCompShift, 0o100755, bulk},
		"chunked": {FlagFile | FlagFileChunk, 0o100644, nil},
	}
	for name, w := range want {
		var flags, mode int64
		var hash []byte
		var isNull bool
		if err := cat.db.QueryRow("SELECT flags, mode, hash, hash IS NULL FROM catalog WHERE name = ?", name).
			Scan(&flags, &mode, &hash, &isNull); err != nil {
			t.Fatalf("SELECT %s: %v", name, err)
		}
		if flags != w.flags || mode != w.mode || !bytes.Equal(hash, w.hash) || isNull != (w.hash == nil) {
			t.Errorf("%s: flags=%d mode=%o hash=%x null=%v; want flags=%d mode=%o hash=%x",
				name, flags, mode, hash, isNull, w.flags, w.mode, w.hash)
		}
	}

	rows, err := cat.db.Query("SELECT hash FROM chunks ORDER BY offset")
	if err != nil {
		t.Fatalf("SELECT chunks: %v", err)
	}
	defer rows.Close()
	var got [][]byte
	for rows.Next() {
		var h []byte
		if err := rows.Scan(&h); err != nil {
			t.Fatal(err)
		}
		got = append(got, h)
	}
	if len(got) != 2 || !bytes.Equal(got[0], c0) || !bytes.Equal(got[1], c1) {
		t.Errorf("chunk hashes = %x; want [%x %x]", got, c0, c1)
	}

	// Hash-less directories carry no hash-algorithm bits, as in CVMFS.
	var rootFlags int64
	if err := cat.db.QueryRow("SELECT flags FROM catalog WHERE name = ''").Scan(&rootFlags); err != nil {
		t.Fatalf("SELECT root: %v", err)
	}
	if rootFlags != FlagDir {
		t.Errorf("root flags = %d, want %d", rootFlags, FlagDir)
	}
}

func TestParseManifestAlgorithmSuffix(t *testing.T) {
	const hexHash = "713ca8a74dd20682338da781e314ac2b8ce883e4"
	tests := []struct {
		suffix string
		algo   HashAlgo
	}{
		{"", HashSha1},
		{"-rmd160", HashRipeMD160},
		{"-shake128", HashShake128},
	}
	for _, tt := range tests {
		m, err := ParseManifest([]byte("C" + hexHash + tt.suffix + "\nNrepo.cern.ch\nS2\n--\nsig\n"))
		if err != nil {
			t.Fatalf("%q: %v", tt.suffix, err)
		}
		if m.RootHash != hexHash || m.HashAlgo != tt.algo {
			t.Errorf("%q: RootHash=%q HashAlgo=%d; want %q %d",
				tt.suffix, m.RootHash, m.HashAlgo, hexHash, tt.algo)
		}

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("C" + hexHash + tt.suffix + "\nNrepo.cern.ch\nS2\n--\nsig\n")) //nolint:errcheck
		}))
		root, err := FetchManifestRootHash(context.Background(), srv.Client(), srv.URL, "repo")
		srv.Close()
		if err != nil {
			t.Fatalf("%q: FetchManifestRootHash: %v", tt.suffix, err)
		}
		if want := hexHash + tt.suffix + "C"; root != want {
			t.Errorf("FetchManifestRootHash = %q, want %q", root, want)
		}
	}
}

// DownloadObject must use the CVMFS CAS path, which carries the algorithm
// suffix after the digest.
func TestDownloadObjectAlgorithmPath(t *testing.T) {
	const hexHash = "713ca8a74dd20682338da781e314ac2b8ce883e4"
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	zw.Write([]byte("payload")) //nolint:errcheck
	zw.Close()
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Write(buf.Bytes()) //nolint:errcheck
	}))
	defer srv.Close()

	for algo, want := range map[HashAlgo]string{
		HashSha1:      "/repo/data/71/3ca8a74dd20682338da781e314ac2b8ce883e4",
		HashRipeMD160: "/repo/data/71/3ca8a74dd20682338da781e314ac2b8ce883e4-rmd160",
		HashShake128:  "/repo/data/71/3ca8a74dd20682338da781e314ac2b8ce883e4-shake128",
	} {
		data, err := DownloadObject(context.Background(), srv.Client(), srv.URL, "repo", hexHash, algo)
		if err != nil || string(data) != "payload" {
			t.Fatalf("algo %d: data=%q err=%v", algo, data, err)
		}
		if gotPath != want {
			t.Errorf("algo %d: path %q, want %q", algo, gotPath, want)
		}
	}
}
