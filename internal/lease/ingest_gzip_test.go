// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package lease

import (
	"bytes"
	"compress/gzip"
	"context"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// gzipped writes the tar at p gzip-compressed beside it; it returns the new
// file and the tar's bytes.
func gzipped(t *testing.T, p string) (string, []byte) {
	t.Helper()
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	zw.Write(raw)
	zw.Close()
	out := p + ".gz"
	if err := os.WriteFile(out, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return out, raw
}

func testPackageTar(t *testing.T) string {
	return writeTestTar(t, []batchTestEntry{batchDir("./"), batchFile("./bin/a", strings.Repeat("A", 4096))})
}

func commitWithin(t *testing.T, b *IngestBackend, req CommitRequest) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- b.Commit(context.Background(), req) }()
	select {
	case err := <-done:
		return err
	case <-time.After(20 * time.Second):
		t.Fatal("Commit did not return")
		return nil
	}
}

// A gzip payload reaches the ingest decompressed on its input, as the tar it was.
func TestIngestCommit_GzipPayloadIsFedDecompressed(t *testing.T) {
	in := filepath.Join(t.TempDir(), "in.tar")
	b, token, argv := batchBackend(t, "cat > "+in)
	gz, raw := gzipped(t, testPackageTar(t))
	stats := &PublishStats{}
	err := commitWithin(t, b, CommitRequest{Token: token, TarPath: gz, TarEncoding: EncodingGzip,
		CVMFSDir: path.Join(b.cvmfsMount, batchTestRepo, "x/1-1"), Stats: stats})
	if err != nil {
		t.Fatal(err)
	}
	if got := readArgv(t, argv); !strings.HasPrefix(got, "ingest -t - -b x/1-1 ") {
		t.Errorf("argv = %q", got)
	}
	stream, _ := os.ReadFile(in)
	got := readTestTar(t, stream)
	if e, ok := got["./bin/a"]; !ok || e.body != strings.Repeat("A", 4096) {
		t.Errorf("the ingest did not read the package's tar: %v", got)
	}
	if stats.TarBytes == nil || *stats.TarBytes != int64(len(raw)) {
		t.Errorf("tar bytes = %v, want %d", stats.TarBytes, len(raw))
	}
}

// A payload too large once decompressed, or a damaged one, kills the ingest
// before it sees the end of its input.
func TestIngestCommit_BadGzipPayloadKillsTheIngest(t *testing.T) {
	gz, raw := gzipped(t, testPackageTar(t))
	data, _ := os.ReadFile(gz)
	cut, crc := gz+".cut", gz+".crc"
	os.WriteFile(cut, data[:len(data)-12], 0o600) // no trailer: unexpected EOF
	data[len(data)-8] ^= 0xff                     // the tar intact, its CRC wrong
	os.WriteFile(crc, data, 0o600)
	for name, req := range map[string]CommitRequest{
		"too large": {TarPath: gz, TarEncoding: EncodingGzip, TarLimit: int64(len(raw)) / 2},
		"truncated": {TarPath: cut, TarEncoding: EncodingGzip},
		"bad crc":   {TarPath: crc, TarEncoding: EncodingGzip},
	} {
		dir := t.TempDir()
		seen := filepath.Join(dir, "seen")
		b, token, _ := batchBackend(t, `[ "$1" = ingest ] || exit 0
cat > /dev/null; echo end-of-input > `+seen)
		req.Token, req.CVMFSDir = token, path.Join(b.cvmfsMount, batchTestRepo, "x/1-1")
		if err := commitWithin(t, b, req); err == nil || !strings.Contains(err.Error(), "writing the package") {
			t.Errorf("%s: err = %v, want the cause", name, err)
		}
		time.Sleep(200 * time.Millisecond)
		if _, err := os.Stat(seen); err == nil {
			t.Errorf("%s: the ingest reached the end of its input", name)
		}
	}
}

func TestIngestCommit_UnknownEncodingRefused(t *testing.T) {
	b, token, _ := batchBackend(t, "exit 0")
	err := commitWithin(t, b, CommitRequest{Token: token, TarPath: testPackageTar(t), TarEncoding: "zstd",
		CVMFSDir: path.Join(b.cvmfsMount, batchTestRepo, "x/1-1")})
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Errorf("err = %v", err)
	}
}

// A batch takes gzip and plain members together; its tar bytes are the tars'.
func TestCommitBatch_GzipAndPlainMembers(t *testing.T) {
	in := filepath.Join(t.TempDir(), "in.tar")
	b, token, _ := batchBackend(t, "cat > "+in)
	gz, raw := gzipped(t, testPackageTar(t))
	plain := testPackageTar(t)
	fi, _ := os.Stat(plain)
	stats := &PublishStats{}
	err := commitBatchWithin(t, b, BatchCommitRequest{Token: token, Base: "x", Stats: stats, Members: []BatchMember{
		{TarPath: gz, RelPath: "a/1-1", Encoding: EncodingGzip}, {TarPath: plain, RelPath: "b/1-1"}}})
	if err != nil {
		t.Fatal(err)
	}
	stream, _ := os.ReadFile(in)
	got := readTestTar(t, stream)
	for _, name := range []string{"a/1-1/bin/a", "a/1-1/.cvmfscatalog", "b/1-1/bin/a"} {
		if _, ok := got[name]; !ok {
			t.Errorf("%s missing from the stream", name)
		}
	}
	if stats.TarBytes == nil || *stats.TarBytes != int64(len(raw))+fi.Size() {
		t.Errorf("tar bytes = %v, want %d", stats.TarBytes, int64(len(raw))+fi.Size())
	}
	// Too large once decompressed: the batch is not published.
	b2, token2, _ := batchBackend(t, "cat > /dev/null")
	err = commitBatchWithin(t, b2, BatchCommitRequest{Token: token2, Base: "x", Members: []BatchMember{
		{TarPath: gz, RelPath: "a/1-1", Encoding: EncodingGzip, Limit: 100}}})
	if err == nil || !strings.Contains(err.Error(), "larger than allowed") {
		t.Errorf("err = %v", err)
	}
	// A wrong CRC, found past the tar's end: the batch is not published either.
	data, _ := os.ReadFile(gz)
	data[len(data)-8] ^= 0xff
	bad := gz + ".crc"
	os.WriteFile(bad, data, 0o600)
	b3, token3, _ := batchBackend(t, "cat > /dev/null")
	err = commitBatchWithin(t, b3, BatchCommitRequest{Token: token3, Base: "x", Members: []BatchMember{
		{TarPath: bad, RelPath: "a/1-1", Encoding: EncodingGzip}}})
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Errorf("err = %v", err)
	}
	// An entry leaving its directory, found only as it streams: the ingest is
	// killed before its input ends.
	escape, _ := gzipped(t, writeTestTar(t, []batchTestEntry{batchDir("./"), batchFile("../escape", "x")}))
	seen := filepath.Join(t.TempDir(), "seen")
	b4, token4, _ := batchBackend(t, `[ "$1" = ingest ] || exit 0
cat > /dev/null; echo end-of-input > `+seen)
	err = commitBatchWithin(t, b4, BatchCommitRequest{Token: token4, Base: "x", Members: []BatchMember{
		{TarPath: plain, RelPath: "b/1-1"}, {TarPath: escape, RelPath: "a/1-1", Encoding: EncodingGzip}}})
	time.Sleep(200 * time.Millisecond)
	if _, statErr := os.Stat(seen); err == nil || statErr == nil {
		t.Errorf("err = %v; the ingest reached the end of its input: %v", err, statErr == nil)
	}
}

// Backends that read the payload as a plain tar refuse a compressed one.
func TestLocalBackend_RefusesACompressedPayload(t *testing.T) {
	b := &LocalBackend{}
	if err := b.Commit(context.Background(), CommitRequest{Token: "r", TarEncoding: EncodingGzip}); err == nil {
		t.Error("accepted")
	}
}
