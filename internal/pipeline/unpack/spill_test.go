// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package unpack

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// buildTar returns a tar containing n regular files of size each.
func buildTar(t *testing.T, n int, size int) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	body := bytes.Repeat([]byte("x"), size)
	for i := 0; i < n; i++ {
		if err := tw.WriteHeader(&tar.Header{
			Name: fmt.Sprintf("pkg/file%03d.bin", i),
			Mode: 0o644, Size: int64(size), Typeflag: tar.TypeReg,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func collect(t *testing.T, tarBytes []byte, opts Options) []FileEntry {
	t.Helper()
	ch := make(chan FileEntry, 1024)
	errCh := make(chan error, 1)
	go func() {
		errCh <- ExtractWithOptions(context.Background(), bytes.NewReader(tarBytes), ch, opts)
		close(ch)
	}()
	var out []FileEntry
	for e := range ch {
		out = append(out, e)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("ExtractWithOptions: %v", err)
	}
	return out
}

// TestSpillKeepsPackageOutOfMemory is the regression guard for the OOM that
// killed the service in production: collecting entries used to retain the
// ENTIRE uncompressed package (once in the hard-link table, once in the sorted
// entry list), so peak memory scaled with package size and no amount of worker
// tuning helped.
//
// It measures retained heap, not correctness — a future change that reverts to
// buffering will fail here rather than in production at 3am.
func TestSpillKeepsPackageOutOfMemory(t *testing.T) {
	const (
		files    = 16
		fileSize = 1 << 20 // 1 MiB each => 16 MiB package
	)
	tarBytes := buildTar(t, files, fileSize)
	dir := t.TempDir()

	readHeap := func() uint64 {
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		return ms.HeapAlloc
	}

	before := readHeap()
	entries := collect(t, tarBytes, Options{SpillDir: dir, InlineMaxSize: 4096})
	retained := readHeap()

	if len(entries) != files {
		t.Fatalf("got %d entries, want %d", len(entries), files)
	}
	// Every entry exceeded the inline threshold, so none should carry bytes.
	for _, e := range entries {
		if e.ContentPath == "" {
			t.Errorf("%s was not spilled", e.Path)
		}
		if e.Data != nil {
			t.Errorf("%s still holds %d bytes in memory", e.Path, len(e.Data))
		}
	}

	// Retained heap must be a small fraction of the package, not a multiple of
	// it. Signed arithmetic: the heap legitimately ends up SMALLER than the
	// baseline once the tar bytes are collected, and an unsigned subtraction
	// would wrap into a huge bogus number.
	grew := int64(retained) - int64(before)
	if grew > 4<<20 {
		t.Errorf("retained %d bytes after collecting a %d-byte package — content is still being buffered",
			grew, files*fileSize)
	}
	t.Logf("retained %d bytes for a %d-byte package", grew, files*fileSize)
}

// Content must survive the round trip through the spill file unchanged.
func TestSpilledContentRoundTrips(t *testing.T) {
	dir := t.TempDir()
	entries := collect(t, buildTar(t, 3, 128*1024), Options{SpillDir: dir, InlineMaxSize: 1024})

	for _, e := range entries {
		got, err := e.Bytes()
		if err != nil {
			t.Fatalf("%s: Bytes: %v", e.Path, err)
		}
		if len(got) != 128*1024 || !bytes.Equal(got, bytes.Repeat([]byte("x"), 128*1024)) {
			t.Errorf("%s: content mismatch (%d bytes)", e.Path, len(got))
		}
		if e.Size != int64(len(got)) {
			t.Errorf("%s: Size=%d but content is %d bytes", e.Path, e.Size, len(got))
		}
		// Open() must give a fresh reader each time.
		for i := 0; i < 2; i++ {
			rc, oerr := e.Open()
			if oerr != nil {
				t.Fatalf("%s: Open #%d: %v", e.Path, i, oerr)
			}
			n, _ := io.Copy(io.Discard, rc)
			_ = rc.Close()
			if n != e.Size {
				t.Errorf("%s: Open #%d read %d bytes, want %d", e.Path, i, n, e.Size)
			}
		}
	}
}

// Small files stay in memory: spilling every tiny file would trade memory for
// an inode and several syscalls per file, and a software tree is mostly tiny
// files by count.
func TestSmallFilesStayInline(t *testing.T) {
	dir := t.TempDir()
	entries := collect(t, buildTar(t, 4, 100), Options{SpillDir: dir, InlineMaxSize: 4096})
	for _, e := range entries {
		if e.ContentPath != "" {
			t.Errorf("%s was spilled despite being below the inline threshold", e.Path)
		}
		if len(e.Data) != 100 {
			t.Errorf("%s: Data has %d bytes, want 100", e.Path, len(e.Data))
		}
	}
	ents, _ := os.ReadDir(dir)
	if len(ents) != 0 {
		t.Errorf("spill dir should be empty, has %d files", len(ents))
	}
}

// With no SpillDir the previous all-in-memory behaviour is preserved, so
// existing callers and tests are unaffected.
func TestNoSpillDirKeepsLegacyBehaviour(t *testing.T) {
	entries := collect(t, buildTar(t, 2, 256*1024), Options{})
	for _, e := range entries {
		if e.ContentPath != "" {
			t.Errorf("%s spilled without a SpillDir", e.Path)
		}
		if len(e.Data) != 256*1024 {
			t.Errorf("%s: Data has %d bytes", e.Path, len(e.Data))
		}
	}
}

// A hard link must reference the target's spill file rather than duplicating
// its bytes — the hard-link table was the second full-package retention.
func TestHardLinkSharesSpillFile(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	body := bytes.Repeat([]byte("y"), 200*1024)
	if err := tw.WriteHeader(&tar.Header{Name: "pkg/real.bin", Mode: 0o644, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: "pkg/link.bin", Mode: 0o644, Typeflag: tar.TypeLink, Linkname: "pkg/real.bin"}); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	entries := collect(t, buf.Bytes(), Options{SpillDir: dir, InlineMaxSize: 1024})
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}
	if entries[0].ContentPath != entries[1].ContentPath {
		t.Errorf("hard link points at %q, target at %q — content was duplicated",
			entries[1].ContentPath, entries[0].ContentPath)
	}
	if entries[1].Data != nil {
		t.Error("hard link carries an in-memory copy of the target's bytes")
	}
	// Exactly one spill file for the two paths.
	ents, _ := os.ReadDir(filepath.Clean(dir))
	if len(ents) != 1 {
		t.Errorf("spill dir has %d files, want 1 shared by both paths", len(ents))
	}
}
