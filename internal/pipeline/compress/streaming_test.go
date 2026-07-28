// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package compress

import (
	"bytes"
	"context"
	"crypto/sha1" //nolint:gosec // CVMFS protocol
	"encoding/hex"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"cvmfs.io/prepub/internal/pipeline/unpack"
	"cvmfs.io/prepub/pkg/observe"
)

// spilledEntry writes size bytes of incompressible data to a file and returns
// an entry referencing it, mimicking what unpack does for a large file.
func spilledEntry(t *testing.T, dir string, size int) (unpack.FileEntry, []byte) {
	t.Helper()
	data := make([]byte, size)
	r := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic test data
	_, _ = r.Read(data)

	path := filepath.Join(dir, "big.bin")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return unpack.FileEntry{
		Path: "pkg/big.bin", Mode: 0o644, Size: int64(size), ContentPath: path,
	}, data
}

// TestStreamingDoesNotHoldFileOrChunks is the memory guarantee: with a fixed
// grid and a spill dir, neither the file nor its compressed chunks are
// resident. Previously a Result pinned the whole file (via Bytes) AND every
// chunk's Compressed bytes, which kept the service at 2 GB RSS on a large
// Clang binary even after the unpack spill landed.
func TestStreamingDoesNotHoldFileOrChunks(t *testing.T) {
	const grid = 1 << 20 // 1 MiB grid
	const size = 24 << 20

	src := t.TempDir()
	spill := t.TempDir()
	entry, data := spilledEntry(t, src, size)

	readHeap := func() uint64 {
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		return ms.HeapAlloc
	}

	var seq int64
	before := readHeap()
	res, err := compressEntryStreaming(entry, grid, 0, spill, &seq)
	if err != nil {
		t.Fatalf("compressEntryStreaming: %v", err)
	}
	after := readHeap()

	if len(res.Chunks) != size/grid {
		t.Fatalf("got %d chunks, want %d", len(res.Chunks), size/grid)
	}
	for i, c := range res.Chunks {
		if c.Path == "" {
			t.Errorf("chunk %d was not spilled", i)
		}
		if c.Compressed != nil {
			t.Errorf("chunk %d still holds %d compressed bytes in memory", i, len(c.Compressed))
		}
	}

	// A few MiB of working buffers is fine; a multiple of the 24 MiB file is not.
	if grew := int64(after) - int64(before); grew > 8<<20 {
		t.Errorf("retained %d bytes compressing a %d-byte file — still buffering", grew, size)
	}

	// The bulk hash must equal SHA-1 of the raw content, computed incrementally.
	want := sha1.Sum(data) //nolint:gosec
	if res.Hash != hex.EncodeToString(want[:]) {
		t.Errorf("bulk hash = %s, want %s", res.Hash, hex.EncodeToString(want[:]))
	}
}

// The streaming path must produce byte-identical chunk boundaries, hashes and
// content to the in-memory path — this is what the published catalog records,
// so any divergence would be a silent corruption.
func TestStreamingMatchesInMemoryChunking(t *testing.T) {
	const grid = 1 << 20
	const size = 5<<20 + 12345 // deliberately not a grid multiple

	src, spill := t.TempDir(), t.TempDir()
	entry, data := spilledEntry(t, src, size)

	var seq int64
	streamed, err := compressEntryStreaming(entry, grid, 0, spill, &seq)
	if err != nil {
		t.Fatalf("streaming: %v", err)
	}

	// In-memory reference: same fixed grid via the CDC path.
	inMem, err := compressEntry(unpack.FileEntry{
		Path: entry.Path, Mode: entry.Mode, Size: entry.Size, Data: data,
	}, grid, 0)
	if err != nil {
		t.Fatalf("in-memory: %v", err)
	}

	if len(streamed.Chunks) != len(inMem.Chunks) {
		t.Fatalf("chunk count: streamed %d, in-memory %d", len(streamed.Chunks), len(inMem.Chunks))
	}
	if streamed.Hash != inMem.Hash {
		t.Errorf("bulk hash differs: %s vs %s", streamed.Hash, inMem.Hash)
	}
	for i := range streamed.Chunks {
		a, b := streamed.Chunks[i], inMem.Chunks[i]
		if a.Hash != b.Hash || a.Offset != b.Offset || a.UncompressedSize != b.UncompressedSize {
			t.Errorf("chunk %d differs: streamed{%s off=%d len=%d} vs inmem{%s off=%d len=%d}",
				i, a.Hash, a.Offset, a.UncompressedSize, b.Hash, b.Offset, b.UncompressedSize)
		}
		// And the spilled bytes must be exactly what the in-memory path produced.
		rc, oerr := a.Open()
		if oerr != nil {
			t.Fatalf("chunk %d Open: %v", i, oerr)
		}
		got, _ := io.ReadAll(rc)
		_ = rc.Close()
		if !bytes.Equal(got, b.Compressed) {
			t.Errorf("chunk %d compressed bytes differ (%d vs %d)", i, len(got), len(b.Compressed))
		}
	}
}

// An empty file must still yield exactly one zero-length chunk: ingestsql
// forces expected_num_chunks to 1 when size == 0.
func TestStreamingEmptyFileYieldsOneChunk(t *testing.T) {
	src, spill := t.TempDir(), t.TempDir()
	path := filepath.Join(src, "empty")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	var seq int64
	res, err := compressEntryStreaming(
		unpack.FileEntry{Path: "pkg/empty", Mode: 0o644, ContentPath: path}, 1<<20, 0, spill, &seq)
	if err != nil {
		t.Fatalf("compressEntryStreaming: %v", err)
	}
	if len(res.Chunks) != 1 {
		t.Fatalf("got %d chunks for an empty file, want exactly 1", len(res.Chunks))
	}
	if res.Chunks[0].UncompressedSize != 0 {
		t.Errorf("chunk size = %d, want 0", res.Chunks[0].UncompressedSize)
	}
}

// Run must only stream when the grid is FIXED: content-defined boundaries need
// a rolling window over the whole buffer and must keep the in-memory path.
func TestRunStreamsOnlyOnFixedGrid(t *testing.T) {
	obs, shutdown, oerr := observe.New("test-compress-streaming")
	if oerr != nil {
		t.Fatalf("observe.New: %v", oerr)
	}
	defer shutdown() //nolint:errcheck // test teardown
	run := func(cfg Config, e unpack.FileEntry) Result {
		t.Helper()
		in := make(chan unpack.FileEntry, 1)
		out := make(chan Result, 1)
		in <- e
		close(in)
		if err := Run(context.Background(), in, out, cfg, obs); err != nil {
			t.Fatalf("Run: %v", err)
		}
		close(out)
		return <-out
	}

	src, spill := t.TempDir(), t.TempDir()
	entry, _ := spilledEntry(t, src, 3<<20)

	fixed := run(Config{Workers: 1, ChunkMin: 1 << 20, ChunkAvg: 1 << 20, ChunkMax: 1 << 20, SpillDir: spill}, entry)
	for i, c := range fixed.Chunks {
		if c.Path == "" {
			t.Errorf("fixed grid: chunk %d not streamed to disk", i)
		}
	}

	cdc := run(Config{Workers: 1, ChunkMin: 1 << 19, ChunkAvg: 1 << 20, ChunkMax: 1 << 21, SpillDir: spill}, entry)
	for i, c := range cdc.Chunks {
		if c.Path != "" {
			t.Errorf("content-defined grid: chunk %d was streamed; that path needs the whole buffer", i)
		}
	}
}
