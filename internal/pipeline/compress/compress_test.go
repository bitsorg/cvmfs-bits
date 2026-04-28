package compress

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/pipeline/unpack"
	"cvmfs.io/prepub/pkg/observe"
)

func TestCompressSmallFileNotChunked(t *testing.T) {
	// File smaller than chunk size should not be chunked
	data := make([]byte, 1024)
	entry := unpack.FileEntry{
		Path:    "/small.bin",
		Mode:    0o100644,
		Size:    1024,
		ModTime: time.Now(),
		Data:    data,
	}

	result, err := compressEntry(entry, 4*1024*1024) // 4 MB chunk size
	if err != nil {
		t.Fatalf("compressEntry failed: %v", err)
	}

	if result.Chunks != nil && len(result.Chunks) > 0 {
		t.Errorf("Expected no chunks for small file, got %d", len(result.Chunks))
	}
	if result.Compressed == nil || len(result.Compressed) == 0 {
		t.Errorf("Expected Compressed to be non-empty for non-chunked file")
	}
	if result.Hash == "" {
		t.Errorf("Expected non-empty Hash")
	}
}

func TestCompressZeroChunkSizeNoChunking(t *testing.T) {
	// ChunkSize=0 should disable chunking even for large files
	data := make([]byte, 10*1024*1024)
	entry := unpack.FileEntry{
		Path:    "/large.bin",
		Mode:    0o100644,
		Size:    int64(len(data)),
		ModTime: time.Now(),
		Data:    data,
	}

	result, err := compressEntry(entry, 0) // 0 = no chunking
	if err != nil {
		t.Fatalf("compressEntry failed: %v", err)
	}

	if result.Chunks != nil && len(result.Chunks) > 0 {
		t.Errorf("Expected no chunks with ChunkSize=0, got %d", len(result.Chunks))
	}
	if result.Compressed == nil || len(result.Compressed) == 0 {
		t.Errorf("Expected Compressed to be non-empty")
	}
}

func TestCompressChunked(t *testing.T) {
	// File larger than chunk size should be chunked
	data := make([]byte, 9*1024*1024) // 9 MB
	for i := range data {
		data[i] = byte(i % 256)
	}
	entry := unpack.FileEntry{
		Path:    "/large.bin",
		Mode:    0o100644,
		Size:    int64(len(data)),
		ModTime: time.Now(),
		Data:    data,
	}

	chunkSize := int64(4 * 1024 * 1024) // 4 MB
	result, err := compressEntry(entry, chunkSize)
	if err != nil {
		t.Fatalf("compressEntry failed: %v", err)
	}

	// Verify chunking occurred
	if len(result.Chunks) == 0 {
		t.Fatalf("Expected chunks, got none")
	}

	// 9 MB / 4 MB = 3 chunks (4 MB, 4 MB, 1 MB)
	if len(result.Chunks) != 3 {
		t.Errorf("Expected 3 chunks, got %d", len(result.Chunks))
	}

	// Verify Compressed is nil for chunked files
	if result.Compressed != nil {
		t.Errorf("Expected Compressed=nil for chunked files, got non-nil")
	}

	// Verify offsets and sizes
	expectedOffsets := []int64{0, 4 * 1024 * 1024, 8 * 1024 * 1024}
	expectedSizes := []int64{4 * 1024 * 1024, 4 * 1024 * 1024, 1024 * 1024}

	for i, chunk := range result.Chunks {
		if chunk.Offset != expectedOffsets[i] {
			t.Errorf("Chunk %d: expected offset %d, got %d", i, expectedOffsets[i], chunk.Offset)
		}
		if chunk.UncompressedSize != expectedSizes[i] {
			t.Errorf("Chunk %d: expected size %d, got %d", i, expectedSizes[i], chunk.UncompressedSize)
		}
		if chunk.Hash == "" {
			t.Errorf("Chunk %d: expected non-empty Hash", i)
		}
		if chunk.Compressed == nil || len(chunk.Compressed) == 0 {
			t.Errorf("Chunk %d: expected non-empty Compressed", i)
		}
	}

	// Verify bulk hash (result.Hash) is present
	if result.Hash == "" {
		t.Errorf("Expected non-empty bulk Hash for chunked file")
	}
}

func TestCompressBulkHashMatchesFullContent(t *testing.T) {
	// Bulk hash for a chunked file should match hash of full content
	data := []byte("test data for hashing")
	entry := unpack.FileEntry{
		Path:    "/test.bin",
		Mode:    0o100644,
		Size:    int64(len(data)),
		ModTime: time.Now(),
		Data:    data,
	}

	result, err := compressEntry(entry, 0) // No chunking to get single hash
	if err != nil {
		t.Fatalf("compressEntry failed: %v", err)
	}

	// Manually compute expected hash
	h := sha256.New()
	h.Write(data)
	expectedHash := hex.EncodeToString(h.Sum(nil))

	if result.Hash != expectedHash {
		t.Errorf("Hash mismatch: expected %s, got %s", expectedHash, result.Hash)
	}
}

func TestCompressChunkHashesCorrect(t *testing.T) {
	// Each chunk hash should match the SHA256 of that chunk's uncompressed data
	data := []byte("chunk0chunk1chunk2")
	entry := unpack.FileEntry{
		Path:    "/chunked.bin",
		Mode:    0o100644,
		Size:    int64(len(data)),
		ModTime: time.Now(),
		Data:    data,
	}

	// 6 bytes per chunk
	chunkSize := int64(6)
	result, err := compressEntry(entry, chunkSize)
	if err != nil {
		t.Fatalf("compressEntry failed: %v", err)
	}

	expectedChunks := []string{"chunk0", "chunk1", "chunk2"}
	if len(result.Chunks) != len(expectedChunks) {
		t.Fatalf("Expected %d chunks, got %d", len(expectedChunks), len(result.Chunks))
	}

	for i, chunk := range result.Chunks {
		h := sha256.New()
		h.Write([]byte(expectedChunks[i]))
		expectedHash := hex.EncodeToString(h.Sum(nil))

		if chunk.Hash != expectedHash {
			t.Errorf("Chunk %d hash mismatch: expected %s, got %s", i, expectedHash, chunk.Hash)
		}
	}
}

func TestCompressDirectory(t *testing.T) {
	// Directories should get sentinel hash
	entry := unpack.FileEntry{
		Path:    "/dir",
		Mode:    fs.ModeDir | 0o755,
		Size:    4096,
		ModTime: time.Now(),
	}

	result, err := compressEntry(entry, 1024)
	if err != nil {
		t.Fatalf("compressEntry failed: %v", err)
	}

	if result.Hash != "0000000000000000000000000000000000000000000000000000000000000000" {
		t.Errorf("Expected zero sentinel hash for directory, got %s", result.Hash)
	}
	if result.Compressed != nil {
		t.Errorf("Expected nil Compressed for directory")
	}
}

func TestCompressSymlink(t *testing.T) {
	// Symlinks should get sentinel hash
	entry := unpack.FileEntry{
		Path:       "/link",
		Mode:       fs.ModeSymlink | 0o777,
		ModTime:    time.Now(),
		LinkTarget: "/target",
	}

	result, err := compressEntry(entry, 1024)
	if err != nil {
		t.Fatalf("compressEntry failed: %v", err)
	}

	if result.Hash != "0000000000000000000000000000000000000000000000000000000000000000" {
		t.Errorf("Expected zero sentinel hash for symlink, got %s", result.Hash)
	}
	if result.Compressed != nil {
		t.Errorf("Expected nil Compressed for symlink")
	}
}

func TestRunWithConfig(t *testing.T) {
	// Test Run function with Config parameter
	in := make(chan unpack.FileEntry, 2)
	out := make(chan Result, 10)

	entry1 := unpack.FileEntry{
		Path:    "/file1.txt",
		Mode:    0o100644,
		Size:    100,
		ModTime: time.Now(),
		Data:    make([]byte, 100),
	}
	entry2 := unpack.FileEntry{
		Path:    "/file2.txt",
		Mode:    0o100644,
		Size:    50,
		ModTime: time.Now(),
		Data:    make([]byte, 50),
	}

	go func() {
		in <- entry1
		in <- entry2
		close(in)
	}()

	ctx := context.Background()
	obs, shutdown, err := observe.New("test-compress")
	if err != nil {
		t.Fatalf("observe.New failed: %v", err)
	}
	defer shutdown()

	cfg := Config{
		Workers:   1,
		ChunkSize: 0, // No chunking
	}

	err = Run(ctx, in, out, cfg, obs)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	close(out)

	results := make([]Result, 0)
	for r := range out {
		results = append(results, r)
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	for i, r := range results {
		if r.FileEntry.Path != fmt.Sprintf("/file%d.txt", i+1) {
			t.Errorf("Result %d: expected path /file%d.txt, got %s", i, i+1, r.FileEntry.Path)
		}
		if r.Hash == "" {
			t.Errorf("Result %d: expected non-empty Hash", i)
		}
	}
}

// TestCompressEntryChunkedGuardNonPositiveChunkSize verifies that
// compressEntryChunked returns an error for zero and negative chunkSize values
// (Fix C4).
func TestCompressEntryChunkedGuardNonPositiveChunkSize(t *testing.T) {
	entry := unpack.FileEntry{
		Path:    "/file.bin",
		Mode:    0o100644,
		Size:    100,
		ModTime: time.Now(),
		Data:    make([]byte, 100),
	}

	for _, badSize := range []int64{0, -1, -1024} {
		_, err := compressEntryChunked(entry, badSize, "aabbcc")
		if err == nil {
			t.Errorf("chunkSize=%d: expected error, got nil", badSize)
		}
	}
}

// TestCompressZeroChunkSizeFallsBackToWhole verifies that compressEntry does NOT
// call compressEntryChunked when chunkSize is 0 — the file should be processed
// as a single whole object regardless of its size (Fix C4 companion).
func TestCompressZeroChunkSizeFallsBackToWhole(t *testing.T) {
	// Large file: if chunkSize were applied it would be split.
	data := make([]byte, 16*1024)
	entry := unpack.FileEntry{
		Path:    "/big.bin",
		Mode:    0o100644,
		Size:    int64(len(data)),
		ModTime: time.Now(),
		Data:    data,
	}

	result, err := compressEntry(entry, 0) // chunkSize=0 means no chunking
	if err != nil {
		t.Fatalf("compressEntry(chunkSize=0) failed: %v", err)
	}
	if len(result.Chunks) != 0 {
		t.Errorf("expected no chunks when chunkSize=0, got %d", len(result.Chunks))
	}
	if result.Hash == "" {
		t.Error("expected non-empty hash for whole-file compression")
	}
	if len(result.Compressed) == 0 {
		t.Error("expected non-empty compressed bytes for whole-file compression")
	}
}

// TestChunkCompressedSizeMatchesLen verifies that Chunk.CompressedSize equals
// len(Chunk.Compressed) for every chunk (Fix L1 — was int64(len(compBuf.Bytes()))
// called twice; now uses compBuf.Len() and an explicit copy).
func TestChunkCompressedSizeMatchesLen(t *testing.T) {
	// Make a file large enough to produce multiple chunks.
	const chunkSize = 1024
	data := make([]byte, 5*chunkSize+13) // 5 full chunks + one partial
	for i := range data {
		data[i] = byte(i)
	}
	entry := unpack.FileEntry{
		Path:    "/big.bin",
		Mode:    0o100644,
		Size:    int64(len(data)),
		ModTime: time.Now(),
		Data:    data,
	}

	result, err := compressEntry(entry, chunkSize)
	if err != nil {
		t.Fatalf("compressEntry failed: %v", err)
	}
	if len(result.Chunks) == 0 {
		t.Fatal("expected chunks, got none")
	}
	for i, ch := range result.Chunks {
		if int64(len(ch.Compressed)) != ch.CompressedSize {
			t.Errorf("chunk %d: len(Compressed)=%d but CompressedSize=%d",
				i, len(ch.Compressed), ch.CompressedSize)
		}
	}
}

// TestChunkBufferReuse verifies that chunks produced on separate iterations are
// independent — modifying one chunk's Compressed slice does not affect another
// (Fix L2 — buffer is reset and content is copied, not shared).
//
// The approach: save a deep copy of chunk[1].Compressed before mutating
// chunk[0], then verify chunk[1] is byte-for-byte identical to the saved copy.
// This is robust regardless of what the compressed data looks like — using a
// value-comparison approach avoids the false-positive from zlib output that
// happens to contain the sentinel byte we would write into chunk[0].
func TestChunkBufferReuse(t *testing.T) {
	const chunkSize = 512
	// Use distinct content per chunk so the two chunks differ and aliasing
	// would be immediately visible even without mutation.
	data := make([]byte, 2*chunkSize)
	for i := 0; i < chunkSize; i++ {
		data[i] = byte(i & 0xFF) // ramp up
	}
	for i := 0; i < chunkSize; i++ {
		data[chunkSize+i] = byte((chunkSize - i) & 0xFF) // ramp down
	}
	entry := unpack.FileEntry{
		Path:    "/two.bin",
		Mode:    0o100644,
		Size:    int64(len(data)),
		ModTime: time.Now(),
		Data:    data,
	}

	result, err := compressEntry(entry, chunkSize)
	if err != nil {
		t.Fatalf("compressEntry failed: %v", err)
	}
	if len(result.Chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(result.Chunks))
	}

	// Save a byte-level copy of chunk[1].Compressed before we touch chunk[0].
	chunk1Before := make([]byte, len(result.Chunks[1].Compressed))
	copy(chunk1Before, result.Chunks[1].Compressed)

	// XOR every byte of chunk[0].Compressed with 0xFF to flip all bits.
	for i := range result.Chunks[0].Compressed {
		result.Chunks[0].Compressed[i] ^= 0xFF
	}

	// chunk[1].Compressed must be byte-for-byte identical to the saved copy —
	// i.e. our mutation of chunk[0] must not have touched chunk[1]'s backing array.
	for i, b := range result.Chunks[1].Compressed {
		if b != chunk1Before[i] {
			t.Errorf("chunk[1].Compressed[%d] changed from 0x%02x to 0x%02x after mutating chunk[0] — shared backing array",
				i, chunk1Before[i], b)
			break
		}
	}
}
