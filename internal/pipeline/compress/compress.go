// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Package compress provides parallel compression and hashing of tar file entries.
package compress

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1" //nolint:gosec // CVMFS CAS key = SHA-1(zlib(content)); see hash.go
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"cvmfs.io/prepub/internal/pipeline/chunker"
	"cvmfs.io/prepub/internal/pipeline/unpack"
	"cvmfs.io/prepub/pkg/observe"
)

// zlibWriterPool caches zlib.Writer instances by compression level to avoid
// allocating a fresh 32 KB zlib internal window on every file.  Each pool is
// keyed by the effective level constant so Reset(w) is always called on a
// writer whose level matches the caller's requirement.
//
// The pool for each level is created on first use via sync.Pool.New.
var zlibWriterPools sync.Map // map[int]*sync.Pool

func getZlibWriterPool(level int) *sync.Pool {
	if p, ok := zlibWriterPools.Load(level); ok {
		return p.(*sync.Pool)
	}
	p := &sync.Pool{
		New: func() any {
			// NewWriterLevel only errors on invalid level constants; the caller has
			// already validated the level via zlibLevel(), so discard the error.
			w, _ := zlib.NewWriterLevel(io.Discard, level)
			return w
		},
	}
	actual, _ := zlibWriterPools.LoadOrStore(level, p)
	return actual.(*sync.Pool)
}

// sha1Pool caches hash.Hash (SHA-1) instances.  sha1.New() allocates ~100 B
// of internal state; with thousands of small files the pool eliminates the
// per-file allocation and the resulting GC pressure.
var sha1Pool = sync.Pool{
	New: func() any { return sha1.New() }, //nolint:gosec
}

// Config holds configuration for the compress stage.
type Config struct {
	Workers   int   // number of concurrent workers
	ChunkSize int64 // 0 = no chunking; files with len(Data) > ChunkSize are split
	// Chunk{Min,Avg,Max} are CVMFS content-defined (xor32) chunking sizes in
	// bytes. When ChunkAvg > 0, files larger than ChunkMin are split at
	// CVMFS-compatible boundaries instead of the fixed-size ChunkSize path.
	ChunkMin int64
	ChunkAvg int64
	ChunkMax int64
	// CompressLevel is the zlib compression level (1=fastest … 9=best).
	// 0 is treated as zlib.DefaultCompression (-1 = level 6).
	// Use zlib.BestSpeed (1) to roughly halve CPU time for CPU-bound publishes
	// at the cost of slightly larger objects.
	CompressLevel int

	// SpillDir enables the streaming path: the entry is read one grid block at
	// a time and each compressed chunk is written here instead of being held
	// in memory, so peak memory per worker is bounded by the grid size rather
	// than by the file size. Empty keeps the in-memory path.
	//
	// Only used when the chunk grid is FIXED (ChunkMin == ChunkAvg == ChunkMax):
	// content-defined boundaries need a rolling window over the whole buffer,
	// whereas a fixed grid cuts at known offsets and streams trivially.
	SpillDir string
}

// Chunk represents a single compressed chunk of a larger file.
//
// Exactly one of Compressed / Path carries the data. Path is used by the
// streaming path: holding every chunk's Compressed bytes meant a Result for a
// multi-GB file pinned the whole compressed file in memory, which is what kept
// the service at 2 GB RSS after the unpack spill landed.
type Chunk struct {
	Offset           int64  // byte offset in the uncompressed file
	UncompressedSize int64  // size of this chunk's uncompressed data
	Hash             string // hex SHA-1 of compressed chunk bytes (= CAS key)
	Compressed       []byte // zlib-compressed chunk data (nil when Path is set)
	Path             string // spill file holding the compressed bytes
	CompressedSize   int64  // size of the compressed data in bytes
}

// Open returns a reader over the chunk's compressed bytes, from memory or from
// its spill file. Callers that retry must call Open again rather than reusing
// a consumed reader.
func (c Chunk) Open() (io.ReadCloser, error) {
	if c.Path == "" {
		return io.NopCloser(bytes.NewReader(c.Compressed)), nil
	}
	f, err := os.Open(c.Path)
	if err != nil {
		return nil, fmt.Errorf("opening compressed chunk %s: %w", c.Hash, err)
	}
	return f, nil
}

// Result carries a processed file entry alongside its compressed form and hash.
type Result struct {
	// FileEntry is the original unpacked entry.
	FileEntry unpack.FileEntry
	// Hash is the SHA-1 hash of the compressed content (= CAS key, CVMFS convention).
	Hash string
	// Compressed is the zlib-compressed content bytes (nil for chunked files).
	Compressed []byte
	// CompressedSize is the size of Compressed in bytes.
	CompressedSize int64
	// Chunks is nil for non-chunked files, populated for chunked files.
	Chunks []Chunk
}

// MaxWorkers is the hard cap on the compress worker pool size.
// It prevents a misconfigured workers value from spawning an unbounded
// number of goroutines that could exhaust available memory.
const MaxWorkers = 256

// Run drains entries from in, compresses and hashes each FileEntry using a worker pool,
// and sends Results to out. It blocks until all entries have been processed and all
// workers have returned. It does NOT close out—the caller is responsible for closing
// out after Run returns. This makes the ownership of out explicit and prevents
// double-close panics when Run is used inside an errgroup.
//
// Worker count is clamped to a safe range: negative or zero values use runtime.NumCPU(),
// and values exceeding 4*runtime.NumCPU() (max MaxWorkers) are capped. This prevents
// misconfiguration from creating an unbounded goroutine explosion.
func Run(ctx context.Context, in <-chan unpack.FileEntry, out chan<- Result, cfg Config, obs *observe.Provider) error {
	ctx, span := obs.Tracer.Start(ctx, "pipeline.compress")
	defer span.End()

	// Fix #16: Clamp workers to a safe range so a bad config value cannot
	// create an unbounded goroutine explosion.
	workers := cfg.Workers
	maxSane := 4 * runtime.NumCPU()
	if maxSane > MaxWorkers {
		maxSane = MaxWorkers
	}
	switch {
	case workers <= 0:
		workers = runtime.NumCPU()
	case workers > maxSane:
		workers = maxSane
	}

	// Stream when the grid is fixed and a spill dir is configured: peak memory
	// then depends on the grid, not on the largest file in the tree.
	streaming := cfg.SpillDir != "" && cfg.ChunkAvg > 0 &&
		cfg.ChunkMin == cfg.ChunkAvg && cfg.ChunkAvg == cfg.ChunkMax
	if streaming {
		obs.Logger.InfoContext(ctx, "compress: streaming mode",
			"grid_bytes", cfg.ChunkAvg, "workers", workers)
	}
	var chunkSeq int64

	eg, egCtx := errgroup.WithContext(ctx)
	sem := semaphore.NewWeighted(int64(workers))

	// Fix #P1: capture sem.Acquire failure without returning early.
	// If we returned here, already-launched eg.Go workers would still be
	// running when our caller closes out (via defer close(compressOut)),
	// causing a "send on closed channel" panic.  Breaking out of the loop
	// and always reaching eg.Wait() ensures every worker exits before we
	// return — and therefore before out is closed.
	var semErr error
	for entry := range in {
		entry := entry // capture for closure
		if err := sem.Acquire(egCtx, 1); err != nil {
			span.RecordError(err)
			semErr = fmt.Errorf("compress semaphore: %w", err)
			break
		}

		eg.Go(func() error {
			defer sem.Release(1)

			_, wspan := obs.Tracer.Start(egCtx, "compress.file")
			defer wspan.End()

			var result Result
			var err error
			switch {
			case streaming:
				result, err = compressEntryStreaming(entry, cfg.ChunkAvg, cfg.CompressLevel, cfg.SpillDir, &chunkSeq)
			case cfg.ChunkAvg > 0:
				det := chunker.NewXor32(uint64(cfg.ChunkMin), uint64(cfg.ChunkAvg), uint64(cfg.ChunkMax))
				result, err = compressEntryCDC(entry, det, cfg.CompressLevel)
			default:
				result, err = compressEntry(entry, cfg.ChunkSize, cfg.CompressLevel)
			}
			if err != nil {
				wspan.RecordError(err)
				return fmt.Errorf("compressing %s: %w", entry.Path, err)
			}

			// Fix #24: guard against nil Metrics (e.g. a manually constructed
			// Provider in tests that omit metric initialisation).
			if obs != nil && obs.Metrics != nil {
				obs.Metrics.PipelineFilesProcessed.Inc()
				obs.Metrics.PipelineBytesCompressed.Add(float64(result.CompressedSize))
			}

			select {
			case out <- result:
				return nil
			case <-egCtx.Done():
				return egCtx.Err()
			}
		})
	}

	// CRITICAL: always wait for every in-flight worker before returning.
	// Our caller (pipeline.go stage 3a) does "defer close(out)" — if any
	// worker goroutine is still running when we return, it will try to send
	// on the now-closed channel and panic.  eg.Wait() provides the
	// happens-before guarantee that out is safe to close after this call.
	if err := eg.Wait(); err != nil {
		span.RecordError(err)
		return err
	}
	return semErr
}

// compressEntryStreaming compresses an entry WITHOUT ever holding the whole
// file, or the whole compressed file, in memory.
//
// It reads one fixed grid block at a time from the entry, compresses it,
// writes the compressed bytes to a spill file, and keeps only the chunk's hash
// and size. Peak memory per worker is therefore
//
//	one grid block + one compressed block  (~2 x grid)
//
// independent of file size, where the previous path cost
//
//	whole file + every compressed chunk
//
// which pinned ~2 GB for a large Clang binary.
//
// Requires a FIXED grid (min == avg == max). Content-defined chunking needs a
// rolling window over the buffer and is left on the in-memory path.
func compressEntryStreaming(entry unpack.FileEntry, grid int64, level int, spillDir string, seq *int64) (Result, error) {
	result := Result{FileEntry: entry}

	if !entry.Mode.IsRegular() {
		result.Hash = "0000000000000000000000000000000000000000"
		return result, nil
	}

	rc, err := entry.Open()
	if err != nil {
		return result, err
	}
	defer rc.Close() //nolint:errcheck // read-only

	effectiveLevel := zlibLevel(level)
	pool := getZlibWriterPool(effectiveLevel)
	w := pool.Get().(*zlib.Writer)
	defer pool.Put(w)

	bulkH := sha1Pool.Get().(hash.Hash) //nolint:gosec
	bulkH.Reset()
	defer sha1Pool.Put(bulkH)

	buf := make([]byte, grid)
	var compBuf bytes.Buffer
	var chunks []Chunk
	var offset int64

	for {
		n, rerr := io.ReadFull(rc, buf)
		if rerr != nil && rerr != io.EOF && rerr != io.ErrUnexpectedEOF {
			return result, fmt.Errorf("reading %s: %w", entry.Path, rerr)
		}
		// Emit a chunk for every block read, and exactly one zero-length chunk
		// for an empty file (ingestsql forces expected_num_chunks to 1 when
		// size == 0, swissknife_ingestsql.cc:1360).
		if n == 0 && offset > 0 {
			break
		}
		block := buf[:n]
		bulkH.Write(block)

		h := sha1Pool.Get().(hash.Hash) //nolint:gosec
		h.Reset()
		compBuf.Reset()
		w.Reset(io.MultiWriter(&compBuf, h))
		if _, werr := w.Write(block); werr != nil {
			sha1Pool.Put(h)
			return result, fmt.Errorf("zlib write at offset %d: %w", offset, werr)
		}
		if cerr := w.Close(); cerr != nil {
			sha1Pool.Put(h)
			return result, fmt.Errorf("zlib close at offset %d: %w", offset, cerr)
		}
		chunkHash := hex.EncodeToString(h.Sum(nil))
		sha1Pool.Put(h)

		path, perr := writeChunkSpill(spillDir, seq, compBuf.Bytes())
		if perr != nil {
			return result, perr
		}
		size := int64(compBuf.Len())
		chunks = append(chunks, Chunk{
			Offset:           offset,
			UncompressedSize: int64(n),
			Hash:             chunkHash,
			Path:             path,
			CompressedSize:   size,
		})
		result.CompressedSize += size
		offset += int64(n)

		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
	}

	result.Hash = hex.EncodeToString(bulkH.Sum(nil))
	result.Chunks = chunks
	return result, nil
}

// writeChunkSpill writes one compressed chunk to the spill directory. Names are
// sequence-based so nothing derived from tar content reaches the filesystem.
func writeChunkSpill(dir string, seq *int64, data []byte) (string, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("creating chunk spill dir: %w", err)
	}
	n := atomic.AddInt64(seq, 1)
	path := filepath.Join(dir, fmt.Sprintf("c%012d.z", n))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return "", fmt.Errorf("writing compressed chunk: %w", err)
	}
	return path, nil
}

// zlibLevel converts a pipeline compress level (0 = default) to a zlib level constant.
func zlibLevel(level int) int {
	if level == 0 {
		return zlib.DefaultCompression
	}
	return level
}

func compressEntry(entry unpack.FileEntry, chunkSize int64, level int) (Result, error) {
	result := Result{FileEntry: entry}

	// Directories and symlinks get a zero-hash sentinel — no content to store.
	// 40 zeros is the SHA-1-length zero sentinel (matches CVMFS CAS key width).
	if !entry.Mode.IsRegular() {
		result.Hash = "0000000000000000000000000000000000000000"
		return result, nil
	}

	// Check if we should chunk this file (before compression, based on raw size).
	entryData, derr := entry.Bytes()
	if derr != nil {
		return Result{}, derr
	}
	if chunkSize > 0 && int64(len(entryData)) > chunkSize {
		return compressEntryChunked(entry, chunkSize, level)
	}

	// Single-pass compress + hash.
	//
	// The CAS key is SHA-1(zlib(content)) (CVMFS convention; see hash.go).
	// zlib output flows into both the accumulation buffer and the SHA-1 hasher
	// simultaneously via io.MultiWriter, halving the memory traffic for this
	// stage.
	//
	// Both the sha1.Hash and the zlib.Writer are reused across files via
	// sync.Pool to avoid per-file allocations (~100 B for sha1, ~32 KB for
	// the zlib internal window state) and the associated GC pressure.
	h := sha1Pool.Get().(hash.Hash) //nolint:gosec
	h.Reset()
	defer sha1Pool.Put(h)

	effectiveLevel := zlibLevel(level)
	pool := getZlibWriterPool(effectiveLevel)
	w := pool.Get().(*zlib.Writer)
	defer pool.Put(w)

	var compBuf bytes.Buffer
	w.Reset(io.MultiWriter(&compBuf, h))

	if _, err := w.Write(entryData); err != nil {
		return result, fmt.Errorf("zlib write: %w", err)
	}
	if err := w.Close(); err != nil {
		return result, fmt.Errorf("zlib close: %w", err)
	}

	result.Compressed = compBuf.Bytes()
	result.CompressedSize = int64(len(result.Compressed))
	result.Hash = hex.EncodeToString(h.Sum(nil))

	return result, nil
}

func compressEntryChunked(entry unpack.FileEntry, chunkSize int64, level int) (Result, error) {
	// Fix C4: guard against a non-positive chunkSize reaching this function.
	// compressEntry already checks this via the caller, but a defensive check
	// here prevents subtle bugs if compressEntryChunked is ever called directly.
	if chunkSize <= 0 {
		return Result{}, fmt.Errorf("compressEntryChunked: chunkSize must be positive, got %d", chunkSize)
	}

	result := Result{FileEntry: entry}

	data, derr := entry.Bytes()
	if derr != nil {
		return Result{}, derr
	}
	var chunks []Chunk
	offset := int64(0)

	// Compute the bulk hash: SHA-1 of the full UNCOMPRESSED file content.
	// CVMFS standard for chunked files: the catalog's "bulk hash" is the SHA-1
	// of the complete uncompressed file, not of any individual chunk.  This
	// differs from non-chunked files (where catalog hash = CAS key =
	// SHA-1(compressed)) but matches what CVMFS clients expect when verifying
	// chunked file integrity.  Per-chunk CAS keys are still SHA-1(zlib(chunk)).
	bulkH := sha1Pool.Get().(hash.Hash) //nolint:gosec
	bulkH.Reset()
	bulkH.Write(data)
	bulkHash := hex.EncodeToString(bulkH.Sum(nil))
	sha1Pool.Put(bulkH)

	// Allocate the compression buffer once and reset it between chunks
	// rather than creating a new bytes.Buffer on every iteration.  For files
	// split into thousands of chunks this materially reduces GC pressure.
	var compBuf bytes.Buffer

	// Acquire the pooled zlib.Writer once for all chunks in this file — Reset
	// is called per-chunk to retarget the writer without reallocating the 32 KB
	// internal window state.
	effectiveLevel := zlibLevel(level)
	pool := getZlibWriterPool(effectiveLevel)
	w := pool.Get().(*zlib.Writer)
	defer pool.Put(w)

	// Split data into chunks, compress each independently, and hash the
	// COMPRESSED bytes.  CVMFS CAS convention: SHA-1(zlib(content)).
	for offset < int64(len(data)) {
		chunkEnd := offset + chunkSize
		if chunkEnd > int64(len(data)) {
			chunkEnd = int64(len(data))
		}

		chunkData := data[offset:chunkEnd]
		uncompressedSize := int64(len(chunkData))

		// Single-pass compress + hash: zlib output flows into compBuf and the
		// SHA-1 hasher simultaneously via io.MultiWriter (same optimisation as
		// compressEntry).  A pooled sha1.Hash is used and returned after each
		// chunk; each chunk is an independent CAS object with its own key.
		h := sha1Pool.Get().(hash.Hash) //nolint:gosec
		h.Reset()
		compBuf.Reset()
		w.Reset(io.MultiWriter(&compBuf, h))
		if _, err := w.Write(chunkData); err != nil {
			sha1Pool.Put(h)
			return result, fmt.Errorf("zlib write for chunk at offset %d: %w", offset, err)
		}
		if err := w.Close(); err != nil {
			sha1Pool.Put(h)
			return result, fmt.Errorf("zlib close for chunk at offset %d: %w", offset, err)
		}

		// compBuf.Len() reads the length from an internal int field (cheaper
		// than len(compBuf.Bytes()) which constructs a full slice header).
		compressedSize := int64(compBuf.Len())
		compressed := make([]byte, compressedSize)
		copy(compressed, compBuf.Bytes())
		chunkHash := hex.EncodeToString(h.Sum(nil))
		sha1Pool.Put(h)

		chunks = append(chunks, Chunk{
			Offset:           offset,
			UncompressedSize: uncompressedSize,
			Hash:             chunkHash,
			Compressed:       compressed,
			CompressedSize:   compressedSize,
		})

		offset = chunkEnd
	}

	// For chunked files, result.Hash is the CVMFS standard bulk hash:
	// SHA-1 of the full uncompressed content (computed above as bulkHash).
	// Per-chunk CAS keys (SHA-1(zlib(chunk))) are stored in result.Chunks[i].Hash.
	// If data is empty (no chunks produced), use the 40-zero sentinel.
	if len(chunks) > 0 {
		result.Hash = bulkHash
	} else {
		result.Hash = "0000000000000000000000000000000000000000"
	}

	result.Chunks = chunks
	// For chunked files, Compressed is nil and we don't set CompressedSize
	// (each chunk has its own compressed size)
	return result, nil
}

// compressEntryCDC compresses a file using CVMFS-compatible content-defined
// (xor32) chunking. The file is split at det.Cuts boundaries; each chunk is an
// independent CAS object keyed by SHA-1(zlib(chunk)).
//
// A file that yields a single piece (size <= min, or no cut found before EOF)
// is NOT collapsed to a bulk object: it becomes a one-chunk file, so its CAS
// key carries the 'P' (kSuffixPartial) suffix like any other chunk.
//
// The sole-piece collapse used to be applied here, and it made the coarse
// publish path unreadable.  swissknife_ingestsql.cc:1433 calls
// set_is_chunked_file(true) for EVERY file it ingests, and CVMFS reads chunk
// hashes back with shash::kSuffixPartial (catalog_sql.cc:688).  A client
// therefore requests <hash>P for a sole piece too, while the collapse had
// stored it under the bare <hash> — so every file below the chunk grid (i.e.
// nearly all of them) returned EIO, "failed to fetch chunk".
//
// Emitting one chunk instead keeps a single representation across both publish
// paths: the upload key, the descriptor's hashes column and the catalog's
// chunks table all agree.  result.Hash stays the CVMFS bulk hash (SHA-1 of the
// full uncompressed content) for the catalog's own hash column.
func compressEntryCDC(entry unpack.FileEntry, det *chunker.Xor32, level int) (Result, error) {
	result := Result{FileEntry: entry}

	if !entry.Mode.IsRegular() {
		result.Hash = "0000000000000000000000000000000000000000"
		return result, nil
	}

	data, derr := entry.Bytes()
	if derr != nil {
		return Result{}, derr
	}
	cuts := det.Cuts(data)

	// bounds always spans the whole file, so len(cuts)==0 yields exactly one
	// chunk [0,len(data)).  An empty file yields one zero-length chunk, which
	// is what ingestsql expects: it forces expected_num_chunks to 1 when
	// size==0 (swissknife_ingestsql.cc:1360).
	bounds := make([]int64, 0, len(cuts)+2)
	bounds = append(bounds, 0)
	bounds = append(bounds, cuts...)
	bounds = append(bounds, int64(len(data)))

	bulkH := sha1Pool.Get().(hash.Hash) //nolint:gosec
	bulkH.Reset()
	bulkH.Write(data)
	bulkHash := hex.EncodeToString(bulkH.Sum(nil))
	sha1Pool.Put(bulkH)

	effectiveLevel := zlibLevel(level)
	pool := getZlibWriterPool(effectiveLevel)
	w := pool.Get().(*zlib.Writer)
	defer pool.Put(w)

	var compBuf bytes.Buffer
	chunks := make([]Chunk, 0, len(bounds)-1)
	for i := 0; i+1 < len(bounds); i++ {
		start, end := bounds[i], bounds[i+1]
		chunkData := data[start:end]

		h := sha1Pool.Get().(hash.Hash) //nolint:gosec
		h.Reset()
		compBuf.Reset()
		w.Reset(io.MultiWriter(&compBuf, h))
		if _, err := w.Write(chunkData); err != nil {
			sha1Pool.Put(h)
			return result, fmt.Errorf("zlib write for chunk at offset %d: %w", start, err)
		}
		if err := w.Close(); err != nil {
			sha1Pool.Put(h)
			return result, fmt.Errorf("zlib close for chunk at offset %d: %w", start, err)
		}
		compressedSize := int64(compBuf.Len())
		var compressed []byte
		if len(bounds) == 2 {
			// Sole piece: compBuf is function-local and not reused across
			// iterations, so hand the buffer off directly. This is now the
			// common case (every file below the grid), and the make+copy below
			// would be pure overhead on the hot path the sha1/zlib pools exist
			// to keep allocation-free.
			compressed = compBuf.Bytes()
		} else {
			compressed = make([]byte, compressedSize)
			copy(compressed, compBuf.Bytes())
		}
		chunkHash := hex.EncodeToString(h.Sum(nil))
		sha1Pool.Put(h)

		chunks = append(chunks, Chunk{
			Offset:           start,
			UncompressedSize: end - start,
			Hash:             chunkHash,
			Compressed:       compressed,
			CompressedSize:   compressedSize,
		})
		// Accumulate so Result.CompressedSize is the total across chunks.
		// Without this the PipelineBytesCompressed metric only ever added 0
		// once every file became chunked.
		result.CompressedSize += compressedSize
	}

	result.Hash = bulkHash
	result.Chunks = chunks
	return result, nil
}
