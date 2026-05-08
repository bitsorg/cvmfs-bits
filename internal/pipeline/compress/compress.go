// Package compress provides parallel compression and hashing of tar file entries.
package compress

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1" //nolint:gosec // CVMFS CAS key = SHA-1(zlib(content)); see hash.go
	"encoding/hex"
	"fmt"
	"io"
	"runtime"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"cvmfs.io/prepub/internal/pipeline/unpack"
	"cvmfs.io/prepub/pkg/observe"
)

// Config holds configuration for the compress stage.
type Config struct {
	Workers   int   // number of concurrent workers
	ChunkSize int64 // 0 = no chunking; files with len(Data) > ChunkSize are split
	// CompressLevel is the zlib compression level (1=fastest … 9=best).
	// 0 is treated as zlib.DefaultCompression (-1 = level 6).
	// Use zlib.BestSpeed (1) to roughly halve CPU time for CPU-bound publishes
	// at the cost of slightly larger objects.
	CompressLevel int
}

// Chunk represents a single compressed chunk of a larger file.
type Chunk struct {
	Offset           int64  // byte offset in the uncompressed file
	UncompressedSize int64  // size of this chunk's uncompressed data
	Hash             string // hex SHA-1 of compressed chunk bytes (= CAS key)
	Compressed       []byte // zlib-compressed chunk data
	CompressedSize   int64  // size of Compressed in bytes
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

			result, err := compressEntry(entry, cfg.ChunkSize, cfg.CompressLevel)
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
	if chunkSize > 0 && int64(len(entry.Data)) > chunkSize {
		return compressEntryChunked(entry, chunkSize, level)
	}

	// Single-pass compress + hash.
	//
	// The CAS key is SHA-1(zlib(content)) (CVMFS convention; see hash.go).
	// Previously we compressed into a bytes.Buffer, then made a second pass
	// over the result bytes to compute SHA-1 — two full memory reads of the
	// compressed data.  Using io.MultiWriter the zlib output flows into both
	// the accumulation buffer and the SHA-1 hasher simultaneously, halving
	// the memory traffic for this stage.
	//
	// NewWriterLevel only errors on invalid level constants; we check anyway.
	h := sha1.New() //nolint:gosec
	var compBuf bytes.Buffer
	w, err := zlib.NewWriterLevel(io.MultiWriter(&compBuf, h), zlibLevel(level))
	if err != nil {
		return result, fmt.Errorf("zlib init: %w", err)
	}
	if _, err := w.Write(entry.Data); err != nil {
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

	data := entry.Data
	var chunks []Chunk
	offset := int64(0)

	// Fix L2: allocate the compression buffer once and reset it between chunks
	// rather than creating a new bytes.Buffer on every iteration.  For files
	// split into thousands of chunks this materially reduces GC pressure.
	var compBuf bytes.Buffer

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
		// compressEntry).  A fresh sha1.Hash is allocated per chunk because each
		// chunk is an independent CAS object with its own key.
		h := sha1.New() //nolint:gosec
		compBuf.Reset()
		w, err := zlib.NewWriterLevel(io.MultiWriter(&compBuf, h), zlibLevel(level))
		if err != nil {
			return result, fmt.Errorf("zlib init for chunk at offset %d: %w", offset, err)
		}
		if _, err := w.Write(chunkData); err != nil {
			return result, fmt.Errorf("zlib write for chunk at offset %d: %w", offset, err)
		}
		if err := w.Close(); err != nil {
			return result, fmt.Errorf("zlib close for chunk at offset %d: %w", offset, err)
		}

		// Fix L1: use compBuf.Len() instead of len(compBuf.Bytes()).
		// compBuf.Len() reads the length from an internal int field; Bytes()
		// returns a slice header (no allocation, but an extra method call).
		compressedSize := int64(compBuf.Len())
		compressed := make([]byte, compressedSize)
		copy(compressed, compBuf.Bytes())
		chunkHash := hex.EncodeToString(h.Sum(nil))

		chunks = append(chunks, Chunk{
			Offset:           offset,
			UncompressedSize: uncompressedSize,
			Hash:             chunkHash,
			Compressed:       compressed,
			CompressedSize:   compressedSize,
		})

		offset = chunkEnd
	}

	// For chunked files, Hash holds the SHA-1 of the FIRST chunk's compressed
	// bytes as the "bulk hash" (used for catalog entry and dedup key of the
	// overall file object).  The first chunk's hash is a stable, deterministic
	// representative that the CVMFS catalog stores for chunked files.
	// If there are no chunks (empty file that somehow reached here), use the
	// 40-zero sentinel.
	if len(chunks) > 0 {
		result.Hash = chunks[0].Hash
	} else {
		result.Hash = "0000000000000000000000000000000000000000"
	}

	result.Chunks = chunks
	// For chunked files, Compressed is nil and we don't set CompressedSize
	// (each chunk has its own compressed size)
	return result, nil
}
