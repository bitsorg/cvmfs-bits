// Package compress provides parallel compression and hashing of tar file entries.
package compress

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha256"
	"fmt"
	"runtime"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"cvmfs.io/prepub/internal/pipeline/unpack"
	"cvmfs.io/prepub/pkg/cvmfshash"
	"cvmfs.io/prepub/pkg/observe"
)

// Config holds configuration for the compress stage.
type Config struct {
	Workers   int   // number of concurrent workers
	ChunkSize int64 // 0 = no chunking; files with len(Data) > ChunkSize are split
}

// Chunk represents a single compressed chunk of a larger file.
type Chunk struct {
	Offset           int64  // byte offset in the uncompressed file
	UncompressedSize int64  // size of this chunk's uncompressed data
	Hash             string // hex SHA-256 of uncompressed chunk bytes (= CAS key)
	Compressed       []byte // zlib-compressed chunk data
	CompressedSize   int64  // size of Compressed in bytes
}

// Result carries a processed file entry alongside its compressed form and hash.
type Result struct {
	// FileEntry is the original unpacked entry.
	FileEntry unpack.FileEntry
	// Hash is the SHA256 hash of the uncompressed content (or bulk hash for chunked files).
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

	for entry := range in {
		entry := entry // capture for closure
		if err := sem.Acquire(egCtx, 1); err != nil {
			span.RecordError(err)
			return fmt.Errorf("compress semaphore: %w", err)
		}

		eg.Go(func() error {
			defer sem.Release(1)

			_, wspan := obs.Tracer.Start(egCtx, "compress.file")
			defer wspan.End()

			result, err := compressEntry(entry, cfg.ChunkSize)
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

	// Wait for all in-flight workers to finish before returning.
	if err := eg.Wait(); err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

func compressEntry(entry unpack.FileEntry, chunkSize int64) (Result, error) {
	result := Result{FileEntry: entry}

	// Directories and symlinks get a zero-hash sentinel — no content to store.
	if !entry.Mode.IsRegular() {
		result.Hash = "0000000000000000000000000000000000000000000000000000000000000000"
		return result, nil
	}

	// Hash original content.
	hash, _, err := cvmfshash.HashReader(bytes.NewReader(entry.Data))
	if err != nil {
		return result, fmt.Errorf("hashing: %w", err)
	}
	result.Hash = hash

	// Check if we should chunk this file
	if chunkSize > 0 && int64(len(entry.Data)) > chunkSize {
		return compressEntryChunked(entry, chunkSize, hash)
	}

	// Compress with zlib. NewWriterLevel only errors on invalid level
	// constants, but we check it anyway for correctness.
	var compBuf bytes.Buffer
	w, err := zlib.NewWriterLevel(&compBuf, zlib.BestCompression)
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
	return result, nil
}

func compressEntryChunked(entry unpack.FileEntry, chunkSize int64, bulkHash string) (Result, error) {
	// Fix C4: guard against a non-positive chunkSize reaching this function.
	// compressEntry already checks this via the caller, but a defensive check
	// here prevents subtle bugs if compressEntryChunked is ever called directly.
	if chunkSize <= 0 {
		return Result{}, fmt.Errorf("compressEntryChunked: chunkSize must be positive, got %d", chunkSize)
	}

	result := Result{FileEntry: entry, Hash: bulkHash}

	data := entry.Data
	var chunks []Chunk
	offset := int64(0)

	// Fix L2: allocate the compression buffer once and reset it between chunks
	// rather than creating a new bytes.Buffer on every iteration.  For files
	// split into thousands of chunks this materially reduces GC pressure.
	var compBuf bytes.Buffer

	// Split data into chunks and compress each independently.
	for offset < int64(len(data)) {
		chunkEnd := offset + chunkSize
		if chunkEnd > int64(len(data)) {
			chunkEnd = int64(len(data))
		}

		chunkData := data[offset:chunkEnd]
		uncompressedSize := int64(len(chunkData))

		// Hash this chunk's uncompressed data.
		chunkHasher := sha256.New()
		chunkHasher.Write(chunkData)
		chunkHashHex := fmt.Sprintf("%064x", chunkHasher.Sum(nil))

		// Compress this chunk, reusing the buffer from the previous iteration.
		compBuf.Reset()
		w, err := zlib.NewWriterLevel(&compBuf, zlib.BestCompression)
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

		chunks = append(chunks, Chunk{
			Offset:           offset,
			UncompressedSize: uncompressedSize,
			Hash:             chunkHashHex,
			Compressed:       compressed,
			CompressedSize:   compressedSize,
		})

		offset = chunkEnd
	}

	result.Chunks = chunks
	// For chunked files, Compressed is nil and we don't set CompressedSize
	// (each chunk has its own compressed size)
	return result, nil
}
