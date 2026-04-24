// Package compress provides parallel compression and hashing of tar file entries.
package compress

import (
	"bytes"
	"compress/zlib"
	"context"
	"fmt"
	"runtime"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"cvmfs.io/prepub/internal/pipeline/unpack"
	"cvmfs.io/prepub/pkg/cvmfshash"
	"cvmfs.io/prepub/pkg/observe"
)

// Result carries a processed file entry alongside its compressed form and hash.
type Result struct {
	// FileEntry is the original unpacked entry.
	FileEntry unpack.FileEntry
	// Hash is the SHA256 hash of the uncompressed content.
	Hash string
	// Compressed is the zlib-compressed content bytes.
	Compressed []byte
	// CompressedSize is the size of Compressed in bytes.
	CompressedSize int64
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
func Run(ctx context.Context, in <-chan unpack.FileEntry, out chan<- Result, workers int, obs *observe.Provider) error {
	ctx, span := obs.Tracer.Start(ctx, "pipeline.compress")
	defer span.End()

	// Fix #16: Clamp workers to a safe range so a bad config value cannot
	// create an unbounded goroutine explosion.
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

			result, err := compressEntry(entry)
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

func compressEntry(entry unpack.FileEntry) (Result, error) {
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
