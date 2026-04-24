// Package pipeline orchestrates the tar-to-catalog transformation:
// unpacking tar entries, compressing and hashing content, deduplicating against
// prior uploads, building a deduplicated catalog, and uploading objects to CAS.
package pipeline

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sync/errgroup"

	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/internal/pipeline/catalog"
	"cvmfs.io/prepub/internal/pipeline/compress"
	"cvmfs.io/prepub/internal/pipeline/dedup"
	"cvmfs.io/prepub/internal/pipeline/unpack"
	"cvmfs.io/prepub/internal/pipeline/upload"
	"cvmfs.io/prepub/pkg/observe"
)

// Config holds configuration for the pipeline execution.
type Config struct {
	// Workers is the number of concurrent compress workers.
	Workers int
	// UploadConc is the max concurrent uploads to CAS (unused, reserved for future).
	UploadConc int
	// CAS is the content-addressable storage backend.
	CAS cas.Backend
	// SpoolDir is the temporary directory for catalog.db and upload.log.
	SpoolDir string
	// Obs provides logging, tracing, and metrics.
	Obs *observe.Provider
	// SharedFilter enables cross-node Bloom filter snapshot sharing (Option B).
	// Off by default.  See dedup.SharedFilterConfig for details.
	SharedFilter dedup.SharedFilterConfig
}

// Result is returned after a successful pipeline run.
type Result struct {
	// CatalogHash is the SHA256 hash of the deduplicated CVMFS catalog.
	CatalogHash string
	// ObjectHashes are the SHA256 hashes of all objects (files and catalog) pushed to CAS.
	ObjectHashes []string
	// NFiles is the total number of files processed from the tar.
	NFiles int
	// NBytesRaw is the sum of uncompressed content bytes.
	NBytesRaw int64
	// NBytesComp is the sum of compressed content bytes (dedup-hit objects are not counted).
	NBytesComp int64
}

// Run processes the tar file at tarPath through the full pipeline.
// It is a thin wrapper around RunFromReader that opens the file and delegates.
func Run(ctx context.Context, tarPath string, cfg Config) (*Result, error) {
	f, err := os.Open(tarPath)
	if err != nil {
		return nil, fmt.Errorf("opening tar %q: %w", tarPath, err)
	}
	defer f.Close()
	return RunFromReader(ctx, f, cfg)
}

// RunFromReader processes a tar stream through the full pipeline and returns
// the catalog hash, object hashes, and byte counts. This allows streaming tar
// processing without requiring the complete tar to be saved to disk first.
//
// Pipeline stages:
//   1. Unpack: Extract tar entries into FileEntry structs
//   2. Fan-out: Route each FileEntry to both compress and catalog workers
//   3. Compress: Compress and hash each file, updating the Bloom filter for dedup
//   4. Catalog: Build a deduplicated SQLite catalog from all entries
//   5. Dedup+Upload: Check the Bloom filter for each object, confirm against CAS,
//      and upload new objects. Dedup-hit objects are included in the result but
//      not re-uploaded.
//
// Both compress and catalog stages receive every entry concurrently, allowing
// compression and catalog construction to overlap without blocking each other.
//
// The result includes all objects: newly compressed, dedup-hits, and the final
// catalog hash. These are submitted to the gateway via the lease.
func RunFromReader(ctx context.Context, r io.Reader, cfg Config) (*Result, error) {
	ctx, span := cfg.Obs.Tracer.Start(ctx, "pipeline.run")
	defer span.End()

	cfg.Obs.Logger.InfoContext(ctx, "pipeline starting", "workers", cfg.Workers)

	// Channels between stages.
	unpackChan := make(chan unpack.FileEntry, 64)
	compressChan := make(chan unpack.FileEntry, 64)  // fed by fan-out
	catalogChan := make(chan unpack.FileEntry, 64)   // fed by fan-out
	compressOut := make(chan compress.Result, 64)

	eg, egCtx := errgroup.WithContext(ctx)

	// Stage 1: Unpack tar → unpackChan.
	eg.Go(func() error {
		defer close(unpackChan)
		if err := unpack.Extract(egCtx, r, unpackChan); err != nil {
			return fmt.Errorf("unpack: %w", err)
		}
		return nil
	})

	// Stage 2: Fan-out — send each FileEntry to both compress and catalog.
	//
	// Fix #14: If the first send (compress) succeeds but the second send
	// (catalog) is blocked when the context is cancelled, we return an explicit
	// error rather than silently dropping the entry.  This means the catalog
	// and CAS can never silently diverge — the job fails and must be retried.
	eg.Go(func() error {
		defer close(compressChan)
		defer close(catalogChan)
		for entry := range unpackChan {
			select {
			case compressChan <- entry:
			case <-egCtx.Done():
				return fmt.Errorf("fan-out: compress channel blocked on %q: %w", entry.Path, egCtx.Err())
			}
			select {
			case catalogChan <- entry:
			case <-egCtx.Done():
				// The entry was already sent to compress; returning an error here
				// cancels the errgroup so the compress stage also terminates.
				return fmt.Errorf("fan-out: catalog channel blocked on %q after compress send: %w", entry.Path, egCtx.Err())
			}
		}
		return nil
	})

	// Stage 3a: Compress + hash worker pool → compressOut.
	eg.Go(func() error {
		defer close(compressOut)
		return compress.Run(egCtx, compressChan, compressOut, cfg.Workers, cfg.Obs)
	})

	// Stage 3b: Build catalog from catalogChan.
	catalogPath := filepath.Join(cfg.SpoolDir, "catalog.db")
	builder, err := catalog.New(catalogPath, cfg.Obs)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("creating catalog builder: %w", err)
	}

	eg.Go(func() error {
		_, cspan := cfg.Obs.Tracer.Start(egCtx, "pipeline.catalog_build")
		defer cspan.End()
		for entry := range catalogChan {
			if err := builder.Add(egCtx, entry, ""); err != nil {
				cspan.RecordError(err)
				return fmt.Errorf("catalog add %s: %w", entry.Path, err)
			}
		}
		return nil
	})

	// Stage 4: Dedup + upload from compressOut.
	// Use NewWithConfig so the filter parameters honour SharedFilter settings —
	// all nodes must use the same capacity/FPRate for snapshots to be mergeable.
	dedupChecker, err := dedup.NewWithConfig(ctx, cfg.CAS, cfg.SharedFilter, cfg.Obs)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("creating dedup checker: %w", err)
	}

	// Shared filter (Option B): merge peer snapshots before processing so the
	// current job benefits from objects already pushed by sibling build nodes.
	if cfg.SharedFilter.Enabled {
		if mergeErr := dedupChecker.LoadPeerSnapshots(cfg.SharedFilter, cfg.Obs); mergeErr != nil {
			cfg.Obs.Logger.Warn("shared bloom: peer snapshot merge failed (continuing with local filter)",
				"error", mergeErr)
		}
	}

	uploadLogPath := filepath.Join(cfg.SpoolDir, "upload.log")
	uploadLog := upload.OpenUploadLog(uploadLogPath)

	var result Result
	var resultMu sync.Mutex
	seenHashes := make(map[string]bool)

	eg.Go(func() error {
		_, uspan := cfg.Obs.Tracer.Start(egCtx, "pipeline.upload")
		defer uspan.End()

		for compResult := range compressOut {
			resultMu.Lock()
			result.NFiles++
			result.NBytesRaw += compResult.FileEntry.Size
			resultMu.Unlock()

			hash := compResult.Hash
			if hash == "" || !compResult.FileEntry.Mode.IsRegular() {
				continue
			}

			isDup, err := dedupChecker.Check(egCtx, hash)
			if err != nil {
				uspan.RecordError(err)
				return fmt.Errorf("dedup check %s: %w", hash, err)
			}

			resultMu.Lock()
			alreadySeen := seenHashes[hash]
			if !isDup && !alreadySeen {
				seenHashes[hash] = true
			}
			resultMu.Unlock()

			if isDup || alreadySeen {
				// dedup.Check() already increments PipelineDedupHits for CAS-confirmed
				// hits (isDup=true).  Only increment here for the intra-job case
				// (alreadySeen=true, isDup=false) to avoid double-counting.
				if alreadySeen && !isDup {
					cfg.Obs.Metrics.PipelineDedupHits.Inc()
				}
				// Dedup-hit objects are already in CAS but the gateway still needs
				// them listed in SubmitPayload so it can build a complete catalog.
				resultMu.Lock()
				result.ObjectHashes = append(result.ObjectHashes, hash)
				resultMu.Unlock()
				continue
			}

			// Upload to CAS with per-object retry for transient errors.
			if err := upload.PutWithRetry(egCtx, cfg.CAS, hash, compResult.Compressed, compResult.CompressedSize); err != nil {
				uspan.RecordError(err)
				return fmt.Errorf("cas put %s: %w", hash, err)
			}
			// Fix #8: update the Bloom filter so subsequent jobs (and other
			// concurrent upload workers) can dedup against this new object
			// without a CAS round-trip.
			dedupChecker.Add(hash)
			if err := uploadLog.Record(hash); err != nil {
				uspan.RecordError(err)
				return fmt.Errorf("recording upload %s: %w", hash, err)
			}

			resultMu.Lock()
			result.NBytesComp += compResult.CompressedSize
			result.ObjectHashes = append(result.ObjectHashes, hash)
			resultMu.Unlock()
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("pipeline: %w", err)
	}

	// Finalize catalog: compress + hash the SQLite file, upload it.
	compPath, catalogHash, err := builder.Finalize(ctx)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("finalizing catalog: %w", err)
	}

	if compPath != "" {
		cf, err := os.Open(compPath)
		if err != nil {
			span.RecordError(err)
			return nil, fmt.Errorf("opening compressed catalog: %w", err)
		}
		fi, err := cf.Stat()
		if err != nil {
			cf.Close()
			span.RecordError(err)
			return nil, fmt.Errorf("stat compressed catalog: %w", err)
		}
		putErr := cfg.CAS.Put(ctx, catalogHash, cf, fi.Size())
		// Fix #21: always check cf.Close() — a flush error on the read side is
		// rare but indicates the OS released the file before CAS finished reading.
		if closeErr := cf.Close(); closeErr != nil && putErr == nil {
			putErr = fmt.Errorf("closing catalog file after upload: %w", closeErr)
		}
		if putErr != nil {
			span.RecordError(putErr)
			return nil, fmt.Errorf("uploading catalog: %w", putErr)
		}

		// Fix #15: Record the catalog object in the upload log so that crash
		// recovery knows the catalog was successfully written to CAS.
		if err := uploadLog.Record(catalogHash); err != nil {
			span.RecordError(err)
			return nil, fmt.Errorf("recording catalog upload: %w", err)
		}
	}

	result.CatalogHash = catalogHash
	result.ObjectHashes = append(result.ObjectHashes, catalogHash)

	// Shared filter (Option B): persist the updated filter so peer nodes can
	// merge it on their next job start.  Non-fatal — a failed save only means
	// peers won't benefit from this run's objects until the next successful save.
	if cfg.SharedFilter.Enabled {
		if saveErr := dedupChecker.SaveSnapshot(cfg.SharedFilter, cfg.Obs); saveErr != nil {
			cfg.Obs.Logger.Warn("shared bloom: snapshot save failed (continuing)",
				"error", saveErr)
		}
	}

	cfg.Obs.Logger.InfoContext(ctx, "pipeline complete",
		"files", result.NFiles,
		"objects", len(result.ObjectHashes),
		"bytes_raw", result.NBytesRaw,
		"bytes_comp", result.NBytesComp,
		"catalog_hash", catalogHash,
	)

	return &result, nil
}
