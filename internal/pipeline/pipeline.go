// Package pipeline orchestrates the tar-to-catalog transformation:
// unpacking tar entries, compressing and hashing content, deduplicating against
// prior uploads, building a deduplicated catalog, and uploading objects to CAS.
package pipeline

import (
	"context"
	"encoding/hex"
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
	"cvmfs.io/prepub/pkg/cvmfscatalog"
	"cvmfs.io/prepub/pkg/observe"
)

// Config holds configuration for the pipeline execution.
type Config struct {
	// Workers is the number of concurrent compress workers.
	Workers int
	// ChunkSize is the max size of a file before chunking (0 = no chunking).
	ChunkSize int64
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
	// CatalogEntries are the CVMFS catalog entries collected from the tar.
	// These are passed to cvmfscatalog.Merge() after the gateway lease is acquired.
	// The catalog is NOT finalised here — merging with the existing repository
	// catalog happens in the orchestrator after lease acquisition.
	CatalogEntries []cvmfscatalog.Entry
	// ObjectHashes are the SHA256 hashes of all CAS objects (file chunks only,
	// NOT including catalog — the catalog hashes come from cvmfscatalog.Merge).
	ObjectHashes []string
	// DirtabContent is the raw content of the .cvmfsdirtab file if one was
	// present in the tar payload.  It is passed to cvmfscatalog.MergeConfig so
	// that catalog-split rules from the new publish take effect immediately.
	// Nil when no .cvmfsdirtab was found in the tar.
	DirtabContent []byte
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
	//
	// Fix L5: detect duplicate paths in the tar.  The tar format allows
	// multiple entries with the same path (later entries typically override
	// earlier ones), but CVMFS requires each path to appear exactly once in a
	// catalog.  Without detection, resultsByPath silently retains only the last
	// hash while the catalog may accumulate duplicate rows.  We fail fast
	// instead of producing a silently inconsistent publish.
	//
	// .cvmfsdirtab capture: when a .cvmfsdirtab file is encountered its raw
	// content is saved so cvmfscatalog.Merge can apply the split rules without
	// a second round-trip to the stratum0 CAS.  The capture runs inside the
	// fan-out goroutine (serial), so no additional locking is needed; the
	// result is read only after eg.Wait() (happens-before guarantee).
	var capturedDirtab []byte
	eg.Go(func() error {
		defer close(compressChan)
		defer close(catalogChan)
		seenPaths := make(map[string]struct{})
		for entry := range unpackChan {
			if _, dup := seenPaths[entry.Path]; dup {
				return fmt.Errorf("fan-out: duplicate path %q in tar — each path must appear exactly once", entry.Path)
			}
			seenPaths[entry.Path] = struct{}{}

			// Capture .cvmfsdirtab content (last one wins on duplicate paths,
			// but duplicates are rejected above so there is at most one).
			if filepath.Base(entry.Path) == ".cvmfsdirtab" && entry.Mode.IsRegular() && len(entry.Data) > 0 {
				capturedDirtab = make([]byte, len(entry.Data))
				copy(capturedDirtab, entry.Data)
			}

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
		return compress.Run(egCtx, compressChan, compressOut, compress.Config{
			Workers:   cfg.Workers,
			ChunkSize: cfg.ChunkSize,
		}, cfg.Obs)
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

	// Fix H2: store only the hash strings we need for catalog patching, not the
	// full compress.Result (which holds the compressed byte slices that are
	// already in CAS and should be GC'd after upload).
	type chunkMeta struct {
		offset           int64
		uncompressedSize int64
		hash             string
	}
	type fileMeta struct {
		bulkHash string
		chunks   []chunkMeta // nil for non-chunked files
	}

	var result Result
	var resultMu sync.Mutex
	seenHashes := make(map[string]bool)
	resultsByPath := make(map[string]fileMeta)

	eg.Go(func() error {
		_, uspan := cfg.Obs.Tracer.Start(egCtx, "pipeline.upload")
		defer uspan.End()

		for compResult := range compressOut {
			// Fix N7: build the chunk-meta slice before acquiring the mutex so
			// the allocation and copy work happens outside the critical section,
			// keeping lock hold-time as short as possible.
			var fm fileMeta
			if compResult.FileEntry.Mode.IsRegular() {
				if len(compResult.Chunks) > 0 {
					chunks := make([]chunkMeta, len(compResult.Chunks))
					for i, ch := range compResult.Chunks {
						chunks[i] = chunkMeta{
							offset:           ch.Offset,
							uncompressedSize: ch.UncompressedSize,
							hash:             ch.Hash,
						}
					}
					fm = fileMeta{bulkHash: compResult.Hash, chunks: chunks}
				} else {
					fm = fileMeta{bulkHash: compResult.Hash}
				}
			}

			// Fix H3: merge all counter updates into a single lock acquisition
			// to avoid two separate round-trips to the mutex.
			resultMu.Lock()
			result.NFiles++
			result.NBytesRaw += compResult.FileEntry.Size
			// Fix H2: store only lightweight hash metadata, not compressed bytes.
			if compResult.FileEntry.Mode.IsRegular() {
				resultsByPath[compResult.FileEntry.Path] = fm
			}
			resultMu.Unlock()

			// Handle non-regular files
			if !compResult.FileEntry.Mode.IsRegular() {
				continue
			}

			// Handle chunked files: upload each chunk independently
			if len(compResult.Chunks) > 0 {
				for _, chunk := range compResult.Chunks {
					isDup, err := dedupChecker.Check(egCtx, chunk.Hash)
					if err != nil {
						uspan.RecordError(err)
						return fmt.Errorf("dedup check chunk %s: %w", chunk.Hash, err)
					}

					resultMu.Lock()
					alreadySeen := seenHashes[chunk.Hash]
					if !isDup && !alreadySeen {
						seenHashes[chunk.Hash] = true
					}
					resultMu.Unlock()

					if isDup || alreadySeen {
						if alreadySeen && !isDup {
							cfg.Obs.Metrics.PipelineDedupHits.Inc()
						}
						resultMu.Lock()
						result.ObjectHashes = append(result.ObjectHashes, chunk.Hash)
						resultMu.Unlock()
						continue
					}

					// Upload chunk to CAS
					if err := upload.PutWithRetry(egCtx, cfg.CAS, chunk.Hash, chunk.Compressed, chunk.CompressedSize); err != nil {
						uspan.RecordError(err)
						return fmt.Errorf("cas put chunk %s: %w", chunk.Hash, err)
					}
					dedupChecker.Add(chunk.Hash)
					if err := uploadLog.Record(chunk.Hash); err != nil {
						uspan.RecordError(err)
						return fmt.Errorf("recording upload %s: %w", chunk.Hash, err)
					}

					resultMu.Lock()
					result.NBytesComp += chunk.CompressedSize
					result.ObjectHashes = append(result.ObjectHashes, chunk.Hash)
					resultMu.Unlock()
				}
				continue
			}

			// Handle non-chunked files (original logic)
			hash := compResult.Hash
			if hash == "" {
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

	// Propagate the captured .cvmfsdirtab content (safe to read after eg.Wait).
	result.DirtabContent = capturedDirtab

	// Patch catalog entries with hashes and chunks from compress results.
	// Fix C2: hex decode errors are now propagated rather than silently ignored.
	rawEntries := builder.Entries()
	result.CatalogEntries = make([]cvmfscatalog.Entry, len(rawEntries))
	for i, e := range rawEntries {
		result.CatalogEntries[i] = *e

		// Skip non-regular files
		if !e.Mode.IsRegular() {
			continue
		}

		// Look up the lightweight metadata for this entry.
		fm, ok := resultsByPath[e.FullPath]
		if !ok || fm.bulkHash == "" {
			continue
		}

		hashBytes, err := hex.DecodeString(fm.bulkHash)
		if err != nil {
			return nil, fmt.Errorf("decoding hash for %s: %w", e.FullPath, err)
		}
		result.CatalogEntries[i].Hash = hashBytes
		result.CatalogEntries[i].HashAlgo = cvmfscatalog.HashSha256
		result.CatalogEntries[i].CompAlgo = cvmfscatalog.CompZlib

		// For chunked files: populate chunk records.
		if len(fm.chunks) > 0 {
			chunks := make([]cvmfscatalog.ChunkRecord, len(fm.chunks))
			for j, ch := range fm.chunks {
				// Fix C2: propagate decode error instead of silently using nil bytes.
				chBytes, decErr := hex.DecodeString(ch.hash)
				if decErr != nil {
					return nil, fmt.Errorf("decoding chunk hash for %s at offset %d: %w",
						e.FullPath, ch.offset, decErr)
				}
				chunks[j] = cvmfscatalog.ChunkRecord{
					Offset: ch.offset,
					Size:   ch.uncompressedSize,
					Hash:   chBytes,
				}
			}
			result.CatalogEntries[i].Chunks = chunks
		}

		// Inject synthetic xattrs now that Hash, CompAlgo, and Chunks are
		// populated.  SyntheticAttrs generates user.cvmfs.hash,
		// user.cvmfs.compression, and (for chunked files) user.cvmfs.chunk_list.
		// These are merged on top of any user xattrs already present in the
		// entry so that synthetic keys always reflect the actual pipeline output.
		synthetic := cvmfscatalog.SyntheticAttrs(&result.CatalogEntries[i])
		if len(synthetic) > 0 {
			if result.CatalogEntries[i].Xattr == nil {
				result.CatalogEntries[i].Xattr = synthetic
			} else {
				// cvmfsxattr.Merge writes src keys into dst, with src winning.
				for k, v := range synthetic {
					result.CatalogEntries[i].Xattr[k] = v
				}
			}
		}
	}

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
		"catalog_entries", len(result.CatalogEntries),
		"bytes_raw", result.NBytesRaw,
		"bytes_comp", result.NBytesComp,
	)

	return &result, nil
}
