// Package pipeline orchestrates the tar-to-catalog transformation:
// unpacking tar entries, compressing and hashing content, deduplicating against
// prior uploads, building a deduplicated catalog, and uploading objects to CAS.
package pipeline

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

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
	// UploadConc is the number of concurrent dedup+upload workers per pipeline
	// run.  Values ≤ 0 default to 1 (sequential) for backward compatibility.
	// Setting this to 4–8 dramatically reduces staging latency for jobs with
	// many new objects, since each CAS write (write+fsync+rename) can overlap
	// with other writes rather than serialising on a single goroutine.
	UploadConc int
	// CompressLevel is the zlib compression level (1=fastest … 9=best, 0=default).
	// Defaults to zlib.DefaultCompression (-1) when zero.  Setting this to
	// compress/zlib.BestSpeed (1) roughly halves CPU time for compression-bound
	// publishes with only a modest size increase.
	CompressLevel int
	// CAS is the content-addressable storage backend.
	CAS cas.Backend
	// SpoolDir is the temporary directory for catalog.db and upload.log.
	SpoolDir string
	// Obs provides logging, tracing, and metrics.
	Obs *observe.Provider
	// SharedFilter enables cross-node Bloom filter snapshot sharing (Option B).
	// Off by default.  See dedup.SharedFilterConfig for details.
	SharedFilter dedup.SharedFilterConfig
	// DedupChecker is the optional Bloom-filter dedup checker.  When non-nil
	// the pipeline uses a two-step dedup path: fast in-memory Bloom filter
	// negative test followed by a CAS.Exists confirmation for positives.
	//
	// Enable this for high-latency CAS backends (e.g. remote S3 or a network
	// mount) where per-object CAS.Exists calls are expensive enough to make a
	// pre-seeded in-memory index worthwhile.  Pass a shared *dedup.Checker
	// created once at service startup so all concurrent jobs share the same
	// filter state.  Enable via --bloom-filter or --bloom-snapshot-dir.
	//
	// When nil (the default) each pipeline run calls cfg.CAS.Exists directly
	// before every Put — a single stat/HEAD per object.  For local-disk CAS
	// this is fast and requires no startup walk, no memory overhead, and has
	// no risk of filter saturation.  The caller owns the checker's lifecycle.
	DedupChecker *dedup.Checker
}

// Result is returned after a successful pipeline run.
type Result struct {
	// CatalogEntries are the CVMFS catalog entries collected from the tar.
	// These are passed to cvmfscatalog.BuildSubtree() in the orchestrator after
	// the gateway lease is acquired.  The catalog is NOT finalised here — the
	// subtree build happens in the orchestrator after lease acquisition.
	CatalogEntries []cvmfscatalog.Entry
	// ObjectHashes are the SHA-1 hashes (of zlib-compressed content) for ALL
	// CAS objects in this publish (file chunks only, NOT including catalog — the
	// catalog hashes come from cvmfscatalog.BuildSubtree).  This includes both newly
	// uploaded objects and objects that were already in the CAS (dedup hits).
	//
	// This is the complete set of objects that Stratum 1 replicas need to have.
	// Pass to DistManager.Enqueue so S1 workers can push the full object set.
	//
	// For the gateway's SubmitPayload use NewObjectHashes instead — that subset
	// excludes objects already present in the gateway CAS and therefore keeps
	// the leased-state window proportional to the size of the current change
	// rather than the total repository size.
	ObjectHashes []string
	// NewObjectHashes are the SHA-1 hashes of CAS objects that were newly
	// uploaded in this pipeline run (not found in the local CAS at pipeline
	// start, i.e. not dedup hits).
	//
	// Pass this to SubmitPayload so only genuinely new objects are transmitted
	// to the gateway during the lease window.  Dedup-hit objects were already
	// sent to the gateway in a previous commit and are still present there.
	NewObjectHashes []string
	// DirtabContent is the raw content of the .cvmfsdirtab file if one was
	// present in the tar payload.  It is passed to cvmfscatalog.SubtreeConfig so
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

// PrefetchResult holds the sorted, in-memory tar entries produced by Phase 0
// (collect + validate + sort).  Pass to RunFromPrefetch to skip the blocking
// tar scan when the concurrency slot is acquired — the data is already resident
// in memory from a background goroutine that ran before the job waited.
type PrefetchResult struct {
	SortedEntries []unpack.FileEntry
	DirtabContent []byte
}

// Prefetch performs Phase 0 only (collect + validate + sort) from a tar file.
// It is designed to run BEFORE the concurrency slot is acquired so that the
// compress workers can start immediately when the slot opens — the tar scan
// overlaps with earlier jobs' compress/upload work instead of serialising.
//
// On error the caller should fall back to pipeline.Run() which re-reads the
// tar from scratch.  A non-nil *PrefetchResult is always valid and ready to
// pass to RunFromPrefetch.
func Prefetch(ctx context.Context, tarPath string, obs *observe.Provider) (*PrefetchResult, error) {
	f, err := os.Open(tarPath)
	if err != nil {
		return nil, fmt.Errorf("opening tar for prefetch %q: %w", tarPath, err)
	}
	defer f.Close()
	return PrefetchFromReader(ctx, f, obs)
}

// PrefetchFromReader performs Phase 0 from an io.Reader.
// It is the same collect+validate+sort logic used by RunFromReader, extracted
// so it can run before the concurrency slot is acquired.
func PrefetchFromReader(ctx context.Context, r io.Reader, obs *observe.Provider) (*PrefetchResult, error) {
	collectChan := make(chan unpack.FileEntry, 256)
	collectErrCh := make(chan error, 1)
	go func() {
		collectErrCh <- unpack.Extract(ctx, r, collectChan)
		close(collectChan)
	}()

	var capturedDirtab []byte
	var sortedEntries []unpack.FileEntry
	seenPaths := make(map[string]struct{})
	for entry := range collectChan {
		if _, dup := seenPaths[entry.Path]; dup {
			for range collectChan { //nolint:revive
			}
			<-collectErrCh
			return nil, fmt.Errorf("duplicate path %q in tar — each path must appear exactly once", entry.Path)
		}
		seenPaths[entry.Path] = struct{}{}
		if filepath.Base(entry.Path) == ".cvmfsdirtab" && entry.Mode.IsRegular() && len(entry.Data) > 0 {
			capturedDirtab = make([]byte, len(entry.Data))
			copy(capturedDirtab, entry.Data)
		}
		sortedEntries = append(sortedEntries, entry)
	}
	if err := <-collectErrCh; err != nil {
		return nil, fmt.Errorf("unpack prefetch: %w", err)
	}

	sort.SliceStable(sortedEntries, func(i, j int) bool {
		return sortedEntries[i].Size > sortedEntries[j].Size
	})

	if obs != nil {
		obs.Logger.Info("prefetch complete (phase 0 done early)",
			"entries", len(sortedEntries))
	}

	return &PrefetchResult{
		SortedEntries: sortedEntries,
		DirtabContent: capturedDirtab,
	}, nil
}

// RunFromPrefetch runs pipeline stages 1–4 using a pre-loaded PrefetchResult.
// Phase 0 (tar scan + sort) is skipped because the data is already in memory.
// This eliminates the O(tar_size / disk_bandwidth) blocking gate that normally
// delays the first compress worker when a job has been waiting in queue.
func RunFromPrefetch(ctx context.Context, prefetch *PrefetchResult, cfg Config) (*Result, error) {
	ctx, span := cfg.Obs.Tracer.Start(ctx, "pipeline.run_from_prefetch")
	defer span.End()

	cfg.Obs.Logger.InfoContext(ctx, "pipeline starting (using prefetch — phase 0 already done)",
		"workers", cfg.Workers,
		"entries", len(prefetch.SortedEntries))

	return runFromSortedEntries(ctx, prefetch.SortedEntries, prefetch.DirtabContent, cfg)
}

// RunFromReader processes a tar stream through the full pipeline and returns
// the catalog hash, object hashes, and byte counts. This allows streaming tar
// processing without requiring the complete tar to be saved to disk first.
//
// Pipeline stages:
//   0. Collect: buffer all tar entries and sort by descending size (largest first)
//   1. Fan-out: route each sorted FileEntry to both compress and catalog workers
//   2. Compress: compress and hash each file using a worker pool
//   3. Catalog: build the in-memory entry list from catalogChan
//   4. Dedup+Upload: concurrent dedup check + CAS upload worker pool
//
// Sorting by descending size ensures the largest objects enter the compress and
// upload workers first, so they are in CAS before smaller objects complete.
// Without this, a tar that happens to place small files early and large files
// late creates a long tail: the pipeline appears nearly done while a handful of
// large files are still in flight.
func RunFromReader(ctx context.Context, r io.Reader, cfg Config) (*Result, error) {
	ctx, span := cfg.Obs.Tracer.Start(ctx, "pipeline.run")
	defer span.End()

	cfg.Obs.Logger.InfoContext(ctx, "pipeline starting", "workers", cfg.Workers)

	// ── Phase 0: collect + validate + sort ────────────────────────────────────
	//
	// Buffer every tar entry so we can sort by descending size before handing
	// anything to the compress workers.  This guarantees that the upload
	// critical path (large objects) starts immediately rather than arriving
	// last in a tar ordered by name or mtime.
	//
	// Trade-off: compression doesn't begin until the entire tar has been read
	// from disk, adding O(read_time) latency before the first compress worker
	// picks up a file.  For typical payloads (< 10 GB on NVMe) this is
	// milliseconds to low seconds — negligible compared to compress+upload time.
	// The benefit is a predictable finish time regardless of tar entry order.
	//
	// Duplicate-path detection and .cvmfsdirtab capture move here from the
	// fan-out goroutine so the collect phase remains the single owner of the
	// raw entry slice.
	collectChan := make(chan unpack.FileEntry, 256)
	collectErrCh := make(chan error, 1)
	go func() {
		collectErrCh <- unpack.Extract(ctx, r, collectChan)
		close(collectChan)
	}()

	var capturedDirtab []byte
	var sortedEntries []unpack.FileEntry
	seenPaths := make(map[string]struct{})
	for entry := range collectChan {
		if _, dup := seenPaths[entry.Path]; dup {
			// Drain to unblock the extract goroutine before returning.
			for range collectChan { //nolint:revive
			}
			<-collectErrCh
			err := fmt.Errorf("duplicate path %q in tar — each path must appear exactly once", entry.Path)
			span.RecordError(err)
			return nil, err
		}
		seenPaths[entry.Path] = struct{}{}

		if filepath.Base(entry.Path) == ".cvmfsdirtab" && entry.Mode.IsRegular() && len(entry.Data) > 0 {
			capturedDirtab = make([]byte, len(entry.Data))
			copy(capturedDirtab, entry.Data)
		}

		sortedEntries = append(sortedEntries, entry)
	}
	if err := <-collectErrCh; err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("unpack: %w", err)
	}

	// Sort largest-first so compress and upload workers saturate I/O with the
	// heaviest objects immediately.  Stable sort preserves original tar order
	// for files of equal size, keeping the output deterministic.
	sort.SliceStable(sortedEntries, func(i, j int) bool {
		return sortedEntries[i].Size > sortedEntries[j].Size
	})
	cfg.Obs.Logger.InfoContext(ctx, "pipeline entries collected and sorted",
		"count", len(sortedEntries))

	return runFromSortedEntries(ctx, sortedEntries, capturedDirtab, cfg)
}

// ArchiveSource describes a single (possibly compressed) archive to be
// processed by RunFromArchiveList.  Path is required; CompressedSize may be
// pre-populated to avoid a stat syscall.  A value of 0 means "auto-stat; place
// at the end of the processing order if the stat also returns 0 or fails."
type ArchiveSource struct {
	Path           string
	CompressedSize int64 // 0 = auto-stat; still 0 after stat → placed at end
}

// streamArchive opens the file at path, auto-detects gzip compression by
// peeking at the first two magic bytes (0x1f 0x8b), and streams all
// FileEntry values produced by unpack.Extract to out.
// The file is closed before streamArchive returns.
func streamArchive(ctx context.Context, path string, out chan<- unpack.FileEntry) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening archive %q: %w", path, err)
	}
	defer f.Close()

	br := bufio.NewReaderSize(f, 512)
	magic, peekErr := br.Peek(2)
	// peekErr is non-nil only for very small (< 2 byte) or empty files — treat
	// those as plain tar (unpack.Extract will return an appropriate error).
	var r io.Reader = br
	if peekErr == nil && len(magic) >= 2 && magic[0] == 0x1f && magic[1] == 0x8b {
		gr, err := gzip.NewReader(br)
		if err != nil {
			return fmt.Errorf("gzip reader for %q: %w", path, err)
		}
		defer gr.Close()
		r = gr
	}
	return unpack.Extract(ctx, r, out)
}

// RunFromArchiveList processes a list of (possibly compressed) archives through
// the full pipeline.  Archives are streamed sequentially in descending
// compressed-size order so that the largest archives — and therefore the most
// content — enter the compress and upload workers first.
//
// If CompressedSize is 0 for an ArchiveSource the file is stat'd to determine
// its size.  Archives whose size cannot be determined are placed at the end of
// the processing order.
//
// Within each archive, entries are buffered and sorted by descending
// uncompressed size before being forwarded to the compress workers — matching
// the large-first behaviour of RunFromReader.  The result is a two-level sort:
// archive-level ordering by compressed size, entry-level ordering by
// uncompressed size within each archive.
//
// Duplicate path detection is cross-archive: a path that appears in more than
// one archive causes an error.
func RunFromArchiveList(ctx context.Context, archives []ArchiveSource, cfg Config) (*Result, error) {
	ctx, span := cfg.Obs.Tracer.Start(ctx, "pipeline.run_archive_list")
	defer span.End()

	cfg.Obs.Logger.InfoContext(ctx, "pipeline starting (archive list)",
		"archives", len(archives), "workers", cfg.Workers)

	// Phase 0a: populate missing CompressedSize fields via os.Stat.
	resolved := make([]ArchiveSource, len(archives))
	copy(resolved, archives)
	for i := range resolved {
		if resolved[i].CompressedSize == 0 && resolved[i].Path != "" {
			if info, statErr := os.Stat(resolved[i].Path); statErr == nil {
				resolved[i].CompressedSize = info.Size()
			}
			// Ignore stat errors: size stays 0 → sorted to end.
		}
	}

	// Phase 0b: sort archives by descending compressed size.
	// Archives with unknown size (0) sort to the end.
	sort.SliceStable(resolved, func(i, j int) bool {
		si, sj := resolved[i].CompressedSize, resolved[j].CompressedSize
		switch {
		case si == 0 && sj == 0:
			return false // both unknown → preserve original order
		case si == 0:
			return false // i unknown → goes after j
		case sj == 0:
			return true // j unknown → i goes first
		default:
			return si > sj
		}
	})

	// Phase 0c: stream archives one-by-one, collecting + sorting within each.
	// Archives are processed sequentially so the gzip decompressor for one
	// archive finishes before the next begins — this avoids concurrent disk
	// reads from multiple large compressed files that would thrash I/O.
	var allEntries []unpack.FileEntry
	var capturedDirtab []byte
	seenPaths := make(map[string]struct{})

	for _, arch := range resolved {
		entryCh := make(chan unpack.FileEntry, 256)
		extractErrCh := make(chan error, 1)
		archPath := arch.Path
		go func() {
			extractErrCh <- streamArchive(ctx, archPath, entryCh)
			close(entryCh)
		}()

		var archEntries []unpack.FileEntry
		var dupErr error
		for entry := range entryCh {
			if _, dup := seenPaths[entry.Path]; dup {
				// Drain to unblock the extract goroutine before returning.
				for range entryCh { //nolint:revive
				}
				dupErr = fmt.Errorf(
					"duplicate path %q in archive %q — each path must appear exactly once across all archives",
					entry.Path, archPath)
				break
			}
			seenPaths[entry.Path] = struct{}{}
			if filepath.Base(entry.Path) == ".cvmfsdirtab" && entry.Mode.IsRegular() && len(entry.Data) > 0 {
				capturedDirtab = make([]byte, len(entry.Data))
				copy(capturedDirtab, entry.Data)
			}
			archEntries = append(archEntries, entry)
		}
		if err := <-extractErrCh; err != nil {
			span.RecordError(err)
			return nil, fmt.Errorf("extracting archive %q: %w", archPath, err)
		}
		if dupErr != nil {
			span.RecordError(dupErr)
			return nil, dupErr
		}

		// Sort within-archive by descending uncompressed size.
		sort.SliceStable(archEntries, func(i, j int) bool {
			return archEntries[i].Size > archEntries[j].Size
		})
		allEntries = append(allEntries, archEntries...)
	}

	cfg.Obs.Logger.InfoContext(ctx, "pipeline entries collected and sorted",
		"archives", len(resolved), "count", len(allEntries))

	return runFromSortedEntries(ctx, allEntries, capturedDirtab, cfg)
}

// runFromSortedEntries executes pipeline stages 1–4 plus post-processing on a
// pre-sorted, pre-validated slice of FileEntry values.  It is the shared core
// used by both RunFromReader and RunFromArchiveList.
func runFromSortedEntries(
	ctx context.Context,
	sortedEntries []unpack.FileEntry,
	capturedDirtab []byte,
	cfg Config,
) (*Result, error) {
	_, span := cfg.Obs.Tracer.Start(ctx, "pipeline.stages")
	defer span.End()

	// Channels for pipeline stages.
	compressChan := make(chan unpack.FileEntry, 64)
	catalogChan := make(chan unpack.FileEntry, 64)
	compressOut := make(chan compress.Result, 64)

	eg, egCtx := errgroup.WithContext(ctx)

	// Stage 1: Fan-out — feed sorted entries to both compress and catalog.
	//
	// Fix #14: if the compress send succeeds but the catalog send is blocked
	// at context cancellation, we return an error so the two stages cannot
	// silently diverge.
	eg.Go(func() error {
		defer close(compressChan)
		defer close(catalogChan)
		for _, entry := range sortedEntries {
			select {
			case compressChan <- entry:
			case <-egCtx.Done():
				return fmt.Errorf("fan-out: compress channel blocked on %q: %w", entry.Path, egCtx.Err())
			}
			select {
			case catalogChan <- entry:
			case <-egCtx.Done():
				return fmt.Errorf("fan-out: catalog channel blocked on %q after compress send: %w", entry.Path, egCtx.Err())
			}
		}
		return nil
	})

	// Stage 3a: Compress + hash worker pool → compressOut.
	eg.Go(func() error {
		defer close(compressOut)
		return compress.Run(egCtx, compressChan, compressOut, compress.Config{
			Workers:       cfg.Workers,
			ChunkSize:     cfg.ChunkSize,
			CompressLevel: cfg.CompressLevel,
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
			if err := builder.Add(egCtx, entry); err != nil {
				cspan.RecordError(err)
				return fmt.Errorf("catalog add %s: %w", entry.Path, err)
			}
		}
		return nil
	})

	// Stage 4: Concurrent dedup + upload worker pool.
	//
	// Dedup strategy — two modes, selected by whether DedupChecker is set:
	//
	//   Default (DedupChecker == nil): call cfg.CAS.Exists for each candidate
	//   hash before uploading.  For local-disk CAS or S3 this is a single
	//   stat/HEAD request per object — fast enough that no pre-seeded index is
	//   needed.  There is no startup walk, no memory overhead, and no risk of
	//   filter saturation.  This mode is also automatically chosen when the CAS
	//   backend implements cas.NativeExistsChecker even if a DedupChecker is
	//   configured (see below).
	//
	//   Bloom-filter mode (DedupChecker != nil): use a pre-seeded Bloom filter
	//   for a fast in-memory negative test, confirming positives with CAS.Exists.
	//   Enable this when CAS.Exists is expensive (e.g. high-latency network CAS
	//   where each HEAD request takes tens of milliseconds) by passing a shared
	//   *dedup.Checker seeded at service startup via --bloom-filter or
	//   --bloom-snapshot-dir.  Backends that implement cas.NativeExistsChecker
	//   always use the direct path regardless of this setting.
	dedupChecker := cfg.DedupChecker // nil → direct CAS.Exists path
	// If the CAS backend supports native existence checks, discard any
	// configured Bloom filter: the filter adds an in-memory lookup plus RWMutex
	// overhead on top of an already-cheap CAS.Exists call (os.Stat or S3 HEAD).
	// This is a safety net; the startup code in main.go already sets DedupChecker
	// to nil for such backends, but a manually constructed Config could still
	// carry both.
	if dedupChecker != nil {
		if nec, ok := cfg.CAS.(cas.NativeExistsChecker); ok && nec.ExistsIsNative() {
			cfg.Obs.Logger.DebugContext(ctx, "pipeline: discarding Bloom filter — CAS backend has native exists check (stat/HEAD)")
			dedupChecker = nil
		}
	}
	if dedupChecker != nil {
		cfg.Obs.Logger.DebugContext(ctx, "pipeline: Bloom-filter dedup active")
	} else {
		cfg.Obs.Logger.DebugContext(ctx, "pipeline: using direct CAS.Exists for dedup (no Bloom filter)")
	}

	// checkExists reports whether hash is already in CAS.
	// When Bloom-filter mode is active it uses the two-step filter+confirm path;
	// otherwise it calls CAS.Exists directly — a single stat/HEAD per object.
	checkExists := func(workerCtx context.Context, hash string) (bool, error) {
		if dedupChecker != nil {
			return dedupChecker.Check(workerCtx, hash)
		}
		exists, err := cfg.CAS.Exists(workerCtx, hash)
		if err != nil {
			return false, fmt.Errorf("CAS existence check %s: %w", hash, err)
		}
		if exists {
			cfg.Obs.Metrics.PipelineDedupHits.Inc()
		}
		return exists, nil
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
	// seenHashes guards intra-job deduplication across concurrent upload workers.
	// The pre-claim pattern (claim under lock, then dedup-check and upload outside
	// the lock) ensures each hash is uploaded at most once per job without
	// requiring the dedup check to be inside a critical section.
	seenHashes := make(map[string]bool)
	resultsByPath := make(map[string]fileMeta)

	// uploadConc is the number of concurrent dedup+upload workers.
	// Default to 1 for backward compatibility when UploadConc is not set.
	uploadConc := cfg.UploadConc
	if uploadConc <= 0 {
		uploadConc = 1
	}
	uploadSem := semaphore.NewWeighted(int64(uploadConc))

	// processHash performs dedup check and CAS upload for a single object hash.
	// It uses the pre-claim pattern: the caller must have already reserved the
	// hash in seenHashes under resultMu before calling this function, so multiple
	// concurrent workers never attempt to upload the same object.
	// compressedData and compressedSize refer to the object bytes to upload;
	// they may be nil/0 for objects that are already confirmed dedup hits.
	processHash := func(workerCtx context.Context, hash string, compressedData []byte, compressedSize int64) error {
		isDup, err := checkExists(workerCtx, hash)
		if err != nil {
			return fmt.Errorf("dedup check %s: %w", hash, err)
		}

		if isDup {
			// Already in CAS — confirmed by dedup.Check (which increments the
			// dedup-hits metric).  Add to ObjectHashes so S1 distribution workers
			// know to push this object to Stratum 1 replicas.
			//
			// Do NOT add to NewObjectHashes: the object was uploaded to the gateway
			// CAS in a previous commit and is still present there.  Excluding it
			// from NewObjectHashes prevents SubmitPayload from re-uploading it
			// during the lease window, keeping the leased-state window proportional
			// to the size of the current change rather than the total repository size.
			resultMu.Lock()
			result.ObjectHashes = append(result.ObjectHashes, hash)
			resultMu.Unlock()
			return nil
		}

		// New object: upload to CAS.
		if err := upload.PutWithRetry(workerCtx, cfg.CAS, hash, compressedData, compressedSize); err != nil {
			return fmt.Errorf("cas put %s: %w", hash, err)
		}
		// Update the Bloom filter (when active) so subsequent jobs and concurrent
		// workers can dedup against this object without a CAS.Exists round-trip.
		if dedupChecker != nil {
			dedupChecker.Add(hash)
		}
		if err := uploadLog.Record(hash); err != nil {
			return fmt.Errorf("recording upload %s: %w", hash, err)
		}

		resultMu.Lock()
		result.NBytesComp += compressedSize
		result.ObjectHashes = append(result.ObjectHashes, hash)
		result.NewObjectHashes = append(result.NewObjectHashes, hash)
		resultMu.Unlock()
		return nil
	}

	eg.Go(func() error {
		_, uspan := cfg.Obs.Tracer.Start(egCtx, "pipeline.upload")
		defer uspan.End()

		// inner errgroup runs upload workers concurrently; its context is
		// derived from egCtx so any pipeline stage failure cancels all workers.
		inner, innerCtx := errgroup.WithContext(egCtx)

		// Fix #P1 (upload stage): capture sem.Acquire failure without returning early.
		// If we returned on Acquire error, in-flight inner.Go workers would still be
		// running when we return, and they access shared state (result, resultMu,
		// uploadLog) after the outer eg proceeds past eg.Wait() — a data race.
		// Breaking out and always reaching inner.Wait() provides the happens-before
		// guarantee that all workers have exited before any shared state is torn down.
		var semErr error
		for compResult := range compressOut {
			compResult := compResult // capture for closure

			// Build the lightweight file metadata outside any lock — allocation
			// work happens before entering the critical section.
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

			// Update per-file counters and path→meta map.
			resultMu.Lock()
			result.NFiles++
			result.NBytesRaw += compResult.FileEntry.Size
			if compResult.FileEntry.Mode.IsRegular() {
				resultsByPath[compResult.FileEntry.Path] = fm
			}
			resultMu.Unlock()

			if !compResult.FileEntry.Mode.IsRegular() {
				continue
			}

			// Determine which hashes actually need uploading via the pre-claim
			// pattern: atomically check-and-set seenHashes for each candidate
			// hash.  Hashes that are already claimed (intra-job dup) are added
			// to ObjectHashes immediately without spawning a worker.
			type uploadTask struct {
				hash           string
				compressed     []byte
				compressedSize int64
			}
			var tasks []uploadTask

			resultMu.Lock()
			if len(compResult.Chunks) > 0 {
				for _, chunk := range compResult.Chunks {
					if seenHashes[chunk.Hash] {
						// Intra-job duplicate: the first occurrence already
						// added this hash to ObjectHashes (if it was a new
						// upload) or skipped it (if it was a dedup hit).
						// Either way, do not add again — SubmitPayload would
						// otherwise upload the same object twice.
						cfg.Obs.Metrics.PipelineDedupHits.Inc()
					} else {
						seenHashes[chunk.Hash] = true
						tasks = append(tasks, uploadTask{
							hash:           chunk.Hash,
							compressed:     chunk.Compressed,
							compressedSize: chunk.CompressedSize,
						})
					}
				}
			} else {
				hash := compResult.Hash
				if hash != "" && hash != "0000000000000000000000000000000000000000" {
					if seenHashes[hash] {
						// Intra-job duplicate — see chunked case above.
						cfg.Obs.Metrics.PipelineDedupHits.Inc()
					} else {
						seenHashes[hash] = true
						tasks = append(tasks, uploadTask{
							hash:           hash,
							compressed:     compResult.Compressed,
							compressedSize: compResult.CompressedSize,
						})
					}
				}
			}
			resultMu.Unlock()

			// Spawn a worker for each hash that was not an intra-job duplicate.
			for _, task := range tasks {
				task := task // capture for closure
				if err := uploadSem.Acquire(innerCtx, 1); err != nil {
					uspan.RecordError(err)
					semErr = err
					break // break inner task loop; semErr != nil breaks the outer loop below
				}
				inner.Go(func() error {
					defer uploadSem.Release(1)
					if err := processHash(innerCtx, task.hash, task.compressed, task.compressedSize); err != nil {
						uspan.RecordError(err)
						return err
					}
					return nil
				})
			}
			if semErr != nil {
				break // drain compressOut is not needed; compress.Run drains on ctx cancel
			}
		}

		// CRITICAL: always wait for every in-flight worker before returning.
		// Our caller (eg.Wait) proceeds to close shared state once all eg.Go
		// goroutines return — if any inner worker is still running, it will race
		// on result/resultMu/uploadLog.  inner.Wait() is the happens-before fence.
		if waitErr := inner.Wait(); waitErr != nil {
			uspan.RecordError(waitErr)
			if semErr == nil {
				return waitErr
			}
		}
		return semErr
	})

	if err := eg.Wait(); err != nil {
		uploadLog.Close() //nolint:errcheck // best-effort flush on error path
		span.RecordError(err)
		return nil, fmt.Errorf("pipeline: %w", err)
	}

	// Flush and close the upload log now that all CAS writes are complete.
	// Errors here are non-fatal: the objects are in CAS; the log is for crash
	// recovery, not correctness.
	if err := uploadLog.Close(); err != nil {
		cfg.Obs.Logger.Warn("pipeline: upload log close failed (non-fatal)", "error", err)
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
		result.CatalogEntries[i].HashAlgo = cvmfscatalog.HashSha1
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

	// Bloom filter snapshot: persist the updated filter so peer nodes can merge
	// it on their next job start.  Only runs when Bloom-filter mode is active
	// AND snapshot sharing is enabled (--bloom-snapshot-dir).  Non-fatal — a
	// missed save only means peers won't see this run's new objects until the
	// next successful save.  Runs asynchronously to avoid blocking the caller
	// on shared-filesystem I/O.
	if dedupChecker != nil && cfg.SharedFilter.Enabled {
		// Capture locals for the goroutine closure; ctx is already done by the
		// time we reach here (egCtx is derived from it), so pass background.
		snapshotDedupChecker := dedupChecker
		snapshotFilter := cfg.SharedFilter
		snapshotObs := cfg.Obs
		go func() {
			if saveErr := snapshotDedupChecker.SaveSnapshot(snapshotFilter, snapshotObs); saveErr != nil {
				snapshotObs.Logger.Warn("shared bloom: snapshot save failed (continuing)",
					"error", saveErr)
			}
		}()
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
