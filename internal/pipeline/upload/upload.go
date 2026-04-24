package upload

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/internal/pipeline/compress"
	"cvmfs.io/prepub/pkg/observe"
)

// maxUploadAttempts is the number of times a single object upload is retried
// before the error is surfaced to the caller.
const maxUploadAttempts = 3

// PutWithRetry tries casBackend.Put up to maxUploadAttempts times with
// exponential backoff (100 ms, 200 ms, …).  It returns immediately on
// context cancellation so abort signals are not swallowed.
// Exported so the streaming pipeline (pipeline.go) can share the same retry
// logic without duplicating it.
func PutWithRetry(ctx context.Context, casBackend cas.Backend, hash string, data []byte, size int64) error {
	var lastErr error
	delay := 100 * time.Millisecond
	for attempt := 0; attempt < maxUploadAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				delay *= 2
			}
		}
		err := casBackend.Put(ctx, hash, bytes.NewReader(data), size)
		if err == nil {
			return nil
		}
		// Do not retry on context cancellation / deadline exceeded.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		lastErr = err
	}
	return fmt.Errorf("upload failed after %d attempts: %w", maxUploadAttempts, lastErr)
}

type UploadLog struct {
	path string
	mu   sync.Mutex
}

func OpenUploadLog(path string) *UploadLog {
	return &UploadLog{path: path}
}

func (l *UploadLog) Record(hash string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Fix: 0600 so other local users cannot read upload logs that may reveal
	// object hashes or internal CAS layout.
	f, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("opening upload log: %w", err)
	}
	defer f.Close()

	data, err := json.Marshal(map[string]string{"hash": hash, "time": time.Now().Format(time.RFC3339)})
	if err != nil {
		return fmt.Errorf("marshaling upload log entry: %w", err)
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("writing to upload log: %w", err)
	}

	return f.Sync()
}

// ReadHashes returns the deduplicated list of object hashes recorded in the
// upload log.  This is the authoritative list of objects that were
// successfully written to CAS for this job — useful when reconstructing
// ObjectHashes after a crash mid-pipeline.
//
// The returned slice preserves first-occurrence order and contains no
// duplicates.  If the log does not exist, a nil slice is returned without error.
func (l *UploadLog) ReadHashes() ([]string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	data, err := os.ReadFile(l.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading upload log: %w", err)
	}

	seen := make(map[string]struct{})
	var hashes []string
	dec := json.NewDecoder(bytes.NewReader(data))

	for dec.More() {
		var entry struct {
			Hash string `json:"hash"`
		}
		if err := dec.Decode(&entry); err != nil {
			return nil, fmt.Errorf("decoding upload log entry: %w", err)
		}
		if entry.Hash == "" {
			continue
		}
		if _, dup := seen[entry.Hash]; !dup {
			seen[entry.Hash] = struct{}{}
			hashes = append(hashes, entry.Hash)
		}
	}

	return hashes, nil
}

// Contains reports whether hash has been recorded in the upload log.
// It uses JSON decoding rather than substring search so a hash that is a
// prefix of another recorded hash does not produce a false positive.
func (l *UploadLog) Contains(hash string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	data, err := os.ReadFile(l.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("reading upload log: %w", err)
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	for dec.More() {
		var entry struct {
			Hash string `json:"hash"`
		}
		if err := dec.Decode(&entry); err != nil {
			return false, fmt.Errorf("decoding upload log entry: %w", err)
		}
		if entry.Hash == hash {
			return true, nil
		}
	}
	return false, nil
}

// Run uploads compress.Results that are not already in CAS.
func Run(ctx context.Context, in <-chan compress.Result, casBackend cas.Backend, log *UploadLog, concurrency int, obs *observe.Provider) error {
	ctx, span := obs.Tracer.Start(ctx, "pipeline.upload")
	defer span.End()

	eg, egCtx := errgroup.WithContext(ctx)
	sem := semaphore.NewWeighted(int64(concurrency))

	for result := range in {
		if err := sem.Acquire(egCtx, 1); err != nil {
			span.RecordError(err)
			return err
		}

		result := result // capture for closure
		eg.Go(func() error {
			defer sem.Release(1)

			uctx, uspan := obs.Tracer.Start(egCtx, "upload.object")
			defer uspan.End()

			start := time.Now()

			// Upload to CAS with per-object retry for transient errors.
			err := PutWithRetry(uctx, casBackend, result.Hash, result.Compressed, result.CompressedSize)
			if err != nil {
				uspan.RecordError(err)
				return fmt.Errorf("uploading %s: %w", result.Hash, err)
			}

			// Record in log
			if err := log.Record(result.Hash); err != nil {
				uspan.RecordError(err)
				return fmt.Errorf("recording upload: %w", err)
			}

			obs.Metrics.CASUploadDuration.Observe(time.Since(start).Seconds())

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		span.RecordError(err)
		return err
	}

	return nil
}
