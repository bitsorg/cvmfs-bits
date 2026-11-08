// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// BatchCommitRequest is one ingest of several packages: their spooled tars as
// one stream at Base, the repository-relative directory above them all.
type BatchCommitRequest struct {
	Token            string        // from Acquire
	Base             string        // "" is the repository root
	Members          []BatchMember // each RelPath relative to Base
	DirectS3         bool
	ObjectList       bool
	ConfirmedObjects *[]string
	Stats            *PublishStats // TarBytes is the whole batch's
}

// writeBatch is WriteBatchTar; tests replace it to fail partway.
var writeBatch = WriteBatchTar

// CommitBatch publishes req.Members in one `cvmfs_server ingest -t - -b Base`.
// The repository slot is released either way; on error nothing is published.

// batchWriterGrace bounds the wait for the writer once the ingest has ended.
var batchWriterGrace = 30 * time.Second

// guardedInput refuses writes once the input is abandoned, so the end of the
// archive cannot follow.
type guardedInput struct {
	w         *os.File
	abandoned *atomic.Bool
}

func (g guardedInput) Write(p []byte) (int, error) {
	if g.abandoned.Load() {
		return 0, errors.New("batch input abandoned")
	}
	return g.w.Write(p)
}

// abandonedInputs holds batch inputs left open on purpose: an unreachable
// *os.File is closed by the garbage collector, which would end the stream.
var abandonedInputs struct {
	sync.Mutex
	files []*os.File
}

func (b *IngestBackend) CommitBatch(ctx context.Context, req BatchCommitRequest) error {
	defer b.release(req.Token)
	repo := repoOf(req.Token)
	base := req.Base
	if base != "" && (path.IsAbs(base) || path.Clean(base) != base || base == "." ||
		base == ".." || strings.HasPrefix(base, "../") || strings.HasPrefix(base, "-")) {
		return fmt.Errorf("ingest backend: batch base %q is not a clean relative path", base)
	}
	// Before the ingest starts: a bad member then costs nothing to refuse.
	if err := CheckBatch(req.Members); err != nil {
		return fmt.Errorf("ingest backend: %w", err)
	}

	ancestorsStart := time.Now()
	err := b.ensureAncestors(ctx, repo, path.Join(b.cvmfsMount, repo, base))
	if req.Stats != nil {
		req.Stats.Ancestors = time.Since(ancestorsStart)
	}
	if err != nil {
		return err
	}
	baseArg := base
	if baseArg == "" {
		baseArg = "/"
	}
	useObjectList := req.ObjectList && req.DirectS3
	// No -c: every member carries its own catalog marker, and -c would make the
	// base one catalog for them all (baseExists=true leaves it out).
	args := b.commitArgs(repo, baseArg, "-", req.DirectS3, useObjectList, true)
	if req.Stats != nil {
		var n int64
		for _, m := range req.Members {
			if fi, statErr := os.Stat(m.TarPath); statErr == nil {
				n += fi.Size()
			}
		}
		req.Stats.TarBytes = &n
	}
	b.obs.Logger.Info("ingest backend: publishing a batch", "repo", repo, "base", baseArg,
		"packages", len(req.Members), "direct_s3", req.DirectS3, "object_list", req.ObjectList)

	// A failed write kills the ingest before closing its input: a stream that
	// ends early can read as a shorter, complete archive.
	r, w, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("ingest backend: batch pipe: %w", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	fed := make(chan error, 1)
	var abandoned atomic.Bool // set, a slow writer cannot complete the stream later
	go func() {
		werr := writeBatch(guardedInput{w, &abandoned}, req.Members)
		if werr != nil {
			cancel()
		} else if !abandoned.Load() {
			werr = w.Close()
		}
		fed <- werr
	}()

	var (
		listed    int
		confirmed []string
	)
	var onLine func(string)
	if useObjectList {
		onLine = func(line string) {
			listed++
			if req.ConfirmedObjects != nil {
				if name, ok := ConfirmedObjectName(line); ok {
					confirmed = append(confirmed, name)
				}
			}
		}
	}
	// Whether the kill reached the process group while its leader was alive.
	var groupKilled atomic.Bool
	setup := func(c *exec.Cmd) {
		if kill := c.Cancel; kill != nil {
			c.Cancel = func() error {
				e := kill()
				if e == nil || errors.Is(e, os.ErrProcessDone) {
					groupKilled.Store(true)
				}
				return e
			}
		}
	}
	start := time.Now()
	out, tl, readToEOF, err := b.cvmfsServerRun(runCtx, r, onLine, setup, args...)
	r.Close() // a writer still blocked on the pipe now fails instead of waiting
	var feedErr error
	select {
	case feedErr = <-fed:
	case <-time.After(batchWriterGrace):
		abandoned.Store(true)
		feedErr = errors.New("the batch writer did not stop")
	}
	// A short stream is closed only once nothing can read it: the group was
	// killed, or the write failed for want of any reader (EPIPE).
	if feedErr != nil && !groupKilled.Load() && !errors.Is(feedErr, syscall.EPIPE) {
		abandoned.Store(true)
	}
	if abandoned.Load() {
		b.obs.Logger.Error("ingest backend: batch input left open: a child of the ingest "+
			"may still read it", "repo", repo, "base", baseArg, "error", feedErr)
		abandonedInputs.Lock()
		abandonedInputs.files = append(abandonedInputs.files, w)
		abandonedInputs.Unlock()
	} else {
		w.Close()
	}
	if (err != nil || feedErr != nil) && (groupKilled.Load() || abandoned.Load()) {
		b.abortOwn(ctx, repo, "batch ingest stopped") // it cannot drop its own lease
	}
	b.logTimeline(repo, baseArg, tl, err)

	if err != nil || feedErr != nil {
		if useObjectList {
			b.obs.Logger.Warn("ingest backend: batch failed, object list is partial",
				"repo", repo, "base", baseArg, "object_list_lines", listed)
		}
		if err == nil {
			err = feedErr
		}
		return fmt.Errorf("cvmfs_server ingest of %d packages into %q: %w (writing the batch: %v; output: %s)",
			len(req.Members), baseArg, err, feedErr, truncateLog(out))
	}
	if req.Stats != nil {
		req.Stats.Backend = time.Since(start)
		if useObjectList {
			n := listed
			req.Stats.Objects = &n
			req.Stats.ObjectsAuthoritative = readToEOF
		}
	}
	if useObjectList && !readToEOF {
		b.obs.Logger.Warn("ingest backend: batch published but the object list was truncated; "+
			"do not pre-warm from it", "repo", repo, "base", baseArg, "object_list_lines", listed)
	} else if useObjectList && req.ConfirmedObjects != nil {
		*req.ConfirmedObjects = confirmed
	}
	b.obs.Logger.Info("ingest backend: published a batch", "repo", repo, "base", baseArg,
		"packages", len(req.Members), "duration", time.Since(start).String())
	return nil
}
