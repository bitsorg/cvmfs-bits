// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	start := time.Now()
	out, tl, readToEOF, err, feedErr := b.runFed(ctx, repo, baseArg, "batch", onLine,
		func(w io.Writer) error { return writeBatch(w, req.Members) }, args...)
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

// runFed runs cvmfs_server with args reading its tar from stdin, which feed writes
// from a goroutine. A failed feed kills the ingest before closing its input: a
// stream that ends early can read as a shorter, complete archive. what names the
// input in the log ("batch", "package"); the ingest's error and the feed's are returned.
func (b *IngestBackend) runFed(ctx context.Context, repo, base, what string, onLine func(string),
	feed func(io.Writer) error, args ...string) (string, *timeline, bool, error, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return "", nil, false, fmt.Errorf("ingest backend: %s pipe: %w", what, err), nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	fed := make(chan error, 1)
	var abandoned atomic.Bool // set, a slow writer cannot complete the stream later
	go func() {
		werr := feed(guardedInput{w, &abandoned})
		if werr != nil {
			cancel()
		} else if !abandoned.Load() {
			werr = w.Close()
		}
		fed <- werr
	}()

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
	out, tl, readToEOF, err := b.cvmfsServerRun(runCtx, r, onLine, setup, args...)
	r.Close() // a writer still blocked on the pipe now fails instead of waiting
	var feedErr error
	select {
	case feedErr = <-fed:
	case <-time.After(batchWriterGrace):
		abandoned.Store(true)
		feedErr = fmt.Errorf("the %s writer did not stop", what)
	}
	// A short stream is closed only once nothing can read it: the group was
	// killed, or the write failed for want of any reader (EPIPE).
	if feedErr != nil && !groupKilled.Load() && !errors.Is(feedErr, syscall.EPIPE) {
		abandoned.Store(true)
	}
	if abandoned.Load() {
		b.obs.Logger.Error("ingest backend: "+what+" input left open: a child of the ingest "+
			"may still read it", "repo", repo, "base", base, "error", feedErr)
		abandonedInputs.Lock()
		abandonedInputs.files = append(abandonedInputs.files, w)
		abandonedInputs.Unlock()
	} else {
		w.Close()
	}
	if (err != nil || feedErr != nil) && (groupKilled.Load() || abandoned.Load()) {
		b.abortOwn(ctx, repo, what+" ingest stopped") // it cannot drop its own lease
	}
	return out, tl, readToEOF, err, feedErr
}
