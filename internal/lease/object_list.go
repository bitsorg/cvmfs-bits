// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package lease

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"time"
)

// objectListExtraIndex is this pipe's position in cmd.ExtraFiles.
//
// The child sees ExtraFiles[i] as descriptor 3+i, because 0,1,2 are stdio.
// runWithObjectList REFUSES to run when ExtraFiles is already populated, so
// this stays 0 and fd 3 is unambiguous.
const objectListExtraIndex = 0

// objectListDrainGrace bounds the wait for EOF after the child has exited.
//
// EOF arrives only when every write end is closed, so a grandchild that
// outlived the process-group kill still holds it. os/exec's WaitDelay does NOT
// cover ExtraFiles — it bounds only the stdio pipes os/exec created itself —
// so this is the only bound on that case.
const objectListDrainGrace = 10 * time.Second

// objectListCancelGrace replaces the full grace when the context is already
// cancelled: the process-group kill has run, so anything still holding the
// descriptor escaped the group and is not about to release it. Measured 1.31s
// with this, 10.31s without. When a grandchild also holds stdout/stderr, Wait
// itself first pays cvmfsServerWaitDelay, and the two graces compound —
// that is the 20s case this avoids, not the common one.
const objectListCancelGrace = 1 * time.Second

// objectListMaxLine caps one line. The writer emits "<key> <ok|failed>
// <created|present|->", so ~120 bytes; 1 MiB is far past any legitimate line
// and exists only so a corrupt stream cannot stall the reader.
const objectListMaxLine = 1 << 20

// objectListChildPath is what --object-list receives: the write end of the
// pipe, as seen from inside the child.
//
// DANGER: only valid for a command wrapped by runWithObjectList. /proc/self/fd
// resolves in the CHILD, and Go marks every other descriptor CLOEXEC, so an
// unwrapped child has no fd 3 at all — unless it opened one itself, which
// cvmfs_server does for shell lock descriptors. fopen(path,"w") is O_TRUNC, so
// this would truncate that. Never build this path anywhere else.
//
// Linux-only, which is why this file is build-tagged: /proc/self/fd does not
// exist on macOS. /dev/fd/N would be more portable, but /proc is the spelling
// verified end to end on the testbed and is not worth changing unproven.
func objectListChildPath() string {
	return fmt.Sprintf("/proc/self/fd/%d", 3+objectListExtraIndex)
}

// runWithObjectList runs cmd with an extra inherited pipe and calls onLine for
// every line the child writes to it.
//
// The bool is readToEOF, and it means exactly that: the reader reached EOF
// without a scanner error or an expired grace. It does NOT mean the list is
// the publish's full object set — a SIGKILLed publisher also closes the pipe
// cleanly, which reads as a perfectly good EOF.
//
// The list is authoritative only when readToEOF is true AND the returned error
// is nil. Callers must apply both; either alone is a way to warm a cache from
// a revision that was never published.
//
// onLine runs on another goroutine, is joined before return, and must not
// block: the pipe buffer is 64 KiB and the writer is the thread inside
// swissknife reaping S3 completions, so a slow consumer back-pressures the
// publish. A panic in onLine is recovered rather than killing the daemon.
func runWithObjectList(
	ctx context.Context, cmd *exec.Cmd, log *slog.Logger, onLine func(string),
) (bool, error) {
	if onLine == nil {
		return false, fmt.Errorf("object list: onLine is nil")
	}
	if len(cmd.ExtraFiles) != objectListExtraIndex {
		return false, fmt.Errorf("object list: ExtraFiles has %d entries, "+
			"expected %d — fd %d would not be the object list",
			len(cmd.ExtraFiles), objectListExtraIndex, 3+objectListExtraIndex)
	}

	pr, pw, err := os.Pipe()
	if err != nil {
		return false, fmt.Errorf("object list pipe: %w", err)
	}
	cmd.ExtraFiles = append(cmd.ExtraFiles, pw)

	if err := cmd.Start(); err != nil {
		pr.Close()
		pw.Close()
		return false, fmt.Errorf("object list: start: %w", err)
	}

	// Close the parent's copy NOW. A pipe reports EOF only when every write end
	// is closed, and this process holds one until it does. Skip this and the
	// read blocks until the drain grace expires — the publish still completes,
	// just ten seconds later, every time, with no clue why.
	pw.Close()

	// START the reader before Wait. The child blocks writing into a full 64 KiB
	// pipe and a blocked child never exits, so a reader that only starts after
	// Wait() returns deadlocks on any publish emitting more than one buffer's
	// worth. Measured: moving this goroutine below Wait() hangs at ~1k lines.
	drained := make(chan struct{})
	readToEOF := false
	go func() {
		defer close(drained)
		// Whatever happens above, keep draining to EOF. Stopping early while
		// the child is alive wedges it in write(2) on a full pipe, and Wait()
		// then never returns — a hung publish holding the commit lock, which
		// is the failure this package exists to avoid. WaitDelay does not
		// cover this descriptor, so nothing else would break the deadlock.
		defer io.Copy(io.Discard, pr) //nolint:errcheck // draining, not reading

		sc := bufio.NewScanner(pr)
		sc.Buffer(make([]byte, 0, 64*1024), objectListMaxLine)
		for sc.Scan() {
			// A panic here would otherwise kill the daemon mid-publish and
			// strand the gateway lease.
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Error("object list: onLine panicked", "panic", r)
					}
				}()
				onLine(sc.Text())
			}()
		}
		if err := sc.Err(); err != nil {
			log.Error("object list: read failed, list is incomplete", "err", err)
			return
		}
		readToEOF = true
	}()

	waitErr := cmd.Wait()

	grace := objectListDrainGrace
	if ctx.Err() != nil {
		grace = objectListCancelGrace
	}

	select {
	case <-drained:
		pr.Close()
	case <-time.After(grace):
		// Something in the process group still holds the write end, so EOF will
		// never come. Closing the read end unblocks the reader; it is also the
		// only close on this path, since the reader owns pr until it returns.
		log.Warn("object list: no EOF after the child exited, list is incomplete",
			"grace", grace.String(), "ctx_err", ctx.Err())
		pr.Close()
		<-drained
		readToEOF = false
	}

	return readToEOF, waitErr
}
