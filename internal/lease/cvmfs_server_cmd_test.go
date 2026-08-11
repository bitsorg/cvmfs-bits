// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package lease

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestCvmfsServerCmd_ReapRaceDoesNotFailASuccessfulCommand covers the window
// where Cancel runs AFTER Wait has already reaped the child: os/exec selects
// between the two at random when both are ready, so a raw kill then returns
// ESRCH. os/exec promotes a non-nil, non-ErrProcessDone cancel error into the
// command's error, which reports a command that SUCCEEDED as failed — at the
// ingest call site that marks a package that did publish as failed, and the
// publisher retries it.
//
// Only assertions on a process that genuinely exited 0 are meaningful; a kill
// landing first is legitimate and produces a real failure.
func TestCvmfsServerCmd_ReapRaceDoesNotFailASuccessfulCommand(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "cvmfs_server"),
		[]byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	const iterations = 400
	spurious := 0
	for i := 0; i < iterations; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cmd := newCvmfsServerCmd(ctx, "publish", "test.cvmfs.io")
		if err := cmd.Start(); err != nil {
			cancel()
			t.Fatalf("start: %v", err)
		}
		go cancel() // race the reap
		err := cmd.Wait()

		// A bare context.Canceled here is os/exec's documented behaviour: when
		// the context fires while the command is finishing, Wait reports the
		// cancellation. The stdlib's own cancel does the same, and callers must
		// (and do) check ctx.Err(). What must NOT happen is a DIFFERENT error —
		// that is the ESRCH path, where os/exec promotes the cancel failure into
		// the command's error and a successful run looks like a failed one.
		if cmd.ProcessState != nil && cmd.ProcessState.Success() && err != nil &&
			!errors.Is(err, context.Canceled) {
			spurious++
			if spurious == 1 {
				t.Logf("first spurious failure on iteration %d: %v", i, err)
			}
		}
	}
	if spurious != 0 {
		t.Errorf("%d/%d successful runs failed with a non-cancellation error; "+
			"ESRCH from the group kill is not mapped to os.ErrProcessDone",
			spurious, iterations)
	}
}

// TestCvmfsServerCmd_CancelKillsTheWholeGroup is the regression test for an
// ingest that outlived its own 30m timeout by nine hours.
//
// The stub mimics the real process tree: cvmfs_server is a shell script that
// leaves a longer-lived process holding the output pipe. exec.CommandContext
// signals only the direct child, so without a process-group kill CombinedOutput
// blocks on a pipe the survivor still holds — the call never returns, and the
// per-repo commit lock the caller holds is never released. 66 queued jobs died
// that way.
//
// The 5s bound is deliberately below cvmfsServerWaitDelay: WaitDelay alone
// would also unblock this eventually, and this test must fail if the group kill
// is removed and only the backstop remains.
func TestCvmfsServerCmd_CancelKillsTheWholeGroup(t *testing.T) {
	if cvmfsServerWaitDelay <= 5*time.Second {
		t.Fatalf("test bound (5s) must stay below cvmfsServerWaitDelay (%s), "+
			"otherwise it cannot tell a group kill from the backstop",
			cvmfsServerWaitDelay)
	}

	dir := t.TempDir()
	// Background a child that inherits stdout/stderr, then linger: exactly the
	// shape of cvmfs_server -> sh -c -> cvmfs_swissknife.
	stub := "#!/bin/sh\nsh -c 'sleep 120' &\nsleep 120\n"
	if err := os.WriteFile(filepath.Join(dir, "cvmfs_server"), []byte(stub), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		cmd := newCvmfsServerCmd(ctx, "publish", "test.cvmfs.io")
		_, _ = cmd.CombinedOutput()
	}()

	// Let the stub get as far as spawning its child before pulling the plug.
	time.Sleep(300 * time.Millisecond)
	cancel()

	select {
	case <-done:
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Errorf("CombinedOutput returned only after %s; the group kill is "+
				"not working and WaitDelay is carrying the test", elapsed)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("CombinedOutput never returned after the context was cancelled: " +
			"a surviving process in the group still holds the output pipe")
	}
}
