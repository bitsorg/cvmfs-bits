// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
