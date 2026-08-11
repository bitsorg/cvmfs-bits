// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"os/exec"
	"syscall"
	"time"
)

// cvmfsServerWaitDelay bounds how long Wait may block after the process group
// has been signalled.  It is a backstop for a process that ignores SIGKILL in
// uninterruptible sleep; the group kill below is what normally ends things.
const cvmfsServerWaitDelay = 10 * time.Second

// newCvmfsServerCmd builds a cvmfs_server invocation that dies *completely*
// when ctx is cancelled.
//
// exec.CommandContext on its own is not enough here. cvmfs_server is a shell
// script that execs cvmfs_swissknife, so the process tree is
//
//	cvmfs_server (bash)  →  sh -c  →  cvmfs_swissknife
//
// and CommandContext signals only the direct child. The surviving grandchild
// inherits the output pipe, so CombinedOutput blocks reading a pipe that will
// never close — the call does not return even though the job timed out, and
// whatever lock the caller holds is never released.
//
// That is not hypothetical: a wedged 2.5 GB ingest ran 9h23m past its own 30m
// timeout still holding the per-repo commit lock, and the 66 jobs queued behind
// it each waited the full 30m and failed. Setpgid + killing the group closes
// the pipe, so Wait returns and the caller unwinds.
func newCvmfsServerCmd(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "cvmfs_server", args...)

	// Put the child in its own process group so the whole tree can be signalled
	// with one kill(-pgid) — killing the leader alone leaves the grandchildren.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		// Negative pid: signal the entire process group, not just the leader.
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}

	// If a group member still holds the pipe after the kill, give up on it
	// rather than blocking the caller forever.
	cmd.WaitDelay = cvmfsServerWaitDelay

	return cmd
}
