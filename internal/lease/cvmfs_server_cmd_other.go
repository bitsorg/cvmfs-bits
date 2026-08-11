// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package lease

import (
	"context"
	"os/exec"
	"time"
)

// Mirror of the Unix build's constant — the two files are mutually exclusive,
// so this is a redeclaration only in the sense that exactly one is ever built.
// Keep the values in step.
const cvmfsServerWaitDelay = 10 * time.Second

// newCvmfsServerCmd — non-Unix fallback.
//
// Process groups (Setpgid) and kill(-pgid) are Unix concepts, and the Unix
// build uses them to make sure a cancelled cvmfs_server takes its whole tree
// with it. There is no portable equivalent, so here the cancellation is the
// stdlib's: only the direct child is signalled.
//
// A grandchild holding the output pipe would therefore still block Wait, which
// is the bug the Unix version exists to prevent — WaitDelay bounds it rather
// than fixing it. cvmfs_server does not run on these platforms; this file
// exists so the module still compiles for them, as it did before the Unix
// version was added.
func newCvmfsServerCmd(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "cvmfs_server", args...)
	cmd.WaitDelay = cvmfsServerWaitDelay
	return cmd
}
