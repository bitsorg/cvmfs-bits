// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package lease

import (
	"context"
	"fmt"
	"log/slog"
	"os/exec"
)

// The object list needs /proc/self/fd to name the inherited pipe, which is
// Linux-only. prepub runs on Linux; this stub exists so the package still
// builds elsewhere (macOS development, `go vet` on a laptop) and fails loudly
// rather than silently publishing without the list it was asked for.

func objectListChildPath() string { return "" }

func runWithObjectList(
	ctx context.Context, cmd *exec.Cmd, log *slog.Logger, onLine func(string),
) (bool, error) {
	return false, fmt.Errorf("object list requires Linux (/proc/self/fd)")
}
