// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

package buildset

import (
	"context"
	"fmt"
	"os"
	"os/exec"

	"cvmfs.io/prepub/pkg/cvmfsdescriptor"
)

// IngestOptions configures the single end-of-build cvmfs_swissknife ingestsql
// invocation. LeasePath is optional: when empty, ingestsql auto-detects the
// lease as the longest common prefix of the descriptor paths (the natural
// publish root for the whole build).
type IngestOptions struct {
	Swissknife   string   // path to cvmfs_swissknife (default: "cvmfs_swissknife")
	Repo         string   // -N fully-qualified repo name
	ConfigPrefix string   // -C gateway-client config prefix dir
	TempDir      string   // -t scratch dir (required by ingestsql)
	LeasePath    string   // -l; empty => auto-detect
	ExtraEnv     []string // appended to os.Environ (e.g. "LD_LIBRARY_PATH=...")
}

// Finalize assembles the build into one descriptor and publishes it in a single
// gateway commit via ingestsql. Returns the conflicts that were excluded
// (validate-then-commit partial success) and the ingestsql output.
func Finalize(ctx context.Context, members []Member, descriptorPath string, opt IngestOptions) (conflicts []Conflict, output string, err error) {
	entries, conflicts := Assemble(members)
	if len(entries) == 0 {
		return conflicts, "", fmt.Errorf("buildset.Finalize: nothing to publish (%d conflict(s))", len(conflicts))
	}
	if err := cvmfsdescriptor.Write(descriptorPath, entries); err != nil {
		return conflicts, "", fmt.Errorf("buildset.Finalize: write descriptor: %w", err)
	}
	out, rerr := runIngest(ctx, descriptorPath, opt)
	if rerr != nil {
		return conflicts, out, fmt.Errorf("buildset.Finalize: ingestsql: %w", rerr)
	}
	return conflicts, out, nil
}

func runIngest(ctx context.Context, descriptorPath string, opt IngestOptions) (string, error) {
	bin := opt.Swissknife
	if bin == "" {
		bin = "cvmfs_swissknife"
	}
	args := []string{
		"ingestsql",
		"-D", descriptorPath,
		"-N", opt.Repo,
		"-C", opt.ConfigPrefix,
		"-t", opt.TempDir,
		"-z", // create missing nested catalogs
	}
	if opt.LeasePath != "" {
		args = append(args, "-l", opt.LeasePath)
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = append(os.Environ(), opt.ExtraEnv...)
	b, err := cmd.CombinedOutput()
	return string(b), err
}
