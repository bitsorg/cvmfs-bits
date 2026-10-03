// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"os/exec"
	"runtime/debug"
	"strings"
	"testing"
)

// TestMain lets a test re-run this binary as cvmfs-prepub: with
// PREPUB_TEST_MAIN_ARGS set it runs main() with those arguments instead.
func TestMain(m *testing.M) {
	if args, ok := os.LookupEnv("PREPUB_TEST_MAIN_ARGS"); ok {
		os.Args = append([]string{"cvmfs-prepub"}, strings.Fields(args)...)
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestVersionFlag(t *testing.T) {
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), "PREPUB_TEST_MAIN_ARGS=--version")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("--version: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "cvmfs-prepub ") || lines[0] == "cvmfs-prepub " {
		t.Fatalf("--version printed %q", out)
	}
}

func TestVersionFrom(t *testing.T) {
	vcs := &debug.BuildInfo{Settings: []debug.BuildSetting{
		{Key: "vcs.revision", Value: "0123456789abcdef0123"},
		{Key: "vcs.time", Value: "2026-10-01T12:00:00Z"},
		{Key: "vcs.modified", Value: "true"},
	}}
	cases := []struct {
		name, ld string
		info     *debug.BuildInfo
		ok       bool
		want     string
	}{
		{"ldflags wins", "v1.2.3-4-gabc", vcs, true, "v1.2.3-4-gabc"},
		{"vcs fallback", "", vcs, true, "0123456789ab-dirty (2026-10-01T12:00:00Z)"},
		{"no vcs", "", &debug.BuildInfo{}, true, "dev"},
		{"no build info", "", nil, false, "dev"},
	}
	for _, c := range cases {
		if got := versionFrom(c.ld, c.info, c.ok); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}
