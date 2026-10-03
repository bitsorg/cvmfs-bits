// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package main

import "runtime/debug"

// version is set at build time by the Makefile:
//
//	go build -ldflags "-X main.version=$(git describe --tags --always --dirty)"
var version string

// versionString returns the build version: the -ldflags value, else the VCS
// revision and time the go tool stamped into the binary, else "dev".
func versionString() string {
	info, ok := debug.ReadBuildInfo()
	return versionFrom(version, info, ok)
}

func versionFrom(ldflags string, info *debug.BuildInfo, ok bool) string {
	if ldflags != "" {
		return ldflags
	}
	if !ok || info == nil {
		return "dev"
	}
	var rev, at, dirty string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.time":
			at = s.Value
		case "vcs.modified":
			if s.Value == "true" {
				dirty = "-dirty"
			}
		}
	}
	if rev == "" {
		return "dev"
	}
	if len(rev) > 12 {
		rev = rev[:12]
	}
	if at != "" {
		return rev + dirty + " (" + at + ")"
	}
	return rev + dirty
}
