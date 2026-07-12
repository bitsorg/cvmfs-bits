// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

// Command prepub-finalize publishes a build's accumulated packages in one
// ingestsql commit (ADR-0007 coarse publish). It runs on the release-manager
// host, where cvmfs_swissknife and the repository store are available — the
// containerized cvmfs-prepub is not, since it commits via the gateway API and
// does not carry swissknife or a store mount. The bits build invokes this once,
// after all package jobs sharing a build_id have reached StateAccumulated.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"cvmfs.io/prepub/internal/buildset"
)

func main() {
	spoolRoot := flag.String("spool-root", "", "prepub spool root (contains builds/<id>/)")
	buildID := flag.String("build", "", "build id to finalize")
	swissknife := flag.String("swissknife", "cvmfs_swissknife", "path to cvmfs_swissknife")
	configPrefix := flag.String("config-prefix", "", "ingestsql -C gateway-client config dir")
	leasePath := flag.String("lease-path", "", "ingestsql -l; empty auto-detects the common prefix")
	keep := flag.Bool("keep", false, "do not remove the accumulator on success")
	flag.Parse()

	if *spoolRoot == "" || *buildID == "" || *configPrefix == "" {
		fmt.Fprintln(os.Stderr, "usage: prepub-finalize -spool-root DIR -build ID -config-prefix DIR "+
			"[-swissknife BIN] [-lease-path P] [-keep]")
		os.Exit(2)
	}

	members, err := buildset.Load(*spoolRoot, *buildID)
	if err != nil {
		fatal(err)
	}
	if len(members) == 0 {
		fatal(fmt.Errorf("no accumulated packages for build %q", *buildID))
	}

	work, err := os.MkdirTemp("", "prepub-finalize-")
	if err != nil {
		fatal(err)
	}
	defer os.RemoveAll(work)
	ingestTmp := filepath.Join(work, "ingest-tmp")
	if err := os.MkdirAll(ingestTmp, 0o755); err != nil {
		fatal(err)
	}

	conflicts, out, ferr := buildset.Finalize(context.Background(), members,
		filepath.Join(work, "descriptor.db"), buildset.IngestOptions{
			Swissknife:   *swissknife,
			Repo:         members[0].Repo,
			ConfigPrefix: *configPrefix,
			TempDir:      ingestTmp,
			LeasePath:    *leasePath,
		})

	if out != "" {
		fmt.Fprintln(os.Stderr, out)
	}
	summary := map[string]interface{}{
		"build":     *buildID,
		"repo":      members[0].Repo,
		"packages":  len(members),
		"published": len(members) - len(conflicts),
		"conflicts": conflicts,
	}
	b, _ := json.MarshalIndent(summary, "", "  ")
	fmt.Println(string(b))
	if ferr != nil {
		fatal(ferr)
	}
	if !*keep {
		_ = buildset.Remove(*spoolRoot, *buildID)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "prepub-finalize:", err)
	os.Exit(1)
}
