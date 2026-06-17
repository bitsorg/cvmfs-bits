// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package spool

import (
	"os"
	"path/filepath"
	"testing"

	"cvmfs.io/prepub/pkg/observe"
)

// TestNew_NoDoubledRootDir is a regression test for the spool-root path-doubling
// bug. The directory list passed to New must NOT include `root` itself: doing so
// computed filepath.Join(root, root), which Go cleans into a spurious nested dir
// (e.g. /data/spool -> /data/spool/data/spool). That junk dir broke prepub
// startup with "permission denied" once its parent was not writable by the
// container. New must create the real state subdirs directly under root and
// never the doubled path.
func TestNew_NoDoubledRootDir(t *testing.T) {
	obs, shutdown, err := observe.New("test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)

	root := t.TempDir()
	if _, err := New(root, obs); err != nil {
		t.Fatalf("New: %v", err)
	}

	for _, d := range []string{"incoming", "staging", "uploading", "distributing",
		"leased", "committing", "published", "failed", "aborted"} {
		if fi, err := os.Stat(filepath.Join(root, d)); err != nil || !fi.IsDir() {
			t.Errorf("expected state dir %q under root, err=%v", d, err)
		}
	}

	doubled := filepath.Join(root, root)
	if _, err := os.Stat(doubled); err == nil {
		t.Errorf("spurious doubled spool dir was created: %s", doubled)
	}
}
