// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package lease

import (
	"archive/tar"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ingestLine returns the recorded `ingest ...` invocation from the stub log.
func ingestLine(t *testing.T, calls []string) string {
	t.Helper()
	for _, c := range calls {
		if strings.HasPrefix(c, "ingest ") {
			return c
		}
	}
	t.Fatalf("no ingest invocation recorded; calls: %v", calls)
	return ""
}

func oneEntryTar(t *testing.T, dir string) string {
	t.Helper()
	p := filepath.Join(dir, "p.tar")
	f, err := os.Create(p)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()
	tw := tar.NewWriter(f)
	if err := tw.WriteHeader(&tar.Header{Name: "f", Mode: 0o644, Size: 1}); err != nil {
		t.Fatalf("hdr: %v", err)
	}
	if _, err := tw.Write([]byte("x")); err != nil {
		t.Fatalf("body: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return p
}

// TestCommit_DirectS3Flag pins how the direct-to-S3 request reaches
// cvmfs_server.
//
// The flag is what actually enables the feature. An earlier prototype switched
// on the mere existence of /etc/cvmfs/<repo>.s3.conf, and the testbed was built
// around that; the current cvmfs_server requires --direct-s3 (or
// CVMFS_INGEST_DIRECT_S3=true) and treats the file only as where to read the
// config from. Publishing therefore appeared to work while quietly using the
// gateway, with an empty bucket as the only clue.
func TestCommit_DirectS3Flag(t *testing.T) {
	for _, tc := range []struct {
		name     string
		directS3 bool
		wantFlag bool
	}{
		{"requested", true, true},
		{"not requested", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := fakeCvmfsServer(t, "")
			repo := "test.cvmfs.io"
			b, mount := newAncestorBackend(t, repo)
			// Pre-create the parent so ensureAncestors stays out of the way.
			base := filepath.Join(mount, repo, "pkg")
			if err := os.MkdirAll(filepath.Dir(base), 0o755); err != nil {
				t.Fatalf("seed: %v", err)
			}

			if err := b.Commit(context.Background(), CommitRequest{
				Token:    repo,
				TarPath:  oneEntryTar(t, t.TempDir()),
				CVMFSDir: base,
				DirectS3: tc.directS3,
			}); err != nil {
				t.Fatalf("commit: %v", err)
			}

			line := ingestLine(t, calls())
			if got := strings.Contains(line, "--direct-s3"); got != tc.wantFlag {
				t.Errorf("--direct-s3 present = %v, want %v\n  argv: %s",
					got, tc.wantFlag, line)
			}
			// cvmfs_server's option loop runs `while [ "$2" != "" ]` and then
			// takes $1 as the repository, so the repository MUST stay last —
			// a flag appended after it is consumed AS the repository name.
			if !strings.HasSuffix(line, " "+repo) {
				t.Errorf("repository must be the final argument, got: %s", line)
			}
		})
	}
}
