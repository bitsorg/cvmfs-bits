// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package lease

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cvmfs.io/prepub/pkg/cvmfscatalog"
)

// The testbed run of CommitBatch: real `cvmfs_server ingest` (a mountless
// publisher registered with the gateway), under batch-it/<time>/ only. Skipped
// unless PREPUB_TESTBED_REPO and PREPUB_TESTBED_STRATUM0 name the testbed.
func testbedBatch(t *testing.T) (b *IngestBackend, repo, s0, root string) {
	repo, s0 = os.Getenv("PREPUB_TESTBED_REPO"), strings.TrimRight(os.Getenv("PREPUB_TESTBED_STRATUM0"), "/")
	if repo == "" || s0 == "" {
		t.Skip("PREPUB_TESTBED_REPO and PREPUB_TESTBED_STRATUM0 (e.g. http://minio:9000/cvmfs) not set")
	}
	b = NewIngestBackend(IngestOptions{NestedCatalog: true}, newTestObs(t))
	return b, repo, s0, fmt.Sprintf("batch-it/%d", time.Now().Unix())
}

// nestedCatalogAt reports whether abs is the mount point of a nested catalog in
// the published revision, walking down from the root catalog.
func nestedCatalogAt(t *testing.T, s0, repo, abs string) bool {
	t.Helper()
	ctx := context.Background()
	h, err := cvmfscatalog.FetchManifestRootHash(ctx, nil, s0, repo)
	if err != nil {
		t.Fatal(err)
	}
	h = strings.TrimSuffix(h, "C")
	parts := strings.Split(strings.Trim(abs, "/"), "/")
	for depth := 0; depth < 64; depth++ {
		db := filepath.Join(t.TempDir(), "catalog.db")
		if err := cvmfscatalog.DownloadCatalog(ctx, nil, s0, repo, h, db); err != nil {
			t.Fatal(err)
		}
		cat, err := cvmfscatalog.Open(db)
		if err != nil {
			t.Fatal(err)
		}
		next := ""
		for i := len(parts); i > 0 && next == ""; i-- { // the deepest mount on the way
			p := "/" + strings.Join(parts[:i], "/")
			if child, _, found, err := cat.FindNestedMount(p); err != nil {
				t.Fatal(err)
			} else if found {
				if p == "/"+strings.Join(parts, "/") {
					cat.Close()
					return true
				}
				next = child
			}
		}
		cat.Close()
		if next == "" {
			return false
		}
		h = next
	}
	t.Fatalf("catalog walk too deep for %s", abs)
	return false
}

func published(t *testing.T, s0, repo, rel string) (string, bool) {
	t.Helper()
	data, found, err := cvmfscatalog.ReadPublishedFile(context.Background(), nil, s0, repo, rel)
	if err != nil {
		t.Fatal(err)
	}
	return string(data), found
}

func testbedMember(t *testing.T, name string) BatchMember {
	return BatchMember{TarPath: writeTestTar(t, []batchTestEntry{batchDir("./"), batchDir("./bin/"),
		batchFile("./bin/x", "content of "+name)}), RelPath: name + "/1-1"}
}

func TestTestbed_CommitBatch(t *testing.T) {
	b, repo, s0, root := testbedBatch(t)
	ctx := context.Background()
	base := root + "/Packages"
	t.Logf("test tree /%s; to remove it: cvmfs_server ingest -d %s %s", root, root, repo)

	// 1. Three packages in one ingest: each published, each its own catalog.
	members := []BatchMember{testbedMember(t, "a"), testbedMember(t, "b"), testbedMember(t, "c")}
	token, err := b.Acquire(ctx, repo, base)
	if err != nil {
		t.Fatal(err)
	}
	stats := &PublishStats{}
	if err := b.CommitBatch(ctx, BatchCommitRequest{Token: token, Base: base, Members: members, Stats: stats}); err != nil {
		t.Fatal(err)
	}
	t.Logf("batch of 3: %s", stats.Backend)
	for _, m := range members {
		name := strings.TrimSuffix(m.RelPath, "/1-1")
		if got, found := published(t, s0, repo, path.Join(base, m.RelPath, "bin/x")); !found || got != "content of "+name {
			t.Errorf("%s: found=%v content=%q", m.RelPath, found, got)
		}
		if !nestedCatalogAt(t, s0, repo, path.Join(base, m.RelPath)) {
			t.Errorf("%s is not a nested catalog", m.RelPath)
		}
	}

	// 2. A writer failing after one member's entries: nothing published.
	orig := writeBatch
	writeBatch = func(w io.Writer, ms []BatchMember) error {
		tw := tar.NewWriter(w)
		if err := tw.WriteHeader(&tar.Header{Name: "d/1-1/bin/x", Typeflag: tar.TypeReg, Mode: 0o644, Size: 1}); err != nil {
			return err
		}
		tw.Write([]byte("d"))
		tw.Flush()
		time.Sleep(time.Second)
		return errors.New("injected failure")
	}
	token, err = b.Acquire(ctx, repo, base)
	if err != nil {
		t.Fatal(err)
	}
	err = b.CommitBatch(ctx, BatchCommitRequest{Token: token, Base: base,
		Members: []BatchMember{testbedMember(t, "d"), testbedMember(t, "e")}})
	writeBatch = orig
	if err == nil || !strings.Contains(err.Error(), "injected failure") {
		t.Fatalf("err = %v, want the injected failure", err)
	}
	if _, found := published(t, s0, repo, path.Join(base, "d/1-1/bin/x")); found {
		t.Error("a member of the killed batch was published")
	}

	// 3. The repository is free again: the next batch publishes.
	token, err = b.Acquire(ctx, repo, base)
	if err != nil {
		t.Fatal(err)
	}
	f := testbedMember(t, "f")
	if err := b.CommitBatch(ctx, BatchCommitRequest{Token: token, Base: base, Members: []BatchMember{f}}); err != nil {
		t.Fatalf("after the killed batch: %v", err)
	}
	if _, found := published(t, s0, repo, path.Join(base, "f/1-1/bin/x")); !found {
		t.Error("f not published")
	}
}
