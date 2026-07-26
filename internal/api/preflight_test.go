// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"strings"
	"testing"

	"cvmfs.io/prepub/internal/buildset"
	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/pkg/cvmfscatalog"
)

// putObj stores content under its CAS key (hex sha256 + optional suffix) and
// returns the raw hash bytes.
func putObj(t *testing.T, backend cas.Backend, content, suffix string) []byte {
	t.Helper()
	sum := sha256.Sum256([]byte(content))
	key := hex.EncodeToString(sum[:]) + suffix
	if err := backend.Put(context.Background(), key,
		bytes.NewReader([]byte(content)), int64(len(content))); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
	return sum[:]
}

func member(path string, entries ...cvmfscatalog.Entry) buildset.Member {
	return buildset.Member{Repo: "test.cvmfs.io", Path: path, Entries: entries}
}

func fileEntry(name string, hash []byte, size int64) cvmfscatalog.Entry {
	return cvmfscatalog.Entry{FullPath: "/" + name, Name: name, Hash: hash,
		Size: size, Mode: 0o644}
}

func chunkedEntry(name string, chunk []byte, size int64) cvmfscatalog.Entry {
	return cvmfscatalog.Entry{FullPath: "/" + name, Name: name, Size: size,
		Mode: 0o644, Chunks: []cvmfscatalog.ChunkRecord{{Offset: 0, Size: size, Hash: chunk}}}
}

func TestPreflightObjects(t *testing.T) {
	backend, err := cas.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatalf("localfs: %v", err)
	}
	o := &Orchestrator{CAS: backend}
	ctx := context.Background()

	present := putObj(t, backend, "plain content", "")
	presentChunk := putObj(t, backend, "chunked content", "P")

	t.Run("all present", func(t *testing.T) {
		members := []buildset.Member{
			member("x86_64-el9/Packages/foo/1.0",
				fileEntry("foo.sh", present, 13),
				chunkedEntry("libfoo.so", presentChunk, 15)),
		}
		if err := o.preflightObjects(ctx, members); err != nil {
			t.Fatalf("expected pass, got: %v", err)
		}
	})

	t.Run("missing object fails with named path", func(t *testing.T) {
		missing := sha256.Sum256([]byte("never uploaded"))
		members := []buildset.Member{
			member("x86_64-el9/Packages/foo/1.0", fileEntry("foo.sh", present, 13)),
			member("x86_64-el9/Packages/bar/2.0", fileEntry("bar.sh", missing[:], 7)),
		}
		err := o.preflightObjects(ctx, members)
		if err == nil {
			t.Fatal("expected pre-flight failure for missing object")
		}
		if !strings.Contains(err.Error(), "bar/2.0/bar.sh") {
			t.Fatalf("error should name the missing path, got: %v", err)
		}
		if !strings.Contains(err.Error(), "re-publish") {
			t.Fatalf("error should advise re-publish, got: %v", err)
		}
	})

	t.Run("dirs symlinks deletions empties are skipped", func(t *testing.T) {
		members := []buildset.Member{
			member("x86_64-el9/Packages/baz/3.0",
				cvmfscatalog.Entry{FullPath: "/d", Name: "d", Mode: fs.ModeDir | 0o755},
				cvmfscatalog.Entry{FullPath: "/l", Name: "l", Symlink: "d", Mode: fs.ModeSymlink | 0o777},
				cvmfscatalog.Entry{FullPath: "/gone", Name: "gone", IsDelete: true},
				fileEntry("empty", nil, 0)),
		}
		if err := o.preflightObjects(ctx, members); err != nil {
			t.Fatalf("expected pass (nothing to probe), got: %v", err)
		}
	})

	t.Run("nil CAS skips", func(t *testing.T) {
		noCAS := &Orchestrator{}
		missing := sha256.Sum256([]byte("x"))
		members := []buildset.Member{
			member("p", fileEntry("f", missing[:], 1)),
		}
		if err := noCAS.preflightObjects(ctx, members); err != nil {
			t.Fatalf("nil CAS must skip pre-flight, got: %v", err)
		}
	})
}
