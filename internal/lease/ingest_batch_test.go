// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package lease

import (
	"archive/tar"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const batchTestRepo = "test.repo.example"

// batchBackend is an ingest backend (nested catalogs on, as in production)
// with the repository slot taken, and a stub cvmfs_server running script.
func batchBackend(t *testing.T, script string) (*IngestBackend, string, string) {
	t.Helper()
	argv := stubCvmfsServer(t, script)
	b := NewIngestBackend(IngestOptions{CVMFSMount: t.TempDir(), NestedCatalog: true, SkipAncestorDirs: true},
		newTestObs(t))
	token, err := b.Acquire(context.Background(), batchTestRepo, "")
	if err != nil {
		t.Fatal(err)
	}
	return b, token, argv
}

func twoMembers(t *testing.T) []BatchMember {
	return []BatchMember{
		{writeTestTar(t, []batchTestEntry{batchDir("./"), batchFile("./bin/a", "A")}), "a/1-1"},
		{writeTestTar(t, []batchTestEntry{batchDir("./"), batchFile("./lib/b", "B")}), "b/1-1"},
	}
}

// commitBatchWithin fails the test instead of hanging when CommitBatch does not return.
func commitBatchWithin(t *testing.T, b *IngestBackend, req BatchCommitRequest) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- b.CommitBatch(context.Background(), req) }()
	select {
	case err := <-done:
		return err
	case <-time.After(20 * time.Second):
		t.Fatal("CommitBatch did not return")
		return nil
	}
}

// One ingest at the base from stdin, without -c, carrying every member under
// its path with its marker; the stats are the batch's and the slot is free.
func TestCommitBatch_StreamsOneTarAtTheBase(t *testing.T) {
	in := filepath.Join(t.TempDir(), "in.tar")
	b, token, argv := batchBackend(t, "cat > "+in)
	members := twoMembers(t)
	stats := &PublishStats{}
	err := commitBatchWithin(t, b, BatchCommitRequest{Token: token, Base: "x/Packages",
		Members: members, Stats: stats})
	if err != nil {
		t.Fatal(err)
	}
	if got := readArgv(t, argv); got != "ingest -t - -b x/Packages "+batchTestRepo {
		t.Errorf("argv = %q", got)
	}
	data, err := os.ReadFile(in)
	if err != nil {
		t.Fatal(err)
	}
	got := readTestTar(t, data)
	for _, name := range []string{"a/1-1/bin/a", "a/1-1/.cvmfscatalog", "b/1-1/lib/b", "b/1-1/.cvmfscatalog"} {
		if _, ok := got[name]; !ok {
			t.Errorf("%s missing from the stream", name)
		}
	}
	var want int64
	for _, m := range members {
		fi, _ := os.Stat(m.TarPath)
		want += fi.Size()
	}
	if stats.TarBytes == nil || *stats.TarBytes != want || stats.Backend <= 0 {
		t.Errorf("stats = %+v, want tar bytes %d and a backend time", stats, want)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := b.Acquire(ctx, batchTestRepo, ""); err != nil {
		t.Errorf("slot not released: %v", err)
	}
}

// A writer failing partway kills the ingest while its input is still open, so
// it never sees the end of a short stream (which could read as complete).
func TestCommitBatch_WriterFailureKillsTheIngest(t *testing.T) {
	dir := t.TempDir()
	seen, calls := filepath.Join(dir, "seen"), filepath.Join(dir, "calls")
	b, token, _ := batchBackend(t, `echo "$*" >> `+calls+`; [ "$1" = ingest ] || exit 0
cat > /dev/null; echo end-of-input > `+seen)
	failWriterPartway(t)
	err := commitBatchWithin(t, b, BatchCommitRequest{Token: token, Base: "x", Members: twoMembers(t)})
	if err == nil || !strings.Contains(err.Error(), "disk read failed") {
		t.Fatalf("err = %v, want the writer's error", err)
	}
	time.Sleep(200 * time.Millisecond)
	if _, statErr := os.Stat(seen); statErr == nil {
		t.Error("the ingest reached the end of its input: a short stream could have been published")
	}
	if got, _ := os.ReadFile(calls); !strings.Contains(string(got), "abort -f "+batchTestRepo) {
		t.Errorf("calls %q: the killed ingest's transaction was not aborted", got)
	}
}

// failWriterPartway makes the batch writer send one member's entries, without
// an end of archive, and then fail.
func failWriterPartway(t *testing.T) {
	orig := writeBatch
	t.Cleanup(func() { writeBatch = orig })
	writeBatch = func(w io.Writer, members []BatchMember) error {
		tw := tar.NewWriter(w)
		if err := tw.WriteHeader(&tar.Header{Name: "a/1-1/f", Typeflag: tar.TypeReg, Mode: 0o644}); err != nil {
			return err
		}
		tw.Flush()
		time.Sleep(200 * time.Millisecond) // the stub is reading by now
		return errors.New("disk read failed")
	}
}

// When the ingest's leader exits by itself and a child of it still reads, the
// input is left open: that child must not see the end of a short stream.
func TestCommitBatch_LeaderGoneInputStaysOpen(t *testing.T) {
	seen := filepath.Join(t.TempDir(), "seen")
	b, token, _ := batchBackend(t, `[ "$1" = ingest ] || exit 0
exec 3<&0 # a background list's own stdin would be /dev/null
(cat <&3 > /dev/null; echo end-of-input > `+seen+`) > /dev/null 2>&1 &
exit 0`)
	failWriterPartway(t)
	err := commitBatchWithin(t, b, BatchCommitRequest{Token: token, Base: "x", Members: twoMembers(t)})
	if err == nil || !strings.Contains(err.Error(), "disk read failed") {
		t.Fatalf("err = %v, want the writer's error", err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, statErr := os.Stat(seen); statErr == nil {
		t.Error("a surviving child reached the end of a short stream")
	}
}

// An ingest that fails without reading its input does not leave the writer
// blocked on a full pipe.
func TestCommitBatch_IngestFailingEarlyDoesNotHang(t *testing.T) {
	b, token, _ := batchBackend(t, "echo 'ingest refused' >&2; exit 3")
	abandonedInputs.Lock()
	before := len(abandonedInputs.files)
	abandonedInputs.Unlock()
	big := writeTestTar(t, []batchTestEntry{batchDir("./"), batchFile("./big", strings.Repeat("x", 4<<20))})
	err := commitBatchWithin(t, b, BatchCommitRequest{Token: token, Base: "x",
		Members: []BatchMember{{big, "a/1-1"}}})
	if err == nil || !strings.Contains(err.Error(), "exit status 3") || !strings.Contains(err.Error(), "ingest refused") {
		t.Fatalf("err = %v, want the ingest's exit status and output", err)
	}
	abandonedInputs.Lock()
	defer abandonedInputs.Unlock()
	if n := len(abandonedInputs.files) - before; n != 0 {
		t.Errorf("%d inputs left open after an ingest that had nothing reading", n)
	}
}

func TestCommitBatch_RefusesBadRequests(t *testing.T) {
	for _, base := range []string{"/abs", "../x", "a/../b", ".", ".."} {
		b, token, argv := batchBackend(t, "exit 0")
		if err := b.CommitBatch(context.Background(), BatchCommitRequest{Token: token, Base: base,
			Members: twoMembers(t)}); err == nil {
			t.Errorf("base %q accepted", base)
		}
		if _, err := os.Stat(argv); err == nil {
			t.Errorf("base %q: cvmfs_server ran", base)
		}
	}
	b, token, _ := batchBackend(t, "exit 0")
	if err := b.CommitBatch(context.Background(), BatchCommitRequest{Token: token}); err == nil {
		t.Error("a batch without members accepted")
	}
}

func TestCommitBatch_RootBase(t *testing.T) {
	b, token, argv := batchBackend(t, "cat > /dev/null")
	if err := commitBatchWithin(t, b, BatchCommitRequest{Token: token, Members: twoMembers(t)}); err != nil {
		t.Fatal(err)
	}
	if got := readArgv(t, argv); got != "ingest -t - -b / "+batchTestRepo {
		t.Errorf("argv = %q", got)
	}
}

// A writer still going after the grace period cannot finish the archive for a
// child that still reads: its later writes, the end of archive among them, fail.
func TestCommitBatch_SlowWriterCannotFinishTheStream(t *testing.T) {
	got := filepath.Join(t.TempDir(), "read")
	b, token, _ := batchBackend(t, `[ "$1" = ingest ] || exit 0
exec 3<&0
cat <&3 > `+got+` 2> /dev/null &
exit 0`)
	grace := batchWriterGrace
	t.Cleanup(func() { batchWriterGrace = grace })
	batchWriterGrace = 300 * time.Millisecond
	orig := writeBatch
	t.Cleanup(func() { writeBatch = orig })
	writeBatch = func(w io.Writer, members []BatchMember) error {
		tw := tar.NewWriter(w)
		if err := tw.WriteHeader(&tar.Header{Name: "a/1-1/f", Typeflag: tar.TypeReg, Mode: 0o644}); err != nil {
			return err
		}
		tw.Flush()
		time.Sleep(time.Second) // past the grace period
		return tw.Close()       // the end of archive
	}
	if err := commitBatchWithin(t, b, BatchCommitRequest{Token: token, Base: "x", Members: twoMembers(t)}); err == nil {
		t.Fatal("a writer that did not stop was not reported")
	}
	time.Sleep(time.Second)
	if fi, err := os.Stat(got); err != nil || fi.Size() != 512 {
		t.Errorf("the child read %v bytes (%v), want only the 512-byte header", fi, err)
	}
}
