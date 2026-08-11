// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package lease

import (
	"archive/tar"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// tinyTar writes a one-entry tar so Commit gets past extraction and reaches the
// publish step, which is what this test is about.
func tinyTar(t *testing.T, dir string) string {
	t.Helper()
	p := filepath.Join(dir, "payload.tar")
	f, err := os.Create(p)
	if err != nil {
		t.Fatalf("create tar: %v", err)
	}
	defer f.Close()
	tw := tar.NewWriter(f)
	body := []byte("x")
	if err := tw.WriteHeader(&tar.Header{
		Name: "f", Mode: 0o644, Size: int64(len(body)),
	}); err != nil {
		t.Fatalf("tar header: %v", err)
	}
	if _, err := tw.Write(body); err != nil {
		t.Fatalf("tar body: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	return p
}

// TestCommit_InterruptedPublishIsNotReportedAsPublished drives the real Commit
// path with a publish that hangs AFTER printing the commit marker, then lets the
// deadline fire.
//
// Two things must hold, and both were broken at different points:
//
//   - The marker must not be believed. Before the process group was killed on
//     cancel, an interrupted publish never returned, so "marker printed =>
//     commit finished" held by accident; once it could return, that branch
//     promoted a killed publish to StatePublished.
//   - The error must not carry a context error in its chain. cvmfs_server's own
//     error is frequently ctx.Err() verbatim, and context.DeadlineExceeded
//     satisfies net.Error, so ClassOf would call it transient and invite a retry
//     of a publish that may already be in the repository.
func TestCommit_InterruptedPublishIsNotReportedAsPublished(t *testing.T) {
	dir := t.TempDir()
	stub := "#!/bin/sh\n" +
		"if [ \"$1\" = publish ]; then echo 'Exporting repository manifest'; sleep 120; fi\n" +
		"exit 0\n"
	if err := os.WriteFile(filepath.Join(dir, "cvmfs_server"), []byte(stub), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	b := NewLocalBackend(dir, newTestObs(t))
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	err := b.Commit(ctx, CommitRequest{
		Token:    "test.cvmfs.io",
		CVMFSDir: filepath.Join(dir, "target"),
		TarPath:  tinyTar(t, dir),
	})

	if err == nil {
		t.Fatal("interrupted publish returned nil error")
	}
	if errors.Is(err, ErrCommittedNotRemounted) {
		t.Errorf("interrupted publish reported as committed: the orchestrator "+
			"treats ErrCommittedNotRemounted as published. err = %v", err)
	}
	if !errors.Is(err, ErrPublishInterrupted) {
		t.Errorf("want ErrPublishInterrupted in the chain, got %v", err)
	}
	for _, ce := range []error{context.DeadlineExceeded, context.Canceled} {
		if errors.Is(err, ce) {
			t.Errorf("error still carries %v, so its class depends on why the "+
				"context ended (DeadlineExceeded satisfies net.Error => transient "+
				"=> retried). err = %v", ce, err)
		}
	}
	if !strings.Contains(err.Error(), "commit marker seen: true") {
		t.Errorf("the marker should still be reported for triage; got %v", err)
	}
}

// TestInterruptedPublishErr_NeverCarriesAContextError pins the %v-not-%w rule
// directly, because the end-to-end test above cannot reach it: there the child
// is always killed, so cvmfs_server's error is an *exec.ExitError. The cases
// that matter are the ones where exec hands back ctx.Err() verbatim.
func TestInterruptedPublishErr_NeverCarriesAContextError(t *testing.T) {
	for _, ce := range []error{context.Canceled, context.DeadlineExceeded} {
		// pubErr == ctxErr is the reachable shape: exec.Cmd.Start returns
		// ctx.Err() when the context is already done, and watchCtx injects it
		// when the process exits 0 at the deadline.
		err := interruptedPublishErr("test.cvmfs.io", ce, ce, true)
		if !errors.Is(err, ErrPublishInterrupted) {
			t.Errorf("%v: sentinel missing from chain", ce)
		}
		if errors.Is(err, ce) {
			t.Errorf("%v is still in the error chain; ClassOf keys on net.Error, "+
				"so DeadlineExceeded would be classed transient and retried", ce)
		}
		if !strings.Contains(err.Error(), ce.Error()) {
			t.Errorf("%v: cause dropped from the message", ce)
		}
	}
}
