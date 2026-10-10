// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package lease

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// helper: run sh with the object-list pipe attached, collecting lines.
func runSh(t *testing.T, script string) ([]string, error) {
	t.Helper()
	var mu sync.Mutex
	var lines []string
	cmd := exec.Command("sh", "-c", script)
	readToEOF, err := runWithObjectList(context.Background(), cmd, testLogger(), func(s string) {
		mu.Lock()
		lines = append(lines, s)
		mu.Unlock()
	})
	_ = readToEOF
	mu.Lock()
	defer mu.Unlock()
	return append([]string(nil), lines...), err
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// runShFull also reports readToEOF.
func runShFull(t *testing.T, script string) ([]string, bool, error) {
	t.Helper()
	var mu sync.Mutex
	var lines []string
	cmd := exec.Command("sh", "-c", script)
	readToEOF, err := runWithObjectList(context.Background(), cmd, testLogger(), func(s string) {
		mu.Lock()
		lines = append(lines, s)
		mu.Unlock()
	})
	mu.Lock()
	defer mu.Unlock()
	return append([]string(nil), lines...), readToEOF, err
}

// The child writes to the path we advertise, and we read exactly those lines.
// This is the whole contract: swissknife opens objectListChildPath() and writes.
func TestRunWithObjectList_ChildWritesViaAdvertisedPath(t *testing.T) {
	script := fmt.Sprintf(`exec 9>%s
echo "repo/data/ab/cdef ok created" >&9
echo "repo/data/12/3456 ok present" >&9
echo "repo/data/78/9abc failed -"   >&9`, objectListChildPath())

	lines, err := runSh(t, script)
	if err != nil {
		t.Fatalf("command failed: %v", err)
	}
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3: %q", len(lines), lines)
	}
	if !strings.HasSuffix(lines[2], "failed -") {
		t.Errorf("third line mangled: %q", lines[2])
	}
}

// The deadlock guard. More than one 64 KiB pipe buffer of output must flow,
// which only works because the drain runs concurrently with Wait.
//
// NEGATIVE CONTROL, verified: move the whole `go func(){...}()` block BELOW
// `cmd.Wait()` and this fails after 60s with the message below. Moving only
// the `<-drained` receive does NOT reproduce it — the goroutine is still
// draining — which is why the invariant is "start the reader before Wait",
// not "receive after Wait".
func TestRunWithObjectList_LargerThanPipeBufferDoesNotDeadlock(t *testing.T) {
	const n = 5000 // ~60 bytes each => ~300 KiB, several pipe buffers
	script := fmt.Sprintf(`exec 9>%s
i=0
while [ $i -lt %d ]; do
  echo "repo/data/ab/cdef0123456789012345678901234567890123 ok created" >&9
  i=$((i+1))
done`, objectListChildPath(), n)

	done := make(chan struct{})
	var lines []string
	var err error
	go func() {
		defer close(done)
		lines, err = runSh(t, script)
	}()

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("deadlocked: the drain is not running concurrently with Wait")
	}
	if err != nil {
		t.Fatalf("command failed: %v", err)
	}
	if len(lines) != n {
		t.Errorf("got %d lines, want %d — output was truncated", len(lines), n)
	}
}

// A child that writes nothing is normal (a publish with no new objects) and
// must not hang or error.
//
// The elapsed-time assertion is the point of this test, not an extra. EOF
// arrives only when EVERY write end is closed, including the parent's copy of
// it. Without pw.Close() the read blocks until objectListDrainGrace expires
// and then completes anyway — so a test that only checked lines and error
// would PASS while every publish silently paid the full grace period.
// NEGATIVE CONTROL: delete pw.Close() and this must fail on duration.
// Measured: 0.004s with the close, 10.02s without.
func TestRunWithObjectList_NoOutput(t *testing.T) {
	start := time.Now()
	lines, err := runSh(t, "true")
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("command failed: %v", err)
	}
	if len(lines) != 0 {
		t.Errorf("expected no lines, got %q", lines)
	}
	if elapsed > objectListDrainGrace/2 {
		t.Errorf("took %v: EOF came from the drain grace expiring, not from "+
			"the pipe closing — the parent's write end is being leaked", elapsed)
	}
}

// EOF is not success: a failing child still closes the pipe cleanly, so the
// error must come from Wait and the caller must be able to see it.
func TestRunWithObjectList_FailureIsReportedDespiteCleanEOF(t *testing.T) {
	script := fmt.Sprintf(`exec 9>%s
echo "repo/data/ab/cdef ok created" >&9
exit 3`, objectListChildPath())

	lines, err := runSh(t, script)
	if err == nil {
		t.Fatal("child exited 3 but runWithObjectList reported success")
	}
	// The partial list is still returned; the caller decides what to do with it.
	if len(lines) != 1 {
		t.Errorf("expected the partial line, got %q", lines)
	}
}

// A grandchild holding the write end must not hang the publish forever: the
// drain grace bounds it. Uses a background sleeper that outlives its parent.
func TestRunWithObjectList_GrandchildHoldingPipeIsBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("bounded by objectListDrainGrace")
	}
	script := fmt.Sprintf(`exec 9>%s
echo "repo/data/ab/cdef ok created" >&9
sleep 15 >&9 &
exit 0`, objectListChildPath())

	start := time.Now()
	lines, readToEOF, err := runShFull(t, script)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("command failed: %v", err)
	}
	if readToEOF {
		t.Error("EOF never arrived (a grandchild holds the pipe): want readToEOF=false")
	}
	if elapsed > objectListDrainGrace+15*time.Second {
		t.Errorf("took %v; the drain grace did not bound the wait", elapsed)
	}
	// Lower bound too: with an upper bound alone, shrinking the grace to the
	// 1s cancel value passes (and runs faster), so the constant that is
	// supposed to apply here would be unfalsifiable.
	if elapsed < objectListCancelGrace*2 {
		t.Errorf("returned in %v — the full drain grace was not applied", elapsed)
	}
	if len(lines) != 1 {
		t.Errorf("expected the line written before the sleeper, got %q", lines)
	}
}

// ExtraFiles already populated => refuse, because fd 3 would then belong to
// someone else and --object-list would point at the wrong pipe.
func TestRunWithObjectList_RefusesPrepopulatedExtraFiles(t *testing.T) {
	cmd := exec.Command("true")
	cmd.ExtraFiles = []*os.File{os.Stdin}
	_, err := runWithObjectList(context.Background(), cmd, testLogger(), func(string) {})
	if err == nil {
		t.Fatal("expected a refusal when ExtraFiles is already populated")
	}
	if !strings.Contains(err.Error(), "ExtraFiles") {
		t.Errorf("unhelpful error: %v", err)
	}
}

// S1 regression: a line longer than the scanner's buffer must NOT wedge the
// publish. Before the io.Copy drain, the reader stopped on ErrTooLong while
// the child kept writing, the pipe filled, and cmd.Wait() never returned —
// a hung publish holding the commit lock.
//
// NEGATIVE CONTROL: remove `defer io.Copy(io.Discard, pr)` and this hangs.
func TestRunWithObjectList_OverlongLineDoesNotWedgeThePublish(t *testing.T) {
	script := fmt.Sprintf(`exec 9>%s
head -c 2000000 /dev/zero | tr '\0' 'x' >&9
echo "" >&9
i=0; while [ $i -lt 3000 ]; do echo "repo/data/ab/cd ok created" >&9; i=$((i+1)); done`,
		objectListChildPath())

	done := make(chan struct{})
	var readToEOF bool
	var err error
	go func() { defer close(done); _, readToEOF, err = runShFull(t, script) }()

	select {
	case <-done:
	case <-time.After(90 * time.Second):
		t.Fatal("wedged: reader stopped while the child was still writing")
	}
	if err != nil {
		t.Fatalf("command failed: %v", err)
	}
	if readToEOF {
		t.Error("an over-long line truncated the list but it reported readToEOF")
	}
}

// A panic in the callback must not kill the daemon.
func TestRunWithObjectList_CallbackPanicIsContained(t *testing.T) {
	script := fmt.Sprintf(`exec 9>%s
echo "repo/data/ab/cd ok created" >&9`, objectListChildPath())
	cmd := exec.Command("sh", "-c", script)
	_, err := runWithObjectList(context.Background(), cmd, testLogger(), func(string) { panic("boom") })
	if err != nil {
		t.Fatalf("a panicking callback broke the publish: %v", err)
	}
}

// A nil callback is a programming error, refused rather than dereferenced.
func TestRunWithObjectList_NilCallbackRefused(t *testing.T) {
	if _, err := runWithObjectList(context.Background(), exec.Command("true"), testLogger(), nil); err == nil {
		t.Fatal("expected a refusal for a nil onLine")
	}
}
