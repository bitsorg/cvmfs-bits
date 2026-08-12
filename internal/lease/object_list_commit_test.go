// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package lease

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"cvmfs.io/prepub/pkg/observe"
)

// stubCvmfsServer puts a stub `cvmfs_server` first on PATH for this test. The
// stub records its argv and runs the supplied script, so the real Commit path
// — argv construction, the pipe, the drain, the logging branch — is exercised
// without a CVMFS installation.
func stubCvmfsServer(t *testing.T, script string) (argvFile string) {
	t.Helper()
	dir := t.TempDir()
	argvFile = filepath.Join(dir, "argv")
	stub := "#!/bin/sh\nprintf '%s\\n' \"$*\" > " + argvFile + "\n" + script + "\n"
	if err := os.WriteFile(filepath.Join(dir, "cvmfs_server"), []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return argvFile
}

func readArgv(t *testing.T, f string) string {
	t.Helper()
	b, err := os.ReadFile(f)
	if err != nil {
		t.Fatalf("stub never ran: %v", err)
	}
	return strings.TrimSpace(string(b))
}

// The publisher writes to the inherited pipe and prepub counts what it wrote.
func TestIngestCommit_CollectsObjectList(t *testing.T) {
	argvFile := stubCvmfsServer(t, `
case "$*" in
  *--object-list*)
    p=$(printf '%s\n' "$*" | tr ' ' '\n' | grep -A1 -- '--object-list' | tail -1)
    exec 9>"$p"
    echo "test.cvmfs.io/data/ab/cdef ok created" >&9
    echo "test.cvmfs.io/data/12/3456 ok present" >&9
    echo "test.cvmfs.io/data/78/9abc ok created" >&9
    ;;
esac
exit 0`)

	b := &IngestBackend{obs: newTestObs(t)}
	var lines []string
	out, readToEOF, err := b.cvmfsServerOutputWithObjectList(context.Background(),
		func(s string) { lines = append(lines, s) },
		"ingest", "-t", "/tmp/p.tar", "-b", "base",
		"--direct-s3", "--object-list", objectListChildPath(), "test.cvmfs.io")
	if err != nil {
		t.Fatalf("commit failed: %v (output %q)", err, out)
	}
	if !readToEOF {
		t.Error("clean EOF but readToEOF was false")
	}
	if len(lines) != 3 {
		t.Fatalf("collected %d lines, want 3: %q", len(lines), lines)
	}
	if !strings.HasSuffix(lines[1], "ok present") {
		t.Errorf("line mangled: %q", lines[1])
	}
	if argv := readArgv(t, argvFile); !strings.Contains(argv, "--object-list /proc/self/fd/3") {
		t.Errorf("stub did not receive the pipe path: %s", argv)
	}
}

// stdout/stderr capture must survive the switch from CombinedOutput to
// explicit Start/Wait — the error path quotes this output and is often the
// only diagnostic a failed publish leaves.
func TestIngestCommit_CapturesCombinedOutputWithList(t *testing.T) {
	stubCvmfsServer(t, `
echo "to stdout"
echo "to stderr" >&2
exit 7`)

	b := &IngestBackend{obs: newTestObs(t)}
	out, _, err := b.cvmfsServerOutputWithObjectList(context.Background(),
		func(string) {}, "ingest", "test.cvmfs.io")
	if err == nil {
		t.Fatal("exit 7 reported as success")
	}
	for _, want := range []string{"to stdout", "to stderr"} {
		if !strings.Contains(out, want) {
			t.Errorf("combined output lost %q: %q", want, out)
		}
	}
}

// A publisher that never opens the pipe (feature absent, or an older binary)
// must not hang or fail: zero lines, clean exit.
func TestIngestCommit_PublisherIgnoringThePipe(t *testing.T) {
	stubCvmfsServer(t, "exit 0")

	b := &IngestBackend{obs: newTestObs(t)}
	n := 0
	_, readToEOF, err := b.cvmfsServerOutputWithObjectList(context.Background(),
		func(string) { n++ }, "ingest", "test.cvmfs.io")
	if err != nil {
		t.Fatalf("unexpected failure: %v", err)
	}
	if n != 0 {
		t.Errorf("got %d lines from a publisher that wrote none", n)
	}
	if !readToEOF {
		t.Error("an empty list read to EOF is not truncated")
	}
}

// INERTNESS: Commit without ObjectList must not pass --object-list and must
// still take the CombinedOutput path.
func TestIngestCommit_NoObjectListFlagWhenDisabled(t *testing.T) {
	argvFile := stubCvmfsServer(t, "exit 0")

	b := &IngestBackend{obs: newTestObs(t)}
	if _, err := b.cvmfsServerOutput(context.Background(),
		b.commitArgs("test.cvmfs.io", "base", "/tmp/p.tar", true, false)...); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if argv := readArgv(t, argvFile); strings.Contains(argv, "object-list") {
		t.Errorf("object-list passed while disabled: %s", argv)
	}
}

// The real inertness assertion: with the feature off the child must not even
// RECEIVE the pipe. Asserting only on argv is too weak — a stub that ignores
// fd 3 behaves identically down both branches, so forcing the list branch
// unconditionally passed every other test in this file.
//
// SCOPE: this covers the two helpers, NOT Commit's choice between them —
// forcing that branch open does not fail this test. TestCommitObjectList_
// PipeAttachedOnlyWhenRequested drives Commit itself and does catch it.
func TestIngestCommit_NoPipeAttachedWhenDisabled(t *testing.T) {
	probe := func(t *testing.T) string {
		dir := t.TempDir()
		out := filepath.Join(dir, "fd3")
		stub := "#!/bin/sh\nif [ -e /proc/self/fd/3 ]; then echo yes > " + out +
			"; else echo no > " + out + "; fi\nexit 0\n"
		if err := os.WriteFile(filepath.Join(dir, "cvmfs_server"), []byte(stub), 0o755); err != nil {
			t.Fatal(err)
		}
		t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
		return out
	}

	t.Run("disabled", func(t *testing.T) {
		out := probe(t)
		b := &IngestBackend{obs: newTestObs(t)}
		if _, err := b.cvmfsServerOutput(context.Background(), "ingest", "test.cvmfs.io"); err != nil {
			t.Fatalf("commit failed: %v", err)
		}
		got, err := os.ReadFile(out)
		if err != nil {
			t.Fatalf("probe never ran: %v", err)
		}
		if strings.TrimSpace(string(got)) != "no" {
			t.Error("fd 3 was attached to a publish that did not ask for a list")
		}
	})

	t.Run("enabled", func(t *testing.T) {
		out := probe(t)
		b := &IngestBackend{obs: newTestObs(t)}
		if _, _, err := b.cvmfsServerOutputWithObjectList(context.Background(),
			func(string) {}, "ingest", "test.cvmfs.io"); err != nil {
			t.Fatalf("commit failed: %v", err)
		}
		got, _ := os.ReadFile(out)
		if strings.TrimSpace(string(got)) != "yes" {
			t.Error("the object-list publish did not receive fd 3")
		}
	})
}

// Commit's own branch: the pipe must be attached for exactly one of the four
// {ObjectList, DirectS3} combinations. Drives Commit end to end with a stub
// cvmfs_server, modelled on TestIngestBackend_DirectS3Flag.
//
// NEGATIVE CONTROL, verified: force the `req.ObjectList && req.DirectS3`
// branch in Commit to `true` and three of the four rows fail with
// "fd 3 attached = true, want false".
func TestCommitObjectList_PipeAttachedOnlyWhenRequested(t *testing.T) {
	for _, tc := range []struct {
		name               string
		objectList, dS3    bool
		wantFD3, wantInArg bool
	}{
		{"off", false, false, false, false},
		{"direct_s3 only", false, true, false, false},
		{"object_list only", true, false, false, false},
		{"both", true, true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			fd3 := filepath.Join(dir, "fd3")
			argv := filepath.Join(dir, "argv")
			stub := "#!/bin/sh\nprintf '%s\\n' \"$*\" >> " + argv +
				"\nif [ \"$1\" = ingest ]; then\n" +
				"  if [ -e /proc/self/fd/3 ]; then echo yes > " + fd3 +
				"; else echo no > " + fd3 + "; fi\nfi\nexit 0\n"
			if err := os.WriteFile(filepath.Join(dir, "cvmfs_server"), []byte(stub), 0o755); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

			repo := "test.cvmfs.io"
			b, mount := newAncestorBackend(t, repo)
			base := filepath.Join(mount, repo, "pkg")
			if err := os.MkdirAll(filepath.Dir(base), 0o755); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if err := b.Commit(context.Background(), CommitRequest{
				Token:      repo,
				TarPath:    oneEntryTar(t, t.TempDir()),
				CVMFSDir:   base,
				DirectS3:   tc.dS3,
				ObjectList: tc.objectList,
			}); err != nil {
				t.Fatalf("commit: %v", err)
			}

			got, err := os.ReadFile(fd3)
			if err != nil {
				t.Fatalf("stub never ran the ingest: %v", err)
			}
			if attached := strings.TrimSpace(string(got)) == "yes"; attached != tc.wantFD3 {
				t.Errorf("fd 3 attached = %v, want %v", attached, tc.wantFD3)
			}
			a, _ := os.ReadFile(argv)
			if inArg := strings.Contains(string(a), "--object-list"); inArg != tc.wantInArg {
				t.Errorf("--object-list in argv = %v, want %v\n  %s", inArg, tc.wantInArg, a)
			}
		})
	}
}

// A7: the exit-status gate. A publish that FAILS must never have its list
// treated as authoritative, even though a killed or failing publisher closes
// the pipe cleanly and so reads as a perfectly good EOF.
func TestObjectList_FailedPublishIsNotAuthoritative(t *testing.T) {
	stubCvmfsServer(t, `
case "$*" in
  *--object-list*)
    p=$(printf '%s\n' "$*" | tr ' ' '\n' | grep -A1 -- '--object-list' | tail -1)
    exec 9>"$p"
    echo "test.cvmfs.io/data/ab/cdef ok created" >&9
    ;;
esac
exit 4`)

	b := &IngestBackend{obs: newTestObs(t)}
	n := 0
	_, readToEOF, err := b.cvmfsServerOutputWithObjectList(context.Background(),
		func(string) { n++ },
		"ingest", "--object-list", objectListChildPath(), "test.cvmfs.io")

	if err == nil {
		t.Fatal("exit 4 reported as success")
	}
	// readToEOF is true — the pipe closed cleanly — which is exactly why it is
	// not sufficient on its own. Authoritative = readToEOF AND err == nil.
	if !readToEOF {
		t.Error("a failing publisher still closes the pipe: expected a clean EOF")
	}
	if n != 1 {
		t.Errorf("partial list should still be delivered, got %d lines", n)
	}
	// NOTE: no `readToEOF && err == nil` assertion here — err is provably
	// non-nil by the Fatal above, so it could never fire. The gate itself is
	// tested through Commit's log output in
	// TestCommitObjectList_VerdictIsLogged, which is where it actually lives.
}

// A cancelled publish must not pay the drain grace twice. Before the ctx-aware
// grace it was WaitDelay (10s) inside Wait plus a full 10s grace after it.
//
// NEGATIVE CONTROL: drop the `if ctx.Err() != nil` grace selection in
// runWithObjectList and this exceeds the bound.
func TestObjectList_CancelledPublishUnwindsOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("timing bound")
	}
	// A grandchild that escapes the process group and holds the write end, so
	// EOF never arrives and the grace is what ends the wait.
	//
	// The 2>/dev/null matters: without it the grandchild also holds the
	// stderr pipe os/exec created, so cmd.Wait() blocks on ITS copy goroutine
	// and the elapsed time measures the sleep instead of the grace. Dropping
	// it made this test report 5.01s against a 4s bound.
	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("setsid is required to detach the grandchild from the process group")
	}
	stubCvmfsServer(t, `
case "$*" in
  *--object-list*)
    p=$(printf '%s\n' "$*" | tr ' ' '\n' | grep -A1 -- '--object-list' | tail -1)
    exec 9>"$p"
    setsid sleep 5 >&9 2>/dev/null &
    ;;
esac
sleep 30
exit 0`)

	ctx, cancel := context.WithCancel(context.Background())
	b := &IngestBackend{obs: newTestObs(t)}
	go func() { time.Sleep(300 * time.Millisecond); cancel() }()

	start := time.Now()
	_, readToEOF, err := b.cvmfsServerOutputWithObjectList(ctx, func(string) {},
		"ingest", "--object-list", objectListChildPath(), "test.cvmfs.io")
	elapsed := time.Since(start)

	if err == nil {
		t.Error("a cancelled publish reported success")
	}
	if readToEOF {
		t.Error("the pipe never reached EOF; readToEOF must be false")
	}
	// The group kill reaps the child promptly, so Wait returns fast and the
	// GRACE is what dominates. Bound it just above objectListCancelGrace: with
	// the full objectListDrainGrace this is ~10s and fails, which is what makes
	// the ctx-aware selection falsifiable rather than merely asserted.
	if bound := objectListCancelGrace + 3*time.Second; elapsed > bound {
		t.Errorf("unwind took %v, want < %v — the cancel grace is not being used",
			elapsed, bound)
	}
}

// captureObs returns an observe.Provider whose logger records every record, so
// the A7 verdict can be asserted. Without this, Commit's authoritative/partial
// logging is unfalsifiable: hardcoding the verdict to true passes the suite.
type capturedLog struct {
	mu      sync.Mutex
	records []slog.Record
}

func (c *capturedLog) Enabled(context.Context, slog.Level) bool { return true }
func (c *capturedLog) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r.Clone())
	return nil
}
func (c *capturedLog) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *capturedLog) WithGroup(string) slog.Handler      { return c }

// find returns the attrs of the first record whose message contains want.
func (c *capturedLog) find(want string) (map[string]any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.records {
		if !strings.Contains(r.Message, want) {
			continue
		}
		m := map[string]any{}
		r.Attrs(func(a slog.Attr) bool { m[a.Key] = a.Value.Any(); return true })
		return m, true
	}
	return nil, false
}

func captureObs(t *testing.T) (*observe.Provider, *capturedLog) {
	t.Helper()
	obs := newTestObs(t)
	cap := &capturedLog{}
	obs.Logger = slog.New(cap)
	return obs, cap
}

// A7's gate, where it actually lives: Commit's log. Authoritative requires
// BOTH a clean EOF and a successful publish.
//
// NEGATIVE CONTROL, verified: hardcode "object_list_authoritative", true at
// ingest.go and the truncated row fails; delete the failure-path warn and the
// failed row fails.
func TestCommitObjectList_VerdictIsLogged(t *testing.T) {
	const line = `echo "test.cvmfs.io/data/ab/cdef ok created" >&9`
	for _, tc := range []struct {
		name              string
		body              string // shell, with fd 9 already open on the pipe
		wantMsg           string
		wantAuthoritative bool
		wantLines         int64
	}{
		{"clean publish", line + "\nexit 0", "published", true, 1},
		// An over-long line trips the scanner: the publish SUCCEEDS but the
		// reader stopped early, so the list is not authoritative. Uses the
		// scanner route rather than a grandchild because it is deterministic
		// and instant — a sleeping grandchild has to outlive the 10s grace,
		// and one that does not (my first attempt used 5s) releases the pipe
		// early and the case silently tests nothing.
		{"truncated list", line + "\nhead -c 2000000 /dev/zero | tr '\\0' 'x' >&9\nexit 0",
			"published", false, 1},
		// A failed publish: the pipe closes cleanly, but the revision does not
		// exist, so the list must never be presented as authoritative.
		{"failed publish", line + "\nexit 4", "object list is partial", false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			stub := "#!/bin/sh\ncase \"$*\" in\n  *--object-list*)\n" +
				"    p=$(printf '%s\\n' \"$*\" | tr ' ' '\\n' | grep -A1 -- '--object-list' | tail -1)\n" +
				"    exec 9>\"$p\"\n" + tc.body + "\n    ;;\nesac\nexit 0\n"
			if err := os.WriteFile(filepath.Join(dir, "cvmfs_server"), []byte(stub), 0o755); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

			obs, logs := captureObs(t)
			repo := "test.cvmfs.io"
			b, mount := newAncestorBackend(t, repo)
			b.obs = obs
			base := filepath.Join(mount, repo, "pkg")
			if err := os.MkdirAll(filepath.Dir(base), 0o755); err != nil {
				t.Fatal(err)
			}
			_ = b.Commit(context.Background(), CommitRequest{
				Token: repo, TarPath: oneEntryTar(t, t.TempDir()), CVMFSDir: base,
				DirectS3: true, ObjectList: true,
			})

			attrs, ok := logs.find(tc.wantMsg)
			if !ok {
				t.Fatalf("no log record containing %q", tc.wantMsg)
			}
			if got := attrs["object_list_authoritative"]; got != tc.wantAuthoritative {
				t.Errorf("object_list_authoritative = %v, want %v", got, tc.wantAuthoritative)
			}
			if got := attrs["object_list_lines"]; got != tc.wantLines {
				t.Errorf("object_list_lines = %v, want %v", got, tc.wantLines)
			}
		})
	}
}
