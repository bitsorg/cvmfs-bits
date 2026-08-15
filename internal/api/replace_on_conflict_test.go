// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/lease"
)

// realConflictErr reproduces (trimmed, otherwise verbatim) the commit error
// observed on the testbed on 2026-08-15 — prepub log, jobs e0adbb19 and the
// 170-job re-runs of 12:52Z and 16:2xZ. The remediation keys on the UNIQUE
// constraint marker inside it, so the test must use the real shape, not a
// convenient sentinel (see MEASUREMENTS.md §25 on fakes that diverge from the
// system they fake).
var realConflictErr = errors.New(`cvmfs_server ingest into "el9-x86_64/Packages/GCC-Toolchain/v14.2.0-alice2-3": exit status 1 (output: terminate called after throwing an instance of 'ECvmfsException'
  what():  PANIC: cvmfs/catalog_rw.cc : 168
failed to add '/el9-x86_64/Packages/GCC-Toolchain/v14.2.0-alice2-3/lib64/libgomp.so' (parent '/el9-x86_64/Packages/GCC-Toolchain/v14.2.0-alice2-3') to catalog '/el9-x86_64/Packages/GCC-Toolchain/v14.2.0-alice2-3': UNIQUE constraint failed: catalog.md5path_1, catalog.md5path_2
Aborted (core dumped)
Synchronization failed)`)

// replBackend implements lease.Backend plus DeleteSubtree, recording the call
// order the remediation makes.
type replBackend struct {
	calls      []string
	deleteErr  error
	acquireErr error
	commitErr  error // returned by Commit (the RETRY commit in these tests)
}

func (b *replBackend) Acquire(_ context.Context, repo, path string) (string, error) {
	b.calls = append(b.calls, "acquire")
	if b.acquireErr != nil {
		return "", b.acquireErr
	}
	return "retry-token", nil
}

func (b *replBackend) Heartbeat(_ context.Context, _ string, _ time.Duration, _ context.CancelFunc) func() {
	return func() {}
}

func (b *replBackend) Commit(_ context.Context, req lease.CommitRequest) error {
	b.calls = append(b.calls, "commit:"+req.Token)
	return b.commitErr
}

func (b *replBackend) Abort(_ context.Context, _ string) error {
	b.calls = append(b.calls, "abort")
	return nil
}

func (b *replBackend) NeedsPipeline() bool           { return false }
func (b *replBackend) Probe(_ context.Context) error { return nil }

func (b *replBackend) DeleteSubtree(_ context.Context, repo, rel string) error {
	b.calls = append(b.calls, "delete:"+rel)
	return b.deleteErr
}

// plainBackend is a lease.Backend that can NOT delete a subtree.
type plainBackend struct{ replBackend }

// stubPathExists swaps the package seam, restoring the ORIGINAL on cleanup
// (captured before the swap — capturing after restores the stub itself, a bug
// this repo has met before).
func stubPathExists(t *testing.T, exists bool, err error) {
	t.Helper()
	real := pathExistsFn
	pathExistsFn = func(_ context.Context, _ *http.Client, _, _, _ string) (bool, error) {
		return exists, err
	}
	t.Cleanup(func() { pathExistsFn = real })
}

func replOrch(t *testing.T, b lease.Backend, flagOn bool) *Orchestrator {
	t.Helper()
	o, _ := minimalOrch(t, b)
	o.Stratum0URL = "http://stratum0.test"
	o.ReplaceOnConflict = flagOn
	return o
}

func replJob() *job.Job {
	return &job.Job{ID: "j1", Repo: "test-repo.example.com",
		Path: "el9-x86_64/Packages/GCC-Toolchain/v14.2.0-alice2-3"}
}

func TestReplaceOnConflict_ReplacesAndRetriesOnce(t *testing.T) {
	b := &replBackend{}
	o := replOrch(t, b, true)
	stubPathExists(t, true, nil)
	req := &lease.CommitRequest{Token: "orig-token"}

	attempted, err := o.replaceOnConflict(context.Background(), replJob(), req,
		realConflictErr, o.Obs.Logger)
	if !attempted || err != nil {
		t.Fatalf("want (true, nil), got (%v, %v)", attempted, err)
	}
	want := []string{
		"delete:el9-x86_64/Packages/GCC-Toolchain/v14.2.0-alice2-3",
		"acquire",
		"commit:retry-token",
	}
	if strings.Join(b.calls, "|") != strings.Join(want, "|") {
		t.Errorf("call order = %v, want %v", b.calls, want)
	}
	if req.Token != "retry-token" {
		t.Errorf("req.Token = %q — the retry must not reuse the released token", req.Token)
	}
}

// NEGATIVE CONTROL: with the flag off nothing is deleted and nothing retried,
// whatever the error looks like. Remove the flag check from replaceOnConflict
// and this fails.
func TestReplaceOnConflict_FlagOffDoesNothing(t *testing.T) {
	b := &replBackend{}
	o := replOrch(t, b, false)
	stubPathExists(t, true, nil)

	attempted, err := o.replaceOnConflict(context.Background(), replJob(),
		&lease.CommitRequest{}, realConflictErr, o.Obs.Logger)
	if attempted || err != nil {
		t.Fatalf("want (false, nil), got (%v, %v)", attempted, err)
	}
	if len(b.calls) != 0 {
		t.Errorf("backend was touched with the flag off: %v", b.calls)
	}
}

// NEGATIVE CONTROL: a failure that is not conflict-shaped must never delete,
// even with the flag on and the path genuinely occupied — network and spool
// errors are not licences to destroy published state.
func TestReplaceOnConflict_NonConflictErrorDoesNothing(t *testing.T) {
	b := &replBackend{}
	o := replOrch(t, b, true)
	stubPathExists(t, true, nil)

	attempted, err := o.replaceOnConflict(context.Background(), replJob(),
		&lease.CommitRequest{}, errors.New("gateway: connection refused"),
		o.Obs.Logger)
	if attempted || err != nil {
		t.Fatalf("want (false, nil), got (%v, %v)", attempted, err)
	}
	if len(b.calls) != 0 {
		t.Errorf("backend was touched for a non-conflict error: %v", b.calls)
	}
}

// The error string alone is not evidence: when the published catalogs do NOT
// have the path, or cannot answer, no deletion happens and the original error
// stands.
func TestReplaceOnConflict_UnconfirmedPathDoesNothing(t *testing.T) {
	for name, tc := range map[string]struct {
		exists bool
		err    error
	}{
		"absent":       {exists: false, err: nil},
		"inconclusive": {exists: false, err: errors.New("stratum0 unreachable")},
	} {
		t.Run(name, func(t *testing.T) {
			b := &replBackend{}
			o := replOrch(t, b, true)
			stubPathExists(t, tc.exists, tc.err)

			attempted, err := o.replaceOnConflict(context.Background(), replJob(),
				&lease.CommitRequest{}, realConflictErr, o.Obs.Logger)
			if attempted || err != nil {
				t.Fatalf("want (false, nil), got (%v, %v)", attempted, err)
			}
			if len(b.calls) != 0 {
				t.Errorf("backend was touched: %v", b.calls)
			}
		})
	}
}

func TestReplaceOnConflict_BackendWithoutDeleteLeavesErrorTerminal(t *testing.T) {
	b := &plainBackend{}
	// Hand the orchestrator ONLY the lease.Backend surface: a type assertion
	// inside replaceOnConflict must not find DeleteSubtree.
	var iface lease.Backend = backendOnly{b}
	o := replOrch(t, iface, true)
	stubPathExists(t, true, nil)

	attempted, err := o.replaceOnConflict(context.Background(), replJob(),
		&lease.CommitRequest{}, realConflictErr, o.Obs.Logger)
	if attempted || err != nil {
		t.Fatalf("want (false, nil), got (%v, %v)", attempted, err)
	}
	if len(b.calls) != 0 {
		t.Errorf("backend was touched: %v", b.calls)
	}
}

// backendOnly hides every method except the lease.Backend interface, so the
// embedded type's DeleteSubtree is not reachable by assertion.
type backendOnly struct{ b *plainBackend }

func (w backendOnly) Acquire(ctx context.Context, repo, path string) (string, error) {
	return w.b.Acquire(ctx, repo, path)
}
func (w backendOnly) Heartbeat(_ context.Context, _ string, _ time.Duration, _ context.CancelFunc) func() {
	return func() {}
}
func (w backendOnly) Commit(ctx context.Context, req lease.CommitRequest) error {
	return w.b.Commit(ctx, req)
}
func (w backendOnly) Abort(ctx context.Context, token string) error { return w.b.Abort(ctx, token) }
func (w backendOnly) NeedsPipeline() bool                           { return false }
func (w backendOnly) Probe(ctx context.Context) error               { return nil }

func TestReplaceOnConflict_DeleteFailureCarriesBothErrors(t *testing.T) {
	b := &replBackend{deleteErr: errors.New("cvmfs_server ingest -f: exit status 1")}
	o := replOrch(t, b, true)
	stubPathExists(t, true, nil)

	attempted, err := o.replaceOnConflict(context.Background(), replJob(),
		&lease.CommitRequest{}, realConflictErr, o.Obs.Logger)
	if !attempted || err == nil {
		t.Fatalf("want (true, err), got (%v, %v)", attempted, err)
	}
	for _, needle := range []string{"UNIQUE constraint", "ingest -f"} {
		if !strings.Contains(err.Error(), needle) {
			t.Errorf("error %q does not carry %q", err, needle)
		}
	}
	if strings.Contains(strings.Join(b.calls, "|"), "commit") {
		t.Errorf("commit was retried after a failed delete: %v", b.calls)
	}
}

func TestReplaceOnConflict_RetryFailureNamesTheAbsentPath(t *testing.T) {
	b := &replBackend{commitErr: errors.New("gateway: commit_lease failed")}
	o := replOrch(t, b, true)
	stubPathExists(t, true, nil)

	attempted, err := o.replaceOnConflict(context.Background(), replJob(),
		&lease.CommitRequest{}, realConflictErr, o.Obs.Logger)
	if !attempted || err == nil {
		t.Fatalf("want (true, err), got (%v, %v)", attempted, err)
	}
	// The subtree is gone and the retry failed: the operator must be told the
	// path is now absent, not left to infer it.
	if !strings.Contains(err.Error(), "ABSENT") {
		t.Errorf("error %q does not state the path is now absent", err)
	}
}

// ── Run()-level wiring ────────────────────────────────────────────────────────

// runBackend is a mockBackend that ALSO deletes subtrees, and whose Commit
// fails the first time with the real conflict error and succeeds after.
// It is the whole point of the feature seen from the outside: a job whose
// first commit hits an occupied path must still reach StatePublished.
type runBackend struct {
	mu       sync.Mutex
	commits  int
	deletes  int
	failEach bool // when true, EVERY commit fails (delete cannot rescue it)
}

func (b *runBackend) Acquire(_ context.Context, _, _ string) (string, error) {
	return "tok", nil
}
func (b *runBackend) Heartbeat(_ context.Context, _ string, _ time.Duration, _ context.CancelFunc) func() {
	return func() {}
}
func (b *runBackend) Commit(_ context.Context, _ lease.CommitRequest) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.commits++
	if b.failEach || b.commits == 1 {
		return realConflictErr
	}
	return nil
}
func (b *runBackend) Abort(_ context.Context, _ string) error { return nil }
func (b *runBackend) NeedsPipeline() bool                     { return false }
func (b *runBackend) Probe(_ context.Context) error           { return nil }
func (b *runBackend) DeleteSubtree(_ context.Context, _, _ string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deletes++
	return nil
}

// The wiring test the direct unit tests cannot give: a conflict-shaped commit
// failure carries the job all the way to StatePublished, with the lease token
// cleared exactly as a first-try success leaves it.
//
// NEGATIVE CONTROL: with ReplaceOnConflict false (below) the same job fails.
func TestRun_ConflictIsReplacedAndJobPublishes(t *testing.T) {
	b := &runBackend{}
	o, sp := minimalOrch(t, b)
	o.Stratum0URL = "http://stratum0.test"
	o.ReplaceOnConflict = true
	stubPathExists(t, true, nil)
	j := newIncomingJob(t, sp)

	if err := o.Run(context.Background(), j, nil); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if j.State != job.StatePublished {
		t.Errorf("state = %q, want %q", j.State, job.StatePublished)
	}
	if j.LeaseToken != "" {
		t.Errorf("LeaseToken = %q, want cleared", j.LeaseToken)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.deletes != 1 || b.commits != 2 {
		t.Errorf("deletes=%d commits=%d, want 1 and 2 (delete then retry once)",
			b.deletes, b.commits)
	}
}

func TestRun_ConflictWithFlagOffFailsTheJob(t *testing.T) {
	b := &runBackend{}
	o, sp := minimalOrch(t, b)
	o.Stratum0URL = "http://stratum0.test"
	o.ReplaceOnConflict = false
	stubPathExists(t, true, nil)
	j := newIncomingJob(t, sp)

	if err := o.Run(context.Background(), j, nil); err == nil {
		t.Fatal("Run succeeded with the flag off; want the conflict to be terminal")
	}
	if j.State != job.StateFailed {
		t.Errorf("state = %q, want %q", j.State, job.StateFailed)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.deletes != 0 {
		t.Errorf("deletes=%d with the flag off; want 0", b.deletes)
	}
}

// A retry that fails again must abort the job — and must not loop.
func TestRun_RetryFailureAbortsWithoutLooping(t *testing.T) {
	b := &runBackend{failEach: true}
	o, sp := minimalOrch(t, b)
	o.Stratum0URL = "http://stratum0.test"
	o.ReplaceOnConflict = true
	stubPathExists(t, true, nil)
	j := newIncomingJob(t, sp)

	if err := o.Run(context.Background(), j, nil); err == nil {
		t.Fatal("Run succeeded though every commit failed")
	}
	if j.State != job.StateFailed {
		t.Errorf("state = %q, want %q", j.State, job.StateFailed)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.commits != 2 || b.deletes != 1 {
		t.Errorf("commits=%d deletes=%d, want exactly 2 and 1 (retry is once, not a loop)",
			b.commits, b.deletes)
	}
}

// The acquire-failure branch: the subtree is already gone, so the operator
// must be told the path is absent rather than left with a bare lease error.
func TestReplaceOnConflict_AcquireFailureSaysThePathIsAbsent(t *testing.T) {
	b := &replBackend{acquireErr: errors.New("gateway: path_busy")}
	o := replOrch(t, b, true)
	stubPathExists(t, true, nil)

	attempted, err := o.replaceOnConflict(context.Background(), replJob(),
		&lease.CommitRequest{}, realConflictErr, o.Obs.Logger)
	if !attempted || err == nil {
		t.Fatalf("want (true, err), got (%v, %v)", attempted, err)
	}
	if !strings.Contains(err.Error(), "ABSENT") {
		t.Errorf("error %q does not state the path is now absent", err)
	}
	if strings.Contains(strings.Join(b.calls, "|"), "commit") {
		t.Errorf("commit was attempted after a failed acquire: %v", b.calls)
	}
}
