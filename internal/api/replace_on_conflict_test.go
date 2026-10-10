// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"errors"
	"fmt"
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
// 170-job re-runs of 12:52Z and 16:2xZ. It is the commit failure after which
// nothing may be deleted, in its real shape rather than a convenient sentinel.
var realConflictErr = errors.New(`cvmfs_server ingest into "el9-x86_64/Packages/GCC-Toolchain/v14.2.0-alice2-3": exit status 1 (output: terminate called after throwing an instance of 'ECvmfsException'
  what():  PANIC: cvmfs/catalog_rw.cc : 168
failed to add '/el9-x86_64/Packages/GCC-Toolchain/v14.2.0-alice2-3/lib64/libgomp.so' (parent '/el9-x86_64/Packages/GCC-Toolchain/v14.2.0-alice2-3') to catalog '/el9-x86_64/Packages/GCC-Toolchain/v14.2.0-alice2-3': UNIQUE constraint failed: catalog.md5path_1, catalog.md5path_2
Aborted (core dumped)
Synchronization failed)`)

// replBackend implements lease.Backend plus DeleteSubtree, recording the call
// order the replacement makes.
type replBackend struct {
	calls      []string
	deleteErr  error
	acquireErr error
	commitErr  error // returned by Commit
	abortErr   error // returned by Abort (the pre-delete lease release)
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

func (b *replBackend) Abort(_ context.Context, token string) error {
	b.calls = append(b.calls, "abort:"+token)
	return b.abortErr
}

func (b *replBackend) NeedsPipeline() bool           { return false }
func (b *replBackend) Probe(_ context.Context) error { return nil }

func (b *replBackend) DeleteSubtree(_ context.Context, repo, rel string) error {
	b.calls = append(b.calls, "delete:"+rel)
	return b.deleteErr
}

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
	const p = "el9-x86_64/Packages/GCC-Toolchain/v14.2.0-alice2-3"
	return &job.Job{ID: "j1", Repo: "test-repo.example.com", Path: p,
		IdentityPath: p, IdentityHash: "this-build", Replace: true}
}

// The open lease is released BEFORE the delete: the delete takes the
// repository's slot (ingest) or a gateway lease on the same path (staged), so
// a lease still held would block it. Then a fresh lease for the commit, and a
// new nested catalog, since the old one went with the subtree.
//
// NEGATIVE CONTROL: remove the pre-delete Abort in replaceFirst and this
// fails — "abort" no longer precedes "delete" in the recorded call order.
func TestReplaceFirst_ReleasesDeletesAndReacquires(t *testing.T) {
	b := &replBackend{}
	o := replOrch(t, b, true)
	j := replJob()
	j.LeaseToken = "orig-lease"
	req := &lease.CommitRequest{Token: "orig-lease", BaseExists: true}

	if err := o.replaceFirst(context.Background(), j, req, o.Obs.Logger); err != nil {
		t.Fatalf("replaceFirst: %v", err)
	}
	want := []string{
		"abort:orig-lease",
		"delete:el9-x86_64/Packages/GCC-Toolchain/v14.2.0-alice2-3",
		"acquire",
	}
	if strings.Join(b.calls, "|") != strings.Join(want, "|") {
		t.Errorf("call order = %v, want %v", b.calls, want)
	}
	if j.LeaseToken != "retry-token" || req.Token != "retry-token" {
		t.Errorf("tokens job=%q req=%q, want the re-acquired one", j.LeaseToken, req.Token)
	}
	if req.BaseExists {
		t.Error("BaseExists still true: the commit must create the nested catalog again")
	}
}

// If the lease cannot be released, the delete would be blocked anyway: fail
// with a legible error and do NOT delete a subtree we cannot then republish.
// The token is kept so the release can be retried.
func TestReplaceFirst_LeaseReleaseFailureDoesNotDelete(t *testing.T) {
	b := &replBackend{abortErr: errors.New("gateway: 503 releasing lease")}
	o := replOrch(t, b, true)
	j := replJob()
	j.LeaseToken = "orig-lease"

	if err := o.replaceFirst(context.Background(), j, &lease.CommitRequest{}, o.Obs.Logger); err == nil {
		t.Fatal("want an error")
	}
	if strings.Contains(strings.Join(b.calls, "|"), "delete") {
		t.Errorf("deleted despite a failed lease release: %v", b.calls)
	}
	if j.LeaseToken != "orig-lease" {
		t.Errorf("LeaseToken = %q, want it retained after a failed release", j.LeaseToken)
	}
}

// backendOnly hides every method except the lease.Backend interface, so the
// embedded type's DeleteSubtree is not reachable by assertion.
type backendOnly struct{ b *replBackend }

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

// unsupportedDeleter implements the capability but cannot do the work here --
// the shape StagedBackend has when this prepub offers no ingest path.
type unsupportedDeleter struct {
	replBackend
	deletes int
}

func (b *unsupportedDeleter) DeleteSubtree(_ context.Context, _, _ string) error {
	b.deletes++
	return fmt.Errorf("wrapped: %w", lease.ErrSubtreeDeleteUnsupported)
}

// A backend that cannot delete, by type or in this deployment, fails the job
// permanently: retrying would only decline again.
func TestReplaceFirst_CannotDeleteIsPermanent(t *testing.T) {
	plain := &replBackend{}
	for name, b := range map[string]lease.Backend{
		"no DeleteSubtree": backendOnly{plain},
		"unsupported here": &unsupportedDeleter{},
	} {
		t.Run(name, func(t *testing.T) {
			o := replOrch(t, b, true)
			err := o.replaceFirst(context.Background(), replJob(), &lease.CommitRequest{}, o.Obs.Logger)
			if err == nil || ClassOf(err) != ErrClassPermanent {
				t.Fatalf("err = %v (class %v), want a permanent error", err, ClassOf(err))
			}
		})
	}
	if len(plain.calls) != 0 {
		t.Errorf("a backend without DeleteSubtree was touched: %v", plain.calls)
	}
}

func TestReplaceFirst_DeleteFailureNamesThePath(t *testing.T) {
	b := &replBackend{deleteErr: errors.New("cvmfs_server ingest -f: exit status 1")}
	o := replOrch(t, b, true)

	err := o.replaceFirst(context.Background(), replJob(), &lease.CommitRequest{}, o.Obs.Logger)
	if err == nil {
		t.Fatal("want an error")
	}
	for _, needle := range []string{"GCC-Toolchain", "ingest -f"} {
		if !strings.Contains(err.Error(), needle) {
			t.Errorf("error %q does not carry %q", err, needle)
		}
	}
	if strings.Contains(strings.Join(b.calls, "|"), "acquire") {
		t.Errorf("re-acquired after a failed delete: %v", b.calls)
	}
}

// The subtree is already gone when the re-acquire fails: say the path is absent.
func TestReplaceFirst_AcquireFailureSaysThePathIsAbsent(t *testing.T) {
	b := &replBackend{acquireErr: errors.New("gateway: path_busy")}
	o := replOrch(t, b, true)

	err := o.replaceFirst(context.Background(), replJob(), &lease.CommitRequest{}, o.Obs.Logger)
	if err == nil || !strings.Contains(err.Error(), "ABSENT") {
		t.Errorf("error %v does not state the path is now absent", err)
	}
}

// ── Run()-level wiring ────────────────────────────────────────────────────────

// runBackend counts commits and deletes; with failEach every commit fails
// with the real conflict error.
type runBackend struct {
	mu       sync.Mutex
	commits  int
	deletes  int
	failEach bool
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
	if b.failEach {
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

func (b *runBackend) counts() (commits, deletes int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.commits, b.deletes
}

// runReplace runs one job through Run on a node that allows replacing, with
// publishedHash at the job's path (found=false when empty).
func runReplace(t *testing.T, b *runBackend, publishedHash string, jobAsks bool) (*job.Job, error) {
	t.Helper()
	o, sp := minimalOrch(t, b)
	o.Stratum0URL = "http://stratum0.test"
	o.ReplaceOnConflict = true
	stubPathExists(t, true, nil)
	stubHash(t, publishedHash, publishedHash != "")
	j := newIncomingJob(t, sp)
	j.IdentityPath, j.IdentityHash, j.Replace = j.Path, "this-build", jobAsks
	if err := sp.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	return j, o.Run(context.Background(), j, nil)
}

// Replace if different, end to end: another build's hash at the job's own
// path is deleted BEFORE the commit, so the job publishes with one commit.
func TestRun_DifferentHashIsReplacedBeforeTheCommit(t *testing.T) {
	b := &runBackend{}
	j, err := runReplace(t, b, "other-build", true)
	if err != nil || j.State != job.StatePublished {
		t.Fatalf("Run: %v, state %q; want published", err, j.State)
	}
	if c, d := b.counts(); d != 1 || c != 1 {
		t.Errorf("deletes=%d commits=%d, want 1 and 1 (delete, then one commit)", d, c)
	}
}

// Nothing is deleted unless the content is recognisably another build's: the
// same hash skips; no readable hash (a shared root, or no .meta.json) and a
// job that did not ask both fail.
//
// NEGATIVE CONTROL: replace on !found in preCommitChecks and the "no hash"
// case deletes once.
func TestRun_ReplaceDeletesOnlyAnotherBuildsContent(t *testing.T) {
	for _, tc := range []struct {
		name      string
		published string
		asks      bool
		wantState job.State
	}{
		{"same hash skips", "this-build", true, job.StatePublished},
		{"no published hash fails", "", true, job.StateFailed},
		{"job did not ask fails", "other-build", false, job.StateFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &runBackend{}
			j, _ := runReplace(t, b, tc.published, tc.asks)
			if j.State != tc.wantState {
				t.Errorf("state = %q, want %q", j.State, tc.wantState)
			}
			if c, d := b.counts(); d != 0 || c != 0 {
				t.Errorf("deletes=%d commits=%d, want 0 and 0", d, c)
			}
		})
	}
}

// A conflicting commit is never followed by a delete, whatever the node
// allows: replacement is decided before the commit or not at all.
func TestRun_ConflictFailsTheJobAndDeletesNothing(t *testing.T) {
	for name, asks := range map[string]bool{"job asks": true, "job does not ask": false} {
		t.Run(name, func(t *testing.T) {
			b := &runBackend{failEach: true}
			o, sp := minimalOrch(t, b)
			o.Stratum0URL = "http://stratum0.test"
			o.ReplaceOnConflict = true
			stubPathExists(t, true, nil)
			j := newIncomingJob(t, sp)
			j.Replace = asks

			if err := o.Run(context.Background(), j, nil); err == nil {
				t.Fatal("Run succeeded; want the conflict to fail the job")
			}
			if c, d := b.counts(); d != 0 || c != 1 {
				t.Errorf("deletes=%d commits=%d, want 0 and 1", d, c)
			}
		})
	}
}

// A replacement whose commit then fails is not deleted again.
func TestRun_ReplacedFirstIsNotRetriedAgain(t *testing.T) {
	b := &runBackend{failEach: true}
	if _, err := runReplace(t, b, "other-build", true); err == nil {
		t.Fatal("Run succeeded though the commit failed")
	}
	if c, d := b.counts(); d != 1 || c != 1 {
		t.Errorf("deletes=%d commits=%d, want 1 and 1", d, c)
	}
}
