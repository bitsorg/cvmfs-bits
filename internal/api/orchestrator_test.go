package api

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/lease"
	"cvmfs.io/prepub/internal/notify"
	"cvmfs.io/prepub/internal/spool"
	"cvmfs.io/prepub/pkg/observe"
)

// ── mock lease.Backend ────────────────────────────────────────────────────────

// mockBackend is a controllable lease.Backend for orchestrator unit tests.
type mockBackend struct {
	// needsPipeline controls NeedsPipeline().
	needsPipeline bool

	// acquireToken is returned by Acquire.  acquireErr overrides with an error.
	acquireToken string
	acquireErr   error

	// commitErr is returned by Commit.
	commitErr error

	// abortCalls counts the number of times Abort is called.
	abortCalls int32

	// probeFn, if non-nil, is called by Probe instead of returning nil.
	probeFn func(ctx context.Context) error
}

func (m *mockBackend) Acquire(_ context.Context, _, _ string) (string, error) {
	if m.acquireErr != nil {
		return "", m.acquireErr
	}
	return m.acquireToken, nil
}

func (m *mockBackend) Heartbeat(_ context.Context, _ string, _ time.Duration, _ context.CancelFunc) func() {
	return func() {}
}

func (m *mockBackend) Commit(_ context.Context, _ lease.CommitRequest) error {
	return m.commitErr
}

func (m *mockBackend) Abort(_ context.Context, _ string) error {
	atomic.AddInt32(&m.abortCalls, 1)
	return nil
}

func (m *mockBackend) NeedsPipeline() bool { return m.needsPipeline }

func (m *mockBackend) Probe(ctx context.Context) error {
	if m.probeFn != nil {
		return m.probeFn(ctx)
	}
	return nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newOrchTestObs(t *testing.T) *observe.Provider {
	t.Helper()
	obs, shutdown, err := observe.New("orch-test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)
	return obs
}

// minimalOrch returns an Orchestrator wired with the given backend, a temp
// spool, and no CAS / distribution.
func minimalOrch(t *testing.T, backend lease.Backend) (*Orchestrator, *spool.Spool) {
	t.Helper()
	obs := newOrchTestObs(t)
	sp, err := spool.New(t.TempDir(), obs)
	if err != nil {
		t.Fatalf("spool.New: %v", err)
	}
	o := &Orchestrator{
		Spool:  sp,
		Lease:  backend,
		Notify: notify.NewBus(),
		Obs:    obs,
	}
	return o, sp
}

// testJobCounter provides unique job IDs across parallel sub-tests without
// relying on wall-clock time, which can collide at nanosecond resolution.
var testJobCounter int64

// newIncomingJob creates a job in StateIncoming and writes it into the spool.
// TarPath points to a real (empty) file so that orchestrator code that
// stats/reads the path does not error before reaching the mock backend.
func newIncomingJob(t *testing.T, sp *spool.Spool) *job.Job {
	t.Helper()

	// Create an empty tar file for the job.
	tmp, err := os.CreateTemp(t.TempDir(), "test-*.tar")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	tmp.Close()

	id := atomic.AddInt64(&testJobCounter, 1)
	j := &job.Job{
		ID:      fmt.Sprintf("test-job-%d", id),
		Repo:    "test-repo.example.com",
		Path:    "atlas/24.0",
		State:   job.StateIncoming,
		TarPath: tmp.Name(),
	}
	if err := sp.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	return j
}

// ── abort-count regression tests ─────────────────────────────────────────────

// TestOrchestrator_AbortOnceOnCommitError verifies that when Commit returns an
// error, Abort is called exactly once.
//
// Before the double-abort fix the orchestrator called o.Lease.Abort explicitly
// in the commit-error path and then called abortJob, which also called
// o.Lease.Abort because j.LeaseToken was still set.  This caused two abort
// calls: a spurious cvmfs_server abort / gateway DELETE on an already-released
// token.  After the fix only abortJob calls Abort.
func TestOrchestrator_AbortOnceOnCommitError(t *testing.T) {
	backend := &mockBackend{
		needsPipeline: false,
		acquireToken:  "test-repo.example.com",
		// Commit returns an error that triggers the abort path.
		commitErr: errors.New("injected commit error"),
	}
	o, sp := minimalOrch(t, backend)
	j := newIncomingJob(t, sp)

	ctx := context.Background()
	_ = o.Run(ctx, j)

	abortCount := atomic.LoadInt32(&backend.abortCalls)
	if abortCount != 1 {
		t.Errorf("Abort called %d times; want exactly 1 (double-abort regression)", abortCount)
	}
}

// TestOrchestrator_NoAbortWhenAcquireFails verifies that Abort is NOT called
// when Acquire fails — there is no transaction to abort.
func TestOrchestrator_NoAbortWhenAcquireFails(t *testing.T) {
	backend := &mockBackend{
		needsPipeline: false,
		acquireErr:    errors.New("injected acquire error"),
	}
	o, sp := minimalOrch(t, backend)
	j := newIncomingJob(t, sp)

	_ = o.Run(context.Background(), j)

	if got := atomic.LoadInt32(&backend.abortCalls); got != 0 {
		t.Errorf("Abort called %d times after Acquire failure; want 0", got)
	}
}

// TestOrchestrator_AbortCalledOnCommitError verifies that when Commit returns
// an error (other than ErrCommittedNotRemounted), Abort is called exactly once.
func TestOrchestrator_AbortCalledOnCommitError(t *testing.T) {
	backend := &mockBackend{
		needsPipeline: false,
		acquireToken:  "repo",
		commitErr:     fmt.Errorf("disk full"),
	}
	o, sp := minimalOrch(t, backend)
	j := newIncomingJob(t, sp)

	_ = o.Run(context.Background(), j)

	if got := atomic.LoadInt32(&backend.abortCalls); got != 1 {
		t.Errorf("Abort called %d times after commit error; want 1", got)
	}
}

// TestOrchestrator_NoAbortOnErrCommittedNotRemounted verifies that when Commit
// returns ErrCommittedNotRemounted the orchestrator does NOT abort — the catalog
// is already committed and cannot be rolled back.
func TestOrchestrator_NoAbortOnErrCommittedNotRemounted(t *testing.T) {
	backend := &mockBackend{
		needsPipeline: false,
		acquireToken:  "repo",
		commitErr:     lease.ErrCommittedNotRemounted,
	}
	o, sp := minimalOrch(t, backend)
	j := newIncomingJob(t, sp)

	err := o.Run(context.Background(), j)
	// ErrCommittedNotRemounted should NOT propagate as a job failure.
	if err != nil {
		t.Errorf("Run returned error %v; want nil (committed-not-remounted is treated as published)", err)
	}

	if got := atomic.LoadInt32(&backend.abortCalls); got != 0 {
		t.Errorf("Abort called %d times after ErrCommittedNotRemounted; want 0", got)
	}
}

// ── nil CAS guard test ────────────────────────────────────────────────────────

// TestOrchestrator_NilCASGuard_GatewayMode verifies that an Orchestrator in
// gateway mode (NeedsPipeline==true) with a nil CAS backend fails gracefully
// rather than panicking with a nil pointer dereference.
func TestOrchestrator_NilCASGuard_GatewayMode(t *testing.T) {
	backend := &mockBackend{
		needsPipeline: true, // gateway mode — CAS would be required
		acquireToken:  "tok",
	}
	// CAS is left nil intentionally — this is the misconfiguration we're guarding.
	o, sp := minimalOrch(t, backend)
	j := newIncomingJob(t, sp)

	err := o.Run(context.Background(), j)
	if err == nil {
		t.Fatal("expected error for nil CAS in gateway mode, got nil")
	}
	if j.State != job.StateFailed {
		t.Errorf("job state: got %q, want %q", j.State, job.StateFailed)
	}
	// The guard must fire before Acquire is attempted — no token, no abort.
	if got := atomic.LoadInt32(&backend.abortCalls); got != 0 {
		t.Errorf("Abort called %d times; want 0 (guard should fire before acquire)", got)
	}
}

// TestOrchestrator_NilCAS_LocalMode verifies that a nil CAS is harmless in
// local mode (NeedsPipeline==false) — the CAS is never accessed.
func TestOrchestrator_NilCAS_LocalMode(t *testing.T) {
	backend := &mockBackend{
		needsPipeline: false,
		acquireToken:  "repo",
		// Commit succeeds — in a real deployment Commit would call cvmfs_server.
	}
	o, sp := minimalOrch(t, backend)
	// CAS is nil — should be fine in local mode.

	j := newIncomingJob(t, sp)
	// We expect Run to reach StatePublished without panicking.
	err := o.Run(context.Background(), j)
	if err != nil {
		t.Errorf("unexpected error in local mode with nil CAS: %v", err)
	}
	if j.State != job.StatePublished {
		t.Errorf("job state: got %q, want %q", j.State, job.StatePublished)
	}
}
