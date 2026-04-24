package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/notify"
	"cvmfs.io/prepub/internal/spool"
	"cvmfs.io/prepub/pkg/observe"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func newTestServer(t *testing.T) (*Server, *spool.Spool, *Orchestrator) {
	t.Helper()
	dir := t.TempDir()
	obs, shutdown, err := observe.New("test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)
	sp, err := spool.New(dir, obs)
	if err != nil {
		t.Fatalf("spool.New: %v", err)
	}
	nb := notify.NewBus()
	orch := &Orchestrator{Spool: sp, Notify: nb, Obs: obs}
	srv := New(obs, "" /*no auth*/, orch, sp, nb, dir)
	return srv, sp, orch
}

// withMuxVars injects gorilla/mux route variables into a test request so that
// handlers invoked directly (not via the router) can read them via mux.Vars().
func withMuxVars(r *http.Request, vars map[string]string) *http.Request {
	return setMuxVars(r, vars) // implemented in server_muxvars_test.go
}

// ── Fix #2: job goroutine WaitGroup ──────────────────────────────────────────

// TestShutdown_WaitsForInFlightJob verifies that Shutdown blocks until all
// background job goroutines finish (Fix #2).
func TestShutdown_WaitsForInFlightJob(t *testing.T) {
	srv, _, _ := newTestServer(t)

	jobDone := make(chan struct{})
	srv.jobWg.Add(1)
	go func() {
		defer srv.jobWg.Done()
		time.Sleep(150 * time.Millisecond)
		close(jobDone)
	}()

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	elapsed := time.Since(start)

	select {
	case <-jobDone:
	default:
		t.Error("job goroutine had not finished when Shutdown returned")
	}
	if elapsed < 100*time.Millisecond {
		t.Errorf("Shutdown returned too quickly (%v), want ≥150ms", elapsed)
	}
}

// TestShutdown_RespectsContextDeadline verifies that Shutdown returns when its
// context expires even if jobs are still running (Fix #2).
func TestShutdown_RespectsContextDeadline(t *testing.T) {
	srv, _, _ := newTestServer(t)

	srv.jobWg.Add(1)
	go func() {
		defer srv.jobWg.Done()
		time.Sleep(10 * time.Second)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := srv.Shutdown(ctx)
	if err == nil {
		t.Error("want non-nil error when context expires before jobs finish")
	}
}

// ── Fix #3: CancelJob and real abort handler ──────────────────────────────────

func TestCancelJob_RegisterUnregister(t *testing.T) {
	orch := &Orchestrator{}
	called := false
	orch.registerJob("job-1", func() { called = true })
	if !orch.CancelJob("job-1") {
		t.Error("CancelJob returned false for a registered job")
	}
	if !called {
		t.Error("cancel function was not called")
	}
	orch.unregisterJob("job-1")
	if orch.CancelJob("job-1") {
		t.Error("CancelJob returned true after unregister")
	}
}

func TestCancelJob_UnknownJob(t *testing.T) {
	orch := &Orchestrator{}
	if orch.CancelJob("does-not-exist") {
		t.Error("CancelJob should return false for unknown job")
	}
}

// TestCancelJob_Concurrent checks that concurrent register/cancel/unregister
// operations are race-free (run with -race).
func TestCancelJob_Concurrent(t *testing.T) {
	orch := &Orchestrator{}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		id := fmt.Sprintf("job-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			orch.registerJob(id, func() {})
			orch.CancelJob(id)
			orch.unregisterJob(id)
		}()
	}
	wg.Wait()
}

// TestAbortHandler_EmptyID returns 404 when the router provides no job id.
func TestAbortHandler_EmptyID(t *testing.T) {
	srv, _, _ := newTestServer(t)
	req := httptest.NewRequest("POST", "/api/v1/jobs//abort", nil)
	rec := httptest.NewRecorder()
	srv.abortJobHandler(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("want 404 for empty id, got %d", rec.Code)
	}
}

// TestAbortHandler_NotFound returns 404 for an id that has no job on disk.
func TestAbortHandler_NotFound(t *testing.T) {
	srv, _, _ := newTestServer(t)
	req := httptest.NewRequest("POST", "/api/v1/jobs/nonexistent/abort", nil)
	req = withMuxVars(req, map[string]string{"id": "nonexistent"})
	rec := httptest.NewRecorder()
	srv.abortJobHandler(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("want 404, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestAbortHandler_TerminalJob returns 409 for a job already in a terminal state.
func TestAbortHandler_TerminalJob(t *testing.T) {
	srv, sp, _ := newTestServer(t)
	j := job.NewJob("job-terminal", "repo.cern.ch", "", "")
	j.State = job.StatePublished
	if err := sp.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	req := httptest.NewRequest("POST", "/api/v1/jobs/job-terminal/abort", nil)
	req = withMuxVars(req, map[string]string{"id": "job-terminal"})
	rec := httptest.NewRecorder()
	srv.abortJobHandler(rec, req)
	if rec.Code != http.StatusConflict {
		t.Errorf("want 409 for terminal job, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestAbortHandler_RunningJob returns 202 and calls the registered cancel function.
func TestAbortHandler_RunningJob(t *testing.T) {
	srv, sp, orch := newTestServer(t)
	j := job.NewJob("job-running", "repo.cern.ch", "", "")
	j.State = job.StateStaging
	if err := sp.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	cancelled := false
	orch.registerJob("job-running", func() { cancelled = true })
	defer orch.unregisterJob("job-running")

	req := httptest.NewRequest("POST", "/api/v1/jobs/job-running/abort", nil)
	req = withMuxVars(req, map[string]string{"id": "job-running"})
	rec := httptest.NewRecorder()
	srv.abortJobHandler(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Errorf("want 202, got %d: %s", rec.Code, rec.Body.String())
	}
	if !cancelled {
		t.Error("cancel function was not called")
	}
	var resp map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["status"] != "aborting" {
		t.Errorf("want status=aborting, got %q", resp["status"])
	}
}

// TestShutdown_WaitsForWebhookGoroutines verifies that Shutdown also waits for
// webhook delivery goroutines tracked by Orchestrator.webhookWg.
func TestShutdown_WaitsForWebhookGoroutines(t *testing.T) {
	srv, _, orch := newTestServer(t)

	webhookDone := make(chan struct{})
	orch.webhookWg.Add(1)
	go func() {
		defer orch.webhookWg.Done()
		time.Sleep(150 * time.Millisecond)
		close(webhookDone)
	}()

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	elapsed := time.Since(start)

	select {
	case <-webhookDone:
	default:
		t.Error("webhook goroutine had not finished when Shutdown returned")
	}
	if elapsed < 100*time.Millisecond {
		t.Errorf("Shutdown returned too quickly (%v), want ≥150ms", elapsed)
	}
}

// TestAbortHandler_NotRunning returns 409 when the job exists but is not in the
// running map (narrow race: job completed between FindJob and CancelJob).
func TestAbortHandler_NotRunning(t *testing.T) {
	srv, sp, _ := newTestServer(t)
	j := job.NewJob("job-notrunning", "repo.cern.ch", "", "")
	j.State = job.StateStaging
	if err := sp.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	// deliberately NOT registered in orch.running

	req := httptest.NewRequest("POST", "/api/v1/jobs/job-notrunning/abort", nil)
	req = withMuxVars(req, map[string]string{"id": "job-notrunning"})
	rec := httptest.NewRecorder()
	srv.abortJobHandler(rec, req)
	if rec.Code != http.StatusConflict {
		t.Errorf("want 409, got %d: %s", rec.Code, rec.Body.String())
	}
}
