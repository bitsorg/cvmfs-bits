// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/lease"
)

func TestRetryAt(t *testing.T) {
	transient := errors.New("gateway: 503 service unavailable")
	for _, tc := range []struct {
		name      string
		window    time.Duration
		attempts  int
		age       time.Duration
		err       error
		coarse    bool
		cancelled bool
		wantOK    bool
		wantDelay time.Duration
	}{
		{name: "first retry", window: 24 * time.Hour, err: transient, wantOK: true, wantDelay: time.Minute},
		{name: "fourth retry", window: 24 * time.Hour, attempts: 3, err: transient, wantOK: true, wantDelay: 8 * time.Minute},
		{name: "capped", window: 24 * time.Hour, attempts: 9, err: transient, wantOK: true, wantDelay: 30 * time.Minute},
		{name: "retries off", err: transient},
		{name: "window spent", window: 24 * time.Hour, age: 24 * time.Hour, err: transient},
		{name: "conflict", window: 24 * time.Hour, err: realConflictErr},
		{name: "classified permanent", window: 24 * time.Hour, err: Classify(ErrClassPermanent, transient)},
		{name: "unreadable payload", window: 24 * time.Hour,
			err: errors.New("cvmfs_server ingest: Impossible to open the archive")},
		{name: "operator abort", window: 24 * time.Hour, err: context.Canceled, cancelled: true},
		{name: "coarse member", window: 24 * time.Hour, err: transient, coarse: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, _ := minimalOrch(t, &noopBackend{})
			o.RetryWindow = tc.window
			j := &job.Job{ID: "j", State: job.StateCommitting, Attempts: tc.attempts,
				CreatedAt: time.Now().Add(-tc.age)}
			if tc.coarse {
				c := true
				j.Coarse = &c
				j.BuildID = "b"
			}
			if tc.cancelled {
				o.cancelled.Store(j.ID, true)
			}
			next, ok := o.retryAt(j, tc.err)
			if ok != tc.wantOK {
				t.Fatalf("retry = %v, want %v", ok, tc.wantOK)
			}
			if ok {
				if d := time.Until(next); d < tc.wantDelay-5*time.Second || d > tc.wantDelay {
					t.Errorf("delay %v, want %v", d, tc.wantDelay)
				}
			}
		})
	}
}

// A retryable failure puts the job back in incoming with its payload; a
// permanent one fails it and deletes the payload.
func TestAbortJob_RetryOrFail(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		wantState job.State
		wantTar   bool
	}{
		{"transient", errors.New("connection refused"), job.StateIncoming, true},
		{"conflict", realConflictErr, job.StateFailed, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, sp := minimalOrch(t, &noopBackend{})
			o.RetryWindow = time.Hour
			j := &job.Job{ID: "j", Repo: "r.example.org", Path: "p", State: job.StateCommitting, CreatedAt: time.Now()}
			if err := sp.WriteManifest(j); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(sp.JobDir(j), "payload.tar"), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
			err := o.abortJob(context.Background(), j, tc.err)
			if retried := errors.Is(err, ErrRetryScheduled); retried != (tc.wantState == job.StateIncoming) {
				t.Errorf("ErrRetryScheduled = %v for %v", retried, err)
			}
			got, ferr := sp.FindJob("j")
			if ferr != nil || got.State != tc.wantState {
				t.Fatalf("state: %+v, %v; want %s", got, ferr, tc.wantState)
			}
			if got.LastError == "" {
				t.Error("last_error not recorded")
			}
			if tc.wantState == job.StateIncoming && (got.Attempts != 1 || got.NextAttemptAt == nil) {
				t.Errorf("retry bookkeeping: attempts=%d next=%v", got.Attempts, got.NextAttemptAt)
			}
			_, serr := os.Stat(filepath.Join(sp.JobDir(got), "payload.tar"))
			if (serr == nil) != tc.wantTar {
				t.Errorf("payload present = %v, want %v", serr == nil, tc.wantTar)
			}
		})
	}
}

// flakyBackend fails its first commit with a transient error, then succeeds.
type flakyBackend struct {
	noopBackend
	mu      sync.Mutex
	commits int
}

func (b *flakyBackend) Commit(_ context.Context, _ lease.CommitRequest) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.commits++
	if b.commits == 1 {
		return fmt.Errorf("cvmfs_server ingest: exit status 1 (output: Gateway reply: missing_reflog)")
	}
	return nil
}

// End to end: an accepted job whose first commit fails transiently is retried
// by prepub itself and published, with no resubmission.
func TestRun_RetriesUntilPublished(t *testing.T) {
	oldBase := retryBase
	retryBase = 20 * time.Millisecond
	t.Cleanup(func() { retryBase = oldBase })

	srv, sp, orch := newTestServer(t)
	b := &flakyBackend{}
	orch.Lease = b
	orch.RetryWindow = time.Hour
	req := newMultipartRequest(t, map[string]string{
		"repo": "software.cern.ch", "path": "x86_64-el9/pkg/1.0",
	}, []byte("payload"))
	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		JobID string `json:"job_id"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	j := waitTerminal(t, sp, body.JobID)
	if j.State != job.StatePublished || j.Attempts != 1 {
		t.Fatalf("state=%s attempts=%d last_error=%q; want published after 1 retry", j.State, j.Attempts, j.LastError)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.commits != 2 {
		t.Errorf("commits = %d, want 2", b.commits)
	}
}

// A job found waiting to retry at startup resumes waiting, uncounted, and runs.
func TestRecover_ResumesWaitingJob(t *testing.T) {
	o, sp := minimalOrch(t, &noopBackend{})
	o.RetryWindow = time.Hour
	due := time.Now().Add(20 * time.Millisecond)
	j := &job.Job{ID: "w", Repo: "r.example.org", Path: "p", State: job.StateIncoming,
		CreatedAt: time.Now(), Attempts: 2, NextAttemptAt: &due}
	if err := sp.WriteManifest(j); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sp.JobDir(j), "payload.tar"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	j.TarPath = filepath.Join(sp.JobDir(j), "payload.tar")
	if err := o.Recover(context.Background(), j, true); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	got, err := sp.FindJob("w")
	if err != nil || got.State != job.StatePublished || got.InterruptCount != 0 {
		t.Fatalf("after Recover: %+v, %v", got, err)
	}
}

// alwaysFailing fails every commit with a transient error.
type alwaysFailing struct{ noopBackend }

func (*alwaysFailing) Commit(_ context.Context, _ lease.CommitRequest) error {
	return errors.New("gateway: 503 service unavailable")
}

// waitState polls the spool until the job is in want.
func waitState(t *testing.T, srvSp interface {
	FindJob(string) (*job.Job, error)
}, id string, want job.State) *job.Job {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if j, err := srvSp.FindJob(id); err == nil && j.State == want {
			return j
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s never reached %s", id, want)
	return nil
}

func submitRetryJob(t *testing.T, srv *Server) string {
	t.Helper()
	req := newMultipartRequest(t, map[string]string{
		"repo": "software.cern.ch", "path": "x86_64-el9/pkg/1.0",
	}, []byte("payload"))
	rec := httptest.NewRecorder()
	srv.submitJob(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		JobID string `json:"job_id"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return body.JobID
}

// An operator abort reaches a job waiting to retry: it fails, payload gone.
func TestRetry_AbortWhileWaiting(t *testing.T) {
	srv, sp, orch := newTestServer(t)
	orch.Lease = &alwaysFailing{}
	orch.RetryWindow = 24 * time.Hour // first retry an hour away: it waits
	oldBase := retryBase
	retryBase = time.Hour
	t.Cleanup(func() { retryBase = oldBase })

	id := submitRetryJob(t, srv)
	waiting := waitState(t, sp, id, job.StateIncoming)
	for waiting.Attempts == 0 { // incoming before the first attempt, too
		time.Sleep(5 * time.Millisecond)
		waiting, _ = sp.FindJob(id)
	}
	if !orch.CancelJob(id) {
		t.Fatal("CancelJob: job not registered while waiting")
	}
	j := waitTerminal(t, sp, id)
	if j.State != job.StateFailed {
		t.Fatalf("state %s, want failed", j.State)
	}
}

// Shutdown releases a job waiting to retry and leaves it in incoming.
func TestRetry_ShutdownWhileWaiting(t *testing.T) {
	srv, sp, orch := newTestServer(t)
	orch.Lease = &alwaysFailing{}
	orch.RetryWindow = 24 * time.Hour
	oldBase := retryBase
	retryBase = time.Hour
	t.Cleanup(func() { retryBase = oldBase })

	id := submitRetryJob(t, srv)
	for {
		if j, err := sp.FindJob(id); err == nil && j.Attempts == 1 && j.State == job.StateIncoming {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v (a waiting job held it up)", err)
	}
	j, err := sp.FindJob(id)
	if err != nil || j.State != job.StateIncoming || j.NextAttemptAt == nil {
		t.Fatalf("after shutdown: %+v, %v; want incoming with a due time", j, err)
	}
}
