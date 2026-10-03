// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/internal/notify"
)

// runEvents serves GET /jobs/{id}/events until the handler returns, failing
// the test if it does not return within the deadline. Calls tick, if given,
// while waiting.
func runEvents(t *testing.T, srv *Server, id string, tick func()) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := withMuxVars(httptest.NewRequest("GET", "/api/v1/jobs/"+id+"/events", nil).WithContext(ctx),
		map[string]string{"id": id})
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { srv.jobEvents(rec, req); close(done) }()
	for {
		select {
		case <-done:
			if ctx.Err() != nil {
				t.Fatal("event stream did not close by itself")
			}
			return rec.Body.String()
		case <-time.After(10 * time.Millisecond):
			if tick != nil {
				tick()
			}
		}
	}
}

// Subscribing to a job that has already finished gets its state and an end of
// stream, instead of silence forever.
func TestJobEvents_FinishedJobSendsStateAndCloses(t *testing.T) {
	srv, sp, _ := newTestServer(t)
	j := &job.Job{ID: "done-1", Repo: "software.cern.ch", State: job.StateFailed,
		Error: "boom", CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := sp.WriteManifest(j); err != nil {
		t.Fatal(err)
	}
	body := runEvents(t, srv, j.ID, nil)
	if strings.Count(body, "event: state_change") != 1 ||
		!strings.Contains(body, `"state":"failed"`) || !strings.Contains(body, `"error":"boom"`) {
		t.Errorf("unexpected stream: %q", body)
	}
}

// A running job gets its current state first, then live transitions.
func TestJobEvents_RunningJobSendsCurrentStateFirst(t *testing.T) {
	srv, sp, _ := newTestServer(t)
	j := &job.Job{ID: "run-1", Repo: "software.cern.ch", State: job.StateIncoming,
		CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := sp.WriteManifest(j); err != nil {
		t.Fatal(err)
	}
	body := runEvents(t, srv, j.ID, func() {
		srv.notifyBus.Publish(notify.Event{JobID: j.ID, State: job.StatePublished, Time: time.Now()})
	})
	first := strings.Index(body, `"state":"incoming"`)
	last := strings.Index(body, `"state":"published"`)
	if first < 0 || last < first {
		t.Errorf("want incoming then published, got %q", body)
	}
}
