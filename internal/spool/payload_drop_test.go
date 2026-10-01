// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package spool

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"cvmfs.io/prepub/internal/job"
)

// A job's payload is deleted once it is terminal: nothing reads it again, and
// keeping them filled the spool.
func TestTransition_PublishedDropsPayload(t *testing.T) {
	for _, tc := range []struct {
		to   job.State
		keep bool
	}{
		{job.StatePublished, false},
		{job.StateAccumulated, false},
		{job.StateFailed, false},
	} {
		t.Run(string(tc.to), func(t *testing.T) {
			s := newTestSpool(t)
			from := job.StateCommitting
			if tc.to == job.StateAccumulated {
				from = job.StateUploading
			}
			j := &job.Job{ID: "j1", Repo: "r.example.org", Path: "p", State: from}
			if err := s.WriteManifest(j); err != nil {
				t.Fatalf("WriteManifest: %v", err)
			}
			if err := os.WriteFile(filepath.Join(s.JobDir(j), "payload.tar"), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := s.Transition(context.Background(), j, tc.to); err != nil {
				t.Fatalf("Transition: %v", err)
			}
			_, err := os.Stat(filepath.Join(s.JobDir(j), "payload.tar"))
			if kept := err == nil; kept != tc.keep {
				t.Errorf("payload kept = %v, want %v", kept, tc.keep)
			}
			if got, err := s.FindJob("j1"); err != nil || got.State != tc.to {
				t.Errorf("job record after transition: %v, %v", got, err)
			}
		})
	}
}

// Requeue moves a job back to incoming without touching its recovery
// counters: a retry is neither a crash nor an interruption.
func TestRequeue_KeepsCountersAndPayload(t *testing.T) {
	s := newTestSpool(t)
	j := &job.Job{ID: "j2", Repo: "r.example.org", Path: "p", State: job.StateCommitting, RecoveryCount: 1}
	if err := s.WriteManifest(j); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.JobDir(j), "payload.tar"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Requeue(j); err != nil {
		t.Fatalf("Requeue: %v", err)
	}
	got, err := s.FindJob("j2")
	if err != nil || got.State != job.StateIncoming || got.RecoveryCount != 1 || got.InterruptCount != 0 {
		t.Fatalf("after Requeue: %+v, %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(s.JobDir(j), "payload.tar")); err != nil {
		t.Errorf("payload not kept: %v", err)
	}
}
