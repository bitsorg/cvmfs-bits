package simulate

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/api"
	"cvmfs.io/prepub/internal/job"
)

// setupJob creates a new job and places payload.tar inside the job's incoming
// spool directory.
//
// The tar file MUST live inside the job directory (not at the spool root) so
// that spool.Transition's atomic rename of incoming/<id>/ → staging/<id>/
// carries the tar along.  After the rename the orchestrator derives the new
// tar path as staging/<id>/payload.tar, which must already exist.
//
// The incoming directory is created here; calling WriteManifest afterwards is
// idempotent w.r.t. the directory (os.MkdirAll is a no-op on an existing dir).
func setupJob(t *testing.T, cluster *Cluster, id, repo string, files map[string]string) *job.Job {
	t.Helper()
	tarBuf := createTestTar(files)
	j := job.NewJob(id, repo, "test-pkg", "")

	// Pre-create the incoming job directory so that payload.tar resides inside
	// it before any manifest is written.
	incomingDir := filepath.Join(cluster.SpoolRoot, "incoming", id)
	if err := os.MkdirAll(incomingDir, 0700); err != nil {
		t.Fatalf("setupJob: creating incoming dir %q: %v", incomingDir, err)
	}
	tarPath := filepath.Join(incomingDir, "payload.tar")
	if err := os.WriteFile(tarPath, tarBuf.Bytes(), 0644); err != nil {
		t.Fatalf("setupJob: writing payload.tar: %v", err)
	}
	j.TarPath = tarPath
	return j
}

// transitionTo pre-stages a job to the given state without running the
// pipeline.  It simulates a process that crashed after reaching that state.
func transitionTo(t *testing.T, orch *api.Orchestrator, j *job.Job, target job.State) {
	t.Helper()
	ctx := context.Background()

	// WriteManifest creates the initial incoming/<jobID>/ directory.
	if err := orch.Spool.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	// Walk the FSM forward one step at a time to the target state.
	path := statesUpTo(target)
	for _, s := range path {
		if err := orch.Spool.Transition(ctx, j, s); err != nil {
			t.Fatalf("Transition to %s: %v", s, err)
		}
	}
}

// statesUpTo returns the ordered slice of states the FSM must visit to reach
// target, starting after StateIncoming.
// The order reflects the lease-after-distribute optimisation:
//
//	incoming → staging → uploading → distributing → leased → committing → published
func statesUpTo(target job.State) []job.State {
	full := []job.State{
		job.StateStaging,
		job.StateUploading,
		job.StateDistributing,
		job.StateLeased,
		job.StateCommitting,
	}
	var out []job.State
	for _, s := range full {
		out = append(out, s)
		if s == target {
			break
		}
	}
	return out
}

// TestRecovery_FromIncoming verifies that a job interrupted before any state
// transition is recovered and eventually published.
func TestRecovery_FromIncoming(t *testing.T) {
	cluster := NewCluster(t, 0)
	defer cluster.Close()

	j := setupJob(t, cluster, "recover-incoming-1", "test.cvmfs.io", map[string]string{
		"file.txt": "hello",
	})
	orch := cluster.NewOrchestrator("test.cvmfs.io")

	// Write manifest in incoming state (no transition — job is stuck in incoming).
	if err := orch.Spool.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := orch.Recover(ctx, j); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if j.State != job.StatePublished {
		t.Errorf("expected StatePublished, got %s", j.State)
	}
	if j.RecoveryCount != 1 {
		t.Errorf("expected RecoveryCount=1, got %d", j.RecoveryCount)
	}
}

// TestRecovery_FromLeased verifies recovery of a job that crashed after
// acquiring a gateway lease but before committing (the lease window).
func TestRecovery_FromLeased(t *testing.T) {
	cluster := NewCluster(t, 0)
	defer cluster.Close()

	j := setupJob(t, cluster, "recover-leased-1", "test.cvmfs.io", map[string]string{
		"a.txt": "alpha",
		"b.txt": "beta",
	})
	orch := cluster.NewOrchestrator("test.cvmfs.io")

	// Simulate crash after acquiring lease.
	transitionTo(t, orch, j, job.StateLeased)
	j.LeaseToken = "stale-lease-from-previous-run"
	if err := orch.Spool.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := orch.Recover(ctx, j); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if j.State != job.StatePublished {
		t.Errorf("expected StatePublished, got %s", j.State)
	}
	if j.LeaseToken == "stale-lease-from-previous-run" {
		t.Error("stale lease token should have been cleared")
	}
	if j.RecoveryCount != 1 {
		t.Errorf("expected RecoveryCount=1, got %d", j.RecoveryCount)
	}
}

// TestRecovery_FromStaging verifies recovery of a job that crashed mid-pipeline.
func TestRecovery_FromStaging(t *testing.T) {
	cluster := NewCluster(t, 0)
	defer cluster.Close()

	j := setupJob(t, cluster, "recover-staging-1", "test.cvmfs.io", map[string]string{
		"data.bin": "some binary content",
	})
	orch := cluster.NewOrchestrator("test.cvmfs.io")

	transitionTo(t, orch, j, job.StateStaging)
	j.LeaseToken = "stale-staging-lease"
	if err := orch.Spool.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := orch.Recover(ctx, j); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if j.State != job.StatePublished {
		t.Errorf("expected StatePublished, got %s", j.State)
	}
	if j.RecoveryCount != 1 {
		t.Errorf("expected RecoveryCount=1, got %d", j.RecoveryCount)
	}
}

// TestRecovery_FromUploading verifies recovery of a job interrupted after the
// pipeline completed but before committing the payload to the gateway.
func TestRecovery_FromUploading(t *testing.T) {
	cluster := NewCluster(t, 0)
	defer cluster.Close()

	j := setupJob(t, cluster, "recover-uploading-1", "test.cvmfs.io", map[string]string{
		"pkg/lib.so": "elf data",
	})
	orch := cluster.NewOrchestrator("test.cvmfs.io")

	transitionTo(t, orch, j, job.StateUploading)
	j.LeaseToken = "stale-uploading-lease"
	if err := orch.Spool.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := orch.Recover(ctx, j); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if j.State != job.StatePublished {
		t.Errorf("expected StatePublished, got %s", j.State)
	}
}

// TestRecovery_CountIncremented verifies that each Recover call increments
// RecoveryCount, and that a job can be recovered multiple times up to the limit.
func TestRecovery_CountIncremented(t *testing.T) {
	cluster := NewCluster(t, 0)
	defer cluster.Close()

	orch := cluster.NewOrchestrator("test.cvmfs.io")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for attempt := 1; attempt <= api.MaxRecoveries-1; attempt++ {
		// Use a unique job ID per attempt to avoid directory collisions from
		// previously published jobs sharing the same ID.
		id := fmt.Sprintf("recover-count-%d", attempt)
		j := setupJob(t, cluster, id, "test.cvmfs.io", map[string]string{
			"run.sh": "#!/bin/sh",
		})
		j.RecoveryCount = attempt - 1 // pre-set count to simulate prior recoveries

		if err := orch.Spool.WriteManifest(j); err != nil {
			t.Fatalf("attempt %d WriteManifest: %v", attempt, err)
		}

		if err := orch.Recover(ctx, j); err != nil {
			t.Fatalf("attempt %d Recover: %v", attempt, err)
		}

		if j.RecoveryCount != attempt {
			t.Errorf("attempt %d: expected RecoveryCount=%d, got %d", attempt, attempt, j.RecoveryCount)
		}
		if j.State != job.StatePublished {
			t.Errorf("attempt %d: expected StatePublished, got %s", attempt, j.State)
		}
	}
}

// TestRecovery_MaxRecoveries verifies that a job at the recovery limit is
// failed rather than re-queued, preventing infinite recovery loops.
func TestRecovery_MaxRecoveries(t *testing.T) {
	cluster := NewCluster(t, 0)
	defer cluster.Close()

	j := setupJob(t, cluster, "recover-max-1", "test.cvmfs.io", map[string]string{
		"file.txt": "content",
	})
	j.RecoveryCount = api.MaxRecoveries // already at the limit
	orch := cluster.NewOrchestrator("test.cvmfs.io")

	if err := orch.Spool.WriteManifest(j); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := orch.Recover(ctx, j)
	if err == nil {
		t.Fatal("expected Recover to return an error when recovery limit is reached")
	}

	// The job should have been moved to a terminal failed state.
	if !job.IsTerminal(j.State) {
		t.Errorf("expected terminal state, got %s", j.State)
	}
}
