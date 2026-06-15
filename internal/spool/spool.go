// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Package spool manages persistent job state with atomic transitions and crash-safe
// durability guarantees using journaling, fsync ordering, and atomic renames.
package spool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/pkg/observe"
)

// Spool manages persistent job state across the FSM lifecycle.
// Jobs are organized in subdirectories by state (incoming, staging, uploading, etc.)
// and transitions are atomic: a job either moves to the next state or remains in the
// current state on crash. The manifest (job metadata) is durably persisted using
// atomic writes and parent directory fsyncs.
type Spool struct {
	// Root is the root directory for all spool state.
	Root string
	// obs provides logging and metrics.
	obs *observe.Provider
}

// New creates a new Spool with the given root directory.
// All spool directories are created with mode 0700 to prevent other local users
// from reading sensitive job metadata (lease tokens, manifests). Returns an error
// if the root directory cannot be created.
func New(root string, obs *observe.Provider) (*Spool, error) {
	// Fix #12: Spool directories are 0700 — job metadata (including lease tokens)
	// must not be readable by other local users.
	// Directories are listed in FSM order: lease is now acquired after distribution.
	for _, dir := range []string{root, "incoming", "staging", "uploading", "distributing", "leased", "committing", "published", "failed", "aborted"} {
		path := filepath.Join(root, dir)
		if err := os.MkdirAll(path, 0700); err != nil {
			return nil, fmt.Errorf("creating spool directory %q: %w", path, err)
		}
	}
	return &Spool{
		Root: root,
		obs:  obs,
	}, nil
}

// stateDir returns the directory for a job state.
func (s *Spool) stateDir(state job.State) string {
	return filepath.Join(s.Root, string(state))
}

// JobDir returns the current directory for a job based on its state.
// The job directory contains the manifest.json and state-specific files.
func (s *Spool) JobDir(j *job.Job) string {
	return filepath.Join(s.stateDir(j.State), j.ID)
}

// Transition atomically moves a job directory from its current state to a new state.
// It appends a journal entry before the move, fsyncs the old directory, renames the directory,
// and fsyncs the new directory to ensure crash consistency. The job state is updated in memory
// and j.UpdatedAt is set to the current time.
func (s *Spool) Transition(ctx context.Context, j *job.Job, to job.State) error {
	ctx, span := s.obs.Tracer.Start(ctx, "spool.transition")
	defer span.End()

	if err := job.Transition(j.State, to); err != nil {
		span.RecordError(err)
		return err
	}

	oldDir := s.JobDir(j)
	newDir := filepath.Join(s.stateDir(to), j.ID)

	// Write journal entry before moving.
	// oldDir is always Root/<state>/<jobID>, so filepath.Dir gives Root/<state>.
	// The guard "if jdir == s.Root" could never be true and has been removed.
	jdir := filepath.Dir(oldDir)
	journal := OpenJournal(jdir)
	entry := Entry{
		T:     time.Now(),
		JobID: j.ID,
		From:  j.State,
		To:    to,
		RunID: fmt.Sprintf("%d", time.Now().UnixNano()),
	}
	if err := journal.Append(entry); err != nil {
		span.RecordError(err)
		return fmt.Errorf("writing journal: %w", err)
	}

	// Fsync the old directory
	f, err := os.Open(jdir)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("opening old directory for fsync: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		span.RecordError(err)
		return fmt.Errorf("syncing old directory before rename: %w", err)
	}
	f.Close()

	// Create new state directory if needed (keep consistent 0700 mode).
	newStateDir := s.stateDir(to)
	if err := os.MkdirAll(newStateDir, 0700); err != nil {
		span.RecordError(err)
		return fmt.Errorf("creating new state directory: %w", err)
	}

	// Rename job directory.  If the destination already exists (left behind by
	// a previous crash before the rename completed), remove it first so the
	// rename can succeed.  The stale directory is superseded by the current
	// job state being transitioned now.
	if err := os.Rename(oldDir, newDir); err != nil {
		if isErrExist(err) {
			if rmErr := os.RemoveAll(newDir); rmErr != nil {
				span.RecordError(rmErr)
				return fmt.Errorf("removing stale job directory %q: %w", newDir, rmErr)
			}
			if err = os.Rename(oldDir, newDir); err != nil {
				span.RecordError(err)
				return fmt.Errorf("renaming job directory after stale removal: %w", err)
			}
		} else {
			span.RecordError(err)
			return fmt.Errorf("renaming job directory: %w", err)
		}
	}

	// Fsync the new directory
	f, err = os.Open(newStateDir)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("opening new directory for fsync: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		span.RecordError(err)
		return fmt.Errorf("fsyncing new directory: %w", err)
	}
	f.Close()

	// Update job state
	j.State = to
	j.UpdatedAt = time.Now()

	// Write the manifest into the new directory so the on-disk state is
	// consistent with j.State and the spool directory name.  Without this,
	// FindJob reads the old manifest (which has the pre-transition state) and
	// returns a stale state to callers — e.g. GET /api/v1/jobs/{id} returns
	// "leased" even after the job directory has been moved to published/.
	// Pollers that check for "published" then hang until their deadline.
	//
	// This write is best-effort: a failure here means the directory was
	// successfully renamed (the job is in the correct spool state) but the
	// manifest carries a stale state field.  We log and continue rather than
	// failing the transition itself.
	if wErr := s.WriteManifest(j); wErr != nil {
		s.obs.Logger.Warn("manifest write after transition failed — state field may be stale",
			"from", string(entry.From), "to", string(to), "job_id", j.ID, "error", wErr)
	}

	// Record metric
	s.obs.Metrics.SpoolTransitions.WithLabelValues(string(entry.From), string(entry.To)).Inc()

	return nil
}

// Scan returns all jobs in non-terminal states, in FSM order.
// Used at service startup to identify jobs in progress and those requiring recovery.
func (s *Spool) Scan(ctx context.Context) ([]*job.Job, error) {
	ctx, span := s.obs.Tracer.Start(ctx, "spool.scan")
	defer span.End()

	var jobs []*job.Job

	// Scan all non-terminal states in FSM order.
	// StateLeased is listed after StateDistributing because the lease is now
	// acquired only after objects have been distributed to Stratum 1 replicas.
	for _, state := range []job.State{
		job.StateIncoming,
		job.StateStaging,
		job.StateUploading,
		job.StateDistributing,
		job.StateLeased,
		job.StateCommitting,
	} {
		stateDir := s.stateDir(state)
		entries, err := os.ReadDir(stateDir)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			span.RecordError(err)
			return nil, fmt.Errorf("reading state directory %s: %w", stateDir, err)
		}

		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			jobDir := filepath.Join(stateDir, entry.Name())
			j, err := s.ReadManifest(jobDir)
			if err != nil {
				span.RecordError(err)
				return nil, fmt.Errorf("reading manifest for job %s: %w", entry.Name(), err)
			}
			jobs = append(jobs, j)
		}
	}

	return jobs, nil
}

// WriteManifest durably persists job metadata to manifest.json in the job directory.
//
// Durability: Uses write-to-temp-then-atomic-rename with fsync of the parent directory
// (Fix #6) to ensure a crash mid-write never leaves a partial or zero-byte manifest.
// The manifest is written with mode 0600 (Fix #12) so other local users cannot
// read sensitive data like lease tokens.
func (s *Spool) WriteManifest(j *job.Job) error {
	jobDir := s.JobDir(j)
	if err := os.MkdirAll(jobDir, 0700); err != nil {
		return fmt.Errorf("creating job directory: %w", err)
	}

	data, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling manifest: %w", err)
	}

	manifestPath := filepath.Join(jobDir, "manifest.json")
	tmpPath := manifestPath + ".tmp"

	// Write to a sibling temp file first.
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return fmt.Errorf("writing manifest temp file: %w", err)
	}

	// Sync the temp file before renaming so the data is durable on crash.
	// Must use O_RDWR, not os.Open (which gives a read-only fd): fsync(2) on a
	// read-only fd is not guaranteed to succeed on all kernels/filesystems and
	// returns EBADF on some.  A writable fd always works.
	f, err := os.OpenFile(tmpPath, os.O_RDWR, 0)
	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("opening manifest temp for sync: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("syncing manifest temp: %w", err)
	}
	f.Close()

	// Atomic rename — the manifest is either the old version or the new one,
	// never a partial write.
	if err := os.Rename(tmpPath, manifestPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("renaming manifest: %w", err)
	}

	// Fix #6: fsync the parent directory so the directory entry for the
	// renamed file is durable.  Without this a crash between the rename and
	// the next sync could leave the directory pointing at the old inode.
	// Best-effort: data was already written; a sync failure here does not
	// corrupt it, but we log it so hardware I/O errors are not silent.
	if dir, err := os.Open(jobDir); err == nil {
		if syncErr := dir.Sync(); syncErr != nil {
			s.obs.Logger.Warn("fsync parent directory after manifest rename failed",
				"path", jobDir, "error", syncErr)
		}
		dir.Close()
	}

	return nil
}

// findJobDir returns the actual on-disk directory for a job, regardless of
// what the manifest's State field says.  It checks the expected directory for
// hintState first (fast path), then scans all non-terminal state directories.
// Returns "" if the directory cannot be found anywhere.
func (s *Spool) findJobDir(id string, hintState job.State) string {
	// Fast path: the manifest state matches the actual directory.
	expected := filepath.Join(s.Root, string(hintState), id)
	if _, err := os.Stat(expected); err == nil {
		return expected
	}

	// Slow path: crash left the directory in a different state directory.
	// Search all non-terminal states (terminal jobs are never recovered).
	nonTerminal := []job.State{
		job.StateIncoming,
		job.StateStaging,
		job.StateUploading,
		job.StateDistributing,
		job.StateLeased,
		job.StateCommitting,
	}
	for _, state := range nonTerminal {
		if state == hintState {
			continue // already checked above
		}
		candidate := filepath.Join(s.Root, string(state), id)
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return ""
}

// ResetForRecovery moves a non-terminal job back to the incoming state for reprocessing
// after a service crash. It bypasses normal FSM transition rules because recovery is
// an administrative operation, not a regular workflow step.
//
// ResetForRecovery increments RecoveryCount, clears the lease token and error message,
// and moves the job directory back to incoming. The caller is responsible for releasing
// any stale gateway lease before invoking this method.
func (s *Spool) ResetForRecovery(j *job.Job) error {
	if job.IsTerminal(j.State) {
		return fmt.Errorf("cannot reset terminal job %s in state %s", j.ID, j.State)
	}

	// Locate the actual on-disk directory for this job.  After a crash the
	// manifest's State field may not match the real directory location — e.g.
	// the manifest was rewritten with state=leased but the directory rename
	// to leased/ never completed, so the directory is still in staging/.
	// Use s.JobDir(j) as the first guess, then scan all non-terminal states.
	oldDir := s.findJobDir(j.ID, j.State)
	if oldDir == "" {
		return fmt.Errorf("resetting job for recovery: cannot find on-disk directory for job %s (state=%s)", j.ID, j.State)
	}

	j.RecoveryCount++
	j.State = job.StateIncoming
	j.LeaseToken = ""
	j.Error = ""
	j.UpdatedAt = time.Now()

	newDir := s.JobDir(j) // now reflects StateIncoming

	// Ensure the incoming directory exists (it always should, but be defensive).
	if err := os.MkdirAll(filepath.Dir(newDir), 0700); err != nil {
		return fmt.Errorf("ensuring incoming directory: %w", err)
	}

	// Only rename if the paths differ (job may already be in incoming).
	if oldDir != newDir {
		if err := os.Rename(oldDir, newDir); err != nil {
			return fmt.Errorf("moving job %s to incoming for recovery: %w", j.ID, err)
		}
	}

	// Rewrite the manifest with the updated state and recovery count.
	if err := s.WriteManifest(j); err != nil {
		return fmt.Errorf("writing recovery manifest: %w", err)
	}

	s.obs.Metrics.JobsRecovered.Inc()
	return nil
}

// ReadManifest reads and unmarshals job metadata from manifest.json in the given directory.
func (s *Spool) ReadManifest(dir string) (*job.Job, error) {
	manifestPath := filepath.Join(dir, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("reading manifest: %w", err)
	}

	var j job.Job
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("unmarshaling manifest: %w", err)
	}

	return &j, nil
}

// FindJob searches all state directories for a job with the given ID and returns
// the job manifest. It searches non-terminal states first (faster path for in-flight jobs),
// then terminal states. Returns os.ErrNotExist if no job with that ID is found.
func (s *Spool) FindJob(id string) (*job.Job, error) {
	allStates := []job.State{
		// non-terminal first
		job.StateIncoming,
		job.StateStaging,
		job.StateUploading,
		job.StateDistributing,
		job.StateLeased,
		job.StateCommitting,
		// terminal
		job.StatePublished,
		job.StateFailed,
		job.StateAborted,
	}
	for _, state := range allStates {
		dir := filepath.Join(s.Root, string(state), id)
		j, err := s.ReadManifest(dir)
		if err == nil {
			return j, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			// Unexpected error (e.g. permissions) — return it.
			return nil, fmt.Errorf("reading manifest in %s: %w", string(state), err)
		}
	}
	return nil, fmt.Errorf("job %q: %w", id, os.ErrNotExist)
}

// ReadJobJournal returns all journal entries for a specific job ID, ordered
// by timestamp.  It scans every state directory's journal.jsonl and filters
// by job_id, so the full FSM history is reconstructed even after the job
// directory has been renamed across multiple state transitions.
//
// Returns nil (not an error) when no journal entries exist yet (e.g. job was
// just submitted and has not transitioned yet).
func (s *Spool) ReadJobJournal(jobID string) ([]Entry, error) {
	allStates := []job.State{
		job.StateIncoming,
		job.StateStaging,
		job.StateUploading,
		job.StateDistributing,
		job.StateLeased,
		job.StateCommitting,
		job.StatePublished,
		job.StateFailed,
		job.StateAborted,
	}

	var entries []Entry
	for _, state := range allStates {
		stateDir := filepath.Join(s.Root, string(state))
		j := OpenJournal(stateDir)
		all, err := j.Read()
		if err != nil {
			// Corrupt journal in this state directory — skip it; log but don't fail.
			continue
		}
		for _, e := range all {
			if e.JobID == jobID {
				entries = append(entries, e)
			}
		}
	}

	// Sort by time ascending so the caller sees a chronological history.
	sort.Slice(entries, func(i, k int) bool {
		return entries[i].T.Before(entries[k].T)
	})

	return entries, nil
}

// IncomingBySize returns all jobs currently in the incoming state, sorted by
// TarSize descending (largest first).
//
// This is the preferred way to enumerate jobs at startup or recovery so that
// large jobs enter the pipeline first and overlap with smaller ones, reducing
// overall makespan.  Jobs with TarSize == 0 (e.g. written before this field
// was introduced) sort last and are otherwise treated normally.
func (s *Spool) IncomingBySize(ctx context.Context) ([]*job.Job, error) {
	_, span := s.obs.Tracer.Start(ctx, "spool.incoming_by_size")
	defer span.End()

	incomingDir := s.stateDir(job.StateIncoming)
	entries, err := os.ReadDir(incomingDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		span.RecordError(err)
		return nil, fmt.Errorf("reading incoming directory: %w", err)
	}

	var jobs []*job.Job
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		jobDir := filepath.Join(incomingDir, entry.Name())
		j, err := s.ReadManifest(jobDir)
		if err != nil {
			span.RecordError(err)
			return nil, fmt.Errorf("reading manifest for job %s: %w", entry.Name(), err)
		}
		jobs = append(jobs, j)
	}

	// Sort largest TarSize first so long-running jobs start early and overlap
	// with shorter ones.  Ties are broken by submission time (CreatedAt) so
	// older jobs are preferred among equal-size jobs, giving FIFO within each
	// size bucket.
	sort.Slice(jobs, func(i, k int) bool {
		if jobs[i].TarSize != jobs[k].TarSize {
			return jobs[i].TarSize > jobs[k].TarSize // descending size
		}
		return jobs[i].CreatedAt.Before(jobs[k].CreatedAt) // ascending age (older first)
	})

	return jobs, nil
}

// isErrExist reports whether err (or any wrapped error) indicates that a file
// or directory already exists.  os.IsExist does not unwrap, so we check both.
func isErrExist(err error) bool {
	return os.IsExist(err) || errors.Is(err, os.ErrExist)
}
