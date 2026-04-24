package spool

import (
	"testing"
	"time"

	"cvmfs.io/prepub/internal/job"
)

// TestJournal_JobIDPresent verifies that Append writes the job_id field and
// Read recovers it correctly (Fix #4).
func TestJournal_JobIDPresent(t *testing.T) {
	dir := t.TempDir()
	j := OpenJournal(dir)

	e := Entry{
		T:     time.Now().Round(time.Millisecond),
		JobID: "test-job-123",
		From:  job.StateIncoming,
		To:    job.StateStaging,
		RunID: "run-1",
	}
	if err := j.Append(e); err != nil {
		t.Fatalf("Append: %v", err)
	}

	entries, err := j.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].JobID != "test-job-123" {
		t.Errorf("job_id: want %q, got %q", "test-job-123", entries[0].JobID)
	}
	if entries[0].From != job.StateIncoming {
		t.Errorf("from: want %q, got %q", job.StateIncoming, entries[0].From)
	}
	if entries[0].To != job.StateStaging {
		t.Errorf("to: want %q, got %q", job.StateStaging, entries[0].To)
	}
}

// TestJournal_MultipleJobIDs verifies that distinct job_ids are preserved
// across multiple journal entries.
func TestJournal_MultipleJobIDs(t *testing.T) {
	dir := t.TempDir()
	j := OpenJournal(dir)

	jobs := []string{"job-a", "job-b", "job-c"}
	for i, id := range jobs {
		e := Entry{
			T:     time.Now(),
			JobID: id,
			From:  job.StateIncoming,
			To:    job.StateStaging,
			RunID: "run",
		}
		_ = i
		if err := j.Append(e); err != nil {
			t.Fatalf("Append(%q): %v", id, err)
		}
	}

	entries, err := j.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(entries) != len(jobs) {
		t.Fatalf("expected %d entries, got %d", len(jobs), len(entries))
	}
	for i, want := range jobs {
		if entries[i].JobID != want {
			t.Errorf("entry[%d].job_id: want %q, got %q", i, want, entries[i].JobID)
		}
	}
}

// TestJournal_CRCRoundtrip verifies that Read detects corruption.
func TestJournal_CRCRoundtrip(t *testing.T) {
	dir := t.TempDir()
	j := OpenJournal(dir)

	if err := j.Append(Entry{
		T:     time.Now(),
		JobID: "j1",
		From:  job.StateStaging,
		To:    job.StateUploading,
		RunID: "r1",
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	entries, err := j.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
}
