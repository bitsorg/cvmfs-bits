// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package spool

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"cvmfs.io/prepub/internal/job"
)

// TestProvenanceRecord_Sidecar: the signed record goes to a sidecar, the
// manifest keeps only its name and hash, and the sidecar moves with the job
// directory through state renames and a recovery reset.
func TestProvenanceRecord_Sidecar(t *testing.T) {
	s := newTestSpool(t)
	signed := []byte(`{"job_id":"j","object_hashes":["aa","bb"],"x":"<&>"}`)
	j := &job.Job{ID: "j", Repo: "r", State: job.StateIncoming, Provenance: &job.Provenance{RekorUUID: "u"}}
	if err := s.WriteManifest(j); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteProvenanceRecord(j, signed); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteManifest(j); err != nil {
		t.Fatal(err)
	}

	m, err := os.ReadFile(filepath.Join(s.JobDir(j), "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(m, []byte("signed_record\"")) || bytes.Contains(m, []byte("object_hashes")) {
		t.Errorf("manifest still carries the record:\n%s", m)
	}
	if fi, err := os.Stat(filepath.Join(s.JobDir(j), ProvenanceRecordFile)); err != nil || fi.Mode().Perm() != 0600 {
		t.Fatalf("sidecar: %v %v", fi, err)
	}

	if err := s.Transition(context.Background(), j, job.StateStaging); err != nil {
		t.Fatal(err)
	}
	if err := s.ResetForRecovery(j, false); err != nil {
		t.Fatal(err)
	}
	back, err := s.ReadManifest(s.JobDir(j))
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.ReadProvenanceRecord(back)
	if err != nil || !bytes.Equal(got, signed) {
		t.Fatalf("ReadProvenanceRecord = %q, %v; want the signed bytes", got, err)
	}

	// A sidecar that no longer matches its hash is an error, not a record.
	if err := os.WriteFile(filepath.Join(s.JobDir(j), ProvenanceRecordFile), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadProvenanceRecord(back); err == nil {
		t.Error("tampered sidecar accepted")
	}
}

// TestProvenanceRecord_None: a job without a sidecar has no record.
func TestProvenanceRecord_None(t *testing.T) {
	s := newTestSpool(t)
	for _, j := range []*job.Job{{ID: "a"}, {ID: "b", Provenance: &job.Provenance{RekorUUID: "u"}}} {
		if got, err := s.ReadProvenanceRecord(j); got != nil || err != nil {
			t.Errorf("%s: %q, %v; want nil, nil", j.ID, got, err)
		}
	}
}
