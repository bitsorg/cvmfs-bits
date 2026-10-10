// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package spool

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"cvmfs.io/prepub/internal/job"
)

func TestCollector(t *testing.T) {
	s := newTestSpool(t)
	due := time.Now().Add(time.Hour)
	for _, j := range []*job.Job{
		{ID: "a", State: job.StateIncoming},
		{ID: "b", State: job.StateIncoming, NextAttemptAt: &due},
		{ID: "c", State: job.StateCommitting},
		{ID: "d", State: job.StatePublished},
		{ID: "e", State: job.StatePublished},
		{ID: "f", State: job.StateFailed},
	} {
		if err := s.WriteManifest(j); err != nil {
			t.Fatal(err)
		}
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewCollector(s))

	want := `
# HELP cvmfs_prepub_spool_jobs Jobs in each spool state.
# TYPE cvmfs_prepub_spool_jobs gauge
cvmfs_prepub_spool_jobs{state="aborted"} 0
cvmfs_prepub_spool_jobs{state="accumulated"} 0
cvmfs_prepub_spool_jobs{state="committing"} 1
cvmfs_prepub_spool_jobs{state="distributing"} 0
cvmfs_prepub_spool_jobs{state="failed"} 1
cvmfs_prepub_spool_jobs{state="incoming"} 2
cvmfs_prepub_spool_jobs{state="leased"} 0
cvmfs_prepub_spool_jobs{state="published"} 2
cvmfs_prepub_spool_jobs{state="staging"} 0
cvmfs_prepub_spool_jobs{state="uploading"} 0
# HELP cvmfs_prepub_spool_jobs_waiting_retry Incoming jobs waiting to retry a failed attempt.
# TYPE cvmfs_prepub_spool_jobs_waiting_retry gauge
cvmfs_prepub_spool_jobs_waiting_retry 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want),
		"cvmfs_prepub_spool_jobs", "cvmfs_prepub_spool_jobs_waiting_retry"); err != nil {
		t.Error(err)
	}
	// Host and filesystem gauges are present (values are the test machine's).
	n, err := testutil.GatherAndCount(reg, "cvmfs_prepub_spool_fs_size_bytes",
		"cvmfs_prepub_host_cpus", "cvmfs_prepub_host_memory_total_bytes")
	if err != nil || n != 3 {
		t.Errorf("host gauges: %d, %v", n, err)
	}
}
