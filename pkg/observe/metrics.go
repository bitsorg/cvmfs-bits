// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package observe

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	JobsSubmitted           prometheus.Counter
	JobsCompleted           prometheus.Counter
	PublishedBytes          prometheus.Counter
	JobsFailed              prometheus.Counter
	JobsRecovered           prometheus.Counter
	JobFailuresByClass      *prometheus.CounterVec
	PipelineFilesProcessed  prometheus.Counter
	PipelineBytesCompressed prometheus.Counter
	PipelineDedupHits       prometheus.Counter
	CASUploadDuration       prometheus.Histogram
	LeaseAcquireDuration    prometheus.Histogram
	SpoolTransitions        *prometheus.CounterVec
	LeaseHeartbeatErrors    prometheus.Counter
	PipelineAbortCount      prometheus.Counter

	// Per-phase job duration histograms.
	// Label "phase" takes values:
	//   pipeline       — tar unpack + compress + dedup + CAS upload
	//   subtree_build  — subtree catalog build and CAS upload
	//   submit_payload — upload of the subtree catalog(s) to the gateway
	//   manifest_fetch — .cvmfspublished fetch for old_root_hash
	//   commit         — gateway commit round-trip
	//   total_s0       — wall time from job submission to StatePublished
	JobPhaseDuration *prometheus.HistogramVec

	// ── pull-based distribution ─────────────────────────────────────────────
	// Receiver (Stratum 1) side.
	PullTransactions *prometheus.CounterVec // result=warmed|failed
	PullObjects      *prometheus.CounterVec // result=fetched|skipped|failed
	PullDuration     prometheus.Histogram   // per-transaction warming wall time
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	return &Metrics{
		JobsSubmitted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cvmfs_prepub_jobs_submitted_total",
			Help: "Total number of jobs submitted.",
		}),
		JobsCompleted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cvmfs_prepub_jobs_completed_total",
			Help: "Total number of jobs completed successfully.",
		}),
		PublishedBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cvmfs_prepub_published_bytes_total",
			Help: "Payload bytes of published jobs (the submitted tar; uncompressed content when there is none).",
		}),
		JobsFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cvmfs_prepub_jobs_failed_total",
			Help: "Total number of jobs that failed.",
		}),
		JobsRecovered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cvmfs_prepub_jobs_recovered_total",
			Help: "Total number of jobs reset and re-queued via recovery.",
		}),
		JobFailuresByClass: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cvmfs_prepub_job_failures_by_class_total",
			Help: "Job failures broken down by error class (transient, permanent, internal).",
		}, []string{"class"}),
		PipelineFilesProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cvmfs_prepub_pipeline_files_processed_total",
			Help: "Total number of files processed through the pipeline.",
		}),
		PipelineBytesCompressed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cvmfs_prepub_pipeline_bytes_compressed_total",
			Help: "Total bytes compressed in the pipeline.",
		}),
		PipelineDedupHits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cvmfs_prepub_pipeline_dedup_hits_total",
			Help: "Total number of deduplication hits (Bloom filter + CAS confirmed).",
		}),
		CASUploadDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "cvmfs_prepub_cas_upload_duration_seconds",
			Help:    "Duration of CAS uploads.",
			Buckets: prometheus.DefBuckets,
		}),
		LeaseAcquireDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "cvmfs_prepub_lease_acquire_duration_seconds",
			Help:    "Duration of lease acquisition.",
			Buckets: prometheus.DefBuckets,
		}),
		SpoolTransitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cvmfs_prepub_spool_transitions_total",
			Help: "Total number of spool state transitions.",
		}, []string{"from", "to"}),
		LeaseHeartbeatErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cvmfs_prepub_lease_heartbeat_errors_total",
			Help: "Total lease heartbeat errors.",
		}),
		PipelineAbortCount: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cvmfs_prepub_pipeline_abort_count_total",
			Help: "Total number of aborted pipelines.",
		}),
		JobPhaseDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "cvmfs_prepub_job_phase_seconds",
			Help: "Wall-clock duration of each job processing phase.",
			// Buckets cover 0.1 s → ~17 minutes; well-suited for gateway round-trips
			// (sub-second) through full pipeline + distribution runs (minutes).
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 15), // 0.1s … 1638s
		}, []string{"phase"}),

		// ── pull distribution ──
		PullTransactions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cvmfs_receiver_pull_transactions_total",
			Help: "Pull-warming attempts by outcome (result=warmed|failed).",
		}, []string{"result"}),
		PullObjects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cvmfs_receiver_pull_objects_total",
			Help: "Objects handled during pull warming (result=fetched|skipped|failed).",
		}, []string{"result"}),
		PullDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "cvmfs_receiver_pull_duration_seconds",
			Help:    "Wall time to warm one transaction by pulling its missing objects.",
			Buckets: prometheus.ExponentialBuckets(0.05, 2, 12),
		}),
	}
}

// MustRegister registers all metrics with a registerer, panicking on error.
func (m *Metrics) MustRegister(reg prometheus.Registerer) {
	reg.MustRegister(
		m.JobsSubmitted,
		m.JobsCompleted,
		m.PublishedBytes,
		m.JobsFailed,
		m.JobsRecovered,
		m.JobFailuresByClass,
		m.PipelineFilesProcessed,
		m.PipelineBytesCompressed,
		m.PipelineDedupHits,
		m.CASUploadDuration,
		m.LeaseAcquireDuration,
		m.SpoolTransitions,
		m.LeaseHeartbeatErrors,
		m.PipelineAbortCount,
		m.JobPhaseDuration,
		m.PullTransactions,
		m.PullObjects,
		m.PullDuration,
	)
}
