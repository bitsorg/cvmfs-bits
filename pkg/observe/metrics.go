package observe

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	JobsSubmitted              prometheus.Counter
	JobsCompleted              prometheus.Counter
	JobsFailed                 prometheus.Counter
	JobsRecovered              prometheus.Counter
	JobFailuresByClass         *prometheus.CounterVec
	PipelineFilesProcessed     prometheus.Counter
	PipelineBytesCompressed    prometheus.Counter
	PipelineDedupHits          prometheus.Counter
	CASUploadDuration          prometheus.Histogram
	LeaseAcquireDuration       prometheus.Histogram
	DistributionDuration       *prometheus.HistogramVec
	SpoolTransitions           *prometheus.CounterVec
	LeaseHeartbeatErrors       prometheus.Counter
	PipelineAbortCount         prometheus.Counter
	CASObjectCount             prometheus.Gauge
	CASBytesUsed               prometheus.Gauge
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
			Help: "Total number of deduplication hits.",
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
		DistributionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cvmfs_prepub_distribution_duration_seconds",
			Help:    "Duration of distribution to Stratum 1.",
			Buckets: prometheus.DefBuckets,
		}, []string{"stratum1"}),
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
		CASObjectCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cvmfs_prepub_cas_object_count",
			Help: "Current number of objects in CAS.",
		}),
		CASBytesUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cvmfs_prepub_cas_bytes_used",
			Help: "Current bytes used in CAS.",
		}),
	}
}

// MustRegister registers all metrics with a registerer, panicking on error.
func (m *Metrics) MustRegister(reg prometheus.Registerer) {
	reg.MustRegister(
		m.JobsSubmitted,
		m.JobsCompleted,
		m.JobsFailed,
		m.JobsRecovered,
		m.JobFailuresByClass,
		m.PipelineFilesProcessed,
		m.PipelineBytesCompressed,
		m.PipelineDedupHits,
		m.CASUploadDuration,
		m.LeaseAcquireDuration,
		m.DistributionDuration,
		m.SpoolTransitions,
		m.LeaseHeartbeatErrors,
		m.PipelineAbortCount,
		m.CASObjectCount,
		m.CASBytesUsed,
	)
}
