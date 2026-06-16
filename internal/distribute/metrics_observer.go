// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package distribute

import (
	"cvmfs.io/prepub/internal/distribute/commit"
	"cvmfs.io/prepub/pkg/observe"
)

// metricsObserver adapts *observe.Metrics to commit.Observer so the three-phase
// commit orchestrator (ADR-0001 P3) feeds the publisher-side pull-distribution
// metrics surfaced at /api/v1/metrics. It is the wiring glue kept out of the
// dependency-free commit package.
type metricsObserver struct {
	m *observe.Metrics
}

// NewMetricsObserver returns a commit.Observer backed by m (nil-safe: a nil m
// yields a no-op observer).
func NewMetricsObserver(m *observe.Metrics) commit.Observer {
	return &metricsObserver{m: m}
}

func (o *metricsObserver) Prepared(string) {}

func (o *metricsObserver) Warmed(_ string, quorum bool) {
	if o.m == nil {
		return
	}
	result := "timeout"
	if quorum {
		result = "reached"
	}
	o.m.DistWarmQuorum.WithLabelValues(result).Inc()
}

func (o *metricsObserver) Committed(string) {
	if o.m == nil {
		return
	}
	o.m.DistTxn.WithLabelValues("committed").Inc()
}

func (o *metricsObserver) Aborted(string) {
	if o.m == nil {
		return
	}
	o.m.DistTxn.WithLabelValues("aborted").Inc()
}

var _ commit.Observer = (*metricsObserver)(nil)
