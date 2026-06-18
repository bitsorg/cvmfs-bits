// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package receiver

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"
)

// probeRTT estimates the round-trip time to Stratum 0 by timing a few cheap GETs
// to its health endpoint and taking the best (least-loaded) sample. Returns 0 on
// failure, which autoTune treats as "very low RTT".
func probeRTT(ctx context.Context, client *http.Client, base string) time.Duration {
	if base == "" || client == nil {
		return 0
	}
	url := strings.TrimRight(base, "/") + "/api/v1/health"
	var best time.Duration
	for i := 0; i < 3; i++ {
		start := time.Now()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return best
		}
		resp, err := client.Do(req)
		if err != nil {
			return best
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		d := time.Since(start)
		if best == 0 || d < best {
			best = d
		}
	}
	return best
}

// autoTune maps a measured RTT to (concurrency, filesPerRequest) using the
// operator's four S1 latency classes: high RTT → bigger bundles to amortize the
// round-trip; low RTT → per-file with more parallelism (best failure isolation).
func autoTune(rtt time.Duration) (concurrency, filesPerRequest int) {
	switch {
	case rtt < 5*time.Millisecond: // CERN (<1ms)
		return 32, 1
	case rtt < 50*time.Millisecond: // EU (~20ms)
		return 16, 8
	case rtt < 150*time.Millisecond: // US (~100ms)
		return 8, 32
	default: // Asia (>200ms)
		return 8, 64
	}
}
