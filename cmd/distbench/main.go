// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Command distbench drives the pull-distribution bundling benchmark (ADR-0001
// P-A). It sweeps a set of simulated round-trip latencies for a fixed object
// fan-out and prints, for each, the per-object vs bundled cost and the go/no-go
// verdict — the evidence for whether to ship object bundling.
//
//	distbench -objects 2000 -slots 8 -rtts 0,1ms,5ms,20ms,50ms -threshold 1.5
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"cvmfs.io/prepub/internal/distribute/bench"
)

func main() {
	objects := flag.Int("objects", 2000, "number of objects in the synthetic delta")
	slots := flag.Int("slots", 8, "per-object fetch concurrency for the per-object mode")
	seed := flag.Int64("seed", 1, "RNG seed (reproducibility)")
	threshold := flag.Float64("threshold", 1.5, "minimum speedup for a 'bundle' verdict")
	rttsFlag := flag.String("rtts", "0,1ms,5ms,20ms,50ms", "comma-separated simulated RTTs")
	flag.Parse()

	rtts, err := parseDurations(*rttsFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bad -rtts:", err)
		os.Exit(2)
	}

	fmt.Printf("Bundling benchmark — objects=%d slots=%d threshold=%.2fx\n\n", *objects, *slots, *threshold)
	fmt.Printf("%-8s  %12s  %12s  %9s  %9s  %s\n", "RTT", "per-object", "bundled", "speedup", "reqs", "verdict")
	fmt.Println(strings.Repeat("-", 78))

	anyBundle := false
	for _, rtt := range rtts {
		c, err := bench.Run(context.Background(), bench.Scenario{
			Objects: *objects, Seed: *seed, RTT: rtt, Slots: *slots,
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, "run failed:", err)
			os.Exit(1)
		}
		rec := c.Decide(*threshold)
		anyBundle = anyBundle || rec.Bundle
		verdict := "per-object"
		if rec.Bundle {
			verdict = "BUNDLE"
		}
		fmt.Printf("%-8s  %12s  %12s  %8.2fx  %4d→%-4d  %s\n",
			rtt, round(c.PerObject.Duration), round(c.Bundled.Duration),
			c.Speedup(), c.PerObject.Requests, c.Bundled.Requests, verdict)
	}

	fmt.Println()
	if anyBundle {
		fmt.Printf("Recommendation: ship bundling — it clears the %.2fx bar at the higher RTTs above,\n"+
			"where per-object round-trips dominate. Keep per-object for same-datacentre (low-RTT) peers.\n", *threshold)
	} else {
		fmt.Printf("Recommendation: per-object is sufficient at every tested RTT (never reached %.2fx).\n", *threshold)
	}
}

func parseDurations(s string) ([]time.Duration, error) {
	var out []time.Duration
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		// Bare "0" is a valid duration; everything else needs a unit.
		d, err := time.ParseDuration(part)
		if err != nil {
			return nil, fmt.Errorf("%q: %w", part, err)
		}
		out = append(out, d)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no RTTs given")
	}
	return out, nil
}

func round(d time.Duration) string {
	switch {
	case d >= time.Second:
		return d.Round(time.Millisecond).String()
	default:
		return d.Round(100 * time.Microsecond).String()
	}
}
