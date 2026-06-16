// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package bench

import (
	"context"
	"testing"
	"time"
)

func TestRunCorrectnessAndRequestCounts(t *testing.T) {
	c, err := Run(context.Background(), Scenario{Objects: 120, Seed: 1, RTT: 0, Slots: 8})
	if err != nil {
		t.Fatal(err)
	}
	// Both modes must fetch every object.
	if c.PerObject.Objects != 120 || c.Bundled.Objects != 120 {
		t.Fatalf("objects fetched: per-object=%d bundled=%d, want 120", c.PerObject.Objects, c.Bundled.Objects)
	}
	// The whole point of bundling: one request instead of one-per-object.
	if c.Bundled.Requests != 1 {
		t.Fatalf("bundled requests = %d, want 1", c.Bundled.Requests)
	}
	if c.PerObject.Requests < 120 {
		t.Fatalf("per-object requests = %d, want >= 120", c.PerObject.Requests)
	}
	if rr := c.RequestReduction(); rr < 0.9 {
		t.Fatalf("request reduction = %.2f, want >= 0.9", rr)
	}
}

func TestBundlingWinsUnderLatency(t *testing.T) {
	// With a meaningful RTT the per-object path pays ~ceil(N/slots) round-trips
	// while the bundle pays one, so bundling should be clearly faster.
	c, err := Run(context.Background(), Scenario{Objects: 200, Seed: 2, RTT: 8 * time.Millisecond, Slots: 8})
	if err != nil {
		t.Fatal(err)
	}
	if sp := c.Speedup(); sp < 1.5 {
		t.Fatalf("speedup under RTT = %.2fx, want >= 1.5x (perObj=%s bundled=%s)",
			sp, c.PerObject.Duration, c.Bundled.Duration)
	}
	rec := c.Decide(1.5)
	if !rec.Bundle {
		t.Fatalf("decision should recommend bundling under latency: %s", rec.Reason)
	}
	t.Logf("RTT=%s: %s", c.Scenario.RTT, rec.Reason)
}

func TestNoLatencyDecisionIsModest(t *testing.T) {
	// At zero RTT the request-count win does not translate to a large speedup, so
	// a high threshold should advise against bundling — the harness must not be
	// rigged to always say "bundle".
	c, err := Run(context.Background(), Scenario{Objects: 150, Seed: 3, RTT: 0, Slots: 8})
	if err != nil {
		t.Fatal(err)
	}
	rec := c.Decide(3.0)
	t.Logf("RTT=0: speedup=%.2fx reqReduction=%.0f%% → bundle=%v (%s)",
		c.Speedup(), 100*c.RequestReduction(), rec.Bundle, rec.Reason)
	// No hard assertion on Bundle here (timing-dependent); the test documents that
	// Decide is threshold-driven, and that request reduction is ~total even when
	// the speedup is modest.
	if c.RequestReduction() < 0.9 {
		t.Fatalf("request reduction should still be high: %.2f", c.RequestReduction())
	}
}
