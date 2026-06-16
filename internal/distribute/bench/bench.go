// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Package bench is the measurement harness behind ADR-0001's open question P-A:
// is it worth bundling many small CAS objects into one request, or is per-object
// HTTP good enough? It builds a synthetic object set with a realistic small-file
// size distribution, serves it over a loopback HTTP server with a configurable
// simulated round-trip latency, then pulls the whole set both ways — one request
// per object (ObjectHandler + Puller.Pull) versus one bundled request
// (BundleHandler + Puller.PullBundle) — and reports a side-by-side comparison
// plus a go/no-go recommendation.
//
// The decisive variable is latency: per-object cost grows with
// ceil(objects/slots) round-trips, whereas a bundle pays a single round-trip.
// Running the harness across a few RTTs shows the crossover for a given fan-out.
package bench

import (
	"bytes"
	"context"
	"crypto/sha1" //nolint:gosec // CVMFS CAS key algorithm
	"encoding/hex"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"time"

	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/internal/distribute/manifest"
	"cvmfs.io/prepub/internal/distribute/puller"
	"cvmfs.io/prepub/internal/distribute/serve"
)

// Scenario parameterises one comparison run.
type Scenario struct {
	Objects int           // number of objects in the delta
	Seed    int64         // RNG seed (reproducibility)
	RTT     time.Duration // simulated per-request latency
	Slots   int           // per-object fetch concurrency (Puller.Slots); default 8
}

func (s Scenario) slots() int {
	if s.Slots > 0 {
		return s.Slots
	}
	return 8
}

// Stat captures one mode's measured cost.
type Stat struct {
	Mode     string
	Objects  int
	Bytes    int64
	Requests int64
	Duration time.Duration
}

// ThroughputMBs returns transfer throughput in MB/s.
func (s Stat) ThroughputMBs() float64 {
	if s.Duration <= 0 {
		return 0
	}
	return (float64(s.Bytes) / 1e6) / s.Duration.Seconds()
}

// Comparison is the result of a Run.
type Comparison struct {
	Scenario  Scenario
	PerObject Stat
	Bundled   Stat
}

// Speedup is per-object duration / bundled duration (>1 means bundling is faster).
func (c Comparison) Speedup() float64 {
	if c.Bundled.Duration <= 0 {
		return 0
	}
	return float64(c.PerObject.Duration) / float64(c.Bundled.Duration)
}

// RequestReduction is the fraction of requests bundling eliminates (0..1).
func (c Comparison) RequestReduction() float64 {
	if c.PerObject.Requests == 0 {
		return 0
	}
	return 1 - float64(c.Bundled.Requests)/float64(c.PerObject.Requests)
}

// Recommendation is the go/no-go verdict.
type Recommendation struct {
	Bundle bool
	Reason string
}

// Decide returns a recommendation: bundle when it is at least minSpeedup times
// faster. The request-reduction figure is included in the rationale because it is
// the latency-independent driver of the speedup.
func (c Comparison) Decide(minSpeedup float64) Recommendation {
	sp := c.Speedup()
	if sp >= minSpeedup {
		return Recommendation{Bundle: true, Reason: fmt.Sprintf(
			"bundling is %.2fx faster (>= %.2fx threshold) at RTT=%s, cutting requests %d→%d (%.0f%% fewer)",
			sp, minSpeedup, c.Scenario.RTT, c.PerObject.Requests, c.Bundled.Requests, 100*c.RequestReduction())}
	}
	return Recommendation{Bundle: false, Reason: fmt.Sprintf(
		"per-object is adequate: bundling only %.2fx faster (< %.2fx threshold) at RTT=%s",
		sp, minSpeedup, c.Scenario.RTT)}
}

// Run executes one scenario and returns the comparison. It creates and cleans up
// its own temporary CAS directories.
func Run(ctx context.Context, sc Scenario) (Comparison, error) {
	srcDir, err := os.MkdirTemp("", "bench-src-")
	if err != nil {
		return Comparison{}, err
	}
	defer os.RemoveAll(srcDir)
	src, err := cas.NewLocalFS(srcDir)
	if err != nil {
		return Comparison{}, err
	}

	m, totalBytes, err := generate(ctx, src, sc)
	if err != nil {
		return Comparison{}, err
	}

	var reqs int64
	count := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt64(&reqs, 1)
			if sc.RTT > 0 {
				select {
				case <-time.After(sc.RTT):
				case <-r.Context().Done():
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
	mux := http.NewServeMux()
	mux.Handle("/cvmfs/", &serve.ObjectHandler{Store: src})
	mux.Handle("/s1/bundle", &serve.BundleHandler{Store: src})
	ts := httptest.NewServer(count(mux))
	defer ts.Close()

	repo := m.Repo
	m.BaseURLs = []string{ts.URL + "/cvmfs/" + repo + "/data"}

	// ── per-object ──
	atomic.StoreInt64(&reqs, 0)
	dst1, clean1, err := tempCAS()
	if err != nil {
		return Comparison{}, err
	}
	defer clean1()
	p1 := &puller.Puller{Store: dst1, Fetcher: &puller.HTTPFetcher{}, Slots: sc.slots()}
	t0 := time.Now()
	r1, err := p1.Pull(ctx, m)
	d1 := time.Since(t0)
	if err != nil {
		return Comparison{}, fmt.Errorf("per-object pull: %w", err)
	}
	perObject := Stat{Mode: "per-object", Objects: r1.Fetched, Bytes: totalBytes, Requests: atomic.LoadInt64(&reqs), Duration: d1}

	// ── bundled ──
	atomic.StoreInt64(&reqs, 0)
	dst2, clean2, err := tempCAS()
	if err != nil {
		return Comparison{}, err
	}
	defer clean2()
	p2 := &puller.Puller{Store: dst2}
	t1 := time.Now()
	r2, err := p2.PullBundle(ctx, ts.URL+"/s1/bundle", m)
	d2 := time.Since(t1)
	if err != nil {
		return Comparison{}, fmt.Errorf("bundled pull: %w", err)
	}
	bundled := Stat{Mode: "bundled", Objects: r2.Fetched, Bytes: totalBytes, Requests: atomic.LoadInt64(&reqs), Duration: d2}

	return Comparison{Scenario: sc, PerObject: perObject, Bundled: bundled}, nil
}

// generate fills src with sc.Objects synthetic objects (realistic small-file size
// mix) and returns a manifest over them plus the total byte count.
func generate(ctx context.Context, src cas.Backend, sc Scenario) (*manifest.Manifest, int64, error) {
	rng := rand.New(rand.NewSource(sc.Seed)) //nolint:gosec // deterministic test data
	m := &manifest.Manifest{
		TransactionID:  "bench",
		Repo:           "bench.cern.ch",
		TargetRootHash: "benchroot",
		Generator:      manifest.GeneratorDiff,
		Auth:           manifest.AuthPublic,
		Objects:        make([]manifest.ObjRef, 0, sc.Objects),
	}
	var total int64
	for i := 0; i < sc.Objects; i++ {
		size := sampleSize(rng)
		data := make([]byte, size)
		rng.Read(data)
		sum := sha1.Sum(data) //nolint:gosec
		h := hex.EncodeToString(sum[:])
		if err := src.Put(ctx, h, bytes.NewReader(data), int64(size)); err != nil {
			return nil, 0, err
		}
		m.Objects = append(m.Objects, manifest.ObjRef{Hash: h, Size: int64(size)})
		total += int64(size)
	}
	m.TotalSize = total
	return m, total, nil
}

// sampleSize draws a compressed-object size from a small-file-heavy mixture
// resembling a CVMFS software repository.
func sampleSize(rng *rand.Rand) int {
	switch x := rng.Float64(); {
	case x < 0.80: // 80%: 1–16 KiB
		return 1024 + rng.Intn(15*1024)
	case x < 0.95: // 15%: 16–128 KiB
		return 16*1024 + rng.Intn(112*1024)
	default: // 5%: 128 KiB–1 MiB
		return 128*1024 + rng.Intn(896*1024)
	}
}

func tempCAS() (cas.Backend, func(), error) {
	dir, err := os.MkdirTemp("", "bench-dst-")
	if err != nil {
		return nil, nil, err
	}
	c, err := cas.NewLocalFS(dir)
	if err != nil {
		os.RemoveAll(dir)
		return nil, nil, err
	}
	return c, func() { os.RemoveAll(dir) }, nil
}
