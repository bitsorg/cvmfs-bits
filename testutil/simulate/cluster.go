package simulate

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"cvmfs.io/prepub/internal/api"
	"cvmfs.io/prepub/internal/distribute"
	"cvmfs.io/prepub/internal/lease"
	"cvmfs.io/prepub/internal/notify"
	"cvmfs.io/prepub/internal/pipeline"
	"cvmfs.io/prepub/internal/spool"
	"cvmfs.io/prepub/pkg/observe"
	"cvmfs.io/prepub/testutil/fakecas"
	"cvmfs.io/prepub/testutil/fakegateway"
	"cvmfs.io/prepub/testutil/fakestratum1"
	testobserve "cvmfs.io/prepub/testutil/observe"
)

type Cluster struct {
	Gateway   *fakegateway.Gateway
	CAS       *fakecas.CAS
	Stratum1s []*fakestratum1.Stratum1
	Obs       *observe.Provider
	SpoolRoot string

	NetworkJitter  time.Duration
	GlobalFailRate float64
}

func NewCluster(t testing.TB, nStratum1 int) *Cluster {
	// Create a new Prometheus registry for this cluster
	reg := prometheus.NewRegistry()

	// Create observability provider with an in-memory span recorder for tests.
	recorder := &testobserve.SpanRecorder{}
	obs, shutdown, err := observe.New("test-cluster",
		observe.WithPrometheus(reg),
		observe.WithTestExporter(recorder),
	)
	if err != nil {
		t.Fatalf("failed to create observer: %v", err)
	}
	t.Cleanup(shutdown)

	// Create temporary spool directory
	tmpDir, err := os.MkdirTemp("", "cvmfs-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	// Create fake components
	gw := fakegateway.New(obs)
	cas := fakecas.New(obs)

	var stratum1s []*fakestratum1.Stratum1
	var endpoints []string
	for i := 0; i < nStratum1; i++ {
		s1 := fakestratum1.New(fmt.Sprintf("stratum1-%d", i), obs)
		stratum1s = append(stratum1s, s1)
		endpoints = append(endpoints, s1.URL())
	}

	return &Cluster{
		Gateway:   gw,
		CAS:       cas,
		Stratum1s: stratum1s,
		Obs:       obs,
		SpoolRoot: tmpDir,
	}
}

func (c *Cluster) NewOrchestrator(repo string) *api.Orchestrator {
	sp, err := spool.New(c.SpoolRoot, c.Obs)
	if err != nil {
		panic(err)
	}

	leaseClient := lease.NewClient(c.Gateway.URL(), "test-key", "test-secret", c.Obs)

	var distConfig *distribute.Config
	if len(c.Stratum1s) > 0 {
		var endpoints []string
		for _, s1 := range c.Stratum1s {
			endpoints = append(endpoints, s1.URL())
		}
		distConfig = &distribute.Config{
			Endpoints:   endpoints,
			Quorum:      1.0,
			Timeout:     5 * time.Second,
			Concurrency: 4,
			Obs:         c.Obs,
			DevMode:     true, // test servers use http://127.0.0.1
		}
	}

	return &api.Orchestrator{
		Spool:      sp,
		CAS:        c.CAS,
		Lease:      leaseClient,
		Distribute: distConfig,
		Pipeline: pipeline.Config{
			Workers:    2,
			UploadConc: 2,
			CAS:        c.CAS,
			SpoolDir:   c.SpoolRoot,
			Obs:        c.Obs,
		},
		Notify: notify.NewBus(),
		Obs:    c.Obs,
	}
}

func (c *Cluster) PartitionStratum1(name string) {
	for _, s1 := range c.Stratum1s {
		if s1.Name == name {
			s1.Partitioned = true
			return
		}
	}
}

func (c *Cluster) HealStratum1(name string) {
	for _, s1 := range c.Stratum1s {
		if s1.Name == name {
			s1.Partitioned = false
			return
		}
	}
}

func (c *Cluster) Close() {
	for _, s1 := range c.Stratum1s {
		s1.Close()
	}
	c.Gateway.Close()
	os.RemoveAll(c.SpoolRoot)
}

func (c *Cluster) SubmittedObjectCount() int {
	return c.CAS.ObjectCount()
}

func (c *Cluster) AllStratum1sHave(hash string) bool {
	for _, s1 := range c.Stratum1s {
		if !s1.Has(hash) {
			return false
		}
	}
	return true
}
