// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Package receiver implements the Stratum 1 pull receiver agent.
//
// The receiver connects outbound to the publisher's MQTT control plane and
// pulls CAS objects from Stratum 0 into its local CAS: on an announce it fetches
// the transaction manifest and pulls the missing objects before the catalog
// flip; on a (retained) published message it fetches the new root catalog.
// Its only inbound listener is a plain-HTTP /metrics endpoint.
//
// See REFERENCE.md (Pull Distribution Protocol) for the protocol.
package receiver

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"cvmfs.io/prepub/internal/broker"
	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/internal/distribute/puller"
	"cvmfs.io/prepub/pkg/observe"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Config holds the configuration for the receiver agent.
type Config struct {
	// ControlAddr is the plain-HTTP listen address of the /metrics endpoint.
	// Defaults to ":9100".
	ControlAddr string

	// CASRoot is the local CAS root directory where received objects are stored.
	// Objects are written to {CASRoot}/{hash[0:2]}/{hash}C.
	CASRoot string

	// NodeID is the stable identifier for this receiver node (MQTT client id
	// and presence topic). Must be non-empty when BrokerURL is set.
	NodeID string

	// Repos is the list of CVMFS repository names served by this receiver
	// (e.g. ["atlas.cern.ch", "cms.cern.ch"]). Empty accepts every repo.
	Repos []string

	// Stratum0URL is the cvmfs-prepub publisher base URL (e.g.
	// "http://stratum0:8080"). Manifests and bundles are fetched from
	// {Stratum0URL}/s1/..., post-commit objects from
	// {Stratum0URL}/cvmfs/{repo}/data/.... Empty disables both pulls.
	Stratum0URL string

	// BrokerURL is the MQTT broker address (learned from discovery). When
	// non-empty the receiver connects, publishes a retained presence message,
	// and subscribes to the announce and published topics. Empty disables MQTT.
	BrokerURL string

	// BrokerCACert is the path to the PEM-encoded CA certificate used to
	// verify the broker's server certificate.  When empty the system pool
	// is used.
	BrokerCACert string

	// PullConcurrency bounds parallel object transfers / bundle requests in pull
	// mode (0 = default 16).
	PullConcurrency int
	// PullFilesPerRequest sets objects per chunked-bundle request in pull mode:
	// >1 enables the bundle path; 0 or 1 keeps the per-object path.
	PullFilesPerRequest int
	// PullAuto measures RTT to Stratum 0 at startup and picks PullConcurrency /
	// PullFilesPerRequest from a latency-class table when they are left unset.
	PullAuto bool

	// BrokerCredentialsProvider, when set, supplies the MQTT username/password
	// (node id + a freshly-enrolled bearer token) on each broker (re)connect for
	// the token control plane; nil leaves the connection unauthenticated.
	BrokerCredentialsProvider func() (string, string)

	// Obs provides structured logging and metrics.
	Obs *observe.Provider
}

// Receiver runs the pull receiver agent.
type Receiver struct {
	cfg          Config
	casStore     cas.Backend        // local CAS used to compute the absent-hash set
	mqttMu       sync.RWMutex       // guards mqttClient
	mqttClient   *broker.Client     // nil when BrokerURL is empty; always access under mqttMu
	metrics      *http.Server       // serves /metrics for Prometheus scraping
	bgCtx        context.Context    // cancelled by Shutdown to stop background goroutines
	bgCancel     context.CancelFunc // cancels bgCtx
	shutdownOnce sync.Once          // ensures Shutdown can be safely called multiple times

	// httpClient is used for outbound S0 fetch requests triggered by
	// PublishedMessage notifications.  Initialised once in New() and shared
	// across all fetch goroutines.
	httpClient *http.Client

	// s0PullMu is a per-repo map that prevents concurrent S0 pull goroutines
	// for the same repository.  The value is a *sync.Mutex that the pull
	// goroutine tries to TryLock; if it cannot, the notification is dropped
	// (the in-progress pull will fetch the latest objects anyway).
	//
	// Key: repo name string.  Value: *sync.Mutex.
	s0PullMu sync.Map

	// pullCoordinator drives all pulls; nil when Stratum0URL is empty.
	pullCoordinator *puller.Coordinator
	// pullSem bounds concurrent pull goroutines; pullInflight coalesces
	// concurrent pulls of the same transaction. Both used only in pull mode.
	pullSem      chan struct{}
	pullInflight sync.Map
}

// New creates a Receiver from cfg but does not start any listeners.
// Call Start to begin serving.
func New(cfg Config) (*Receiver, error) {
	if cfg.CASRoot == "" {
		return nil, fmt.Errorf("receiver: CASRoot must not be empty")
	}
	if cfg.ControlAddr == "" {
		cfg.ControlAddr = ":9100"
	}
	for _, repo := range cfg.Repos {
		if err := broker.ValidateRepo(repo); err != nil {
			return nil, fmt.Errorf("receiver: %w", err)
		}
	}

	// Build the local CAS backend used to answer "do I already hold this hash?"
	// during announce processing.  This is the single source of truth for the
	// absent-hash computation that replaces the former inventory Bloom filter.
	casStore, err := cas.NewLocalFS(cfg.CASRoot)
	if err != nil {
		return nil, fmt.Errorf("receiver: CAS at %s: %w", cfg.CASRoot, err)
	}
	bgCtx, bgCancel := context.WithCancel(context.Background())
	r := &Receiver{
		cfg:      cfg,
		casStore: casStore,
		bgCtx:    bgCtx,
		bgCancel: bgCancel,
		httpClient: &http.Client{
			Timeout: 5 * time.Minute, // generous for large CAS objects
			// A dedicated transport sized for the pull worker pool. The nil
			// default (http.DefaultTransport) keeps only 2 idle connections
			// per host, so with N concurrent per-object fetches against the
			// single S0 host every connection beyond 2 was closed after one
			// response — a TIME_WAIT flood that exhausted ephemeral ports on
			// large builds ("dial tcp: cannot assign requested address",
			// thousands of failed objects per pull transaction).
			Transport: &http.Transport{
				Proxy:               http.ProxyFromEnvironment,
				MaxIdleConns:        128,
				MaxIdleConnsPerHost: 128, // >= max pull concurrency
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}

	// Build the coordinator that fetches manifests and pulls missing objects
	// into the local CAS (announce and published paths).
	if cfg.Stratum0URL != "" {
		store := casStore
		// Resolve transfer tuning: explicit flags win; --pull-auto fills any unset
		// knob from a measured-RTT latency class; otherwise sensible defaults.
		n := cfg.PullConcurrency
		k := cfg.PullFilesPerRequest
		if cfg.PullAuto && (n == 0 || k == 0) {
			rtt := probeRTT(bgCtx, r.httpClient, cfg.Stratum0URL)
			an, ak := autoTune(rtt)
			if n == 0 {
				n = an
			}
			if k == 0 {
				k = ak
			}
			cfg.Obs.Logger.Info("pull: auto-tuned transfer parameters from RTT",
				"rtt", rtt.String(), "concurrency", n, "files_per_request", k)
		}
		if n == 0 {
			n = 16
		}
		if k == 0 {
			k = 1
		}
		mode := "per-object"
		if k > 1 {
			mode = "chunked-bundle"
		}
		r.pullCoordinator = &puller.Coordinator{
			ManifestBase: cfg.Stratum0URL,
			BundleBase:   cfg.Stratum0URL,
			Client:       r.httpClient,
			Puller: &puller.Puller{
				Store:           store,
				Fetcher:         &puller.HTTPFetcher{Client: r.httpClient},
				Slots:           n,
				FilesPerRequest: k,
				Client:          r.httpClient,
			},
		}
		cfg.Obs.Logger.Info("pull: transfer tuning",
			"concurrency", n, "files_per_request", k, "mode", mode, "auto", cfg.PullAuto)
		r.pullSem = make(chan struct{}, pullConcurrency)
	}

	// Metrics server: pull-mode distribution exposes only /metrics for
	// Prometheus scraping (the legacy HTTP push announce + object listeners are
	// gone). Use the observer's isolated registry so receiver-specific metrics
	// (cvmfs_receiver_*, pull_*) are visible; promhttp.Handler() would use the
	// process-global default registry, which does not contain them.
	metricsMux := http.NewServeMux()
	metricsHandler := promhttp.HandlerFor(cfg.Obs.Registry, promhttp.HandlerOpts{})
	metricsMux.Handle("/metrics", metricsHandler)

	r.metrics = &http.Server{
		Addr:         cfg.ControlAddr,
		Handler:      metricsMux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	return r, nil
}

// Start launches both HTTP servers and a background session-cleanup goroutine.
// It returns as soon as both listeners are bound; actual request handling
// continues in background goroutines.  Call Shutdown to stop the servers.
func (r *Receiver) Start() error {
	// Remove CAS temp files left by Puts interrupted in a previous run. The
	// cutoff is taken before MQTT starts, so no pull of this run is affected.
	// bgCtx lets Shutdown() interrupt the sweep promptly on a stalling filesystem.
	cutoff := time.Now()
	go func() {
		if err := sweepTmpFiles(r.bgCtx, r.cfg.CASRoot, cutoff, r.cfg.Obs.Logger.Info); err != nil &&
			err != context.Canceled {
			r.cfg.Obs.Logger.Warn("receiver: temp-file sweep failed", "error", err)
		}
	}()

	// Bind the /metrics listener (plain HTTP) for Prometheus scraping. This is
	// the only inbound HTTP surface in pull mode; the legacy push announce and
	// object listeners have been removed.
	metricsLn, err := net.Listen("tcp", r.cfg.ControlAddr)
	if err != nil {
		r.bgCancel()
		return fmt.Errorf("receiver: binding metrics listener %s: %w", r.cfg.ControlAddr, err)
	}
	go func() {
		if serveErr := r.metrics.Serve(metricsLn); serveErr != nil && serveErr != http.ErrServerClosed {
			r.cfg.Obs.Logger.Error("metrics listener error", "error", serveErr)
		}
	}()
	r.cfg.Obs.Logger.Info("receiver metrics listening", "addr", r.cfg.ControlAddr)

	// Start the MQTT control plane (no-op when BrokerURL is empty). This is how
	// the receiver learns of new transactions (announce) and commits (published)
	// and triggers its pulls.
	if err := r.startMQTT(); err != nil {
		// MQTT is the only trigger in pull mode; log loudly but do not abort so
		// the metrics endpoint stays up for diagnosis.
		r.cfg.Obs.Logger.Error("receiver: MQTT startup failed — no pull trigger active",
			"error", err)
	}

	return nil
}

// Shutdown gracefully stops both servers, waiting up to the deadline in ctx.
// Safe to call multiple times.
func (r *Receiver) Shutdown(ctx context.Context) error {
	var metricsErr error
	r.shutdownOnce.Do(func() {
		// Cancel background goroutines (e.g. the .tmp sweep) first so they can
		// exit before the process terminates.
		r.bgCancel()
		// Disconnect from the MQTT broker before closing the listener so an
		// explicit offline presence is published before the TCP connection closes.
		r.stopMQTT()
		// Shut down the metrics listener.
		metricsErr = r.metrics.Shutdown(ctx)
	})
	return metricsErr
}

// Addrs returns the actual listen address of the metrics endpoint after Start
// has been called. Useful in tests where ":0" lets the OS assign a free port.
func (r *Receiver) Addrs() (metricsAddr string) {
	return r.cfg.ControlAddr
}
