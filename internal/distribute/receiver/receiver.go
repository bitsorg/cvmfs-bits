// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Package receiver implements the Stratum 1 pre-warming receiver agent.
//
// The receiver accepts compressed CAS objects pushed by the cvmfs-prepub
// distributor before the catalog commit, so that Stratum 1 nodes already hold
// the new objects when the catalog flip occurs and replication becomes
// catalog-only rather than catalog-plus-objects.
//
// It runs two HTTP servers with deliberately different security properties:
//
//   - Control channel (HTTPS): handles announce requests authenticated with
//     HMAC-SHA256.  Protects the shared secret and session tokens.
//   - Data channel (plain HTTP): handles object PUT requests authenticated with
//     per-session bearer tokens.  SHA-256 hash verification at the receiver
//     provides transfer integrity without the CPU overhead of TLS on potentially
//     gigabytes of already-compressed objects.
//
// When BrokerURL is configured the HMAC/HTTP announce path is supplemented (or
// replaced) by an MQTT control plane: the receiver subscribes to announce
// topics for its configured repositories, computes the absent set by checking
// each announced hash directly against its local CAS, and publishes a
// ReadyMessage carrying its session token and the list of absent hashes — all
// without any inbound firewall rules (outbound
// TCP 8883 only).  The data channel (plain HTTP PUT) is unchanged.
//
// See REFERENCE.md §20 for the HTTP protocol and §21 for the MQTT control plane.
package receiver

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
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
	// ControlAddr is the HTTPS listen address for announce requests.
	// Defaults to ":9100".
	ControlAddr string

	// DataAddr is the plain-HTTP listen address for object PUTs.
	// Defaults to ":9101".
	DataAddr string

	// DataHost is the publicly reachable hostname or IP of this receiver,
	// used to construct the data_endpoint URL returned in announce responses.
	// If empty, the listener's local address is used (suitable for tests).
	DataHost string

	// TLSCert is the path to the TLS certificate for the control channel.
	// Required unless DevMode is true.
	TLSCert string

	// TLSKey is the path to the TLS private key for the control channel.
	// Required unless DevMode is true.
	TLSKey string

	// HMACSecret is the shared secret used to verify HMAC-SHA256 signatures on
	// announce requests.  Must be identical on the sender and all receivers.
	// Ignored when DevMode is true.
	HMACSecret string

	// CASRoot is the local CAS root directory where received objects are stored.
	// Objects are written to {CASRoot}/{hash[0:2]}/{hash}C.
	CASRoot string

	// SessionTTL controls how long a session remains valid after the announce.
	// Defaults to 1 hour.
	SessionTTL time.Duration

	// DiskHeadroom is the multiplier applied to the announced total_bytes when
	// checking available disk space.  Defaults to 1.2 (20 % margin).
	DiskHeadroom float64

	// DevMode disables TLS on the control channel and skips HMAC verification.
	// Never set in production; intended for integration tests only.
	DevMode bool

	// CoordURL is the base URL of the HepCDN coordination service
	// (e.g. "https://coord.hepcdn.example.com").  Empty disables coordination.
	CoordURL string

	// CoordToken is the bearer token used to authenticate with the coordination
	// service.  Typically read from the PREPUB_COORD_TOKEN environment variable.
	CoordToken string

	// NodeID is the stable identifier for this receiver node, used in
	// coordination service registration and heartbeat requests.
	// Defaults to the system hostname if empty.
	NodeID string

	// Repos is the list of CVMFS repository names served by this receiver
	// (e.g. ["atlas.cern.ch", "cms.cern.ch"]).  Sent during registration so
	// the coordination service can build the routing table.
	Repos []string

	// Stratum0URL is the HTTP base URL of the Stratum 0 server from which new
	// CAS objects are fetched when a PublishedMessage is received over MQTT.
	// Must include the /cvmfs path prefix, matching the convention used by the
	// publisher's Stratum0URL.
	// Example: "http://stratum0.example.org/cvmfs"
	// When empty, published-notification-triggered pulls are disabled: the
	// receiver still subscribes to the published topic and logs notifications,
	// but takes no action.  This is safe — objects pushed via the bits pipeline
	// are already present before the commit; the pull is only needed for objects
	// committed via the native ingest path.
	Stratum0URL string

	// BrokerURL is the MQTT broker address (e.g. "tls://broker.cern.ch:8883").
	// When non-empty the receiver connects to the broker, publishes a retained
	// presence message, and subscribes to announce topics for the configured
	// repositories.  This replaces the HTTP coordination service client for
	// discovery and presence tracking.  The HTTP announce endpoint remains
	// active for backward compatibility.
	// Leave empty to disable MQTT (default).
	BrokerURL string

	// BrokerClientCert is the path to the PEM-encoded client TLS certificate
	// for authenticating with the MQTT broker.
	BrokerClientCert string

	// BrokerClientKey is the path to the PEM-encoded client TLS private key.
	BrokerClientKey string

	// BrokerCACert is the path to the PEM-encoded CA certificate used to
	// verify the broker's server certificate.  When empty the system pool
	// is used.
	BrokerCACert string

	// MaxObjectSize is the maximum body size in bytes accepted for a single
	// PUT /api/v1/objects/{hash} request.  Requests exceeding this limit are
	// rejected with HTTP 413 before any bytes are written to disk.
	// Defaults to 1 GiB (1 << 30).  Set to 0 to use the default.
	MaxObjectSize int64

	// PullMode enables ADR-0001 pull-based distribution: on a prepare announce
	// the receiver fetches the transaction manifest and pulls the objects it is
	// missing, instead of waiting to be pushed to. Default false (legacy push).
	PullMode bool

	// PullManifestBase is the cvmfs-prepub base URL the receiver fetches
	// manifests from in pull mode (the manifest for a transaction is at
	// PullManifestBase + "/s1/{txn}/manifest"). Object locations come from the
	// manifest's own BaseURLs.
	PullManifestBase string

	// PullConcurrency bounds parallel object transfers / bundle requests in pull
	// mode (0 = default 16).
	PullConcurrency int
	// PullFilesPerRequest sets objects per chunked-bundle request in pull mode:
	// >1 enables the bundle path; 0 or 1 keeps the per-object path.
	PullFilesPerRequest int
	// PullAuto measures RTT to Stratum 0 at startup and picks PullConcurrency /
	// PullFilesPerRequest from a latency-class table when they are left unset.
	PullAuto bool

	// OnWarmed, when set, is invoked exactly once after each pull-mode warming
	// attempt completes (ADR-0001 D6). warmed is true only when every missing
	// object was fetched and verified, i.e. the receiver is warm for txn; the
	// publisher routes a true result into its WarmGate quorum. It MUST NOT block
	// (it runs on the pull goroutine); the broker publish it typically performs
	// is fire-and-forget. A nil hook disables the ack (pre-broker / tests).
	OnWarmed func(payloadID, repo string, warmed bool)

	// BrokerCredentialsProvider, when set, supplies the MQTT username/password
	// (node id + a freshly-enrolled bearer token) on each broker (re)connect for
	// the token control plane; nil leaves the connection unauthenticated.
	BrokerCredentialsProvider func() (string, string)

	// Obs provides structured logging and metrics.
	Obs *observe.Provider
}

// dataEndpoint returns the base URL of the plain-HTTP data channel as announced
// to senders in announce responses.
func (c *Config) dataEndpoint() string {
	host := c.DataHost
	if host == "" {
		host = "localhost"
	}
	_, port, _ := net.SplitHostPort(c.DataAddr)
	if port == "" {
		port = "9101"
	}
	return fmt.Sprintf("http://%s:%s", host, port)
}

// controlEndpoint returns the base URL of the HTTPS control channel for this
// receiver, used when registering with the coordination service so that the
// service can route announce requests to the node.
func (c *Config) controlEndpoint() string {
	host := c.DataHost // DataHost is the public hostname; same for both channels
	if host == "" {
		host = "localhost"
	}
	_, port, _ := net.SplitHostPort(c.ControlAddr)
	if port == "" {
		port = "9100"
	}
	scheme := "https"
	if c.DevMode {
		scheme = "http"
	}
	return fmt.Sprintf("%s://%s:%s", scheme, host, port)
}

// Receiver runs the two-channel pre-warming server.
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

	// pullCoordinator drives ADR-0001 pull-based warming; non-nil only when
	// cfg.PullMode is set.
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
	// HMACSecret is required unless DevMode disables HMAC verification.
	// TLS cert/key are only required at Start time when actually binding a server;
	// they are not checked here so that unit tests can call handlers directly.
	if !cfg.DevMode && cfg.HMACSecret == "" {
		return nil, fmt.Errorf("receiver: HMACSecret must not be empty (set DevMode to skip HMAC)")
	}
	if cfg.ControlAddr == "" {
		cfg.ControlAddr = ":9100"
	}
	if cfg.DataAddr == "" {
		cfg.DataAddr = ":9101"
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
		},
	}

	// Pull mode (ADR-0001): build the coordinator that fetches manifests and
	// pulls missing objects into the local CAS on announce.
	if cfg.PullMode {
		store := casStore
		// Resolve transfer tuning: explicit flags win; --pull-auto fills any unset
		// knob from a measured-RTT latency class; otherwise sensible defaults.
		n := cfg.PullConcurrency
		k := cfg.PullFilesPerRequest
		if cfg.PullAuto && (n == 0 || k == 0) {
			rtt := probeRTT(bgCtx, r.httpClient, cfg.PullManifestBase)
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
			ManifestBase: cfg.PullManifestBase,
			BundleBase:   cfg.PullManifestBase,
			Client:       r.httpClient,
			Puller: &puller.Puller{
				Store:           store,
				Fetcher:         &puller.HTTPFetcher{Client: r.httpClient},
				State:           puller.NewState(filepath.Join(cfg.CASRoot, ".bits-state")),
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
	// Remove any ".tmp" files left under the CAS by object fetches that were
	// interrupted by a previous crash, so they are not mistaken for objects.
	// bgCtx lets Shutdown() interrupt the sweep promptly on a stalling filesystem.
	go func() {
		if err := sweepTmpFiles(r.bgCtx, r.cfg.CASRoot, r.cfg.Obs.Logger.Info); err != nil &&
			err != context.Canceled {
			r.cfg.Obs.Logger.Warn("receiver: .tmp sweep failed", "error", err)
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
