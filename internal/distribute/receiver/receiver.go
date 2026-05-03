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
// topics for its configured repositories, computes the delta against its own
// Bloom filter, and publishes a ReadyMessage carrying its session token and the
// list of absent hashes — all without any inbound firewall rules (outbound
// TCP 8883 only).  The data channel (plain HTTP PUT) is unchanged.
//
// See REFERENCE.md §20 for the HTTP protocol and §21 for the MQTT control plane.
package receiver

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"cvmfs.io/prepub/internal/broker"
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

	// BloomCapacity is the expected number of distinct CAS objects for the
	// inventory Bloom filter.  0 uses the package default (5 million).
	BloomCapacity uint

	// BloomFPRate is the desired false-positive rate for the inventory Bloom
	// filter (0 < BloomFPRate < 1).  0 uses the package default (0.1%).
	BloomFPRate float64

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
// service can route bloom-filter queries and announce requests to the node.
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
	store        *sessionStore
	inv          *inventory
	coord        *CoordClient    // nil when CoordURL is empty
	mqttMu       sync.RWMutex    // guards mqttClient
	mqttClient   *broker.Client  // nil when BrokerURL is empty; always access under mqttMu
	control      *http.Server
	data         *http.Server
	stopClean    chan struct{}        // signal to stop the cleanup goroutine
	bgCtx        context.Context     // cancelled by Shutdown to stop background goroutines
	bgCancel     context.CancelFunc  // cancels bgCtx
	shutdownOnce sync.Once           // ensures Shutdown can be safely called multiple times

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

	inv := newInventory(cfg.BloomCapacity, cfg.BloomFPRate)
	bgCtx, bgCancel := context.WithCancel(context.Background())
	r := &Receiver{
		cfg:       cfg,
		store:     newSessionStore(),
		inv:       inv,
		coord:     newCoordClient(cfg.CoordURL, cfg.CoordToken, cfg.NodeID, cfg.Repos, cfg.controlEndpoint(), cfg.dataEndpoint(), inv, cfg.Obs),
		stopClean: make(chan struct{}),
		bgCtx:     bgCtx,
		bgCancel:  bgCancel,
		httpClient: &http.Client{
			Timeout: 5 * time.Minute, // generous for large CAS objects
		},
	}

	// Control channel mux (HTTPS): announce, bloom filter snapshot, metrics.
	controlMux := http.NewServeMux()
	controlMux.HandleFunc("/api/v1/announce", r.announceHandler)
	controlMux.HandleFunc("/api/v1/bloom", r.bloomHandler)
	controlMux.Handle("/metrics", promhttp.Handler())

	r.control = &http.Server{
		Addr:         cfg.ControlAddr,
		Handler:      controlMux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Data channel mux (plain HTTP): object PUTs only.
	// The body size is not bounded here because CAS objects may be large;
	// the handler streams directly to disk rather than buffering in memory.
	dataMux := http.NewServeMux()
	dataMux.HandleFunc("/api/v1/objects/", r.putObjectHandler)

	r.data = &http.Server{
		Addr:              cfg.DataAddr,
		Handler:           dataMux,
		ReadHeaderTimeout: 30 * time.Second,  // guards against Slowloris header drip
		ReadTimeout:       0,                 // streaming: no per-body read deadline
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	return r, nil
}

// Start launches both HTTP servers and a background session-cleanup goroutine.
// It returns as soon as both listeners are bound; actual request handling
// continues in background goroutines.  Call Shutdown to stop the servers.
func (r *Receiver) Start() error {
	if !r.cfg.DevMode && (r.cfg.TLSCert == "" || r.cfg.TLSKey == "") {
		return fmt.Errorf("receiver: TLSCert and TLSKey are required for the control channel (set DevMode for tests)")
	}
	// Start the background session reaper.
	go r.runCleanup()

	// Remove any .tmp files left by PUT handlers that were interrupted by a
	// previous crash.  This must run before the data channel listener opens so
	// that no new PUT can race with the sweep, and before the CAS walk so that
	// orphaned temp files are not counted as CAS objects.
	// Both goroutines receive bgCtx so Shutdown() can interrupt them promptly
	// on a stalling filesystem (e.g. NFS timeout).
	go func() {
		if err := sweepTmpFiles(r.bgCtx, r.cfg.CASRoot, r.cfg.Obs.Logger.Info); err != nil &&
			err != context.Canceled {
			r.cfg.Obs.Logger.Warn("receiver: .tmp sweep failed", "error", err)
		}
	}()

	// Populate the inventory Bloom filter from the on-disk CAS in the background.
	// The filter is empty (and isReady() returns false) until the walk completes,
	// so GET /api/v1/bloom returns 503 with Retry-After during the walk.
	//
	// Ordering note: the walk goroutine is started before the listeners are
	// bound so that the walk begins as early as possible.  This is intentional:
	// the control and data listeners are bound immediately after, and by the time
	// a sender can reach the announce endpoint the walk is already in progress.
	// If the walk finishes before any bloom query arrives the 503 window is zero.
	go func() {
		if err := r.inv.populateFromCAS(r.bgCtx, r.cfg.CASRoot, r.cfg.Obs.Logger.Info); err != nil &&
			err != context.Canceled {
			r.cfg.Obs.Logger.Error("inventory: CAS walk failed", "error", err)
		}
	}()

	// Start the data channel first (plain HTTP — always succeeds if the port
	// is free) so it is ready before senders receive announce responses.
	dataLn, err := net.Listen("tcp", r.cfg.DataAddr)
	if err != nil {
		// On early failure, cancel background goroutines and stop the cleanup
		// goroutine to avoid goroutine leaks.
		r.bgCancel()
		close(r.stopClean)
		return fmt.Errorf("receiver: binding data channel %s: %w", r.cfg.DataAddr, err)
	}
	go func() {
		if serveErr := r.data.Serve(dataLn); serveErr != nil && serveErr != http.ErrServerClosed {
			r.cfg.Obs.Logger.Error("data channel error", "error", serveErr)
		}
	}()
	r.cfg.Obs.Logger.Info("receiver data channel listening (plain HTTP)", "addr", r.cfg.DataAddr)

	// Start the control channel (HTTPS unless DevMode).
	controlLn, err := net.Listen("tcp", r.cfg.ControlAddr)
	if err != nil {
		// On early failure, cancel background goroutines, stop the cleanup
		// goroutine, and shut down the data channel to avoid goroutine leaks.
		r.bgCancel()
		close(r.stopClean)
		_ = r.data.Shutdown(context.Background())
		return fmt.Errorf("receiver: binding control channel %s: %w", r.cfg.ControlAddr, err)
	}

	if r.cfg.DevMode {
		go func() {
			if serveErr := r.control.Serve(controlLn); serveErr != nil && serveErr != http.ErrServerClosed {
				r.cfg.Obs.Logger.Error("control channel error", "error", serveErr)
			}
		}()
		r.cfg.Obs.Logger.Warn("receiver control channel listening (plain HTTP — DEV MODE ONLY)",
			"addr", r.cfg.ControlAddr)
	} else {
		go func() {
			if serveErr := r.control.ServeTLS(controlLn, r.cfg.TLSCert, r.cfg.TLSKey); serveErr != nil && serveErr != http.ErrServerClosed {
				r.cfg.Obs.Logger.Error("control channel TLS error", "error", serveErr)
			}
		}()
		r.cfg.Obs.Logger.Info("receiver control channel listening (HTTPS)", "addr", r.cfg.ControlAddr)
	}

	// Start the coordination service client (no-op when CoordURL is empty).
	r.coord.Start()

	// Start the MQTT control plane (no-op when BrokerURL is empty).
	if err := r.startMQTT(); err != nil {
		// MQTT is supplemental — log the error but do not abort startup so
		// the HTTP announce path continues to work.
		r.cfg.Obs.Logger.Error("receiver: MQTT startup failed — continuing without broker",
			"error", err)
	}

	return nil
}

// Shutdown gracefully stops both servers, waiting up to the deadline in ctx.
// Safe to call multiple times.
func (r *Receiver) Shutdown(ctx context.Context) error {
	var ctlErr, dataErr error
	r.shutdownOnce.Do(func() {
		// Cancel background goroutines (CAS walk, tmp sweep) first so they can
		// exit before the process terminates.  On a stalling filesystem this
		// prevents Shutdown from blocking waiting for goroutines that will never
		// complete on their own.
		r.bgCancel()
		// Stop the cleanup goroutine.
		close(r.stopClean)
		// Deregister from the coordination service before closing listeners.
		r.coord.Stop()
		// Disconnect from the MQTT broker before closing listeners so an
		// explicit offline presence is published before the TCP connection closes.
		r.stopMQTT()
		// Shut down the control channel first to stop new announces.
		ctlErr = r.control.Shutdown(ctx)
		// Then the data channel once in-flight PUTs can complete.
		dataErr = r.data.Shutdown(ctx)
	})
	if ctlErr != nil {
		return ctlErr
	}
	return dataErr
}

// Addrs returns the actual listen addresses of the control and data channels
// after Start has been called.  Useful in tests where ":0" is used to let the
// OS assign a free port.
func (r *Receiver) Addrs() (controlAddr, dataAddr string) {
	return r.cfg.ControlAddr, r.cfg.DataAddr
}

// runCleanup sweeps expired sessions every minute for the lifetime of the
// receiver.  It exits when Shutdown() is called and closes stopClean.
func (r *Receiver) runCleanup() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopClean:
			return
		case <-ticker.C:
			r.store.cleanup()
		}
	}
}
