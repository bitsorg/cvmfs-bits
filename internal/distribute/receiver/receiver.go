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
// See REFERENCE.md §20 for the full protocol specification.
package receiver

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"cvmfs.io/prepub/pkg/observe"
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

// Receiver runs the two-channel pre-warming server.
type Receiver struct {
	cfg         Config
	store       *sessionStore
	control     *http.Server
	data        *http.Server
	stopClean   chan struct{} // signal to stop the cleanup goroutine
	shutdownOnce sync.Once    // ensures Shutdown can be safely called multiple times
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

	r := &Receiver{
		cfg:       cfg,
		store:     newSessionStore(),
		stopClean: make(chan struct{}),
	}

	// Control channel mux (HTTPS): announce only.
	controlMux := http.NewServeMux()
	controlMux.HandleFunc("/api/v1/announce", r.announceHandler)

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
		Addr:        cfg.DataAddr,
		Handler:     dataMux,
		ReadTimeout: 0, // streaming: no read deadline on the body
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
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

	// Start the data channel first (plain HTTP — always succeeds if the port
	// is free) so it is ready before senders receive announce responses.
	dataLn, err := net.Listen("tcp", r.cfg.DataAddr)
	if err != nil {
		// On early failure, stop the cleanup goroutine to avoid a goroutine leak.
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
		// On early failure, stop the cleanup goroutine and shut down the data channel
		// to avoid a goroutine leak.
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

	return nil
}

// Shutdown gracefully stops both servers, waiting up to the deadline in ctx.
// Safe to call multiple times.
func (r *Receiver) Shutdown(ctx context.Context) error {
	var ctlErr, dataErr error
	r.shutdownOnce.Do(func() {
		// Stop the cleanup goroutine first.
		close(r.stopClean)
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
