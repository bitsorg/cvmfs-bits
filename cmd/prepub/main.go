// Command prepub is the cvmfs-prepub service binary.  It can run in two modes:
//
//   - publisher (default): accepts publish jobs via an HTTP API, coordinates
//     the pre-publish pipeline (dedup → compress → CAS → gateway commit), and
//     distributes pre-warmed objects to Stratum 1 receivers before the catalog
//     flip.
//
//   - receiver: runs the two-channel Stratum 1 pre-warming server.  An HTTPS
//     control channel handles announce requests (HMAC-authenticated); a plain-
//     HTTP data channel accepts object PUTs (per-session bearer token + SHA-256
//     hash verification).  See REFERENCE.md §20 for the full protocol spec.
//
// Select the mode with --mode publisher|receiver.  All flags except --mode,
// --log-level, and --dev are mode-specific; unrecognised flags for the active
// mode are silently ignored (standard flag package behaviour when flags are
// defined but unused).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"cvmfs.io/prepub/internal/api"
	"cvmfs.io/prepub/internal/broker"
	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/internal/distribute"
	"cvmfs.io/prepub/internal/distribute/receiver"
	"cvmfs.io/prepub/internal/lease"
	"cvmfs.io/prepub/internal/notify"
	"cvmfs.io/prepub/internal/pipeline"
	"cvmfs.io/prepub/internal/pipeline/dedup"
	"cvmfs.io/prepub/internal/probe"
	"cvmfs.io/prepub/internal/provenance"
	"cvmfs.io/prepub/internal/spool"
	"cvmfs.io/prepub/pkg/observe"
)

func main() {
	// ── Flags shared by both modes ────────────────────────────────────────────
	mode     := flag.String("mode", "publisher", "Operating mode: publisher or receiver")
	logLevel := flag.String("log-level", "info", "Log level: debug, info, warn, error")
	devMode  := flag.Bool("dev", false, "Development mode: relaxes security checks (NEVER use in production)")
	config   := flag.String("config", "", "Config file path (reserved for future use)")

	// ── Publisher-mode flags ───────────────────────────────────────────────────
	spoolRoot    := flag.String("spool-root", "/var/spool/cvmfs-prepub", "Spool root directory [publisher]")
	stagingRoot  := flag.String("staging-root", "", "Directory from which tar_path references (JSON submissions) are allowed; empty disables JSON/tar_path mode [publisher]")
	listen       := flag.String("listen", ":8080", "HTTP listen address for the API server [publisher]")
	publishMode  := flag.String("publish-mode", "gateway", "Publish backend: 'gateway' (cvmfs_gateway HTTP API) or 'local' (cvmfs_server direct, no gateway required) [publisher]")
	gatewayURL   := flag.String("gateway-url", "https://localhost:4929", "cvmfs_gateway URL (must be HTTPS in production; ignored in local publish mode) [publisher]")
	cvmfsMount   := flag.String("cvmfs-mount", "/cvmfs", "CVMFS repository mount point used in local publish mode [publisher]")
	casType      := flag.String("cas-type", "localfs", "CAS backend type: localfs or memory (used in gateway mode only) [publisher]")
	casRoot      := flag.String("cas-root", "/var/lib/cvmfs-prepub/cas", "CAS root directory [publisher|receiver]")

	// ── Stratum 1 distribution flags (publisher) ─────────────────────────────
	s1Endpoints       := flag.String("s1-endpoints", "", "Comma-separated Stratum 1 HTTPS URLs to pre-warm (e.g. https://s1a.cern.ch,https://s1b.cern.ch) [publisher]")
	s1Quorum          := flag.Float64("s1-quorum", 1.0, "Fraction of Stratum 1 endpoints that must confirm receipt for the publish to proceed (0.5 = majority, 1.0 = all) [publisher]")
	s1Timeout         := flag.Duration("s1-timeout", 60*time.Second, "Per-object/batch timeout for Stratum 1 pushes [publisher]")
	s1BloomTimeout    := flag.Duration("s1-bloom-timeout", 0, "Per-endpoint timeout for fetching inventory Bloom filter (0 = disable delta push) [publisher]")
	s1MQTTTimeout     := flag.Duration("s1-mqtt-quorum-timeout", 30*time.Second, "Time to wait for receiver ready replies before proceeding (MQTT mode) [publisher]")

	// Shared Bloom filter — off by default.
	bloomSnapshotDir    := flag.String("bloom-snapshot-dir", "", "Directory for shared Bloom filter snapshots (enables cross-node dedup; must be on a shared filesystem) [publisher]")
	bloomNodeID         := flag.String("bloom-node-id", "", "Unique node ID for this build node (defaults to hostname) [publisher]")
	bloomMaxSnapshotAge := flag.Duration("bloom-max-snapshot-age", 0, "Maximum age of peer Bloom snapshots to merge (default 24h) [publisher]")
	bloomFilterCapacity := flag.Uint("bloom-filter-capacity", 0, "Bloom filter capacity — must match across all nodes (default 1 000 000) [publisher]")
	bloomFilterFPRate   := flag.Float64("bloom-filter-fp-rate", 0, "Bloom filter false-positive rate — must match across all nodes (default 0.01) [publisher]")

	// Provenance & Rekor transparency log — off by default.
	provenanceEnabled := flag.Bool("provenance", false, "Enable provenance recording and Rekor transparency log submission [publisher]")
	rekorServer       := flag.String("rekor-server", provenance.DefaultRekorServer, "Rekor transparency log URL [publisher]")
	rekorSigningKey   := flag.String("rekor-signing-key", "", "Path to Ed25519 private key (PEM/PKCS#8) for signing Rekor entries; auto-generated if absent [publisher]")
	oidcIssuers       := flag.String("oidc-issuers", "", "Comma-separated list of allowed OIDC issuer URLs for CI token validation [publisher]")

	// ── Receiver-mode flags ────────────────────────────────────────────────────
	//
	// The CAS root for the receiver is shared with --cas-root above so that a
	// node running both modes (unusual but possible in a test setup) uses the
	// same directory by default.  Override with --cas-root as needed.
	controlAddr  := flag.String("control-addr", ":9100", "HTTPS listen address for announce requests [receiver]")
	dataAddr     := flag.String("data-addr", ":9101", "Plain-HTTP listen address for object PUTs [receiver]")
	dataHost     := flag.String("data-host", "", "Publicly reachable hostname or IP returned to senders as the data endpoint [receiver]")
	tlsCert      := flag.String("tls-cert", "", "Path to TLS certificate for the control channel [receiver]")
	tlsKey       := flag.String("tls-key", "", "Path to TLS private key for the control channel [receiver]")
	sessionTTL   := flag.Duration("session-ttl", time.Hour, "How long announce sessions remain valid [receiver]")
	diskHeadroom := flag.Float64("disk-headroom", 1.2, "Multiplier applied to announced payload size when checking available disk space [receiver]")

	// Receiver inventory Bloom filter — controls the in-memory filter that
	// tracks which CAS objects are present locally.  Values must be consistent
	// across all receivers that the distributor will compare filters between.
	recvBloomCapacity := flag.Uint("bloom-capacity", 0, "Inventory Bloom filter capacity (default 5 000 000) [receiver]")
	recvBloomFPRate   := flag.Float64("bloom-fp-rate", 0, "Inventory Bloom filter false-positive rate (default 0.001) [receiver]")

	// HepCDN coordination service — off by default.
	// CoordURL enables registration, heartbeat, and topology-aware routing.
	// The bearer token is read from PREPUB_COORD_TOKEN for security.
	coordURL := flag.String("coord-url", "", "HepCDN coordination service base URL (e.g. https://coord.hepcdn.example.com) [receiver]")
	nodeID   := flag.String("node-id", "", "Stable identifier for this receiver node; defaults to hostname [receiver]")
	repos    := flag.String("repos", "", "Comma-separated list of CVMFS repositories served by this receiver (e.g. atlas.cern.ch,cms.cern.ch) [receiver]")

	// MQTT broker — shared by publisher and receiver modes.
	// When set, receivers connect outbound to the broker and publish retained
	// presence messages; publishers use pub/sub announce instead of HTTP.
	// The broker URL uses Paho format: "tls://broker.cern.ch:8883" (production)
	// or "tcp://localhost:1883" (development).  mTLS cert/key are required in
	// production; --broker-ca-cert overrides the system CA pool.
	brokerURL        := flag.String("broker-url", "", "MQTT broker URL (e.g. tls://broker.cern.ch:8883); empty disables MQTT [publisher+receiver]")
	brokerClientCert := flag.String("broker-client-cert", "", "Path to PEM client certificate for MQTT mTLS [publisher+receiver]")
	brokerClientKey  := flag.String("broker-client-key", "", "Path to PEM client private key for MQTT mTLS [publisher+receiver]")
	brokerCACert     := flag.String("broker-ca-cert", "", "Path to PEM CA certificate to verify the MQTT broker; empty uses system pool [publisher+receiver]")

	flag.Parse()

	_ = config // reserved for future use

	// ── Observability (common setup) ──────────────────────────────────────────
	obs, obsShutdown, err := observe.New("cvmfs-prepub")
	if err != nil {
		slog.Error("failed to create observability provider", "error", err)
		os.Exit(1)
	}
	defer obsShutdown()

	obs.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: parseLogLevel(*logLevel),
	}))

	obs.Logger.Info("starting cvmfs-prepub", "mode", *mode)

	switch *mode {
	case "publisher":
		runPublisher(obs, *devMode, *spoolRoot, *stagingRoot, *listen, *publishMode, *gatewayURL, *cvmfsMount, *casType, *casRoot,
			*bloomSnapshotDir, *bloomNodeID, *bloomMaxSnapshotAge, *bloomFilterCapacity, *bloomFilterFPRate,
			*provenanceEnabled, *rekorServer, *rekorSigningKey, *oidcIssuers,
			*s1Endpoints, *s1Quorum, *s1Timeout, *s1BloomTimeout, *s1MQTTTimeout,
			*brokerURL, *brokerClientCert, *brokerClientKey, *brokerCACert)
	case "receiver":
		runReceiver(obs, *devMode, *controlAddr, *dataAddr, *dataHost, *tlsCert, *tlsKey, *casRoot, *sessionTTL, *diskHeadroom,
			*recvBloomCapacity, *recvBloomFPRate, *coordURL, *nodeID, *repos,
			*brokerURL, *brokerClientCert, *brokerClientKey, *brokerCACert)
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q — valid modes are: publisher, receiver\n", *mode)
		os.Exit(1)
	}
}

// runPublisher starts the publisher-mode HTTP API server.  It never returns
// normally; it blocks until a SIGINT or SIGTERM is received and then performs
// a graceful shutdown.
func runPublisher(
	obs *observe.Provider,
	devMode bool,
	spoolRoot, stagingRoot, listen, publishMode, gatewayURL, cvmfsMount, casType, casRoot string,
	bloomSnapshotDir, bloomNodeID string,
	bloomMaxSnapshotAge time.Duration,
	bloomFilterCapacity uint,
	bloomFilterFPRate float64,
	provenanceEnabled bool,
	rekorServer, rekorSigningKey, oidcIssuers string,
	s1Endpoints string,
	s1Quorum float64,
	s1Timeout, s1BloomTimeout, s1MQTTTimeout time.Duration,
	brokerURL, brokerClientCert, brokerClientKey, brokerCACert string,
) {
	apiToken := os.Getenv("PREPUB_API_TOKEN")
	if apiToken == "" {
		if devMode {
			obs.Logger.Warn("SECURITY: PREPUB_API_TOKEN not set — API is unauthenticated (development mode only)")
		} else {
			obs.Logger.Error("PREPUB_API_TOKEN environment variable must be set")
			os.Exit(1)
		}
	}

	// Create spool.
	sp, err := spool.New(spoolRoot, obs)
	if err != nil {
		obs.Logger.Error("failed to create spool", "error", err)
		os.Exit(1)
	}

	// ── Publish backend selection ─────────────────────────────────────────────
	//
	// Gateway mode:  full compress/dedup/CAS pipeline → cvmfs_gateway HTTP API.
	// Local mode:    raw tar extraction → cvmfs_server transaction/publish.
	//
	// Local mode does not require cvmfs_gateway, a CAS, or a gateway secret.

	var casBackend cas.Backend
	var leaseBackend lease.Backend

	switch publishMode {
	case "gateway":
		// Enforce HTTPS for gateway communication (loopback is accepted as-is
		// because cvmfs_gateway typically listens on http://localhost:4929).
		isLoopback := strings.HasPrefix(gatewayURL, "http://localhost") ||
			strings.HasPrefix(gatewayURL, "http://127.0.0.1") ||
			strings.HasPrefix(gatewayURL, "http://[::1]")
		if !strings.HasPrefix(gatewayURL, "https://") && !isLoopback {
			if devMode {
				obs.Logger.Warn("SECURITY: gateway URL is not HTTPS — development mode only, NEVER use in production", "url", gatewayURL)
			} else {
				obs.Logger.Error("gateway URL must use HTTPS (or http://localhost for local gateway); use --dev to override", "url", gatewayURL)
				os.Exit(1)
			}
		}

		gatewaySecret := os.Getenv("CVMFS_GATEWAY_SECRET")
		if gatewaySecret == "" {
			if devMode {
				obs.Logger.Warn("SECURITY: CVMFS_GATEWAY_SECRET not set — using insecure placeholder (development mode only)")
				gatewaySecret = "dev-insecure-placeholder"
			} else {
				obs.Logger.Error("CVMFS_GATEWAY_SECRET environment variable must be set in gateway mode")
				os.Exit(1)
			}
		}

		switch casType {
		case "localfs":
			lfs, err := cas.NewLocalFS(casRoot)
			if err != nil {
				obs.Logger.Error("failed to create localfs CAS", "error", err)
				os.Exit(1)
			}
			casBackend = lfs
		default:
			obs.Logger.Error("unknown CAS type", "type", casType)
			os.Exit(1)
		}

		leaseBackend = lease.NewClient(gatewayURL, "cvmfs-prepub", gatewaySecret, obs)
		obs.Logger.Info("publish backend: gateway", "url", gatewayURL)

	case "local":
		// No gateway, no CAS — cvmfs_server handles everything.
		leaseBackend = lease.NewLocalBackend(cvmfsMount, obs)
		obs.Logger.Info("publish backend: local (cvmfs_server direct)", "cvmfs_mount", cvmfsMount)

	default:
		obs.Logger.Error("unknown publish mode — valid values are: gateway, local", "mode", publishMode)
		os.Exit(1)
	}

	// Startup probe: confirm backends are reachable before accepting jobs.
	obs.Logger.Info("running startup probe")
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := probe.Run(probeCtx, casBackend, leaseBackend, obs); err != nil {
		probeCancel()
		obs.Logger.Error("startup probe failed", "error", err)
		os.Exit(1)
	}
	probeCancel()
	obs.Logger.Info("startup probe passed")

	notifyBus := notify.NewBus()

	// Shared Bloom filter — enabled only when --bloom-snapshot-dir is set.
	sharedFilter := dedup.SharedFilterConfig{
		Enabled:        bloomSnapshotDir != "",
		Dir:            bloomSnapshotDir,
		NodeID:         bloomNodeID,
		MaxSnapshotAge: bloomMaxSnapshotAge,
		Capacity:       bloomFilterCapacity,
		FPRate:         bloomFilterFPRate,
	}
	if sharedFilter.Enabled {
		nodeIDForLog := bloomNodeID
		if nodeIDForLog == "" {
			if h, err := os.Hostname(); err == nil {
				nodeIDForLog = h
			} else {
				nodeIDForLog = "(hostname unavailable)"
			}
		}
		obs.Logger.Info("shared Bloom filter enabled", "dir", sharedFilter.Dir, "node_id", nodeIDForLog)
	}

	// Log staging root status so operators can confirm the mode at startup.
	if stagingRoot != "" {
		obs.Logger.Info("tar_path submission mode enabled", "staging_root", stagingRoot)
	} else {
		obs.Logger.Info("tar_path submission mode disabled (no --staging-root configured)")
	}

	// Provenance provider — no-op when --provenance is not set.
	var oidcIssuerList []string
	for _, s := range strings.Split(oidcIssuers, ",") {
		if trimmed := strings.TrimSpace(s); trimmed != "" {
			oidcIssuerList = append(oidcIssuerList, trimmed)
		}
	}
	provCfg := provenance.Config{
		Enabled:       provenanceEnabled,
		RekorServer:   rekorServer,
		SigningKeyPath: rekorSigningKey,
		OIDCIssuers:   oidcIssuerList,
	}
	provProvider, err := provenance.New(provCfg, spoolRoot, obs)
	if err != nil {
		obs.Logger.Error("failed to initialise provenance provider", "error", err)
		os.Exit(1)
	}

	// Build the orchestrator.
	// Build the Stratum 1 distribution config.
	// When neither --s1-endpoints nor --broker-url is set, Distribute is nil
	// and distribution is skipped (backward-compatible default).
	var distCfg *distribute.Config
	{
		var endpoints []string
		for _, ep := range strings.Split(s1Endpoints, ",") {
			if trimmed := strings.TrimSpace(ep); trimmed != "" {
				endpoints = append(endpoints, trimmed)
			}
		}
		var brokerCfg *broker.Config
		if brokerURL != "" {
			brokerCfg = &broker.Config{
				BrokerURL:  brokerURL,
				ClientCert: brokerClientCert,
				ClientKey:  brokerClientKey,
				CACert:     brokerCACert,
			}
		}
		if len(endpoints) > 0 || brokerCfg != nil {
			hmacSecret := os.Getenv("PREPUB_HMAC_SECRET")
			if hmacSecret == "" && len(endpoints) > 0 && !devMode {
				obs.Logger.Error("PREPUB_HMAC_SECRET must be set when --s1-endpoints is configured")
				os.Exit(1)
			}
			distCfg = &distribute.Config{
				Endpoints:         endpoints,
				Quorum:            s1Quorum,
				Timeout:           s1Timeout,
				Obs:               obs,
				DevMode:           devMode,
				HMACSecret:        hmacSecret,
				BloomQueryTimeout: s1BloomTimeout,
				BrokerConfig:      brokerCfg,
				MQTTQuorumTimeout: s1MQTTTimeout,
			}
			obs.Logger.Info("Stratum 1 distribution configured",
				"endpoints", len(endpoints),
				"mqtt", brokerCfg != nil,
				"quorum", s1Quorum)
		}
	}

	orch := &api.Orchestrator{
		Spool:      sp,
		CAS:        casBackend,
		Lease:      leaseBackend,
		CVMFSMount: cvmfsMount,
		Pipeline: pipeline.Config{
			Workers:      4,
			UploadConc:   4,
			CAS:          casBackend,
			SpoolDir:     spoolRoot,
			Obs:          obs,
			SharedFilter: sharedFilter,
		},
		Distribute: distCfg,
		Notify:     notifyBus,
		Provenance: provProvider,
		Obs:        obs,
	}

	apiServer := api.New(obs, apiToken, orch, sp, notifyBus, spoolRoot, stagingRoot)

	// Crash-recovery: re-run jobs that were interrupted by a previous crash.
	recoverCtx, cancelRecover := context.WithCancel(context.Background())

	obs.Logger.Info("scanning for in-progress jobs")
	inProgressJobs, err := sp.Scan(recoverCtx)
	if err != nil {
		cancelRecover()
		obs.Logger.Error("failed to scan jobs", "error", err)
		os.Exit(1)
	}

	var recoveryWg sync.WaitGroup
	for _, j := range inProgressJobs {
		j := j
		obs.Logger.Info("found in-progress job — scheduling recovery", "job_id", j.ID, "state", j.State)
		recoveryWg.Add(1)
		go func() {
			defer recoveryWg.Done()
			if err := orch.Recover(recoverCtx, j); err != nil {
				obs.Logger.Error("job recovery failed", "job_id", j.ID, "error", err)
			}
		}()
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		obs.Logger.Info("starting API server", "address", listen)
		if err := apiServer.ListenAndServe(listen); err != nil && err != http.ErrServerClosed {
			obs.Logger.Error("API server error", "error", err)
		}
	}()

	<-sigChan
	obs.Logger.Info("shutting down — draining in-flight work")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := apiServer.Shutdown(shutdownCtx); err != nil {
		obs.Logger.Error("HTTP server shutdown error", "error", err)
	}

	cancelRecover()
	doneCh := make(chan struct{})
	go func() {
		recoveryWg.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
		obs.Logger.Info("all recovery goroutines finished")
	case <-shutdownCtx.Done():
		obs.Logger.Warn("timed out waiting for recovery goroutines")
	}

	obs.Logger.Info("shutdown complete")
}

// runReceiver starts the two-channel Stratum 1 pre-warming server.  It never
// returns normally; it blocks until a SIGINT or SIGTERM is received and then
// performs a graceful shutdown.
//
// The HMAC shared secret is read from the PREPUB_HMAC_SECRET environment
// variable.  It must be identical on the publisher and all receivers.  When
// --dev is set the HMAC check is skipped and the control channel uses plain
// HTTP instead of TLS (never use in production).
func runReceiver(
	obs *observe.Provider,
	devMode bool,
	controlAddr, dataAddr, dataHost string,
	tlsCert, tlsKey string,
	casRoot string,
	sessionTTL time.Duration,
	diskHeadroom float64,
	bloomCapacity uint,
	bloomFPRate float64,
	coordURL, nodeID, reposFlag string,
	brokerURL, brokerClientCert, brokerClientKey, brokerCACert string,
) {
	// Load the HMAC shared secret from the environment.  In DevMode the
	// receiver skips HMAC verification entirely, so the secret is not required.
	hmacSecret := os.Getenv("PREPUB_HMAC_SECRET")
	if hmacSecret == "" && !devMode {
		obs.Logger.Error("PREPUB_HMAC_SECRET environment variable must be set (or use --dev for testing)")
		os.Exit(1)
	}
	if hmacSecret == "" && devMode {
		obs.Logger.Warn("SECURITY: PREPUB_HMAC_SECRET not set — HMAC verification disabled (development mode only)")
	}

	// Validate TLS configuration early so the error is reported before any
	// listeners are bound.  In DevMode TLS is not used.
	if !devMode {
		if tlsCert == "" || tlsKey == "" {
			obs.Logger.Error("--tls-cert and --tls-key are required for the control channel (or use --dev for testing)")
			os.Exit(1)
		}
		if _, err := os.Stat(tlsCert); err != nil {
			obs.Logger.Error("TLS certificate file not found", "path", tlsCert, "error", err)
			os.Exit(1)
		}
		if _, err := os.Stat(tlsKey); err != nil {
			obs.Logger.Error("TLS key file not found", "path", tlsKey, "error", err)
			os.Exit(1)
		}
	}

	// Coordination service token — loaded from env to keep credentials out of
	// command lines and logs.  Empty means coordination is disabled even if
	// --coord-url is set (the receiver will log a warning in that case).
	coordToken := os.Getenv("PREPUB_COORD_TOKEN")
	if coordURL != "" && coordToken == "" {
		if devMode {
			obs.Logger.Warn("SECURITY: PREPUB_COORD_TOKEN not set — coordination service requests will be unauthenticated (development mode only)")
		} else {
			obs.Logger.Error("PREPUB_COORD_TOKEN environment variable must be set when --coord-url is configured")
			os.Exit(1)
		}
	}

	// Parse --repos flag into a slice of repository names.
	var repoList []string
	for _, r := range strings.Split(reposFlag, ",") {
		if trimmed := strings.TrimSpace(r); trimmed != "" {
			repoList = append(repoList, trimmed)
		}
	}

	cfg := receiver.Config{
		ControlAddr:      controlAddr,
		DataAddr:         dataAddr,
		DataHost:         dataHost,
		TLSCert:          tlsCert,
		TLSKey:           tlsKey,
		HMACSecret:       hmacSecret,
		CASRoot:          casRoot,
		SessionTTL:       sessionTTL,
		DiskHeadroom:     diskHeadroom,
		DevMode:          devMode,
		BloomCapacity:    bloomCapacity,
		BloomFPRate:      bloomFPRate,
		CoordURL:         coordURL,
		CoordToken:       coordToken,
		NodeID:           nodeID,
		Repos:            repoList,
		BrokerURL:        brokerURL,
		BrokerClientCert: brokerClientCert,
		BrokerClientKey:  brokerClientKey,
		BrokerCACert:     brokerCACert,
		Obs:              obs,
	}

	recv, err := receiver.New(cfg)
	if err != nil {
		obs.Logger.Error("failed to create receiver", "error", err)
		os.Exit(1)
	}

	if err := recv.Start(); err != nil {
		obs.Logger.Error("failed to start receiver", "error", err)
		os.Exit(1)
	}

	obs.Logger.Info("receiver ready",
		"control_addr", controlAddr,
		"data_addr", dataAddr,
		"cas_root", casRoot,
		"dev_mode", devMode,
	)

	// Block until a signal is received, then shut down gracefully.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	obs.Logger.Info("received signal — shutting down receiver", "signal", sig)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := recv.Shutdown(shutdownCtx); err != nil {
		obs.Logger.Error("receiver shutdown error", "error", err)
		os.Exit(1)
	}

	obs.Logger.Info("receiver shutdown complete")
}

// parseLogLevel maps a level name string to a slog.Level.  Unknown names
// default to Info.
func parseLogLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
