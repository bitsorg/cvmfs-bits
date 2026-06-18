// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

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
	"crypto/tls"
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
	"cvmfs.io/prepub/internal/distribute/commit"
	"cvmfs.io/prepub/internal/distribute/credential"
	"cvmfs.io/prepub/internal/distribute/receiver"
	"cvmfs.io/prepub/internal/distribute/serve"
	"cvmfs.io/prepub/internal/lease"
	"cvmfs.io/prepub/internal/notify"
	"cvmfs.io/prepub/internal/pipeline"
	"cvmfs.io/prepub/internal/pipeline/dedup"
	"cvmfs.io/prepub/internal/provenance"
	"cvmfs.io/prepub/internal/spool"
	"cvmfs.io/prepub/pkg/observe"
)

func main() {
	// ── Flags shared by both modes ────────────────────────────────────────────
	// Subcommand: prepub revoke <node> -- revoke a receiver's control-plane
	// access (denylist + active disconnect) via the publisher's TLS endpoint.
	if len(os.Args) > 1 && os.Args[1] == "revoke" {
		runRevoke(os.Args[2:])
		return
	}
	mode := flag.String("mode", "publisher", "Operating mode: publisher or receiver")
	// ADR-0001 (reserved; not yet active in P0). Data-plane direction and
	// control-plane transport selectors; parsed now so config/tooling can set
	// them, wired into behaviour in later phases.
	distributeMode := flag.String("distribute-mode", "push", "Data-plane distribution: push (legacy) or pull (ADR-0001) [publisher|receiver]")
	controlPlane := flag.String("control-plane", "mqtt", "Control-plane transport: mqtt or sse (sse reserved; not yet active) [publisher|receiver]")
	embeddedBrokerWSAddr := flag.String("embedded-broker-ws-addr", "", "If set, run an in-process MQTT broker with a WebSocket listener at this address (e.g. :1882); the control plane then runs on S0 with no separate broker [publisher]")
	controlPlaneURL := flag.String("control-plane-url", "", "Control-plane (broker) URL advertised to receivers via discovery, e.g. ws://cvmfs-prepub:1882 or wss://... [publisher]")
	pullObjectBaseURL := flag.String("pull-object-base-url", "", "Externally reachable base URL for content-addressed object GETs, embedded in pull manifests as {url}/cvmfs/{repo}/data (e.g. http://cvmfs-prepub:8080) [publisher]")
	embeddedBrokerTLSCert := flag.String("embedded-broker-tls-cert", "", "PEM server certificate for the embedded broker WebSocket listener; enables wss:// [publisher]")
	embeddedBrokerTLSKey := flag.String("embedded-broker-tls-key", "", "PEM private key for --embedded-broker-tls-cert [publisher]")
	embeddedBrokerAuth := flag.Bool("embedded-broker-auth", false, "Require token authentication on the embedded broker (needs PREPUB_HMAC_SECRET); receivers enrol via challenge/response [publisher]")
	enrollTLSAddr := flag.String("enroll-tls-addr", "", "With --embedded-broker-auth and a broker TLS cert, serve enroll/revoke over HTTPS at this bind address (e.g. :8443) so the enrollment token never travels in plaintext [publisher]")
	enrollURL := flag.String("enroll-url", "", "HTTPS base URL for the TLS enroll/revoke endpoint, advertised to receivers via discovery (e.g. https://cvmfs-prepub:8443) [publisher]")
	logLevel := flag.String("log-level", "info", "Log level: debug, info, warn, error")
	devMode := flag.Bool("dev", false, "Development mode: relaxes security checks (NEVER use in production)")
	config := flag.String("config", "", "Config file path (reserved for future use)")

	// ── Publisher-mode flags ───────────────────────────────────────────────────
	spoolRoot := flag.String("spool-root", "/var/spool/cvmfs-prepub", "Spool root directory [publisher]")
	stagingRoot := flag.String("staging-root", "", "Directory from which tar_path references (JSON submissions) are allowed; empty disables JSON/tar_path mode [publisher]")
	listen := flag.String("listen", ":8080", "HTTP listen address for the API server [publisher]")
	publishMode := flag.String("publish-mode", "gateway", "Publish backend: 'gateway' (cvmfs_gateway HTTP API) or 'local' (cvmfs_server direct, no gateway required) [publisher]")
	gatewayURL := flag.String("gateway-url", "https://localhost:4929", "cvmfs_gateway URL (must be HTTPS in production; ignored in local publish mode) [publisher]")
	gatewayDirectGraft := flag.Bool("gateway-direct-graft", true, "Use the direct-graft fast path on commit: skips DiffRec on the receiver and grafts the pre-built subtree catalog directly. Only correct when the lease path has no pre-existing content. Set to false to fall back to the standard DiffRec path (safe for all cases, but slower). [publisher]")
	cvmfsMount := flag.String("cvmfs-mount", "/cvmfs", "CVMFS repository mount point used in local publish mode [publisher]")
	stratum0URL := flag.String("stratum0-url", "", "Stratum 0 HTTP base URL for catalog merge, e.g. http://stratum0/cvmfs (gateway mode only) [publisher]")
	casType := flag.String("cas-type", "localfs", "CAS backend type: localfs or memory (used in gateway mode only) [publisher]")
	casRoot := flag.String("cas-root", "/var/lib/cvmfs-prepub/cas", "CAS root directory [publisher|receiver]")

	// Per-job wall-clock timeout (publisher) — prevents any phase from hanging
	// indefinitely.  0 (default) disables the timeout for backward compatibility.
	// When --max-concurrent-jobs is also set, the timeout starts AFTER the job
	// acquires a concurrency slot, so queue-wait time does not count against it.
	jobTimeout := flag.Duration("job-timeout", 0, "Maximum wall-clock time a single publish job may run before it is cancelled and failed; 0 disables the timeout [publisher]")

	// Server-side job concurrency limiter.  Limits how many jobs can run the
	// pipeline + critical section simultaneously, preventing CPU
	// over-subscription when many jobs are submitted at once.  Jobs that
	// cannot start immediately queue in StateIncoming.
	//
	// Recommended value: match the number of parallel uploads from the client
	// (e.g. 4 when using --concurrency 4 in upload-filelist.sh).  With N
	// concurrent jobs, at most N compress worker pools run at once, so zlib
	// workers are used efficiently rather than competing for CPU.
	//
	// 0 (default) disables the limit — all submitted jobs start immediately,
	// which is the legacy behaviour.
	// Dynamic concurrency:
	//   --min-concurrent-jobs  — floor for the slot count (default 4).
	//                            Set to 0 to disable the limit entirely (legacy behaviour).
	//   --max-concurrent-jobs  — ceiling for the slot count (default 0 = runtime.NumCPU()).
	//
	// Effective slots = max(min, numCPU - load1min), clamped to [min, max].
	// As load drops, waiting jobs are released without any delay.
	minConcurrentJobs := flag.Int("min-concurrent-jobs", 4, "Minimum (guaranteed) number of concurrent jobs regardless of load; 0 = disable dynamic limiting [publisher]")
	maxConcurrentJobs := flag.Int("max-concurrent-jobs", 0, "Maximum concurrent jobs ceiling (0 = runtime.NumCPU()); effective slots adapt between min and max based on 1-min load average [publisher]")

	// Lease path_busy retry window — how long Acquire() will keep retrying when
	// the gateway reports another publisher holds the lease.  Should be set to
	// a value slightly greater than the gateway's max_lease_time so that a
	// single Acquire call can outlast a stale lease left by a crashed job.
	// Default is 12 min (outlasts the common 600 s testbed TTL).
	leaseRetryMax := flag.Duration("lease-retry-max", 0, "Maximum time to retry lease acquisition when path_busy; 0 = 12 min default (should exceed gateway max_lease_time) [publisher]")

	// Pipeline performance tuning.
	pipelineUploadConc := flag.Int("pipeline-upload-conc", 4, "Concurrent dedup+upload workers per job (higher = better throughput for new-object-heavy publishes) [publisher]")
	pipelineCompressLevel := flag.Int("pipeline-compress-level", 0, "zlib compression level: 0=default(6), 1=fastest, 9=best; lower levels reduce CPU at cost of slightly larger objects [publisher]")

	// Optional: repository name for catalog-based dedup seeding at startup.
	// When set alongside --stratum0-url, the Bloom filter is seeded by walking
	// the CVMFS catalog tree (much faster than scanning the CAS filesystem for
	// large repositories).  If not set, falls back to the CAS walk.
	// In single-repo deployments set this to the same value as the repo field
	// in job submissions (e.g. "atlas.cern.ch").
	repoName := flag.String("repo-name", "", "CVMFS repository name for catalog-based dedup seeding at startup (e.g. atlas.cern.ch); leave empty to use CAS walk [publisher]")

	// ── Stratum 1 distribution flags (publisher) ─────────────────────────────
	s1Endpoints := flag.String("s1-endpoints", "", "Comma-separated Stratum 1 HTTPS URLs to pre-warm (e.g. https://s1a.cern.ch,https://s1b.cern.ch) [publisher]")
	s1Quorum := flag.Float64("s1-quorum", 1.0, "Fraction of Stratum 1 endpoints that must confirm receipt for the publish to proceed (0.5 = majority, 1.0 = all) [publisher]")
	s1Timeout := flag.Duration("s1-timeout", 60*time.Second, "Per-object/batch timeout for Stratum 1 pushes [publisher]")
	s1BloomTimeout := flag.Duration("s1-bloom-timeout", 0, "Per-endpoint timeout for fetching inventory Bloom filter (0 = disable delta push) [publisher]")
	s1MQTTTimeout := flag.Duration("s1-mqtt-quorum-timeout", 30*time.Second, "Time to wait for receiver ready replies before proceeding (MQTT mode) [publisher]")
	// Queue-driven distribution worker flags.
	s1WorkerConcurrency := flag.Int("s1-worker-concurrency", 0, "Concurrent transfer goroutines per Stratum 1 endpoint (0 = default 2) [publisher]")
	s1AttemptTimeout := flag.Duration("s1-attempt-timeout", 0, "Per-attempt timeout for a single endpoint distribution run (0 = default 90s) [publisher]")
	s1InitialBackoff := flag.Duration("s1-initial-backoff", 0, "Initial backoff after a failed distribution attempt (0 = default 5s) [publisher]")
	s1MaxBackoff := flag.Duration("s1-max-backoff", 0, "Maximum exponential backoff between retry attempts (0 = default 5m) [publisher]")
	s1MaxAttempts := flag.Int("s1-max-attempts", 0, "Max delivery attempts per item per endpoint; 0 = unlimited [publisher]")
	s1QueueDepth := flag.Int("s1-queue-depth", 0, "In-memory queue depth per Stratum 1 endpoint (0 = default 512) [publisher]")
	s1QueueSpoolDir := flag.String("s1-queue-spool-dir", "", "Directory for persistent per-endpoint distribution queues (default: {spool-root}/dist-queue) [publisher]")
	s1BatchSize := flag.Int("s1-batch-size", 0, "Objects per multipart PUT to each Stratum 1 endpoint (0 = per-object PUTs) [publisher]")

	// Bloom filter dedup — off by default.
	//
	// By default the pipeline calls CAS.Exists (stat/HEAD) once per object to
	// detect duplicates before uploading.  For local-disk or S3 CAS this is
	// fast and requires no startup walk.
	//
	// Enable --bloom-filter when CAS.Exists is expensive (high-latency network
	// CAS) to replace per-object calls with a pre-seeded in-memory index.
	// --bloom-snapshot-dir additionally enables cross-node snapshot sharing so
	// all build nodes merge each other's filter state at startup.
	bloomFilter := flag.Bool("bloom-filter", false, "Enable in-process Bloom filter for dedup; use when CAS.Exists (stat/HEAD) is expensive (e.g. high-latency network CAS) [publisher]")
	bloomSnapshotDir := flag.String("bloom-snapshot-dir", "", "Directory for shared Bloom filter snapshots (enables cross-node dedup; must be on a shared filesystem; implies --bloom-filter) [publisher]")
	bloomNodeID := flag.String("bloom-node-id", "", "Unique node ID for this build node (defaults to hostname) [publisher]")
	bloomMaxSnapshotAge := flag.Duration("bloom-max-snapshot-age", 0, "Maximum age of peer Bloom snapshots to merge (default 24h) [publisher]")
	bloomFilterCapacity := flag.Uint("bloom-filter-capacity", 0, "Bloom filter capacity — must match across all nodes (default 10 000 000); auto-sized to catalog count when not using shared snapshots [publisher]")
	bloomFilterFPRate := flag.Float64("bloom-filter-fp-rate", 0, "Bloom filter false-positive rate — must match across all nodes (default 0.01) [publisher]")

	// Provenance & Rekor transparency log — off by default.
	provenanceEnabled := flag.Bool("provenance", false, "Enable provenance recording and Rekor transparency log submission [publisher]")
	rekorServer := flag.String("rekor-server", provenance.DefaultRekorServer, "Rekor transparency log URL [publisher]")
	rekorSigningKey := flag.String("rekor-signing-key", "", "Path to Ed25519 private key (PEM/PKCS#8) for signing Rekor entries; auto-generated if absent [publisher]")
	oidcIssuers := flag.String("oidc-issuers", "", "Comma-separated list of allowed OIDC issuer URLs for CI token validation [publisher]")

	// ── Receiver-mode flags ────────────────────────────────────────────────────
	//
	// The CAS root for the receiver is shared with --cas-root above so that a
	// node running both modes (unusual but possible in a test setup) uses the
	// same directory by default.  Override with --cas-root as needed.
	controlAddr := flag.String("control-addr", ":9100", "HTTPS listen address for announce requests [receiver]")
	dataAddr := flag.String("data-addr", ":9101", "Plain-HTTP listen address for object PUTs [receiver]")
	dataHost := flag.String("data-host", "", "Publicly reachable hostname or IP returned to senders as the data endpoint [receiver]")
	tlsCert := flag.String("tls-cert", "", "Path to TLS certificate for the control channel [receiver]")
	tlsKey := flag.String("tls-key", "", "Path to TLS private key for the control channel [receiver]")
	sessionTTL := flag.Duration("session-ttl", time.Hour, "How long announce sessions remain valid [receiver]")
	diskHeadroom := flag.Float64("disk-headroom", 1.2, "Multiplier applied to announced payload size when checking available disk space [receiver]")

	// Receiver inventory Bloom filter — controls the in-memory filter that
	// tracks which CAS objects are present locally.  Values must be consistent
	// across all receivers that the distributor will compare filters between.
	recvBloomCapacity := flag.Uint("bloom-capacity", 0, "Inventory Bloom filter capacity (default 5 000 000) [receiver]")
	recvBloomFPRate := flag.Float64("bloom-fp-rate", 0, "Inventory Bloom filter false-positive rate (default 0.001) [receiver]")

	// HepCDN coordination service — off by default.
	// CoordURL enables registration, heartbeat, and topology-aware routing.
	// The bearer token is read from PREPUB_COORD_TOKEN for security.
	coordURL := flag.String("coord-url", "", "HepCDN coordination service base URL (e.g. https://coord.hepcdn.example.com) [receiver]")
	nodeID := flag.String("node-id", "", "Stable identifier for this receiver node; defaults to hostname [receiver]")
	repos := flag.String("repos", "", "Comma-separated list of CVMFS repositories served by this receiver (e.g. atlas.cern.ch,cms.cern.ch) [receiver]")
	// recvStratum0URL is the Stratum 0 base URL the receiver uses to pull CAS
	// objects on published-notification.  Distinct from --stratum0-url (which
	// is publisher-mode only) to avoid flag-name collisions in the shared flag
	// set.  Using --receiver-stratum0-url makes the purpose explicit.
	recvStratum0URL := flag.String("receiver-stratum0-url", "", "Stratum 0 HTTP base URL used by the receiver to pull objects on commit notification (e.g. http://stratum0/cvmfs) [receiver]")
	discoveryURL := flag.String("discovery-url", "", "Fixed S0 endpoint serving the discovery doc GET {url}/cvmfs/{repo}/.cvmfsbits; the receiver learns its control-plane broker URL from it [receiver]")
	brokerAuth := flag.Bool("broker-auth", false, "Enrol (challenge/response) and present a bearer token to the control-plane broker; needs PREPUB_HMAC_SECRET and --discovery-url [receiver]")

	// MQTT broker — shared by publisher and receiver modes.
	// When set, receivers connect outbound to the broker and publish retained
	// presence messages; publishers use pub/sub announce instead of HTTP.
	// The broker URL uses Paho format: "tls://broker.cern.ch:8883" (production)
	// or "tcp://localhost:1883" (development).  mTLS cert/key are required in
	// production; --broker-ca-cert overrides the system CA pool.
	brokerURL := flag.String("broker-url", "", "MQTT broker URL (e.g. tls://broker.cern.ch:8883); empty disables MQTT [publisher+receiver]")
	brokerClientCert := flag.String("broker-client-cert", "", "Path to PEM client certificate for MQTT mTLS [publisher+receiver]")
	brokerClientKey := flag.String("broker-client-key", "", "Path to PEM client private key for MQTT mTLS [publisher+receiver]")
	brokerCACert := flag.String("broker-ca-cert", "", "Path to PEM CA certificate to verify the MQTT broker; empty uses system pool [publisher+receiver]")

	flag.Parse()

	// ── Config file (applied after flag.Parse so CLI flags take precedence) ───
	//
	// Collect the set of flags that were explicitly provided on the command
	// line.  Config-file values are only applied where a flag was NOT set
	// explicitly, preserving the CLI-overrides-file contract.
	if *config != "" {
		explicit := make(map[string]bool)
		flag.Visit(func(f *flag.Flag) { explicit[f.Name] = true })

		fc, err := loadFileConfig(*config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cvmfs-prepub: %v\n", err)
			os.Exit(1)
		}
		applyFileConfig(fc, explicit,
			mode, logLevel, devMode,
			spoolRoot, stagingRoot, listen, publishMode, gatewayURL, cvmfsMount, casType, casRoot,
			stratum0URL, repoName,
			jobTimeout, minConcurrentJobs, maxConcurrentJobs,
			s1Endpoints, s1Quorum, s1Timeout, s1BloomTimeout, s1MQTTTimeout,
			s1WorkerConcurrency, s1MaxAttempts, s1QueueDepth,
			s1AttemptTimeout, s1InitialBackoff, s1MaxBackoff,
			s1QueueSpoolDir, s1BatchSize,
			brokerURL, brokerClientCert, brokerClientKey, brokerCACert,
			controlAddr, dataAddr, dataHost, tlsCert, tlsKey,
			sessionTTL, diskHeadroom,
			nodeID, repos, coordURL, recvStratum0URL,
			bloomFilter, bloomSnapshotDir, bloomNodeID,
			bloomMaxSnapshotAge, bloomFilterCapacity, bloomFilterFPRate,
			recvBloomCapacity, recvBloomFPRate,
			provenanceEnabled, rekorServer, rekorSigningKey, oidcIssuers,
			gatewayDirectGraft,
		)
	}

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
	obs.Logger.Debug("distribution config (ADR-0001)",
		"distribute_mode", *distributeMode, "control_plane", *controlPlane)

	switch *mode {
	case "publisher":
		runPublisher(obs, *devMode, *spoolRoot, *stagingRoot, *listen, *publishMode, *gatewayURL, *gatewayDirectGraft, *cvmfsMount, *stratum0URL, *repoName, *casType, *casRoot,
			*bloomFilter, *bloomSnapshotDir, *bloomNodeID, *bloomMaxSnapshotAge, *bloomFilterCapacity, *bloomFilterFPRate,
			*provenanceEnabled, *rekorServer, *rekorSigningKey, *oidcIssuers,
			*jobTimeout, *leaseRetryMax, *minConcurrentJobs, *maxConcurrentJobs,
			*pipelineUploadConc, *pipelineCompressLevel,
			*s1Endpoints, *s1Quorum, *s1Timeout, *s1BloomTimeout, *s1MQTTTimeout,
			*s1WorkerConcurrency, *s1MaxAttempts, *s1QueueDepth,
			*s1AttemptTimeout, *s1InitialBackoff, *s1MaxBackoff, *s1QueueSpoolDir,
			*s1BatchSize,
			*brokerURL, *brokerClientCert, *brokerClientKey, *brokerCACert,
			*distributeMode, *embeddedBrokerWSAddr, *controlPlaneURL, *pullObjectBaseURL, *embeddedBrokerTLSCert, *embeddedBrokerTLSKey, *embeddedBrokerAuth, *enrollTLSAddr, *enrollURL)
	case "receiver":
		runReceiver(obs, *devMode, *controlAddr, *dataAddr, *dataHost, *tlsCert, *tlsKey, *casRoot, *sessionTTL, *diskHeadroom,
			*recvBloomCapacity, *recvBloomFPRate, *coordURL, *nodeID, *repos,
			*brokerURL, *brokerClientCert, *brokerClientKey, *brokerCACert,
			*recvStratum0URL, *distributeMode, *discoveryURL, *brokerAuth)
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
	spoolRoot, stagingRoot, listen, publishMode, gatewayURL string,
	gatewayDirectGraft bool,
	cvmfsMount, stratum0URL, repoName, casType, casRoot string,
	bloomFilterEnabled bool,
	bloomSnapshotDir, bloomNodeID string,
	bloomMaxSnapshotAge time.Duration,
	bloomFilterCapacity uint,
	bloomFilterFPRate float64,
	provenanceEnabled bool,
	rekorServer, rekorSigningKey, oidcIssuers string,
	jobTimeout, leaseRetryMax time.Duration,
	minConcurrentJobs, maxConcurrentJobs int,
	pipelineUploadConc, pipelineCompressLevel int,
	s1Endpoints string,
	s1Quorum float64,
	s1Timeout, s1BloomTimeout, s1MQTTTimeout time.Duration,
	s1WorkerConcurrency, s1MaxAttempts, s1QueueDepth int,
	s1AttemptTimeout, s1InitialBackoff, s1MaxBackoff time.Duration,
	s1QueueSpoolDir string,
	s1BatchSize int,
	brokerURL, brokerClientCert, brokerClientKey, brokerCACert string,
	distributeMode string,
	embeddedBrokerWSAddr, controlPlaneURL, pullObjectBaseURL string,
	embeddedBrokerTLSCert, embeddedBrokerTLSKey string,
	embeddedBrokerAuth bool,
	enrollTLSAddr, enrollURL string,
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
	var gatewayQueue *api.GatewayQueue // non-nil only in gateway mode

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

		// CVMFS_GATEWAY_KEY_ID selects which key the gateway recognises.
		// Must match a key_id in the gateway's key file
		// (e.g. /etc/cvmfs/keys/<repo>.gw: "plain_text <key_id> <secret>").
		// Defaults to "cvmfs-prepub" if not set.
		gatewayKeyID := os.Getenv("CVMFS_GATEWAY_KEY_ID")
		if gatewayKeyID == "" {
			gatewayKeyID = "cvmfs-prepub"
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

		lc := lease.NewClient(gatewayURL, gatewayKeyID, gatewaySecret, obs)
		if leaseRetryMax > 0 {
			lc.RetryMax = leaseRetryMax
		}
		leaseBackend = lc
		gatewayQueue = api.NewGatewayQueue(lc, obs)
		obs.Logger.Info("gateway credentials", "key_id", gatewayKeyID)
		obs.Logger.Info("publish backend: gateway", "url", gatewayURL,
			"lease_retry_max", lc.EffectiveRetryMax(),
			"poll_interval", "1s")

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
	if err := runProbe(probeCtx, casBackend, leaseBackend, obs); err != nil {
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
		Enabled:        provenanceEnabled,
		RekorServer:    rekorServer,
		SigningKeyPath: rekorSigningKey,
		OIDCIssuers:    oidcIssuerList,
	}
	provProvider, err := provenance.New(provCfg, spoolRoot, obs)
	if err != nil {
		obs.Logger.Error("failed to initialise provenance provider", "error", err)
		os.Exit(1)
	}

	// ── Shared dedup Bloom filter (optional, gateway mode only) ─────────────
	//
	// Default dedup path: the pipeline calls cfg.CAS.Exists once per object
	// (a stat or S3 HEAD request) before each Put.  For local-disk or S3 CAS
	// this is fast and requires no startup walk and no memory overhead.
	//
	// Enable --bloom-filter (or --bloom-snapshot-dir, which implies it) when
	// CAS.Exists is expensive — e.g. a high-latency network CAS where each
	// HEAD request takes tens of milliseconds.  The Bloom filter replaces most
	// CAS.Exists calls with an in-memory lookup seeded at startup.
	//
	// Seed path: walk the CVMFS catalog tree via Stratum 0 (when --stratum0-url
	// and --repo-name are set) — 10–100× faster than a CAS filesystem walk for
	// large repositories.  Falls back to a CAS walk when either flag is absent
	// or Stratum 0 is temporarily unreachable.
	//
	// Add() is called after each successful CAS upload so the filter stays
	// current without any per-job walk.
	useBloom := bloomFilterEnabled || sharedFilter.Enabled // --bloom-snapshot-dir implies --bloom-filter
	// If the CAS backend can check existence natively (e.g. local os.Stat or a
	// direct S3 HEAD), the Bloom filter adds overhead — an in-memory lookup plus
	// an RWMutex acquire — on top of an already-cheap CAS.Exists call.  Suppress
	// it unconditionally for such backends and warn if the operator explicitly
	// requested it, so the flag does not silently become a no-op.
	if nec, ok := casBackend.(cas.NativeExistsChecker); ok && nec.ExistsIsNative() {
		if useBloom {
			obs.Logger.Warn("--bloom-filter / --bloom-snapshot-dir ignored: CAS backend supports native existence checks (os.Stat / S3 HEAD); using direct CAS.Exists instead")
		}
		useBloom = false
	}
	var sharedDedup *dedup.Checker
	if casBackend != nil && useBloom {
		seedMethod := "CAS filesystem walk"
		if stratum0URL != "" && repoName != "" {
			seedMethod = "CVMFS catalog tree walk (stratum0)"
		} else if stratum0URL != "" {
			seedMethod = "CAS filesystem walk (--repo-name not set; catalog walk unavailable)"
		}
		obs.Logger.Info("initializing shared dedup Bloom filter", "method", seedMethod)

		// Ensure the temp directory for catalog SQLite downloads exists.
		// walkCatalogTree writes one file per catalog hash here; each is removed
		// immediately after the catalog is processed.
		dedupTmpDir := spoolRoot + "/dedup-tmp"
		if mkErr := os.MkdirAll(dedupTmpDir, 0o750); mkErr != nil {
			obs.Logger.Error("failed to create dedup temp directory", "dir", dedupTmpDir, "error", mkErr)
			os.Exit(1)
		}

		// Use a generous timeout: the catalog walk downloads and queries many
		// SQLite files over HTTP for large repositories.  5 minutes handles repos
		// with thousands of nested catalogs.  NewFromCatalog's internal CAS-walk
		// fallback applies its own 30-second timeout if catalog seeding is skipped.
		dedupInitCtx, dedupInitCancel := context.WithTimeout(context.Background(), 5*time.Minute)
		var dedupInitErr error
		sharedDedup, dedupInitErr = dedup.NewFromCatalog(
			dedupInitCtx,
			stratum0URL, // empty string → CAS walk fallback inside NewFromCatalog
			repoName,    // empty string → CAS walk fallback inside NewFromCatalog
			dedupTmpDir, // temp dir for catalog SQLite files; cleaned up per-catalog
			casBackend,
			sharedFilter,
			obs,
		)
		dedupInitCancel()
		if dedupInitErr != nil {
			obs.Logger.Error("failed to initialize shared dedup Bloom filter at startup", "error", dedupInitErr)
			os.Exit(1)
		}
		obs.Logger.Info("shared dedup Bloom filter ready — all jobs will use this checker")
	} else if casBackend != nil {
		if nec, ok := casBackend.(cas.NativeExistsChecker); ok && nec.ExistsIsNative() {
			obs.Logger.Info("dedup: using direct CAS.Exists per object (CAS backend has native existence check; Bloom filter not needed)")
		} else {
			obs.Logger.Info("dedup: using direct CAS.Exists per object (Bloom filter disabled; enable with --bloom-filter for high-latency CAS)")
		}
	}

	// Embedded control-plane broker (alternative to an external mosquitto): run
	// an in-process MQTT broker with a WebSocket listener on S0. The publisher's
	// own broker clients connect on localhost; receivers connect via the URL
	// advertised in discovery (ADR-0001 D7/D10).
	var brokerClose func()
	var enrollSrv *credential.EnrollServer
	var pubCreds func() (string, string)
	var authHook *brokerAuthHook
	var ctrlSecret []byte
	var revoc *revocation
	var ctrlTLSClose func()
	var enrollOverTLS bool
	if embeddedBrokerWSAddr != "" {
		// Build the broker's server TLS config (H1: real wss://). When no cert is
		// configured the listener stays plaintext ws:// (dev), but advertising a
		// wss:// control-plane URL without a cert is a hard misconfiguration.
		var brokerTLS *tls.Config
		if embeddedBrokerTLSCert != "" && embeddedBrokerTLSKey != "" {
			cert, cerr := tls.LoadX509KeyPair(embeddedBrokerTLSCert, embeddedBrokerTLSKey)
			if cerr != nil {
				obs.Logger.Error("embedded broker: loading TLS cert/key", "error", cerr)
				os.Exit(1)
			}
			brokerTLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
		} else if strings.HasPrefix(controlPlaneURL, "wss://") {
			obs.Logger.Error("embedded broker: --control-plane-url is wss:// but --embedded-broker-tls-cert/--embedded-broker-tls-key are not set")
			os.Exit(1)
		}
		if embeddedBrokerAuth {
			secret := []byte(os.Getenv("PREPUB_HMAC_SECRET"))
			if len(secret) < 16 {
				obs.Logger.Error("embedded broker: --embedded-broker-auth requires PREPUB_HMAC_SECRET (>= 16 bytes)")
				os.Exit(1)
			}
			ctrlSecret = secret
			revoc = newRevocation()
			minter := credential.NewMinter(secret)
			authHook = newBrokerAuthHook(credential.NewVerifier(secret), "publisher", revoc, obs)
			enrollSrv = credential.NewEnrollServer(&derivedEnrollStore{secret: secret, revoc: revoc},
				minter, func(m string, a ...any) { obs.Logger.Info(m, a...) })
			enrollSrv.Scope = "control"
			pubCreds = func() (string, string) {
				tok, _, merr := minter.Mint("publisher", "control", randNonce(), 10*time.Minute)
				if merr != nil {
					return "", ""
				}
				return "publisher", tok
			}
			obs.Logger.Info("embedded broker: token authentication enabled")
		}
		c, brokerSrv, berr := startEmbeddedBroker(embeddedBrokerWSAddr, brokerTLS, authHook, obs)
		if berr != nil {
			obs.Logger.Error("failed to start embedded broker", "error", berr)
			os.Exit(1)
		}
		brokerClose = c
		// Serve enroll/revoke over TLS so the enrollment token never travels in plaintext.
		if embeddedBrokerAuth && enrollTLSAddr != "" {
			if brokerTLS == nil {
				obs.Logger.Error("--enroll-tls-addr requires --embedded-broker-tls-cert/--embedded-broker-tls-key")
				os.Exit(1)
			}
			rl := credential.NewIPRateLimiter(5, 10, 4096, 100, 200)
			ctClose, cterr := startControlTLS(enrollTLSAddr, brokerTLS, enrollSrv, rl.Middleware,
				credential.NewVerifier(ctrlSecret), revoc, authHook, brokerSrv, obs)
			if cterr != nil {
				obs.Logger.Error("failed to start TLS control endpoint", "error", cterr)
				os.Exit(1)
			}
			ctrlTLSClose = ctClose
			enrollOverTLS = true
		}
		if brokerURL == "" {
			scheme := "ws"
			if brokerTLS != nil {
				scheme = "wss"
			}
			brokerURL = scheme + "://localhost" + embeddedBrokerWSAddr
		}
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
				Endpoints:            endpoints,
				Quorum:               s1Quorum,
				Timeout:              s1Timeout,
				Obs:                  obs,
				DevMode:              devMode,
				HMACSecret:           hmacSecret,
				BloomQueryTimeout:    s1BloomTimeout,
				BrokerConfig:         brokerCfg,
				MQTTQuorumTimeout:    s1MQTTTimeout,
				WorkerConcurrency:    s1WorkerConcurrency,
				QueueDepth:           s1QueueDepth,
				WorkerAttemptTimeout: s1AttemptTimeout,
				WorkerInitialBackoff: s1InitialBackoff,
				WorkerMaxBackoff:     s1MaxBackoff,
				WorkerMaxAttempts:    s1MaxAttempts,
				BatchSize:            s1BatchSize,
				// Default spool dir: {spoolRoot}/dist-queue.
				// Operators can override via --s1-queue-spool-dir or YAML config (queue_spool_dir).
				QueueSpoolDir: func() string {
					if s1QueueSpoolDir != "" {
						return s1QueueSpoolDir
					}
					return spoolRoot + "/dist-queue"
				}(),
			}
			obs.Logger.Info("Stratum 1 distribution configured",
				"endpoints", len(endpoints),
				"mqtt", brokerCfg != nil,
				"quorum", s1Quorum,
				"worker_concurrency", distCfg.WorkerConcurrency,
				"attempt_timeout", distCfg.WorkerAttemptTimeout,
				"batch_size", distCfg.BatchSize)
		}
	}

	// Create the queue-driven distribution manager when S1 endpoints are
	// configured.  The manager is started after the orchestrator is wired up
	// so that the server startup sequence is linear.
	// Attach the publisher's token credentials to the distribution broker config
	// BEFORE NewManager snapshots it, so announce clients present a token (H3).
	if pubCreds != nil && distCfg != nil && distCfg.BrokerConfig != nil {
		distCfg.BrokerConfig.CredentialsProvider = pubCreds
	}
	var distManager *distribute.Manager
	if distCfg != nil && len(distCfg.Endpoints) > 0 {
		distManager = distribute.NewManager(*distCfg, casBackend)
	}

	// Build a broker config for post-commit publish notifications.
	// Reuses the same broker credentials as the distribution path so that
	// operators only need one set of MQTT credentials per node.
	var publishBrokerCfg *broker.Config
	if brokerURL != "" {
		publishBrokerCfg = &broker.Config{
			BrokerURL:  brokerURL,
			ClientCert: brokerClientCert,
			ClientKey:  brokerClientKey,
			CACert:     brokerCACert,
			// ClientID is left empty; publishMQTTNotification derives a unique
			// per-notification suffix from the new root hash.
		}
	}

	if pubCreds != nil && publishBrokerCfg != nil {
		publishBrokerCfg.CredentialsProvider = pubCreds
	}
	pullManifestStore := serve.NewMemManifestStore()
	orch := &api.Orchestrator{
		Spool:        sp,
		CAS:          casBackend,
		Lease:        leaseBackend,
		GatewayQueue: gatewayQueue,
		CVMFSMount:   cvmfsMount,
		Stratum0URL:  stratum0URL,
		DirectGraft:  gatewayDirectGraft,
		JobTimeout:   jobTimeout,
		BrokerConfig: publishBrokerCfg,
		Pipeline: pipeline.Config{
			Workers:       4,
			UploadConc:    pipelineUploadConc,
			CompressLevel: pipelineCompressLevel,
			CAS:           casBackend,
			SpoolDir:      spoolRoot,
			Obs:           obs,
			SharedFilter:  sharedFilter,
			DedupChecker:  sharedDedup, // shared across all jobs — no per-job CAS walk
		},
		Distribute:        distCfg,
		DistManager:       distManager,
		Notify:            notifyBus,
		Provenance:        provProvider,
		Obs:               obs,
		Manifests:         pullManifestStore,
		PullObjectBaseURL: pullObjectBaseURL,
	}

	if jobTimeout > 0 {
		obs.Logger.Info("per-job timeout enabled", "job_timeout", jobTimeout)
	}

	// Start the distribution manager after the orchestrator is created so the
	// manager's worker goroutines have access to the shared CAS backend.
	if distManager != nil {
		distManager.Start(context.Background())
		obs.Logger.Info("distribution manager started", "endpoints", len(distCfg.Endpoints))
	}

	apiServer := api.New(obs, apiToken, orch, sp, notifyBus, spoolRoot, stagingRoot, minConcurrentJobs, maxConcurrentJobs)

	// Control-plane DoS limiter (internet-exposed; no firewall assumed).
	ctrlRateLimit := credential.NewIPRateLimiter(5, 10, 4096, 100, 200)
	if controlPlaneURL != "" {
		var discoSigner serve.Signer
		if ctrlSecret != nil {
			discoSigner = hmacDiscoverySigner(ctrlSecret)
		}
		disco := &staticDiscovery{repos: []string{repoName}, cp: serve.ControlPlaneRef{Type: "mqtt", URL: controlPlaneURL}, signer: discoSigner}
		if enrollOverTLS {
			disco.enrollURL = enrollURL
		}
		apiServer.MountDiscovery(ctrlRateLimit.Middleware(&serve.DiscoveryHandler{Source: disco}))
		obs.Logger.Info("control-plane: discovery advertising broker", "url", controlPlaneURL)
	}

	// ADR-0001 pull mode: serve objects + manifests (incl. the gateway POST
	// ingest) so Stratum 1 can pull on a prepare announce. Default push leaves
	// these routes unmounted.
	if distributeMode == "pull" {
		// Admission control (ADR D6): cap concurrent receiver pulls and issue one
		// lease per node at a time. Limits are conservative defaults for the small
		// Stratum 1 fleet; make them configurable when the benchmark (P5) lands.
		admission := commit.NewAdmission(commit.Options{MaxConcurrent: 16, MaxPerNode: 1})
		plaintextEnroll := enrollSrv
		if enrollOverTLS {
			plaintextEnroll = nil // enrollment is served over TLS only
		}
		apiServer.MountDistributeServing(api.DistributeServing{
			CAS:       casBackend,
			Manifests: pullManifestStore,
			Admission: admission,
			Enroll:    plaintextEnroll,
			RateLimit: ctrlRateLimit.Middleware,
		})
		obs.Logger.Info("ADR-0001: pull-mode distribute serving enabled")
	}

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
	if ctrlTLSClose != nil {
		ctrlTLSClose()
	}
	if brokerClose != nil {
		brokerClose()
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
	stratum0URL string,
	distributeMode string,
	discoveryURL string,
	brokerAuth bool,
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

	enrollBase := discoveryURL
	if discoveryURL != "" && len(repoList) > 0 {
		discoCtx, discoStop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		d, derr := fetchDiscoveryWithRetry(discoCtx, discoveryURL, repoList[0], obs)
		discoStop()
		if derr != nil {
			if discoCtx.Err() != nil {
				obs.Logger.Info("control-plane: discovery interrupted — shutting down")
				os.Exit(0)
			}
			obs.Logger.Error("control-plane: discovery failed", "error", derr)
			os.Exit(1)
		}
		if brokerAuth {
			if !d.Verify(hmacDiscoveryVerify([]byte(os.Getenv("PREPUB_HMAC_SECRET")))) {
				obs.Logger.Error("control-plane: discovery signature verification FAILED — refusing advertised broker (possible MITM)")
				os.Exit(1)
			}
		}
		if d.ControlPlane.Type != "" && d.ControlPlane.Type != "mqtt" {
			obs.Logger.Error("control-plane: discovery advertised unsupported transport", "type", d.ControlPlane.Type)
			os.Exit(1)
		}
		if d.ControlPlane.URL == "" {
			obs.Logger.Error("control-plane: discovery returned an empty broker URL")
			os.Exit(1)
		}
		brokerURL = d.ControlPlane.URL
		obs.Logger.Info("control-plane: broker URL learned from discovery", "url", brokerURL, "type", d.ControlPlane.Type)
		if d.EnrollURL != "" {
			enrollBase = d.EnrollURL
			obs.Logger.Info("control-plane: enroll endpoint learned from discovery (TLS)", "url", enrollBase)
		}
	}

	var brokerCreds func() (string, string)
	if brokerAuth {
		secret := []byte(os.Getenv("PREPUB_HMAC_SECRET"))
		if len(secret) < 16 {
			obs.Logger.Error("--broker-auth requires PREPUB_HMAC_SECRET (>= 16 bytes)")
			os.Exit(1)
		}
		if discoveryURL == "" {
			obs.Logger.Error("--broker-auth requires --discovery-url (the enroll endpoint base)")
			os.Exit(1)
		}
		var enrollHTTP *http.Client
		if strings.HasPrefix(enrollBase, "https://") {
			if brokerCACert == "" {
				obs.Logger.Error("control-plane: TLS enroll endpoint advertised but --broker-ca-cert is not set")
				os.Exit(1)
			}
			hc, herr := caHTTPClient(brokerCACert)
			if herr != nil {
				obs.Logger.Error("control-plane: loading enroll CA", "error", herr)
				os.Exit(1)
			}
			enrollHTTP = hc
		}
		ec := &credential.Client{Base: enrollBase, HTTP: enrollHTTP, Node: nodeID, Key: deriveNodeKey(secret, nodeID)}
		brokerCreds = func() (string, string) {
			tok, terr := ec.Token(context.Background())
			if terr != nil {
				obs.Logger.Warn("control-plane: enrollment failed", "error", terr)
				return "", ""
			}
			return nodeID, tok
		}
	}
	cfg := receiver.Config{
		ControlAddr:               controlAddr,
		DataAddr:                  dataAddr,
		DataHost:                  dataHost,
		TLSCert:                   tlsCert,
		TLSKey:                    tlsKey,
		HMACSecret:                hmacSecret,
		CASRoot:                   casRoot,
		SessionTTL:                sessionTTL,
		DiskHeadroom:              diskHeadroom,
		DevMode:                   devMode,
		BloomCapacity:             bloomCapacity,
		BloomFPRate:               bloomFPRate,
		CoordURL:                  coordURL,
		CoordToken:                coordToken,
		NodeID:                    nodeID,
		Repos:                     repoList,
		Stratum0URL:               stratum0URL,
		PullMode:                  distributeMode == "pull",
		PullManifestBase:          stratum0URL, // in pull mode this points at the cvmfs-prepub endpoint
		BrokerURL:                 brokerURL,
		BrokerClientCert:          brokerClientCert,
		BrokerClientKey:           brokerClientKey,
		BrokerCACert:              brokerCACert,
		Obs:                       obs,
		BrokerCredentialsProvider: brokerCreds,
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
