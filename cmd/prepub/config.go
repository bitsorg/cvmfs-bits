// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package main

// config.go — YAML configuration file support for cvmfs-prepub.
//
// Priority (highest → lowest):
//   1. Command-line flag explicitly set by the operator
//   2. Value from the config file (--config /etc/cvmfs-prepub/config.yaml)
//   3. Compiled-in flag default
//
// This means you can always override any config file setting on the command
// line without editing the file — useful for one-off tests.

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// fileConfig mirrors the YAML config file structure.  All fields are optional;
// missing fields leave the corresponding flag at its default value.
//
// Example minimal config (Option A, local publish mode):
//
//	spool_root: /mnt/build/bits/spool
//	publish_mode: local
//	cas:
//	  type: localfs
//	  root: /mnt/build/bits/cas
//
// Example gateway-mode publisher:
//
//	server:
//	  listen: ":8080"
//	spool_root: /var/spool/cvmfs-prepub
//	publish_mode: gateway
//	gateway:
//	  url: http://localhost:4929
//	cas:
//	  type: localfs
//	  root: /srv/cvmfs/cas
//
// Example receiver:
//
//	mode: receiver
//	control_addr: ":9100"
//	repos: [atlas.cern.ch]
//	receiver_stratum0_url: http://stratum0.example.org:8080
//	broker_ca_cert: /etc/cvmfs-prepub/tls/ca.crt
//
// Bool keys are pointers so an explicit `false` overrides a default-true flag.
type fileConfig struct {
	Mode        string `yaml:"mode"`
	LogLevel    string `yaml:"log_level"`
	Dev         *bool  `yaml:"dev"`
	SpoolRoot   string `yaml:"spool_root"`
	StagingRoot string `yaml:"staging_root"`
	PublishMode string `yaml:"publish_mode"`
	// JobTimeout is the maximum wall-clock time a single job may run before it
	// is cancelled.  Zero (default) means no timeout.
	JobTimeout yamlDuration `yaml:"job_timeout"`
	// MinConcurrentJobs is the guaranteed floor for the dynamic concurrency
	// limit.  The effective slot count is max(MinConcurrentJobs, numCPU - load1min).
	// 0 (default) falls back to the CLI flag default (4).
	MinConcurrentJobs int `yaml:"min_concurrent_jobs"`
	// MaxConcurrentJobs is the ceiling for the dynamic concurrency limit.
	// 0 (default) means runtime.NumCPU().
	MaxConcurrentJobs int    `yaml:"max_concurrent_jobs"`
	CVMFSMount        string `yaml:"cvmfs_mount"`

	// IngestPublish offers the "ingest" publish path in addition to the default
	// selected by publish_mode: a job may ask for its tar to be handed to
	// `cvmfs_server ingest` so the gateway does the chunking, dedup and
	// catalogs.  IngestPublishOwner maps to `ingest -u`.
	IngestPublish      *bool  `yaml:"ingest_publish"`
	IngestPublishOwner string `yaml:"ingest_publish_owner"`

	// ReplaceOnConflict lets a job that asks for it replace what another build
	// published at its path (--replace-on-conflict). Destructive by design, so
	// it is opt-in, defaults to off, and never applies to a job that did not ask.
	ReplaceOnConflict *bool `yaml:"replace_on_conflict"`
	// PreWarm makes Stratum 1 pre-warming available (--prewarm); jobs opt in.
	PreWarm *bool `yaml:"prewarm"`

	// PromoteWorkers is the concurrency of the staged path's server-side copy
	// into the CAS (--promote-workers). Zero/omitted keeps the CLI default.
	PromoteWorkers int `yaml:"promote_workers"`

	// MeasurementsDir is where per-publish measurement records are written
	// (--measurements-dir). Empty uses <spool>/measurements; "off" disables.
	MeasurementsDir string `yaml:"measurements_dir"`

	// CatalogCacheDir keeps the published catalogs that existence and hash
	// checks download (--catalog-cache-dir). Empty uses systemd's cache
	// directory ($CACHE_DIRECTORY/catalogs), else <spool>/catalog-cache; "off"
	// disables. CatalogCacheMiB caps it (--catalog-cache-mib).
	CatalogCacheDir string `yaml:"catalog_cache_dir"`
	CatalogCacheMiB int    `yaml:"catalog_cache_mib"`

	// Coarse-publish finalize: one cvmfs_swissknife ingestsql
	// invocation commits a whole build. Without IngestConfigPrefix the finalize
	// is DISABLED, and since a sealed build finalizes server-side, that failure
	// is silent from the producer's side — packages upload, the pipeline goes
	// green, and nothing is ever committed. These had no config keys at all
	// until now: the unit passes only --config, so a stock install could not
	// configure the finalize without editing the unit file.
	IngestSwissknife   string   `yaml:"ingest_swissknife"`
	IngestConfigPrefix string   `yaml:"ingest_config_prefix"`
	IngestEnv          []string `yaml:"ingest_env"`

	// Stratum0URL is the base URL of the Stratum 0 CVMFS server
	// (e.g. "http://stratum0/cvmfs").  Required in gateway mode so the
	// orchestrator can fetch the existing root catalog for merging.
	// Without this the catalog merge step is skipped (only valid for the
	// very first publish of an empty repository).
	Stratum0URL string `yaml:"stratum0_url"`
	// RepoName is the CVMFS repository name for catalog-based dedup seeding at
	// startup (e.g. "atlas.cern.ch").  Retained for labelling publishes; no
	// longer used for dedup seeding (dedup is a direct CAS.Exists per object).
	// Leave empty to fall back to the CAS walk (safe, just slower).
	RepoName string `yaml:"repo_name"`

	Server struct {
		Listen string `yaml:"listen"`
		// DebugListen is the pprof listener address (e.g. 127.0.0.1:6060).
		// Empty disables it. Loopback only — profiles expose heap contents.
		DebugListen string `yaml:"debug_listen"`
		// AuthMode: bearer | both | hmac (see --auth-mode). Empty = both.
		AuthMode string `yaml:"auth_mode"`
		// SignatureSkew is how far a signed request's timestamp may lag before
		// it is refused; the replay cache retains nonces for twice this. Empty
		// = the built-in default (2m). Raise it only if the fleet's clocks are
		// genuinely that far apart — every second of it is a second longer a
		// captured signature stays usable.
		SignatureSkew yamlDuration `yaml:"signature_skew"`
	} `yaml:"server"`

	Gateway struct {
		URL string `yaml:"url"`
		// DirectGraft controls the fast-path commit that bypasses DiffRec on the
		// receiver.  Defaults to true (enabled).  Set to false only when publishes
		// via this node may update pre-existing content at the lease path, in which
		// case the standard DiffRec path is required for correctness.
		DirectGraft *bool `yaml:"direct_graft"`
		// AllowPlaintext permits a plaintext http:// gateway URL. Gateway
		// requests are HMAC-SHA256 signed and the secret never transits, so
		// plaintext does not expose the credential; it exposes what is being
		// published and lets an on-path attacker forge gateway responses.
		// Reasonable on a trusted internal network, and deliberately separate
		// from `dev`, which also disables authentication requirements.
		AllowPlaintext *bool `yaml:"allow_plaintext"`
	} `yaml:"gateway"`

	CAS struct {
		Type string `yaml:"type"`
		Root string `yaml:"root"`
		// ServerConf is the repository's own CVMFS server.conf
		// (/etc/cvmfs/repositories.d/<repo>/server.conf). For type "s3" the
		// backend follows its CVMFS_UPSTREAM_STORAGE to the S3 config file and
		// takes bucket, endpoint, credentials and repository alias from there,
		// so prepub cannot drift from the storage the repository is served
		// from. Defaults to the path implied by repo_name.
		ServerConf string `yaml:"server_conf"`
	} `yaml:"cas"`

	// MQTT broker CA — verifies the control-plane broker's server certificate.
	// The broker URL is derived from the embedded broker / learned from discovery;
	// there is no external broker URL or client-cert mTLS.
	BrokerCACert string `yaml:"broker_ca_cert"`

	// Receiver-mode settings.
	ControlAddr string `yaml:"control_addr"`
	NodeID      string `yaml:"node_id"`
	// Repos is a list of CVMFS repositories served by this receiver.
	// Equivalent to --repos (comma-separated on the CLI).
	Repos []string `yaml:"repos"`
	// ReceiverStratum0URL is the cvmfs-prepub publisher base URL the receiver
	// pulls from (e.g. "http://stratum0.example.org:8080").
	// Equivalent to --receiver-stratum0-url.
	ReceiverStratum0URL string `yaml:"receiver_stratum0_url"`

	// Provenance / Rekor transparency log.
	Provenance      *bool    `yaml:"provenance"`
	RekorServer     string   `yaml:"rekor_server"`
	RekorSigningKey string   `yaml:"rekor_signing_key"`
	OIDCIssuers     []string `yaml:"oidc_issuers"`

	// AllowedPublishPrefixes are the CVMFS group-root paths this deployment may
	// publish into (e.g. "/cvmfs/repo.cern.ch/lcg"). A reserve/submit whose target
	// falls outside every root is rejected. Empty disables the check. For a group
	// whose user area is a sibling of releases/ (…/<group>/user vs …/<group>/releases),
	// list the group ROOT so both are covered. Equivalent to --allowed-publish-prefix.
	AllowedPublishPrefixes []string `yaml:"allowed_publish_prefixes"`

	// MaxTarSizeGiB is the largest package tar one submission may carry
	// (default 10). Equivalent to --max-tar-size-gib.
	MaxTarSizeGiB int `yaml:"max_tar_size_gib"`
	// RetryWindow is how long from submission a job failing for a retryable
	// reason is retried (default 24h; 0 disables retries). A pointer, like the
	// bools, so that an explicit 0 is distinguishable from an absent key.
	RetryWindow *yamlDuration `yaml:"retry_window"`
	// SpoolMinFreeGiB is the free space an upload must leave on the spool
	// filesystem, else it is refused with 507 (default 20; 0 disables).
	// A pointer for the same reason. Equivalent to --spool-min-free-gib.
	SpoolMinFreeGiB *int `yaml:"spool_min_free_gib"`

	// Chunking overrides the CVMFS content-defined (xor32) chunk sizes in
	// bytes. Zero/omitted fields keep the CLI defaults, which are pinned to a
	// FIXED cvmfsdescriptor.ChunkGrid (6 MiB, min==avg==max) for coarse-publish
	// /ingestsql compatibility — override only in per-package-only deployments.
	// Equivalent to --chunk-min/--chunk-avg/--chunk-max.
	Chunking struct {
		Min int64 `yaml:"min"`
		Avg int64 `yaml:"avg"`
		Max int64 `yaml:"max"`
	} `yaml:"chunking"`

	// Pipeline tunes the compress/upload stages. Workers is the memory lever:
	// each compress worker holds one whole file plus its compressed chunks, so
	// peak RSS scales with workers x largest-file. Zero/omitted keeps the CLI
	// default. Equivalent to --pipeline-workers / --pipeline-upload-conc.
	Pipeline struct {
		Workers           int `yaml:"workers"`
		UploadConcurrency int `yaml:"upload_concurrency"`
		// PrefetchLimit is the budget for concurrent tar scans (phase 0),
		// in units of 128 MiB. Phase 0 runs before a job takes a concurrency
		// slot and so is not covered by it. Each scan is charged by tar size:
		// a flat count treats a 4 KiB modulefile and a 600 MiB ROOT tar as
		// equivalent, which holds up until several large packages coincide.
		//
		PrefetchLimit int `yaml:"prefetch_limit"`
		// Prefetch turns the phase-0 look-ahead on or off. A POINTER so that an
		// absent key (nil, meaning "use the default") is distinguishable from
		// an explicit `prefetch: false`. Disabling is right on I/O-bound
		// storage: the look-ahead spills the unpacked tar and the pipeline
		// reads it back, doubling I/O on the resource that is the bottleneck.
		Prefetch *bool `yaml:"prefetch"`
	} `yaml:"pipeline"`
}

// yamlDuration allows duration strings like "30s", "10m", "1h" in YAML.
// time.Duration does not implement yaml.Unmarshaler by default.
type yamlDuration struct{ time.Duration }

func (d *yamlDuration) UnmarshalYAML(value *yaml.Node) error {
	if value.Value == "" {
		return nil
	}
	dur, err := time.ParseDuration(value.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", value.Value, err)
	}
	d.Duration = dur
	return nil
}

// loadFileConfig reads and parses the YAML config file at path.
func loadFileConfig(path string) (*fileConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}
	var fc fileConfig
	if err := yaml.Unmarshal(data, &fc); err != nil {
		return nil, fmt.Errorf("parsing config file %q: %w", path, err)
	}
	return &fc, nil
}

// applyFileConfig copies values from fc into the flag variables, skipping
// any flag whose name appears in explicit (i.e. was set on the command line).
//
// String/numeric zero values and absent bool keys are treated as "not set"
// and leave the flag at its default; a bool key present as true or false is
// applied, as is a retry_window or spool_min_free_gib present as 0.
func applyFileConfig(fc *fileConfig, explicit map[string]bool,
	mode, logLevel *string,
	devMode *bool,
	spoolRoot, stagingRoot, listen, publishMode, gatewayURL, cvmfsMount, casType, casRoot,
	casServerConf *string,
	stratum0URL, repoName *string,
	jobTimeout *time.Duration,
	minConcurrentJobs, maxConcurrentJobs *int,
	brokerCACert *string,
	controlAddr *string,
	nodeID, repos, recvStratum0URL *string,
	provenanceEnabled *bool,
	rekorServer, rekorSigningKey, oidcIssuers *string,
	allowedPublishPrefixes *string,
	gatewayDirectGraft *bool,
	gatewayAllowPlaintext *bool,
	authMode *string,
	debugListen *string,
	signatureSkew *time.Duration,
	ingestPublish *bool,
	ingestPublishOwner *string,
	replaceOnConflict *bool,
	measurementsDir *string,
	ingestSwissknife, ingestConfigPrefix, ingestEnv *string,
	chunkMin, chunkAvg, chunkMax *int64,
	pipelineWorkers, pipelineUploadConc, prefetchLimit, promoteWorkers *int,
	prefetch *bool,
	maxTarSizeGiB, spoolMinFreeGiB *int,
	retryWindow *time.Duration,
	preWarm *bool,
	catalogCacheDir *string, catalogCacheMiB *int,
) {
	has := func(name string) bool { return explicit[name] }
	str := func(flag string, dst *string, val string) {
		if !has(flag) && val != "" {
			*dst = val
		}
	}
	dur := func(flag string, dst *time.Duration, val yamlDuration) {
		if !has(flag) && val.Duration != 0 {
			*dst = val.Duration
		}
	}
	bl := func(flag string, dst *bool, val *bool) {
		if !has(flag) && val != nil {
			*dst = *val
		}
	}
	i := func(flag string, dst *int, val int) {
		if !has(flag) && val != 0 {
			*dst = val
		}
	}
	// Pointer-typed settings: present (even as 0) is applied, absent is not.
	durP := func(flag string, dst *time.Duration, val *yamlDuration) {
		if !has(flag) && val != nil {
			*dst = val.Duration
		}
	}
	iP := func(flag string, dst *int, val *int) {
		if !has(flag) && val != nil {
			*dst = *val
		}
	}
	i64 := func(flag string, dst *int64, val int64) {
		if !has(flag) && val != 0 {
			*dst = val
		}
	}

	str("mode", mode, fc.Mode)
	str("log-level", logLevel, fc.LogLevel)
	bl("dev", devMode, fc.Dev)

	str("spool-root", spoolRoot, fc.SpoolRoot)
	str("staging-root", stagingRoot, fc.StagingRoot)
	str("listen", listen, fc.Server.Listen)
	str("publish-mode", publishMode, fc.PublishMode)
	str("gateway-url", gatewayURL, fc.Gateway.URL)
	str("cvmfs-mount", cvmfsMount, fc.CVMFSMount)
	str("stratum0-url", stratum0URL, fc.Stratum0URL)
	str("repo-name", repoName, fc.RepoName)
	str("cas-type", casType, fc.CAS.Type)
	str("cas-root", casRoot, fc.CAS.Root)
	str("cas-server-conf", casServerConf, fc.CAS.ServerConf)
	str("auth-mode", authMode, fc.Server.AuthMode)
	str("debug-listen", debugListen, fc.Server.DebugListen)
	dur("signature-skew", signatureSkew, fc.Server.SignatureSkew)
	i("pipeline-workers", pipelineWorkers, fc.Pipeline.Workers)
	i("pipeline-upload-conc", pipelineUploadConc, fc.Pipeline.UploadConcurrency)
	i("prefetch-limit", prefetchLimit, fc.Pipeline.PrefetchLimit)
	i("promote-workers", promoteWorkers, fc.PromoteWorkers)
	i("max-tar-size-gib", maxTarSizeGiB, fc.MaxTarSizeGiB)
	iP("spool-min-free-gib", spoolMinFreeGiB, fc.SpoolMinFreeGiB)
	durP("retry-window", retryWindow, fc.RetryWindow)
	bl("prefetch", prefetch, fc.Pipeline.Prefetch)
	dur("job-timeout", jobTimeout, fc.JobTimeout)
	if !has("min-concurrent-jobs") && fc.MinConcurrentJobs != 0 {
		*minConcurrentJobs = fc.MinConcurrentJobs
	}
	if !has("max-concurrent-jobs") && fc.MaxConcurrentJobs != 0 {
		*maxConcurrentJobs = fc.MaxConcurrentJobs
	}

	// MQTT broker CA (the only broker flag; the broker URL is derived from the
	// embedded broker / learned from discovery, and there is no client-cert mTLS).
	str("broker-ca-cert", brokerCACert, fc.BrokerCACert)

	// Receiver.
	str("control-addr", controlAddr, fc.ControlAddr)
	str("node-id", nodeID, fc.NodeID)
	if !has("repos") && len(fc.Repos) > 0 {
		*repos = strings.Join(fc.Repos, ",")
	}
	str("receiver-stratum0-url", recvStratum0URL, fc.ReceiverStratum0URL)

	// Provenance.
	bl("provenance", provenanceEnabled, fc.Provenance)
	str("rekor-server", rekorServer, fc.RekorServer)
	str("rekor-signing-key", rekorSigningKey, fc.RekorSigningKey)
	if !has("oidc-issuers") && len(fc.OIDCIssuers) > 0 {
		*oidcIssuers = strings.Join(fc.OIDCIssuers, ",")
	}
	if !has("allowed-publish-prefix") && len(fc.AllowedPublishPrefixes) > 0 {
		*allowedPublishPrefixes = strings.Join(fc.AllowedPublishPrefixes, ",")
	}

	bl("gateway-direct-graft", gatewayDirectGraft, fc.Gateway.DirectGraft)
	bl("gateway-allow-plaintext", gatewayAllowPlaintext, fc.Gateway.AllowPlaintext)
	bl("ingest-publish", ingestPublish, fc.IngestPublish)
	str("ingest-publish-owner", ingestPublishOwner, fc.IngestPublishOwner)
	bl("replace-on-conflict", replaceOnConflict, fc.ReplaceOnConflict)
	bl("prewarm", preWarm, fc.PreWarm)
	str("measurements-dir", measurementsDir, fc.MeasurementsDir)
	str("catalog-cache-dir", catalogCacheDir, fc.CatalogCacheDir)
	i("catalog-cache-mib", catalogCacheMiB, fc.CatalogCacheMiB)
	str("ingest-swissknife", ingestSwissknife, fc.IngestSwissknife)
	str("ingest-config-prefix", ingestConfigPrefix, fc.IngestConfigPrefix)
	if !has("ingest-env") && len(fc.IngestEnv) > 0 {
		*ingestEnv = strings.Join(fc.IngestEnv, ",")
	}

	// Content-defined chunking sizes (xor32); zero/omitted -> CLI default.
	i64("chunk-min", chunkMin, fc.Chunking.Min)
	i64("chunk-avg", chunkAvg, fc.Chunking.Avg)
	i64("chunk-max", chunkMax, fc.Chunking.Max)
}
