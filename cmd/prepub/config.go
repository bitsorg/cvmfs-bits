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
// Example full config (Option B with MQTT):
//
//	server:
//	  listen: ":8080"
//	  tls_cert: /etc/cvmfs-prepub/tls/server.crt
//	  tls_key:  /etc/cvmfs-prepub/tls/server.key
//	spool_root: /var/spool/cvmfs-prepub
//	publish_mode: gateway
//	gateway:
//	  url: http://localhost:4929
//	cas:
//	  type: localfs
//	  root: /srv/cvmfs/cas
//	distribution:
//	  stratum1_endpoints:
//	    - https://s1a.example.org:9100
//	    - https://s1b.example.org:9100
//	  quorum: 0.75
//	  timeout: 10m
//	broker_url: tls://broker.example.org:8883
//	broker_client_cert: /etc/cvmfs-prepub/tls/publisher.crt
//	broker_client_key:  /etc/cvmfs-prepub/tls/publisher.key
//	broker_ca_cert:     /etc/cvmfs-prepub/tls/ca.crt
type fileConfig struct {
	Mode        string `yaml:"mode"`
	LogLevel    string `yaml:"log_level"`
	Dev         bool   `yaml:"dev"`
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
		Listen  string `yaml:"listen"`
		TLSCert string `yaml:"tls_cert"`
		TLSKey  string `yaml:"tls_key"`
	} `yaml:"server"`

	Gateway struct {
		URL string `yaml:"url"`
		// DirectGraft controls the fast-path commit that bypasses DiffRec on the
		// receiver.  Defaults to true (enabled).  Set to false only when publishes
		// via this node may update pre-existing content at the lease path, in which
		// case the standard DiffRec path is required for correctness.
		// Can be overridden at runtime with --gateway-direct-graft=false.
		DirectGraft bool `yaml:"direct_graft"`
		// RootMerge selects the optional A/B "root-merge" commit path: build the
		// catalog from the repository root down to the lease path and let the
		// gateway 3-way merge it (instead of grafting a leaf subtree + mkdir-p).
		// Defaults to false.  Mutually exclusive with DirectGraft; when set it
		// wins (root-merge implies no direct graft).  Override at runtime with
		// --gateway-root-merge.
		RootMerge bool `yaml:"root_merge"`
	} `yaml:"gateway"`

	CAS struct {
		Type string `yaml:"type"`
		Root string `yaml:"root"`
	} `yaml:"cas"`

	Distribution struct {
		// WarmQuorum is the fraction of authoritative Stratum 1 replicas that must
		// report warm before the catalog commit proceeds (ADR-0001 D6).
		WarmQuorum float64 `yaml:"warm_quorum"`
	} `yaml:"distribution"`

	// MQTT broker CA — verifies the control-plane broker's server certificate.
	// The broker URL is derived from the embedded broker / learned from discovery;
	// there is no external broker URL or client-cert mTLS.
	BrokerCACert string `yaml:"broker_ca_cert"`

	// Receiver-mode settings.
	ControlAddr  string       `yaml:"control_addr"`
	DataAddr     string       `yaml:"data_addr"`
	DataHost     string       `yaml:"data_host"`
	SessionTTL   yamlDuration `yaml:"session_ttl"`
	DiskHeadroom float64      `yaml:"disk_headroom"`
	NodeID       string       `yaml:"node_id"`
	// Repos is a list of CVMFS repositories served by this receiver.
	// Equivalent to --repos (comma-separated on the CLI).
	Repos []string `yaml:"repos"`
	// ReceiverStratum0URL is the Stratum 0 HTTP base URL used by the receiver
	// to pull CAS objects when a PublishedMessage is received over MQTT.
	// Example: "http://stratum0.example.org/cvmfs"
	// Equivalent to --receiver-stratum0-url.
	ReceiverStratum0URL string `yaml:"receiver_stratum0_url"`

	// Provenance / Rekor transparency log.
	Provenance      bool     `yaml:"provenance"`
	RekorServer     string   `yaml:"rekor_server"`
	RekorSigningKey string   `yaml:"rekor_signing_key"`
	OIDCIssuers     []string `yaml:"oidc_issuers"`

	// Chunking overrides the CVMFS content-defined (xor32) chunk sizes in
	// bytes. Zero/omitted fields keep the CLI defaults (4/8/16 MiB).
	// Equivalent to --chunk-min/--chunk-avg/--chunk-max.
	Chunking struct {
		Min int64 `yaml:"min"`
		Avg int64 `yaml:"avg"`
		Max int64 `yaml:"max"`
	} `yaml:"chunking"`
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
// String/numeric zero values in the config struct are treated as "not set"
// and leave the flag at its default.  Bool flags are only set when true in
// the config (there is no way to force a flag to false via the config file;
// use the command line for that).
func applyFileConfig(fc *fileConfig, explicit map[string]bool,
	mode, logLevel *string,
	devMode *bool,
	spoolRoot, stagingRoot, listen, publishMode, gatewayURL, cvmfsMount, casType, casRoot *string,
	stratum0URL, repoName *string,
	jobTimeout *time.Duration,
	minConcurrentJobs, maxConcurrentJobs *int,
	warmQuorum *float64,
	brokerCACert *string,
	controlAddr, dataAddr, dataHost, tlsCert, tlsKey *string,
	sessionTTL *time.Duration,
	diskHeadroom *float64,
	nodeID, repos, recvStratum0URL *string,
	provenanceEnabled *bool,
	rekorServer, rekorSigningKey, oidcIssuers *string,
	gatewayDirectGraft *bool,
	gatewayRootMerge *bool,
	chunkMin, chunkAvg, chunkMax *int64,
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
	flt := func(flag string, dst *float64, val float64) {
		if !has(flag) && val != 0 {
			*dst = val
		}
	}
	i64 := func(flag string, dst *int64, val int64) {
		if !has(flag) && val != 0 {
			*dst = val
		}
	}

	str("mode", mode, fc.Mode)
	str("log-level", logLevel, fc.LogLevel)
	if !has("dev") && fc.Dev {
		*devMode = true
	}

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
	dur("job-timeout", jobTimeout, fc.JobTimeout)
	if !has("min-concurrent-jobs") && fc.MinConcurrentJobs != 0 {
		*minConcurrentJobs = fc.MinConcurrentJobs
	}
	if !has("max-concurrent-jobs") && fc.MaxConcurrentJobs != 0 {
		*maxConcurrentJobs = fc.MaxConcurrentJobs
	}

	// server.tls_cert / tls_key apply to both publisher and receiver.
	str("tls-cert", tlsCert, fc.Server.TLSCert)
	str("tls-key", tlsKey, fc.Server.TLSKey)

	// Warm-quorum: fraction of authoritative Stratum 1 replicas that must report
	// warm before the catalog commit proceeds (ADR-0001 D6).
	flt("warm-quorum", warmQuorum, fc.Distribution.WarmQuorum)

	// MQTT broker CA (the only broker flag; the broker URL is derived from the
	// embedded broker / learned from discovery, and there is no client-cert mTLS).
	str("broker-ca-cert", brokerCACert, fc.BrokerCACert)

	// Receiver.
	str("control-addr", controlAddr, fc.ControlAddr)
	str("data-addr", dataAddr, fc.DataAddr)
	str("data-host", dataHost, fc.DataHost)
	dur("session-ttl", sessionTTL, fc.SessionTTL)
	flt("disk-headroom", diskHeadroom, fc.DiskHeadroom)
	str("node-id", nodeID, fc.NodeID)
	if !has("repos") && len(fc.Repos) > 0 {
		*repos = strings.Join(fc.Repos, ",")
	}
	str("receiver-stratum0-url", recvStratum0URL, fc.ReceiverStratum0URL)

	// Provenance.
	if !has("provenance") && fc.Provenance {
		*provenanceEnabled = true
	}
	str("rekor-server", rekorServer, fc.RekorServer)
	str("rekor-signing-key", rekorSigningKey, fc.RekorSigningKey)
	if !has("oidc-issuers") && len(fc.OIDCIssuers) > 0 {
		*oidcIssuers = strings.Join(fc.OIDCIssuers, ",")
	}

	// Gateway commit mode.  The flag defaults to true; config can only reaffirm
	// true (bool fields have no zero-vs-explicit-false distinction in YAML).
	// To disable direct-graft use --gateway-direct-graft=false on the CLI.
	if !has("gateway-direct-graft") && fc.Gateway.DirectGraft {
		*gatewayDirectGraft = true
	}
	// Optional A/B root-merge commit path.  The flag defaults to false; config
	// can only reaffirm true (bool fields have no zero-vs-explicit-false
	// distinction in YAML).  Enable on the CLI with --gateway-root-merge.
	if !has("gateway-root-merge") && fc.Gateway.RootMerge {
		*gatewayRootMerge = true
	}

	// Content-defined chunking sizes (xor32); zero/omitted -> CLI default.
	i64("chunk-min", chunkMin, fc.Chunking.Min)
	i64("chunk-avg", chunkAvg, fc.Chunking.Avg)
	i64("chunk-max", chunkMax, fc.Chunking.Max)
}
