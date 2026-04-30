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
	CVMFSMount  string `yaml:"cvmfs_mount"`
	// Stratum0URL is the base URL of the Stratum 0 CVMFS server
	// (e.g. "http://stratum0/cvmfs").  Required in gateway mode so the
	// orchestrator can fetch the existing root catalog for merging.
	// Without this the catalog merge step is skipped (only valid for the
	// very first publish of an empty repository).
	Stratum0URL string `yaml:"stratum0_url"`

	Server struct {
		Listen  string `yaml:"listen"`
		TLSCert string `yaml:"tls_cert"`
		TLSKey  string `yaml:"tls_key"`
	} `yaml:"server"`

	Gateway struct {
		URL string `yaml:"url"`
	} `yaml:"gateway"`

	CAS struct {
		Type string `yaml:"type"`
		Root string `yaml:"root"`
	} `yaml:"cas"`

	Distribution struct {
		// Endpoints is a list of Stratum 1 HTTPS URLs.
		// Equivalent to --s1-endpoints (comma-separated on the CLI).
		Endpoints         []string     `yaml:"stratum1_endpoints"`
		Quorum            float64      `yaml:"quorum"`
		Timeout           yamlDuration `yaml:"timeout"`
		BloomQueryTimeout yamlDuration `yaml:"bloom_query_timeout"`
		MQTTQuorumTimeout yamlDuration `yaml:"mqtt_quorum_timeout"`
	} `yaml:"distribution"`

	// MQTT broker settings — used by both publisher and receiver.
	BrokerURL        string `yaml:"broker_url"`
	BrokerClientCert string `yaml:"broker_client_cert"`
	BrokerClientKey  string `yaml:"broker_client_key"`
	BrokerCACert     string `yaml:"broker_ca_cert"`

	// Receiver-mode settings.
	ControlAddr  string       `yaml:"control_addr"`
	DataAddr     string       `yaml:"data_addr"`
	DataHost     string       `yaml:"data_host"`
	SessionTTL   yamlDuration `yaml:"session_ttl"`
	DiskHeadroom float64      `yaml:"disk_headroom"`
	NodeID       string       `yaml:"node_id"`
	// Repos is a list of CVMFS repositories served by this receiver.
	// Equivalent to --repos (comma-separated on the CLI).
	Repos    []string `yaml:"repos"`
	CoordURL string   `yaml:"coord_url"`

	// Shared Bloom filter (publisher).
	BloomSnapshotDir    string       `yaml:"bloom_snapshot_dir"`
	BloomNodeID         string       `yaml:"bloom_node_id"`
	BloomMaxSnapshotAge yamlDuration `yaml:"bloom_max_snapshot_age"`
	BloomFilterCapacity uint         `yaml:"bloom_filter_capacity"`
	BloomFilterFPRate   float64      `yaml:"bloom_filter_fp_rate"`

	// Inventory Bloom filter (receiver).
	BloomCapacity uint    `yaml:"bloom_capacity"`
	BloomFPRate   float64 `yaml:"bloom_fp_rate"`

	// Provenance / Rekor transparency log.
	Provenance      bool     `yaml:"provenance"`
	RekorServer     string   `yaml:"rekor_server"`
	RekorSigningKey string   `yaml:"rekor_signing_key"`
	OIDCIssuers     []string `yaml:"oidc_issuers"`
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
	stratum0URL *string,
	s1Endpoints *string,
	s1Quorum *float64,
	s1Timeout, s1BloomTimeout, s1MQTTTimeout *time.Duration,
	brokerURL, brokerClientCert, brokerClientKey, brokerCACert *string,
	controlAddr, dataAddr, dataHost, tlsCert, tlsKey *string,
	sessionTTL *time.Duration,
	diskHeadroom *float64,
	nodeID, repos, coordURL *string,
	bloomSnapshotDir, bloomNodeID *string,
	bloomMaxSnapshotAge *time.Duration,
	bloomFilterCapacity *uint,
	bloomFilterFPRate *float64,
	recvBloomCapacity *uint,
	recvBloomFPRate *float64,
	provenanceEnabled *bool,
	rekorServer, rekorSigningKey, oidcIssuers *string,
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
	str("cas-type", casType, fc.CAS.Type)
	str("cas-root", casRoot, fc.CAS.Root)

	// server.tls_cert / tls_key apply to both publisher and receiver.
	str("tls-cert", tlsCert, fc.Server.TLSCert)
	str("tls-key", tlsKey, fc.Server.TLSKey)

	// Distribution / Stratum 1.
	if !has("s1-endpoints") && len(fc.Distribution.Endpoints) > 0 {
		*s1Endpoints = strings.Join(fc.Distribution.Endpoints, ",")
	}
	flt("s1-quorum", s1Quorum, fc.Distribution.Quorum)
	dur("s1-timeout", s1Timeout, fc.Distribution.Timeout)
	dur("s1-bloom-timeout", s1BloomTimeout, fc.Distribution.BloomQueryTimeout)
	dur("s1-mqtt-quorum-timeout", s1MQTTTimeout, fc.Distribution.MQTTQuorumTimeout)

	// MQTT broker.
	str("broker-url", brokerURL, fc.BrokerURL)
	str("broker-client-cert", brokerClientCert, fc.BrokerClientCert)
	str("broker-client-key", brokerClientKey, fc.BrokerClientKey)
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
	str("coord-url", coordURL, fc.CoordURL)

	// Bloom filter (publisher).
	str("bloom-snapshot-dir", bloomSnapshotDir, fc.BloomSnapshotDir)
	str("bloom-node-id", bloomNodeID, fc.BloomNodeID)
	dur("bloom-max-snapshot-age", bloomMaxSnapshotAge, fc.BloomMaxSnapshotAge)
	if !has("bloom-filter-capacity") && fc.BloomFilterCapacity != 0 {
		*bloomFilterCapacity = fc.BloomFilterCapacity
	}
	flt("bloom-filter-fp-rate", bloomFilterFPRate, fc.BloomFilterFPRate)

	// Bloom filter (receiver).
	if !has("bloom-capacity") && fc.BloomCapacity != 0 {
		*recvBloomCapacity = fc.BloomCapacity
	}
	flt("bloom-fp-rate", recvBloomFPRate, fc.BloomFPRate)

	// Provenance.
	if !has("provenance") && fc.Provenance {
		*provenanceEnabled = true
	}
	str("rekor-server", rekorServer, fc.RekorServer)
	str("rekor-signing-key", rekorSigningKey, fc.RekorSigningKey)
	if !has("oidc-issuers") && len(fc.OIDCIssuers) > 0 {
		*oidcIssuers = strings.Join(fc.OIDCIssuers, ",")
	}
}
