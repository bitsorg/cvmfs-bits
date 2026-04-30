package main

// main_test.go — unit tests for standalone startup helpers in cmd/prepub.
//
// These tests cover the pure functions that are testable without spinning up
// an HTTP server or touching the file-system:
//
//   • parseLogLevel     — string → slog.Level mapping
//   • loadFileConfig    — YAML file → *fileConfig
//   • applyFileConfig   — config-file values applied to flag vars
//
// Each test is self-contained and race-safe (no shared state).

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ── parseLogLevel ─────────────────────────────────────────────────────────────

func TestParseLogLevel_KnownLevels(t *testing.T) {
	cases := []struct {
		input string
		want  slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"error", slog.LevelError},
	}
	for _, tc := range cases {
		got := parseLogLevel(tc.input)
		if got != tc.want {
			t.Errorf("parseLogLevel(%q) = %v; want %v", tc.input, got, tc.want)
		}
	}
}

func TestParseLogLevel_UnknownDefaultsToInfo(t *testing.T) {
	for _, input := range []string{"", "verbose", "TRACE", "42", "DEBUG"} {
		got := parseLogLevel(input)
		if got != slog.LevelInfo {
			t.Errorf("parseLogLevel(%q) = %v; want Info (default)", input, got)
		}
	}
}

// ── loadFileConfig ────────────────────────────────────────────────────────────

func writeYAML(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "*.yaml")
	if err != nil {
		t.Fatalf("creating temp file: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("writing temp file: %v", err)
	}
	f.Close()
	return f.Name()
}

func TestLoadFileConfig_ValidYAML(t *testing.T) {
	path := writeYAML(t, `
spool_root: /custom/spool
publish_mode: gateway
gateway:
  url: http://gw.example.com:4929
log_level: debug
`)
	fc, err := loadFileConfig(path)
	if err != nil {
		t.Fatalf("loadFileConfig: %v", err)
	}
	if fc.SpoolRoot != "/custom/spool" {
		t.Errorf("SpoolRoot = %q; want /custom/spool", fc.SpoolRoot)
	}
	if fc.PublishMode != "gateway" {
		t.Errorf("PublishMode = %q; want gateway", fc.PublishMode)
	}
	if fc.Gateway.URL != "http://gw.example.com:4929" {
		t.Errorf("Gateway.URL = %q; want http://gw.example.com:4929", fc.Gateway.URL)
	}
	if fc.LogLevel != "debug" {
		t.Errorf("LogLevel = %q; want debug", fc.LogLevel)
	}
}

func TestLoadFileConfig_InvalidYAML(t *testing.T) {
	path := writeYAML(t, `{not: valid yaml: [`)
	_, err := loadFileConfig(path)
	if err == nil {
		t.Error("expected error for malformed YAML, got nil")
	}
}

func TestLoadFileConfig_FileNotFound(t *testing.T) {
	_, err := loadFileConfig(filepath.Join(t.TempDir(), "nonexistent.yaml"))
	if err == nil {
		t.Error("expected error for missing file, got nil")
	}
}

func TestLoadFileConfig_EmptyFile(t *testing.T) {
	path := writeYAML(t, "")
	fc, err := loadFileConfig(path)
	if err != nil {
		t.Fatalf("loadFileConfig on empty file: %v", err)
	}
	// All fields should be zero values.
	if fc.SpoolRoot != "" || fc.PublishMode != "" {
		t.Errorf("expected zero-value fileConfig, got non-empty fields")
	}
}

func TestLoadFileConfig_Distribution(t *testing.T) {
	path := writeYAML(t, `
distribution:
  stratum1_endpoints:
    - https://s1a.example.org
    - https://s1b.example.org
  quorum: 0.75
  timeout: 10m
`)
	fc, err := loadFileConfig(path)
	if err != nil {
		t.Fatalf("loadFileConfig: %v", err)
	}
	if len(fc.Distribution.Endpoints) != 2 {
		t.Errorf("len(Endpoints) = %d; want 2", len(fc.Distribution.Endpoints))
	}
	if fc.Distribution.Quorum != 0.75 {
		t.Errorf("Quorum = %v; want 0.75", fc.Distribution.Quorum)
	}
	if fc.Distribution.Timeout.Duration != 10*time.Minute {
		t.Errorf("Timeout = %v; want 10m", fc.Distribution.Timeout.Duration)
	}
}

// ── applyFileConfig ───────────────────────────────────────────────────────────

func TestApplyFileConfig_CopiesWhenNotExplicit(t *testing.T) {
	fc := &fileConfig{
		SpoolRoot:   "/from/config",
		LogLevel:    "warn",
		PublishMode: "local",
	}
	explicit := map[string]bool{} // nothing set on CLI

	// Set up flag variables at their defaults.
	spoolRoot := "/default/spool"
	logLevel := "info"
	mode := "publisher"
	devMode := false
	stagingRoot := ""
	listen := ":8080"
	publishMode := "gateway"
	gatewayURL := "https://localhost:4929"
	cvmfsMount := "/cvmfs"
	casType := "localfs"
	casRoot := "/var/lib/cas"
	stratum0URL := ""
	s1Endpoints := ""
	s1Quorum := 1.0
	s1Timeout := 60 * time.Second
	s1BloomTimeout := time.Duration(0)
	s1MQTTTimeout := 30 * time.Second
	brokerURL := ""
	brokerClientCert := ""
	brokerClientKey := ""
	brokerCACert := ""
	controlAddr := ":9100"
	dataAddr := ":9101"
	dataHost := ""
	tlsCert := ""
	tlsKey := ""
	sessionTTL := time.Hour
	diskHeadroom := 1.2
	nodeID := ""
	repos := ""
	coordURL := ""
	bloomSnapshotDir := ""
	bloomNodeID := ""
	bloomMaxSnapshotAge := time.Duration(0)
	bloomFilterCapacity := uint(0)
	bloomFilterFPRate := 0.0
	recvBloomCapacity := uint(0)
	recvBloomFPRate := 0.0
	provenanceEnabled := false
	rekorServer := ""
	rekorSigningKey := ""
	oidcIssuers := ""

	applyFileConfig(fc, explicit,
		&mode, &logLevel, &devMode,
		&spoolRoot, &stagingRoot, &listen, &publishMode, &gatewayURL, &cvmfsMount, &casType, &casRoot,
		&stratum0URL,
		&s1Endpoints, &s1Quorum, &s1Timeout, &s1BloomTimeout, &s1MQTTTimeout,
		&brokerURL, &brokerClientCert, &brokerClientKey, &brokerCACert,
		&controlAddr, &dataAddr, &dataHost, &tlsCert, &tlsKey,
		&sessionTTL, &diskHeadroom,
		&nodeID, &repos, &coordURL,
		&bloomSnapshotDir, &bloomNodeID,
		&bloomMaxSnapshotAge, &bloomFilterCapacity, &bloomFilterFPRate,
		&recvBloomCapacity, &recvBloomFPRate,
		&provenanceEnabled, &rekorServer, &rekorSigningKey, &oidcIssuers,
	)

	if spoolRoot != "/from/config" {
		t.Errorf("spoolRoot = %q; want /from/config", spoolRoot)
	}
	if logLevel != "warn" {
		t.Errorf("logLevel = %q; want warn", logLevel)
	}
	if publishMode != "local" {
		t.Errorf("publishMode = %q; want local", publishMode)
	}
}

func TestApplyFileConfig_CLIOverridesConfig(t *testing.T) {
	fc := &fileConfig{
		SpoolRoot: "/from/config",
	}
	// Mark spool-root as explicitly set via CLI.
	explicit := map[string]bool{"spool-root": true}

	spoolRoot := "/from/cli"
	logLevel := "info"
	mode := "publisher"
	devMode := false
	stagingRoot := ""
	listen := ":8080"
	publishMode := "gateway"
	gatewayURL := "https://localhost:4929"
	cvmfsMount := "/cvmfs"
	casType := "localfs"
	casRoot := "/var/lib/cas"
	stratum0URL := ""
	s1Endpoints := ""
	s1Quorum := 1.0
	s1Timeout := 60 * time.Second
	s1BloomTimeout := time.Duration(0)
	s1MQTTTimeout := 30 * time.Second
	brokerURL, brokerClientCert, brokerClientKey, brokerCACert := "", "", "", ""
	controlAddr, dataAddr, dataHost, tlsCert, tlsKey := ":9100", ":9101", "", "", ""
	sessionTTL := time.Hour
	diskHeadroom := 1.2
	nodeID, repos, coordURL := "", "", ""
	bloomSnapshotDir, bloomNodeID := "", ""
	bloomMaxSnapshotAge := time.Duration(0)
	bloomFilterCapacity := uint(0)
	bloomFilterFPRate := 0.0
	recvBloomCapacity := uint(0)
	recvBloomFPRate := 0.0
	provenanceEnabled := false
	rekorServer, rekorSigningKey, oidcIssuers := "", "", ""

	applyFileConfig(fc, explicit,
		&mode, &logLevel, &devMode,
		&spoolRoot, &stagingRoot, &listen, &publishMode, &gatewayURL, &cvmfsMount, &casType, &casRoot,
		&stratum0URL,
		&s1Endpoints, &s1Quorum, &s1Timeout, &s1BloomTimeout, &s1MQTTTimeout,
		&brokerURL, &brokerClientCert, &brokerClientKey, &brokerCACert,
		&controlAddr, &dataAddr, &dataHost, &tlsCert, &tlsKey,
		&sessionTTL, &diskHeadroom,
		&nodeID, &repos, &coordURL,
		&bloomSnapshotDir, &bloomNodeID,
		&bloomMaxSnapshotAge, &bloomFilterCapacity, &bloomFilterFPRate,
		&recvBloomCapacity, &recvBloomFPRate,
		&provenanceEnabled, &rekorServer, &rekorSigningKey, &oidcIssuers,
	)

	// spool-root was explicitly set on CLI — config value must not override it.
	if spoolRoot != "/from/cli" {
		t.Errorf("spoolRoot = %q; want /from/cli (CLI should win)", spoolRoot)
	}
}

func TestApplyFileConfig_EndpointSlice(t *testing.T) {
	fc := &fileConfig{}
	fc.Distribution.Endpoints = []string{"https://s1a.example.org", "https://s1b.example.org"}

	explicit := map[string]bool{}
	s1Endpoints := ""
	// fill remaining params with throwaway variables
	mode, logLevel := "publisher", "info"
	devMode := false
	spoolRoot, stagingRoot, listen, publishMode, gatewayURL, cvmfsMount, casType, casRoot := "/sp", "", ":8080", "gateway", "https://gw", "/cvmfs", "localfs", "/cas"
	stratum0URL := ""
	s1Quorum := 1.0
	s1Timeout, s1BloomTimeout, s1MQTTTimeout := 60*time.Second, time.Duration(0), 30*time.Second
	brokerURL, brokerClientCert, brokerClientKey, brokerCACert := "", "", "", ""
	controlAddr, dataAddr, dataHost, tlsCert, tlsKey := ":9100", ":9101", "", "", ""
	sessionTTL := time.Hour
	diskHeadroom := 1.2
	nodeID, repos, coordURL := "", "", ""
	bloomSnapshotDir, bloomNodeID := "", ""
	bloomMaxSnapshotAge := time.Duration(0)
	bloomFilterCapacity := uint(0)
	bloomFilterFPRate := 0.0
	recvBloomCapacity := uint(0)
	recvBloomFPRate := 0.0
	provenanceEnabled := false
	rekorServer, rekorSigningKey, oidcIssuers := "", "", ""

	applyFileConfig(fc, explicit,
		&mode, &logLevel, &devMode,
		&spoolRoot, &stagingRoot, &listen, &publishMode, &gatewayURL, &cvmfsMount, &casType, &casRoot,
		&stratum0URL,
		&s1Endpoints, &s1Quorum, &s1Timeout, &s1BloomTimeout, &s1MQTTTimeout,
		&brokerURL, &brokerClientCert, &brokerClientKey, &brokerCACert,
		&controlAddr, &dataAddr, &dataHost, &tlsCert, &tlsKey,
		&sessionTTL, &diskHeadroom,
		&nodeID, &repos, &coordURL,
		&bloomSnapshotDir, &bloomNodeID,
		&bloomMaxSnapshotAge, &bloomFilterCapacity, &bloomFilterFPRate,
		&recvBloomCapacity, &recvBloomFPRate,
		&provenanceEnabled, &rekorServer, &rekorSigningKey, &oidcIssuers,
	)

	// Endpoints slice should be joined with comma.
	if s1Endpoints != "https://s1a.example.org,https://s1b.example.org" {
		t.Errorf("s1Endpoints = %q; want joined comma list", s1Endpoints)
	}
}
