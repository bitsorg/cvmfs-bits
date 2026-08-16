// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

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
promote_workers: 32
`)
	fc, err := loadFileConfig(path)
	if err != nil {
		t.Fatalf("loadFileConfig: %v", err)
	}
	// The key must be exercised through YAML, not just through a Go literal:
	// loadFileConfig does a plain Unmarshal with no KnownFields, so a typo in
	// the struct tag makes the operator's setting a silent no-op.
	//
	// NEGATIVE CONTROL: change the tag to `yaml:"promote_workerz"` and this
	// fails with "PromoteWorkers = 0; want 32".
	if fc.PromoteWorkers != 32 {
		t.Errorf("PromoteWorkers = %d; want 32", fc.PromoteWorkers)
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

func TestLoadFileConfig_WarmQuorum(t *testing.T) {
	path := writeYAML(t, `
distribution:
  warm_quorum: 0.75
`)
	fc, err := loadFileConfig(path)
	if err != nil {
		t.Fatalf("loadFileConfig: %v", err)
	}
	if fc.Distribution.WarmQuorum != 0.75 {
		t.Errorf("WarmQuorum = %v; want 0.75", fc.Distribution.WarmQuorum)
	}
}

// ── applyFileConfig ───────────────────────────────────────────────────────────

// applyTestVars holds default-valued flag variables for an applyFileConfig call.
type applyTestVars struct {
	mode, logLevel                                          string
	devMode                                                 bool
	spoolRoot, stagingRoot, listen, publishMode, gatewayURL string
	cvmfsMount, casType, casRoot, casServerConf             string
	stratum0URL, repoName                                   string
	jobTimeout                                              time.Duration
	minConcurrentJobs, maxConcurrentJobs                    int
	warmQuorum                                              float64
	brokerCACert                                            string
	controlAddr, dataAddr, dataHost, tlsCert, tlsKey        string
	sessionTTL                                              time.Duration
	diskHeadroom                                            float64
	nodeID, repos, recvStratum0URL                          string
	provenanceEnabled                                       bool
	rekorServer, rekorSigningKey, oidcIssuers               string
	allowedPublishPrefixes                                  string
	gatewayDirectGraft                                      bool
	gatewayAllowPlaintext                                   bool
	authMode                                                string
	debugListen                                             string
	signatureSkew                                           time.Duration
	ingestPublish                                           bool
	ingestPublishOwner                                      string
	replaceOnConflict                                       bool
	measurementsDir                                         string
	ingestSwissknife, ingestConfigPrefix, ingestEnv         string
	chunkMin, chunkAvg, chunkMax                            int64
	pipelineWorkers, pipelineUploadConc, prefetchLimit      int
	promoteWorkers                                          int
	prefetch                                                bool
}

func defaultApplyVars() *applyTestVars {
	return &applyTestVars{
		mode: "publisher", logLevel: "info",
		spoolRoot: "/default/spool", listen: ":8080", publishMode: "gateway",
		gatewayURL: "https://localhost:4929", cvmfsMount: "/cvmfs",
		casType: "localfs", casRoot: "/var/lib/cas",
		jobTimeout: 0, warmQuorum: 1.0,
		controlAddr: ":9100", dataAddr: ":9101",
		sessionTTL: time.Hour, diskHeadroom: 1.2,
	}
}

func (v *applyTestVars) apply(fc *fileConfig, explicit map[string]bool) {
	applyFileConfig(fc, explicit,
		&v.mode, &v.logLevel, &v.devMode,
		&v.spoolRoot, &v.stagingRoot, &v.listen, &v.publishMode, &v.gatewayURL, &v.cvmfsMount, &v.casType, &v.casRoot,
		&v.casServerConf,
		&v.stratum0URL, &v.repoName,
		&v.jobTimeout, &v.minConcurrentJobs, &v.maxConcurrentJobs,
		&v.warmQuorum,
		&v.brokerCACert,
		&v.controlAddr, &v.dataAddr, &v.dataHost, &v.tlsCert, &v.tlsKey,
		&v.sessionTTL, &v.diskHeadroom,
		&v.nodeID, &v.repos, &v.recvStratum0URL,
		&v.provenanceEnabled, &v.rekorServer, &v.rekorSigningKey, &v.oidcIssuers,
		&v.allowedPublishPrefixes,
		&v.gatewayDirectGraft, &v.gatewayAllowPlaintext,
		&v.authMode,
		&v.debugListen,
		&v.signatureSkew,
		&v.ingestPublish, &v.ingestPublishOwner,
		&v.replaceOnConflict, &v.measurementsDir,
		&v.ingestSwissknife, &v.ingestConfigPrefix, &v.ingestEnv,
		&v.chunkMin, &v.chunkAvg, &v.chunkMax,
		&v.pipelineWorkers, &v.pipelineUploadConc, &v.prefetchLimit, &v.promoteWorkers, &v.prefetch,
	)
}

func TestApplyFileConfig_CopiesWhenNotExplicit(t *testing.T) {
	fc := &fileConfig{SpoolRoot: "/from/config", LogLevel: "warn", PublishMode: "local"}
	v := defaultApplyVars()
	v.apply(fc, map[string]bool{})

	if v.spoolRoot != "/from/config" {
		t.Errorf("spoolRoot = %q; want /from/config", v.spoolRoot)
	}
	if v.logLevel != "warn" {
		t.Errorf("logLevel = %q; want warn", v.logLevel)
	}
	if v.publishMode != "local" {
		t.Errorf("publishMode = %q; want local", v.publishMode)
	}
}

func TestApplyFileConfig_CLIOverridesConfig(t *testing.T) {
	fc := &fileConfig{SpoolRoot: "/from/config"}
	v := defaultApplyVars()
	v.spoolRoot = "/from/cli"
	v.apply(fc, map[string]bool{"spool-root": true})

	if v.spoolRoot != "/from/cli" {
		t.Errorf("spoolRoot = %q; want /from/cli (CLI should win)", v.spoolRoot)
	}
}

func TestApplyFileConfig_WarmQuorum(t *testing.T) {
	fc := &fileConfig{}
	fc.Distribution.WarmQuorum = 0.5
	v := defaultApplyVars()
	v.apply(fc, map[string]bool{})

	if v.warmQuorum != 0.5 {
		t.Errorf("warmQuorum = %v; want 0.5 (copied from config)", v.warmQuorum)
	}
}

// TestApplyFileConfig_PipelineWorkers guards the memory lever: peak RSS scales
// with the compress worker count (each worker holds a whole file plus its
// compressed chunks), so a constrained host must be able to lower it from the
// config file, and an explicit flag must still win.
func TestApplyFileConfig_PipelineWorkers(t *testing.T) {
	fc := &fileConfig{}
	fc.Pipeline.Workers = 1
	fc.Pipeline.UploadConcurrency = 2

	v := defaultApplyVars()
	v.pipelineWorkers, v.pipelineUploadConc = 4, 4
	v.apply(fc, map[string]bool{})
	if v.pipelineWorkers != 1 {
		t.Errorf("pipeline.workers = %d; want 1 from config", v.pipelineWorkers)
	}
	if v.pipelineUploadConc != 2 {
		t.Errorf("pipeline.upload_concurrency = %d; want 2 from config", v.pipelineUploadConc)
	}

	// An explicitly-set flag must not be overridden by the file.
	v = defaultApplyVars()
	v.pipelineWorkers = 8
	v.apply(fc, map[string]bool{"pipeline-workers": true})
	if v.pipelineWorkers != 8 {
		t.Errorf("explicit --pipeline-workers was overridden: %d", v.pipelineWorkers)
	}
}

func TestApplyFileConfig_ReplaceOnConflict(t *testing.T) {
	fc := &fileConfig{ReplaceOnConflict: true}
	v := defaultApplyVars()
	v.apply(fc, map[string]bool{})
	if !v.replaceOnConflict {
		t.Error("replace_on_conflict: true in config was not applied")
	}
	// An explicit CLI flag wins over the config file, both directions.
	v2 := defaultApplyVars()
	v2.apply(fc, map[string]bool{"replace-on-conflict": true})
	if v2.replaceOnConflict {
		t.Error("explicit --replace-on-conflict=false was overridden by config")
	}
}

// The config file supplies the promote concurrency only when the flag was not
// given; an explicit --promote-workers wins. Same rule as every other int.
func TestApplyFileConfig_PromoteWorkers(t *testing.T) {
	fc := &fileConfig{PromoteWorkers: 32}
	v := defaultApplyVars()
	v.promoteWorkers = 16
	v.apply(fc, map[string]bool{})
	if v.promoteWorkers != 32 {
		t.Errorf("promoteWorkers = %d; want 32 from config", v.promoteWorkers)
	}

	v2 := defaultApplyVars()
	v2.promoteWorkers = 64
	v2.apply(fc, map[string]bool{"promote-workers": true})
	if v2.promoteWorkers != 64 {
		t.Errorf("promoteWorkers = %d; explicit flag must win", v2.promoteWorkers)
	}
}
