// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// applyYAML loads yaml through the real loader and applies it over the
// service defaults for the two settings under test.
func applyYAML(t *testing.T, yaml string) *applyTestVars {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	fc, err := loadFileConfig(path)
	if err != nil {
		t.Fatalf("loadFileConfig: %v", err)
	}
	v := defaultApplyVars()
	v.retryWindow = 24 * time.Hour
	v.spoolMinFreeGiB = 20
	v.apply(fc, map[string]bool{})
	return v
}

// An explicit 0 in YAML turns the setting off, as the flag does.
func TestApplyFileConfig_ExplicitZeroDisables(t *testing.T) {
	v := applyYAML(t, "retry_window: 0s\nspool_min_free_gib: 0\n")
	if v.retryWindow != 0 {
		t.Errorf("retry_window: 0s gave %v, want 0 (off)", v.retryWindow)
	}
	if v.spoolMinFreeGiB != 0 {
		t.Errorf("spool_min_free_gib: 0 gave %d, want 0 (off)", v.spoolMinFreeGiB)
	}
}

// Absent keys keep the defaults; set values are applied.
func TestApplyFileConfig_AbsentKeepsDefault(t *testing.T) {
	v := applyYAML(t, "log_level: info\n")
	if v.retryWindow != 24*time.Hour || v.spoolMinFreeGiB != 20 {
		t.Errorf("absent keys changed defaults: retry=%v min_free=%d", v.retryWindow, v.spoolMinFreeGiB)
	}
	v = applyYAML(t, "retry_window: 2h\nspool_min_free_gib: 5\n")
	if v.retryWindow != 2*time.Hour || v.spoolMinFreeGiB != 5 {
		t.Errorf("set values not applied: retry=%v min_free=%d", v.retryWindow, v.spoolMinFreeGiB)
	}
}

// A flag given on the command line still wins over an explicit 0 in YAML.
func TestApplyFileConfig_ExplicitZeroDoesNotOverrideFlag(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("retry_window: 0s\nspool_min_free_gib: 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fc, err := loadFileConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	v := defaultApplyVars()
	v.retryWindow = time.Hour
	v.spoolMinFreeGiB = 7
	v.apply(fc, map[string]bool{"retry-window": true, "spool-min-free-gib": true})
	if v.retryWindow != time.Hour || v.spoolMinFreeGiB != 7 {
		t.Errorf("YAML overrode explicit flags: retry=%v min_free=%d", v.retryWindow, v.spoolMinFreeGiB)
	}
}
