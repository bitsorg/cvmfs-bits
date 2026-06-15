// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0
//go:build !linux

package api

// readLoadAvg returns 0 on non-Linux platforms where /proc/loadavg is
// unavailable.  The DynamicSemaphore therefore acts as a static pool capped at
// MaxSlots — the same behaviour as before load-aware throttling was introduced.
func readLoadAvg() (float64, error) {
	return 0, nil
}
