// SPDX-FileCopyrightText: 2026 CERN (European Organization for Nuclear Research)
// SPDX-License-Identifier: Apache-2.0
//go:build linux

package api

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// readLoadAvg reads the 1-minute load average from /proc/loadavg.
// Only compiled on Linux where /proc/loadavg is available.
func readLoadAvg() (float64, error) {
	f, err := os.Open("/proc/loadavg")
	if err != nil {
		return 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	if !scanner.Scan() {
		return 0, fmt.Errorf("empty /proc/loadavg")
	}
	fields := strings.Fields(scanner.Text())
	if len(fields) < 1 {
		return 0, fmt.Errorf("unexpected /proc/loadavg format")
	}
	return strconv.ParseFloat(fields[0], 64)
}
