// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package receiver

import (
	"testing"
	"time"
)

func TestAutoTune(t *testing.T) {
	cases := []struct {
		name string
		rtt  time.Duration
		n, k int
	}{
		{"CERN <1ms", 500 * time.Microsecond, 32, 1},
		{"EU ~20ms", 20 * time.Millisecond, 16, 8},
		{"US ~100ms", 100 * time.Millisecond, 8, 32},
		{"Asia >200ms", 300 * time.Millisecond, 8, 64},
	}
	for _, c := range cases {
		n, k := autoTune(c.rtt)
		if n != c.n || k != c.k {
			t.Errorf("%s: autoTune(%v) = (%d,%d), want (%d,%d)", c.name, c.rtt, n, k, c.n, c.k)
		}
	}
}
