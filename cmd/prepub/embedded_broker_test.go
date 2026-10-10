// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package main

import "testing"

// The publisher's own client always dials localhost on the listener's port,
// whatever address the listener is bound to.
func TestLocalBrokerURL(t *testing.T) {
	for _, tc := range []struct {
		addr string
		tls  bool
		want string
	}{
		{":1882", false, "ws://localhost:1882"},
		{"0.0.0.0:1882", false, "ws://localhost:1882"},
		{"[::]:1882", true, "wss://localhost:1882"},
		{"127.0.0.1:1883", true, "wss://localhost:1883"},
	} {
		got, err := localBrokerURL(tc.addr, tc.tls)
		if err != nil || got != tc.want {
			t.Errorf("localBrokerURL(%q, %v) = %q, %v; want %q", tc.addr, tc.tls, got, err, tc.want)
		}
	}
	for _, bad := range []string{"1882", "localhost", "host:"} {
		if got, err := localBrokerURL(bad, false); err == nil {
			t.Errorf("localBrokerURL(%q) = %q, want an error", bad, got)
		}
	}
}
