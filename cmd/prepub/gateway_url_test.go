// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package main

// checkGatewayURL is a security gate: it decides whether prepub will talk to a
// gateway over an unencrypted hop. The cases below pin both directions — that
// the default refuses, and that the documented opt-ins work — so that a future
// change cannot quietly widen or narrow it.

import (
	"strings"
	"testing"
)

func TestCheckGatewayURL(t *testing.T) {
	cases := []struct {
		name           string
		url            string
		allowPlaintext bool
		devMode        bool
		wantErr        bool
		wantWarn       bool
	}{
		{name: "https is always fine", url: "https://gateway.cern.ch:4929"},
		{name: "https with allow flag set is still silent", url: "https://gateway.cern.ch:4929", allowPlaintext: true},

		// Loopback needs no flag: cvmfs_gateway conventionally listens on
		// http://localhost:4929 and there is no network to observe.
		{name: "loopback localhost", url: "http://localhost:4929"},
		{name: "loopback v4", url: "http://127.0.0.1:4929"},
		{name: "loopback v6", url: "http://[::1]:4929"},

		// The default for a remote plaintext gateway is refusal.
		{name: "remote plaintext refused by default", url: "http://gateway:4929", wantErr: true},

		// Explicit, narrow opt-in for a trusted network.
		{name: "remote plaintext allowed by flag", url: "http://gateway:4929", allowPlaintext: true, wantWarn: true},

		// --dev still works but is not the recommended route.
		{name: "remote plaintext under dev", url: "http://gateway:4929", devMode: true, wantWarn: true},

		// The narrow flag must be enough on its own — a site that made an
		// informed transport choice must not be pushed into --dev, which also
		// disables the gateway-secret and API-token requirements.
		{name: "flag alone suffices without dev", url: "http://gw.internal:4929", allowPlaintext: true, wantWarn: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			warn, err := checkGatewayURL(tc.url, tc.allowPlaintext, tc.devMode)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("checkGatewayURL(%q) = nil error; want refusal", tc.url)
				}
				// The error has to tell the operator about the narrow opt-in,
				// or they will reach for --dev instead.
				if !strings.Contains(err.Error(), "--gateway-allow-plaintext") {
					t.Errorf("refusal should name the opt-in flag, got: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("checkGatewayURL(%q) = %v; want acceptance", tc.url, err)
			}
			if tc.wantWarn && warn == "" {
				t.Error("an unencrypted hop must be logged as a warning")
			}
			if !tc.wantWarn && warn != "" {
				t.Errorf("unexpected warning for %q: %s", tc.url, warn)
			}
		})
	}
}

// TestCheckGatewayURL_WarningDescribesTheActualExposure guards the wording that
// makes the trade-off decidable. The credential is NOT exposed — requests are
// HMAC-signed and the secret never transits — so a warning implying otherwise
// would push operators towards TLS work they may not need, while one that
// omitted the real exposure would understate it.
func TestCheckGatewayURL_WarningDescribesTheActualExposure(t *testing.T) {
	warn, err := checkGatewayURL("http://gateway:4929", true, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, want := range []string{"HMAC", "secret never transits", "responses"} {
		if !strings.Contains(warn, want) {
			t.Errorf("warning should mention %q; got: %s", want, warn)
		}
	}
}
