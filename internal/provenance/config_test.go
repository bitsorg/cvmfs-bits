// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package provenance

import "testing"

// Security review H3: with OIDC issuers configured, an audience is mandatory —
// CI OIDC issuers are global, so an unset audience lets any workflow obtain
// Verified=true. The provider must fail closed (refuse to start).
func TestCheckOIDCAudience(t *testing.T) {
	cases := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"issuers set, audience empty -> error",
			Config{OIDCIssuers: []string{"https://token.actions.githubusercontent.com"}}, true},
		{"issuers set, audience set -> ok",
			Config{OIDCIssuers: []string{"https://gitlab.com"}, OIDCAudience: "https://prepub.example.org"}, false},
		{"no issuers -> ok (OIDC disabled)",
			Config{}, false},
		{"no issuers, audience set -> ok",
			Config{OIDCAudience: "https://prepub.example.org"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.cfg.checkOIDCAudience(); (err != nil) != tc.wantErr {
				t.Fatalf("checkOIDCAudience() err=%v, wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

// New must refuse to start (fail closed) when provenance is enabled with OIDC
// issuers but no audience — the guard fires before any key generation, so a nil
// observer is never dereferenced on this path.
func TestNewFailsClosedOnMissingAudience(t *testing.T) {
	cfg := Config{Enabled: true, OIDCIssuers: []string{"https://gitlab.com"}}
	if _, err := New(cfg, t.TempDir(), nil); err == nil {
		t.Fatal("New() should refuse to start with OIDC issuers set and no audience")
	}
}
