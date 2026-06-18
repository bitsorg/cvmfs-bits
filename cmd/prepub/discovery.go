// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"cvmfs.io/prepub/internal/distribute/serve"
	"cvmfs.io/prepub/pkg/observe"
)

// staticDiscovery is a minimal serve.DiscoverySource advertising a fixed
// control-plane reference for the repos this publisher serves (ADR-0001 D10).
// Unsigned in dev (nil Signer).
type staticDiscovery struct {
	repos  []string
	cp     serve.ControlPlaneRef
	signer serve.Signer // nil => unsigned (dev)
}

func (d *staticDiscovery) Discovery(_ context.Context, repo string) (serve.Discovery, bool, error) {
	// The control-plane endpoint is identical for every repo this Stratum 0
	// serves, so answer for any requested repo (the receiver only needs
	// ControlPlane.URL). Repos echoes the configured list when known.
	repos := d.repos
	if len(repos) == 0 || (len(repos) == 1 && repos[0] == "") {
		repos = []string{repo}
	}
	doc := serve.Discovery{Repos: repos, ControlPlane: d.cp}
	signed, err := doc.Sign(d.signer) // nil signer => returned unchanged
	if err != nil {
		return serve.Discovery{}, false, err
	}
	return signed, true, nil
}

// hmacDiscoverySigner signs the discovery doc with HMAC-SHA256(secret, payload),
// hex-encoded — the shared-secret integrity guard (H2). No asymmetric keys.
func hmacDiscoverySigner(secret []byte) serve.Signer {
	return func(payload []byte) (string, error) {
		mac := hmac.New(sha256.New, secret)
		mac.Write(payload)
		return hex.EncodeToString(mac.Sum(nil)), nil
	}
}

// hmacDiscoveryVerify is the receiver-side verifier matching hmacDiscoverySigner.
func hmacDiscoveryVerify(secret []byte) func(payload []byte, sig string) bool {
	return func(payload []byte, sig string) bool {
		mac := hmac.New(sha256.New, secret)
		mac.Write(payload)
		want := mac.Sum(nil)
		got, err := hex.DecodeString(sig)
		if err != nil {
			return false
		}
		return hmac.Equal(got, want)
	}
}

// fetchDiscovery GETs the signed discovery document for repo from the fixed S0
// endpoint base, e.g. base + "/cvmfs/<repo>/.cvmfsbits". A per-request timeout
// bounds the call.
func fetchDiscovery(ctx context.Context, base, repo string) (serve.Discovery, error) {
	url := strings.TrimRight(base, "/") + "/cvmfs/" + repo + "/.cvmfsbits"
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return serve.Discovery{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return serve.Discovery{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return serve.Discovery{}, fmt.Errorf("discovery GET %s: status %d", url, resp.StatusCode)
	}
	var d serve.Discovery
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return serve.Discovery{}, fmt.Errorf("discovery decode %s: %w", url, err)
	}
	return d, nil
}

// fetchDiscoveryWithRetry retries with capped exponential backoff so the
// receiver tolerates the publisher coming up after it (~60s ceiling).
func fetchDiscoveryWithRetry(ctx context.Context, base, repo string, obs *observe.Provider) (serve.Discovery, error) {
	const maxWait = 60 * time.Second
	backoff := 1 * time.Second
	deadline := time.Now().Add(maxWait)
	var lastErr error
	for {
		d, err := fetchDiscovery(ctx, base, repo)
		if err == nil {
			return d, nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return serve.Discovery{}, fmt.Errorf("after %s: %w", maxWait, lastErr)
		}
		obs.Logger.Info("control-plane: discovery not ready — retrying", "repo", repo, "error", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return serve.Discovery{}, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 8*time.Second {
			backoff *= 2
		}
	}
}
