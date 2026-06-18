// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"cvmfs.io/prepub/internal/distribute/serve"
	"cvmfs.io/prepub/pkg/observe"
)

// staticDiscovery is a minimal serve.DiscoverySource advertising a fixed
// control-plane reference for the repos this publisher serves (ADR-0001 D10).
// Unsigned in dev (nil Signer).
type staticDiscovery struct {
	repos     []string
	cp        serve.ControlPlaneRef
	enrollURL string       // HTTPS enroll base advertised to receivers ("" => none)
	signer    serve.Signer // nil => unsigned (dev)
}

func (d *staticDiscovery) Discovery(_ context.Context, repo string) (serve.Discovery, bool, error) {
	// The control-plane endpoint is identical for every repo this Stratum 0
	// serves, so answer for any requested repo (the receiver only needs
	// ControlPlane.URL). Repos echoes the configured list when known.
	repos := d.repos
	if len(repos) == 0 || (len(repos) == 1 && repos[0] == "") {
		repos = []string{repo}
	}
	doc := serve.Discovery{Repos: repos, ControlPlane: d.cp, EnrollURL: d.enrollURL}
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

// --- Asymmetric (Ed25519) discovery signing ------------------------------------
// The publisher signs the discovery document with an Ed25519 private key; each
// receiver verifies with only the matching public key. Unlike the HMAC variant,
// the verifier holds no secret that could mint tokens or forge documents, so the
// master secret never needs to reach a receiver.

// ed25519SignerFromFile loads a PEM (PKCS#8) Ed25519 private key and returns a
// serve.Signer producing base64 detached signatures.
func ed25519SignerFromFile(path string) (serve.Signer, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	blk, _ := pem.Decode(b)
	if blk == nil {
		return nil, fmt.Errorf("discovery signing key %s: no PEM block", path)
	}
	k, err := x509.ParsePKCS8PrivateKey(blk.Bytes)
	if err != nil {
		return nil, err
	}
	priv, ok := k.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("discovery signing key %s: not an Ed25519 private key", path)
	}
	return func(payload []byte) (string, error) {
		return base64.StdEncoding.EncodeToString(ed25519.Sign(priv, payload)), nil
	}, nil
}

// ed25519VerifierFromFile loads a PEM (PKIX) Ed25519 public key and returns a
// verify function matching ed25519SignerFromFile.
func ed25519VerifierFromFile(path string) (func(payload []byte, sig string) bool, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	blk, _ := pem.Decode(b)
	if blk == nil {
		return nil, fmt.Errorf("discovery verify key %s: no PEM block", path)
	}
	k, err := x509.ParsePKIXPublicKey(blk.Bytes)
	if err != nil {
		return nil, err
	}
	pub, ok := k.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("discovery verify key %s: not an Ed25519 public key", path)
	}
	return func(payload []byte, sig string) bool {
		raw, derr := base64.StdEncoding.DecodeString(sig)
		if derr != nil {
			return false
		}
		return ed25519.Verify(pub, payload, raw)
	}, nil
}
