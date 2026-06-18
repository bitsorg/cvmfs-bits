// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	mqttbroker "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"cvmfs.io/prepub/internal/distribute/credential"
	"cvmfs.io/prepub/pkg/observe"
)

// startControlTLS serves the security-sensitive control endpoints over HTTPS so
// the bearer token returned at enrollment never travels in plaintext:
//
//	GET  /control/challenge, POST /control/enroll  (rate-limited)
//	POST /control/revoke                           (admin: publisher-minted token)
//
// It reuses the embedded broker's server certificate (tlsCfg) and returns a
// shutdown func. When this is active the plaintext API must NOT also mount the
// enroll routes (the caller nils them out), or the token would still leak.
func startControlTLS(addr string, tlsCfg *tls.Config, enroll *credential.EnrollServer,
	rateLimit func(http.Handler) http.Handler, verifier *credential.Verifier,
	revoc *revocation, hook *brokerAuthHook, srv *mqttbroker.Server,
	obs *observe.Provider) (func(), error) {

	if enroll == nil {
		return nil, fmt.Errorf("startControlTLS: enroll server is required")
	}
	mux := http.NewServeMux()
	var eh http.Handler = enroll.Handler()
	if rateLimit != nil {
		eh = rateLimit(eh)
	}
	mux.Handle("/control/challenge", eh)
	mux.Handle("/control/enroll", eh)
	mux.Handle("/control/revoke", revokeHandler(verifier, revoc, hook, srv, obs))

	httpSrv := &http.Server{
		Handler:           mux,
		TLSConfig:         tlsCfg,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	go func() {
		if err := httpSrv.ServeTLS(ln, "", ""); err != nil && err != http.ErrServerClosed {
			obs.Logger.Error("control-plane TLS listener error", "error", err)
		}
	}()
	obs.Logger.Info("control-plane: TLS enroll/revoke listener started", "addr", addr)
	return func() { _ = httpSrv.Close() }, nil
}

type revokeRequest struct {
	Node string `json:"node"`
}

// revokeHandler revokes a node's control-plane access: it adds the node to the
// shared denylist (refusing future enroll/connect) and actively disconnects any
// live broker sessions. Gated by a publisher-minted bearer token, so only the
// operator (holder of PREPUB_HMAC_SECRET) can revoke.
func revokeHandler(verifier *credential.Verifier, revoc *revocation, hook *brokerAuthHook,
	srv *mqttbroker.Server, obs *observe.Provider) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		claims, err := verifier.Verify(bearerToken(r), "")
		if err != nil || claims.Node != "publisher" {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		var req revokeRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil || req.Node == "" {
			http.Error(w, "bad request: {\"node\":\"...\"} required", http.StatusBadRequest)
			return
		}
		if req.Node == "publisher" {
			http.Error(w, "refusing to revoke the publisher", http.StatusBadRequest)
			return
		}
		revoc.Revoke(req.Node)
		dropped := 0
		if hook != nil && srv != nil {
			for _, cid := range hook.clientsForNode(req.Node) {
				if cl, ok := srv.Clients.Get(cid); ok {
					_ = srv.DisconnectClient(cl, packets.ErrAdministrativeAction)
					dropped++
				}
			}
		}
		obs.Logger.Info("control-plane: node revoked", "node", req.Node, "sessions_dropped", dropped)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"revoked": req.Node, "sessions_dropped": dropped})
	})
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if strings.HasPrefix(h, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
	}
	return ""
}

// caHTTPClient returns an *http.Client that trusts only the CA in caPath. Used
// by receivers (TLS enroll) and the revoke CLI to verify the control endpoint.
func caHTTPClient(caPath string) (*http.Client, error) {
	pemBytes, err := os.ReadFile(caPath)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("no PEM certificates in %s", caPath)
	}
	return &http.Client{
		Timeout:   15 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}},
	}, nil
}

// runRevoke implements:  prepub revoke <node> [--enroll-url URL] [--ca-cert PEM]
// It mints a short-lived publisher token from PREPUB_HMAC_SECRET and calls the
// publisher's TLS /control/revoke endpoint.
func runRevoke(args []string) {
	// Order-tolerant parse: <node> may appear before or after the flags (Go's
	// flag package would stop at the first positional and skip later flags).
	enrollURL := "https://localhost:8443"
	caCert := ""
	node := ""
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--enroll-url" || a == "-enroll-url":
			i++
			if i < len(args) {
				enrollURL = args[i]
			}
		case strings.HasPrefix(a, "--enroll-url="):
			enrollURL = strings.TrimPrefix(a, "--enroll-url=")
		case a == "--ca-cert" || a == "-ca-cert":
			i++
			if i < len(args) {
				caCert = args[i]
			}
		case strings.HasPrefix(a, "--ca-cert="):
			caCert = strings.TrimPrefix(a, "--ca-cert=")
		default:
			if node == "" {
				node = a
			}
		}
	}
	if node == "" {
		fmt.Fprintln(os.Stderr, "usage: prepub revoke <node> [--enroll-url https://host:8443] [--ca-cert ca.pem]")
		os.Exit(2)
	}
	secret := []byte(os.Getenv("PREPUB_HMAC_SECRET"))
	if len(secret) < 16 {
		fmt.Fprintln(os.Stderr, "PREPUB_HMAC_SECRET (>= 16 bytes) must be set to mint the admin token")
		os.Exit(1)
	}
	tok, _, err := credential.NewMinter(secret).Mint("publisher", "control", randNonce(), time.Minute)
	if err != nil {
		fmt.Fprintln(os.Stderr, "minting admin token:", err)
		os.Exit(1)
	}
	client := http.DefaultClient
	if caCert != "" {
		c, cerr := caHTTPClient(caCert)
		if cerr != nil {
			fmt.Fprintln(os.Stderr, "loading CA:", cerr)
			os.Exit(1)
		}
		client = c
	}
	body, _ := json.Marshal(revokeRequest{Node: node})
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost,
		strings.TrimRight(enrollURL, "/")+"/control/revoke", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "revoke request failed:", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "revoke failed: status %d: %s\n", resp.StatusCode, strings.TrimSpace(string(out)))
		os.Exit(1)
	}
	fmt.Printf("revoked %s: %s\n", node, strings.TrimSpace(string(out)))
}
