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

	"cvmfs.io/prepub/internal/api"
	"cvmfs.io/prepub/internal/broker"
	"cvmfs.io/prepub/internal/distribute/credential"
	"cvmfs.io/prepub/internal/httpsig"
	"cvmfs.io/prepub/pkg/observe"
)

// startControlTLS serves the security-sensitive control endpoints over HTTPS so
// the bearer token returned at enrollment never travels in plaintext:
//
//	GET  /control/challenge, POST /control/enroll  (rate-limited)
//	POST /control/revoke, POST /control/unrevoke   (admin: publisher-minted token)
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
	mux.Handle("/control/revoke", adminOnly(verifier, revokeCore(revoc, hook, srv, obs, false)))
	mux.Handle("/control/unrevoke", adminOnly(verifier, revokeCore(revoc, hook, srv, obs, true)))

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

// revokeRequest names exactly one node; unknown fields are refused.
type revokeRequest struct {
	Node string `json:"node"`
}

// adminOnly gates a TLS control endpoint (revoke, unrevoke) behind a
// publisher-minted bearer token, so only the operator (holder of
// PREPUB_HMAC_SECRET) can use it.
func adminOnly(verifier *credential.Verifier, core http.Handler) http.Handler {
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
		core.ServeHTTP(w, r)
	})
}

// revokeCore handles an already authorized revoke: it adds the node to the
// shared denylist (refusing future enroll/connect) and disconnects its live
// broker sessions. With undo it lifts the revocation instead; that is a
// separate route, so a publisher without it answers 404 rather than revoking.
// The TLS control endpoints and the API routes (behind the API auth) share it.
func revokeCore(revoc *revocation, hook *brokerAuthHook, srv *mqttbroker.Server,
	obs *observe.Provider, undo bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req revokeRequest
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil || req.Node == "" {
			http.Error(w, "bad request: {\"node\":\"...\"} required", http.StatusBadRequest)
			return
		}
		if err := broker.ValidateNodeID(req.Node); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		if req.Node == "publisher" {
			http.Error(w, "refusing to revoke the publisher", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if undo {
			if err := revoc.Unrevoke(req.Node); err != nil {
				obs.Logger.Error("control-plane: un-revoke not saved; node stays revoked", "node", req.Node, "error", err)
				http.Error(w, "still revoked: saving the denylist failed", http.StatusInternalServerError)
				return
			}
			obs.Logger.Info("control-plane: node un-revoked", "node", req.Node)
			_ = json.NewEncoder(w).Encode(map[string]any{"unrevoked": req.Node})
			return
		}
		perr := revoc.Revoke(req.Node)
		dropped := 0
		if hook != nil && srv != nil {
			for _, cid := range hook.clientsForNode(req.Node) {
				if cl, ok := srv.Clients.Get(cid); ok {
					_ = srv.DisconnectClient(cl, packets.ErrAdministrativeAction)
					dropped++
				}
			}
		}
		if perr != nil {
			obs.Logger.Error("control-plane: node revoked but the denylist could not be saved",
				"node", req.Node, "sessions_dropped", dropped, "error", perr)
			http.Error(w, "revoked until restart only: saving the denylist failed", http.StatusInternalServerError)
			return
		}
		obs.Logger.Info("control-plane: node revoked", "node", req.Node, "sessions_dropped", dropped)
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
// for the enroll endpoint and the revoke CLI, both served under the
// deployment's own CA.
func caHTTPClient(caPath string) (*http.Client, error) {
	return pemHTTPClient(caPath, x509.NewCertPool())
}

// pemHTTPClient returns a client trusting pool plus the CA in caPath. It
// clones the default transport, so HTTPS_PROXY/NO_PROXY and the default dial
// timeouts still apply.
func pemHTTPClient(caPath string, pool *x509.CertPool) (*http.Client, error) {
	pemBytes, err := os.ReadFile(caPath)
	if err != nil {
		return nil, err
	}
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("no PEM certificates in %s", caPath)
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	return &http.Client{Timeout: 15 * time.Second, Transport: tr}, nil
}

// runRevoke implements:
//
//	prepub revoke <node> [--enroll-url URL] [--ca-cert PEM]  (TLS control endpoint, PREPUB_HMAC_SECRET)
//	prepub revoke <node> --api-url URL [--ca-cert PEM]       (API route, PREPUB_API_TOKEN)
//
// --undo lifts the revocation through the matching unrevoke endpoint.
func runRevoke(args []string) {
	// Order-tolerant parse: <node> may appear before or after the flags (Go's
	// flag package would stop at the first positional and skip later flags).
	enrollURL := "https://localhost:8443"
	apiURL := ""
	caCert := ""
	node := ""
	undo := false
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
		case a == "--api-url" || a == "-api-url":
			i++
			if i < len(args) {
				apiURL = args[i]
			}
		case strings.HasPrefix(a, "--api-url="):
			apiURL = strings.TrimPrefix(a, "--api-url=")
		case a == "--ca-cert" || a == "-ca-cert":
			i++
			if i < len(args) {
				caCert = args[i]
			}
		case strings.HasPrefix(a, "--ca-cert="):
			caCert = strings.TrimPrefix(a, "--ca-cert=")
		case a == "--undo" || a == "-undo":
			undo = true
		default:
			if node == "" {
				node = a
			}
		}
	}
	if node == "" {
		fmt.Fprintln(os.Stderr, "usage: prepub revoke [--undo] <node> [--enroll-url https://host:8443 | --api-url http://host:8080] [--ca-cert ca.pem]")
		os.Exit(2)
	}
	req, err := buildRevokeRequest(node, undo, enrollURL, apiURL)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
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
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "revoke request failed:", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err := checkRevokeResponse(resp.StatusCode, out, node, undo); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	verb := "revoked"
	if undo {
		verb = "un-revoked"
	}
	fmt.Printf("%s %s: %s\n", verb, node, strings.TrimSpace(string(out)))
}

// checkRevokeResponse accepts only a 200 whose body confirms the action for
// node, so an older publisher without the unrevoke route (404) or any other
// answer is reported as a failure.
func checkRevokeResponse(status int, body []byte, node string, undo bool) error {
	action, key := "revoke", "revoked"
	if undo {
		action, key = "un-revoke", "unrevoked"
	}
	if status != http.StatusOK {
		msg := fmt.Sprintf("%s failed: status %d: %s", action, status, strings.TrimSpace(string(body)))
		if undo && status == http.StatusNotFound {
			msg += " (the publisher may predate un-revoke)"
		}
		return fmt.Errorf("%s", msg)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil || got[key] != node {
		return fmt.Errorf("%s not confirmed by the publisher: %s", action, strings.TrimSpace(string(body)))
	}
	return nil
}

// buildRevokeRequest builds the revoke (or, with undo, un-revoke) call. With apiURL it targets the API
// route, HMAC-signed with PREPUB_API_TOKEN (accepted under auth_mode both and
// hmac); otherwise the TLS control endpoint with a publisher token minted from
// PREPUB_HMAC_SECRET.
func buildRevokeRequest(node string, undo bool, enrollURL, apiURL string) (*http.Request, error) {
	body, _ := json.Marshal(revokeRequest{Node: node})
	apiPath, tlsPath := api.RevokePath, "/control/revoke"
	if undo {
		apiPath, tlsPath = api.UnrevokePath, "/control/unrevoke"
	}
	if apiURL != "" {
		tok := os.Getenv("PREPUB_API_TOKEN")
		if tok == "" {
			return nil, fmt.Errorf("PREPUB_API_TOKEN must be set to revoke via --api-url")
		}
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
			strings.TrimRight(apiURL, "/")+apiPath, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set(httpsig.HeaderName, httpsig.Sign([]byte(tok), api.SigningKeyID, http.MethodPost,
			req.URL.RequestURI(), httpsig.NoFields, httpsig.BodyDigest(body), time.Now(), randNonce()))
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	}
	secret := []byte(os.Getenv("PREPUB_HMAC_SECRET"))
	if len(secret) < 16 {
		return nil, fmt.Errorf("PREPUB_HMAC_SECRET (>= 16 bytes) must be set to mint the admin token")
	}
	tok, _, err := credential.NewMinter(secret).Mint("publisher", "control", randNonce(), time.Minute)
	if err != nil {
		return nil, fmt.Errorf("minting admin token: %w", err)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		strings.TrimRight(enrollURL, "/")+tlsPath, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}
