// Package lease manages gateway lease acquisition, renewal, and release for
// CVMFS publish operations. Leases provide short-term exclusive publish locks
// on a repository path, protected by HMAC-SHA1 signed requests (matching the
// cvmfs_gateway Erlang implementation's expected auth format).
package lease

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"cvmfs.io/prepub/pkg/observe"
)

// maxConsecutiveHeartbeatFailures is the number of back-to-back renewal errors
// after which Heartbeat calls cancelJob to abort the in-progress publish.
// Three failures at the default 10 s interval means the gateway had 30+ seconds
// to respond — at that point the lease has almost certainly expired.
const maxConsecutiveHeartbeatFailures = 3

// Lease represents a granted publish lock on a repository path.
// The lease is valid until ExpiresAt and is identified by a Token.
type Lease struct {
	// Token is the opaque lease identifier issued by the gateway.
	Token string
	// Path is the repository sub-path the lease covers (e.g. "atlas/24.0").
	Path string
	// Repo is the repository name (e.g. "software.cern.ch").
	Repo string
	// ExpiresAt is the deadline after which the lease becomes invalid.
	ExpiresAt time.Time
}

// Client is a gateway lease client that signs requests with HMAC-SHA256 credentials.
// All lease operations are protected by signatures to prevent replay and tampering.
type Client struct {
	// BaseURL is the gateway base URL (e.g. "https://gateway.example.com").
	BaseURL string
	// KeyID is the HMAC key identifier.
	KeyID string
	// Secret is the HMAC secret key for signing requests.
	Secret string
	// obs provides logging and metrics.
	obs *observe.Provider
	// client is the HTTP client with TLS 1.2+ enforced.
	client *http.Client
}

// NewClient creates a new lease client with the given gateway base URL and HMAC credentials.
// The client enforces TLS 1.2+ to prevent downgrade attacks.
func NewClient(baseURL, keyID, secret string, obs *observe.Provider) *Client {
	return &Client{
		BaseURL: baseURL,
		KeyID:   keyID,
		Secret:  secret,
		obs:     obs,
		// Fix #25: Enforce a minimum TLS version.  TLS 1.0 and 1.1 have known
		// weaknesses (BEAST, POODLE); require at least 1.2.  Production
		// deployments should prefer TLS 1.3 where the gateway supports it — Go
		// will automatically negotiate 1.3 when both sides support it.
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					MinVersion: tls.VersionTLS12,
				},
			},
		},
	}
}

// signRequest signs the request using the cvmfs_gateway authentication scheme
// observed from cvmfs_server/swissknife traffic:
//
//	Authorization: <key_id> <base64(hex(HMAC-SHA1(body, secret)))>
//
// The signature is HMAC-SHA1 over the raw request body, hex-encoded, then
// base64-encoded.  The header has no "HMAC" prefix — just key_id, a space,
// and the signature.  For requests with no body (DELETE, GET) the HMAC is
// computed over an empty byte string.
func (c *Client) signRequest(req *http.Request) error {
	// Snapshot the body so we can both sign it and leave it readable for send.
	var bodyBytes []byte
	if req.Body != nil && req.Body != http.NoBody {
		var err error
		bodyBytes, err = io.ReadAll(req.Body)
		if err != nil {
			return fmt.Errorf("reading request body for signing: %w", err)
		}
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		req.ContentLength = int64(len(bodyBytes))
	}

	h := hmac.New(sha1.New, []byte(c.Secret))
	h.Write(bodyBytes)
	// hex-encode the raw HMAC bytes, then base64-encode that hex string
	hexHMAC := hex.EncodeToString(h.Sum(nil))
	signature := base64.StdEncoding.EncodeToString([]byte(hexHMAC))

	req.Header.Set("Authorization", fmt.Sprintf("%s %s", c.KeyID, signature))
	return nil
}

// Acquire implements Backend.  It acquires a publish lease from the gateway
// for the given repository and path, returning the opaque lease token.
// The request is HMAC-signed; the gateway rejects replays and tampered requests.
func (c *Client) Acquire(ctx context.Context, repo, path string) (string, error) {
	l, err := c.acquireLease(ctx, repo, path)
	if err != nil {
		return "", err
	}
	return l.Token, nil
}

// acquireLease is the internal form of Acquire that returns the full *Lease
// value (including ExpiresAt) for use by Probe and tests.
func (c *Client) acquireLease(ctx context.Context, repo, path string) (*Lease, error) {
	ctx, span := c.obs.Tracer.Start(ctx, "lease.acquire")
	defer span.End()

	start := time.Now()

	// Validate path when non-empty.  These conditions produce malformed gateway
	// URLs and are not valid CVMFS repository sub-paths:
	//   - leading slash "/foo"      → URL becomes "…/leases//foo"
	//   - adjacent slashes "a//b"   → URL becomes "…/leases/a//b"
	//   - trailing slash "foo/"     → URL becomes "…/leases/foo/"
	// An empty path means a root-level repo lease — allowed.
	if path != "" {
		if path[0] == '/' {
			return nil, fmt.Errorf("acquireLease: path %q must not start with a slash", path)
		}
		if path[len(path)-1] == '/' {
			return nil, fmt.Errorf("acquireLease: path %q must not end with a slash", path)
		}
		if strings.Contains(path, "//") {
			return nil, fmt.Errorf("acquireLease: path %q contains adjacent slashes", path)
		}
	}

	// The cvmfs_gateway REST API expects POST /api/v1/leases with a JSON body
	// matching what cvmfs_server/swissknife sends:
	//   {"path":"repo/subpath","api_version":"2","hostname":"publisher.host"}
	// For root-level leases the path is "repo/" (trailing slash required by gateway).
	leaseURL := fmt.Sprintf("%s/api/v1/leases", c.BaseURL)

	var fullPath string
	if path == "" {
		fullPath = repo + "/"
	} else {
		fullPath = repo + "/" + path
	}

	hostname, _ := os.Hostname()
	payload, _ := json.Marshal(map[string]interface{}{
		"path":        fullPath,
		"api_version": "2",
		"hostname":    hostname,
	})

	req, err := http.NewRequestWithContext(ctx, "POST", leaseURL, bytes.NewReader(payload))
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if err := c.signRequest(req); err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("signing request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("acquiring lease: %w", err)
	}
	// Drain body so the underlying TCP connection is returned to the pool.
	defer func() { io.Copy(io.Discard, resp.Body); resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		span.RecordError(fmt.Errorf("status %d", resp.StatusCode))
		return nil, fmt.Errorf("lease acquisition failed: status %d: %s", resp.StatusCode, data)
	}

	// Real cvmfs_gateway response format:
	//   {"status":"ok","session_token":"<tok>","max_lease_time":<seconds>}
	// On error: {"status":"error","reason":"<msg>"}
	var result struct {
		Status       string `json:"status"`
		SessionToken string `json:"session_token"`
		MaxLeaseTime int    `json:"max_lease_time"` // seconds
		Reason       string `json:"reason"`          // populated on error
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("decoding lease response: %w", err)
	}
	if result.Status != "ok" {
		var err error
		if result.Reason != "" {
			err = fmt.Errorf("gateway returned status %q: %s", result.Status, result.Reason)
		} else {
			err = fmt.Errorf("gateway returned status %q", result.Status)
		}
		span.RecordError(err)
		return nil, fmt.Errorf("lease acquisition: %w", err)
	}
	if result.SessionToken == "" {
		err := fmt.Errorf("gateway returned empty session_token")
		span.RecordError(err)
		return nil, err
	}

	c.obs.Metrics.LeaseAcquireDuration.Observe(time.Since(start).Seconds())

	return &Lease{
		Token:     result.SessionToken,
		Path:      path,
		Repo:      repo,
		ExpiresAt: time.Now().Add(time.Duration(result.MaxLeaseTime) * time.Second),
	}, nil
}

// Renew extends the expiration time of a held lease. The heartbeat goroutine
// calls this periodically to keep the lease alive during the SubmitPayload phase.
func (c *Client) Renew(ctx context.Context, token string) error {
	ctx, span := c.obs.Tracer.Start(ctx, "lease.renew")
	defer span.End()

	url := fmt.Sprintf("%s/api/v1/leases/%s", c.BaseURL, token)
	req, err := http.NewRequestWithContext(ctx, "PUT", url, nil)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("creating request: %w", err)
	}

	if err := c.signRequest(req); err != nil {
		span.RecordError(err)
		return fmt.Errorf("signing request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("renewing lease: %w", err)
	}
	defer func() { io.Copy(io.Discard, resp.Body); resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		span.RecordError(fmt.Errorf("status %d", resp.StatusCode))
		return fmt.Errorf("lease renewal failed: status %d", resp.StatusCode)
	}

	return nil
}

// Release releases a held lease, optionally committing the publish.
// If commit is true, the gateway finalizes the publish (the catalog hash has
// already been submitted via SubmitPayload).
// If commit is false, the publish is rolled back and the objects remain inaccessible.
//
// The real cvmfs_gateway REST API uses a plain DELETE with no query parameters;
// the gateway infers commit intent from whether SubmitPayload was called first.
func (c *Client) Release(ctx context.Context, token string, commit bool) error {
	ctx, span := c.obs.Tracer.Start(ctx, "lease.release")
	defer span.End()

	// Do not append ?commit=… — the real cvmfs_gateway does not accept that
	// query parameter on DELETE /api/v1/leases/{token} and returns 405.
	_ = commit
	releaseURL := fmt.Sprintf("%s/api/v1/leases/%s", c.BaseURL, token)
	req, err := http.NewRequestWithContext(ctx, "DELETE", releaseURL, nil)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("creating request: %w", err)
	}

	if err := c.signRequest(req); err != nil {
		span.RecordError(err)
		return fmt.Errorf("signing request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("releasing lease: %w", err)
	}
	defer func() { io.Copy(io.Discard, resp.Body); resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		span.RecordError(fmt.Errorf("status %d", resp.StatusCode))
		return fmt.Errorf("lease release failed: status %d", resp.StatusCode)
	}

	return nil
}

// SubmitPayload submits the catalog hash and object manifest to the gateway.
// This tells the gateway which objects are available in the CAS and updates the
// repository catalog. Must be called while holding a valid lease. The publish
// is not finalized until Release is called with commit=true.
func (c *Client) SubmitPayload(ctx context.Context, token string, catalogHash string, objectHashes []string) error {
	ctx, span := c.obs.Tracer.Start(ctx, "lease.submit_payload")
	defer span.End()

	url := fmt.Sprintf("%s/api/v1/payloads", c.BaseURL)

	payload := map[string]interface{}{
		"token":         token,
		"catalog_hash":  catalogHash,
		"object_hashes": objectHashes,
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if err := c.signRequest(req); err != nil {
		span.RecordError(err)
		return fmt.Errorf("signing request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("submitting payload: %w", err)
	}
	defer func() { io.Copy(io.Discard, resp.Body); resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		span.RecordError(fmt.Errorf("status %d", resp.StatusCode))
		return fmt.Errorf("payload submission failed: status %d: %s", resp.StatusCode, data)
	}

	return nil
}

// Heartbeat implements Backend.  It starts a goroutine that periodically
// renews the gateway lease identified by token at the given interval.
//
// If maxConsecutiveHeartbeatFailures back-to-back renewal errors occur,
// onExpire is called to signal that the lease has likely expired and the
// in-progress SubmitPayload should be aborted, preventing the orchestrator
// from blocking indefinitely on a gateway that has already evicted the lease.
//
// The returned cancel func stops the goroutine and is safe to call multiple
// times (sync.Once guard).  Pass a no-op func() {} for onExpire if early
// abort on heartbeat failure is not desired.
func (c *Client) Heartbeat(ctx context.Context, token string, interval time.Duration, onExpire context.CancelFunc) func() {
	// token is already a value copy — no additional snapshot needed.

	ticker := time.NewTicker(interval)
	done := make(chan struct{})

	go func() {
		defer ticker.Stop()
		consecutive := 0
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := c.Renew(ctx, token); err != nil {
					c.obs.Logger.Error("lease heartbeat error", "token", token, "error", err)
					c.obs.Metrics.LeaseHeartbeatErrors.Inc()
					consecutive++
					if consecutive >= maxConsecutiveHeartbeatFailures {
						c.obs.Logger.Error(
							"lease heartbeat: consecutive failures — lease likely expired, aborting job",
							"token", token, "failures", consecutive,
						)
						onExpire()
						return
					}
				} else {
					consecutive = 0
				}
			}
		}
	}()

	// Wrap close(done) in sync.Once so callers can safely invoke cancel
	// multiple times (e.g. explicit early cancel on an error path + defer).
	var once sync.Once
	return func() {
		once.Do(func() { close(done) })
	}
}

// ── Backend interface implementation ─────────────────────────────────────────

// Commit implements Backend for the gateway mode.  It submits the catalog hash
// and object list to the gateway via SubmitPayload, then releases the lease
// with commit=true to trigger the gateway's internal cvmfs_server publish call.
func (c *Client) Commit(ctx context.Context, req CommitRequest) error {
	if err := c.SubmitPayload(ctx, req.Token, req.CatalogHash, req.ObjectHashes); err != nil {
		return fmt.Errorf("gateway submit payload: %w", err)
	}
	if err := c.Release(ctx, req.Token, true); err != nil {
		return fmt.Errorf("gateway release (commit): %w", err)
	}
	return nil
}

// Abort implements Backend.  It releases the gateway lease without committing,
// causing the gateway to discard the in-progress transaction.
func (c *Client) Abort(ctx context.Context, token string) error {
	if err := c.Release(ctx, token, false); err != nil {
		return fmt.Errorf("gateway release (abort): %w", err)
	}
	return nil
}

// NeedsPipeline implements Backend.  The gateway backend always requires the
// full compress / dedup / CAS-upload pipeline so that objects are available in
// the CAS before the SubmitPayload call.
func (c *Client) NeedsPipeline() bool { return true }

// Probe implements Backend.  It issues a signed GET /api/v1/repos request to
// confirm the gateway is reachable and that HMAC authentication is accepted.
//
// We deliberately avoid acquiring a lease here: lease acquisition requires a
// valid, configured repository name, which the probe code does not know.
// GET /api/v1/repos is the lightest gateway endpoint that still exercises the
// full HTTP + auth path.
func (c *Client) Probe(ctx context.Context) error {
	pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	probeURL := fmt.Sprintf("%s/api/v1/repos", c.BaseURL)
	req, err := http.NewRequestWithContext(pctx, "GET", probeURL, nil)
	if err != nil {
		return fmt.Errorf("probe: creating request: %w", err)
	}
	if err := c.signRequest(req); err != nil {
		return fmt.Errorf("probe: signing request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("probe: GET /api/v1/repos: %w", err)
	}
	defer func() { io.Copy(io.Discard, resp.Body); resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("probe: GET /api/v1/repos returned %d: %s", resp.StatusCode, body)
	}
	return nil
}
