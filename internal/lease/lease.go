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
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
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

// computeSignature returns base64(hex(HMAC-SHA1(input, secret))), the
// Authorization token value expected by cvmfs_gateway for all endpoints.
func (c *Client) computeSignature(input []byte) string {
	h := hmac.New(sha1.New, []byte(c.Secret))
	h.Write(input)
	return base64.StdEncoding.EncodeToString([]byte(hex.EncodeToString(h.Sum(nil))))
}

// signWithToken signs the request using the session token as the HMAC input.
//
// Per gateway/frontend/authz.go, all lease and payload requests that include
// a token in the URL path use the token as the HMAC input:
//
//	DELETE /api/v1/leases/<token>      → HMAC over token  (cancel/abort)
//	POST   /api/v1/leases/<token>      → HMAC over token  (commit)
func (c *Client) signWithToken(req *http.Request, token string) {
	sig := c.computeSignature([]byte(token))
	req.Header.Set("Authorization", fmt.Sprintf("%s %s", c.KeyID, sig))
}

// signRequest signs the request using the raw request body as the HMAC input.
// Used for POST /api/v1/leases (new lease acquisition) where the body is
// the JSON payload.  Reads and rebuffers req.Body so it remains readable.
func (c *Client) signRequest(req *http.Request) error {
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
	sig := c.computeSignature(bodyBytes)
	req.Header.Set("Authorization", fmt.Sprintf("%s %s", c.KeyID, sig))
	return nil
}

// errPathBusy is a sentinel returned by acquireLease when the gateway responds
// with status "path_busy" (another publisher holds the lease).  It is
// intentionally unexported; Acquire handles it internally with retry logic.
var errPathBusy = fmt.Errorf("path_busy: another publisher holds the lease")

// leaseRetryConfig controls the backoff behaviour for path_busy retries.
const (
	// leaseRetryMax is the upper bound on total retry time.  The gateway's
	// default max_lease_time is 300 s, so 5 min is enough to outlast any
	// normally-behaving concurrent publisher.
	leaseRetryMax = 5 * time.Minute
	// leaseRetryBase is the initial delay between retries.
	leaseRetryBase = 5 * time.Second
	// leaseRetryMaxDelay caps the per-attempt sleep so we don't wait too long
	// between retries when there is a long contention window.
	leaseRetryMaxDelay = 30 * time.Second
)

// Acquire implements Backend.  It acquires a publish lease from the gateway
// for the given repository and path, returning the opaque lease token.
//
// When the gateway returns "path_busy" (another publisher currently holds the
// lease), Acquire retries with exponential backoff for up to leaseRetryMax.
// This handles both stale leases from previous failed jobs (which the gateway
// eventually expires) and legitimate concurrent publishers (who will release
// the lease when they finish).
func (c *Client) Acquire(ctx context.Context, repo, path string) (string, error) {
	deadline := time.Now().Add(leaseRetryMax)
	delay := leaseRetryBase
	attempt := 0

	for {
		l, err := c.acquireLease(ctx, repo, path)
		if err == nil {
			return l.Token, nil
		}

		if !errors.Is(err, errPathBusy) {
			// Hard error (auth failure, network error, etc.) — do not retry.
			return "", err
		}

		// path_busy: check whether we still have time left to retry.
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return "", fmt.Errorf("lease acquisition: path still busy after %s (%d attempts)", leaseRetryMax, attempt)
		}

		sleep := delay
		if sleep > remaining {
			sleep = remaining
		}
		attempt++
		c.obs.Logger.Info("lease path_busy — retrying",
			"attempt", attempt,
			"repo", repo,
			"path", path,
			"retry_in", sleep,
			"time_remaining", remaining.Round(time.Second),
		)

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(sleep):
		}

		// Exponential backoff, capped at leaseRetryMaxDelay.
		delay *= 2
		if delay > leaseRetryMaxDelay {
			delay = leaseRetryMaxDelay
		}
	}
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
	if result.Status == "path_busy" {
		// Another publisher holds the lease; signal the caller to retry.
		span.RecordError(errPathBusy)
		return nil, errPathBusy
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

	// PUT /api/v1/leases/<token> — HMAC over token string per authz.go.
	c.signWithToken(req, token)

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

	// DELETE /api/v1/leases/<token> — HMAC over token string per authz.go.
	c.signWithToken(req, token)

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

// buildMsgHeader constructs the JSON message header for the cvmfs_gateway
// binary payload protocol.  The gateway framing is:
//
//	POST /api/v1/payloads
//	Message-Size: N
//
//	<N bytes of JSON><compressed object bytes>
//
// where the JSON contains a "header_size" field that equals N — the byte
// length of the JSON itself.  The circular dependency is resolved by
// convergence: start with an estimate (digits=1) and increase until the
// formatted number has exactly `digits` characters.
func buildMsgHeader(token, hash string) []byte {
	// Build prefix (everything before the header_size value) and suffix.
	// header_size MUST be a JSON string (e.g. "164") not an integer (164).
	// The gateway Go struct has `HeaderSize string` and calls strconv.Atoi on
	// the value; sending an integer causes JSON decode failure in the gateway.
	// The outer quotes are part of prefix/suffix; digits counts only the numeral
	// characters inside them.
	prefix := `{"api_version":"2","session_token":` + strconv.Quote(token) +
		`,"payload_digest":` + strconv.Quote(hash) + `,"header_size":"`
	suffix := `"}`
	for digits := 1; digits <= 9; digits++ {
		n := len(prefix) + digits + len(suffix)
		s := strconv.Itoa(n)
		if len(s) == digits {
			return []byte(prefix + s + suffix)
		}
	}
	// Unreachable for any practical token/hash length.
	panic("buildMsgHeader: failed to converge on header_size")
}

// submitOneObject sends a single compressed CAS object to the gateway using
// the binary payload framing protocol.  objBytes is the raw compressed content
// as stored in the local CAS.
//
// We use the new-style /api/v1/payloads/<token> endpoint (rather than the
// legacy /api/v1/payloads) because:
//  1. The new endpoint signs with the session token (simpler, matches the
//     commit/cancel pattern).
//  2. Some gateway deployments only route the token-URL form to the object-
//     store backend correctly.
//
// The binary framing is identical for both endpoints:
//
//	Message-Size: N
//	<N bytes JSON header><compressed object bytes>
func (c *Client) submitOneObject(ctx context.Context, basePayloadURL, token, hash string, objBytes []byte) error {
	// Use /api/v1/payloads/<token> — HMAC over token string.
	payloadURL := basePayloadURL + "/" + token

	msgHeader := buildMsgHeader(token, hash)
	body := append(msgHeader, objBytes...)

	req, err := http.NewRequestWithContext(ctx, "POST", payloadURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("creating payload request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Message-Size", strconv.Itoa(len(msgHeader)))
	req.ContentLength = int64(len(body))

	// POST /api/v1/payloads/<token> — HMAC over token string per authz.go.
	c.signWithToken(req, token)

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("POST payload: %w", err)
	}
	defer func() { io.Copy(io.Discard, resp.Body); resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("payload upload failed (hash=%s): status %d: %s", hash, resp.StatusCode, data)
	}
	return nil
}

// SubmitPayload uploads all staged CAS objects (file chunks + catalog) to the
// gateway using the binary framing protocol required by cvmfs_gateway.
//
// Protocol per object:
//
//	POST /api/v1/payloads
//	Authorization: <signed>
//	Content-Type: application/octet-stream
//	Message-Size: N
//
//	<N bytes of JSON message header><zlib-compressed object bytes>
//
// Objects are uploaded in the order given (objectHashes first, then
// catalogHash last so the gateway sees the catalog only after all its
// referenced chunks are present).
//
// store must be non-nil; it is used to read the compressed bytes for each
// object from the local CAS before uploading.
func (c *Client) SubmitPayload(ctx context.Context, token, catalogHash string, objectHashes []string, store ObjectReader) error {
	ctx, span := c.obs.Tracer.Start(ctx, "lease.submit_payload")
	defer span.End()

	payloadURL := fmt.Sprintf("%s/api/v1/payloads", c.BaseURL)

	// Upload file-chunk objects first, catalog last.
	allHashes := append(append([]string(nil), objectHashes...), catalogHash)

	for _, hash := range allHashes {
		if hash == "" {
			continue
		}

		// Read compressed object bytes from the local CAS.
		rc, err := store.Get(ctx, hash)
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("reading object %s from CAS: %w", hash, err)
		}
		objBytes, readErr := io.ReadAll(rc)
		rc.Close()
		if readErr != nil {
			span.RecordError(readErr)
			return fmt.Errorf("reading object %s bytes: %w", hash, readErr)
		}

		if err := c.submitOneObject(ctx, payloadURL, token, hash, objBytes); err != nil {
			span.RecordError(err)
			return fmt.Errorf("gateway submit payload: %w", err)
		}
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

// Commit implements Backend for the gateway mode.  It uploads all staged CAS
// objects to the gateway via SubmitPayload, then finalises the publish by
// POSTing to /api/v1/leases/<token> — distinct from DELETE which cancels.
//
// Per gateway/frontend/leases.go:
//
//	POST /api/v1/leases/<token>  → handleCommitLease (finalise/publish)
//	DELETE /api/v1/leases/<token> → handleCancelLease (abort/rollback)
//
// The commit body carries the catalog root hashes and an optional snapshot tag:
//
//	{"old_root_hash":"","new_root_hash":"<catalogHash>","tag_name":"","tag_description":""}
//
// HMAC for POST /api/v1/leases/<token> is computed over the token string
// (same rule as DELETE), per gateway/frontend/authz.go.
func (c *Client) Commit(ctx context.Context, req CommitRequest) error {
	ctx, span := c.obs.Tracer.Start(ctx, "lease.commit")
	defer span.End()

	if req.ObjectStore == nil {
		return fmt.Errorf("Commit: ObjectStore must be set in gateway mode")
	}

	// 1. Upload all CAS objects (chunks + catalog) to the gateway.
	if err := c.SubmitPayload(ctx, req.Token, req.CatalogHash, req.ObjectHashes, req.ObjectStore); err != nil {
		span.RecordError(err)
		return fmt.Errorf("gateway submit payload: %w", err)
	}

	// 2. POST /api/v1/leases/<token> to finalise the publish transaction.
	commitURL := fmt.Sprintf("%s/api/v1/leases/%s", c.BaseURL, req.Token)
	commitBody, err := json.Marshal(map[string]interface{}{
		"old_root_hash":   "",
		"new_root_hash":   req.CatalogHash,
		"tag_name":        "",
		"tag_description": "",
	})
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("marshalling commit body: %w", err)
	}

	postReq, err := http.NewRequestWithContext(ctx, "POST", commitURL, bytes.NewReader(commitBody))
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("creating commit request: %w", err)
	}
	postReq.Header.Set("Content-Type", "application/json")
	postReq.ContentLength = int64(len(commitBody))
	// POST /api/v1/leases/<token> — HMAC over token string per authz.go.
	c.signWithToken(postReq, req.Token)

	resp, err := c.client.Do(postReq)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("committing lease: %w", err)
	}
	defer func() { io.Copy(io.Discard, resp.Body); resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		span.RecordError(fmt.Errorf("status %d", resp.StatusCode))
		return fmt.Errorf("gateway commit failed: status %d: %s", resp.StatusCode, data)
	}

	var result struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		// Non-fatal: commit likely succeeded if status was 200.
		c.obs.Logger.Info("lease commit: could not decode response body", "error", err)
		return nil
	}
	if result.Status != "ok" {
		err := fmt.Errorf("gateway commit status %q: %s", result.Status, result.Reason)
		span.RecordError(err)
		return err
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
