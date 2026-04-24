// Package lease manages gateway lease acquisition, renewal, and release for
// CVMFS publish operations. Leases provide short-term exclusive publish locks
// on a repository path, protected by HMAC-SHA256 signed requests.
package lease

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"cvmfs.io/prepub/pkg/observe"
)

// signatureWindow is the maximum age of a signed request that the gateway will
// accept. Requests older than this are rejected as potential replays.
// (The gateway must enforce the same window on its side.)
const signatureWindow = 60 * time.Second

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

// signRequest signs the request with HMAC-SHA256 over:
//
//	method + " " + requestURI + " " + bodyHash + " " + unixTimestamp
//
// This covers:
//   - method:      prevents cross-verb replay
//   - requestURI:  path + query string — prevents cross-endpoint replay AND
//     parameter tampering (e.g. flipping ?commit=true→false). Fix #13.
//   - bodyHash:    prevents body substitution attacks
//   - timestamp:   prevents replay attacks beyond signatureWindow
func (c *Client) signRequest(req *http.Request) error {
	ts := strconv.FormatInt(time.Now().Unix(), 10)

	// Snapshot the body so we can both hash it and leave it readable for the
	// actual HTTP send.
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

	bodyHash := sha256.Sum256(bodyBytes) // sha256("") for requests with no body
	bodyHashHex := hex.EncodeToString(bodyHash[:])

	// RequestURI() returns path + query string (e.g. /api/v1/leases/tok?commit=true)
	// so query parameters are covered by the signature.
	//
	// NOTE: The Host is intentionally omitted from the signed message.  Adding
	// it would prevent cross-host replay but would require a matching change to
	// the gateway's signature verifier.  Track in the gateway backlog before
	// enabling host binding here.
	message := req.Method + " " + req.URL.RequestURI() + " " + bodyHashHex + " " + ts

	h := hmac.New(sha256.New, []byte(c.Secret))
	h.Write([]byte(message))
	signature := hex.EncodeToString(h.Sum(nil))

	// Include timestamp in the header so the gateway can verify the window.
	req.Header.Set("Authorization", fmt.Sprintf("hmac-sha256 %s:%s", c.KeyID, signature))
	req.Header.Set("X-Prepub-Timestamp", ts)
	return nil
}

// Acquire acquires a publish lease from the gateway for the given repository and path.
// The lease is valid until the returned Lease.ExpiresAt time and is protected by
// an HMAC-signed request. Returns an error if the gateway rejects the request.
func (c *Client) Acquire(ctx context.Context, repo, path string) (*Lease, error) {
	ctx, span := c.obs.Tracer.Start(ctx, "lease.acquire")
	defer span.End()

	start := time.Now()

	url := fmt.Sprintf("%s/api/v1/leases/%s", c.BaseURL, path)
	body := map[string]string{
		"repo": repo,
		"path": path,
	}
	payload, _ := json.Marshal(body)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payload))
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

	var result struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("decoding lease response: %w", err)
	}

	c.obs.Metrics.LeaseAcquireDuration.Observe(time.Since(start).Seconds())

	return &Lease{
		Token:     result.Token,
		Path:      path,
		Repo:      repo,
		ExpiresAt: result.ExpiresAt,
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
// If commit is true, the gateway finalizes the publish operation.
// If commit is false, the publish is rolled back and the objects remain inaccessible.
func (c *Client) Release(ctx context.Context, token string, commit bool) error {
	ctx, span := c.obs.Tracer.Start(ctx, "lease.release")
	defer span.End()

	url := fmt.Sprintf("%s/api/v1/leases/%s?commit=%v", c.BaseURL, token, commit)
	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
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

// Heartbeat starts a goroutine that periodically renews the lease at the given interval.
// The returned cancel function stops the heartbeat and must be called when the lease
// is no longer needed to prevent a goroutine leak.
//
// If maxConsecutiveHeartbeatFailures back-to-back renewal errors occur, the heartbeat
// calls cancelJob to signal that the lease has likely expired on the gateway and the
// in-progress SubmitPayload should be aborted. This prevents the orchestrator from
// blocking indefinitely on a gateway that has already evicted the lease.
// Pass a no-op func() {} if early abort on heartbeat failure is not desired.
func (c *Client) Heartbeat(ctx context.Context, lease *Lease, interval time.Duration, cancelJob context.CancelFunc) func() {
	// Capture the token value once before spawning the goroutine.  The goroutine
	// must not read lease.Token directly: the caller may zero it on the error
	// path (e.g. abortJob), which would be an unsynchronised read otherwise.
	token := lease.Token

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
						cancelJob()
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
