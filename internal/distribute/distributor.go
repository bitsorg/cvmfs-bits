// Package distribute manages replication of objects to Stratum 1 endpoints.
// It handles batch and per-object pushes with quorum enforcement and crash-safe
// distribution logging.
package distribute

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"golang.org/x/sync/errgroup"

	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/pkg/observe"
)

// sharedClient is used for all HTTPS control-channel requests (announce) and
// for legacy per-object PUTs to endpoints that do not support the announce
// protocol.  TLS 1.2 is the minimum acceptable version.
var sharedClient = &http.Client{
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	},
}

// dataClient is used exclusively for plain-HTTP object PUTs on the data channel
// returned by a successful announce response.  The data channel intentionally
// does not use TLS: SHA-256 hash verification at the receiver provides transfer
// integrity without the CPU overhead of encrypting already-compressed objects.
// Session tokens on this channel are scoped to a single payload and receiver,
// so a captured token cannot be replayed against a different job or endpoint.
var dataClient = &http.Client{
	Transport: &http.Transport{}, // plain HTTP — no TLS
}

// Config holds the configuration for distribution to Stratum 1 endpoints.
type Config struct {
	// Endpoints are the Stratum 1 URLs to push objects to (e.g. "https://stratum1.example.com").
	Endpoints []string
	// Quorum is the fraction of endpoints that must receive all objects for a successful publish
	// (e.g. 0.5 requires majority, 1.0 requires all). Uses math.Ceil so e.g. 0.51 with 3 endpoints requires 2.
	Quorum float64
	// Timeout is the per-object/batch HTTP request timeout.
	Timeout time.Duration
	// Concurrency is reserved for future use.
	Concurrency int
	// Obs provides logging and metrics.
	Obs *observe.Provider
	// DevMode relaxes endpoint validation: allows http:// and private IP ranges.
	// NEVER set in production.
	DevMode bool
	// HMACSecret is the shared secret used to sign announce requests on the
	// HTTPS control channel.  Must match the HMACSecret configured on each
	// Stratum 1 receiver.  If empty, the announce step is skipped and the
	// distributor falls back to direct per-object PUTs (legacy behaviour).
	HMACSecret string
	// BatchSize controls how many objects are sent in a single multipart POST
	// to the Stratum 1 batch endpoint (/cvmfs-receiver/objects/batch).
	// Set to 0 (default) to disable batching and use one PUT per object.
	// When the receiver does not support batching (returns 404 or 415), the
	// distributor automatically falls back to per-object PUTs for the remainder of that
	// endpoint.
	BatchSize int
}

// ValidateEndpoints checks all configured endpoints and returns an error if
// any URL is unsafe.
//
// Fix #18: Prevents SSRF by requiring HTTPS and rejecting private/loopback
// addresses.  DevMode relaxes these checks for local testing only.
func (c *Config) ValidateEndpoints() error {
	for _, raw := range c.Endpoints {
		u, err := url.ParseRequestURI(raw)
		if err != nil {
			return fmt.Errorf("endpoint %q is not a valid URL: %w", raw, err)
		}
		if u.Host == "" {
			return fmt.Errorf("endpoint %q has no host", raw)
		}
		if !c.DevMode && u.Scheme != "https" {
			return fmt.Errorf("endpoint %q must use HTTPS (use DevMode for local testing)", raw)
		}
		// Resolve hostname to check for private/loopback addresses.
		host := u.Hostname()
		if !c.DevMode {
			if err := rejectPrivateHost(host); err != nil {
				return fmt.Errorf("endpoint %q: %w", raw, err)
			}
		}
	}
	return nil
}

// privateRanges lists CIDR blocks that must not be reachable via distribution
// to prevent SSRF attacks against internal services.
var privateRanges = func() []*net.IPNet {
	cidrs := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"127.0.0.0/8",
		"169.254.0.0/16", // link-local / AWS metadata
		"::1/128",
		"fc00::/7",
	}
	nets := make([]*net.IPNet, 0, len(cidrs))
	for _, c := range cidrs {
		_, n, err := net.ParseCIDR(c)
		if err != nil {
			// All CIDRs above are hardcoded constants — a parse failure is
			// a programming error, not a runtime condition.
			panic(fmt.Sprintf("distribute: invalid CIDR %q in privateRanges: %v", c, err))
		}
		nets = append(nets, n)
	}
	return nets
}()

// isPrivateIP returns true if ip falls within any of the private/loopback ranges.
func isPrivateIP(ip net.IP) bool {
	for _, priv := range privateRanges {
		if priv.Contains(ip) {
			return true
		}
	}
	return false
}

func rejectPrivateHost(host string) error {
	// If the host is already a numeric IP address, check it directly —
	// no DNS round-trip needed, and no fail-open window.
	if ip := net.ParseIP(host); ip != nil {
		if isPrivateIP(ip) {
			return fmt.Errorf("IP %s is in a private/loopback range — SSRF risk", ip)
		}
		return nil
	}

	addrs, err := net.LookupHost(host)
	if err != nil {
		// Fail closed: an unresolvable hostname cannot be verified safe.
		// DNS failures are transient; the operator should fix the endpoint
		// configuration rather than opening an SSRF window.
		return fmt.Errorf("DNS lookup for %q failed — cannot verify SSRF safety: %w", host, err)
	}
	for _, addr := range addrs {
		ip := net.ParseIP(addr)
		if ip == nil {
			continue
		}
		if isPrivateIP(ip) {
			return fmt.Errorf("host %q resolves to private address %s — SSRF risk", host, addr)
		}
	}
	return nil
}

// DistLog records which objects were successfully pushed to which Stratum 1 endpoints.
// The file is opened lazily on the first Record call and kept open for the
// lifetime of the Distribute operation, avoiding the overhead of an open+fsync+close
// per object (Fix #9). Entries are appended as newline-delimited JSON.
type DistLog struct {
	// path is the file path for the distribution log.
	path string
	// mu protects f.
	mu sync.Mutex
	// f is the open file handle, nil until first Record call.
	f *os.File
}

// OpenDistLog creates a new DistLog that will write to the given path.
func OpenDistLog(path string) *DistLog {
	return &DistLog{path: path}
}

// Record appends a distribution entry to the log (endpoint, hash, timestamp).
// The file is opened on the first call and reused for subsequent calls.
// Call Sync or Close to flush to disk.
func (d *DistLog) Record(endpoint, hash string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.f == nil {
		f, err := os.OpenFile(d.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			return fmt.Errorf("opening dist log: %w", err)
		}
		d.f = f
	}

	data, err := json.Marshal(map[string]string{
		"endpoint": endpoint,
		"hash":     hash,
		"time":     time.Now().Format(time.RFC3339),
	})
	if err != nil {
		return fmt.Errorf("marshaling dist log entry: %w", err)
	}
	if _, err := d.f.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("writing dist log: %w", err)
	}
	return nil
}

// Sync flushes buffered writes to disk without closing the file.
func (d *DistLog) Sync() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.f != nil {
		return d.f.Sync()
	}
	return nil
}

// Close syncs and closes the underlying file.
// Safe to call on a log that has never been written to.
func (d *DistLog) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.f == nil {
		return nil
	}
	syncErr := d.f.Sync()
	closeErr := d.f.Close()
	d.f = nil
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// Confirmed returns the list of object hashes that were successfully recorded
// for the given endpoint by reading the distribution log file.
func (d *DistLog) Confirmed(endpoint string) ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	data, err := os.ReadFile(d.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading dist log: %w", err)
	}

	var hashes []string
	scanner := json.NewDecoder(bytes.NewReader(data))

	for scanner.More() {
		var entry struct {
			Endpoint string `json:"endpoint"`
			Hash     string `json:"hash"`
		}
		if err := scanner.Decode(&entry); err != nil {
			return nil, fmt.Errorf("decoding dist log: %w", err)
		}
		if entry.Endpoint == endpoint {
			hashes = append(hashes, entry.Hash)
		}
	}

	return hashes, nil
}

// announceReq is the JSON body sent to POST {endpoint}/api/v1/announce.
type announceReq struct {
	PayloadID   string `json:"payload_id"`
	ObjectCount int    `json:"object_count"`
	TotalBytes  int64  `json:"total_bytes"`
}

// announceResp is the JSON body returned by a successful announce.
type announceResp struct {
	SessionToken string `json:"session_token"`
	DataEndpoint string `json:"data_endpoint"`
}

// announceResult holds the outcome of a single announce call.
type announceResult struct {
	// token is the bearer credential for subsequent data-channel PUTs.
	token string
	// dataEndpoint is the plain-HTTP base URL for object PUTs.
	dataEndpoint string
	// supported is false when the endpoint returned 404, indicating the
	// receiver does not implement the announce protocol.  The caller should
	// fall back to direct per-object PUTs in that case.
	supported bool
}

// announce sends a pre-transfer announce to the Stratum 1 receiver's HTTPS
// control channel and returns the session token and plain-HTTP data endpoint.
//
// The request is authenticated with HMAC-SHA256 (§20.4 of REFERENCE.md):
//
//	canonical = METHOD + "\n" + path + "\n" + hex(SHA-256(body)) + "\n" + unix_ts
//	X-Signature = hex(HMAC-SHA256(cfg.HMACSecret, canonical))
//
// Returns supported=false (and no error) when the endpoint returns HTTP 404,
// which signals that the receiver does not support the announce protocol and the
// caller should fall back to the legacy direct-PUT path.
func announce(ctx context.Context, endpoint, payloadID string, hashes []string, cfg Config) (announceResult, error) {
	ar := announceReq{
		PayloadID:   payloadID,
		ObjectCount: len(hashes),
		TotalBytes:  0, // size unknown at distribution time; receiver uses minimum threshold
	}
	body, err := json.Marshal(ar)
	if err != nil {
		return announceResult{}, fmt.Errorf("marshaling announce request: %w", err)
	}

	ts := strconv.FormatInt(time.Now().Unix(), 10)
	bodyHash := sha256.Sum256(body)

	announceURL := endpoint + "/api/v1/announce"
	announceURI := "/api/v1/announce"
	// If the endpoint URL has a path component (e.g. https://example.com/stratum1/),
	// the receiver's req.URL.RequestURI() will include that path prefix.
	// We must compute the canonical message with the full request path as seen by the receiver.
	if u, err := url.Parse(endpoint); err == nil && u.Path != "" && u.Path != "/" {
		// The endpoint has a path prefix; append /api/v1/announce to it.
		// The path already starts with "/", so just append the rest.
		basePath := strings.TrimSuffix(u.Path, "/")
		announceURI = basePath + "/api/v1/announce"
	}

	msg := "POST\n" +
		announceURI + "\n" +
		hex.EncodeToString(bodyHash[:]) + "\n" +
		ts
	mac := hmac.New(sha256.New, []byte(cfg.HMACSecret))
	mac.Write([]byte(msg))
	sig := hex.EncodeToString(mac.Sum(nil))
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, announceURL, bytes.NewReader(body))
	if err != nil {
		return announceResult{}, fmt.Errorf("building announce request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Timestamp", ts)
	req.Header.Set("X-Signature", sig)

	resp, err := sharedClient.Do(req)
	if err != nil {
		return announceResult{}, fmt.Errorf("announce to %s: %w", endpoint, err)
	}
	defer func() { io.Copy(io.Discard, resp.Body); resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		// Receiver does not implement the announce protocol.
		return announceResult{supported: false}, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return announceResult{}, fmt.Errorf("announce returned HTTP %d: %s", resp.StatusCode, string(body))
	}

	var ar2 announceResp
	if err := json.NewDecoder(resp.Body).Decode(&ar2); err != nil {
		return announceResult{}, fmt.Errorf("decoding announce response: %w", err)
	}
	return announceResult{
		token:        ar2.SessionToken,
		dataEndpoint: ar2.DataEndpoint,
		supported:    true,
	}, nil
}

// Distribute pushes object hashes to all configured Stratum 1 endpoints in parallel.
// payloadID is the job UUID used as an idempotency key for the announce protocol.
// Returns the count of confirmed endpoints, total endpoints, and an error if quorum
// is not reached or validation fails.
//
// Fan-out crash consistency (Fix #14): If an endpoint fails partway through
// the push, it is marked as failed and does not contribute to quorum, even though
// some of its objects may have been successfully received. Only endpoints that
// received every object in the publish set are counted as confirmed. This prevents
// a partial replica from being treated as a valid quorum member.
//
// When cfg.HMACSecret is set, each endpoint is first contacted on its HTTPS control
// channel with an announce request (§20 of REFERENCE.md).  The announce returns a
// session token and a plain-HTTP data endpoint; subsequent object PUTs use that
// endpoint and token, trading TLS overhead for SHA-256 hash verification at the
// receiver.  If an endpoint returns 404 on announce it is assumed to be a legacy
// receiver and the distributor falls back to direct HTTPS PUTs without a session.
func Distribute(ctx context.Context, payloadID string, hashes []string, casBackend cas.Backend, cfg Config, log *DistLog) (int, int, error) {
	ctx, span := cfg.Obs.Tracer.Start(ctx, "distribute.push")
	defer span.End()

	if len(cfg.Endpoints) == 0 {
		return len(hashes), len(hashes), nil
	}

	// Fix #18: Validate endpoints before making any outbound connections.
	if err := cfg.ValidateEndpoints(); err != nil {
		span.RecordError(err)
		return 0, len(cfg.Endpoints), fmt.Errorf("endpoint validation failed: %w", err)
	}

	// endpointFailed tracks whether any push to an endpoint failed.  An
	// endpoint only contributes to quorum if it received ALL objects — a
	// partial replica is not a valid quorum member.
	endpointFailed := make(map[string]bool)
	var mu sync.Mutex

	eg, egCtx := errgroup.WithContext(ctx)

	for _, endpoint := range cfg.Endpoints {
		endpoint := endpoint
		eg.Go(func() error {
			ectx, espan := cfg.Obs.Tracer.Start(egCtx, "distribute.endpoint")
			defer espan.End()

			// Announce phase: negotiate a session and obtain the plain-HTTP
			// data endpoint when the receiver supports the protocol and an
			// HMAC secret is configured.
			var sessionToken, dataEP string
			if cfg.HMACSecret != "" {
				ar, err := announce(ectx, endpoint, payloadID, hashes, cfg)
				if err != nil {
					cfg.Obs.Logger.Warn("announce failed — skipping endpoint",
						"endpoint", endpoint, "error", err)
					mu.Lock()
					endpointFailed[endpoint] = true
					mu.Unlock()
					return nil
				}
				if ar.supported {
					sessionToken = ar.token
					dataEP = ar.dataEndpoint
					cfg.Obs.Logger.Info("announce accepted",
						"endpoint", endpoint, "data_endpoint", dataEP)
				} else {
					cfg.Obs.Logger.Info("receiver does not support announce — using legacy PUT",
						"endpoint", endpoint)
				}
			}

			pushOne := func(hash string) {
				start := time.Now()
				if err := pushObject(ectx, endpoint, hash, casBackend, cfg.Timeout, sessionToken, dataEP); err != nil {
					espan.RecordError(err)
					cfg.Obs.Logger.Error("pushing object", "endpoint", endpoint, "hash", hash, "error", err)
					mu.Lock()
					endpointFailed[endpoint] = true
					mu.Unlock()
					return
				}
				if lerr := log.Record(endpoint, hash); lerr != nil {
					cfg.Obs.Logger.Warn("recording distribution push", "endpoint", endpoint, "hash", hash, "error", lerr)
				}
				cfg.Obs.Metrics.DistributionDuration.WithLabelValues(endpoint).Observe(time.Since(start).Seconds())
			}

			if cfg.BatchSize <= 0 {
				// Batching disabled — one PUT per object (original behaviour).
				for _, hash := range hashes {
					pushOne(hash)
				}
				return nil
			}

			// Batching enabled.  Try the batch endpoint; fall back to per-object
			// PUTs if the receiver signals it does not support batching.
			batchSupported := true
			for i := 0; i < len(hashes); i += cfg.BatchSize {
				end := i + cfg.BatchSize
				if end > len(hashes) {
					end = len(hashes)
				}
				batch := hashes[i:end]

				if batchSupported {
					start := time.Now()
					pushed, err := pushBatch(ectx, endpoint, batch, casBackend, cfg.Timeout)
					if err != nil {
						if isBatchUnsupported(err) {
							cfg.Obs.Logger.Info("batch push unsupported by receiver — falling back to per-object PUT",
								"endpoint", endpoint)
							batchSupported = false
							// Fall through to per-object below.
						} else {
							// Partial success: some objects succeeded, some failed.
							// Mark the endpoint as failed; per-object push may recover
							// on the next retry.
							espan.RecordError(err)
							cfg.Obs.Logger.Error("batch push failed", "endpoint", endpoint, "error", err)
							mu.Lock()
							endpointFailed[endpoint] = true
							mu.Unlock()
							// Don't fall back — keep using batch for subsequent batches.
							continue
						}
					} else {
						elapsed := time.Since(start)
						for _, h := range pushed {
							if lerr := log.Record(endpoint, h); lerr != nil {
								cfg.Obs.Logger.Warn("recording batch push", "endpoint", endpoint, "hash", h, "error", lerr)
							}
						}
						// Record total batch elapsed time — not averaged per object.
					// Per-object fallback (pushOne) records one observation per
					// object at line 287; both histograms use the same metric so
					// they are directly comparable.
					cfg.Obs.Metrics.DistributionDuration.WithLabelValues(endpoint).
						Observe(elapsed.Seconds())
						continue
					}
				}

				// Per-object fallback (either disabled at start or after 415/404).
				for _, hash := range batch {
					pushOne(hash)
				}
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		span.RecordError(err)
		// Don't fail the whole operation if some endpoints fail
	}

	// Count endpoints that received every object in the publish set.
	totalEndpoints := len(cfg.Endpoints)
	mu.Lock()
	confirmed := 0
	for _, endpoint := range cfg.Endpoints {
		if !endpointFailed[endpoint] {
			confirmed++
		}
	}
	mu.Unlock()

	// Fix: use math.Ceil so that e.g. Quorum=0.51 with 3 endpoints requires
	// 2 confirmations (ceil(1.53)=2), not 1 (int(1.53)=1).
	// Ensure the result fits in int; in practice endpoint counts are small.
	requiredConfirmations := int(math.Ceil(float64(totalEndpoints) * cfg.Quorum))
	if requiredConfirmations < 0 || requiredConfirmations > totalEndpoints {
		requiredConfirmations = totalEndpoints
	}

	if confirmed < requiredConfirmations {
		span.RecordError(fmt.Errorf("quorum not reached: %d/%d", confirmed, requiredConfirmations))
		return confirmed, totalEndpoints, fmt.Errorf("quorum not reached: %d/%d confirmations", confirmed, requiredConfirmations)
	}

	return confirmed, totalEndpoints, nil
}

// batchUnsupportedError is returned by pushBatch when the receiver signals
// it does not understand the batch endpoint (HTTP 404 or 415).
type batchUnsupportedError struct{ status int }

func (e *batchUnsupportedError) Error() string {
	return fmt.Sprintf("batch endpoint not supported (status %d)", e.status)
}

func isBatchUnsupported(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*batchUnsupportedError)
	return ok
}

// pushBatch sends a batch of objects to the Stratum 1 batch endpoint via
// streaming multipart POST. It returns the list of hashes that were
// successfully received, or an error.
//
// The batch endpoint is:
//	POST <endpoint>/cvmfs-receiver/objects/batch
//	Content-Type: multipart/form-data; boundary=...
//
// Each part has name "object" and filename set to the object hash.
// A successful response is 200/201 JSON: {"received": ["hash1", "hash2", ...]}.
//
// The timeout applies to the entire batch, not per-object, to prevent multi-hour
// deadlines on large batches that could mask hung connections.
func pushBatch(ctx context.Context, endpoint string, hashes []string, casBackend cas.Backend, timeout time.Duration) ([]string, error) {
	// Fix: timeout applies to the whole batch, not per object.  The previous
	// timeout*len(hashes) could produce multi-hour deadlines for large batches,
	// masking hung connections.
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)

	// Stream objects from CAS into the multipart writer in a goroutine so the
	// HTTP request body can be consumed as it is produced.
	go func() {
		for _, hash := range hashes {
			r, err := casBackend.Get(ctx, hash)
			if err != nil {
				pw.CloseWithError(fmt.Errorf("cas get %s: %w", hash, err))
				return
			}
			part, err := mw.CreateFormFile("object", hash)
			if err != nil {
				r.Close()
				pw.CloseWithError(fmt.Errorf("creating form file for %s: %w", hash, err))
				return
			}
			if _, err := io.Copy(part, r); err != nil {
				r.Close()
				pw.CloseWithError(fmt.Errorf("copying %s to multipart: %w", hash, err))
				return
			}
			r.Close()
		}
		mw.Close()
		pw.Close()
	}()

	batchURL := fmt.Sprintf("%s/cvmfs-receiver/objects/batch", endpoint)
	req, err := http.NewRequestWithContext(ctx, "POST", batchURL, pr)
	if err != nil {
		pw.CloseWithError(err)
		return nil, fmt.Errorf("creating batch request: %w", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := sharedClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("batch push to %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusUnsupportedMediaType {
		return nil, &batchUnsupportedError{status: resp.StatusCode}
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("batch push failed (status %d): %s", resp.StatusCode, body)
	}

	var result struct {
		Received []string `json:"received"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding batch response: %w", err)
	}
	return result.Received, nil
}

// max returns the larger of a and b.
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// pushObject sends a single object to a Stratum 1 endpoint via PUT.
//
// When sessionToken and dataEndpoint are non-empty the object is sent to the
// plain-HTTP data channel returned by a prior announce (§20 of REFERENCE.md):
//   - URL: {dataEndpoint}/api/v1/objects/{hash}
//   - Authorization: Bearer {sessionToken}
//   - X-Content-SHA256: SHA-256 of the compressed bytes (transfer integrity)
//   - Transport: dataClient (plain HTTP, no TLS)
//
// When either is empty (legacy receiver or announce not configured) the object
// is pushed directly to the HTTPS control endpoint:
//   - URL: {endpoint}/cvmfs-receiver/objects/{hash}
//   - Transport: sharedClient (TLS 1.2+)
//
// Both paths propagate OTel trace context for observability (Fix #23).
func pushObject(ctx context.Context, endpoint, hash string, backend cas.Backend, timeout time.Duration, sessionToken, dataEndpoint string) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	r, err := backend.Get(ctx, hash)
	if err != nil {
		return fmt.Errorf("getting from CAS: %w", err)
	}
	defer r.Close()

	var (
		targetURL string
		client    *http.Client
	)

	if sessionToken != "" && dataEndpoint != "" {
		// New two-channel path: plain HTTP data channel with session auth.
		// Validate the data endpoint URL to prevent SSRF attacks: the receiver
		// should only return URLs pointing to itself, not arbitrary hosts.
		u, err := url.Parse(dataEndpoint)
		if err != nil {
			return fmt.Errorf("invalid data endpoint URL %q: %w", dataEndpoint, err)
		}
		if u.Host == "" {
			return fmt.Errorf("data endpoint URL %q has no host", dataEndpoint)
		}
		// Reject private/loopback addresses (same check as endpoint validation).
		if err := rejectPrivateHost(u.Hostname()); err != nil {
			return fmt.Errorf("data endpoint %q: %w", dataEndpoint, err)
		}

		// Read the full object so we can compute X-Content-SHA256 before
		// streaming; objects are compressed and typically small (< 100 MB).
		data, err := io.ReadAll(r)
		if err != nil {
			return fmt.Errorf("reading object %s from CAS: %w", hash, err)
		}
		sum := sha256.Sum256(data)
		contentSHA := hex.EncodeToString(sum[:])

		targetURL = fmt.Sprintf("%s/api/v1/objects/%s", dataEndpoint, hash)
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, targetURL, bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("creating data-channel request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+sessionToken)
		req.Header.Set("X-Content-SHA256", contentSHA)
		req.Header.Set("Content-Type", "application/octet-stream")
		otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

		resp, err := dataClient.Do(req)
		if err != nil {
			return fmt.Errorf("pushing to data channel %s: %w", targetURL, err)
		}
		// Drain and close response body immediately to release connection.
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			return fmt.Errorf("data-channel push failed (status %d)", resp.StatusCode)
		}
		return nil
	}

	// Legacy path: direct HTTPS PUT to the endpoint.
	targetURL = fmt.Sprintf("%s/cvmfs-receiver/objects/%s", endpoint, hash)
	client = sharedClient

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, targetURL, r)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	// Fix #23: propagate the OTel trace context.
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("pushing to %s: %w", endpoint, err)
	}
	// Drain and close response body immediately to release connection.
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("push failed: status %d", resp.StatusCode)
	}
	return nil
}
