// Package distribute manages replication of objects to Stratum 1 endpoints.
// It handles batch and per-object pushes with quorum enforcement and crash-safe
// distribution logging.
package distribute

import (
	"bufio"
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
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/bits-and-blooms/bloom/v3"

	"cvmfs.io/prepub/internal/broker"
	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/pkg/observe"
)

// sharedClient is used for all HTTPS control-channel requests (announce,
// bloom fetch) and for legacy per-object PUTs to endpoints that do not support
// the announce protocol.  TLS 1.2 is the minimum acceptable version.
//
// A 30 s client-level Timeout acts as a safety net for stalled connections
// that do not honour per-request context deadlines (e.g. a receiver that
// accepts the connection but then stops sending data).  Per-object and
// per-announce contexts impose their own shorter deadlines on top.
var sharedClient = &http.Client{
	Timeout: 30 * time.Second,
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
//
// No client-level Timeout is set here because individual object PUTs are
// bounded by per-request context deadlines (cfg.Timeout).  A hard client
// Timeout would prematurely kill legitimate large-object transfers.
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

	// BloomQueryTimeout is the per-endpoint timeout for fetching a receiver's
	// inventory Bloom filter before computing a delta push (GET /api/v1/bloom).
	// When non-zero and the receiver supports the announce protocol, the
	// distributor fetches the filter and skips objects the receiver already
	// holds, reducing unnecessary network traffic.
	// Set to 0 (default) to disable delta push and always push all objects.
	BloomQueryTimeout time.Duration

	// BrokerConfig, when non-nil, enables the MQTT control plane.  The
	// distributor will publish AnnounceMessages to the broker and collect
	// ReadyMessages from receivers rather than using the HTTP announce protocol.
	// The data channel (plain-HTTP PUT) is used unchanged for the actual
	// object transfer.  When nil the HTTP announce path is used (default).
	BrokerConfig *broker.Config

	// Repo is the CVMFS repository name being published (e.g. "atlas.cern.ch").
	// Required when BrokerConfig is set so the distributor can publish to the
	// correct announce topic.
	Repo string

	// TotalBytes is the total compressed size of all objects in this payload.
	// Passed to receivers in the AnnounceMessage for disk-space pre-checks.
	// Zero means unknown (receivers skip the disk-space check).
	TotalBytes int64

	// MQTTQuorumTimeout is how long to wait for ready replies from receivers
	// before deciding quorum is not reachable.  Defaults to 30 s.
	MQTTQuorumTimeout time.Duration
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

// fetchReceiverBloom fetches and deserialises the inventory Bloom filter from a
// receiver's control channel (GET /api/v1/bloom).
//
// A successful response is the binary encoding of a bloom.BloomFilter written
// by bloom.BloomFilter.WriteTo.  The caller uses the filter to compute a
// delta: objects already present at the receiver are skipped.
//
// Returns an error for any non-200 response or deserialisation failure.
// A 503 response means the receiver is still building its inventory; the
// caller should fall back to pushing all objects.
// fetchReceiverBloom fetches and deserialises the inventory Bloom filter from a
// receiver's control channel (GET /api/v1/bloom).
// hmacSecret is the shared HMAC secret; the function derives the bloom-read
// bearer token from it and attaches it to the Authorization header.
func fetchReceiverBloom(ctx context.Context, endpoint, hmacSecret string) (*bloom.BloomFilter, error) {
	url := strings.TrimRight(endpoint, "/") + "/api/v1/bloom"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("fetchReceiverBloom: building request: %w", err)
	}
	// Authenticate with the static bloom-read token derived from the shared secret.
	if hmacSecret != "" {
		mac := hmac.New(sha256.New, []byte(hmacSecret))
		mac.Write([]byte("bloom-read"))
		token := hex.EncodeToString(mac.Sum(nil))
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := sharedClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetchReceiverBloom: GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetchReceiverBloom: %s returned HTTP %d", url, resp.StatusCode)
	}

	bf := new(bloom.BloomFilter)
	if err := readBloomFilter(bf, resp.Body, url); err != nil {
		return nil, err
	}
	return bf, nil
}

// readBloomFilter wraps bloom.BloomFilter.ReadFrom with a panic recovery.
// The bloom library's ReadFrom panics with "makeslice: len out of range" when
// fed truncated or corrupt data (the header encodes the expected bitset length
// and ReadFrom allocates that many bytes upfront).  We recover and return an
// error so the distributor can fall back gracefully.
func readBloomFilter(bf *bloom.BloomFilter, r io.Reader, url string) (retErr error) {
	defer func() {
		if p := recover(); p != nil {
			retErr = fmt.Errorf("fetchReceiverBloom: corrupt filter body from %s: panic(%v)", url, p)
		}
	}()
	if _, err := bf.ReadFrom(r); err != nil {
		return fmt.Errorf("fetchReceiverBloom: deserialising filter from %s: %w", url, err)
	}
	return nil
}

// rejectPrivateHost resolves host and rejects it if any returned address falls
// within a private/loopback range.  When host is already a numeric IP the check
// is instantaneous and accurate.
//
// DNS TOCTOU limitation: when host is a hostname the DNS lookup happens at
// endpoint-validation time (once per Distribute call), but the actual TCP
// connection is made later inside pushObject.  A DNS rebinding attack could
// make the hostname resolve to a public address at validation time and a
// private address at connection time.  This is a known, fundamental limitation
// of any DNS-based SSRF protection.  Mitigations belong in the network layer
// (e.g. firewall rules that block egress to RFC-1918 ranges regardless of DNS)
// and are outside the scope of this code.
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

// NopDistLog returns a DistLog that silently discards all records.
// Use when distribution is best-effort and per-object crash-recovery tracking
// is not required (e.g. background fire-and-forget distribution).
func NopDistLog() *DistLog {
	return &DistLog{} // path is zero-value (""); Record returns nil on empty path
}

// distLogEntry is the fixed struct written to the distribution log.
// Using a named struct instead of map[string]string avoids a heap allocation
// per Record call (the struct can be stack-allocated and its fields need no
// runtime reflection on map keys).
type distLogEntry struct {
	Endpoint string `json:"endpoint"`
	Hash     string `json:"hash"`
	Time     string `json:"time"`
}

// Record appends a distribution entry to the log (endpoint, hash, timestamp).
// The file is opened on the first call and reused for subsequent calls.
// Call Sync or Close to flush to disk.
// Record is a no-op (returns nil) when d was created with NopDistLog.
func (d *DistLog) Record(endpoint, hash string) error {
	if d.path == "" {
		return nil // NopDistLog — records are discarded
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.f == nil {
		f, err := os.OpenFile(d.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			return fmt.Errorf("opening dist log: %w", err)
		}
		d.f = f
	}

	data, err := json.Marshal(distLogEntry{
		Endpoint: endpoint,
		Hash:     hash,
		Time:     time.Now().Format(time.RFC3339),
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
//
// Individual corrupt log entries (e.g. from a crash mid-write) are silently
// skipped rather than aborting the scan.  Objects whose entries were corrupted
// will be re-pushed on the next distribution run; because PUT is idempotent
// this is safe.  A non-zero skippedEntries count is returned so callers can
// emit a diagnostic if desired.
//
// Each entry is parsed as a self-contained line of JSON.  Scanning line-by-line
// (via bufio.Scanner) rather than with json.Decoder.More()/Decode() is
// intentional: json.Decoder can spin or block indefinitely on an unterminated
// token (e.g. a string without a closing quote written by a crash), whereas a
// line scanner always advances by exactly one newline per iteration.
func (d *DistLog) Confirmed(endpoint string) (hashes []string, skippedEntries int, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	data, err := os.ReadFile(d.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("reading dist log: %w", err)
	}

	// seen deduplicates: the same object may be recorded more than once for a
	// given endpoint when pushOne retries succeed after a previous attempt that
	// already called log.Record (e.g. the PUT succeeded but the ACK was lost, so
	// the sender retried and recorded again).  Callers must not need to handle
	// duplicates; we normalise here so that len(hashes) == count of distinct objects.
	seen := make(map[string]struct{})

	lineScanner := bufio.NewScanner(bytes.NewReader(data))
	for lineScanner.Scan() {
		line := lineScanner.Bytes()
		if len(line) == 0 {
			continue // blank line — not a corrupt entry, just skip
		}
		var entry struct {
			Endpoint string `json:"endpoint"`
			Hash     string `json:"hash"`
		}
		if decErr := json.Unmarshal(line, &entry); decErr != nil {
			// Skip the malformed entry.  A single truncated write (e.g. crash
			// mid-line) must not abort the entire scan and cause all objects to
			// be re-pushed.  The corrupt bytes have already been written to disk
			// and cannot be un-written; skipping is the least-disruptive option.
			skippedEntries++
			continue
		}
		if entry.Endpoint == endpoint {
			if _, dup := seen[entry.Hash]; !dup {
				seen[entry.Hash] = struct{}{}
				hashes = append(hashes, entry.Hash)
			}
		}
	}
	// lineScanner.Err() returns nil at EOF — no real I/O errors from a bytes.Reader.

	return hashes, skippedEntries, nil
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
	defer func() { io.Copy(io.Discard, io.LimitReader(resp.Body, 4096)); resp.Body.Close() }() //nolint:errcheck

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
	// MQTT control plane: when a broker is configured, use pub/sub for
	// discovery and announce, then push via the plain-HTTP data channel.
	// Falls through to the HTTP announce path when BrokerConfig is nil.
	if cfg.BrokerConfig != nil && cfg.BrokerConfig.BrokerURL != "" {
		return distributeMQTT(ctx, payloadID, hashes, casBackend, log, cfg)
	}

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
			announceSupported := false
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
					announceSupported = true
					sessionToken = ar.token
					dataEP = ar.dataEndpoint
					cfg.Obs.Logger.Info("announce accepted",
						"endpoint", endpoint, "data_endpoint", dataEP)
				} else {
					cfg.Obs.Logger.Info("receiver does not support announce — using legacy PUT",
						"endpoint", endpoint)
				}
			}

			// Delta push: fetch the receiver's inventory Bloom filter and skip
			// objects it already holds.  Only attempted when:
			//   • the receiver supported the announce protocol (it also serves /api/v1/bloom), and
			//   • BloomQueryTimeout > 0 (the operator has opted in).
			// On any error we fall back to pushing all objects — the conservative choice.
			effectiveHashes := hashes
			if announceSupported && cfg.BloomQueryTimeout > 0 {
				bctx, bcancel := context.WithTimeout(ectx, cfg.BloomQueryTimeout)
				bf, berr := fetchReceiverBloom(bctx, endpoint, cfg.HMACSecret)
				bcancel()
				if berr != nil {
					cfg.Obs.Logger.Warn("bloom fetch failed — pushing all objects",
						"endpoint", endpoint, "error", berr)
				} else {
					filtered := make([]string, 0, len(hashes))
					for _, h := range hashes {
						if !bf.TestString(h) {
							filtered = append(filtered, h)
						}
					}
					skipped := len(hashes) - len(filtered)
					cfg.Obs.Logger.Info("delta push computed",
						"endpoint", endpoint,
						"total", len(hashes),
						"to_push", len(filtered),
						"skipped", skipped)
					effectiveHashes = filtered
				}
			}

			// pushOne pushes a single object to the endpoint with per-object
			// exponential backoff retry.  Transient network errors (connection
			// refused, timeout, 5xx) are retried up to maxPushAttempts times;
			// permanent errors (4xx other than 429) are not retried.
			// Only after all attempts fail is the endpoint marked as failed.
			const maxPushAttempts = 4
			pushOne := func(hash string) {
				start := time.Now()
				var lastErr error
				for attempt := 0; attempt < maxPushAttempts; attempt++ {
					if attempt > 0 {
						// Exponential backoff: 1s, 2s, 4s (capped at 30s).
						secs := int64(1) << uint(attempt-1) // integer shift, then convert
						backoff := time.Duration(math.Min(float64(time.Second)*float64(secs), float64(30*time.Second)))
						cfg.Obs.Logger.Warn("retrying object push",
							"endpoint", endpoint, "hash", hash,
							"attempt", attempt+1, "backoff", backoff, "error", lastErr)
						select {
						case <-ectx.Done():
							// Context cancelled during backoff wait.  This is not an
							// endpoint fault — do not mark the endpoint as failed.
							return
						case <-time.After(backoff):
						}
					}
					lastErr = pushObject(ectx, endpoint, hash, casBackend, cfg.Timeout, sessionToken, dataEP, cfg.DevMode)
					if lastErr == nil {
						break
					}
					// Do not retry on context cancellation or permanent HTTP errors.
					if errors.Is(lastErr, context.Canceled) || errors.Is(lastErr, context.DeadlineExceeded) {
						break
					}
				}
				if lastErr != nil {
					// Context cancellation is not an endpoint failure.  Only record
					// the endpoint as failed when the error is an actual push error.
					if errors.Is(lastErr, context.Canceled) || errors.Is(lastErr, context.DeadlineExceeded) {
						return
					}
					espan.RecordError(lastErr)
					cfg.Obs.Logger.Error("pushing object (all attempts failed)",
						"endpoint", endpoint, "hash", hash, "error", lastErr)
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
				for _, hash := range effectiveHashes {
					pushOne(hash)
				}
				return nil
			}

			// Batching enabled.  Try the batch endpoint; fall back to per-object
			// PUTs if the receiver signals it does not support batching.
			batchSupported := true
			for i := 0; i < len(effectiveHashes); i += cfg.BatchSize {
				end := i + cfg.BatchSize
				if end > len(effectiveHashes) {
					end = len(effectiveHashes)
				}
				batch := effectiveHashes[i:end]

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

	cfg.Obs.Logger.Info("distribution complete",
		"confirmed", confirmed, "total", totalEndpoints, "objects", len(hashes))
	return confirmed, totalEndpoints, nil
}

// pushObject, pushBatch, batchUnsupportedError, isBatchUnsupported, and max
// have been moved to push.go in this package.
