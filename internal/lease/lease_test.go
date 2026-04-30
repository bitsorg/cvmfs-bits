package lease

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"cvmfs.io/prepub/pkg/observe"
)

// mockObjectReader is a minimal ObjectReader for tests. It stores raw
// "compressed" bytes keyed by hash.
type mockObjectReader struct {
	objects map[string][]byte
}

func (m *mockObjectReader) Get(_ context.Context, hash string) (io.ReadCloser, error) {
	b, ok := m.objects[hash]
	if !ok {
		return nil, fmt.Errorf("mock: object not found: %s", hash)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func newTestClient(t *testing.T, srv *httptest.Server) *Client {
	t.Helper()
	obs, shutdown, err := observe.New("test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)
	c := NewClient(srv.URL, "key", "secret", obs)
	// Use the test server's client so TLS certs are trusted for both
	// regular requests (c.client) and commit requests (c.commitClient,
	// which skips the per-client timeout so slow receiver operations
	// don't time out in production).
	c.client = srv.Client()
	c.commitClient = srv.Client()
	return c
}

// TestHeartbeat_RenewsWithCorrectToken verifies that the heartbeat goroutine
// always renews using the exact token string passed to Heartbeat().
// Previously this was a snapshot test (Fix #3) to guard against *Lease
// mutation; now that Heartbeat accepts a plain string (value type) the test
// verifies the simpler invariant: the token used for renewal equals the one
// passed in.
func TestHeartbeat_RenewsWithCorrectToken(t *testing.T) {
	const wantToken = "original-token"

	var mu sync.Mutex
	var seenTokens []string

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Renew: PUT /api/v1/leases/{token}
		if r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/api/v1/leases/") {
			tok := strings.TrimPrefix(r.URL.Path, "/api/v1/leases/")
			mu.Lock()
			seenTokens = append(seenTokens, tok)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := newTestClient(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopHeartbeat := c.Heartbeat(ctx, wantToken, 30*time.Millisecond, func() {})

	// Let the heartbeat fire a few times.
	time.Sleep(120 * time.Millisecond)
	stopHeartbeat()

	mu.Lock()
	got := append([]string(nil), seenTokens...)
	mu.Unlock()

	if len(got) == 0 {
		t.Fatal("heartbeat did not fire any renewal; test is inconclusive")
	}
	for _, tok := range got {
		if tok != wantToken {
			t.Errorf("heartbeat renewed with %q; want %q", tok, wantToken)
		}
	}
	t.Logf("heartbeat fired %d renewal(s), all with correct token", len(got))
}

// TestHeartbeat_CancelIsIdempotent verifies that calling the cancel function
// returned by Heartbeat multiple times does not panic (sync.Once guard).
func TestHeartbeat_CancelIsIdempotent(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newTestClient(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := c.Heartbeat(ctx, "tok", 10*time.Second, func() {})

	// Must not panic on repeated calls.
	stop()
	stop()
	stop()
}

// TestHeartbeat_StopsOnCancel verifies that after cancel() is called the
// goroutine stops sending renewals.
func TestHeartbeat_StopsOnCancel(t *testing.T) {
	var mu sync.Mutex
	var calls int

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			mu.Lock()
			calls++
			mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newTestClient(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	stop := c.Heartbeat(ctx, "tok", 20*time.Millisecond, func() {})

	// Let it fire a few times.
	time.Sleep(80 * time.Millisecond)
	stop()
	cancel()

	// Record count right after stopping.
	mu.Lock()
	callsAfterStop := calls
	mu.Unlock()

	// Wait a further interval and verify no new calls arrive.
	time.Sleep(60 * time.Millisecond)

	mu.Lock()
	callsAtEnd := calls
	mu.Unlock()

	if callsAtEnd != callsAfterStop {
		t.Errorf("heartbeat continued after stop: got %d calls after stop vs %d at end",
			callsAfterStop, callsAtEnd)
	}
	if callsAfterStop == 0 {
		t.Error("heartbeat never fired before stop; test inconclusive")
	}
	t.Logf("fired %d renewals before stop, 0 after", callsAfterStop)
}

// TestHeartbeat_CancelsJobAfterConsecutiveFailures verifies that onExpire is
// called after maxConsecutiveHeartbeatFailures consecutive renewal errors.
func TestHeartbeat_CancelsJobAfterConsecutiveFailures(t *testing.T) {
	// Server that always returns 503 for renewals so every heartbeat fails.
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newTestClient(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cancelled := make(chan struct{})
	onExpire := func() { close(cancelled) }

	// Short interval so the test completes quickly.
	stop := c.Heartbeat(ctx, "tok", 20*time.Millisecond, onExpire)
	defer stop()

	select {
	case <-cancelled:
		t.Logf("onExpire called after %d consecutive failures", maxConsecutiveHeartbeatFailures)
	case <-time.After(2 * time.Second):
		t.Errorf("onExpire was not called after %d consecutive failures within 2s",
			maxConsecutiveHeartbeatFailures)
	}
}

// TestHeartbeat_ResetConsecutiveOnSuccess verifies that a successful renewal
// resets the consecutive-failure counter so transient errors do not accumulate.
func TestHeartbeat_ResetConsecutiveOnSuccess(t *testing.T) {
	const (
		interval   = 20 * time.Millisecond
		failEveryN = 2 // alternate: fail, succeed, fail, succeed, ...
	)

	var mu sync.Mutex
	attempt := 0

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusOK)
			return
		}
		mu.Lock()
		n := attempt
		attempt++
		mu.Unlock()
		// Fail on even attempts, succeed on odd — never two failures in a row.
		if n%failEveryN == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	c := newTestClient(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cancelCalled := false
	onExpire := func() { cancelCalled = true }

	// Run for enough time that maxConsecutiveHeartbeatFailures would be hit
	// if successful renewals did NOT reset the counter.
	stop := c.Heartbeat(ctx, "tok", interval, onExpire)
	time.Sleep(time.Duration(maxConsecutiveHeartbeatFailures+2) * interval * 2)
	stop()

	if cancelCalled {
		t.Error("onExpire was called even though failures were non-consecutive (counter should reset on success)")
	}
}

// ── acquireLease JSON body tests ─────────────────────────────────────────────

// TestAcquireLease_PathInJSONBody verifies that the repository sub-path is
// sent correctly in the POST /api/v1/leases JSON body as the "path" field.
// The gateway API uses a JSON body (not URL segments) for the lease path, so
// no URL percent-encoding is applied; the path value is transmitted verbatim
// as a JSON string (where only '"' and '\' need escaping per RFC 8259).
//
// For example, the path "a b/c+d" must appear in the JSON body as
// "repo.example.com/a b/c+d" — space and plus sign are valid JSON characters.
func TestAcquireLease_PathInJSONBody(t *testing.T) {
	var capturedPath string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/v1/leases" {
			var body struct {
				Path string `json:"path"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decoding request body: %v", err)
			}
			capturedPath = body.Path
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok","session_token":"tok","max_lease_time":300}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := newTestClient(t, srv)

	// "a b/c+d" — spaces and plus signs are valid JSON string characters.
	_, _ = c.acquireLease(context.Background(), "repo.example.com", "a b/c+d")

	const wantPath = "repo.example.com/a b/c+d"
	if capturedPath != wantPath {
		t.Errorf("JSON body path = %q; want %q", capturedPath, wantPath)
	}
}

// TestAcquireLease_EmptyPathAcquiresRootLease verifies that an empty sub-path
// produces a root-level lease whose JSON body "path" field is "repo/" (trailing
// slash required by the gateway for root-level transactions).
// An empty path is valid and represents a full-repository publish lease.
func TestAcquireLease_EmptyPathAcquiresRootLease(t *testing.T) {
	var capturedPath string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/v1/leases" {
			var body struct {
				Path string `json:"path"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decoding request body: %v", err)
			}
			capturedPath = body.Path
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok","session_token":"tok","max_lease_time":300}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := newTestClient(t, srv)
	_, err := c.acquireLease(context.Background(), "repo.example.com", "")
	if err != nil {
		t.Fatalf("acquireLease with empty path: %v", err)
	}
	// Root-level lease must have a trailing slash in the path field.
	const wantPath = "repo.example.com/"
	if capturedPath != wantPath {
		t.Errorf("JSON body path = %q; want %q (root-level lease needs trailing slash)", capturedPath, wantPath)
	}
}

// TestAcquireLease_RejectsDoubleSlash verifies that a path containing "//"
// is rejected before any HTTP request is made.
func TestAcquireLease_RejectsDoubleSlash(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("HTTP request should not have been made for double-slash path")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newTestClient(t, srv)
	_, err := c.acquireLease(context.Background(), "repo.example.com", "atlas//24.0")
	if err == nil {
		t.Fatal("expected error for double-slash path, got nil")
	}
	if !strings.Contains(err.Error(), "adjacent slashes") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestAcquireLease_RejectsLeadingSlash verifies that a path starting with "/"
// is rejected — a leading slash causes strings.Split to produce an empty first
// segment that joins into a double-slash in the URL ("…/leases//foo").
func TestAcquireLease_RejectsLeadingSlash(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("HTTP request should not have been made for leading-slash path")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newTestClient(t, srv)
	_, err := c.acquireLease(context.Background(), "repo.example.com", "/atlas/24.0")
	if err == nil {
		t.Fatal("expected error for leading-slash path, got nil")
	}
	if !strings.Contains(err.Error(), "start with a slash") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ── Commit tag field tests ────────────────────────────────────────────────────

// TestCommit_SendsTagFields verifies that Client.Commit forwards TagName and
// TagDescription from CommitRequest into the POST body sent to the gateway's
// commit endpoint.
func TestCommit_SendsTagFields(t *testing.T) {
	const (
		token       = "lease-tok-abc"
		catalogHash = "deadbeef"
		wantTag     = "v3.14.0"
		wantDesc    = "PI release"
	)

	var (
		mu            sync.Mutex
		capturedBody  []byte
		commitCalled  bool
		payloadCalled bool
	)

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/payloads":
			// Accept the payload upload without inspection.
			mu.Lock()
			payloadCalled = true
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok"}`))


		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/v1/leases/"):
			// Commit endpoint — capture the request body.
			data, _ := io.ReadAll(r.Body)
			mu.Lock()
			capturedBody = data
			commitCalled = true
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok"}`))

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	c := newTestClient(t, srv)

	store := &mockObjectReader{
		objects: map[string][]byte{
			catalogHash: []byte("fake-compressed-catalog"),
		},
	}

	req := CommitRequest{
		Token:          token,
		CatalogHash:    catalogHash,
		ObjectStore:    store,
		TagName:        wantTag,
		TagDescription: wantDesc,
	}

	if err := c.Commit(context.Background(), req); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	mu.Lock()
	body := capturedBody
	called := commitCalled
	pCalled := payloadCalled
	mu.Unlock()

	if !pCalled {
		t.Error("payload upload endpoint was never called")
	}
	if !called {
		t.Fatal("commit endpoint was never called")
	}

	var got map[string]interface{}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode commit body %q: %v", body, err)
	}
	if v := got["tag_name"]; v != wantTag {
		t.Errorf("commit body tag_name = %q; want %q", v, wantTag)
	}
	if v := got["tag_description"]; v != wantDesc {
		t.Errorf("commit body tag_description = %q; want %q", v, wantDesc)
	}
}

// TestCommit_EmptyTagName checks that when TagName is empty the commit body
// still contains a "tag_name" key (empty string) so the gateway receives a
// well-formed request.
func TestCommit_EmptyTagName(t *testing.T) {
	const (
		token       = "lease-tok-empty"
		catalogHash = "cafe1234"
	)

	var (
		mu           sync.Mutex
		capturedBody []byte
	)

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/payloads":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/v1/leases/"):
			data, _ := io.ReadAll(r.Body)
			mu.Lock()
			capturedBody = data
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	c := newTestClient(t, srv)
	store := &mockObjectReader{
		objects: map[string][]byte{catalogHash: []byte("data")},
	}

	req := CommitRequest{
		Token:       token,
		CatalogHash: catalogHash,
		ObjectStore: store,
		// TagName and TagDescription are intentionally left empty.
	}
	if err := c.Commit(context.Background(), req); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	mu.Lock()
	body := capturedBody
	mu.Unlock()

	var got map[string]interface{}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode commit body: %v", err)
	}
	// key must be present (even if empty string) for a well-formed request
	if _, ok := got["tag_name"]; !ok {
		t.Error("commit body is missing \"tag_name\" key")
	}
	if v, _ := got["tag_name"].(string); v != "" {
		t.Errorf("commit body tag_name = %q; want empty string", v)
	}
}

// TestAcquireLease_RejectsTrailingSlash verifies that a path ending with "/"
// is rejected — it produces a trailing empty segment in the URL path.
func TestAcquireLease_RejectsTrailingSlash(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("HTTP request should not have been made for trailing-slash path")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newTestClient(t, srv)
	_, err := c.acquireLease(context.Background(), "repo.example.com", "atlas/24.0/")
	if err == nil {
		t.Fatal("expected error for trailing-slash path, got nil")
	}
	if !strings.Contains(err.Error(), "end with a slash") {
		t.Errorf("unexpected error: %v", err)
	}
}
