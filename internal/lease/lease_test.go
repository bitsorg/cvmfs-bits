package lease

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"cvmfs.io/prepub/pkg/observe"
)

func newTestClient(t *testing.T, srv *httptest.Server) *Client {
	t.Helper()
	obs, shutdown, err := observe.New("test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)
	c := NewClient(srv.URL, "key", "secret", obs)
	// Use the test server's client so TLS certs are trusted.
	c.client = srv.Client()
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

// ── acquireLease URL encoding tests ──────────────────────────────────────────

// TestAcquireLease_PercentEncodesPath verifies that special characters in the
// repository sub-path are percent-encoded in the URL while slash separators
// are preserved as literal slashes.  For example, the path "a b/c+d" must
// produce the URL segment "a%20b/c%2Bd" not "a b/c+d".
//
// We inspect r.URL.RawPath (the undecoded wire path) rather than r.URL.Path
// (which Go's HTTP server always URL-decodes before handing it to handlers).
func TestAcquireLease_PercentEncodesPath(t *testing.T) {
	var capturedRawPath string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/v1/leases/") {
			// RawPath holds the percent-encoded form; Path is already decoded.
			capturedRawPath = r.URL.RawPath
			// Return a minimal lease JSON so acquireLease does not error on decode.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"token":"tok","expires_at":"2099-01-01T00:00:00Z"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := newTestClient(t, srv)

	// "a b/c+d" — space must become %20, plus must become %2B, slash is preserved.
	_, _ = c.acquireLease(context.Background(), "repo.example.com", "a b/c+d")

	const wantSuffix = "/a%20b/c%2Bd"
	if !strings.HasSuffix(capturedRawPath, wantSuffix) {
		t.Errorf("URL raw path %q does not end with %q (percent-encoding bug)", capturedRawPath, wantSuffix)
	}
}

// TestAcquireLease_RejectsEmptyPath verifies that an empty path is rejected
// before any HTTP request is made.
func TestAcquireLease_RejectsEmptyPath(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("HTTP request should not have been made for empty path")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newTestClient(t, srv)
	_, err := c.acquireLease(context.Background(), "repo.example.com", "")
	if err == nil {
		t.Fatal("expected error for empty path, got nil")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("unexpected error: %v", err)
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
