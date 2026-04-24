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

// TestHeartbeat_UsesTokenSnapshot verifies that the heartbeat goroutine always
// renews with the token value captured when Heartbeat() was called, even if
// the caller mutates lease.Token afterwards (Fix #3).
func TestHeartbeat_UsesTokenSnapshot(t *testing.T) {
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
	lease := &Lease{Token: "original-token"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopHeartbeat := c.Heartbeat(ctx, lease, 30*time.Millisecond, func() {})

	// Immediately mutate the token after starting the heartbeat.
	// The goroutine must have already captured the original value.
	lease.Token = "mutated-token"

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
		if tok != "original-token" {
			t.Errorf("heartbeat renewed with %q; want %q (captured snapshot)", tok, "original-token")
		}
	}
	t.Logf("heartbeat fired %d renewal(s), all with original token", len(got))
}

// TestHeartbeat_CancelIsIdempotent verifies that calling the cancel function
// returned by Heartbeat multiple times does not panic (sync.Once guard).
func TestHeartbeat_CancelIsIdempotent(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newTestClient(t, srv)
	lease := &Lease{Token: "tok"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := c.Heartbeat(ctx, lease, 10*time.Second, func() {})

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
	lease := &Lease{Token: "tok"}

	ctx, cancel := context.WithCancel(context.Background())
	stop := c.Heartbeat(ctx, lease, 20*time.Millisecond, func() {})

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

// TestHeartbeat_CancelsJobAfterConsecutiveFailures verifies that cancelJob is
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
	lease := &Lease{Token: "tok"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cancelled := make(chan struct{})
	cancelJob := func() { close(cancelled) }

	// Short interval so the test completes quickly.
	stop := c.Heartbeat(ctx, lease, 20*time.Millisecond, cancelJob)
	defer stop()

	select {
	case <-cancelled:
		t.Logf("cancelJob called after %d consecutive failures", maxConsecutiveHeartbeatFailures)
	case <-time.After(2 * time.Second):
		t.Errorf("cancelJob was not called after %d consecutive failures within 2s",
			maxConsecutiveHeartbeatFailures)
	}
}

// TestHeartbeat_ResetConsecutiveOnSuccess verifies that a successful renewal
// resets the consecutive-failure counter so transient errors do not accumulate.
func TestHeartbeat_ResetConsecutiveOnSuccess(t *testing.T) {
	const (
		interval    = 20 * time.Millisecond
		failEveryN  = 2 // alternate: fail, succeed, fail, succeed, ...
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
	lease := &Lease{Token: "tok"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cancelCalled := false
	cancelJob := func() { cancelCalled = true }

	// Run for enough time that maxConsecutiveHeartbeatFailures would be hit
	// if successful renewals did NOT reset the counter.
	stop := c.Heartbeat(ctx, lease, interval, cancelJob)
	time.Sleep(time.Duration(maxConsecutiveHeartbeatFailures+2) * interval * 2)
	stop()

	if cancelCalled {
		t.Error("cancelJob was called even though failures were non-consecutive (counter should reset on success)")
	}
}
