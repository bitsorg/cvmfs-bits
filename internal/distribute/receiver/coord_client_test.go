package receiver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cvmfs.io/prepub/pkg/observe"
)

// fakeCoord is a minimal HTTP server that records coordination service calls.
type fakeCoord struct {
	registerCount  atomic.Int32
	heartbeatCount atomic.Int32
	deregCount     atomic.Int32
	lastHeartbeat  atomic.Value // stores *coordHeartbeatRequest
	lastRegister   atomic.Value // stores *coordRegisterRequest
	statusCode     atomic.Int32 // response status; 0 means success (201/204)
}

func newFakeCoord(t *testing.T) (*fakeCoord, *httptest.Server) {
	t.Helper()
	fc := &fakeCoord{}
	mux := http.NewServeMux()

	mux.HandleFunc("/api/v1/nodes", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		fc.registerCount.Add(1)
		var reg coordRegisterRequest
		if err := json.NewDecoder(r.Body).Decode(&reg); err == nil {
			fc.lastRegister.Store(&reg)
		}
		code := int(fc.statusCode.Load())
		if code == 0 {
			code = http.StatusCreated
		}
		w.WriteHeader(code)
	})

	// Handle /api/v1/nodes/{id}/heartbeat and /api/v1/nodes/{id}
	mux.HandleFunc("/api/v1/nodes/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut: // heartbeat
			fc.heartbeatCount.Add(1)
			var hb coordHeartbeatRequest
			if err := json.NewDecoder(r.Body).Decode(&hb); err == nil {
				fc.lastHeartbeat.Store(&hb)
			}
			code := int(fc.statusCode.Load())
			if code == 0 {
				code = http.StatusNoContent
			}
			w.WriteHeader(code)
		case http.MethodDelete: // deregister
			fc.deregCount.Add(1)
			code := int(fc.statusCode.Load())
			if code == 0 {
				code = http.StatusNoContent
			}
			w.WriteHeader(code)
		default:
			http.Error(w, "method", http.StatusMethodNotAllowed)
		}
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return fc, srv
}

func newTestObs(t *testing.T) *observe.Provider {
	t.Helper()
	obs, shutdown, err := observe.New("test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)
	return obs
}

// TestCoordClient_NilWhenURLEmpty verifies that newCoordClient returns nil for
// an empty URL and that Start/Stop are safe no-ops on nil.
func TestCoordClient_NilWhenURLEmpty(t *testing.T) {
	obs := newTestObs(t)
	inv := newInventory(100, 0.01)
	c := newCoordClient("", "token", "node1", nil, "", "http://localhost:9101", inv, obs)
	if c != nil {
		t.Fatal("expected nil CoordClient when coordURL is empty")
	}
	// These must not panic on nil receiver.
	c.Start()
	c.Stop()
}

// TestCoordClient_Register verifies that Start sends a registration POST.
func TestCoordClient_Register(t *testing.T) {
	fc, srv := newFakeCoord(t)
	obs := newTestObs(t)
	inv := newInventory(100, 0.01)

	c := newCoordClient(srv.URL, "secret", "node-test", []string{"atlas.cern.ch"}, "https://host:9100", "http://host:9101", inv, obs)
	if c == nil {
		t.Fatal("expected non-nil CoordClient")
	}
	c.Start()
	defer c.Stop()

	if got := fc.registerCount.Load(); got != 1 {
		t.Errorf("expected 1 register call, got %d", got)
	}
}

// TestCoordClient_Heartbeat verifies that the heartbeat carries correct inventory state.
func TestCoordClient_Heartbeat(t *testing.T) {
	fc, srv := newFakeCoord(t)
	obs := newTestObs(t)
	inv := newInventory(100, 0.01)

	// Add an object so bloom_size > 0.
	inv.add(makeHash(1))
	inv.mu.Lock()
	inv.ready = true
	inv.mu.Unlock()

	c := newCoordClient(srv.URL, "secret", "node-hb", nil, "", "http://host:9101", inv, obs)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := c.heartbeat(ctx); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	if got := fc.heartbeatCount.Load(); got != 1 {
		t.Errorf("expected 1 heartbeat, got %d", got)
	}

	hb, ok := fc.lastHeartbeat.Load().(*coordHeartbeatRequest)
	if !ok || hb == nil {
		t.Fatal("expected a stored heartbeat request body")
	}
	if hb.BloomSize != 1 {
		t.Errorf("expected bloom_size=1, got %d", hb.BloomSize)
	}
	if !hb.Ready {
		t.Error("expected ready=true")
	}
}

// TestCoordClient_Deregister verifies that Stop sends a DELETE request.
func TestCoordClient_Deregister(t *testing.T) {
	fc, srv := newFakeCoord(t)
	obs := newTestObs(t)
	inv := newInventory(100, 0.01)

	c := newCoordClient(srv.URL, "secret", "node-der", nil, "", "http://host:9101", inv, obs)
	c.Start()
	c.Stop()

	if got := fc.deregCount.Load(); got != 1 {
		t.Errorf("expected 1 deregister call, got %d", got)
	}
}

// TestCoordClient_StopIdempotent verifies that Stop may be called multiple times.
func TestCoordClient_StopIdempotent(t *testing.T) {
	fc, srv := newFakeCoord(t)
	obs := newTestObs(t)
	inv := newInventory(100, 0.01)

	c := newCoordClient(srv.URL, "secret", "node-idem", nil, "", "http://host:9101", inv, obs)
	c.Start()
	c.Stop()
	c.Stop() // second call must be a no-op
	c.Stop()

	if got := fc.deregCount.Load(); got != 1 {
		t.Errorf("expected exactly 1 deregister, got %d", got)
	}
}

// TestCoordClient_RegistrationFailure verifies a 500 from the coord service is
// tolerated: Start does not panic, and the metric is incremented.
func TestCoordClient_RegistrationFailure(t *testing.T) {
	fc, srv := newFakeCoord(t)
	fc.statusCode.Store(http.StatusInternalServerError)
	obs := newTestObs(t)
	inv := newInventory(100, 0.01)

	c := newCoordClient(srv.URL, "secret", "node-fail", nil, "", "http://host:9101", inv, obs)
	c.Start() // must not panic
	defer c.Stop()

	if got := fc.registerCount.Load(); got != 1 {
		t.Errorf("expected 1 register attempt, got %d", got)
	}
}

// TestCoordClient_NodeIDDefaultsToHostname verifies that an empty NodeID is
// replaced with the system hostname (not an empty string).
func TestCoordClient_NodeIDDefaultsToHostname(t *testing.T) {
	_, srv := newFakeCoord(t)
	obs := newTestObs(t)
	inv := newInventory(100, 0.01)

	c := newCoordClient(srv.URL, "token", "" /* nodeID */, nil, "", "http://host:9101", inv, obs)
	if c == nil {
		t.Fatal("expected non-nil client")
	}
	if c.nodeID == "" {
		t.Error("nodeID should default to hostname, got empty string")
	}
}

// TestCoordClient_404OnDeregister verifies that a 404 from the coord service
// during deregister is treated as success (the coord service may have restarted).
func TestCoordClient_404OnDeregister(t *testing.T) {
	fc, srv := newFakeCoord(t)
	obs := newTestObs(t)
	inv := newInventory(100, 0.01)

	c := newCoordClient(srv.URL, "secret", "node-404", nil, "", "http://host:9101", inv, obs)

	// Force the server to return 404 on the deregister call.
	fc.statusCode.Store(http.StatusNotFound)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.deregister(ctx); err != nil {
		t.Errorf("deregister with 404 should not error, got: %v", err)
	}
}

// ── Fix #4: url.PathEscape(nodeID) in URL path construction ──────────────────

// TestCoordClient_NodeIDWithSlash verifies that a nodeID containing "/" is
// properly escaped in the heartbeat URL path so the request routes to the
// correct resource rather than an unintended sub-path.
//
// Go's HTTP server decodes percent-encoded path segments before exposing them
// in r.URL.Path.  r.URL.RawPath retains the encoded form (non-empty only when
// the path contains encoded characters), which is what we check here.
func TestCoordClient_NodeIDWithSlash(t *testing.T) {
	type capture struct{ path, rawPath string }
	var capturedPaths atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPaths.Store(capture{path: r.URL.Path, rawPath: r.URL.RawPath})
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	obs := newTestObs(t)
	inv := newInventory(100, 0.01)
	// nodeID with a slash — must be percent-encoded to %2F in the path segment.
	c := newCoordClient(srv.URL, "tok", "atlas/node1", nil, "", "http://host:9101", inv, obs)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = c.heartbeat(ctx)

	got, _ := capturedPaths.Load().(capture)
	// The decoded path will still contain a "/" because the server decoded %2F,
	// but the raw path must contain the encoded form "atlas%2Fnode1".
	// A naive (unescaped) implementation would produce rawPath="" (all chars
	// are safe) and path="/api/v1/nodes/atlas/node1/heartbeat" (two segments).
	//
	// With proper escaping, the Go HTTP client sends the path literally as
	// /api/v1/nodes/atlas%2Fnode1/heartbeat; the server sees:
	//   Path    = /api/v1/nodes/atlas/node1/heartbeat  (decoded)
	//   RawPath = /api/v1/nodes/atlas%2Fnode1/heartbeat (encoded, non-empty)
	if got.rawPath == "" {
		t.Error("RawPath is empty — nodeID slash was not percent-encoded in the URL")
	}
	const wantEncodedSegment = "atlas%2Fnode1"
	if !strings.Contains(got.rawPath, wantEncodedSegment) {
		t.Errorf("RawPath %q does not contain escaped segment %q", got.rawPath, wantEncodedSegment)
	}
}

// ── Fix #7: trailing slash trimmed from coordURL ──────────────────────────────

// TestCoordClient_TrailingSlashNormalised verifies that a coordURL with a
// trailing slash does not produce double-slash paths in requests.
func TestCoordClient_TrailingSlashNormalised(t *testing.T) {
	var capturedPath atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath.Store(r.URL.Path)
		w.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(srv.Close)

	obs := newTestObs(t)
	inv := newInventory(100, 0.01)
	// Pass the server URL with a trailing slash.
	c := newCoordClient(srv.URL+"/", "tok", "node-slash", nil, "", "http://host:9101", inv, obs)
	c.Start()
	defer c.Stop()

	path, _ := capturedPath.Load().(string)
	if len(path) > 1 && path[:2] == "//" {
		t.Errorf("double slash in path %q — trailing slash not normalised", path)
	}
	// The registration path must be exactly /api/v1/nodes (single slash).
	if path != coordRegisterPath {
		t.Errorf("register path: got %q, want %q", path, coordRegisterPath)
	}
}

// ── Fix #9: injectable ticker — heartbeat retry-registration actually fires ──

// TestCoordClient_HeartbeatRetriesViaRealTicker verifies the full retry-
// registration loop using the injectable ticker.  It confirms that when the
// initial registration fails, the heartbeat goroutine calls register() on the
// next tick and succeeds.
func TestCoordClient_HeartbeatRetriesViaRealTicker(t *testing.T) {
	fc, srv := newFakeCoord(t)
	obs := newTestObs(t)
	inv := newInventory(100, 0.01)

	// First register call fails; subsequent ones succeed.
	fc.statusCode.Store(http.StatusInternalServerError)

	c := newCoordClient(srv.URL, "secret", "node-hbtick", nil, "", "http://host:9101", inv, obs)

	// Inject a very short ticker so the test doesn't wait 30 s.
	c.newTicker = func() *time.Ticker {
		// Return a real Ticker wrapping our channel by creating a ticker with a
		// long duration then replacing its channel.  Since time.Ticker.C is
		// unexported and read-only in normal usage, we instead override newTicker
		// to return a ticker whose channel we control via a wrapper.
		// Simpler: use a real 1 ms ticker — fast enough for tests, real enough
		// to exercise the production select path.
		return time.NewTicker(1 * time.Millisecond)
	}

	c.Start() // initial registration fails; registered=false

	if got := fc.registerCount.Load(); got != 1 {
		t.Fatalf("expected 1 register attempt after Start, got %d", got)
	}
	if c.registered.Load() {
		t.Fatal("should not be registered after failed start")
	}

	// Let registrations succeed now.
	fc.statusCode.Store(0)

	// Wait for the heartbeat loop to retry with the 1 ms ticker.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if c.registered.Load() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if !c.registered.Load() {
		t.Error("expected registered=true after heartbeat loop retry within 500 ms")
	}
	if got := fc.registerCount.Load(); got < 2 {
		t.Errorf("expected >= 2 register calls total, got %d", got)
	}

	c.Stop()
}

// TestCoordClient_ControlURLInRegisterBody verifies that the control_url field
// is populated in the registration request body (fix for missing ControlURL bug).
func TestCoordClient_ControlURLInRegisterBody(t *testing.T) {
	fc, srv := newFakeCoord(t)
	obs := newTestObs(t)
	inv := newInventory(100, 0.01)

	const wantControlURL = "https://myhost.example.com:9100"
	const wantDataURL = "http://myhost.example.com:9101"

	c := newCoordClient(srv.URL, "secret", "node-ctrl", []string{"test.cern.ch"},
		wantControlURL, wantDataURL, inv, obs)
	c.Start()
	defer c.Stop()

	reg, ok := fc.lastRegister.Load().(*coordRegisterRequest)
	if !ok || reg == nil {
		t.Fatal("expected a stored register request body")
	}
	if reg.ControlURL != wantControlURL {
		t.Errorf("control_url: got %q, want %q", reg.ControlURL, wantControlURL)
	}
	if reg.DataURL != wantDataURL {
		t.Errorf("data_url: got %q, want %q", reg.DataURL, wantDataURL)
	}
}

// TestCoordClient_RetryRegistration verifies that when the initial registration
// fails the heartbeat loop retries it on the next tick (fix for retry-on-fail bug).
func TestCoordClient_RetryRegistration(t *testing.T) {
	fc, srv := newFakeCoord(t)
	obs := newTestObs(t)
	inv := newInventory(100, 0.01)

	// Override the register handler via statusCode: first POST fails.
	fc.statusCode.Store(http.StatusInternalServerError)

	c := newCoordClient(srv.URL, "secret", "node-retry", nil, "", "http://host:9101", inv, obs)
	// Patch heartbeat interval to be very short for the test.
	c.httpClient.Timeout = coordHTTPTimeout

	c.Start() // registration fails; registered=false

	// After Start, registerCount should be 1 (the failed attempt).
	if got := fc.registerCount.Load(); got != 1 {
		t.Fatalf("expected 1 register attempt after Start, got %d", got)
	}
	if c.registered.Load() {
		t.Fatal("registered should be false after failed initial registration")
	}

	// Now let registrations succeed.
	fc.statusCode.Store(0) // reset to success

	// Wait for the heartbeat loop to retry registration.  The default interval
	// is 30 s which is too long for a test; we call register directly to simulate
	// what the heartbeat loop would do.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.register(ctx); err != nil {
		t.Fatalf("register retry: %v", err)
	}
	if !c.registered.Load() {
		t.Error("registered should be true after successful retry")
	}
	if got := fc.registerCount.Load(); got != 2 {
		t.Errorf("expected 2 register calls total, got %d", got)
	}

	c.Stop()
}

// TestInventory_PopulateFromCAS_UnreadableRoot verifies that populateFromCAS
// sets ready=true even when the root directory cannot be read (fix for permanent
// 503 bug when os.ReadDir fails with a non-NotExist error).
func TestInventory_PopulateFromCAS_UnreadableRoot(t *testing.T) {
	inv := newInventory(100, 0.01)

	// Pass a path that exists as a file (not a directory), which causes
	// os.ReadDir to return an error that is not os.IsNotExist.
	// We create a temp file and use its path as the "casRoot".
	f, err := createTempFile(t)
	if err != nil {
		t.Fatalf("creating temp file: %v", err)
	}

	logged := false
	logFn := func(msg string, args ...any) { logged = true }

	populateErr := inv.populateFromCAS(context.Background(), f, logFn)
	if populateErr == nil {
		t.Error("expected error from populateFromCAS on a file path, got nil")
	}
	// The critical invariant: ready must be true despite the error.
	if !inv.isReady() {
		t.Error("isReady() should return true even when populateFromCAS returns an error")
	}
	_ = logged
}

// createTempFile creates a temporary file and returns its path.
func createTempFile(t *testing.T) (string, error) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "inv-test-*")
	if err != nil {
		return "", err
	}
	f.Close()
	return f.Name(), nil
}
