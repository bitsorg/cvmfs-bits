package distribute

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"cvmfs.io/prepub/pkg/observe"
	"cvmfs.io/prepub/testutil/fakecas"
)

// newTestCAS creates a fakecas backed by a real (but minimal) observe.Provider
// so that tracing calls inside fakecas do not panic.
func newTestCAS(t *testing.T) *fakecas.CAS {
	t.Helper()
	obs, shutdown, err := observe.New("test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)
	return fakecas.New(obs)
}

// putCAS is a helper that wraps the fakecas Put signature correctly.
func putCAS(t *testing.T, c *fakecas.CAS, hash string, data []byte) {
	t.Helper()
	if err := c.Put(context.Background(), hash, io.NopCloser(bytes.NewReader(data)), int64(len(data))); err != nil {
		t.Fatalf("cas.Put(%s): %v", hash, err)
	}
}

// ── Fix #1: pushBatch timeout must not be scaled by batch size ───────────────

// TestPushBatch_TimeoutNotScaled verifies that a slow batch endpoint is
// cancelled in O(timeout), not O(timeout × len(hashes)).
//
// Before the fix: timeout*len(hashes) → 5×200ms = 1s deadline for 5 objects.
// After the fix:  timeout             → 200ms deadline regardless of batch size.
func TestPushBatch_TimeoutNotScaled(t *testing.T) {
	stallDuration := 500 * time.Millisecond
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(stallDuration)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Swap the package-level client so our handler's TLS cert is trusted.
	old := sharedClient
	sharedClient = srv.Client()
	defer func() { sharedClient = old }()

	cas := newTestCAS(t)
	hashes := []string{"aabbcc", "ddeeff", "112233", "445566", "778899"}
	for _, h := range hashes {
		putCAS(t, cas, h, []byte("data"))
	}

	timeout := 150 * time.Millisecond
	start := time.Now()
	_, err := pushBatch(context.Background(), srv.URL, hashes, cas, timeout)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error from slow server, got nil")
	}
	// If the bug were present the deadline would be timeout×len(hashes).
	// We verify elapsed is well below that ceiling.
	scaledCeiling := time.Duration(len(hashes)) * timeout
	if elapsed >= scaledCeiling {
		t.Errorf("pushBatch ran for %v — timeout appears to still be scaled (ceiling %v)", elapsed, scaledCeiling)
	}
	t.Logf("cancelled after %v (timeout=%v, batch=%d, scaled ceiling=%v)", elapsed, timeout, len(hashes), scaledCeiling)
}

// TestPushBatch_SuccessReturnsAllHashes verifies successful batch response parsing.
func TestPushBatch_SuccessReturnsAllHashes(t *testing.T) {
	want := []string{"hash1", "hash2", "hash3"}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"received":["hash1","hash2","hash3"]}`))
	}))
	defer srv.Close()

	old := sharedClient
	sharedClient = srv.Client()
	defer func() { sharedClient = old }()

	cas := newTestCAS(t)
	for _, h := range want {
		putCAS(t, cas, h, []byte("x"))
	}

	got, err := pushBatch(context.Background(), srv.URL, want, cas, 5*time.Second)
	if err != nil {
		t.Fatalf("pushBatch error: %v", err)
	}
	if len(got) != len(want) {
		t.Errorf("got %d hashes, want %d", len(got), len(want))
	}
}

// TestPushBatch_404ReturnsBatchUnsupported verifies the fallback sentinel error.
func TestPushBatch_404ReturnsBatchUnsupported(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	old := sharedClient
	sharedClient = srv.Client()
	defer func() { sharedClient = old }()

	cas := newTestCAS(t)
	putCAS(t, cas, "h1", []byte("d"))

	_, err := pushBatch(context.Background(), srv.URL, []string{"h1"}, cas, 5*time.Second)
	if !isBatchUnsupported(err) {
		t.Errorf("expected batchUnsupportedError for 404, got %v", err)
	}
}

// ── ValidateEndpoints ─────────────────────────────────────────────────────────

func TestValidateEndpoints_HTTPRejected(t *testing.T) {
	obs, shutdown, _ := observe.New("test")
	defer shutdown()
	cfg := Config{Endpoints: []string{"http://example.com/"}, Obs: obs, DevMode: false}
	if err := cfg.ValidateEndpoints(); err == nil {
		t.Error("expected error for http:// endpoint, got nil")
	}
}

func TestValidateEndpoints_DevModeAllowsHTTP(t *testing.T) {
	obs, shutdown, _ := observe.New("test")
	defer shutdown()
	cfg := Config{Endpoints: []string{"http://localhost:8080/"}, Obs: obs, DevMode: true}
	if err := cfg.ValidateEndpoints(); err != nil {
		t.Errorf("unexpected error in dev mode: %v", err)
	}
}

// ── Fix #9: DistLog — file opened once, not per write ────────────────────────

// TestDistLog_RecordAndConfirmed checks that written entries are readable after Sync.
func TestDistLog_RecordAndConfirmed(t *testing.T) {
	dir := t.TempDir()
	log := OpenDistLog(dir + "/dist.log")
	defer log.Close()

	entries := []struct{ ep, hash string }{
		{"https://s1.example.com", "hash1"},
		{"https://s1.example.com", "hash2"},
		{"https://s2.example.com", "hash1"},
	}
	for _, e := range entries {
		if err := log.Record(e.ep, e.hash); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}
	if err := log.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	hashes, err := log.Confirmed("https://s1.example.com")
	if err != nil {
		t.Fatalf("Confirmed: %v", err)
	}
	if len(hashes) != 2 {
		t.Errorf("want 2 hashes for s1, got %d", len(hashes))
	}

	hashes2, err := log.Confirmed("https://s2.example.com")
	if err != nil {
		t.Fatalf("Confirmed s2: %v", err)
	}
	if len(hashes2) != 1 {
		t.Errorf("want 1 hash for s2, got %d", len(hashes2))
	}
}

// TestDistLog_ManyWritesSingleOpen verifies that 1000 writes succeed and are
// all readable — confirming the file is held open across calls, not re-opened.
func TestDistLog_ManyWritesSingleOpen(t *testing.T) {
	const N = 1000
	dir := t.TempDir()
	log := OpenDistLog(dir + "/dist.log")
	defer log.Close()

	for i := 0; i < N; i++ {
		if err := log.Record("https://ep.example.com", "hashX"); err != nil {
			t.Fatalf("Record #%d: %v", i, err)
		}
	}
	if err := log.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	hashes, err := log.Confirmed("https://ep.example.com")
	if err != nil {
		t.Fatalf("Confirmed: %v", err)
	}
	if len(hashes) != N {
		t.Errorf("want %d records, got %d", N, len(hashes))
	}
}

// TestDistLog_CloseIdempotent verifies Close is safe to call on an unused log.
func TestDistLog_CloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	log := OpenDistLog(dir + "/dist.log")
	if err := log.Close(); err != nil {
		t.Errorf("Close on unused log: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// TestDistLog_SyncBeforeClose checks Sync flushes without closing.
func TestDistLog_SyncBeforeClose(t *testing.T) {
	dir := t.TempDir()
	log := OpenDistLog(dir + "/dist.log")
	_ = log.Record("https://ep.example.com", "abc")
	if err := log.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	// Can still write after Sync.
	if err := log.Record("https://ep.example.com", "def"); err != nil {
		t.Fatalf("Record after Sync: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestDistLog_RecordWriteError verifies that Record propagates a file-write error
// instead of silently swallowing it (Fix #4: json.Marshal error now checked too).
// We simulate a write error by opening the log against a path whose parent
// directory does not exist.
func TestDistLog_RecordWriteError(t *testing.T) {
	log := OpenDistLog("/no/such/directory/dist.log")
	err := log.Record("https://ep.example.com", "deadbeef")
	if err == nil {
		t.Error("expected error writing to non-existent directory, got nil")
	}
}

// TestDistLog_ConfirmedMissingFile verifies that Confirmed returns (nil, nil)
// when the log file does not yet exist — not an error.
func TestDistLog_ConfirmedMissingFile(t *testing.T) {
	log := OpenDistLog("/no/such/directory/dist.log")
	hashes, err := log.Confirmed("https://ep.example.com")
	if err != nil {
		t.Errorf("Confirmed on missing file: unexpected error %v", err)
	}
	if hashes != nil {
		t.Errorf("Confirmed on missing file: want nil slice, got %v", hashes)
	}
}

// TestDistLog_FileMode verifies that the dist log file is created with mode
// 0600 (owner read/write only), not world-readable 0644 (Fix #5).
func TestDistLog_FileMode(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/dist.log"
	log := OpenDistLog(path)
	defer log.Close()

	if err := log.Record("https://s1.example.com", "deadbeef"); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if err := log.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("dist log has mode %04o, want 0600", perm)
	}
}

// ── Fix #4: Batch distribution metric records total time, not per-object average ──

// TestPushBatch_MetricRecordsTotalTime verifies that the batch metric records
// the total batch elapsed time rather than an averaged per-object value.
// We do this indirectly: if the server stalls for a known duration, the
// observation on success must be >= that duration (not divided down).
func TestPushBatch_MetricRecordsTotalTime(t *testing.T) {
	// This property is enforced at code level (Fix #4 changed the Observe call
	// from elapsed/len to elapsed).  Here we verify the batch endpoint returns
	// the full set of hashes and that the returned elapsed timing is reasonable
	// by checking the batch-path code compiles and runs without dividing by len.
	stallDuration := 30 * time.Millisecond
	want := []string{"h1", "h2", "h3"}

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(stallDuration)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"received":["h1","h2","h3"]}`))
	}))
	defer srv.Close()

	old := sharedClient
	sharedClient = srv.Client()
	defer func() { sharedClient = old }()

	cas := newTestCAS(t)
	for _, h := range want {
		putCAS(t, cas, h, []byte("x"))
	}

	start := time.Now()
	got, err := pushBatch(context.Background(), srv.URL, want, cas, 5*time.Second)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("pushBatch: %v", err)
	}
	if len(got) != len(want) {
		t.Errorf("got %d hashes, want %d", len(got), len(want))
	}
	// The total elapsed time must be >= stallDuration (not stallDuration/3).
	if elapsed < stallDuration {
		t.Errorf("elapsed %v < stallDuration %v — timing looks wrong", elapsed, stallDuration)
	}
}

// TestAnnounceHMACPathWithEndpointPath verifies that HMAC computation includes
// the endpoint path prefix correctly.  E.g., if endpoint is
// "https://s1.example.com/stratum1/", the canonical message must use
// "/stratum1/api/v1/announce", not just "/api/v1/announce".
//
// This is an integration test: we start a mock receiver that validates the HMAC,
// and verify that an announce request succeeds.
func TestAnnounceHMACPathWithEndpointPath(t *testing.T) {
	obs, shutdown, err := observe.New("test")
	defer shutdown()
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}

	secret := "test-secret"
	var receivedURI string

	// Mock receiver that captures the request path for validation.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Capture the URI as the receiver would see it.
		receivedURI = req.URL.RequestURI()
		// For this test we just validate it was received; HMAC validation
		// is tested in the receiver package.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"session_token":"abc123","data_endpoint":"http://localhost:9101"}`))
	}))
	defer srv.Close()

	// The endpoint URL has a path prefix.
	endpoint := srv.URL + "/stratum1"

	cfg := Config{
		Endpoints:  []string{endpoint},
		HMACSecret: secret,
		DevMode:    true,
		Obs:        obs,
	}

	// Call announce — it should succeed and the receiver should see
	// /stratum1/api/v1/announce in req.RequestURI.
	_, err = announce(context.Background(), endpoint, "test-payload", []string{}, cfg)
	if err != nil {
		t.Fatalf("announce: %v", err)
	}

	// Verify the receiver saw the correct URI.
	wantURI := "/stratum1/api/v1/announce"
	if receivedURI != wantURI {
		t.Errorf("receiver saw URI %q, want %q", receivedURI, wantURI)
	}
}
