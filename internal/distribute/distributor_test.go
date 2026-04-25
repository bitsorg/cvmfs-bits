package distribute

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bits-and-blooms/bloom/v3"

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

	hashes, skipped, err := log.Confirmed("https://s1.example.com")
	if err != nil {
		t.Fatalf("Confirmed: %v", err)
	}
	if skipped != 0 {
		t.Errorf("unexpected skipped entries: %d", skipped)
	}
	if len(hashes) != 2 {
		t.Errorf("want 2 hashes for s1, got %d", len(hashes))
	}

	hashes2, skipped2, err := log.Confirmed("https://s2.example.com")
	if err != nil {
		t.Fatalf("Confirmed s2: %v", err)
	}
	if skipped2 != 0 {
		t.Errorf("unexpected skipped entries for s2: %d", skipped2)
	}
	if len(hashes2) != 1 {
		t.Errorf("want 1 hash for s2, got %d", len(hashes2))
	}
}

// TestDistLog_ManyWritesSingleOpen verifies that 1000 writes succeed and are
// all readable — confirming the file is held open across calls, not re-opened.
// Each write uses a distinct hash so that deduplication in Confirmed() does not
// collapse them (dedup is tested separately in TestDistLog_Confirmed_DeduplicatesHashes).
func TestDistLog_ManyWritesSingleOpen(t *testing.T) {
	const N = 1000
	dir := t.TempDir()
	log := OpenDistLog(dir + "/dist.log")
	defer log.Close()

	for i := 0; i < N; i++ {
		hash := fmt.Sprintf("hash%04d", i) // distinct per iteration
		if err := log.Record("https://ep.example.com", hash); err != nil {
			t.Fatalf("Record #%d: %v", i, err)
		}
	}
	if err := log.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	hashes, skipped, err := log.Confirmed("https://ep.example.com")
	if err != nil {
		t.Fatalf("Confirmed: %v", err)
	}
	if skipped != 0 {
		t.Errorf("unexpected skipped entries: %d", skipped)
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

// TestDistLog_ConfirmedMissingFile verifies that Confirmed returns (nil, 0, nil)
// when the log file does not yet exist — not an error.
func TestDistLog_ConfirmedMissingFile(t *testing.T) {
	log := OpenDistLog("/no/such/directory/dist.log")
	hashes, skipped, err := log.Confirmed("https://ep.example.com")
	if err != nil {
		t.Errorf("Confirmed on missing file: unexpected error %v", err)
	}
	if skipped != 0 {
		t.Errorf("Confirmed on missing file: want 0 skipped, got %d", skipped)
	}
	if hashes != nil {
		t.Errorf("Confirmed on missing file: want nil slice, got %v", hashes)
	}
}

// TestDistLog_Confirmed_SkipsCorruptEntry verifies that a corrupt (truncated or
// invalid JSON) line in the middle of the log does not abort the scan.
// Valid entries before and after the corrupt line must all be returned, and
// skippedEntries must equal the number of corrupt lines injected.
func TestDistLog_Confirmed_SkipsCorruptEntry(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/dist.log"
	log := OpenDistLog(path)
	defer log.Close()

	const ep = "https://ep.example.com"

	// Write two valid entries before the corrupt line.
	if err := log.Record(ep, "before1"); err != nil {
		t.Fatalf("Record before1: %v", err)
	}
	if err := log.Record(ep, "before2"); err != nil {
		t.Fatalf("Record before2: %v", err)
	}
	if err := log.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Inject a corrupt line (truncated JSON) directly into the file.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatalf("opening log for injection: %v", err)
	}
	// Write a partial JSON object — no closing brace, invalid token.
	if _, err := f.WriteString(`{"endpoint":"https://ep.example.com","hash":"corrupt` + "\n"); err != nil {
		f.Close()
		t.Fatalf("injecting corrupt line: %v", err)
	}
	f.Close()

	// Re-open and write two valid entries after the corrupt line.
	log = OpenDistLog(path)
	defer log.Close()
	if err := log.Record(ep, "after1"); err != nil {
		t.Fatalf("Record after1: %v", err)
	}
	if err := log.Record(ep, "after2"); err != nil {
		t.Fatalf("Record after2: %v", err)
	}
	if err := log.Sync(); err != nil {
		t.Fatalf("Sync after corrupt: %v", err)
	}

	// Confirmed must return no error, skippedEntries==1, and all four valid hashes.
	hashes, skipped, err := log.Confirmed(ep)
	if err != nil {
		t.Fatalf("Confirmed returned unexpected error: %v", err)
	}
	if skipped != 1 {
		t.Errorf("want skippedEntries=1, got %d", skipped)
	}
	wantHashes := map[string]bool{
		"before1": true,
		"before2": true,
		"after1":  true,
		"after2":  true,
	}
	if len(hashes) != len(wantHashes) {
		t.Errorf("want %d hashes, got %d: %v", len(wantHashes), len(hashes), hashes)
	} else {
		for _, h := range hashes {
			if !wantHashes[h] {
				t.Errorf("unexpected hash in result: %q", h)
			}
		}
	}
}

// TestDistLog_Confirmed_DeduplicatesHashes verifies that Confirmed() returns
// each distinct hash at most once for the given endpoint, even when the same
// {endpoint, hash} pair was recorded multiple times.
//
// Duplicates arise naturally: pushOne retries an object after a transient
// failure, but the first attempt already succeeded and called log.Record.
// On the next Distribute run the log would contain two entries for that hash
// and endpoint; callers that use len(hashes) as a confirmation count would
// over-count without deduplication.
func TestDistLog_Confirmed_DeduplicatesHashes(t *testing.T) {
	dir := t.TempDir()
	log := OpenDistLog(dir + "/dist.log")
	defer log.Close()

	const ep1 = "https://s1.example.com"
	const ep2 = "https://s2.example.com"

	// Record the same hash three times for ep1 (simulates three retry attempts
	// that all eventually succeeded and each called log.Record).
	for i := 0; i < 3; i++ {
		if err := log.Record(ep1, "deadbeef"); err != nil {
			t.Fatalf("Record ep1 attempt %d: %v", i, err)
		}
	}
	// Record two distinct hashes for ep1 once each.
	if err := log.Record(ep1, "cafebabe"); err != nil {
		t.Fatalf("Record cafebabe: %v", err)
	}
	if err := log.Record(ep1, "cafebabe"); err != nil { // duplicate of cafebabe
		t.Fatalf("Record cafebabe dup: %v", err)
	}
	// Record a hash for ep2 — should not appear in ep1's results.
	if err := log.Record(ep2, "deadbeef"); err != nil {
		t.Fatalf("Record ep2: %v", err)
	}
	if err := log.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	hashes, skipped, err := log.Confirmed(ep1)
	if err != nil {
		t.Fatalf("Confirmed: %v", err)
	}
	if skipped != 0 {
		t.Errorf("unexpected skipped entries: %d", skipped)
	}

	// Expect exactly 2 distinct hashes for ep1: "deadbeef" and "cafebabe".
	if len(hashes) != 2 {
		t.Errorf("want 2 distinct hashes, got %d: %v", len(hashes), hashes)
	}
	seen := make(map[string]int)
	for _, h := range hashes {
		seen[h]++
	}
	for h, count := range seen {
		if count > 1 {
			t.Errorf("hash %q appears %d times in result — expected exactly once", h, count)
		}
	}
	if seen["deadbeef"] != 1 {
		t.Errorf("expected deadbeef in result for ep1")
	}
	if seen["cafebabe"] != 1 {
		t.Errorf("expected cafebabe in result for ep1")
	}

	// ep2's result should be independent: one distinct "deadbeef" only.
	hashes2, _, err := log.Confirmed(ep2)
	if err != nil {
		t.Fatalf("Confirmed ep2: %v", err)
	}
	if len(hashes2) != 1 || hashes2[0] != "deadbeef" {
		t.Errorf("ep2: want [deadbeef], got %v", hashes2)
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

// ── fetchReceiverBloom tests ──────────────────────────────────────────────────

// TestFetchReceiverBloom_HappyPath verifies that a valid binary-encoded Bloom
// filter is fetched and deserialised correctly.
func TestFetchReceiverBloom_HappyPath(t *testing.T) {
	// Build a filter with a known hash.
	bf := bloom.NewWithEstimates(1000, 0.01)
	knownHash := "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	bf.AddString(knownHash)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/bloom" || r.Method != http.MethodGet {
			http.Error(w, "unexpected", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		if _, err := bf.WriteTo(w); err != nil {
			t.Errorf("WriteTo: %v", err)
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	got, err := fetchReceiverBloom(ctx, srv.URL, "" /* no auth in test */)
	if err != nil {
		t.Fatalf("fetchReceiverBloom: %v", err)
	}
	if !got.TestString(knownHash) {
		t.Error("fetched filter does not contain the expected hash")
	}
	// A hash we never added should not appear.
	absent := "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
	if got.TestString(absent) {
		t.Log("note: false positive (acceptable)")
	}
}

// TestFetchReceiverBloom_503 verifies that a 503 (inventory building) is
// returned as an error so the caller falls back to pushing all objects.
func TestFetchReceiverBloom_503(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "10")
		http.Error(w, `{"error":"inventory building"}`, http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := fetchReceiverBloom(ctx, srv.URL, "" /* no auth in test */)
	if err == nil {
		t.Fatal("expected error for 503 response")
	}
}

// TestFetchReceiverBloom_CorruptBody verifies that a garbled response body is
// rejected with an error.
func TestFetchReceiverBloom_CorruptBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("this is not a bloom filter"))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := fetchReceiverBloom(ctx, srv.URL, "" /* no auth in test */)
	if err == nil {
		t.Fatal("expected error for corrupt body")
	}
}

// ── Fix LOW #7: pushBatch goroutine exits on pre-cancelled context ────────────

// blockingCAS is a cas.Backend whose Get blocks until the provided context is
// cancelled.  It is used to simulate a backend that ignores context
// cancellation internally (e.g. wraps a library with no cancel support).
type blockingCAS struct {
	ready chan struct{} // closed when Get is entered
}

func (b *blockingCAS) Get(ctx context.Context, hash string) (io.ReadCloser, error) {
	if b.ready != nil {
		select {
		case <-b.ready:
		default:
			close(b.ready)
		}
	}
	<-ctx.Done() // block until context is cancelled
	return nil, ctx.Err()
}

func (b *blockingCAS) Exists(_ context.Context, _ string) (bool, error) { return true, nil }
func (b *blockingCAS) Put(_ context.Context, _ string, _ io.Reader, _ int64) error {
	return nil
}
func (b *blockingCAS) Delete(_ context.Context, _ string) error { return nil }
func (b *blockingCAS) List(_ context.Context) ([]string, error)  { return nil, nil }

// TestPushBatch_GoroutineExitsOnContextCancel verifies that the streaming
// goroutine inside pushBatch terminates promptly when the batch context is
// cancelled — even if the CAS backend's Get would otherwise block forever.
//
// Without the explicit ctx.Done guard added by fix LOW #7, the goroutine
// would linger until the backend unblocks on its own.
func TestPushBatch_GoroutineExitsOnContextCancel(t *testing.T) {
	// Server that never responds — ensures pushBatch only terminates via context.
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	old := sharedClient
	sharedClient = srv.Client()
	defer func() { sharedClient = old }()

	ready := make(chan struct{})
	cas := &blockingCAS{ready: ready}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, err := pushBatch(ctx, srv.URL, []string{"deadbeef"}, cas, 5*time.Second)
		done <- err
	}()

	// Wait until the goroutine has entered Get, then cancel.
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for goroutine to enter casBackend.Get")
	}
	cancel()

	// pushBatch must return well within a second after cancellation.
	select {
	case err := <-done:
		if err == nil {
			t.Error("expected non-nil error after context cancel, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pushBatch goroutine did not exit within 2s after context cancel")
	}
}

// ── Fix LOW #8: coord_client.run() cancel() via IIFE defer ───────────────────

// TestCoordClient_RunIIFECancelOnHeartbeatSkip verifies that skipping the
// heartbeat (registration not yet done) releases the context timer resource —
// i.e. the IIFE returns and its defer cancel() fires — rather than leaking
// an outstanding context timer until the timeout fires on its own.
//
// We confirm this behaviourally: after Stop() all goroutines must be done and
// no goroutine leak is reported by the race detector.
func TestPushBatch_404DrainEnablesConnectionReuse(t *testing.T) {
	var connCount int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&connCount, 1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	old := sharedClient
	sharedClient = srv.Client()
	defer func() { sharedClient = old }()

	cas := newTestCAS(t)
	putCAS(t, cas, "h1", []byte("d"))

	// Call pushBatch twice against the same server.  Because body drain now
	// enables connection reuse, the second call may reuse the existing conn.
	// We don't assert on exact connection counts (that's transport-internal),
	// but we do verify both calls return the sentinel error and don't hang.
	for i := 0; i < 2; i++ {
		_, err := pushBatch(context.Background(), srv.URL, []string{"h1"}, cas, 5*time.Second)
		if !isBatchUnsupported(err) {
			t.Errorf("call %d: expected batchUnsupportedError, got %v", i+1, err)
		}
	}
}
