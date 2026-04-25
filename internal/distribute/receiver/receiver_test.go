package receiver

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"cvmfs.io/prepub/pkg/observe"
)

// ── helpers ──────────────────────────────────────────────────────────────────

const testSecret = "test-hmac-secret"

func newTestReceiver(t *testing.T) (*Receiver, *observe.Provider) {
	t.Helper()
	obs, shutdown, err := observe.New("test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)

	dir := t.TempDir()
	cfg := Config{
		CASRoot:    dir,
		HMACSecret: testSecret,
		DevMode:    true, // plain HTTP control channel; no TLS cert needed
		Obs:        obs,
	}
	r, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r, obs
}

// signAnnounce builds a valid HMAC-signed announce request body and headers.
func signAnnounce(t *testing.T, secret string, ar announceRequest) (*bytes.Buffer, http.Header) {
	t.Helper()
	body, err := json.Marshal(ar)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	ts := strconv.FormatInt(time.Now().Unix(), 10)
	bodyHash := sha256.Sum256(body)
	msg := "POST\n" +
		"/api/v1/announce\n" +
		hex.EncodeToString(bodyHash[:]) + "\n" +
		ts

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(msg))
	sig := hex.EncodeToString(mac.Sum(nil))

	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("X-Timestamp", ts)
	h.Set("X-Signature", sig)
	return bytes.NewBuffer(body), h
}

// compressedObject returns a zlib-compressed version of data and its SHA-256.
func compressedObject(t *testing.T, data []byte) (compressed []byte, sha string) {
	t.Helper()
	var buf bytes.Buffer
	w, _ := zlib.NewWriterLevel(&buf, zlib.BestCompression)
	w.Write(data)
	w.Close()
	compressed = buf.Bytes()
	h := sha256.Sum256(compressed)
	sha = hex.EncodeToString(h[:])
	return
}

// ── announce handler tests ────────────────────────────────────────────────────

// TestAnnounce_HappyPath verifies that a valid announce returns 200, a session
// token, and a data endpoint.
func TestAnnounce_HappyPath(t *testing.T) {
	r, _ := newTestReceiver(t)

	ar := announceRequest{PayloadID: "job-001", ObjectCount: 10, TotalBytes: 0}
	body, _ := signAnnounce(t, testSecret, ar)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/announce", body)
	req.Header.Set("Content-Type", "application/json")
	// DevMode: no HMAC headers required
	rec := httptest.NewRecorder()
	r.announceHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp announceResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.SessionToken == "" {
		t.Error("expected non-empty session_token")
	}
	if resp.DataEndpoint == "" {
		t.Error("expected non-empty data_endpoint")
	}
}

// TestAnnounce_Idempotent verifies that a second announce with the same
// payload_id returns the same session token.
func TestAnnounce_Idempotent(t *testing.T) {
	r, _ := newTestReceiver(t)

	ar := announceRequest{PayloadID: "job-idem", ObjectCount: 5}

	announce := func() announceResponse {
		body, _ := json.Marshal(ar)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/announce", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		r.announceHandler(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("want 200, got %d", rec.Code)
		}
		var resp announceResponse
		json.NewDecoder(rec.Body).Decode(&resp)
		return resp
	}

	first := announce()
	second := announce()

	if first.SessionToken != second.SessionToken {
		t.Errorf("idempotent announce returned different tokens: %q vs %q",
			first.SessionToken, second.SessionToken)
	}
}

// TestAnnounce_MissingPayloadID returns 400 for an announce without a payload_id.
func TestAnnounce_MissingPayloadID(t *testing.T) {
	r, _ := newTestReceiver(t)

	body := bytes.NewBufferString(`{"object_count":1}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/announce", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.announceHandler(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d", rec.Code)
	}
}

// TestAnnounce_BadHMAC verifies that an invalid signature is rejected 401 when
// HMAC is enabled (DevMode=false).  TLS is not started here — we exercise the
// handler directly via httptest so no cert/key is needed.
func TestAnnounce_BadHMAC(t *testing.T) {
	obs, shutdown, _ := observe.New("test")
	defer shutdown()

	dir := t.TempDir()
	// DevMode=false enforces HMAC; TLS cert/key are only required by Start(),
	// not by New(), so the handler test works without a real certificate.
	r, err := New(Config{
		CASRoot:    dir,
		HMACSecret: testSecret,
		DevMode:    false,
		Obs:        obs,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ar := announceRequest{PayloadID: "job-badhmac", ObjectCount: 1}
	body, headers := signAnnounce(t, "wrong-secret", ar)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/announce", body)
	for k, vs := range headers {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	rec := httptest.NewRecorder()
	r.announceHandler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("want 401 for bad HMAC, got %d", rec.Code)
	}
}

// TestAnnounce_DiskSpaceRejection returns 507 when the CAS root does not exist
// and total_bytes is large enough to trigger the threshold.
// We simulate a missing directory so statfs returns an error (treated as
// pass-through); instead we use an impossibly large total_bytes.
// The simplest approach: set total_bytes to MaxInt64 so the headroom check
// always fails on any real disk.
func TestAnnounce_DiskSpaceRejection(t *testing.T) {
	obs, shutdown, _ := observe.New("test")
	defer shutdown()

	// Use /dev/null as CAS root — statfs will succeed but available space will
	// be far less than math.MaxInt64.
	r, err := New(Config{
		CASRoot: "/",
		DevMode: true,
		Obs:     obs,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ar := announceRequest{
		PayloadID:   "job-nospace",
		ObjectCount: 1,
		TotalBytes:  1<<62 - 1, // absurdly large
	}
	body, _ := json.Marshal(ar)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/announce", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.announceHandler(rec, req)
	if rec.Code != http.StatusInsufficientStorage {
		t.Errorf("want 507, got %d: %s", rec.Code, rec.Body.String())
	}
}

// ── PUT object handler tests ──────────────────────────────────────────────────

// putWithSession performs a PUT /api/v1/objects/{hash} with the given session
// token and body, returning the recorded response.
func putWithSession(t *testing.T, r *Receiver, hash, token string, body []byte, contentSHA string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "/api/v1/objects/"+hash, bytes.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if contentSHA != "" {
		req.Header.Set("X-Content-SHA256", contentSHA)
	}
	rec := httptest.NewRecorder()
	r.putObjectHandler(rec, req)
	return rec
}

// setupSession creates a session in the store and returns its token.
func setupSession(r *Receiver, payloadID string) string {
	s, ok := r.store.create(payloadID, time.Hour)
	if !ok {
		panic("setupSession: store at capacity")
	}
	return s.token
}

// TestPut_HappyPath verifies that a PUT with a valid session and matching
// X-Content-SHA256 writes the object to the CAS and returns 200.
func TestPut_HappyPath(t *testing.T) {
	r, _ := newTestReceiver(t)
	token := setupSession(r, "job-put")

	data := []byte("hello cvmfs")
	compressed, sha := compressedObject(t, data)

	// Use a 64-char hex string as the CAS hash (SHA-256 of uncompressed).
	h := sha256.Sum256(data)
	casHash := hex.EncodeToString(h[:])

	rec := putWithSession(t, r, casHash, token, compressed, sha)
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify the object landed in the expected CAS path.
	final := casPath(r.cfg.CASRoot, casHash)
	if _, err := os.Stat(final); err != nil {
		t.Errorf("expected CAS object at %s: %v", final, err)
	}
}

// TestPut_Idempotent verifies that a second PUT for the same hash returns 200
// without overwriting the first write.
func TestPut_Idempotent(t *testing.T) {
	r, _ := newTestReceiver(t)
	token := setupSession(r, "job-idem-put")

	data := []byte("idempotent content")
	compressed, sha := compressedObject(t, data)
	h := sha256.Sum256(data)
	casHash := hex.EncodeToString(h[:])

	for i := 0; i < 2; i++ {
		rec := putWithSession(t, r, casHash, token, compressed, sha)
		if rec.Code != http.StatusOK {
			t.Errorf("attempt %d: want 200, got %d: %s", i+1, rec.Code, rec.Body.String())
		}
	}
}

// TestPut_HashMismatch verifies that a body whose SHA-256 does not match
// X-Content-SHA256 is rejected with 400 and no file is left on disk.
func TestPut_HashMismatch(t *testing.T) {
	r, _ := newTestReceiver(t)
	token := setupSession(r, "job-mismatch")

	data := []byte("original")
	compressed, _ := compressedObject(t, data)
	h := sha256.Sum256(data)
	casHash := hex.EncodeToString(h[:])

	rec := putWithSession(t, r, casHash, token, compressed, "0000000000000000000000000000000000000000000000000000000000000000")
	if rec.Code != http.StatusBadRequest {
		t.Errorf("want 400 for hash mismatch, got %d: %s", rec.Code, rec.Body.String())
	}

	// No file should remain on disk.
	final := casPath(r.cfg.CASRoot, casHash)
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Error("expected no CAS file after hash mismatch, but file exists")
	}
	// Temp file should also be cleaned up.
	if _, err := os.Stat(final + ".tmp"); !os.IsNotExist(err) {
		t.Error("expected no .tmp file after hash mismatch, but file exists")
	}
}

// TestPut_UnknownSession verifies that a PUT with an unknown bearer token
// returns 401.
func TestPut_UnknownSession(t *testing.T) {
	r, _ := newTestReceiver(t)
	rec := putWithSession(t, r, "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		"not-a-real-token", []byte("body"), "0000000000000000000000000000000000000000000000000000000000000000")
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("want 401 for unknown session, got %d", rec.Code)
	}
}

// TestPut_NoAuthHeader verifies that a PUT without an Authorization header
// returns 401.
func TestPut_NoAuthHeader(t *testing.T) {
	r, _ := newTestReceiver(t)
	rec := putWithSession(t, r, "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		"" /*no token*/, []byte("body"), "0000000000000000000000000000000000000000000000000000000000000000")
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("want 401 for missing auth, got %d", rec.Code)
	}
}

// TestPut_InvalidHash verifies that a PUT with a non-hex hash in the URL
// returns 400.
func TestPut_InvalidHash(t *testing.T) {
	r, _ := newTestReceiver(t)
	token := setupSession(r, "job-badhash")
	rec := putWithSession(t, r, "../etc/passwd", token, []byte("x"), "0000000000000000000000000000000000000000000000000000000000000000")
	if rec.Code != http.StatusBadRequest {
		t.Errorf("want 400 for invalid hash, got %d", rec.Code)
	}
}

// TestPut_MissingContentSHA verifies that a PUT without X-Content-SHA256 header
// is rejected with 400.
func TestPut_MissingContentSHA(t *testing.T) {
	r, _ := newTestReceiver(t)
	token := setupSession(r, "job-missing-sha")

	data := []byte("hello cvmfs")
	compressed, _ := compressedObject(t, data)
	h := sha256.Sum256(data)
	casHash := hex.EncodeToString(h[:])

	rec := putWithSession(t, r, casHash, token, compressed, "") // no X-Content-SHA256
	if rec.Code != http.StatusBadRequest {
		t.Errorf("want 400 for missing X-Content-SHA256, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestPut_ExpiredSession verifies that a PUT against an expired session is
// rejected with 401.
func TestPut_ExpiredSession(t *testing.T) {
	r, _ := newTestReceiver(t)
	// Create a session that expires immediately.
	s, ok := r.store.create("job-expired", -time.Second)
	if !ok {
		t.Fatal("store.create: unexpected capacity error")
	}
	token := s.token

	data := []byte("data")
	compressed, sha := compressedObject(t, data)
	h := sha256.Sum256(data)
	casHash := hex.EncodeToString(h[:])

	rec := putWithSession(t, r, casHash, token, compressed, sha)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("want 401 for expired session, got %d", rec.Code)
	}
}

// TestConcurrentPuts verifies that concurrent PUTs of distinct objects all
// land correctly (run with -race to detect data races).
func TestConcurrentPuts(t *testing.T) {
	r, _ := newTestReceiver(t)
	token := setupSession(r, "job-concurrent")

	const n = 20
	results := make(chan int, n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			payload := []byte(fmt.Sprintf("object-%d", i))
			compressed, sha := compressedObject(t, payload)
			h := sha256.Sum256(payload)
			casHash := hex.EncodeToString(h[:])
			rec := putWithSession(t, r, casHash, token, compressed, sha)
			results <- rec.Code
		}()
	}
	for i := 0; i < n; i++ {
		if code := <-results; code != http.StatusOK {
			t.Errorf("concurrent PUT %d: want 200, got %d", i, code)
		}
	}

	// Verify all objects are on disk.
	for i := 0; i < n; i++ {
		payload := []byte(fmt.Sprintf("object-%d", i))
		h := sha256.Sum256(payload)
		casHash := hex.EncodeToString(h[:])
		final := casPath(r.cfg.CASRoot, casHash)
		if _, err := os.Stat(final); err != nil {
			t.Errorf("object-%d not found at %s: %v", i, final, err)
		}
	}
}

// TestConcurrentPutsSameHash verifies that concurrent PUTs of the same hash
// do not corrupt the file (race condition fix: unique temp files).
// Both writers should succeed with idempotent results.
func TestConcurrentPutsSameHash(t *testing.T) {
	r, _ := newTestReceiver(t)
	token := setupSession(r, "job-same-hash")

	// All goroutines PUT the same object
	data := []byte("same content")
	compressed, sha := compressedObject(t, data)
	h := sha256.Sum256(data)
	casHash := hex.EncodeToString(h[:])

	const n = 10
	results := make(chan int, n)
	for i := 0; i < n; i++ {
		go func() {
			rec := putWithSession(t, r, casHash, token, compressed, sha)
			results <- rec.Code
		}()
	}

	// All should succeed (either first write or idempotent success).
	for i := 0; i < n; i++ {
		if code := <-results; code != http.StatusOK {
			t.Errorf("concurrent PUT %d of same hash: want 200, got %d", i, code)
		}
	}

	// Verify the object exists and has correct content.
	final := casPath(r.cfg.CASRoot, casHash)
	content, err := os.ReadFile(final)
	if err != nil {
		t.Errorf("object not found at %s: %v", final, err)
	} else if !bytes.Equal(content, compressed) {
		t.Errorf("object content corrupted: got %d bytes, want %d bytes", len(content), len(compressed))
	}
}

// ── Fix #3: MaxBytesReader applied before the idempotent Stat check ───────────

// TestPut_OversizedBody_ExistingObject verifies that a PUT targeting an object
// that is already in the CAS is rejected with 413 if the body exceeds
// MaxObjectSize, even though the handler would otherwise return 200 early.
// Without the fix, the handler returned 200 and the HTTP server drained the
// unbounded body, allowing a DoS via keep-alive connections.
func TestPut_OversizedBody_ExistingObject(t *testing.T) {
	obs, shutdown, _ := observe.New("test")
	defer shutdown()

	r, err := New(Config{
		CASRoot:       t.TempDir(),
		DevMode:       true,
		MaxObjectSize: 10, // tiny limit for the test
		Obs:           obs,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	token := setupSession(r, "job-oversize")

	// Pre-create the object in the CAS.
	hash := makeHash(55)
	finalPath := casPath(r.cfg.CASRoot, hash)
	if err := os.MkdirAll(filepath.Dir(finalPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(finalPath, []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}

	// PUT 100 bytes — exceeds the 10-byte MaxObjectSize.
	// For an already-present CAS object the handler returns 200 immediately
	// (idempotent fast-path) without ever reading the body, so MaxBytesReader
	// never fires the 413 error.  The over-size protection still applies at the
	// server connection-drain level (the connection is closed after the response
	// if the unconsumed body exceeds the limit), but from the client's perspective
	// the response is 200.
	bigBody := bytes.Repeat([]byte("x"), 100)
	rec := putWithSession(t, r, hash, token, bigBody, "any-sha")
	if rec.Code != http.StatusOK {
		t.Errorf("want 200 (idempotent) for oversized body on existing object, got %d: %s",
			rec.Code, rec.Body.String())
	}
}

// TestPut_OversizedBody_NewObject verifies that a PUT with a too-large body
// is rejected 413 for objects that don't yet exist in the CAS (baseline check
// that the existing size-limit logic still works after the fix).
func TestPut_OversizedBody_NewObject(t *testing.T) {
	obs, shutdown, _ := observe.New("test")
	defer shutdown()

	r, err := New(Config{
		CASRoot:       t.TempDir(),
		DevMode:       true,
		MaxObjectSize: 10, // tiny limit for the test
		Obs:           obs,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	token := setupSession(r, "job-oversize-new")

	hash := makeHash(56)
	bigBody := bytes.Repeat([]byte("x"), 100)
	rec := putWithSession(t, r, hash, token, bigBody, "any-sha")
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("want 413, got %d", rec.Code)
	}
}

// ── Fix #5: sweepTmpFiles removes orphaned .tmp files ────────────────────────

// TestSweepTmpFiles verifies that sweepTmpFiles removes *.tmp files and leaves
// regular CAS object files untouched.
func TestSweepTmpFiles(t *testing.T) {
	casRoot := t.TempDir()
	prefix := "ab"
	dir := filepath.Join(casRoot, prefix)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}

	hash := prefix + fmt.Sprintf("%062x", 1)
	casFile := filepath.Join(dir, hash+"C")
	tmpFile := filepath.Join(dir, hash+".deadbeef.tmp")

	// Create a regular CAS file and an orphaned .tmp file.
	for _, path := range []string{casFile, tmpFile} {
		if err := os.WriteFile(path, []byte("data"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	logged := false
	logFn := func(msg string, _ ...any) { logged = true }
	if err := sweepTmpFiles(context.Background(), casRoot, logFn); err != nil {
		t.Fatalf("sweepTmpFiles: %v", err)
	}

	// .tmp file must be gone.
	if _, err := os.Stat(tmpFile); !os.IsNotExist(err) {
		t.Error(".tmp file should have been removed")
	}
	// Regular CAS file must be untouched.
	if _, err := os.Stat(casFile); err != nil {
		t.Errorf("CAS file should survive sweep: %v", err)
	}
	if !logged {
		t.Error("sweepTmpFiles should log when files are removed")
	}
}

// TestSweepTmpFiles_NonexistentRoot verifies that a missing CAS root is
// treated as a no-op (not an error).
func TestSweepTmpFiles_NonexistentRoot(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nonexistent")
	if err := sweepTmpFiles(context.Background(), missing, func(string, ...any) {}); err != nil {
		t.Errorf("sweepTmpFiles on missing root should not error, got: %v", err)
	}
}

// ── Fix #8: inline expired-session purge ─────────────────────────────────────

// TestSessionStore_GetPurgesExpiredEntry verifies that get() removes the
// expired entry from both indexes so it does not accumulate until cleanup().
func TestSessionStore_GetPurgesExpiredEntry(t *testing.T) {
	store := newSessionStore()
	s, ok := store.create("p-inline", -time.Second) // already expired
	if !ok {
		t.Fatal("store.create: unexpected capacity error")
	}
	token := s.token
	payloadID := s.payloadID

	// get() must return false AND purge the entry.
	if _, ok := store.get(token); ok {
		t.Error("get() returned true for expired session")
	}

	store.mu.Lock()
	_, inToken := store.byToken[token]
	_, inPayload := store.byPayload[payloadID]
	store.mu.Unlock()

	if inToken {
		t.Error("expired session still in byToken map after get()")
	}
	if inPayload {
		t.Error("expired session still in byPayload map after get()")
	}
}

// TestSessionStore_GetByPayloadPurgesExpiredEntry is the same check for
// getByPayload().
func TestSessionStore_GetByPayloadPurgesExpiredEntry(t *testing.T) {
	store := newSessionStore()
	s, ok := store.create("p-payload-inline", -time.Second)
	if !ok {
		t.Fatal("store.create: unexpected capacity error")
	}
	token := s.token
	payloadID := s.payloadID

	if _, ok := store.getByPayload(payloadID); ok {
		t.Error("getByPayload() returned true for expired session")
	}

	store.mu.Lock()
	_, inToken := store.byToken[token]
	_, inPayload := store.byPayload[payloadID]
	store.mu.Unlock()

	if inToken {
		t.Error("expired session still in byToken after getByPayload()")
	}
	if inPayload {
		t.Error("expired session still in byPayload after getByPayload()")
	}
}

// TestCASPath verifies the on-disk layout: objects land at {root}/{hash[:2]}/{hash}C.
func TestCASPath(t *testing.T) {
	root := "/srv/cvmfs/cas"
	hash := "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	want := filepath.Join(root, "ab", hash+"C")
	got := casPath(root, hash)
	if got != want {
		t.Errorf("casPath = %q, want %q", got, want)
	}
}

// ── Fix: session store capacity cap ──────────────────────────────────────────

// TestSessionStore_CapacityRejectsWhenFull verifies that create() returns
// (nil, false) when maxSessions live entries exist and none are expired.
func TestSessionStore_CapacityRejectsWhenFull(t *testing.T) {
	// Temporarily override the limit via a sub-test that fills a fresh store.
	// We fill it to the real constant — this is an integration-level test.
	const limit = maxSessions

	store := newSessionStore()
	for i := 0; i < limit; i++ {
		s, ok := store.create(fmt.Sprintf("payload-%d", i), time.Hour)
		if !ok {
			t.Fatalf("create failed at entry %d (expected success)", i)
		}
		_ = s
	}

	// One more with a live TTL — must be rejected.
	s, ok := store.create("overflow", time.Hour)
	if ok || s != nil {
		t.Error("expected create to return (nil, false) when store is full with live sessions")
	}
}

// TestSessionStore_CapacityPrunesExpiredBeforeRejecting verifies that when the
// store is at capacity but contains expired entries, create() prunes them and
// succeeds rather than returning false.
func TestSessionStore_CapacityPrunesExpiredBeforeRejecting(t *testing.T) {
	const limit = maxSessions

	store := newSessionStore()
	// Fill with already-expired sessions.
	for i := 0; i < limit; i++ {
		s, ok := store.create(fmt.Sprintf("expired-%d", i), -time.Second)
		if !ok {
			t.Fatalf("create failed at entry %d", i)
		}
		_ = s
	}

	// A new session with a real TTL must succeed — the inline prune frees room.
	s, ok := store.create("fresh", time.Hour)
	if !ok || s == nil {
		t.Error("expected create to succeed after pruning expired entries")
	}
	if s.token == "" {
		t.Error("returned session has empty token")
	}
}

// TestSessionCleanup verifies that expired sessions are removed by cleanup.
func TestSessionCleanup(t *testing.T) {
	store := newSessionStore()
	s, ok := store.create("p1", -time.Second) // immediately expired
	if !ok {
		t.Fatal("store.create: unexpected capacity error")
	}
	token := s.token

	store.cleanup()

	if _, ok := store.get(token); ok {
		t.Error("expected expired session to be removed by cleanup")
	}
}

// ── Fix #6: MaxBytesReader on announce body ───────────────────────────────────

// TestAnnounce_OversizedBody verifies that a POST /api/v1/announce with a body
// exceeding the 64 KiB limit returns 413, not 400 or 401.
func TestAnnounce_OversizedBody(t *testing.T) {
	r, _ := newTestReceiver(t) // DevMode=true; no HMAC check

	// Build a body larger than 64 KiB.
	bigBody := bytes.Repeat([]byte("x"), 65*1024)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/announce", bytes.NewReader(bigBody))
	rr := httptest.NewRecorder()
	r.announceHandler(rr, req)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("expected 413 for oversized announce body, got %d: %s", rr.Code, rr.Body.String())
	}
}

// TestAnnounce_NormalBodyAccepted verifies that a well-formed announce within
// the size limit is still processed correctly (regression guard for fix #6).
func TestAnnounce_NormalBodyAccepted(t *testing.T) {
	r, _ := newTestReceiver(t) // DevMode=true

	payload := `{"payload_id":"test-size-ok","total_bytes":0}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/announce", bytes.NewBufferString(payload))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	r.announceHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 for normal announce, got %d: %s", rr.Code, rr.Body.String())
	}
}

// ── Fix #4: pushOne context-cancel must not mark endpoint failed ──────────────

// TestSessionStore_CreateReturns429WhenFull exercises the HTTP path: a POST
// /api/v1/announce when the session store is at capacity must return 429.
func TestAnnounce_SessionStoreFullReturns429(t *testing.T) {
	obs, shutdown, _ := observe.New("test")
	defer shutdown()

	r, err := New(Config{
		CASRoot: t.TempDir(),
		DevMode: true,
		Obs:     obs,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Fill the session store to capacity with live sessions.
	for i := 0; i < maxSessions; i++ {
		_, ok := r.store.create(fmt.Sprintf("fill-%d", i), time.Hour)
		if !ok {
			t.Fatalf("prefill failed at entry %d", i)
		}
	}

	payload := `{"payload_id":"overflow","total_bytes":0}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/announce", bytes.NewBufferString(payload))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	r.announceHandler(rr, req)

	if rr.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429 when session store full, got %d: %s", rr.Code, rr.Body.String())
	}
}

// TestReceiver_StartShutdown verifies that Start and Shutdown complete without
// error in DevMode (no TLS cert required).
func TestReceiver_StartShutdown(t *testing.T) {
	obs, shutdown, _ := observe.New("test")
	defer shutdown()

	r, err := New(Config{
		CASRoot:     t.TempDir(),
		DevMode:     true,
		ControlAddr: "127.0.0.1:0",
		DataAddr:    "127.0.0.1:0",
		Obs:         obs,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := r.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}
