package receiver

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"cvmfs.io/prepub/pkg/observe"
	"github.com/bits-and-blooms/bloom/v3"
)

// newAuthTestReceiver returns a receiver with DevMode=false so that
// bloomHandler enforces authentication.  TLS cert/key are only required by
// Start(); handler tests never call Start(), so no cert file is needed.
func newAuthTestReceiver(t *testing.T) *Receiver {
	t.Helper()
	obs, shutdown, err := observe.New("test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)
	r, err := New(Config{
		CASRoot:    t.TempDir(),
		HMACSecret: testSecret,
		DevMode:    false, // enforce auth
		Obs:        obs,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Mark inventory ready so auth failures are the only reason for non-200.
	r.inv.mu.Lock()
	r.inv.ready = true
	r.inv.mu.Unlock()
	return r
}

// TestBloomHandler_NotReady verifies that the bloom endpoint returns 503 when
// the inventory has not yet finished its CAS walk.
func TestBloomHandler_NotReady(t *testing.T) {
	r, _ := newTestReceiver(t)
	// The inventory is freshly created and isReady() == false by construction.

	req := httptest.NewRequest(http.MethodGet, "/api/v1/bloom", nil)
	rr := httptest.NewRecorder()
	r.bloomHandler(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
	if got := rr.Header().Get("Retry-After"); got != "10" {
		t.Errorf("expected Retry-After: 10, got %q", got)
	}
}

// TestBloomHandler_Ready verifies that the bloom endpoint returns 200 with a
// parseable Bloom filter once the inventory is ready.
func TestBloomHandler_Ready(t *testing.T) {
	r, _ := newTestReceiver(t)

	// Manually mark the inventory as ready and add a known hash.
	h := makeHash(7)
	r.inv.add(h)
	r.inv.mu.Lock()
	r.inv.ready = true
	r.inv.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/bloom", nil)
	rr := httptest.NewRecorder()
	r.bloomHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/octet-stream" {
		t.Errorf("expected Content-Type application/octet-stream, got %q", ct)
	}

	// Deserialise the response body as a Bloom filter and verify the hash is present.
	bf := new(bloom.BloomFilter)
	if _, err := bf.ReadFrom(rr.Body); err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if !bf.TestString(h) {
		t.Errorf("deserialised filter does not contain hash %s", h[:8]+"…")
	}
	// A hash we never added should not be present (with very high probability).
	if bf.TestString(makeHash(999)) {
		t.Log("note: false positive for hash 999 (acceptable)")
	}
}

// TestBloomHandler_MethodNotAllowed verifies that non-GET requests are rejected.
func TestBloomHandler_MethodNotAllowed(t *testing.T) {
	r, _ := newTestReceiver(t)
	r.inv.mu.Lock()
	r.inv.ready = true
	r.inv.mu.Unlock()

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		req := httptest.NewRequest(method, "/api/v1/bloom", nil)
		rr := httptest.NewRecorder()
		r.bloomHandler(rr, req)
		if rr.Code != http.StatusMethodNotAllowed {
			t.Errorf("method %s: expected 405, got %d", method, rr.Code)
		}
	}
}

// ── Fix #6: bloom endpoint authentication ─────────────────────────────────────

// TestBloomHandler_RequiresAuth verifies that GET /api/v1/bloom returns 401
// when no Authorization header is present and DevMode=false.
func TestBloomHandler_RequiresAuth(t *testing.T) {
	r := newAuthTestReceiver(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/bloom", nil)
	rr := httptest.NewRecorder()
	r.bloomHandler(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 with no auth, got %d", rr.Code)
	}
}

// TestBloomHandler_WrongToken verifies that a wrong bloom-read token returns 401.
func TestBloomHandler_WrongToken(t *testing.T) {
	r := newAuthTestReceiver(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/bloom", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rr := httptest.NewRecorder()
	r.bloomHandler(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 with wrong token, got %d", rr.Code)
	}
}

// TestBloomHandler_CorrectToken verifies that the correct derived bloom-read
// token is accepted and returns 200.
func TestBloomHandler_CorrectToken(t *testing.T) {
	r := newAuthTestReceiver(t)

	token := bloomReadToken(testSecret)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/bloom", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rr := httptest.NewRecorder()
	r.bloomHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 with correct token, got %d: %s", rr.Code, rr.Body.String())
	}
}

// TestBloomHandler_DevModeBypassesAuth verifies that DevMode=true skips the
// token check (existing DevMode tests continue to work unchanged).
func TestBloomHandler_DevModeBypassesAuth(t *testing.T) {
	r, _ := newTestReceiver(t) // DevMode=true
	r.inv.mu.Lock()
	r.inv.ready = true
	r.inv.mu.Unlock()

	// No Authorization header — should still succeed in DevMode.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/bloom", nil)
	rr := httptest.NewRecorder()
	r.bloomHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("DevMode: expected 200 without auth, got %d", rr.Code)
	}
}

// TestBloomReadToken_Deterministic verifies that bloomReadToken is a pure
// function of the secret — calling it twice with the same input returns the
// same token.
func TestBloomReadToken_Deterministic(t *testing.T) {
	a := bloomReadToken("secret-a")
	b := bloomReadToken("secret-a")
	c := bloomReadToken("secret-b")
	if a != b {
		t.Errorf("bloomReadToken not deterministic: %q != %q", a, b)
	}
	if a == c {
		t.Error("different secrets should produce different tokens")
	}
}

// TestBloomHandler_ReflectsSubsequentAdds verifies that objects added after
// isReady() become visible in subsequent bloom endpoint responses.
func TestBloomHandler_ReflectsSubsequentAdds(t *testing.T) {
	r, _ := newTestReceiver(t)

	// Mark ready with an empty filter.
	r.inv.mu.Lock()
	r.inv.ready = true
	r.inv.mu.Unlock()

	// First request — filter is empty.
	req1 := httptest.NewRequest(http.MethodGet, "/api/v1/bloom", nil)
	rr1 := httptest.NewRecorder()
	r.bloomHandler(rr1, req1)
	if rr1.Code != http.StatusOK {
		t.Fatalf("first request: expected 200, got %d", rr1.Code)
	}
	bf1 := new(bloom.BloomFilter)
	if _, err := bf1.ReadFrom(rr1.Body); err != nil {
		t.Fatalf("ReadFrom (first): %v", err)
	}
	h := makeHash(42)
	if bf1.TestString(h) {
		t.Log("note: false positive on empty filter (acceptable)")
	}

	// Add the hash, then request again.
	r.inv.add(h)

	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/bloom", nil)
	rr2 := httptest.NewRecorder()
	r.bloomHandler(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("second request: expected 200, got %d", rr2.Code)
	}
	bf2 := new(bloom.BloomFilter)
	if _, err := bf2.ReadFrom(rr2.Body); err != nil {
		t.Fatalf("ReadFrom (second): %v", err)
	}
	if !bf2.TestString(h) {
		t.Error("second filter should contain the hash added between requests")
	}
}
