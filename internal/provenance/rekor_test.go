package provenance

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestParseRekorConflict_UUIDFromLocation verifies that the Location header
// is used to extract the real entry UUID on a 409 response (Fix #6).
func TestParseRekorConflict_UUIDFromLocation(t *testing.T) {
	body, _ := json.Marshal(rekorResponseEntry{
		LogIndex:       42,
		IntegratedTime: 1700000000,
		Verification: rekorVerification{
			SignedEntryTimestamp: "base64SET==",
		},
	})

	location := "/api/v1/log/entries/abc123def456"
	uuid, logIndex, intTime, set, err := parseRekorConflict(body, location)
	if err != nil {
		t.Fatalf("parseRekorConflict: %v", err)
	}
	if uuid != "abc123def456" {
		t.Errorf("uuid: want %q, got %q", "abc123def456", uuid)
	}
	if logIndex != 42 {
		t.Errorf("logIndex: want 42, got %d", logIndex)
	}
	if intTime != 1700000000 {
		t.Errorf("integratedTime: want 1700000000, got %d", intTime)
	}
	if set != "base64SET==" {
		t.Errorf("SET: want %q, got %q", "base64SET==", set)
	}
}

// TestParseRekorConflict_FallbackSentinel verifies the "duplicate" sentinel is
// used when the Location header is absent.
func TestParseRekorConflict_FallbackSentinel(t *testing.T) {
	body, _ := json.Marshal(rekorResponseEntry{LogIndex: 7})

	uuid, logIndex, _, _, err := parseRekorConflict(body, "" /*no Location*/)
	if err != nil {
		t.Fatalf("parseRekorConflict: %v", err)
	}
	if uuid != "duplicate" {
		t.Errorf("uuid: want %q (sentinel), got %q", "duplicate", uuid)
	}
	if logIndex != 7 {
		t.Errorf("logIndex: want 7, got %d", logIndex)
	}
}

// TestParseRekorConflict_BadBodyWithLocation verifies that a UUID is still
// returned when the body cannot be parsed but Location is present.
func TestParseRekorConflict_BadBodyWithLocation(t *testing.T) {
	uuid, _, _, _, err := parseRekorConflict([]byte("not json"), "/api/v1/log/entries/realuuid")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if uuid != "realuuid" {
		t.Errorf("uuid: want %q, got %q", "realuuid", uuid)
	}
}

// TestParseRekorConflict_BadBodyNoLocation verifies error is returned when
// both Location and body are unusable.
func TestParseRekorConflict_BadBodyNoLocation(t *testing.T) {
	_, _, _, _, err := parseRekorConflict([]byte("not json"), "")
	if err == nil {
		t.Error("expected error when body is invalid JSON and Location is absent")
	}
}

// ── Fix #11: URL construction uses url.Values encoding ────────────────────────

// TestSearchRekor_URLEncoding verifies that the sha256 hash is transmitted as
// a properly parsed query parameter rather than raw string concatenation,
// and that the server receives the correct "hash" value (Fix #11).
func TestSearchRekor_URLEncoding(t *testing.T) {
	var gotHash string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHash = r.URL.Query().Get("hash")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	hash := "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	_, _ = SearchRekor(context.Background(), srv.URL, hash, 5*time.Second)

	want := "sha256:" + hash
	if gotHash != want {
		t.Errorf("query param 'hash': want %q, got %q", want, gotHash)
	}
}
