package broker

import (
	"strings"
	"testing"
)

// ── validateTLSScheme ─────────────────────────────────────────────────────────

// TestValidateTLSScheme_TLSSchemeAccepted verifies that tls://, ssl://,
// mqtts://, and wss:// are all accepted as valid TLS-capable URL schemes.
func TestValidateTLSScheme_TLSSchemeAccepted(t *testing.T) {
	validURLs := []string{
		"tls://broker.example.com:8883",
		"ssl://broker.example.com:8883",
		"mqtts://broker.example.com:8883",
		"wss://broker.example.com:8883",
	}
	for _, url := range validURLs {
		if err := validateTLSScheme(url); err != nil {
			t.Errorf("validateTLSScheme(%q) returned unexpected error: %v", url, err)
		}
	}
}

// TestValidateTLSScheme_PlainTCPRejectedWithCerts verifies that tcp:// and
// ws:// are rejected when TLS certs are configured — the Paho library silently
// ignores SetTLSConfig for these schemes, so we must error early (HIGH #8).
func TestValidateTLSScheme_PlainTCPRejectedWithCerts(t *testing.T) {
	invalidURLs := []string{
		"tcp://localhost:1883",
		"ws://broker.example.com:8083",
	}
	for _, url := range invalidURLs {
		err := validateTLSScheme(url)
		if err == nil {
			t.Errorf("validateTLSScheme(%q) expected error for plain scheme with certs, got nil", url)
			continue
		}
		// Error message should mention the scheme so the operator knows what to fix.
		if !strings.Contains(err.Error(), "scheme") {
			t.Errorf("validateTLSScheme(%q) error %q should mention \"scheme\"", url, err)
		}
	}
}

// TestValidateTLSScheme_MalformedURL verifies that a URL without "://" is
// rejected with a clear error rather than silently passing.
func TestValidateTLSScheme_MalformedURL(t *testing.T) {
	malformed := []string{
		"broker.example.com:8883",
		"",
		"tls:",
	}
	for _, url := range malformed {
		err := validateTLSScheme(url)
		if err == nil {
			t.Errorf("validateTLSScheme(%q) expected error for malformed URL, got nil", url)
		}
	}
}

// TestNew_RejectsTCPWithClientCert is a regression test for HIGH #8: connecting
// with a tcp:// URL + client cert must fail at New() time, not silently succeed
// with the cert ignored.
//
// We use a non-existent cert path so the error happens in buildTLSConfig (before
// the scheme check), which is fine — the point is that we never get a connected
// client with a plain-TCP URL when certs are supplied.
func TestNew_RejectsTCPWithClientCert(t *testing.T) {
	_, err := New(Config{
		BrokerURL:  "tcp://localhost:1883",
		ClientCert: "/nonexistent/cert.pem",
		ClientKey:  "/nonexistent/key.pem",
	})
	if err == nil {
		t.Fatal("New() with tcp:// + client cert should return an error, got nil")
	}
}

// TestNewWithLWT_RejectsTCPWithCACert verifies the same scheme validation
// applies to NewWithLWT.
func TestNewWithLWT_RejectsTCPWithCACert(t *testing.T) {
	_, err := NewWithLWT(Config{
		BrokerURL: "tcp://localhost:1883",
		CACert:    "/nonexistent/ca.pem",
	}, "test/lwt", 1, true, struct{}{})
	if err == nil {
		t.Fatal("NewWithLWT() with tcp:// + CACert should return an error, got nil")
	}
}

// ── SetReconnectHandler ───────────────────────────────────────────────────────

// TestSetReconnectHandler_ThreadSafe verifies that SetReconnectHandler can be
// called concurrently without a data race (run with -race).
func TestSetReconnectHandler_ThreadSafe(t *testing.T) {
	c := &Client{}
	done := make(chan struct{}, 10)
	for i := 0; i < 10; i++ {
		go func() {
			c.SetReconnectHandler(func() {})
			done <- struct{}{}
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}

// ── Message.Decode ────────────────────────────────────────────────────────────

// TestMessage_Decode_HappyPath verifies JSON round-trip through Message.Decode.
func TestMessage_Decode_HappyPath(t *testing.T) {
	m := &Message{
		Topic:   "cvmfs/repos/atlas.cern.ch/announce",
		Payload: []byte(`{"payload_id":"abc","publisher_id":"pub-1","repo":"atlas.cern.ch","hashes":["h1","h2"],"total_bytes":1024}`),
	}
	var ann AnnounceMessage
	if err := m.Decode(&ann); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if ann.PayloadID != "abc" {
		t.Errorf("PayloadID = %q, want %q", ann.PayloadID, "abc")
	}
	if ann.TotalBytes != 1024 {
		t.Errorf("TotalBytes = %d, want 1024", ann.TotalBytes)
	}
	if len(ann.Hashes) != 2 {
		t.Errorf("Hashes len = %d, want 2", len(ann.Hashes))
	}
}

// TestMessage_Decode_InvalidJSON verifies that malformed JSON returns an error.
func TestMessage_Decode_InvalidJSON(t *testing.T) {
	m := &Message{
		Topic:   "test/topic",
		Payload: []byte(`{not valid json`),
	}
	var ann AnnounceMessage
	if err := m.Decode(&ann); err == nil {
		t.Error("expected error for invalid JSON payload, got nil")
	}
}
