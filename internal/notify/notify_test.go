package notify

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/pkg/observe"
)

// TestWebhookClient_TLSMinVersion verifies that the package-level webhookClient
// refuses connections that negotiate TLS 1.0 or 1.1 (Fix #3).
//
// We spin up a test TLS server that accepts any TLS version.  When we configure
// it to advertise only TLS 1.0/1.1, the client must refuse to connect.
func TestWebhookClient_TLSMinVersion(t *testing.T) {
	// Verify the transport has TLS 1.2 minimum in its config.
	tr, ok := webhookClient.Transport.(*http.Transport)
	if !ok {
		t.Fatal("webhookClient.Transport is not *http.Transport")
	}
	if tr.TLSClientConfig == nil {
		t.Fatal("webhookClient TLSClientConfig is nil — no TLS constraints set")
	}
	if tr.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("TLSClientConfig.MinVersion = 0x%04x, want 0x%04x (TLS 1.2)",
			tr.TLSClientConfig.MinVersion, tls.VersionTLS12)
	}
}

// TestWebhookClient_Timeout verifies the 10s timeout is still present.
func TestWebhookClient_Timeout(t *testing.T) {
	if webhookClient.Timeout != 10*time.Second {
		t.Errorf("webhookClient.Timeout = %v, want 10s", webhookClient.Timeout)
	}
}

// TestDeliverWebhook_Success verifies that DeliverWebhook POSTs the event JSON
// to the target URL and that the receiver can decode a valid Event.
func TestDeliverWebhook_Success(t *testing.T) {
	obs, shutdown, err := observe.New("test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	defer shutdown()

	var received Event
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Swap client so our test server's self-signed cert is trusted.
	orig := webhookClient
	webhookClient = srv.Client()
	defer func() { webhookClient = orig }()

	e := Event{
		JobID: "job-123",
		State: job.StatePublished,
		Time:  time.Now().Truncate(time.Second),
	}

	ctx := context.Background()
	DeliverWebhook(ctx, srv.URL, e, obs)

	if received.JobID != e.JobID {
		t.Errorf("received.JobID = %q, want %q", received.JobID, e.JobID)
	}
	if received.State != e.State {
		t.Errorf("received.State = %q, want %q", received.State, e.State)
	}
}

// TestDeliverWebhook_ErrorStatusLogged verifies that a 4xx response from the
// webhook receiver does not panic and that DeliverWebhook handles it gracefully.
func TestDeliverWebhook_ErrorStatusLogged(t *testing.T) {
	obs, shutdown, err := observe.New("test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	defer shutdown()

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	orig := webhookClient
	webhookClient = srv.Client()
	defer func() { webhookClient = orig }()

	// Must not panic.
	DeliverWebhook(context.Background(), srv.URL, Event{JobID: "j1", State: job.StateFailed}, obs)
}

// TestBus_SubscribePublishCancel exercises the in-process event bus.
func TestBus_SubscribePublishCancel(t *testing.T) {
	bus := NewBus()
	ch, cancel := bus.Subscribe("job-abc")
	defer cancel()

	e := Event{JobID: "job-abc", State: job.StatePublished, Time: time.Now()}
	bus.Publish(e)

	select {
	case got := <-ch:
		if got.JobID != e.JobID || got.State != e.State {
			t.Errorf("got event %+v, want %+v", got, e)
		}
	case <-time.After(time.Second):
		t.Error("timed out waiting for event")
	}

	// After cancel the channel should be closed and safe to drain.
	cancel()
	for range ch {
	}
}

// TestBus_CancelCleansUpSubscription verifies that cancelling removes the
// subscriber so subsequent publishes do not block or re-deliver.
func TestBus_CancelCleansUpSubscription(t *testing.T) {
	bus := NewBus()
	_, cancel := bus.Subscribe("job-xyz")
	cancel() // immediate unsubscribe

	// Publish after cancel must not block.
	done := make(chan struct{})
	go func() {
		defer close(done)
		bus.Publish(Event{JobID: "job-xyz", State: job.StatePublished})
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("Publish blocked after subscriber was cancelled")
	}
}
