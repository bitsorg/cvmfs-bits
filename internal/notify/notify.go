// Package notify provides an in-process event bus for job state changes and
// best-effort webhook delivery to external callers.
package notify

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	"cvmfs.io/prepub/internal/job"
	"cvmfs.io/prepub/pkg/observe"
)

// webhookClient is a package-level HTTP client with a short deadline used for
// all webhook deliveries.  Reuse a single client to benefit from connection
// pooling when the same webhook URL is called repeatedly.
// TLS 1.2 is the minimum acceptable version — older protocol versions have
// known weaknesses and must not be negotiated even for webhook endpoints.
var webhookClient = &http.Client{
	Timeout: 10 * time.Second,
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	},
}

// Event is published whenever a job transitions state.
type Event struct {
	JobID string    `json:"job_id"`
	State job.State `json:"state"`
	Error string    `json:"error,omitempty"` // non-empty only on failed/aborted
	Time  time.Time `json:"time"`
}

// Bus is an in-process publish/subscribe hub for job events.
// All methods are safe for concurrent use.
type Bus struct {
	mu   sync.RWMutex
	subs map[string][]chan Event
}

// NewBus creates an empty event bus.
func NewBus() *Bus {
	return &Bus{subs: make(map[string][]chan Event)}
}

// Subscribe returns a buffered channel that receives every Event published for
// jobID, and a cancel function that must be called to unsubscribe and close
// the channel.  Multiple subscribers for the same job are supported.
func (b *Bus) Subscribe(jobID string) (<-chan Event, func()) {
	ch := make(chan Event, 32)

	b.mu.Lock()
	b.subs[jobID] = append(b.subs[jobID], ch)
	b.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			subs := b.subs[jobID]
			for i, s := range subs {
				if s == ch {
					b.subs[jobID] = append(subs[:i], subs[i+1:]...)
					break
				}
			}
			if len(b.subs[jobID]) == 0 {
				delete(b.subs, jobID)
			}
			close(ch)
		})
	}
	return ch, cancel
}

// Publish sends e to all current subscribers for e.JobID.
// It is non-blocking: a subscriber whose channel is full drops the event
// rather than stalling the publishing goroutine.
func (b *Bus) Publish(e Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, ch := range b.subs[e.JobID] {
		select {
		case ch <- e:
		default:
			// subscriber not consuming — drop rather than block the orchestrator
		}
	}
}

// DeliverWebhook POSTs e as JSON to url.  Delivery is best-effort: failures are
// logged at Warn level and do not affect the job outcome.  The caller should
// invoke this in a goroutine so that a slow or unreachable webhook endpoint
// does not delay the orchestrator.
func DeliverWebhook(ctx context.Context, url string, e Event, obs *observe.Provider) {
	body, err := json.Marshal(e)
	if err != nil {
		obs.Logger.Warn("webhook: marshal error", "url", url, "error", err)
		return
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		obs.Logger.Warn("webhook: failed to create request", "url", url, "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := webhookClient.Do(req)
	if err != nil {
		obs.Logger.Warn("webhook: delivery failed", "url", url, "job_id", e.JobID, "error", err)
		return
	}
	// Drain and close so the underlying TCP connection is returned to the pool.
	defer func() { io.Copy(io.Discard, resp.Body); resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		obs.Logger.Warn("webhook: receiver returned error status",
			"url", url, "job_id", e.JobID, "status", resp.StatusCode)
		return
	}

	obs.Logger.Info("webhook: delivered", "url", url, "job_id", e.JobID, "state", e.State)
}
