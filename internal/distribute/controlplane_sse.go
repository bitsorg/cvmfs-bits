// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package distribute

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"cvmfs.io/prepub/internal/broker"
)

// SSE control plane (ADR-0001 P-B): a second, broker-free implementation of the
// ControlPublisher / ControlReceiver contract. Where MQTT uses a pub/sub broker,
// this carries the SAME messages over plain HTTP — Server-Sent Events for the
// publisher→receiver fan-out (announce / published) and HTTP POST for the
// receiver→publisher direction (ready / presence). It exists so a site that does
// not want to run an MQTT broker can still use pull distribution; the orchestrator
// and receiver code are unchanged because both transports satisfy the same
// interfaces.
//
// Delivery is best-effort, exactly like MQTT QoS-0/1 here: a slow or briefly
// disconnected receiver may miss an event, and converges via the .cvmfspublished
// backstop poll (ADR R5). The SSE stream additionally sends keepalive comments so
// idle proxies do not drop the connection.

const (
	sseEventAnnounce  = "announce"
	sseEventPublished = "published"

	SSEEventsPath   = "/control/events"   // GET  — receiver subscribes (SSE)
	SSEReadyPath    = "/control/ready"    // POST — receiver → publisher ReadyMessage
	SSEPresencePath = "/control/presence" // POST — receiver → publisher PresenceMessage

	sseSubBuffer    = 64               // per-subscriber event backlog before drop
	sseKeepalive    = 15 * time.Second // idle keepalive comment interval
	sseMaxBodyBytes = 1 << 20          // cap for POSTed ready/presence bodies
)

// ── Publisher side ─────────────────────────────────────────────────────────────

type sseMsg struct {
	event string
	data  []byte
}

type sseSub struct {
	ch   chan sseMsg
	done chan struct{} // closed by Close() to evict the subscriber
}

// SSEServer is the Stratum-0 side of the SSE control plane. It implements
// ControlPublisher and exposes HTTP handlers (see Handler) to mount on the
// publisher's server.
type SSEServer struct {
	log func(string, ...any)

	mu       sync.Mutex
	subs     map[int]*sseSub
	nextID   int
	ready    map[string]func(broker.ReadyMessage) // key: publisherID|payloadID
	presence func(broker.PresenceMessage)
}

// NewSSEServer returns an empty SSE control-plane server. log may be nil.
func NewSSEServer(log func(string, ...any)) *SSEServer {
	return &SSEServer{
		log:   log,
		subs:  map[int]*sseSub{},
		ready: map[string]func(broker.ReadyMessage){},
	}
}

func (s *SSEServer) logf(msg string, args ...any) {
	if s.log != nil {
		s.log(msg, args...)
	}
}

// Announce broadcasts a prepare announce to all connected receivers.
func (s *SSEServer) Announce(_ string, m broker.AnnounceMessage) error {
	return s.broadcast(sseEventAnnounce, m)
}

// PublishCommitted broadcasts a post-commit notification to all receivers.
func (s *SSEServer) PublishCommitted(_ string, m broker.PublishedMessage) error {
	return s.broadcast(sseEventPublished, m)
}

func (s *SSEServer) broadcast(event string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("sse: marshal %s: %w", event, err)
	}
	msg := sseMsg{event: event, data: b}
	s.mu.Lock()
	subs := make([]*sseSub, 0, len(s.subs))
	for _, su := range s.subs {
		subs = append(subs, su)
	}
	s.mu.Unlock()
	for _, su := range subs {
		select {
		case su.ch <- msg:
		default:
			// Slow consumer: drop rather than block the publisher. The receiver
			// reconciles missed events via the backstop poll (ADR R5).
			s.logf("sse: subscriber backlog full, dropping event", "event", event)
		}
	}
	return nil
}

// OnReady registers a handler for ReadyMessages addressed to publisherID for
// payloadID. Call StopReady when the transaction is resolved to release it (the
// map is otherwise retained for the process lifetime — same contract as the
// WarmGate).
func (s *SSEServer) OnReady(publisherID, payloadID string, handle func(broker.ReadyMessage)) error {
	s.mu.Lock()
	s.ready[publisherID+"|"+payloadID] = handle
	s.mu.Unlock()
	return nil
}

// StopReady removes a previously-registered ready handler.
func (s *SSEServer) StopReady(publisherID, payloadID string) {
	s.mu.Lock()
	delete(s.ready, publisherID+"|"+payloadID)
	s.mu.Unlock()
}

// OnPresence registers a sink for receiver presence updates (optional).
func (s *SSEServer) OnPresence(handle func(broker.PresenceMessage)) {
	s.mu.Lock()
	s.presence = handle
	s.mu.Unlock()
}

// Close disconnects all subscribers and clears handlers. It closes each
// subscriber's done channel (never its event channel) so an in-flight broadcast
// — which does a non-blocking send on the event channel — can never send on a
// closed channel and panic. The serveEvents goroutine observes done and exits,
// removing itself.
func (s *SSEServer) Close() {
	s.mu.Lock()
	for _, su := range s.subs {
		close(su.done)
	}
	s.subs = map[int]*sseSub{}
	s.ready = map[string]func(broker.ReadyMessage){}
	s.presence = nil
	s.mu.Unlock()
}

// SubscriberCount reports the number of connected receivers (for tests / metrics).
func (s *SSEServer) SubscriberCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.subs)
}

func (s *SSEServer) addSub(su *sseSub) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextID
	s.nextID++
	s.subs[id] = su
	return id
}

func (s *SSEServer) removeSub(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subs, id)
}

// Handler returns the HTTP handler exposing the three control-plane endpoints.
// Mount it on the publisher's server (it is self-contained; no auth is applied
// here — front it with the same transport security as the data plane).
func (s *SSEServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(SSEEventsPath, s.serveEvents)
	mux.HandleFunc(SSEReadyPath, s.serveReady)
	mux.HandleFunc(SSEPresencePath, s.servePresence)
	return mux
}

func (s *SSEServer) serveEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	su := &sseSub{ch: make(chan sseMsg, sseSubBuffer), done: make(chan struct{})}
	id := s.addSub(su)
	defer s.removeSub(id)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ka := time.NewTicker(sseKeepalive)
	defer ka.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-su.done: // evicted by Close()
			return
		case msg := <-su.ch:
			// JSON marshalling escapes newlines, so data is always single-line.
			if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", msg.event, msg.data); err != nil {
				return
			}
			flusher.Flush()
		case <-ka.C:
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *SSEServer) serveReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	pub := r.URL.Query().Get("publisher")
	payload := r.URL.Query().Get("payload")
	if pub == "" || payload == "" {
		http.Error(w, "missing publisher/payload", http.StatusBadRequest)
		return
	}
	var rm broker.ReadyMessage
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, sseMaxBodyBytes)).Decode(&rm); err != nil {
		http.Error(w, "bad ready message", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	h := s.ready[pub+"|"+payload]
	s.mu.Unlock()
	if h != nil {
		h(rm)
	}
	// 202 even with no handler: a late/duplicate ack for an already-resolved
	// transaction is harmless and must not look like a client error.
	w.WriteHeader(http.StatusAccepted)
}

func (s *SSEServer) servePresence(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var pm broker.PresenceMessage
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, sseMaxBodyBytes)).Decode(&pm); err != nil {
		http.Error(w, "bad presence message", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	h := s.presence
	s.mu.Unlock()
	if h != nil {
		h(pm)
	}
	w.WriteHeader(http.StatusAccepted)
}

var _ ControlPublisher = (*SSEServer)(nil)

// ── Receiver side ──────────────────────────────────────────────────────────────

// SSEReceiver is the Stratum-1 side of the SSE control plane. It implements
// ControlReceiver by holding one long-lived SSE connection to the publisher (for
// announce / published) and POSTing ready / presence back. It reconnects with
// exponential backoff until Close.
type SSEReceiver struct {
	base   string
	client *http.Client

	mu         sync.Mutex
	announceH  func(broker.AnnounceMessage)
	publishedH func(broker.PublishedMessage)
	started    bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewSSEReceiver returns a receiver-side control plane targeting the publisher at
// baseURL (e.g. "https://stratum0.cern.ch"). client may be nil (uses a default
// with no timeout, required for a long-lived SSE stream).
func NewSSEReceiver(baseURL string, client *http.Client) *SSEReceiver {
	if client == nil {
		client = &http.Client{} // no Timeout: the SSE GET is long-lived
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &SSEReceiver{
		base:   strings.TrimRight(baseURL, "/"),
		client: client,
		ctx:    ctx,
		cancel: cancel,
	}
}

// SubscribeAnnounce registers the announce handler and starts the SSE loop if it
// is not already running.
func (r *SSEReceiver) SubscribeAnnounce(handle func(broker.AnnounceMessage)) error {
	r.mu.Lock()
	r.announceH = handle
	r.mu.Unlock()
	r.ensureStarted()
	return nil
}

// SubscribePublished registers the published handler and starts the SSE loop if
// it is not already running.
func (r *SSEReceiver) SubscribePublished(handle func(broker.PublishedMessage)) error {
	r.mu.Lock()
	r.publishedH = handle
	r.mu.Unlock()
	r.ensureStarted()
	return nil
}

func (r *SSEReceiver) ensureStarted() {
	r.mu.Lock()
	if r.started || r.ctx.Err() != nil {
		r.mu.Unlock()
		return
	}
	r.started = true
	r.mu.Unlock()
	r.wg.Add(1)
	go r.run()
}

func (r *SSEReceiver) run() {
	defer r.wg.Done()
	const (
		initialBackoff = 500 * time.Millisecond
		maxBackoff     = 30 * time.Second
	)
	backoff := initialBackoff
	for r.ctx.Err() == nil {
		connected, err := r.stream()
		if r.ctx.Err() != nil {
			return
		}
		if connected {
			backoff = initialBackoff // reset after a healthy connection
		}
		if err != nil {
			// fall through to backoff
		}
		select {
		case <-time.After(backoff):
		case <-r.ctx.Done():
			return
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// stream opens one SSE connection and dispatches events until it ends. It returns
// whether the connection was established (HTTP 200) so the caller can reset its
// backoff only after a healthy connection.
func (r *SSEReceiver) stream() (connected bool, err error) {
	req, err := http.NewRequestWithContext(r.ctx, http.MethodGet, r.base+SSEEventsPath, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := r.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("sse: events status %d", resp.StatusCode)
	}

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	var event string
	var data strings.Builder
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "": // blank line terminates an event
			if event != "" {
				r.dispatch(event, data.String())
			}
			event = ""
			data.Reset()
		case strings.HasPrefix(line, ":"): // comment / keepalive
			continue
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(line[len("event:"):])
		case strings.HasPrefix(line, "data:"):
			v := line[len("data:"):]
			data.WriteString(strings.TrimPrefix(v, " "))
		}
	}
	return true, sc.Err()
}

func (r *SSEReceiver) dispatch(event, data string) {
	r.mu.Lock()
	aH, pH := r.announceH, r.publishedH
	r.mu.Unlock()
	switch event {
	case sseEventAnnounce:
		if aH != nil {
			var m broker.AnnounceMessage
			if json.Unmarshal([]byte(data), &m) == nil {
				aH(m)
			}
		}
	case sseEventPublished:
		if pH != nil {
			var m broker.PublishedMessage
			if json.Unmarshal([]byte(data), &m) == nil {
				pH(m)
			}
		}
	}
}

// SendReady POSTs a ReadyMessage to the publisher for one payload.
func (r *SSEReceiver) SendReady(publisherID, payloadID, nodeID string, m broker.ReadyMessage) error {
	q := url.Values{}
	q.Set("publisher", publisherID)
	q.Set("payload", payloadID)
	return r.post(SSEReadyPath+"?"+q.Encode(), m)
}

// SetPresence POSTs a PresenceMessage to the publisher.
func (r *SSEReceiver) SetPresence(p broker.PresenceMessage) error {
	return r.post(SSEPresencePath, p)
}

func (r *SSEReceiver) post(path string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(r.ctx, http.MethodPost, r.base+path, strings.NewReader(string(b)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("sse: POST %s status %d", path, resp.StatusCode)
	}
	return nil
}

// Close stops the SSE loop and waits for it to exit.
func (r *SSEReceiver) Close() {
	r.cancel()
	r.wg.Wait()
}

var _ ControlReceiver = (*SSEReceiver)(nil)
