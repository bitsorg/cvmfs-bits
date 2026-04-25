package receiver

import (
	"encoding/json"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/broker"
	"cvmfs.io/prepub/pkg/observe"
)

// storeLen returns the number of active sessions in ss (under lock).
func storeLen(ss *sessionStore) int {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return len(ss.byToken)
}

// ── helpers ───────────────────────────────────────────────────────────────────

// newMQTTTestReceiver creates a Receiver suitable for testing MQTT handler
// logic.  The broker client is left nil so that mqttPublish is a no-op (the
// receiver logs "not active" and returns false) — we test the observable
// side-effects on the session store and inventory instead.
func newMQTTTestReceiver(t *testing.T, repos ...string) *Receiver {
	t.Helper()
	obs, shutdown, err := observe.New("test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)

	dir := t.TempDir()
	cfg := Config{
		CASRoot:    dir,
		HMACSecret: "test-secret",
		DevMode:    true,
		NodeID:     "test-node",
		Repos:      repos,
		SessionTTL: time.Hour,
		Obs:        obs,
	}
	r, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// mqttClient remains nil — mqttPublish will be a no-op.
	return r
}

// fakeMQTTMessage builds a *broker.Message with the given AnnounceMessage
// encoded as JSON, mimicking what the Paho callback delivers.
func fakeMQTTMessage(t *testing.T, ann broker.AnnounceMessage) *broker.Message {
	t.Helper()
	payload, err := json.Marshal(ann)
	if err != nil {
		t.Fatalf("marshal AnnounceMessage: %v", err)
	}
	return &broker.Message{
		Topic:   broker.AnnounceTopic(ann.Repo),
		Payload: payload,
	}
}

// ── startMQTT validation ──────────────────────────────────────────────────────

// TestStartMQTT_EmptyNodeIDReturnsError verifies that startMQTT returns an
// error when NodeID is empty and BrokerURL is configured, preventing multiple
// misconfigured receivers from colliding on the same "unknown" presence topic.
func TestStartMQTT_EmptyNodeIDReturnsError(t *testing.T) {
	obs, shutdown, err := observe.New("test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)

	r, err := New(Config{
		CASRoot:   t.TempDir(),
		DevMode:   true,
		NodeID:    "", // deliberately empty
		BrokerURL: "tcp://localhost:1883",
		Obs:       obs,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := r.startMQTT(); err == nil {
		t.Error("startMQTT() with empty NodeID should return an error, got nil")
	}
}

// ── servesRepo ────────────────────────────────────────────────────────────────

// TestServesRepo_EmptyListAcceptsAll verifies that an empty Repos config means
// the receiver accepts announces for any repository.
func TestServesRepo_EmptyListAcceptsAll(t *testing.T) {
	r := newMQTTTestReceiver(t) // no repos configured
	for _, repo := range []string{"atlas.cern.ch", "cms.cern.ch", "anything"} {
		if !r.servesRepo(repo) {
			t.Errorf("servesRepo(%q) = false; want true for empty Repos list", repo)
		}
	}
}

// TestServesRepo_MatchesCaseInsensitively verifies RFC 4343 (DNS is
// case-insensitive) — "Atlas.CERN.CH" must match configured "atlas.cern.ch".
func TestServesRepo_MatchesCaseInsensitively(t *testing.T) {
	r := newMQTTTestReceiver(t, "atlas.cern.ch", "cms.cern.ch")
	cases := []struct {
		repo string
		want bool
	}{
		{"atlas.cern.ch", true},
		{"ATLAS.CERN.CH", true},
		{"Atlas.Cern.Ch", true},
		{"cms.cern.ch", true},
		{"CMS.CERN.CH", true},
		{"lhcb.cern.ch", false},
		{"", false},
	}
	for _, tc := range cases {
		got := r.servesRepo(tc.repo)
		if got != tc.want {
			t.Errorf("servesRepo(%q) = %v, want %v", tc.repo, got, tc.want)
		}
	}
}

// ── computeAbsentHashes ──────────────────────────────────────────────────────

// TestComputeAbsentHashes_InventoryNotReady verifies that when the inventory
// Bloom filter has not yet been populated (CAS walk still in progress), all
// hashes are reported as absent — the conservative choice.
func TestComputeAbsentHashes_InventoryNotReady(t *testing.T) {
	r := newMQTTTestReceiver(t)
	// inventory.isReady() starts as false; no CAS walk is triggered by New().
	hashes := []string{"aaa", "bbb", "ccc"}
	absent := r.computeAbsentHashes(hashes)
	if len(absent) != len(hashes) {
		t.Errorf("computeAbsentHashes when not ready: got %d absent, want %d (all)", len(absent), len(hashes))
	}
	// Verify it's a copy, not the same slice.
	if len(absent) > 0 && &absent[0] == &hashes[0] {
		t.Error("computeAbsentHashes should return a copy, not the original slice")
	}
}

// TestComputeAbsentHashes_InventoryReady_FiltersKnownHashes verifies that once
// the inventory is marked ready, only hashes absent from the Bloom filter are
// returned.
func TestComputeAbsentHashes_InventoryReady_FiltersKnownHashes(t *testing.T) {
	r := newMQTTTestReceiver(t)

	// Manually mark inventory ready and add some known hashes.
	r.inv.add("known-hash-1")
	r.inv.add("known-hash-2")
	// Mark the inventory as ready so computeAbsentHashes uses the filter
	// rather than the conservative "all absent" fallback.
	r.inv.mu.Lock()
	r.inv.ready = true
	r.inv.mu.Unlock()

	hashes := []string{"known-hash-1", "known-hash-2", "unknown-hash-3"}
	absent := r.computeAbsentHashes(hashes)

	// Only the unknown hash must appear in the absent list.
	if len(absent) != 1 {
		t.Fatalf("computeAbsentHashes: got %d absent hashes, want 1; absent=%v", len(absent), absent)
	}
	if absent[0] != "unknown-hash-3" {
		t.Errorf("computeAbsentHashes: got %q, want %q", absent[0], "unknown-hash-3")
	}
}

// TestComputeAbsentHashes_EmptyInput verifies that an empty hash list returns
// an empty (not nil) slice in both ready and not-ready states.
func TestComputeAbsentHashes_EmptyInput(t *testing.T) {
	r := newMQTTTestReceiver(t)
	if absent := r.computeAbsentHashes(nil); absent == nil {
		t.Error("computeAbsentHashes(nil) should return non-nil empty slice when not ready")
	}
	r.inv.mu.Lock()
	r.inv.ready = true
	r.inv.mu.Unlock()
	if absent := r.computeAbsentHashes([]string{}); absent == nil {
		t.Error("computeAbsentHashes([]) should return non-nil empty slice when ready")
	}
}

// ── mqttPublish ───────────────────────────────────────────────────────────────

// TestMqttPublish_NilClientReturnsFalse verifies that mqttPublish returns false
// gracefully when no MQTT client is connected, instead of panicking (CRITICAL #1
// regression guard — race-safe nil-client handling).
func TestMqttPublish_NilClientReturnsFalse(t *testing.T) {
	r := newMQTTTestReceiver(t)
	// mqttClient is nil; publish must return false without panic.
	ok := r.mqttPublish("any/topic", struct{ V int }{V: 1})
	if ok {
		t.Error("mqttPublish with nil client should return false")
	}
}

// ── mqttAnnounceHandler ───────────────────────────────────────────────────────

// TestMqttAnnounceHandler_MalformedPayload verifies that an invalid JSON payload
// is logged and discarded without panic or session creation.
func TestMqttAnnounceHandler_MalformedPayload(t *testing.T) {
	r := newMQTTTestReceiver(t)
	msg := &broker.Message{
		Topic:   "cvmfs/repos/test.cern.ch/announce",
		Payload: []byte(`{not valid json`),
	}
	// Must not panic.
	r.mqttAnnounceHandler(msg)
	// No session should have been created.
	if storeLen(r.store) != 0 {
		t.Errorf("session store should be empty after malformed payload, got %d entries", storeLen(r.store))
	}
}

// TestMqttAnnounceHandler_MissingRequiredFields verifies that an announce
// missing required fields (payload_id, publisher_id, repo) does not create a
// session.  The handler should publish an error reply and return.
func TestMqttAnnounceHandler_MissingRequiredFields(t *testing.T) {
	r := newMQTTTestReceiver(t)
	// Only payload_id provided; publisher_id and repo are missing.
	msg := &broker.Message{
		Topic:   "cvmfs/repos/test.cern.ch/announce",
		Payload: []byte(`{"payload_id":"abc"}`),
	}
	r.mqttAnnounceHandler(msg)
	if storeLen(r.store) != 0 {
		t.Error("session should not be created for announce with missing required fields")
	}
}

// TestMqttAnnounceHandler_UnknownRepo verifies that an announce for a repository
// not served by this receiver is silently ignored (no session, no error log).
func TestMqttAnnounceHandler_UnknownRepo(t *testing.T) {
	r := newMQTTTestReceiver(t, "atlas.cern.ch") // only serves atlas

	ann := broker.AnnounceMessage{
		PayloadID:   "job-999",
		PublisherID: "pub-test",
		Repo:        "cms.cern.ch", // not served
		Hashes:      []string{"h1"},
	}
	r.mqttAnnounceHandler(fakeMQTTMessage(t, ann))

	if storeLen(r.store) != 0 {
		t.Error("session should not be created for an announce targeting an unknown repo")
	}
}

// TestMqttAnnounceHandler_SessionCreated verifies the happy path: a well-formed
// announce for a served repo results in a new session in the store.
func TestMqttAnnounceHandler_SessionCreated(t *testing.T) {
	r := newMQTTTestReceiver(t, "atlas.cern.ch")

	ann := broker.AnnounceMessage{
		PayloadID:   "job-happy",
		PublisherID: "pub-test",
		Repo:        "atlas.cern.ch",
		Hashes:      []string{"h1", "h2"},
	}
	r.mqttAnnounceHandler(fakeMQTTMessage(t, ann))

	if _, ok := r.store.getByPayload("job-happy"); !ok {
		t.Error("expected a session to be created for the announced payload_id")
	}
}

// TestMqttAnnounceHandler_IdempotentReAnnounce verifies that sending the same
// AnnounceMessage twice returns the same session (idempotency).
func TestMqttAnnounceHandler_IdempotentReAnnounce(t *testing.T) {
	r := newMQTTTestReceiver(t, "atlas.cern.ch")

	ann := broker.AnnounceMessage{
		PayloadID:   "job-idem-mqtt",
		PublisherID: "pub-test",
		Repo:        "atlas.cern.ch",
		Hashes:      []string{"h1"},
	}
	msg := fakeMQTTMessage(t, ann)

	r.mqttAnnounceHandler(msg)
	first, ok := r.store.getByPayload("job-idem-mqtt")
	if !ok {
		t.Fatal("first announce: no session created")
	}

	r.mqttAnnounceHandler(msg)
	second, ok := r.store.getByPayload("job-idem-mqtt")
	if !ok {
		t.Fatal("second announce: session disappeared")
	}

	if first.token != second.token {
		t.Errorf("re-announce should return the same session token: first=%q second=%q",
			first.token, second.token)
	}
	// Store should still have exactly one session.
	if storeLen(r.store) != 1 {
		t.Errorf("expected 1 session after idempotent re-announce, got %d", storeLen(r.store))
	}
}

// TestMqttAnnounceHandler_InsufficientDisk verifies that an announce with an
// absurdly large total_bytes is rejected and no session is created.
func TestMqttAnnounceHandler_InsufficientDisk(t *testing.T) {
	r := newMQTTTestReceiver(t) // empty Repos — accepts all

	ann := broker.AnnounceMessage{
		PayloadID:   "job-nospace",
		PublisherID: "pub-test",
		Repo:        "atlas.cern.ch",
		TotalBytes:  1<<62 - 1, // absurdly large — cannot possibly have this space
	}
	r.mqttAnnounceHandler(fakeMQTTMessage(t, ann))

	if _, ok := r.store.getByPayload("job-nospace"); ok {
		t.Error("session should not be created when disk space check fails")
	}
}

// TestMqttAnnounceHandler_TooManyHashes verifies that an announce carrying
// more than maxHashesPerAnnounce hashes is rejected without creating a session.
// This is a DoS regression guard: a rogue broker client should not be able to
// force GB-scale allocations inside computeAbsentHashes.
//
// The JSON payload is built directly (not via fakeMQTTMessage / json.Marshal)
// to avoid allocating 1M+ Go strings in the test process — the race detector
// would make that prohibitively slow.  The handler still goes through its normal
// json.Unmarshal → bounds-check → replyErr path.
func TestMqttAnnounceHandler_TooManyHashes(t *testing.T) {
	r := newMQTTTestReceiver(t)

	// Build the raw JSON payload in a single linear pass: no intermediate string
	// slice is needed, so memory stays bounded to the ~7 MB output buffer.
	count := maxHashesPerAnnounce + 1
	var buf []byte
	buf = append(buf, `{"payload_id":"job-toobig","publisher_id":"pub-test","repo":"atlas.cern.ch","hashes":[`...)
	entry := []byte(`"aaaa"`)
	sep := []byte(`,`)
	for i := 0; i < count; i++ {
		if i > 0 {
			buf = append(buf, sep...)
		}
		buf = append(buf, entry...)
	}
	buf = append(buf, `],"total_bytes":0}`...)

	msg := &broker.Message{
		Topic:   broker.AnnounceTopic("atlas.cern.ch"),
		Payload: buf,
	}
	r.mqttAnnounceHandler(msg)

	if _, ok := r.store.getByPayload("job-toobig"); ok {
		t.Error("session should not be created when Hashes exceeds maxHashesPerAnnounce")
	}
	if storeLen(r.store) != 0 {
		t.Errorf("store should be empty after over-limit announce, got %d entries", storeLen(r.store))
	}
}

// TestMqttAnnounceHandler_HashTooLong verifies that an announce containing a
// single hash string longer than maxHashLen is rejected without creating a session.
// This prevents pathological CPU usage inside the Bloom filter hash computation.
func TestMqttAnnounceHandler_HashTooLong(t *testing.T) {
	r := newMQTTTestReceiver(t)

	// Build a hash string that is exactly one byte over the limit.
	longHash := make([]byte, maxHashLen+1)
	for i := range longHash {
		longHash[i] = 'a'
	}
	ann := broker.AnnounceMessage{
		PayloadID:   "job-longhash",
		PublisherID: "pub-test",
		Repo:        "atlas.cern.ch",
		Hashes:      []string{string(longHash)},
	}
	r.mqttAnnounceHandler(fakeMQTTMessage(t, ann))

	if _, ok := r.store.getByPayload("job-longhash"); ok {
		t.Error("session should not be created when a hash string exceeds maxHashLen")
	}
}

// TestMqttAnnounceHandler_ConcurrentDoesNotRace is a race-detector test
// (run with -race) verifying that concurrent announce handling does not trigger
// data races on the session store or inventory (CRITICAL #1 regression guard).
func TestMqttAnnounceHandler_ConcurrentDoesNotRace(t *testing.T) {
	r := newMQTTTestReceiver(t) // empty Repos — accepts all

	const n = 20
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer func() { done <- struct{}{} }()
			ann := broker.AnnounceMessage{
				PayloadID:   "job-concurrent",
				PublisherID: "pub-test",
				Repo:        "atlas.cern.ch",
				Hashes:      []string{"h1", "h2"},
			}
			_ = i // suppress lint warning
			r.mqttAnnounceHandler(fakeMQTTMessage(t, ann))
		}()
	}
	for i := 0; i < n; i++ {
		<-done
	}
	// One session per payloadID (idempotent).
	if storeLen(r.store) != 1 {
		t.Errorf("concurrent announces for same payloadID should create exactly 1 session, got %d", storeLen(r.store))
	}
}
