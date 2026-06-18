// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package receiver

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/broker"
	"cvmfs.io/prepub/pkg/observe"
)

// makeHash returns a deterministic 64-hex-char CAS hash for the given seed.
func makeHash(n int) string {
	return fmt.Sprintf("%064x", n)
}

// newMQTTTestReceiver creates a Receiver suitable for testing MQTT handler
// logic. The broker client is left nil so that mqttPublish is a no-op, and
// PullMode is off so the announce handler exercises decode/validate only.
func newMQTTTestReceiver(t *testing.T, repos ...string) *Receiver {
	t.Helper()
	obs, shutdown, err := observe.New("test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)

	cfg := Config{
		CASRoot: t.TempDir(),
		DevMode: true,
		NodeID:  "test-node",
		Repos:   repos,
		Obs:     obs,
	}
	r, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
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
		NodeID:    "",
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

// TestServesRepo_EmptyListAcceptsAll verifies that an empty Repos config means
// the receiver accepts announces for any repository.
func TestServesRepo_EmptyListAcceptsAll(t *testing.T) {
	r := newMQTTTestReceiver(t)
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
		{"lhcb.cern.ch", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := r.servesRepo(tc.repo); got != tc.want {
			t.Errorf("servesRepo(%q) = %v, want %v", tc.repo, got, tc.want)
		}
	}
}

// TestMqttPublish_NilClientReturnsFalse verifies that mqttPublish returns false
// gracefully when no MQTT client is connected, instead of panicking.
func TestMqttPublish_NilClientReturnsFalse(t *testing.T) {
	r := newMQTTTestReceiver(t)
	if ok := r.mqttPublish("any/topic", struct{ V int }{V: 1}); ok {
		t.Error("mqttPublish with nil client should return false")
	}
}

// TestMqttAnnounceHandler_MalformedPayload verifies that an invalid JSON payload
// is logged and discarded without panic.
func TestMqttAnnounceHandler_MalformedPayload(t *testing.T) {
	r := newMQTTTestReceiver(t)
	msg := &broker.Message{
		Topic:   "cvmfs/repos/test.cern.ch/announce",
		Payload: []byte(`{not valid json`),
	}
	r.mqttAnnounceHandler(msg) // must not panic
}

// TestMqttAnnounceHandler_MissingRequiredFields verifies that an announce
// missing required fields is handled without panic.
func TestMqttAnnounceHandler_MissingRequiredFields(t *testing.T) {
	r := newMQTTTestReceiver(t)
	msg := &broker.Message{
		Topic:   "cvmfs/repos/test.cern.ch/announce",
		Payload: []byte(`{"payload_id":"abc"}`),
	}
	r.mqttAnnounceHandler(msg) // must not panic
}

// TestMqttAnnounceHandler_WellFormed verifies that a well-formed announce for a
// served repo is handled without panic (the pull is a no-op when no pull
// coordinator is configured, as in these unit receivers).
func TestMqttAnnounceHandler_WellFormed(t *testing.T) {
	r := newMQTTTestReceiver(t, "atlas.cern.ch")
	ann := broker.AnnounceMessage{
		PayloadID:   "job-1",
		PublisherID: "pub-job-1",
		Repo:        "atlas.cern.ch",
		Hashes:      []string{makeHash(1), makeHash(2)},
	}
	r.mqttAnnounceHandler(fakeMQTTMessage(t, ann)) // must not panic
}

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
	if _, err := os.Stat(tmpFile); !os.IsNotExist(err) {
		t.Error(".tmp file should have been removed")
	}
	if _, err := os.Stat(casFile); err != nil {
		t.Errorf("CAS file should survive sweep: %v", err)
	}
	if !logged {
		t.Error("sweepTmpFiles should log when files are removed")
	}
}

// TestSweepTmpFiles_NonexistentRoot verifies that a missing CAS root is a no-op.
func TestSweepTmpFiles_NonexistentRoot(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nonexistent")
	if err := sweepTmpFiles(context.Background(), missing, func(string, ...any) {}); err != nil {
		t.Errorf("sweepTmpFiles on missing root should not error, got: %v", err)
	}
}

// TestCASPath verifies the on-disk layout: objects land at {root}/{hash[:2]}/{hash}C.
func TestCASPath(t *testing.T) {
	root := "/srv/cvmfs/cas"
	hash := "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	want := filepath.Join(root, "ab", hash+"C")
	if got := casPath(root, hash); got != want {
		t.Errorf("casPath = %q, want %q", got, want)
	}
}

// TestRandomToken verifies randomToken returns a 32-hex-char unique token.
func TestRandomToken(t *testing.T) {
	a, b := randomToken(), randomToken()
	if len(a) != 32 {
		t.Errorf("randomToken length = %d, want 32", len(a))
	}
	if a == b {
		t.Error("randomToken returned identical tokens on consecutive calls")
	}
	_ = time.Now
}
