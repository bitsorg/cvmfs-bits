// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package receiver

import (
	"bytes"
	"context"
	"crypto/sha1" //nolint:gosec // CVMFS CAS key algorithm
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/broker"
	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/internal/distribute/serve"
	"cvmfs.io/prepub/pkg/observe"
)

// newMQTTTestReceiver creates a Receiver suitable for testing MQTT handler
// logic. The broker client is left nil so that mqttPublish is a no-op, and
// Stratum0URL is empty so the announce handler exercises decode/validate only.
func newMQTTTestReceiver(t *testing.T, repos ...string) *Receiver {
	t.Helper()
	obs, shutdown, err := observe.New("test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)

	cfg := Config{
		CASRoot: t.TempDir(),
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
	}
	r.mqttAnnounceHandler(fakeMQTTMessage(t, ann)) // must not panic
}

// TestSweepTmpFiles: stale CAS temp files under data/XX/ are removed; objects
// and temp files modified within sweepMinAge of the cutoff (a Put that may still
// be in flight) survive.
func TestSweepTmpFiles(t *testing.T) {
	casRoot := t.TempDir()
	store, err := cas.NewLocalFS(casRoot)
	if err != nil {
		t.Fatal(err)
	}
	obj := "ab" + fmt.Sprintf("%038x", 1) + "C"
	if err := store.Put(context.Background(), obj, bytes.NewReader([]byte("data")), 4); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(casRoot, "data", "ab")
	stale := filepath.Join(dir, cas.TempPrefix+"stale")
	fresh := filepath.Join(dir, cas.TempPrefix+"fresh")
	for _, p := range []string{stale, fresh} {
		if err := os.WriteFile(p, []byte("partial"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	cutoff := time.Now()
	old := cutoff.Add(-sweepMinAge - time.Minute)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}
	recent := cutoff.Add(-sweepMinAge + time.Minute) // before cutoff, inside the margin
	if err := os.Chtimes(fresh, recent, recent); err != nil {
		t.Fatal(err)
	}

	logged := false
	if err := sweepTmpFiles(context.Background(), casRoot, cutoff, func(string, ...any) { logged = true }); err != nil {
		t.Fatalf("sweepTmpFiles: %v", err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Error("stale temp file should have been removed")
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Errorf("temp file inside the age margin must survive: %v", err)
	}
	if ok, _ := store.Exists(context.Background(), obj); !ok {
		t.Error("CAS object must survive the sweep")
	}
	if !logged {
		t.Error("sweepTmpFiles should log when files are removed")
	}
}

// TestSweepTmpFiles_NonexistentRoot verifies that a missing CAS root is a no-op.
func TestSweepTmpFiles_NonexistentRoot(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nonexistent")
	if err := sweepTmpFiles(context.Background(), missing, time.Now(), func(string, ...any) {}); err != nil {
		t.Errorf("sweepTmpFiles on missing root should not error, got: %v", err)
	}
}

// newPullReceiver builds a receiver whose Stratum0URL is base.
func newPullReceiver(t *testing.T, base string, repos ...string) *Receiver {
	t.Helper()
	obs, shutdown, err := observe.New("test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)
	r, err := New(Config{CASRoot: t.TempDir(), Stratum0URL: base, Repos: repos, Obs: obs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r
}

// TestStratum0URLServesBothPaths checks that one Stratum0URL (the publisher
// base) serves both the /s1/ manifest and the post-commit root catalog, and
// that the catalog lands in the receiver's CAS.
func TestStratum0URLServesBothPaths(t *testing.T) {
	const repo = "atlas.cern.ch"
	src, err := cas.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("compressed root catalog bytes")
	sum := sha1.Sum(body) //nolint:gosec
	root := hex.EncodeToString(sum[:])
	if err := src.Put(context.Background(), root+"C", bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/cvmfs/", &serve.ObjectHandler{Store: src})
	mux.HandleFunc("/s1/txn-1/manifest", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"transaction_id":"txn-1","repo":%q,"target_root_hash":"r","base_urls":["x"],"generator":"pipeline"}`, repo)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	r := newPullReceiver(t, srv.URL)
	if _, err := r.pullCoordinator.OnTransaction(context.Background(), "txn-1"); err != nil {
		t.Fatalf("manifest pull: %v", err)
	}
	r.pullFromS0(context.Background(), broker.PublishedMessage{Repo: repo, NewRootHash: root})
	if ok, _ := r.casStore.Exists(context.Background(), root+"C"); !ok {
		t.Fatal("root catalog was not pulled into the receiver's CAS")
	}
}

// TestMqttAnnounceHandler_IgnoresUnservedRepo: a receiver for [a, b] (which
// subscribes to the wildcard filter) must not pull an announce for repo c.
func TestMqttAnnounceHandler_IgnoresUnservedRepo(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release // hold served pulls in flight so they stay observable
		http.NotFound(w, nil)
	}))
	defer srv.Close()
	defer close(release)

	r := newPullReceiver(t, srv.URL, "a.cern.ch", "b.cern.ch")
	for _, repo := range []string{"a.cern.ch", "c.cern.ch"} {
		r.mqttAnnounceHandler(fakeMQTTMessage(t, broker.AnnounceMessage{
			PayloadID: "job-" + repo, PublisherID: "pub", Repo: repo,
		}))
	}
	if _, ok := r.pullInflight.Load("job-a.cern.ch"); !ok {
		t.Error("announce for served repo a.cern.ch did not start a pull")
	}
	if _, ok := r.pullInflight.Load("job-c.cern.ch"); ok {
		t.Error("announce for unserved repo c.cern.ch started a pull")
	}
}

// TestPullFromS0_RejectsMalformedRootHash: a forged NewRootHash must not be
// fetched or written anywhere.
func TestPullFromS0_RejectsMalformedRootHash(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		hits++
		http.NotFound(w, req)
	}))
	defer srv.Close()

	r := newPullReceiver(t, srv.URL)
	r.pullFromS0(context.Background(), broker.PublishedMessage{Repo: "r.cern.ch", NewRootHash: "../../x"})
	if hits != 0 {
		t.Errorf("malformed root hash reached the network (%d requests)", hits)
	}
	if _, err := os.Stat(filepath.Join(r.cfg.CASRoot, "x")); !os.IsNotExist(err) {
		t.Errorf("malformed root hash wrote outside the CAS layout: %v", err)
	}
}
