// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package distribute

import (
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/broker"
)

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestSSEControlPlaneEndToEnd(t *testing.T) {
	srv := NewSSEServer(nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	recv := NewSSEReceiver(ts.URL, ts.Client())
	defer recv.Close()

	announces := make(chan broker.AnnounceMessage, 4)
	published := make(chan broker.PublishedMessage, 4)
	if err := recv.SubscribeAnnounce(func(m broker.AnnounceMessage) { announces <- m }); err != nil {
		t.Fatal(err)
	}
	if err := recv.SubscribePublished(func(m broker.PublishedMessage) { published <- m }); err != nil {
		t.Fatal(err)
	}

	// Wait until the receiver's SSE stream is registered, else the broadcast races
	// ahead of the subscription.
	waitFor(t, "sse subscriber", func() bool { return srv.SubscriberCount() == 1 })

	// publisher → receiver: announce
	if err := srv.Announce("atlas.cern.ch", broker.AnnounceMessage{PayloadID: "p1", Repo: "atlas.cern.ch"}); err != nil {
		t.Fatal(err)
	}
	select {
	case a := <-announces:
		if a.PayloadID != "p1" || a.Repo != "atlas.cern.ch" {
			t.Fatalf("announce mismatch: %+v", a)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("announce not received over SSE")
	}

	// publisher → receiver: published
	if err := srv.PublishCommitted("atlas.cern.ch", broker.PublishedMessage{Repo: "atlas.cern.ch", NewRootHash: "root99"}); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-published:
		if p.NewRootHash != "root99" {
			t.Fatalf("published mismatch: %+v", p)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("published not received over SSE")
	}

	// receiver → publisher: ready
	gotReady := make(chan broker.ReadyMessage, 1)
	if err := srv.OnReady("pub1", "p1", func(rm broker.ReadyMessage) { gotReady <- rm }); err != nil {
		t.Fatal(err)
	}
	if err := recv.SendReady("pub1", "p1", "node-a", broker.ReadyMessage{NodeID: "node-a"}); err != nil {
		t.Fatal(err)
	}
	select {
	case rm := <-gotReady:
		if rm.NodeID != "node-a" {
			t.Fatalf("ready mismatch: %+v", rm)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ready POST not routed to handler")
	}

	// receiver → publisher: presence
	gotPresence := make(chan broker.PresenceMessage, 1)
	srv.OnPresence(func(pm broker.PresenceMessage) { gotPresence <- pm })
	if err := recv.SetPresence(broker.PresenceMessage{NodeID: "node-a", Online: true}); err != nil {
		t.Fatal(err)
	}
	select {
	case pm := <-gotPresence:
		if pm.NodeID != "node-a" || !pm.Online {
			t.Fatalf("presence mismatch: %+v", pm)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("presence POST not routed to handler")
	}
}

// TestSSEServerCloseDuringBroadcast exercises the close-vs-broadcast race that a
// naive close(su.ch) would turn into a send-on-closed-channel panic.
func TestSSEServerCloseDuringBroadcast(t *testing.T) {
	srv := NewSSEServer(nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	recv := NewSSEReceiver(ts.URL, ts.Client())
	_ = recv.SubscribeAnnounce(func(broker.AnnounceMessage) {})
	waitFor(t, "sse subscriber", func() bool { return srv.SubscriberCount() == 1 })

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			_ = srv.Announce("r", broker.AnnounceMessage{PayloadID: "x"})
		}
	}()
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond)
		srv.Close() // must not panic even with broadcasts in flight
	}()
	wg.Wait()
	recv.Close()
}

func TestSSEReadyBadParams(t *testing.T) {
	srv := NewSSEServer(nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Missing publisher/payload → 400.
	resp, err := ts.Client().Post(ts.URL+SSEReadyPath, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("status %d, want 400", resp.StatusCode)
	}
}
