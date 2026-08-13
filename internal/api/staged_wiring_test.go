// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

// Does a staged job actually reach the gateway, through the backend production
// wires up?
//
// This file exists because the answer was NO, and nothing caught it. The first
// version of the staged path routed jobs to the ingest backend, which requires
// a tar and contains no reference to DirectGraft, NewRootHashSuffixed or
// OldRootHash. Every test passed: they all substituted a fake backend and
// asserted the CommitRequest was populated correctly, which it was — by a
// caller nobody could act on. The bug was found by a reviewer reading main.go.
//
// So these tests use lease.NewStagedBackend over a real lease.Client, wired the
// way cmd/prepub/main.go wires it, against an httptest gateway. They assert
// what the gateway RECEIVES, not what prepub intended to send.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/lease"
)

// fakeGateway records the requests a publish makes against it.
type fakeGateway struct {
	mu     sync.Mutex
	paths  []string          // request paths, in order
	bodies map[string]string // last body per path
	srv    *httptest.Server
}

func newFakeGateway(t *testing.T) *fakeGateway {
	t.Helper()
	g := &fakeGateway{bodies: map[string]string{}}
	g.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		g.mu.Lock()
		g.paths = append(g.paths, r.URL.Path)
		g.bodies[r.URL.Path] = string(body)
		g.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "POST" && r.URL.Path == "/api/v1/leases":
			io.WriteString(w, `{"status":"ok","session_token":"tok-1"}`)
		default:
			io.WriteString(w, `{"status":"ok"}`)
		}
	}))
	t.Cleanup(g.srv.Close)
	return g
}

func (g *fakeGateway) sawPath(sub string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, p := range g.paths {
		if strings.Contains(p, sub) {
			return true
		}
	}
	return false
}

func (g *fakeGateway) bodyFor(sub string) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	for p, b := range g.bodies {
		if strings.Contains(p, sub) {
			return b
		}
	}
	return ""
}

// The end of the wire: a staged job must reach POST .../graft carrying the
// producer's catalog hash, and must send no payload.
//
// NEGATIVE CONTROL: point PublishPaths at the ingest backend instead of the
// staged one — as the first version of this feature did — and this fails with
// "ingest backend: no tar payload". Verified.
func TestStagedJobReachesTheGatewayGraftEndpoint(t *testing.T) {
	_, _, orch := newTestServer(t)
	gw := newFakeGateway(t)

	// Wired exactly as cmd/prepub/main.go does it.
	client := lease.NewClient(gw.srv.URL, "key", "secret", orch.Obs)
	orch.Lease = &noopBackend{}
	orch.PublishPaths = map[string]lease.Backend{
		StagedPublishPath: lease.NewStagedBackend(client),
	}
	fc := newFakeCAS(stagedCatalog)
	orch.CAS = fc

	j := stagedJob(t, orch, "staging/host7/job-1")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := orch.Run(ctx, j, nil); err != nil {
		t.Fatalf("staged publish failed against a live gateway: %v", err)
	}

	if !gw.sawPath("/graft") {
		g := gw.paths
		t.Fatalf("the gateway was never asked to graft; it saw: %v", g)
	}
	// The payload endpoint must not be touched: the objects went in by
	// server-side copy, which is the entire point of the design.
	if gw.sawPath("/payload") {
		t.Error("a staged publish must not submit a payload")
	}

	var body struct {
		NewRootHash string `json:"new_root_hash"`
	}
	raw := gw.bodyFor("/graft")
	if err := json.Unmarshal([]byte(raw), &body); err != nil {
		t.Fatalf("graft body is not JSON: %s", raw)
	}
	if body.NewRootHash != stagedCatalog {
		t.Errorf("gateway received new_root_hash = %q, want the producer's catalog %q",
			body.NewRootHash, stagedCatalog)
	}
}

// StagedBackend's two overrides, asserted directly rather than through Run.
func TestStagedBackendOverrides(t *testing.T) {
	_, _, orch := newTestServer(t)
	gw := newFakeGateway(t)
	b := lease.NewStagedBackend(lease.NewClient(gw.srv.URL, "key", "secret", orch.Obs))

	// NeedsPipeline false, or the orchestrator demands a tar that cannot exist.
	if b.NeedsPipeline() {
		t.Error("NeedsPipeline must be false: a staged job carries no payload")
	}

	// Commit must graft without uploading. The embedded Client.Commit would
	// refuse outright ("ObjectStore must be set"), so reaching /graft with a nil
	// ObjectStore is itself the proof that the override is in effect.
	err := b.Commit(context.Background(), lease.CommitRequest{
		Token:               "tok-1",
		DirectGraft:         true,
		NewRootHashSuffixed: stagedCatalog,
	})
	if err != nil {
		t.Fatalf("staged commit: %v", err)
	}
	if !gw.sawPath("/graft") {
		t.Errorf("commit did not reach the graft endpoint; gateway saw: %v", gw.paths)
	}
}
