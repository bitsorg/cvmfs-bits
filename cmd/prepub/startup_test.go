package main

// startup_test.go — unit tests for the startup readiness-probe logic in probe.go.
//
// These tests were migrated from internal/probe/probe_test.go when the probe
// package was retired and its code moved into cmd/prepub (package main).
//
// Tests covered:
//   - TestRunProbe_HappyPath        — healthy CAS + gateway both pass
//   - TestRunProbe_CASFailure       — CAS unavailable causes runProbe to return an error
//   - TestRunProbe_GatewayFailure   — gateway unreachable triggers backend probe failure
//   - TestRunProbe_ContextCancelled — a cancelled context is propagated correctly
//
// Dependencies:
//   - testutil/fakecas        — in-memory CAS backend for tests
//   - testutil/fakegateway    — in-memory HTTP gateway for tests
//   - internal/lease          — lease.NewClient is used as the Backend under test
//   - pkg/observe             — observability provider wired through tests

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cvmfs.io/prepub/internal/lease"
	"cvmfs.io/prepub/pkg/observe"
	"cvmfs.io/prepub/testutil/fakecas"
	"cvmfs.io/prepub/testutil/fakegateway"
)

func newStartupObs(t *testing.T) *observe.Provider {
	t.Helper()
	obs, shutdown, err := observe.New("startup-test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)
	return obs
}

// TestRunProbe_HappyPath verifies that a healthy CAS and gateway both pass.
func TestRunProbe_HappyPath(t *testing.T) {
	obs := newStartupObs(t)
	c := fakecas.New(obs)
	gw := fakegateway.New(obs)
	defer gw.Close()

	leaseClient := lease.NewClient(gw.URL(), "test-key", "test-secret", obs)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := runProbe(ctx, c, leaseClient, obs); err != nil {
		t.Fatalf("probe failed on healthy backends: %v", err)
	}

	// The probe sentinel object must be cleaned up after the probe.
	ok, err := c.Exists(ctx, probeHash)
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if ok {
		t.Error("probe left sentinel object in CAS — Delete must be called after probe")
	}
}

// TestRunProbe_CASFailure verifies that a CAS Put error surfaces as a probe failure.
func TestRunProbe_CASFailure(t *testing.T) {
	obs := newStartupObs(t)
	c := &failCASForProbe{err: errors.New("disk full")}
	gw := fakegateway.New(obs)
	defer gw.Close()

	leaseClient := lease.NewClient(gw.URL(), "test-key", "test-secret", obs)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := runProbe(ctx, c, leaseClient, obs)
	if err == nil {
		t.Fatal("expected probe to fail when CAS is broken")
	}
	if !strings.Contains(err.Error(), "CAS probe failed") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestRunProbe_GatewayFailure verifies that a gateway that rejects all requests
// surfaces as a probe failure.
func TestRunProbe_GatewayFailure(t *testing.T) {
	obs := newStartupObs(t)
	c := fakecas.New(obs)

	// Stand up a server that always returns 503.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"error":"unavailable"}`)) //nolint:errcheck
	}))
	defer srv.Close()

	leaseClient := lease.NewClient(srv.URL, "test-key", "test-secret", obs)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := runProbe(ctx, c, leaseClient, obs)
	if err == nil {
		t.Fatal("expected probe to fail when gateway is unavailable")
	}
	if !strings.Contains(err.Error(), "backend probe failed") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestRunProbe_ContextCancelled verifies that a cancelled context propagates
// correctly through the probe and returns an error rather than hanging.
func TestRunProbe_ContextCancelled(t *testing.T) {
	obs := newStartupObs(t)
	c := fakecas.New(obs)
	gw := fakegateway.New(obs)
	defer gw.Close()

	leaseClient := lease.NewClient(gw.URL(), "test-key", "test-secret", obs)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := runProbe(ctx, c, leaseClient, obs)
	if err == nil {
		t.Fatal("expected probe to fail with cancelled context")
	}
}

// failCASForProbe is a CAS backend that always returns an error on Put.
type failCASForProbe struct {
	err error
}

func (f *failCASForProbe) Put(_ context.Context, _ string, _ io.Reader, _ int64) error {
	return f.err
}
func (f *failCASForProbe) Exists(_ context.Context, _ string) (bool, error) {
	return false, f.err
}
func (f *failCASForProbe) Get(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, f.err
}
func (f *failCASForProbe) Size(_ context.Context, _ string) (int64, error) {
	return 0, f.err
}
func (f *failCASForProbe) Delete(_ context.Context, _ string) error {
	return f.err
}
func (f *failCASForProbe) List(_ context.Context) ([]string, error) {
	return nil, f.err
}
