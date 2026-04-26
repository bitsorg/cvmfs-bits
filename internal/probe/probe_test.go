package probe

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

func newObs(t *testing.T) *observe.Provider {
	t.Helper()
	obs, shutdown, err := observe.New("probe-test")
	if err != nil {
		t.Fatalf("observe.New: %v", err)
	}
	t.Cleanup(shutdown)
	return obs
}

// TestProbe_HappyPath verifies that a healthy CAS and gateway both pass.
func TestProbe_HappyPath(t *testing.T) {
	obs := newObs(t)
	cas := fakecas.New(obs)
	gw := fakegateway.New(obs)
	defer gw.Close()

	leaseClient := lease.NewClient(gw.URL(), "test-key", "test-secret", obs)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := Run(ctx, cas, leaseClient, obs); err != nil {
		t.Fatalf("probe failed on healthy backends: %v", err)
	}

	// The probe sentinel object must be cleaned up after the probe.
	ok, err := cas.Exists(ctx, probeHash)
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if ok {
		t.Error("probe left sentinel object in CAS — Delete must be called after probe")
	}
}

// TestProbe_CASFailure verifies that a CAS Put error surfaces as a probe failure.
func TestProbe_CASFailure(t *testing.T) {
	obs := newObs(t)
	cas := &failCAS{err: errors.New("disk full")}
	gw := fakegateway.New(obs)
	defer gw.Close()

	leaseClient := lease.NewClient(gw.URL(), "test-key", "test-secret", obs)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := Run(ctx, cas, leaseClient, obs)
	if err == nil {
		t.Fatal("expected probe to fail when CAS is broken")
	}
	if !strings.Contains(err.Error(), "CAS probe failed") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestProbe_GatewayFailure verifies that a gateway that rejects lease
// acquisitions surfaces as a probe failure.
func TestProbe_GatewayFailure(t *testing.T) {
	obs := newObs(t)
	cas := fakecas.New(obs)

	// Stand up a server that always returns 503 for lease requests.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"error":"unavailable"}`))
	}))
	defer srv.Close()

	leaseClient := lease.NewClient(srv.URL, "test-key", "test-secret", obs)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := Run(ctx, cas, leaseClient, obs)
	if err == nil {
		t.Fatal("expected probe to fail when gateway is unavailable")
	}
	if !strings.Contains(err.Error(), "backend probe failed") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestProbe_ContextCancelled verifies that a cancelled context propagates
// correctly through the probe and returns an error rather than hanging.
func TestProbe_ContextCancelled(t *testing.T) {
	obs := newObs(t)
	cas := fakecas.New(obs)
	gw := fakegateway.New(obs)
	defer gw.Close()

	leaseClient := lease.NewClient(gw.URL(), "test-key", "test-secret", obs)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := Run(ctx, cas, leaseClient, obs)
	if err == nil {
		t.Fatal("expected probe to fail with cancelled context")
	}
}

// failCAS is a CAS backend that always returns an error on Put.
type failCAS struct {
	err error
}

func (f *failCAS) Put(_ context.Context, _ string, _ io.Reader, _ int64) error {
	return f.err
}
func (f *failCAS) Exists(_ context.Context, _ string) (bool, error) {
	return false, f.err
}
func (f *failCAS) Get(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, f.err
}
func (f *failCAS) Delete(_ context.Context, _ string) error {
	return f.err
}
func (f *failCAS) List(_ context.Context) ([]string, error) {
	return nil, f.err
}
