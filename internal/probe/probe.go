// Package probe provides a startup readiness check that validates the two
// external dependencies — the content-addressable store (CAS) and the
// cvmfs_gateway — are reachable and behaving correctly before the service
// begins accepting jobs.
//
// A failed probe causes the service to exit rather than accept jobs it cannot
// process, making misconfiguration visible immediately rather than silently
// corrupting the spool.
package probe

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/internal/lease"
	"cvmfs.io/prepub/pkg/observe"
)

const (
	// probeHash is a well-known sentinel value used for the CAS round-trip.
	// It is the SHA-256 hash of the empty string, which the probe writes and
	// immediately deletes.  Using a fixed value makes it easy to filter out
	// probe artefacts in CAS audits.
	probeHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	// probeTimeout is the per-operation deadline applied to each probe step.
	// It is deliberately generous to accommodate cold-start latency while still
	// catching genuinely unreachable backends quickly.
	probeTimeout = 10 * time.Second

	// probeRepo is a placeholder repo name used when acquiring the test lease.
	probeRepo = "__probe__"
)

// Run executes a CAS round-trip (put → exists → delete) and a gateway
// lease round-trip (acquire → release) to confirm both dependencies are
// operational.  It returns the first error encountered, or nil if all checks
// pass.
//
// obs may be nil for callers that run the probe before the observability
// provider is fully initialised.
func Run(ctx context.Context, casBackend cas.Backend, leaseClient *lease.Client, obs *observe.Provider) error {
	if err := probeCAS(ctx, casBackend, obs); err != nil {
		return fmt.Errorf("CAS probe failed: %w", err)
	}
	if err := probeGateway(ctx, leaseClient, obs); err != nil {
		return fmt.Errorf("gateway probe failed: %w", err)
	}
	return nil
}

// probeCAS writes a zero-byte sentinel object, confirms it is visible, then
// deletes it.  This exercises the full write path including any atomic-rename
// and hash-verification logic in the CAS backend.
func probeCAS(ctx context.Context, backend cas.Backend, obs *observe.Provider) error {
	pctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	if obs != nil {
		_, span := obs.Tracer.Start(pctx, "probe.cas")
		defer span.End()
	}

	// Put a zero-byte object with the well-known empty-string hash.
	if err := backend.Put(pctx, probeHash, strings.NewReader(""), 0); err != nil {
		return fmt.Errorf("put: %w", err)
	}

	// Confirm it is immediately visible.
	ok, err := backend.Exists(pctx, probeHash)
	if err != nil {
		return fmt.Errorf("exists check: %w", err)
	}
	if !ok {
		return fmt.Errorf("object not found after put")
	}

	// Clean up — a startup probe must leave no side-effects.
	if err := backend.Delete(pctx, probeHash); err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	return nil
}

// probeGateway acquires a test lease against a sentinel repo path and
// immediately releases it without committing.
func probeGateway(ctx context.Context, client *lease.Client, obs *observe.Provider) error {
	pctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	if obs != nil {
		_, span := obs.Tracer.Start(pctx, "probe.gateway")
		defer span.End()
	}

	l, err := client.Acquire(pctx, probeRepo, probeRepo)
	if err != nil {
		return fmt.Errorf("acquire lease: %w", err)
	}

	if err := client.Release(pctx, l.Token, false); err != nil {
		// Non-fatal: the lease will expire on its own.  Log and continue so
		// a gateway that accepts acquires but rejects releases doesn't block
		// startup indefinitely.
		return fmt.Errorf("release lease (non-blocking): %w", err)
	}

	return nil
}
