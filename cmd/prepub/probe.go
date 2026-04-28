package main

// probe.go — startup readiness checks for cvmfs-prepub.
//
// Formerly internal/probe/probe.go.  The logic lives here (package main) so
// it can be tested alongside the startup code without exposing a separate
// importable package.  External callers (there were none) should inline the
// same two-step pattern: runCASProbe + backend.Probe.

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
	probeTimeout = 10 * time.Second
)

// runProbe validates that the CAS (when needed) and the publish backend are
// operational.  It returns the first error encountered, or nil if all checks
// pass.
//
// CAS probing is skipped when backend.NeedsPipeline() returns false (local
// mode), because the pipeline — and therefore the CAS — is not used.
func runProbe(ctx context.Context, casBackend cas.Backend, backend lease.Backend, obs *observe.Provider) error {
	if backend.NeedsPipeline() {
		if err := runCASProbe(ctx, casBackend, obs); err != nil {
			return fmt.Errorf("CAS probe failed: %w", err)
		}
	}
	if err := backend.Probe(ctx); err != nil {
		return fmt.Errorf("backend probe failed: %w", err)
	}
	return nil
}

// runCASProbe writes a zero-byte sentinel object, confirms it is visible, then
// deletes it.  This exercises the full write path including any atomic-rename
// and hash-verification logic in the CAS backend.
func runCASProbe(ctx context.Context, backend cas.Backend, obs *observe.Provider) error {
	pctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	if obs != nil {
		_, span := obs.Tracer.Start(pctx, "probe.cas")
		defer span.End()
	}

	if err := backend.Put(pctx, probeHash, strings.NewReader(""), 0); err != nil {
		return fmt.Errorf("put: %w", err)
	}

	ok, err := backend.Exists(pctx, probeHash)
	if err != nil {
		return fmt.Errorf("exists check: %w", err)
	}
	if !ok {
		return fmt.Errorf("object not found after put")
	}

	if err := backend.Delete(pctx, probeHash); err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	return nil
}
