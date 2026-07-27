// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

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
	// It must be a VALID CVMFS CAS key or backends that validate keys reject
	// it: CVMFS accepts SHA-1 (40 hex), RIPEMD-160 (47) and SHAKE-128 (49);
	// a 64-char SHA-256 is not in the enum and panics the C++ receiver.
	// This is the first 40 hex chars of SHA-256("") — valid in shape,
	// recognisable in an audit, and not the hash of any real content.
	probeHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4"

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

	// Prefer a read-only probe when the backend offers one: a remote object
	// store should not be written to just because the service restarted.
	if p, okProber := backend.(cas.Prober); okProber {
		if err := p.Probe(pctx); err != nil {
			return err
		}
		return nil
	}

	// Was it already there? Then it is not ours to remove.
	preExisting, err := backend.Exists(pctx, probeHash)
	if err != nil {
		return fmt.Errorf("exists check: %w", err)
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

	if !preExisting {
		if err := backend.Delete(pctx, probeHash); err != nil {
			return fmt.Errorf("delete: %w", err)
		}
	}

	return nil
}
