// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"cvmfs.io/prepub/internal/buildset"

	"github.com/gorilla/mux"
)

// preflightObjects verifies that a sample of the build's referenced content
// objects is still present in CAS before the finalize commits catalogs.
// Accumulated objects are unreferenced by any published catalog until the
// finalize, so a storage wipe / GC between accumulation and finalize would
// otherwise publish a fully-browsable tree whose every read fails with EIO
// (seen in production after a testbed volume rebuild between a failed and a
// retried finalize). A sample of up to two objects per member, capped at 200
// Exists probes, is cheap (stat/HEAD each) and reliably detects a lost store.
func (o *Orchestrator) preflightObjects(ctx context.Context, members []buildset.Member) error {
	if o.CAS == nil {
		return nil // no CAS backend configured (non-gateway deployments)
	}
	type ref struct{ key, path string }
	var refs []ref
	seen := make(map[string]struct{})
	for _, m := range members {
		picked := 0
		for _, e := range m.Entries {
			if picked >= 2 {
				break
			}
			var key string
			switch {
			case e.IsDelete || e.Size == 0:
				continue // deletions carry no object; empty blobs may be elided
			case len(e.Chunks) > 0 && len(e.Chunks[0].Hash) > 0:
				key = hex.EncodeToString(e.Chunks[0].Hash) + "P"
			case len(e.Hash) > 0 && e.Mode.IsRegular():
				key = hex.EncodeToString(e.Hash)
			default:
				continue // dirs, symlinks
			}
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			refs = append(refs, ref{key, m.Path + e.FullPath})
			picked++
		}
	}
	// Cap the probe count: stride-sample evenly so every region of the build
	// is still represented.
	const maxProbes = 200
	if len(refs) > maxProbes {
		sampled := make([]ref, 0, maxProbes)
		stride := len(refs) / maxProbes
		for i := 0; i < len(refs) && len(sampled) < maxProbes; i += stride + 1 {
			sampled = append(sampled, refs[i])
		}
		refs = sampled
	}
	missing := 0
	var first ref
	for _, r := range refs {
		ok, err := o.CAS.Exists(ctx, r.key)
		if err != nil {
			return fmt.Errorf("finalize pre-flight: CAS existence check %s: %w", r.key, err)
		}
		if !ok {
			missing++
			if first.key == "" {
				first = r
			}
		}
	}
	if missing > 0 {
		return fmt.Errorf(
			"finalize pre-flight: %d of %d sampled content objects missing from CAS "+
				"(first: %s for %s) — the store no longer holds this build's accumulated "+
				"content (wiped or GC'd since accumulation?); re-publish the build",
			missing, len(refs), first.key, first.path)
	}
	return nil
}

// FinalizeResult summarises a coarse-publish finalize.
type FinalizeResult struct {
	BuildID   string              `json:"build_id"`
	Repo      string              `json:"repo"`
	Packages  int                 `json:"packages"`
	Published int                 `json:"published"`
	Conflicts []buildset.Conflict `json:"conflicts"`
	Output    string              `json:"output,omitempty"`
}

// defaultAutoFinalizeTimeout bounds an auto-finalize when no JobTimeout is
// configured.  A whole-build ingestsql commit legitimately takes many minutes;
// this only has to stop a hung one from pinning the claim forever.
const defaultAutoFinalizeTimeout = 2 * time.Hour

// FinalizeBuild publishes a whole build's accumulated packages (ADR-0007 coarse
// publish) in one ingestsql commit, using the orchestrator's configured ingest
// settings. It is invoked by the finalize job (Orchestrator.Run), by the
// /builds/{id}/finalize endpoint, and by auto-finalize. On success the build
// accumulator is removed.
//
// Calls for the same build are serialised: three entry points reach this
// function, and a lost seal response is enough to make two of them fire at once
// (the CI falls back to submitting a finalize job while auto-finalize is
// already running). Two concurrent ingestsql commits over the same descriptor
// set are not something to find out about in production.
func (o *Orchestrator) FinalizeBuild(ctx context.Context, buildID string) (*FinalizeResult, error) {
	mu, _ := o.finalizeMu.LoadOrStore(buildID, &sync.Mutex{})
	lock := mu.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()

	if o.IngestConfigPrefix == "" {
		return nil, fmt.Errorf("finalize is not configured on this prepub (ingest config prefix unset)")
	}
	spoolRoot := o.Spool.Root
	members, err := buildset.Load(spoolRoot, buildID)
	if err != nil {
		return nil, fmt.Errorf("load build %q: %w", buildID, err)
	}
	if len(members) == 0 {
		return nil, fmt.Errorf("no accumulated packages for build %q", buildID)
	}
	repo := members[0].Repo
	for _, m := range members {
		if m.Repo != repo {
			return nil, fmt.Errorf("build %q spans multiple repositories", buildID)
		}
	}
	if err := o.preflightObjects(ctx, members); err != nil {
		return nil, err
	}

	// Scratch under the spool root (the sized, persistent volume), NOT the
	// container /tmp: the descriptor and ingestsql scratch for a whole build
	// (87 packages / 170 members for O2) can exceed a small tmpfs, and a full
	// /tmp surfaced as an ingestsql crash (mkstemp -> ENOSPC) rather than a
	// clear error.
	work, err := os.MkdirTemp(spoolRoot, ".finalize-"+buildID+"-")
	if err != nil {
		return nil, fmt.Errorf("finalize workdir: %w", err)
	}
	defer os.RemoveAll(work)
	ingestTmp := filepath.Join(work, "ingest-tmp")
	if err := os.MkdirAll(ingestTmp, 0o755); err != nil {
		return nil, fmt.Errorf("finalize tmpdir: %w", err)
	}

	swissknife := o.IngestSwissknife
	if swissknife == "" {
		swissknife = "cvmfs_swissknife"
	}
	conflicts, out, ferr := buildset.Finalize(ctx, members, filepath.Join(work, "descriptor.db"),
		buildset.IngestOptions{
			Swissknife:   swissknife,
			Repo:         repo,
			ConfigPrefix: o.IngestConfigPrefix,
			TempDir:      ingestTmp,
			ExtraEnv:     o.IngestEnv,
		})
	res := &FinalizeResult{
		BuildID:   buildID,
		Repo:      repo,
		Packages:  len(members),
		Published: len(members) - len(conflicts),
		Conflicts: conflicts,
		Output:    out,
	}
	if ferr != nil {
		return res, ferr
	}
	if rmErr := buildset.Remove(spoolRoot, buildID); rmErr != nil {
		o.Obs.Logger.Warn("finalize: could not remove accumulator", "build_id", buildID, "error", rmErr)
	}
	return res, nil
}

// maybeAutoFinalize publishes a build as soon as its last package has been
// accumulated, so the producer does not have to stay alive polling every job to
// a terminal state just to learn when it may request the finalize.
//
// It runs only when the submitter declared the build's package count
// (build_expect); without a declaration the behaviour is unchanged and the
// finalize must be requested explicitly.
//
// Called from the job goroutine after a successful transition to
// StateAccumulated.  Several jobs of the same build can observe completeness at
// the same instant; buildset.ClaimFinalize resolves that with an O_EXCL marker,
// so exactly one of them proceeds.  The finalize itself is detached: it can run
// for many minutes and must not hold the caller's job context, which is
// cancelled as soon as that job returns.
func (o *Orchestrator) maybeAutoFinalize(buildID string) {
	if buildID == "" {
		return
	}
	spoolRoot := o.Spool.Root
	expect := buildset.Expect(spoolRoot, buildID)
	if expect <= 0 {
		return // no declaration — wait for an explicit finalize
	}
	// Compare against TERMINAL jobs, not accumulated members: a build whose
	// last package failed would otherwise never reach its declared count, and
	// since the producer has already exited nobody would ever notice.
	if n := buildset.Terminal(spoolRoot, buildID); n < expect {
		return
	}

	failures := buildset.Failures(spoolRoot, buildID)
	if len(failures) > 0 {
		// Reaching a decision is the point; the decision is "do not publish".
		// Committing the packages that succeeded would put a build into the
		// repository that no producer ever asked for and nobody is watching.
		// The accumulator is left intact so an operator can inspect it and, if
		// the partial set is genuinely wanted, force it with
		// POST /builds/{id}/finalize.
		if claimed, err := buildset.ClaimFinalize(spoolRoot, buildID); err != nil || !claimed {
			return // already decided (or reported) by another goroutine
		}
		o.Obs.Logger.Error("build will NOT be auto-published: some jobs failed",
			"build_id", buildID, "expect", expect,
			"accumulated", buildset.Count(spoolRoot, buildID),
			"failed", failures,
			"hint", "inspect the failed jobs; POST /builds/{id}/finalize publishes the partial set deliberately")
		_ = buildset.WriteResult(spoolRoot, buildset.Result{
			BuildID:   buildID,
			Packages:  buildset.Count(spoolRoot, buildID),
			Published: 0,
			Error: fmt.Sprintf("%d of %d jobs failed (%s) — not published",
				len(failures), expect, strings.Join(failures, ", ")),
			At: time.Now().UTC(),
		})
		return
	}

	if o.IngestConfigPrefix == "" {
		// Declared but unpublishable: say so once, loudly, rather than leaving
		// the producer waiting for a finalize that can never happen.
		o.Obs.Logger.Error("build complete but finalize is not configured on this prepub "+
			"(ingest config prefix unset) — publish it manually with POST /builds/{id}/finalize",
			"build_id", buildID, "expect", expect)
		return
	}
	claimed, err := buildset.ClaimFinalize(spoolRoot, buildID)
	if err != nil {
		o.Obs.Logger.Error("auto-finalize: could not claim build", "build_id", buildID, "error", err)
		return
	}
	if !claimed {
		return // another job goroutine is finalizing this build
	}

	// Tracked so that Shutdown waits for an in-flight commit instead of letting
	// systemd SIGKILL it half-way through ingestsql.
	o.finalizeWg.Add(1)
	go func() {
		defer o.finalizeWg.Done()
		o.runAutoFinalize(buildID, expect)
	}()
}

// runAutoFinalize executes the claimed finalize on its own goroutine and
// records the outcome where the producer (and the console) can read it.
func (o *Orchestrator) runAutoFinalize(buildID string, expect int) {
	logger := o.Obs.Logger.With("build_id", buildID, "packages", expect)
	logger.Info("auto-finalize: build complete, publishing")

	// Detached from any job context — the last package's context is cancelled
	// the moment its goroutine returns, which would kill the ingestsql commit —
	// but still bounded, so a hung cvmfs_swissknife cannot pin the claim and a
	// goroutine forever.
	timeout := o.JobTimeout
	if timeout <= 0 {
		timeout = defaultAutoFinalizeTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := o.FinalizeBuild(ctx, buildID)

	outcome := buildset.Result{BuildID: buildID, At: time.Now().UTC()}
	if res != nil {
		outcome.Repo = res.Repo
		outcome.Packages = res.Packages
		outcome.Published = res.Published
	}
	if err != nil {
		outcome.Error = err.Error()
		// Only release the claim when nothing was committed: FinalizeBuild
		// returns a nil result for load/validation failures (safe to retry) and
		// a non-nil one once ingestsql has run (repository state may already
		// have changed — a human decides what happens next).
		if res == nil {
			if relErr := buildset.ReleaseFinalize(o.Spool.Root, buildID); relErr != nil {
				logger.Warn("auto-finalize: could not release claim", "error", relErr)
			}
			logger.Error("auto-finalize failed before commit — will retry when another package accumulates, "+
				"or publish manually with POST /builds/{id}/finalize", "error", err)
		} else {
			logger.Error("auto-finalize failed during commit — claim retained, inspect before retrying",
				"error", err, "published", outcome.Published)
		}
	} else {
		logger.Info("auto-finalize: build published",
			"repo", outcome.Repo, "published", outcome.Published,
			"conflicts", len(res.Conflicts))
	}

	if werr := buildset.WriteResult(o.Spool.Root, outcome); werr != nil {
		logger.Warn("auto-finalize: could not record result", "error", werr)
	}
}

// finalizeBuild handles POST /api/v1/builds/{id}/finalize — an out-of-band way
// to publish an accumulated build (the primary path is a finalize job). It uses
// the prepub's configured ingest settings.
func (s *Server) finalizeBuild(w http.ResponseWriter, r *http.Request) {
	buildID := mux.Vars(r)["id"]
	res, err := s.orch.FinalizeBuild(r.Context(), buildID)
	if err != nil {
		code := http.StatusInternalServerError
		if res == nil {
			code = http.StatusBadRequest // load/validation error, nothing published
		}
		body := map[string]interface{}{"build_id": buildID, "error": err.Error()}
		if res != nil {
			body["packages"] = res.Packages
			body["published"] = res.Published
			body["conflicts"] = res.Conflicts
			body["output"] = res.Output
		}
		s.obs.Logger.Error("build finalize failed", "build_id", buildID, "error", err)
		writeFinalizeJSON(w, code, body)
		return
	}
	s.obs.Logger.Info("build finalized", "build_id", buildID, "repo", res.Repo,
		"packages", res.Packages, "conflicts", len(res.Conflicts))
	writeFinalizeJSON(w, http.StatusOK, map[string]interface{}{
		"build_id": res.BuildID, "repo": res.Repo, "packages": res.Packages,
		"published": res.Published, "conflicts": res.Conflicts,
	})
}

// sealBuild handles POST /api/v1/builds/{id}/seal — the producer declares that
// it has submitted everything and states how many jobs it sent.  prepub
// finalizes the build by itself once that many members have accumulated,
// whether that already happened (the common case for a fast build) or happens
// minutes later.
//
// This is the endpoint that lets a CI pipeline exit right after its last
// upload.  Declaring the count up front is often impossible — a package may
// contribute one job or two, depending on whether it ships a modulefile — so
// the count is stated at the end, when it is simply "how many did I send".
//
// Seal is idempotent: re-sealing with the same count is a no-op, and re-sealing
// after the finalize has been claimed reports the current status rather than
// starting a second commit.
func (s *Server) sealBuild(w http.ResponseWriter, r *http.Request) {
	buildID := mux.Vars(r)["id"]

	var req struct {
		Expect int `json:"expect"`
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	if err != nil {
		http.Error(w, `{"error":"failed to read request body"}`, http.StatusBadRequest)
		return
	}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, `{"error":"invalid JSON body"}`, http.StatusBadRequest)
			return
		}
	}
	if req.Expect <= 0 {
		http.Error(w, `{"error":"expect must be a positive integer (the number of jobs submitted for this build)"}`, http.StatusBadRequest)
		return
	}

	// A seal may never shrink a build.  Sealing below what has already arrived
	// would finalize a subset immediately and then remove the accumulator, so
	// members still in flight would be silently dropped — most plausibly when a
	// retried pipeline reuses the same build_id for a smaller package set, and
	// trivially abusable by anyone holding the bearer token.
	if terminal := buildset.Terminal(s.spoolRoot, buildID); req.Expect < terminal {
		s.obs.Logger.Warn("seal rejected: count below jobs already finished",
			"build_id", buildID, "expect", req.Expect, "terminal", terminal)
		writeFinalizeJSON(w, http.StatusConflict, map[string]interface{}{
			"build_id": buildID,
			"error": fmt.Sprintf("expect (%d) is below the %d job(s) already finished for this build; "+
				"a seal may not shrink a build", req.Expect, terminal),
		})
		return
	}
	if declared := buildset.Expect(s.spoolRoot, buildID); req.Expect < declared {
		s.obs.Logger.Warn("seal rejected: count below previous declaration",
			"build_id", buildID, "expect", req.Expect, "declared", declared)
		writeFinalizeJSON(w, http.StatusConflict, map[string]interface{}{
			"build_id": buildID,
			"error": fmt.Sprintf("expect (%d) is below the previously declared %d; "+
				"a seal may not shrink a build", req.Expect, declared),
		})
		return
	}

	if err := buildset.SetExpect(s.spoolRoot, buildID, req.Expect); err != nil {
		s.obs.Logger.Error("seal: could not record expectation", "build_id", buildID, "error", err)
		http.Error(w, `{"error":"internal error recording build expectation"}`, http.StatusInternalServerError)
		return
	}
	s.obs.Logger.Info("build sealed", "build_id", buildID, "expect", req.Expect,
		"accumulated", buildset.Count(s.spoolRoot, buildID))

	// Evaluate completeness now: when every job finished before the seal
	// arrived — the usual case for a small build — no further accumulation
	// event will occur to trigger the finalize.
	s.orch.maybeAutoFinalize(buildID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(buildset.GetStatus(s.spoolRoot, buildID))
}

// buildStatus handles GET /api/v1/builds/{id} — how many packages have
// accumulated, whether the finalize has been claimed, and the outcome once it
// has run.  A producer that declared build_expect can exit after its last
// upload and, if it wants confirmation at all, make a single call here.
func (s *Server) buildStatus(w http.ResponseWriter, r *http.Request) {
	st := buildset.GetStatus(s.spoolRoot, mux.Vars(r)["id"])
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(st)
}

func writeFinalizeJSON(w http.ResponseWriter, code int, body map[string]interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}
