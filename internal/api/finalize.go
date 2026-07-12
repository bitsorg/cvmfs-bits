// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"cvmfs.io/prepub/internal/buildset"

	"github.com/gorilla/mux"
)

// FinalizeResult summarises a coarse-publish finalize.
type FinalizeResult struct {
	BuildID   string              `json:"build_id"`
	Repo      string              `json:"repo"`
	Packages  int                 `json:"packages"`
	Published int                 `json:"published"`
	Conflicts []buildset.Conflict `json:"conflicts"`
	Output    string              `json:"output,omitempty"`
}

// FinalizeBuild publishes a whole build's accumulated packages (ADR-0007 coarse
// publish) in one ingestsql commit, using the orchestrator's configured ingest
// settings. It is invoked both by the finalize job (Orchestrator.Run) and by the
// /builds/{id}/finalize endpoint. On success the build accumulator is removed.
func (o *Orchestrator) FinalizeBuild(ctx context.Context, buildID string) (*FinalizeResult, error) {
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

	work, err := os.MkdirTemp("", "finalize-"+buildID+"-")
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

func writeFinalizeJSON(w http.ResponseWriter, code int, body map[string]interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}
