// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"cvmfs.io/prepub/internal/buildset"

	"github.com/gorilla/mux"
)

// finalizeBuild handles POST /api/v1/builds/{id}/finalize: it assembles every
// package accumulated under the build (StateAccumulated jobs) into one ingestsql
// descriptor and publishes them in a single gateway commit (ADR-0007 coarse
// publish). The repo and packages come from the accumulated members; the
// ingestsql runtime parameters (which are deployment-specific) are supplied in
// the request body: the swissknife binary, the gateway-client config prefix
// (-C, which carries CVMFS_GATEWAY / CVMFS_STRATUM0 / CVMFS_UPSTREAM_STORAGE and
// the gateway key), an optional explicit lease path, and any environment such
// as LD_LIBRARY_PATH.
func (s *Server) finalizeBuild(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	buildID := mux.Vars(r)["id"]

	var req struct {
		Swissknife   string   `json:"swissknife"`
		ConfigPrefix string   `json:"config_prefix"`
		LeasePath    string   `json:"lease_path"`
		Env          []string `json:"env"`
	}
	if r.Body != nil && r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeFinalizeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": "invalid JSON body"})
			return
		}
	}
	if req.ConfigPrefix == "" {
		writeFinalizeJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": "config_prefix is required (the ingestsql gateway-client config dir)"})
		return
	}

	members, err := buildset.Load(s.spoolRoot, buildID)
	if err != nil {
		writeFinalizeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": fmt.Sprintf("load build %q: %v", buildID, err)})
		return
	}
	if len(members) == 0 {
		writeFinalizeJSON(w, http.StatusNotFound, map[string]interface{}{
			"error": "no accumulated packages for build " + buildID})
		return
	}
	repo := members[0].Repo
	for _, m := range members {
		if m.Repo != repo {
			writeFinalizeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"error": "build spans multiple repositories"})
			return
		}
	}

	work, err := os.MkdirTemp(s.spoolRoot, "finalize-")
	if err != nil {
		writeFinalizeJSON(w, http.StatusInternalServerError, map[string]interface{}{"error": "workdir: " + err.Error()})
		return
	}
	defer os.RemoveAll(work)
	tmp := filepath.Join(work, "ingest-tmp")
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		writeFinalizeJSON(w, http.StatusInternalServerError, map[string]interface{}{"error": "tmpdir: " + err.Error()})
		return
	}

	conflicts, out, ferr := buildset.Finalize(ctx, members, filepath.Join(work, "descriptor.db"),
		buildset.IngestOptions{
			Swissknife:   req.Swissknife,
			Repo:         repo,
			ConfigPrefix: req.ConfigPrefix,
			TempDir:      tmp,
			LeasePath:    req.LeasePath,
			ExtraEnv:     req.Env,
		})

	resp := map[string]interface{}{
		"build_id":  buildID,
		"repo":      repo,
		"packages":  len(members),
		"published": len(members) - len(conflicts),
		"conflicts": conflicts,
		"output":    out,
	}
	if ferr != nil {
		resp["error"] = ferr.Error()
		s.obs.Logger.Error("build finalize failed", "build_id", buildID, "repo", repo, "error", ferr)
		writeFinalizeJSON(w, http.StatusInternalServerError, resp)
		return
	}

	// Success: drop the accumulator so the build cannot be re-finalized.
	if rmErr := buildset.Remove(s.spoolRoot, buildID); rmErr != nil {
		s.obs.Logger.Warn("build finalize: could not remove accumulator", "build_id", buildID, "error", rmErr)
	}
	s.obs.Logger.Info("build finalized", "build_id", buildID, "repo", repo,
		"packages", len(members), "conflicts", len(conflicts))
	writeFinalizeJSON(w, http.StatusOK, resp)
}

func writeFinalizeJSON(w http.ResponseWriter, code int, body map[string]interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}
