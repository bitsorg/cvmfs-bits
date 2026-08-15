// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"strings"

	"github.com/gorilla/mux"

	"cvmfs.io/prepub/internal/measure"
)

// measurementsHandler serves GET /api/v1/measurements/{build}.
//
//	{build}          a build id, or "latest" for the most recently written
//	?job=<id>        only that job's record
//	?path=<publish>  only records of one publish path (ingest|staged|prepub)
//	?summary=1       the reduced form: counts, window, exact distributions
//
// The full records are returned by default because the point of this endpoint
// is to let the caller do the arithmetic it wants with jq, rather than to
// guess in advance which reduction is useful. ?summary=1 exists because one
// reduction — the table MEASUREMENTS.md keeps repeating — is worth not
// rewriting each time.
func (s *Server) measurementsHandler(w http.ResponseWriter, r *http.Request) {
	if s.orch == nil || s.orch.Measurements == nil {
		http.Error(w, `{"error":"measurements are not enabled on this prepub"}`,
			http.StatusNotFound)
		return
	}
	build := mux.Vars(r)["build"]
	if build == "latest" {
		builds, err := s.orch.Measurements.Builds()
		if err != nil {
			// A deleted or unreadable directory is not "nothing recorded yet":
			// reporting 404 for it sends the reader looking for a missing run
			// instead of a broken deployment.
			s.obs.Logger.Warn("measurements: listing failed", "error", err)
			http.Error(w, `{"error":"could not list measurements"}`, http.StatusInternalServerError)
			return
		}
		if len(builds) == 0 {
			http.Error(w, `{"error":"no measurements recorded yet"}`, http.StatusNotFound)
			return
		}
		build = builds[0]
	}

	recs, err := s.orch.Measurements.Read(build)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			http.Error(w, `{"error":"no measurements for that build"}`, http.StatusNotFound)
			return
		}
		s.obs.Logger.Warn("measurements: read failed", "build", build, "error", err)
		http.Error(w, `{"error":"could not read measurements"}`, http.StatusInternalServerError)
		return
	}

	if job := r.URL.Query().Get("job"); job != "" {
		recs = filterRecords(recs, func(rec measure.Record) bool { return rec.JobID == job })
	}
	if p := r.URL.Query().Get("path"); p != "" {
		recs = filterRecords(recs, func(rec measure.Record) bool { return rec.PublishPath == p })
	}

	w.Header().Set("Content-Type", "application/json")
	if truthy(r.URL.Query().Get("summary")) {
		writeJSON(w, measure.Summarise(recs))
		return
	}
	// Records, not a wrapper object: `curl … | jq '.[] | .backend_s'` is the
	// intended use, and an envelope would put a step in front of every query.
	if recs == nil {
		recs = []measure.Record{}
	}
	writeJSON(w, recs)
}

// measurementBuildsHandler serves GET /api/v1/measurements — the build ids
// that have records, newest first, so a caller can find a run without knowing
// the pipeline id.
func (s *Server) measurementBuildsHandler(w http.ResponseWriter, _ *http.Request) {
	if s.orch == nil || s.orch.Measurements == nil {
		http.Error(w, `{"error":"measurements are not enabled on this prepub"}`,
			http.StatusNotFound)
		return
	}
	builds, err := s.orch.Measurements.Builds()
	if err != nil {
		s.obs.Logger.Warn("measurements: listing failed", "error", err)
		http.Error(w, `{"error":"could not list measurements"}`, http.StatusInternalServerError)
		return
	}
	if builds == nil {
		builds = []string{}
	}
	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, builds)
}

func filterRecords(in []measure.Record, keep func(measure.Record) bool) []measure.Record {
	out := in[:0:0]
	for _, r := range in {
		if keep(r) {
			out = append(out, r)
		}
	}
	return out
}

func truthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

func writeJSON(w http.ResponseWriter, v any) {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
