// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"log/slog"
	"net/http"

	"github.com/gorilla/mux"

	"cvmfs.io/prepub/internal/cas"
	"cvmfs.io/prepub/internal/distribute/credential"
	"cvmfs.io/prepub/internal/distribute/serve"
)

// DistributeServing holds the dependencies for the pull-distribution serving
// routes (objects, manifests, bundles, enrollment).
type DistributeServing struct {
	CAS       cas.Backend
	Manifests serve.ManifestStore
	// Enroll, when set, mounts the challenge/enroll endpoints
	// (GET /control/challenge, POST /control/enroll) so receivers can exchange
	// their out-of-band node key for a short-lived control-plane token.
	Enroll *credential.EnrollServer
	// RateLimit, when set, wraps the control endpoints (enroll) to bound request
	// floods (R-DoS). Typically credential.IPRateLimiter.Middleware.
	RateLimit func(http.Handler) http.Handler
}

// MountDistributeServing registers the pull-distribution routes on the server's
// router. Call once, before ListenAndServe.
func (s *Server) MountDistributeServing(d DistributeServing) {
	mountDistributeServing(s.router, s.requireAuth, s.obs.Logger, d)
}

// mountDistributeServing is the testable core (no *Server required). The
// receiver-facing object and manifest GETs are unauthenticated (content-
// addressed, public by default); the producer-facing manifest POST
// (gateway or pipeline) requires the bearer token.
func mountDistributeServing(router *mux.Router, requireAuth mux.MiddlewareFunc, log *slog.Logger, d DistributeServing) {
	if d.CAS != nil {
		router.PathPrefix("/cvmfs/").
			Handler(&serve.ObjectHandler{Store: d.CAS}).
			Methods(http.MethodGet, http.MethodHead)
		// Chunked-bundle endpoint: many objects in one streamed response.
		router.Handle("/s1/bundle", &serve.BundleHandler{Store: d.CAS}).
			Methods(http.MethodPost)
	}
	if d.Manifests != nil {
		router.Handle("/s1/{txn}/manifest", &serve.ManifestHandler{Source: d.Manifests}).
			Methods(http.MethodGet)

		ingest := router.PathPrefix("/api/v1/distribute/manifests").Subrouter()
		if requireAuth != nil {
			ingest.Use(requireAuth)
		}
		ingest.Handle("", &serve.ManifestIngestHandler{Store: d.Manifests}).
			Methods(http.MethodPost, http.MethodPut)
	}
	if d.Enroll != nil {
		var eh http.Handler = d.Enroll.Handler()
		if d.RateLimit != nil {
			eh = d.RateLimit(eh)
		}
		router.Handle("/control/challenge", eh).Methods(http.MethodGet)
		router.Handle("/control/enroll", eh).Methods(http.MethodPost)
	}
	if log != nil {
		log.Info("distribute serving mounted (pull mode)",
			"objects", d.CAS != nil, "manifests", d.Manifests != nil)
	}
}
