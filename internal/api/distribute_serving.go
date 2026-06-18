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

// DistributeServing holds the dependencies for the pull-based serving routes
// (ADR-0001 P1/P2). It is mounted only when the publisher runs with
// --distribute-mode pull, so the default (push) server is byte-for-byte
// unchanged.
type DistributeServing struct {
	CAS       cas.Backend
	Manifests serve.ManifestStore
	// Admission, when set, mounts POST /s1/{txn}/lease for receiver admission
	// control (ADR D6). Satisfied by *commit.Admission.
	Admission serve.LeaseGranter
	// Diff, when set, mounts GET /s1/catchup for cumulative catch-up of a
	// receiver that fell behind (ADR D4 / P4). ObjectBaseURLs is the S0 object
	// base URL(s) advertised to receivers in the catch-up manifest header.
	Diff           serve.DiffSource
	ObjectBaseURLs []string
	// CatchupAuth, when set, requires a valid scoped bearer token (scope
	// "catchup") on GET /s1/catchup — the token a receiver obtains by enrolling
	// with its out-of-band node key (data-plane auth). Nil leaves catch-up open
	// (object/manifest GETs remain public regardless).
	CatchupAuth *credential.Verifier
	// Enroll, when set, mounts the challenge/enroll endpoints
	// (GET /control/challenge, POST /control/enroll) so receivers can exchange
	// their out-of-band node key for a short-lived data-plane token.
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
// addressed, public default — ADR D8); the producer-facing manifest POST
// (gateway or pipeline) requires the bearer token.
func mountDistributeServing(router *mux.Router, requireAuth mux.MiddlewareFunc, log *slog.Logger, d DistributeServing) {
	if d.CAS != nil {
		router.PathPrefix("/cvmfs/").
			Handler(&serve.ObjectHandler{Store: d.CAS}).
			Methods(http.MethodGet, http.MethodHead)
		// Chunked-bundle endpoint: many objects in one streamed response (P-A).
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
	if d.Admission != nil {
		router.Handle("/s1/{txn}/lease", &serve.LeaseHandler{Admission: d.Admission}).
			Methods(http.MethodPost)
	}
	if d.Diff != nil && len(d.ObjectBaseURLs) > 0 {
		var catchup http.Handler = &serve.CatchupHandler{
			Source:   d.Diff,
			BaseURLs: d.ObjectBaseURLs,
		}
		// Gate catch-up behind a scoped bearer token when configured (data-plane
		// auth): the receiver enrols with its out-of-band key to obtain it.
		catchup = credential.RequireToken(d.CatchupAuth, "catchup")(catchup)
		router.Handle("/s1/catchup", catchup).Methods(http.MethodGet)
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
			"objects", d.CAS != nil, "manifests", d.Manifests != nil,
			"catchup", d.Diff != nil)
	}
}
