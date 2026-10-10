// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package main

// An opt-in pprof listener.
//
// Why this exists: a publisher that wedges is not debuggable from the outside.
// The pipeline is a set of stages joined by bounded channels, so one stage that
// stops returning parks every other stage behind it — at zero CPU, with no log
// output, because the stages only log at their boundaries. From the outside that
// is indistinguishable from idling, and the only remaining tool is SIGQUIT.
//
// SIGQUIT is a poor tool for it. It kills the process, so the state you wanted
// to inspect is gone either way, and the traceback goes to stderr — which under
// systemd is a pipe to journald, which rate-limits. A dump of a few thousand
// lines then trickles out at seconds per line and is truncated by whatever
// window you happened to capture. That is not a hypothetical: it cost an
// afternoon on a stalled production publish, and the dump was never recovered.
//
// `curl localhost:6060/debug/pprof/goroutine?debug=2` answers the same question
// in one second, without killing anything, and shows every goroutine's stack
// including how long it has been blocked.
//
// It is off by default and must be bound explicitly. Profiles expose process
// memory — heap dumps can contain credentials, signing keys and payload bytes —
// so this must never be reachable from off-host. There is no authentication
// here on purpose: a loopback bind is the access control, and pretending
// otherwise would invite someone to expose it.

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"

	"cvmfs.io/prepub/pkg/observe"
)

// startDebugListener starts the pprof server on addr. A empty addr disables it.
// The returned function shuts the listener down.
func startDebugListener(addr string, obs *observe.Provider) (stop func(), err error) {
	if strings.TrimSpace(addr) == "" {
		return func() {}, nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("debug listener on %q: %w", addr, err)
	}

	// Loudly, because the consequence of getting this wrong is that anyone who
	// can reach the port can read the process's memory.
	if !isLoopback(ln.Addr()) {
		obs.Logger.Warn("debug listener is NOT bound to loopback — pprof exposes heap contents, "+
			"which can include credentials, signing keys and payload bytes; there is no "+
			"authentication on this port",
			"addr", ln.Addr().String())
	}

	srv := &http.Server{
		Handler: mux,
		// A profile capture legitimately runs for 30s+, so no write timeout;
		// the header timeout still bounds a slow-header attack.
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if serr := srv.Serve(ln); serr != nil && serr != http.ErrServerClosed {
			obs.Logger.Error("debug listener stopped", "error", serr)
		}
	}()

	obs.Logger.Info("debug listener enabled",
		"addr", ln.Addr().String(),
		"goroutines", "curl http://"+ln.Addr().String()+"/debug/pprof/goroutine?debug=2",
		"note", "use this instead of SIGQUIT on a wedged publisher")

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}, nil
}

// isLoopback reports whether the listener is confined to the local host.
func isLoopback(a net.Addr) bool {
	host, _, err := net.SplitHostPort(a.String())
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
