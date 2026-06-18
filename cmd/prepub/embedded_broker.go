// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/tls"

	mqttbroker "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"

	"cvmfs.io/prepub/pkg/observe"
)

// startEmbeddedBroker starts an in-process Mochi MQTT broker exposing a single
// WebSocket listener, so the ADR-0001 control plane runs ON Stratum 0 with no
// separate broker (mosquitto) container. Receivers connect over ws:// (dev) or
// wss:// (prod) and the publisher connects to the same listener on localhost.
//
// Auth is allow-all in this first cut (dev/testbed); production should replace
// the AllowHook with an OnConnectAuthenticate/OnACLCheck hook bound to the
// existing HMAC/token scheme. Caller must call server.Close() on shutdown.
func startEmbeddedBroker(wsAddr string, tlsCfg *tls.Config, authHook *brokerAuthHook, obs *observe.Provider) (func(), *mqttbroker.Server, error) {
	server := mqttbroker.New(&mqttbroker.Options{InlineClient: false})
	if authHook != nil {
		if err := server.AddHook(authHook, nil); err != nil {
			return nil, nil, err
		}
	} else {
		// Dev/testbed default: allow-all. Production passes a token-auth hook.
		if err := server.AddHook(new(auth.AllowHook), nil); err != nil {
			return nil, nil, err
		}
	}
	ws := listeners.NewWebsocket(listeners.Config{ID: "cp-ws", Address: wsAddr, TLSConfig: tlsCfg})
	if err := server.AddListener(ws); err != nil {
		return nil, nil, err
	}
	go func() {
		if err := server.Serve(); err != nil {
			obs.Logger.Error("embedded broker serve error", "error", err)
		}
	}()
	scheme := "ws"
	if tlsCfg != nil {
		scheme = "wss"
	}
	obs.Logger.Info("embedded MQTT broker started", "transport", scheme, "ws_addr", wsAddr)
	return func() { _ = server.Close() }, server, nil
}
