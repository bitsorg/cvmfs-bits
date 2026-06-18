// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Package distribute holds the publisher-side distribution configuration for
// ADR-0001 pull distribution. The legacy HTTP push data plane and the
// per-endpoint worker pool have been removed: the pre-commit announce is now
// published directly on the embedded control-plane broker by the API
// orchestrator (see internal/api.Orchestrator.publishAnnounce), and Stratum 1
// receivers pull the announced objects themselves.
package distribute

import (
	"cvmfs.io/prepub/internal/broker"
	"cvmfs.io/prepub/pkg/observe"
)

// Config carries the control-plane broker configuration the publisher uses to
// emit the pre-commit announce. It is attached to the API Orchestrator as
// Distribute; a nil Config (or empty BrokerConfig.BrokerURL) disables the
// announce, in which case receivers converge on the post-commit published
// broadcast and the .cvmfspublished backstop poll.
type Config struct {
	// Obs provides logging and metrics.
	Obs *observe.Provider

	// BrokerConfig, when non-nil with a non-empty BrokerURL, names the
	// control-plane broker the publisher announces transactions on. In the
	// embedded-broker deployment this is the publisher's own loopback broker
	// URL, with a token CredentialsProvider attached for authentication.
	BrokerConfig *broker.Config
}
