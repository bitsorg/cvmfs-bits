// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package distribute

import "cvmfs.io/prepub/internal/broker"

// The control plane carries coordination — discovery, "your turn" notify, lease
// grant, and completion ack — independently of the data plane (ADR-0001 D7). Its
// semantics are defined by the interfaces below so the transport can change
// (MQTT today; SSE under P-B) without touching the manifest/lease/ack contract.
//
// Lease-grant and ack-collection methods are added with admission control in P3;
// P0 defines only the subset that maps 1:1 onto today's MQTT usage so the MQTT
// adapter is a faithful, complete wrapper.

// Budget is an admission grant for a receiver's pull of a transaction (ADR D6).
// Fields are populated when admission control lands in P3.
type Budget struct {
	MaxBytesPerSec int64 // 0 = unlimited
	Slots          int   // concurrent object fetches permitted
}

// ControlPublisher is the Stratum-0 (publisher) side of the control plane.
type ControlPublisher interface {
	// Announce broadcasts a "prepare" to all receivers of repo.
	Announce(repo string, m broker.AnnounceMessage) error
	// PublishCommitted notifies receivers that the catalog has flipped.
	PublishCommitted(repo string, m broker.PublishedMessage) error
	// OnReady registers a handler for ReadyMessage replies for one payload.
	OnReady(publisherID, payloadID string, handle func(broker.ReadyMessage)) error
	// Close releases the control-plane connection.
	Close()
}

// ControlReceiver is the Stratum-1 (receiver) side of the control plane.
type ControlReceiver interface {
	// SubscribeAnnounce delivers AnnounceMessages for the repos this receiver serves.
	SubscribeAnnounce(handle func(broker.AnnounceMessage)) error
	// SubscribePublished delivers post-commit notifications.
	SubscribePublished(handle func(broker.PublishedMessage)) error
	// SendReady replies to the publisher for one payload.
	SendReady(publisherID, payloadID, nodeID string, m broker.ReadyMessage) error
	// SetPresence publishes (retained) the receiver's online presence.
	SetPresence(p broker.PresenceMessage) error
	// Close releases the control-plane connection.
	Close()
}
