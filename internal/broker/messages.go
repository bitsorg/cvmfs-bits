// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package broker

import "time"

// AnnounceMessage is published by a publisher to the announce topic for a
// specific repository (see AnnounceTopic).  All receivers subscribed to that
// topic receive it and pull the transaction's manifest objects they lack.
type AnnounceMessage struct {
	// PayloadID is the publisher's job UUID.  Receivers use it as the
	// transaction id: the manifest is at /s1/{payload_id}/manifest.
	PayloadID string `json:"payload_id"`

	// PublisherID is a stable identifier for the publisher node (typically
	// its hostname). Receivers require it to be set.
	PublisherID string `json:"publisher_id"`

	// Repo is the repository name this payload targets (e.g. "atlas.cern.ch").
	Repo string `json:"repo"`

	// TotalBytes is the total compressed size of all objects (informational).
	TotalBytes int64 `json:"total_bytes"`
}

// PublishedMessage is published by a publisher to the published topic for a
// specific repository (see PublishedTopic) immediately after a successful
// catalog commit — whether via the bits pre-publish pipeline or the native
// cvmfs_server ingest path.
//
// It is retained, so a receiver that missed it (or the announce) gets the
// latest one on (re)connect and pulls the new root catalog.
type PublishedMessage struct {
	// Repo is the CVMFS repository name (e.g. "atlas.cern.ch").
	Repo string `json:"repo"`

	// NewRootHash is the plain-hex SHA-1 hash of the root catalog after the
	// successful commit.  Receivers use this as a cache key to avoid redundant
	// pulls when the same commit hash is broadcast multiple times.
	NewRootHash string `json:"new_root_hash"`

	// PublishedAt is the wall-clock time at which the commit completed on the
	// publisher.  Included for audit / latency-measurement purposes.
	PublishedAt time.Time `json:"published_at"`
}

// PresenceMessage is published (retained) by a receiver on connect and also
// sent as the Last-Will-and-Testament with Online=false.  It lets monitoring
// systems see which receivers are online and which repositories they serve.
type PresenceMessage struct {
	// NodeID is the receiver's stable identifier.
	NodeID string `json:"node_id"`

	// Repos is the list of CVMFS repository names served by this receiver.
	Repos []string `json:"repos"`

	// Online is true when the receiver is connected and subscribed to
	// announces.  The LWT publishes this topic with Online=false so
	// the broker automatically marks the node offline on unexpected disconnect.
	Online bool `json:"online"`

	// Ready mirrors Online: the receiver is ready as soon as it is connected;
	// the LWT/offline presence sets this to false.
	Ready bool `json:"ready"`
}
