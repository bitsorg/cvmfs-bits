package broker

import "time"

// AnnounceMessage is published by a publisher to the announce topic for a
// specific repository (see AnnounceTopic).  All receivers subscribed to that
// topic will receive it and decide whether they can participate.
//
// The Hashes field carries the full set of CAS hashes in the payload.  Each
// receiver intersects this list against its own Bloom filter to compute the
// subset it does not yet hold, avoiding unnecessary network transfers.
type AnnounceMessage struct {
	// PayloadID is the publisher's job UUID.  Receivers echo it back in their
	// ReadyMessage and use it as the session's PayloadID (idempotency key).
	PayloadID string `json:"payload_id"`

	// PublisherID is a stable identifier for the publisher node, used to route
	// ReadyMessage replies (see ReadyTopic).  Typically the publisher's hostname
	// or a UUID assigned at startup.
	PublisherID string `json:"publisher_id"`

	// Repo is the repository name this payload targets (e.g. "atlas.cern.ch").
	// Receivers use this to validate that the announce is for a repo they serve.
	Repo string `json:"repo"`

	// Hashes is the complete list of CAS object hashes in this payload.
	// Receivers subtract their Bloom filter to compute AbsentHashes.
	Hashes []string `json:"hashes"`

	// TotalBytes is the total compressed size of all objects.
	// Used by receivers for disk-space pre-checks.
	TotalBytes int64 `json:"total_bytes"`
}

// ReadyMessage is published by a receiver to the publisher's ready topic
// (see ReadyTopic) after it has processed an AnnounceMessage.
//
// The receiver computes AbsentHashes by testing each hash from the announce
// against its own Bloom inventory filter, so the publisher only needs to push
// the objects the receiver actually lacks — without a separate bloom-fetch
// round-trip.
type ReadyMessage struct {
	// NodeID is the receiver's stable identifier (same as Config.NodeID).
	NodeID string `json:"node_id"`

	// SessionToken is the bearer credential for subsequent PUT requests on
	// the data channel.  The publisher presents this in Authorization: Bearer
	// headers when pushing objects to DataURL.
	SessionToken string `json:"session_token"`

	// DataURL is the base URL of the receiver's plain-HTTP data channel, e.g.
	// "http://stratum1.cern.ch:9101".  All object PUTs go to
	//   PUT DataURL/api/v1/objects/{hash}
	DataURL string `json:"data_url"`

	// AbsentHashes is the subset of the announce's Hashes that the receiver
	// does not yet hold.  The publisher only pushes these hashes to this
	// receiver.  An empty slice means the receiver already holds everything
	// (a no-op push for this node).
	AbsentHashes []string `json:"absent_hashes"`

	// Error is non-empty when the receiver is unable to participate (e.g.
	// insufficient disk space, unknown repo, session cap reached).  Publishers
	// must not count receivers with a non-empty Error field towards quorum.
	Error string `json:"error,omitempty"`
}

// PublishedMessage is published by a publisher to the published topic for a
// specific repository (see PublishedTopic) immediately after a successful
// catalog commit — whether via the bits pre-publish pipeline or the native
// cvmfs_server ingest path.
//
// Receivers subscribed to this topic use it as a trigger to pull any new CAS
// objects from the Stratum 0 that they do not yet hold, so that they are
// synchronised with the canonical repository state after every commit.
//
// When Hashes is non-empty (bits path) the receiver can use it to compute the
// delta against its local Bloom filter and fetch only the missing objects.
// When Hashes is empty (native ingest path) the receiver falls back to pulling
// the new root catalog from Stratum 0 and walking the catalog to discover
// referenced objects — or simply acknowledges the notification and performs a
// full snapshot on its next scheduled window.
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

	// Hashes is the full list of CAS object hashes that were part of this
	// publish.  Populated by the bits pipeline; empty for native ingest.
	// Receivers that have a populated Bloom filter can subtract their local
	// inventory from this list to compute the minimal fetch set.
	Hashes []string `json:"hashes,omitempty"`
}

// PresenceMessage is published (retained) by a receiver on connect and also
// sent as the Last-Will-and-Testament with Online=false.  It allows publishers
// and monitoring systems to discover which receivers are available and which
// repositories they serve, without querying a central coordination service.
type PresenceMessage struct {
	// NodeID is the receiver's stable identifier.
	NodeID string `json:"node_id"`

	// Repos is the list of CVMFS repository names served by this receiver.
	Repos []string `json:"repos"`

	// DataURL is the base URL of the receiver's plain-HTTP data channel.
	// Included here so monitoring tools can cross-reference presence with
	// actual data-channel reachability.
	DataURL string `json:"data_url"`

	// ControlURL is the HTTPS control channel URL of this receiver.
	// Retained for backward compatibility with tools that use the HTTP
	// announce protocol.
	ControlURL string `json:"control_url"`

	// Online is true when the receiver is connected and ready to accept
	// announce requests.  The LWT publishes this topic with Online=false so
	// the broker automatically marks the node offline on unexpected disconnect.
	Online bool `json:"online"`

	// BloomReady is true once the receiver's Bloom inventory filter has been
	// populated from the on-disk CAS.  A receiver with BloomReady=false can
	// still participate but will report all hashes as absent (conservative
	// behaviour equivalent to BloomQueryTimeout=0 on the distributor).
	BloomReady bool `json:"bloom_ready"`
}
