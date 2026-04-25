// Package broker provides the MQTT topic schema and message types used for
// coordination between cvmfs-prepub publishers and Stratum 1 receiver agents.
//
// # Control-plane overview
//
// When a broker URL is configured the entire coordination flow moves from HTTP
// polling to MQTT pub/sub:
//
//   - The broker runs on Stratum 0 infrastructure (e.g. CERN).  Stratum 1 sites
//     only need outbound TCP 8883; no inbound firewall rules are required.
//   - Receivers publish a retained "presence" message on connect and configure a
//     Last-Will-and-Testament so the broker marks them offline on unexpected
//     disconnect — replacing the HTTP heartbeat loop in coord_client.go.
//   - Publishers broadcast an AnnounceMessage to all receivers subscribed to a
//     repository topic.  Each receiver filters the hash list against its own
//     Bloom filter and replies with a ReadyMessage carrying its session token and
//     the subset of hashes it actually needs.
//   - Publishers collect ReadyMessages until quorum is reached (or a timeout
//     fires), then push objects to each receiver's plain-HTTP data channel using
//     the per-session bearer token — identical to the HTTP announce path.
//
// When BrokerURL is empty the system falls back to the legacy HTTP announce
// protocol with no change in behaviour.
//
// # Topic schema
//
//	cvmfs/repos/{repo}/announce
//	    Publisher → all receivers.  Payload: AnnounceMessage (JSON).
//	    QoS 1, retained=false.
//
//	cvmfs/receivers/{node_id}/presence
//	    Receiver → all observers.  Payload: PresenceMessage (JSON).
//	    QoS 1, retained=true.  LWT publishes the same topic with Online=false.
//
//	cvmfs/publishers/{publisher_id}/ready/{payload_id}/{node_id}
//	    Receiver → specific publisher.  Payload: ReadyMessage (JSON).
//	    QoS 1, retained=false.
//
// # Security
//
// All connections use mTLS (broker-issued per-node client certificates).  The
// broker enforces topic ACLs so that a receiver can only publish to its own
// presence and ready topics, and can only subscribe to announce topics for
// repositories it serves.
package broker

import (
	"fmt"
	"strings"
)

// Topic path segments.
const (
	topicBase       = "cvmfs"
	topicRepos      = "repos"
	topicReceivers  = "receivers"
	topicPublishers = "publishers"
	topicAnnounce   = "announce"
	topicPresence   = "presence"
	topicReady      = "ready"
)

// validTopicSegment returns an error if s contains characters that have
// special meaning in MQTT topic strings: forward-slash (level separator),
// plus (single-level wildcard), hash (multi-level wildcard), or NUL (forbidden
// by the MQTT specification).  These must not appear in user-supplied fields
// such as repo names, node IDs, or payload IDs.
func validTopicSegment(name, value string) error {
	if strings.ContainsAny(value, "/+#\x00") {
		return fmt.Errorf("broker: %s %q contains a character that is illegal in an MQTT topic segment (/ + # or NUL)", name, value)
	}
	if value == "" {
		return fmt.Errorf("broker: %s must not be empty", name)
	}
	return nil
}

// AnnounceTopic returns the topic on which publishers broadcast pre-warming
// requests for a specific repository.
//
//	cvmfs/repos/{repo}/announce
//
// Panics if repo contains MQTT-reserved characters (/, +, #, NUL) or is empty.
func AnnounceTopic(repo string) string {
	if err := validTopicSegment("repo", repo); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%s/%s/%s/%s", topicBase, topicRepos, repo, topicAnnounce)
}

// AnnounceTopicFilter returns an MQTT subscription filter that matches
// announce requests for all repositories.
//
//	cvmfs/repos/+/announce
func AnnounceTopicFilter() string {
	return fmt.Sprintf("%s/%s/+/%s", topicBase, topicRepos, topicAnnounce)
}

// PresenceTopic returns the retained topic on which a receiver publishes its
// online/offline status.  The LWT is published to this same topic with
// Online=false.
//
//	cvmfs/receivers/{node_id}/presence
//
// Panics if nodeID contains MQTT-reserved characters or is empty.
func PresenceTopic(nodeID string) string {
	if err := validTopicSegment("node_id", nodeID); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%s/%s/%s/%s", topicBase, topicReceivers, nodeID, topicPresence)
}

// PresenceTopicFilter returns an MQTT subscription filter that matches presence
// messages for all receivers.
//
//	cvmfs/receivers/+/presence
func PresenceTopicFilter() string {
	return fmt.Sprintf("%s/%s/+/%s", topicBase, topicReceivers, topicPresence)
}

// ReadyTopic returns the topic on which a receiver publishes its ReadyMessage
// in response to an AnnounceMessage from a specific publisher/payload pair.
//
//	cvmfs/publishers/{publisher_id}/ready/{payload_id}/{node_id}
//
// Panics if any argument contains MQTT-reserved characters or is empty.
func ReadyTopic(publisherID, payloadID, nodeID string) string {
	if err := validTopicSegment("publisher_id", publisherID); err != nil {
		panic(err)
	}
	if err := validTopicSegment("payload_id", payloadID); err != nil {
		panic(err)
	}
	if err := validTopicSegment("node_id", nodeID); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s",
		topicBase, topicPublishers, publisherID, topicReady, payloadID, nodeID)
}

// ReadyTopicFilter returns an MQTT subscription filter that matches all
// ReadyMessages for a specific publisher/payload pair, regardless of which
// receiver node sent them.
//
//	cvmfs/publishers/{publisher_id}/ready/{payload_id}/+
//
// Panics if either argument contains MQTT-reserved characters or is empty.
func ReadyTopicFilter(publisherID, payloadID string) string {
	if err := validTopicSegment("publisher_id", publisherID); err != nil {
		panic(err)
	}
	if err := validTopicSegment("payload_id", payloadID); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%s/%s/%s/%s/%s/+",
		topicBase, topicPublishers, publisherID, topicReady, payloadID)
}
