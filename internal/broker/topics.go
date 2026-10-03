// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

// Package broker provides the MQTT topic schema and message types used for
// coordination between cvmfs-prepub publishers and Stratum 1 receiver agents.
//
// # Control-plane overview
//
//   - The broker is embedded in the publisher.  Stratum 1 receivers connect
//     outbound only; no inbound firewall rules are required.
//   - Receivers publish a retained "presence" message on connect and configure
//     a Last-Will-and-Testament so the broker marks them offline on unexpected
//     disconnect.
//   - Before a commit the publisher broadcasts an AnnounceMessage; receivers
//     fetch the transaction manifest over HTTP and pull the objects they lack.
//   - After a commit the publisher broadcasts a PublishedMessage; receivers
//     pull the listed objects (or the new root catalog) from Stratum 0.
//
// # Topic schema
//
//	cvmfs/repos/{repo}/announce
//	    Publisher → all receivers.  Payload: AnnounceMessage (JSON).
//	    QoS 1, retained=false.
//
//	cvmfs/repos/{repo}/published
//	    Publisher → all receivers.  Payload: PublishedMessage (JSON).
//	    QoS 1.  Sent after every successful catalog commit (bits pipeline and
//	    native ingest path alike).
//
//	cvmfs/receivers/{node_id}/presence
//	    Receiver → all observers.  Payload: PresenceMessage (JSON).
//	    QoS 1, retained=true.  LWT publishes the same topic with Online=false.
//
// # Security
//
// Clients authenticate to the broker with a bearer token; topic ACLs let a
// receiver publish only to presence topics.
package broker

import (
	"fmt"
	"strings"
)

// Topic path segments.
const (
	topicBase      = "cvmfs"
	topicRepos     = "repos"
	topicReceivers = "receivers"
	topicAnnounce  = "announce"
	topicPublished = "published"
	topicPresence  = "presence"
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

// ValidateRepo returns an error if repo is not a valid MQTT topic segment.
// Call this at the API boundary (job submission) so that downstream topic
// constructors — which panic on invalid input — never receive bad data.
func ValidateRepo(repo string) error {
	return validTopicSegment("repo", repo)
}

// ValidateNodeID returns an error if nodeID is not a valid MQTT topic segment.
// Call this when configuring a receiver node so that downstream topic
// constructors never receive a bad node ID at runtime.
func ValidateNodeID(nodeID string) error {
	return validTopicSegment("node_id", nodeID)
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

// PublishedTopic returns the topic on which a publisher broadcasts a commit
// notification after a successful catalog publish (bits pipeline or native
// ingest).  Receivers subscribed to this topic pull any new CAS objects from
// Stratum 0.
//
//	cvmfs/repos/{repo}/published
//
// Panics if repo contains MQTT-reserved characters (/, +, #, NUL) or is empty.
func PublishedTopic(repo string) string {
	if err := validTopicSegment("repo", repo); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%s/%s/%s/%s", topicBase, topicRepos, repo, topicPublished)
}

// PublishedTopicFilter returns an MQTT subscription filter that matches
// published notifications for all repositories.
//
//	cvmfs/repos/+/published
func PublishedTopicFilter() string {
	return fmt.Sprintf("%s/%s/+/%s", topicBase, topicRepos, topicPublished)
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
