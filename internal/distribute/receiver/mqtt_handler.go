package receiver

import (
	"fmt"
	"strings"
	"time"

	"cvmfs.io/prepub/internal/broker"
)

// maxHashesPerAnnounce is the maximum number of CAS hashes accepted in a
// single AnnounceMessage.  An announce that exceeds this limit is rejected with
// an error ReadyMessage so the publisher can make an informed quorum decision
// rather than timing out.
//
// A typical large CVMFS transaction touches tens of thousands of objects.
// 1 000 000 is a generous upper bound that still prevents a rogue broker client
// from forcing GB-scale allocations in computeAbsentHashes (each hash is
// ~64 bytes, so 1M hashes ≈ 64 MB — well within a receiver's budget).
const maxHashesPerAnnounce = 1_000_000

// maxHashLen is the maximum byte length of a single CAS hash string accepted
// inside an AnnounceMessage.  SHA-256 hex is 64 chars; SHA-512 hex is 128.
// 256 is a generous bound that covers any realistic algorithm while preventing
// pathological Bloom-filter hash computations caused by arbitrarily long strings.
const maxHashLen = 256

// mqttPublish publishes v to topic under the mqttMu read-lock so that
// concurrent Shutdown()/stopMQTT() calls cannot nil-race the client pointer.
// Returns false (and logs) when MQTT is not active or the publish fails.
func (r *Receiver) mqttPublish(topic string, v any) bool {
	r.mqttMu.RLock()
	client := r.mqttClient
	r.mqttMu.RUnlock()
	if client == nil {
		return false
	}
	if err := client.Publish(topic, 1, false, v); err != nil {
		r.cfg.Obs.Logger.Warn("mqtt: publish failed", "topic", topic, "error", err)
		return false
	}
	return true
}

// mqttAnnounceHandler is called by the broker client each time an
// AnnounceMessage arrives on one of the subscribed announce topics.
//
// Flow:
//  1. Decode the JSON payload into an AnnounceMessage.
//  2. Validate that the announced repo is one this receiver serves.
//  3. Check available disk space against the announced total bytes.
//  4. Return the existing session if PayloadID was already announced (idempotent).
//  5. Create a new session (session cap applies; reply with error on rejection).
//  6. Compute AbsentHashes by filtering the announced hash list through the
//     receiver's Bloom inventory filter.
//  7. Publish a ReadyMessage to the publisher's reply topic.
//
// All errors are published back to the publisher as ReadyMessage.Error so
// the publisher can make an informed quorum decision rather than just timing
// out waiting for a reply that will never arrive.
func (r *Receiver) mqttAnnounceHandler(msg *broker.Message) {
	var ann broker.AnnounceMessage
	if err := msg.Decode(&ann); err != nil {
		r.cfg.Obs.Logger.Warn("mqtt: failed to decode AnnounceMessage",
			"topic", msg.Topic, "error", err)
		return
	}

	// Validate required fields before constructing any reply topic.
	if ann.PayloadID == "" || ann.PublisherID == "" || ann.Repo == "" {
		r.cfg.Obs.Logger.Warn("mqtt: AnnounceMessage missing required fields",
			"topic", msg.Topic, "payload_id", ann.PayloadID)
		// Publish a best-effort error reply using whatever fields we have.
		pubID := ann.PublisherID
		if pubID == "" {
			pubID = "unknown"
		}
		payID := ann.PayloadID
		if payID == "" {
			payID = "unknown"
		}
		nodeID := r.cfg.NodeID
		if nodeID == "" {
			nodeID = "unknown"
		}
		replyTopic := broker.ReadyTopic(pubID, payID, nodeID)
		r.mqttPublish(replyTopic, broker.ReadyMessage{
			NodeID: nodeID,
			Error:  "malformed announce: missing required fields (payload_id, publisher_id, repo)",
		})
		return
	}

	replyTopic := broker.ReadyTopic(ann.PublisherID, ann.PayloadID, r.cfg.NodeID)
	nodeID := r.cfg.NodeID
	if nodeID == "" {
		nodeID = "unknown"
	}

	// Helper: publish a ReadyMessage with only an error field set.
	replyErr := func(errMsg string) {
		r.cfg.Obs.Logger.Warn("mqtt: rejecting announce",
			"payload_id", ann.PayloadID, "reason", errMsg)
		r.mqttPublish(replyTopic, broker.ReadyMessage{
			NodeID: nodeID,
			Error:  errMsg,
		})
	}

	// Validate that the announced repository is one we serve.
	if !r.servesRepo(ann.Repo) {
		// Not our repo — ignore silently.  The topic ACL should prevent this,
		// but a misconfigured ACL or wildcard subscription should not cause noise.
		return
	}

	// Disk space pre-check (same logic as announceHandler).
	if ann.TotalBytes > 0 {
		headroom := r.cfg.DiskHeadroom
		if headroom <= 0 {
			headroom = 1.2
		}
		required := int64(float64(ann.TotalBytes) * headroom)
		if required < ann.TotalBytes {
			required = ann.TotalBytes // overflow guard
		}
		if err := checkDiskSpace(r.cfg.CASRoot, required); err != nil {
			replyErr(fmt.Sprintf("insufficient disk space: %v", err))
			return
		}
	}

	// Guard against announce flooding / memory-exhaustion attacks.
	// A rogue broker client could send an AnnounceMessage with millions of
	// hashes; computeAbsentHashes would allocate a copy of that entire slice.
	if len(ann.Hashes) > maxHashesPerAnnounce {
		replyErr(fmt.Sprintf("announce too large: %d hashes exceed limit of %d",
			len(ann.Hashes), maxHashesPerAnnounce))
		return
	}
	// Reject suspiciously long individual hash strings before they reach the
	// Bloom filter (which hashes the string bytes internally).
	for i, h := range ann.Hashes {
		if len(h) > maxHashLen {
			replyErr(fmt.Sprintf("hash[%d] length %d exceeds maximum of %d bytes",
				i, len(h), maxHashLen))
			return
		}
	}

	// Idempotent re-announce: reuse the existing session if still valid.
	if existing, ok := r.store.getByPayload(ann.PayloadID); ok {
		r.cfg.Obs.Logger.Info("mqtt: re-announce — returning existing session",
			"payload_id", ann.PayloadID)
		absentHashes := r.computeAbsentHashes(ann.Hashes)
		r.mqttPublish(replyTopic, broker.ReadyMessage{
			NodeID:       nodeID,
			SessionToken: existing.token,
			DataURL:      r.cfg.dataEndpoint(),
			AbsentHashes: absentHashes,
		})
		return
	}

	// Create a new session.
	ttl := r.cfg.SessionTTL
	if ttl <= 0 {
		ttl = time.Hour
	}
	s, ok := r.store.create(ann.PayloadID, ttl)
	if !ok {
		replyErr("session store at capacity — try again later")
		return
	}

	// Compute the set of hashes the publisher must actually push to us.
	// If the Bloom inventory filter is not yet ready (CAS walk still in
	// progress), we report all hashes as absent — the conservative choice
	// that avoids silently skipping objects we haven't indexed yet.
	absentHashes := r.computeAbsentHashes(ann.Hashes)

	r.cfg.Obs.Logger.Info("mqtt: announce accepted",
		"payload_id", ann.PayloadID,
		"publisher_id", ann.PublisherID,
		"repo", ann.Repo,
		"total_hashes", len(ann.Hashes),
		"absent_hashes", len(absentHashes),
		"session_expires", s.expiresAt.Format(time.RFC3339))

	r.mqttPublish(replyTopic, broker.ReadyMessage{
		NodeID:       nodeID,
		SessionToken: s.token,
		DataURL:      r.cfg.dataEndpoint(),
		AbsentHashes: absentHashes,
	})
}

// computeAbsentHashes returns the subset of hashes that the receiver does not
// currently hold according to its Bloom inventory filter.
//
// When the inventory is not yet ready (CAS walk still in progress) all hashes
// are returned as absent — the conservative choice that ensures we never skip
// an object the publisher needs to send us.
func (r *Receiver) computeAbsentHashes(hashes []string) []string {
	if !r.inv.isReady() {
		// Bloom not ready — report everything as absent to be safe.
		absent := make([]string, len(hashes))
		copy(absent, hashes)
		return absent
	}
	absent := make([]string, 0, len(hashes))
	for _, h := range hashes {
		if !r.inv.contains(h) {
			absent = append(absent, h)
		}
	}
	return absent
}

// servesRepo returns true if repo is listed in r.cfg.Repos.
// An empty Repos slice means the receiver has not been configured with a
// repository list; in that case all repos are accepted (permissive default
// matching the HTTP announce behaviour, which has no repo filter).
// Comparison is case-insensitive because CVMFS repository names are DNS
// hostnames (RFC 4343: DNS is case-insensitive).
func (r *Receiver) servesRepo(repo string) bool {
	if len(r.cfg.Repos) == 0 {
		return true
	}
	for _, served := range r.cfg.Repos {
		if strings.EqualFold(served, repo) {
			return true
		}
	}
	return false
}

// startMQTT connects to the broker, publishes the retained presence message,
// subscribes to announce topics for the configured repositories, and registers
// a reconnect handler that re-publishes the online presence whenever the
// connection is restored (Paho's OnConnectHandler fires on each reconnect).
//
// startMQTT is called from Start() when cfg.BrokerURL is non-empty.
// It is a no-op when cfg.BrokerURL is empty (MQTT disabled).
func (r *Receiver) startMQTT() error {
	if r.cfg.BrokerURL == "" {
		return nil
	}
	nodeID := r.cfg.NodeID
	if nodeID == "" {
		// An empty NodeID would cause all receivers to publish to the same
		// presence and ready topics, making them indistinguishable to publishers.
		return fmt.Errorf("receiver: NodeID must not be empty when BrokerURL is configured")
	}

	presenceTopic := broker.PresenceTopic(nodeID)
	offlineMsg := broker.PresenceMessage{
		NodeID:     nodeID,
		Repos:      r.cfg.Repos,
		DataURL:    r.cfg.dataEndpoint(),
		ControlURL: r.cfg.controlEndpoint(),
		Online:     false,
		BloomReady: false,
	}

	brokerCfg := broker.Config{
		BrokerURL:  r.cfg.BrokerURL,
		ClientCert: r.cfg.BrokerClientCert,
		ClientKey:  r.cfg.BrokerClientKey,
		CACert:     r.cfg.BrokerCACert,
		ClientID:   nodeID + "-receiver",
	}

	// Connect with LWT = offline presence message.
	// The broker will publish this automatically if our connection drops.
	client, err := broker.NewWithLWT(brokerCfg, presenceTopic, 1, true, offlineMsg)
	if err != nil {
		return fmt.Errorf("receiver: connecting to MQTT broker: %w", err)
	}

	// Publish our online presence (retained) immediately after connecting.
	onlineMsg := broker.PresenceMessage{
		NodeID:     nodeID,
		Repos:      r.cfg.Repos,
		DataURL:    r.cfg.dataEndpoint(),
		ControlURL: r.cfg.controlEndpoint(),
		Online:     true,
		BloomReady: r.inv.isReady(),
	}
	if err := client.Publish(presenceTopic, 1, true, onlineMsg); err != nil {
		// Non-fatal: we're connected, presence just didn't publish.
		r.cfg.Obs.Logger.Warn("mqtt: failed to publish online presence",
			"node_id", nodeID, "error", err)
	}

	// Subscribe to announce topics.  When Repos is empty we use a wildcard
	// filter and rely on mqttAnnounceHandler to ignore unserved repos.
	var topicFilter string
	if len(r.cfg.Repos) == 1 {
		topicFilter = broker.AnnounceTopic(r.cfg.Repos[0])
	} else {
		// Zero or multiple repos: use the wildcard filter.
		topicFilter = broker.AnnounceTopicFilter()
	}

	if err := client.Subscribe(topicFilter, 1, r.mqttAnnounceHandler); err != nil {
		client.Disconnect(500)
		return fmt.Errorf("receiver: subscribing to announce topic %q: %w", topicFilter, err)
	}

	// Store the client only after setup is complete so that mqttPublish can use it.
	r.mqttMu.Lock()
	r.mqttClient = client
	r.mqttMu.Unlock()

	// Register a reconnect handler so that when Paho automatically reconnects
	// after an unexpected disconnect, the online presence is re-published.
	// Without this the broker retains the LWT {Online:false} indefinitely and
	// publishers incorrectly see this receiver as offline.
	client.SetReconnectHandler(func() {
		r.cfg.Obs.Logger.Info("mqtt: reconnected — republishing online presence",
			"node_id", nodeID)
		r.mqttPublish(presenceTopic, broker.PresenceMessage{
			NodeID:     nodeID,
			Repos:      r.cfg.Repos,
			DataURL:    r.cfg.dataEndpoint(),
			ControlURL: r.cfg.controlEndpoint(),
			Online:     true,
			BloomReady: r.inv.isReady(), // reflect current state at reconnect time
		})
	})

	r.cfg.Obs.Logger.Info("receiver MQTT control plane active",
		"broker", r.cfg.BrokerURL,
		"node_id", nodeID,
		"presence_topic", presenceTopic,
		"announce_filter", topicFilter)
	return nil
}

// stopMQTT publishes an offline presence message and disconnects from the
// broker.  Called from Shutdown().  Safe to call concurrently or when
// mqttClient is nil.
func (r *Receiver) stopMQTT() {
	// Swap client to nil under the write-lock so that concurrent publish calls
	// from Paho's callback goroutine (mqttPublish) see nil immediately and stop
	// trying to use the client.
	r.mqttMu.Lock()
	client := r.mqttClient
	r.mqttClient = nil
	r.mqttMu.Unlock()

	if client == nil {
		return
	}

	// NodeID is guaranteed non-empty here: startMQTT() returns an error when
	// NodeID is empty and never sets mqttClient, so we cannot reach this point
	// with a nil-client check passing and an empty NodeID.
	nodeID := r.cfg.NodeID

	// Publish explicit offline presence before disconnecting so subscribers
	// see the state change immediately rather than waiting for the LWT broker
	// delay (which may be up to the keep-alive interval).
	presenceTopic := broker.PresenceTopic(nodeID)
	offlineMsg := broker.PresenceMessage{
		NodeID:     nodeID,
		Repos:      r.cfg.Repos,
		DataURL:    r.cfg.dataEndpoint(),
		ControlURL: r.cfg.controlEndpoint(),
		Online:     false,
		BloomReady: false,
	}
	if err := client.Publish(presenceTopic, 1, true, offlineMsg); err != nil {
		r.cfg.Obs.Logger.Warn("mqtt: failed to publish offline presence on shutdown",
			"node_id", nodeID, "error", err)
	}

	client.Disconnect(500) // 500 ms quiesce
}
