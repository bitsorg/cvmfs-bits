// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package receiver

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"cvmfs.io/prepub/internal/broker"
	"cvmfs.io/prepub/internal/distribute/manifest"
)

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
// AnnounceMessage arrives on one of the subscribed announce topics. A complete
// announce for a served repo starts a bounded, deduplicated pull of the transaction's manifest
// objects (see pull.go); a malformed one is logged and dropped.
func (r *Receiver) mqttAnnounceHandler(msg *broker.Message) {
	var ann broker.AnnounceMessage
	if err := msg.Decode(&ann); err != nil {
		r.cfg.Obs.Logger.Warn("mqtt: failed to decode AnnounceMessage",
			"topic", msg.Topic, "error", err)
		return
	}
	if ann.PayloadID == "" || ann.PublisherID == "" || ann.Repo == "" {
		r.cfg.Obs.Logger.Warn("mqtt: AnnounceMessage missing required fields",
			"topic", msg.Topic, "payload_id", ann.PayloadID)
		return
	}
	if !r.servesRepo(ann.Repo) {
		return
	}
	if r.pullCoordinator != nil {
		r.startPull(ann.PayloadID, ann.Repo)
	}
}

// mqttPublishedHandler is called by the broker client each time a
// PublishedMessage arrives on one of the subscribed published topics.
//
// Flow:
//  1. Decode the JSON payload.
//  2. Validate the repo is one this receiver serves.
//  3. If Stratum0URL is not configured, log and return.
//  4. If a pull goroutine is already running for this repo, drop the
//     notification (the in-progress pull will fetch the latest state anyway).
//  5. Launch a background goroutine (governed by bgCtx) that pulls the new
//     root catalog.
//
// The handler is non-blocking: all I/O runs in a separate goroutine so that
// the broker's callback goroutine is never blocked.
func (r *Receiver) mqttPublishedHandler(msg *broker.Message) {
	var pm broker.PublishedMessage
	if err := msg.Decode(&pm); err != nil {
		r.cfg.Obs.Logger.Warn("mqtt: failed to decode PublishedMessage",
			"topic", msg.Topic, "error", err)
		return
	}

	if pm.Repo == "" || pm.NewRootHash == "" {
		r.cfg.Obs.Logger.Warn("mqtt: PublishedMessage missing required fields",
			"topic", msg.Topic, "repo", pm.Repo)
		return
	}

	if !r.servesRepo(pm.Repo) {
		// Not our repo — ignore silently (topic ACLs should prevent this).
		return
	}

	r.cfg.Obs.Logger.Info("mqtt: published notification received",
		"repo", pm.Repo,
		"new_root_hash", pm.NewRootHash,
		"published_at", pm.PublishedAt)

	if r.pullCoordinator == nil {
		r.cfg.Obs.Logger.Info("mqtt: no receiver_stratum0_url configured — skipping pull",
			"repo", pm.Repo)
		return
	}

	// Deduplicate concurrent notifications per repo.  LoadOrStore atomically
	// inserts a new mutex for this repo if one doesn't exist.
	muVal, _ := r.s0PullMu.LoadOrStore(pm.Repo, &sync.Mutex{})
	mu := muVal.(*sync.Mutex)
	if !mu.TryLock() {
		// A pull goroutine is already running for this repo.  It will read the
		// latest objects from S0, so this notification is safe to drop.
		r.cfg.Obs.Logger.Info("mqtt: S0 pull already in progress for repo — dropping notification",
			"repo", pm.Repo, "new_root_hash", pm.NewRootHash)
		return
	}
	// mu is now locked.  The goroutine will unlock it when done.

	go func() {
		defer mu.Unlock()
		r.pullFromS0(r.bgCtx, pm)
	}()
}

// pullFromS0 fetches the new root catalog named by pm from the publisher
// ({Stratum0URL}/cvmfs/{repo}/data/...) into the local CAS, hash-verified, via
// the same Puller the announce path uses. An object already present is skipped,
// so a repeated (retained) notification is cheap. ctx bounds all network I/O.
func (r *Receiver) pullFromS0(ctx context.Context, pm broker.PublishedMessage) {
	logger := r.cfg.Obs.Logger.With("repo", pm.Repo, "new_root_hash", pm.NewRootHash)
	if r.pullCoordinator == nil {
		return
	}
	m := &manifest.Manifest{
		TransactionID:  "published-" + pm.NewRootHash,
		Repo:           pm.Repo,
		TargetRootHash: pm.NewRootHash,
		BaseURLs:       []string{strings.TrimRight(r.cfg.Stratum0URL, "/") + "/cvmfs/" + pm.Repo + "/data"},
		Generator:      manifest.GeneratorPipeline,
		Objects:        []manifest.ObjRef{{Hash: pm.NewRootHash + "C"}},
	}
	// Validate rejects a malformed hash (e.g. "../../x") before it can reach
	// the URL or the local CAS path.
	if err := m.Validate(); err != nil {
		logger.Warn("mqtt: ignoring PublishedMessage with invalid root hash", "error", err)
		return
	}
	res, err := r.pullCoordinator.Puller.Pull(ctx, m)
	if err != nil {
		logger.Warn("mqtt: root catalog pull failed — will retry on next notification", "error", err)
		return
	}
	logger.Info("mqtt: root catalog pull complete", "fetched", res.Fetched, "skipped", res.Skipped)
}

// servesRepo returns true if repo is listed in r.cfg.Repos.
// An empty Repos slice means the receiver has not been configured with a
// repository list; in that case all repos are accepted.
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
		// An empty NodeID would make all receivers share one presence topic and
		// MQTT client id.
		return fmt.Errorf("receiver: NodeID must not be empty when BrokerURL is configured")
	}

	presenceTopic := broker.PresenceTopic(nodeID)
	offlineMsg := broker.PresenceMessage{
		NodeID: nodeID,
		Repos:  r.cfg.Repos,
		Online: false,
		Ready:  false,
	}

	brokerCfg := broker.Config{
		BrokerURL:           r.cfg.BrokerURL,
		CredentialsProvider: r.cfg.BrokerCredentialsProvider,
		CACert:              r.cfg.BrokerCACert,
		ClientID:            nodeID + "-receiver",
	}

	// Connect with LWT = offline presence message.
	// The broker will publish this automatically if our connection drops.
	client, err := broker.NewWithLWT(brokerCfg, presenceTopic, 1, true, offlineMsg)
	if err != nil {
		return fmt.Errorf("receiver: connecting to MQTT broker: %w", err)
	}

	// Publish our online presence (retained) immediately after connecting.
	onlineMsg := broker.PresenceMessage{
		NodeID: nodeID,
		Repos:  r.cfg.Repos,
		Online: true,
		Ready:  true,
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

	// Subscribe to the published topic so that commit notifications (from both
	// the bits pipeline and native ingest) trigger S0 pulls.  The subscription
	// uses the same wildcard filter regardless of the configured repos: the
	// handler filters by repo using servesRepo().  We use QoS 1 so that
	// notifications are not lost on a transient connection drop.
	publishedFilter := broker.PublishedTopicFilter()
	if err := client.Subscribe(publishedFilter, 1, r.mqttPublishedHandler); err != nil {
		// Non-fatal: the announce path still works. Log the error and continue.
		r.cfg.Obs.Logger.Warn("receiver: subscribing to published topic failed — S0 pull disabled",
			"filter", publishedFilter, "error", err)
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
			NodeID: nodeID,
			Repos:  r.cfg.Repos,
			Online: true,
			Ready:  true, // receiver answers presence via direct CAS.Exists
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
		NodeID: nodeID,
		Repos:  r.cfg.Repos,
		Online: false,
		Ready:  false,
	}
	if err := client.Publish(presenceTopic, 1, true, offlineMsg); err != nil {
		r.cfg.Obs.Logger.Warn("mqtt: failed to publish offline presence on shutdown",
			"node_id", nodeID, "error", err)
	}

	client.Disconnect(500) // 500 ms quiesce
}
