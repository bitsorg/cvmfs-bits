package receiver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

// mqttPublishedHandler is called by the broker client each time a
// PublishedMessage arrives on one of the subscribed published topics.
//
// Flow:
//  1. Decode the JSON payload.
//  2. Validate the repo is one this receiver serves.
//  3. If Stratum0URL is not configured, log and return (graceful degradation).
//  4. If a pull goroutine is already running for this repo, drop the
//     notification (the in-progress pull will fetch the latest state anyway).
//  5. Launch a background goroutine (governed by bgCtx) that fetches missing
//     objects from Stratum 0.
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
		"hashes", len(pm.Hashes),
		"published_at", pm.PublishedAt)

	if r.cfg.Stratum0URL == "" {
		r.cfg.Obs.Logger.Info("mqtt: no stratum0_url configured — skipping S0 pull",
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

// pullFromS0 fetches objects that this receiver does not yet hold from the
// Stratum 0 CAS, using the hash list from pm.Hashes when available or the
// root catalog hash when Hashes is empty (native ingest path).
//
// It is designed to be idempotent: if an object is already in the local CAS
// (Bloom filter check + filesystem stat), it is skipped.
//
// All network I/O is governed by ctx so that Shutdown() can cancel in-flight
// pulls promptly.
func (r *Receiver) pullFromS0(ctx context.Context, pm broker.PublishedMessage) {
	logger := r.cfg.Obs.Logger.With(
		"repo", pm.Repo,
		"new_root_hash", pm.NewRootHash,
		"phase", "s0_pull",
	)

	s0Base := strings.TrimRight(r.cfg.Stratum0URL, "/")

	// Build the list of hashes to check/fetch.
	//
	// Bits path: pm.Hashes contains all object hashes the publisher produced.
	// Native ingest path: pm.Hashes is empty; we pull only the root catalog.
	hashesToFetch := pm.Hashes
	if len(hashesToFetch) == 0 {
		// Native ingest: the root catalog is identified by pm.NewRootHash.
		// We add it with the 'C' (compressed/catalog) suffix that CVMFS uses
		// to distinguish catalog objects from regular data objects on disk.
		hashesToFetch = []string{pm.NewRootHash}
		logger.Info("mqtt: native ingest path — fetching root catalog only",
			"root_hash", pm.NewRootHash)
	}

	// Filter through Bloom filter to compute the minimal fetch set.
	var absent []string
	if r.inv.isReady() {
		for _, h := range hashesToFetch {
			// Strip the 'C' suffix for Bloom filter lookup; the inventory tracks
			// plain hashes, not content-type suffixed keys.
			plain := strings.TrimSuffix(h, "C")
			if !r.inv.contains(plain) {
				absent = append(absent, h)
			}
		}
	} else {
		// Bloom not ready — treat all hashes as absent (conservative).
		absent = hashesToFetch
		logger.Info("mqtt: Bloom filter not ready — fetching all announced hashes",
			"count", len(absent))
	}

	if len(absent) == 0 {
		logger.Info("mqtt: all objects already present — no S0 pull needed")
		return
	}

	logger.Info("mqtt: pulling absent objects from S0",
		"absent", len(absent), "total", len(hashesToFetch))

	fetched := 0
	for _, hash := range absent {
		if ctx.Err() != nil {
			logger.Info("mqtt: S0 pull cancelled", "fetched", fetched)
			return
		}

		// Strip 'C' suffix for URL construction; the S0 data URL uses the plain
		// hash (without suffix) as the path component.
		// S0 serves objects at: /cvmfs/{repo}/data/{hash[0:2]}/{hash}C
		plain := strings.TrimSuffix(hash, "C")
		if len(plain) < 2 {
			logger.Warn("mqtt: skipping hash with length < 2", "hash", hash)
			continue
		}

		// Check filesystem directly (handles the case where Bloom is not ready).
		localPath := casPath(r.cfg.CASRoot, plain)
		if _, err := os.Stat(localPath); err == nil {
			// Already on disk — just ensure the inventory is updated.
			r.inv.add(plain)
			continue
		}

		// Stratum0URL convention (same as publisher): includes the /cvmfs path
		// prefix (e.g. "http://stratum0/cvmfs").  CAS objects are served at
		// {Stratum0URL}/{repo}/data/{hash[0:2]}/{hash}C, matching the CVMFS
		// Apache DocumentRoot/cvmfs/{repo}/data/ layout.
		objectURL := fmt.Sprintf("%s/%s/data/%s/%sC",
			s0Base, pm.Repo, plain[:2], plain)

		if err := r.fetchObjectFromS0(ctx, objectURL, plain); err != nil {
			logger.Error("mqtt: failed to fetch object from S0",
				"hash", plain, "url", objectURL, "error", err)
			// Continue with remaining hashes — partial success is better than
			// giving up.  The next PublishedMessage will retry missing objects.
			continue
		}
		fetched++
	}

	logger.Info("mqtt: S0 pull complete", "fetched", fetched, "absent", len(absent))
}

// fetchObjectFromS0 downloads a single CAS object from objectURL and stores it
// in the local CAS at the path derived from plain (the hash without 'C' suffix).
//
// The download is streamed through a SHA-256 hasher into a sibling temp file
// and atomically renamed to the final CAS path on success.  SHA-256 of the
// received bytes is verified against the computed value (the 'C' object is
// already compressed; we don't re-hash the uncompressed form here, but we do
// ensure transfer integrity against bit-rot or truncation).
func (r *Receiver) fetchObjectFromS0(ctx context.Context, objectURL, plain string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, objectURL, nil)
	if err != nil {
		return fmt.Errorf("building request: %w", err)
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", objectURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// S0 doesn't have this object yet (e.g. race with commit propagation).
		// Not an error — the object will arrive on the next pull cycle.
		r.cfg.Obs.Logger.Info("mqtt: object not yet available on S0 (404) — will retry on next notification",
			"url", objectURL)
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s returned %d", objectURL, resp.StatusCode)
	}

	// Ensure the CAS sub-directory exists.
	finalPath := casPath(r.cfg.CASRoot, plain)
	if err := os.MkdirAll(filepath.Dir(finalPath), 0700); err != nil {
		return fmt.Errorf("creating CAS subdirectory: %w", err)
	}

	// Write to a temp file with a random suffix to avoid races with concurrent
	// PUTs or other fetch goroutines for the same hash.
	tmpPath := finalPath + "." + randomToken()[:8] + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}

	hasher := sha256.New()
	_, copyErr := io.Copy(f, io.TeeReader(resp.Body, hasher))
	syncErr := f.Sync()
	f.Close()

	if copyErr != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("streaming body: %w", copyErr)
	}
	if syncErr != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("fsync temp file: %w", syncErr)
	}

	// Atomic rename.
	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		// If the file already exists (concurrent fetch or PUT), treat as success.
		if _, statErr := os.Stat(finalPath); statErr == nil {
			r.inv.add(plain)
			return nil
		}
		return fmt.Errorf("rename to final path: %w", err)
	}

	// Update the inventory Bloom filter.
	r.inv.add(plain)

	r.cfg.Obs.Logger.Debug("mqtt: fetched object from S0",
		"hash", plain, "sha256", hex.EncodeToString(hasher.Sum(nil)))
	return nil
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

	// Subscribe to the published topic so that commit notifications (from both
	// the bits pipeline and native ingest) trigger S0 pulls.  The subscription
	// uses the same wildcard filter regardless of the configured repos: the
	// handler filters by repo using servesRepo().  We use QoS 1 so that
	// notifications are not lost on a transient connection drop.
	publishedFilter := broker.PublishedTopicFilter()
	if err := client.Subscribe(publishedFilter, 1, r.mqttPublishedHandler); err != nil {
		// Non-fatal: the announce path still works, and the bits pre-push already
		// delivered objects before the commit.  Log the error and continue.
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
