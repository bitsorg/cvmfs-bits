package distribute

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"cvmfs.io/prepub/internal/broker"
	"cvmfs.io/prepub/internal/cas"
)

// defaultMQTTQuorumTimeout is how long distributeMQTT waits for quorum before
// giving up on receivers that have not replied.  Publishers that need a tighter
// deadline should set cfg.MQTTQuorumTimeout explicitly.
const defaultMQTTQuorumTimeout = 30 * time.Second

// distributeMQTT is the MQTT-based variant of Distribute.
//
// It is called when cfg.BrokerConfig is non-nil and BrokerConfig.BrokerURL is
// non-empty.  All other behaviour (quorum enforcement, per-object retry,
// distribution logging) is identical to the HTTP announce path.
//
// Flow:
//  1. Connect to the broker and subscribe to the per-payload ready topic.
//  2. Publish an AnnounceMessage to the repository announce topic.
//  3. Collect ReadyMessages until quorum is reached or the timeout fires.
//  4. For each receiver that replied without error, push its AbsentHashes via
//     the existing plain-HTTP data channel using the session token from the
//     ReadyMessage.
//  5. Check quorum and return.
//
// # Delta push
//
// Unlike the HTTP path (where the distributor fetches the receiver's Bloom
// filter via GET /api/v1/bloom), the MQTT path delegates delta computation to
// the receiver: each ReadyMessage carries AbsentHashes — the subset of hashes
// the receiver does not yet hold.  This removes a network round-trip from the
// critical path and keeps the Bloom filter private to each node.
//
// # Backward compatibility
//
// When cfg.BrokerConfig is nil or BrokerURL is empty, Distribute falls back to
// the existing HTTP announce protocol with no change.
func distributeMQTT(
	ctx context.Context,
	payloadID string,
	hashes []string,
	casBackend cas.Backend,
	log *DistLog,
	cfg Config,
) (confirmed, total int, err error) {
	bcfg := cfg.BrokerConfig
	if bcfg == nil || bcfg.BrokerURL == "" {
		return 0, 0, fmt.Errorf("distributeMQTT: BrokerConfig is nil or BrokerURL is empty")
	}
	if cfg.Repo == "" {
		return 0, 0, fmt.Errorf("distributeMQTT: Repo must be set in Config")
	}

	ctx, span := cfg.Obs.Tracer.Start(ctx, "distribute.mqtt")
	defer span.End()

	// publisherID is derived from the full payload ID so that the ready-topic
	// namespace is uniquely scoped to this exact job and cannot collide with a
	// concurrent job whose payload ID happens to share a common prefix.
	publisherID := "pub-" + payloadID

	// expectedNodes is the quorum denominator.  When cfg.Endpoints is populated
	// with receiver node IDs it gives a determinate count; otherwise we derive
	// the denominator from the actual replies received.
	expectedNodes := len(cfg.Endpoints)

	timeout := cfg.MQTTQuorumTimeout
	if timeout <= 0 {
		timeout = defaultMQTTQuorumTimeout
	}

	// --- Step 1: Connect to broker ---
	// Use a per-job ClientID so that two concurrent distributeMQTT calls from
	// the same publisher host do not share a session.  When two clients connect
	// with the same ClientID the broker forcibly disconnects the first, silently
	// killing its subscription.  Appending the first 8 chars of the payload UUID
	// gives sufficient uniqueness without exceeding MQTT 3.1's 23-char limit.
	jobCfg := *bcfg
	if jobCfg.ClientID == "" {
		if h, err := os.Hostname(); err == nil {
			jobCfg.ClientID = h
		} else {
			jobCfg.ClientID = "cvmfs-pub"
		}
	}
	pidSuffix := payloadID
	if len(pidSuffix) > 8 {
		pidSuffix = pidSuffix[:8]
	}
	jobCfg.ClientID += "-" + pidSuffix

	client, err := broker.New(jobCfg)
	if err != nil {
		return 0, 0, fmt.Errorf("distributeMQTT: connecting to broker: %w", err)
	}
	defer client.Disconnect(500)

	// --- Step 2: Subscribe to the per-payload ready topic ---
	readyFilter := broker.ReadyTopicFilter(publisherID, payloadID)
	type readyResult struct {
		nodeID string
		msg    broker.ReadyMessage
	}
	readyCh := make(chan readyResult, 64)

	// maxAbsentHashes is the maximum number of hashes accepted in a single
	// ReadyMessage.AbsentHashes.  Matches the receiver-side maxHashesPerAnnounce
	// limit.  A ReadyMessage that exceeds this is treated as an error reply so
	// the publisher does not attempt to allocate a proportionally large
	// pushToReceiver work list.
	const maxAbsentHashes = 1_000_000

	if err := client.Subscribe(readyFilter, 1, func(m *broker.Message) {
		var rm broker.ReadyMessage
		if err := m.Decode(&rm); err != nil {
			cfg.Obs.Logger.Warn("mqtt: failed to decode ReadyMessage",
				"topic", m.Topic, "error", err)
			return
		}
		// Reject maliciously large AbsentHashes lists before they reach the
		// push loop, which would iterate over every entry.
		if len(rm.AbsentHashes) > maxAbsentHashes {
			cfg.Obs.Logger.Warn("mqtt: ReadyMessage has too many absent hashes — rejecting",
				"node_id", rm.NodeID, "absent_hashes", len(rm.AbsentHashes), "limit", maxAbsentHashes)
			rm = broker.ReadyMessage{
				NodeID: rm.NodeID,
				Error:  fmt.Sprintf("absent_hashes count %d exceeds limit of %d", len(rm.AbsentHashes), maxAbsentHashes),
			}
		}
		// Non-blocking send: if distributeMQTT has already returned (timeout,
		// context cancel, early quorum) the channel may be full or unread.
		// Dropping the reply is safe — the receiver's session token will expire.
		select {
		case readyCh <- readyResult{nodeID: rm.NodeID, msg: rm}:
		default:
			cfg.Obs.Logger.Warn("mqtt: readyCh full; dropping ready reply — increase buffer or reduce receiver count",
				"node_id", rm.NodeID)
		}
	}); err != nil {
		return 0, 0, fmt.Errorf("distributeMQTT: subscribing to %q: %w", readyFilter, err)
	}

	// Unsubscribe when we are done collecting so Paho's delivery goroutine is
	// not left blocked on a full or unread readyCh after distributeMQTT returns.
	// The drain loop after Unsubscribe clears any messages that arrived between
	// the collect loop exiting and the UNSUBACK being processed.
	defer func() {
		if err := client.Unsubscribe(readyFilter); err != nil {
			cfg.Obs.Logger.Warn("mqtt: unsubscribe from ready filter failed", "error", err)
		}
		for {
			select {
			case <-readyCh:
			default:
				return
			}
		}
	}()

	// --- Step 3: Publish the announce ---
	announceTopic := broker.AnnounceTopic(cfg.Repo)
	announceMsg := broker.AnnounceMessage{
		PayloadID:   payloadID,
		PublisherID: publisherID,
		Repo:        cfg.Repo,
		Hashes:      hashes,
		TotalBytes:  cfg.TotalBytes,
	}
	if err := client.Publish(announceTopic, 1, false, announceMsg); err != nil {
		return 0, 0, fmt.Errorf("distributeMQTT: publishing announce to %q: %w", announceTopic, err)
	}
	cfg.Obs.Logger.Info("mqtt: announce published",
		"payload_id", payloadID,
		"repo", cfg.Repo,
		"hashes", len(hashes))

	// --- Step 4: Collect ReadyMessages until quorum or timeout ---
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	type nodeInfo struct {
		msg broker.ReadyMessage
	}
	readyNodes := make(map[string]nodeInfo) // node_id → info
	errorNodes := make(map[string]string)   // node_id → error

	// quorumRequired computes the minimum number of successful nodes needed.
	quorumRequired := func(n int) int {
		if n == 0 {
			return 0
		}
		q := cfg.Quorum
		if q <= 0 {
			q = 1.0
		}
		r := int(math.Ceil(float64(n) * q))
		if r < 0 || r > n {
			r = n
		}
		return r
	}

collect:
	for {
		// Early-exit only when expectedNodes is known (cfg.Endpoints populated).
		// When expectedNodes == 0 we have no denominator until the timeout fires,
		// so attempting early quorum would break on the very first reply
		// (quorumRequired(1) == 1 with default Quorum=1.0).
		if expectedNodes > 0 {
			successCount := len(readyNodes)
			if successCount >= quorumRequired(expectedNodes) {
				cfg.Obs.Logger.Info("mqtt: quorum of ready nodes reached — not waiting for remaining",
					"ready", successCount, "required", quorumRequired(expectedNodes))
				break collect
			}
		}

		select {
		case <-ctx.Done():
			return 0, 0, ctx.Err()

		case <-deadline.C:
			cfg.Obs.Logger.Info("mqtt: quorum timeout — proceeding with ready nodes",
				"ready", len(readyNodes),
				"errors", len(errorNodes))
			break collect

		case result := <-readyCh:
			if result.msg.Error != "" {
				cfg.Obs.Logger.Warn("mqtt: receiver declined announce",
					"node_id", result.nodeID,
					"payload_id", payloadID,
					"error", result.msg.Error)
				errorNodes[result.nodeID] = result.msg.Error
			} else {
				cfg.Obs.Logger.Info("mqtt: receiver ready",
					"node_id", result.nodeID,
					"absent_hashes", len(result.msg.AbsentHashes),
					"data_url", result.msg.DataURL)
				readyNodes[result.nodeID] = nodeInfo{msg: result.msg}
			}
		}
	}

	// --- Step 5: Push objects to each ready receiver ---
	type pushResult struct {
		nodeID  string
		success bool
	}
	results := make([]pushResult, 0, len(readyNodes))
	var mu sync.Mutex
	var wg sync.WaitGroup

	for nodeID, info := range readyNodes {
		nodeID, info := nodeID, info
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok := pushToReceiver(ctx, nodeID, info.msg, casBackend, log, cfg)
			mu.Lock()
			results = append(results, pushResult{nodeID: nodeID, success: ok})
			mu.Unlock()
		}()
	}
	wg.Wait()

	// Count successful pushes.
	successCount := 0
	for _, pr := range results {
		if pr.success {
			successCount++
		}
	}

	totalNodes := expectedNodes
	if totalNodes == 0 {
		totalNodes = len(readyNodes) + len(errorNodes)
	}

	required := quorumRequired(totalNodes)
	if successCount < required {
		return successCount, totalNodes,
			fmt.Errorf("mqtt: quorum not reached: %d/%d confirmations", successCount, required)
	}

	cfg.Obs.Logger.Info("mqtt: distribution complete",
		"confirmed", successCount,
		"total_nodes", totalNodes,
		"objects", len(hashes))
	return successCount, totalNodes, nil
}

// pushToReceiver pushes the AbsentHashes from a ReadyMessage to the receiver's
// plain-HTTP data channel, using the session token from the ReadyMessage.
// Returns true if all objects were delivered successfully (with retries).
//
// The SSRF guard is enforced inside pushObject (which validates the data
// endpoint URL before making any connection), so no additional check is needed
// here.
func pushToReceiver(
	ctx context.Context,
	nodeID string,
	rm broker.ReadyMessage,
	casBackend cas.Backend,
	log *DistLog,
	cfg Config,
) bool {
	if len(rm.AbsentHashes) == 0 {
		cfg.Obs.Logger.Info("mqtt: receiver already holds all objects — no push needed",
			"node_id", nodeID)
		return true
	}

	cfg.Obs.Logger.Info("mqtt: pushing objects to receiver",
		"node_id", nodeID,
		"absent_hashes", len(rm.AbsentHashes),
		"data_url", rm.DataURL)

	const maxAttempts = 4
	allOK := true
	for _, hash := range rm.AbsentHashes {
		var lastErr error
		for attempt := 0; attempt < maxAttempts; attempt++ {
			if attempt > 0 {
				// Exponential backoff: 1s, 2s, 4s (capped at 30s).
				secs := int64(1) << uint(attempt-1)
				backoff := time.Duration(math.Min(
					float64(time.Second)*float64(secs),
					float64(30*time.Second),
				))
				select {
				case <-ctx.Done():
					return false
				case <-time.After(backoff):
				}
			}
			// Pass "" as the legacy endpoint — pushObject uses sessionToken +
			// dataEndpoint for the data-channel path, ignoring the first arg.
			lastErr = pushObject(ctx, "", hash, casBackend, cfg.Timeout, rm.SessionToken, rm.DataURL)
			if lastErr == nil {
				break
			}
		}
		if lastErr != nil {
			cfg.Obs.Logger.Error("mqtt: pushing object to receiver (all attempts failed)",
				"node_id", nodeID, "hash", hash, "error", lastErr)
			allOK = false
		} else {
			if lerr := log.Record(nodeID, hash); lerr != nil {
				cfg.Obs.Logger.Warn("mqtt: recording push to dist log",
					"node_id", nodeID, "hash", hash, "error", lerr)
			}
		}
	}
	return allOK
}
