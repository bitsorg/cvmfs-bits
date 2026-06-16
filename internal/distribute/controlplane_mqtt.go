// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package distribute

import (
	"fmt"

	"cvmfs.io/prepub/internal/broker"
)

// mqttPublisher adapts *broker.Client to ControlPublisher (ADR-0001 D7, control
// plane implementation #1). It is a thin, faithful wrapper over the existing
// MQTT usage. The legacy push call sites continue to use broker.Client directly
// until the pull path adopts this interface (P1/P2).
type mqttPublisher struct {
	c *broker.Client
}

func newMQTTPublisher(cfg broker.Config) (*mqttPublisher, error) {
	c, err := broker.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("control-plane (mqtt publisher): %w", err)
	}
	return &mqttPublisher{c: c}, nil
}

func (p *mqttPublisher) Announce(repo string, m broker.AnnounceMessage) error {
	return p.c.Publish(broker.AnnounceTopic(repo), 1, false, m)
}

func (p *mqttPublisher) PublishCommitted(repo string, m broker.PublishedMessage) error {
	return p.c.Publish(broker.PublishedTopic(repo), 1, false, m)
}

func (p *mqttPublisher) OnReady(publisherID, payloadID string, handle func(broker.ReadyMessage)) error {
	return p.c.Subscribe(broker.ReadyTopicFilter(publisherID, payloadID), 1, func(msg *broker.Message) {
		var rm broker.ReadyMessage
		if err := msg.Decode(&rm); err != nil {
			return
		}
		handle(rm)
	})
}

func (p *mqttPublisher) Close() { p.c.Disconnect(250) }

var _ ControlPublisher = (*mqttPublisher)(nil)

// mqttReceiver adapts *broker.Client to ControlReceiver, connecting with the
// offline PresenceMessage as the Last-Will-and-Testament — matching the existing
// receiver behaviour (retained presence + LWT, ADR D7).
type mqttReceiver struct {
	c *broker.Client
}

func newMQTTReceiver(cfg broker.Config, presence broker.PresenceMessage) (*mqttReceiver, error) {
	offline := presence
	offline.Online = false
	c, err := broker.NewWithLWT(cfg, broker.PresenceTopic(presence.NodeID), 1, true, offline)
	if err != nil {
		return nil, fmt.Errorf("control-plane (mqtt receiver): %w", err)
	}
	return &mqttReceiver{c: c}, nil
}

func (r *mqttReceiver) SubscribeAnnounce(handle func(broker.AnnounceMessage)) error {
	return r.c.Subscribe(broker.AnnounceTopicFilter(), 1, func(msg *broker.Message) {
		var a broker.AnnounceMessage
		if err := msg.Decode(&a); err != nil {
			return
		}
		handle(a)
	})
}

func (r *mqttReceiver) SubscribePublished(handle func(broker.PublishedMessage)) error {
	return r.c.Subscribe(broker.PublishedTopicFilter(), 1, func(msg *broker.Message) {
		var pm broker.PublishedMessage
		if err := msg.Decode(&pm); err != nil {
			return
		}
		handle(pm)
	})
}

func (r *mqttReceiver) SendReady(publisherID, payloadID, nodeID string, m broker.ReadyMessage) error {
	return r.c.Publish(broker.ReadyTopic(publisherID, payloadID, nodeID), 1, false, m)
}

func (r *mqttReceiver) SetPresence(p broker.PresenceMessage) error {
	return r.c.Publish(broker.PresenceTopic(p.NodeID), 1, true, p)
}

func (r *mqttReceiver) Close() { r.c.Disconnect(250) }

var _ ControlReceiver = (*mqttReceiver)(nil)
