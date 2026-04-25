package broker

import (
	"strings"
	"testing"
)

// TestAnnounceTopic verifies the expected topic string for a given repository.
func TestAnnounceTopic(t *testing.T) {
	got := AnnounceTopic("atlas.cern.ch")
	want := "cvmfs/repos/atlas.cern.ch/announce"
	if got != want {
		t.Errorf("AnnounceTopic = %q, want %q", got, want)
	}
}

// TestAnnounceTopicFilter verifies the wildcard announce filter.
func TestAnnounceTopicFilter(t *testing.T) {
	got := AnnounceTopicFilter()
	want := "cvmfs/repos/+/announce"
	if got != want {
		t.Errorf("AnnounceTopicFilter = %q, want %q", got, want)
	}
}

// TestPresenceTopic verifies the retained presence topic for a node.
func TestPresenceTopic(t *testing.T) {
	got := PresenceTopic("stratum1-cern")
	want := "cvmfs/receivers/stratum1-cern/presence"
	if got != want {
		t.Errorf("PresenceTopic = %q, want %q", got, want)
	}
}

// TestPresenceTopicFilter verifies the wildcard presence filter.
func TestPresenceTopicFilter(t *testing.T) {
	got := PresenceTopicFilter()
	want := "cvmfs/receivers/+/presence"
	if got != want {
		t.Errorf("PresenceTopicFilter = %q, want %q", got, want)
	}
}

// TestReadyTopic verifies the per-node reply topic.
func TestReadyTopic(t *testing.T) {
	got := ReadyTopic("pub-abc123", "payload-xyz", "node-1")
	want := "cvmfs/publishers/pub-abc123/ready/payload-xyz/node-1"
	if got != want {
		t.Errorf("ReadyTopic = %q, want %q", got, want)
	}
}

// TestReadyTopicFilter verifies the wildcard ready filter (matches all nodes for
// a specific publisher/payload pair).
func TestReadyTopicFilter(t *testing.T) {
	got := ReadyTopicFilter("pub-abc123", "payload-xyz")
	want := "cvmfs/publishers/pub-abc123/ready/payload-xyz/+"
	if got != want {
		t.Errorf("ReadyTopicFilter = %q, want %q", got, want)
	}
}

// TestReadyTopicFilter_NotMatchOtherPayload verifies that the wildcard filter
// for one payloadID would not lexically match a different payloadID.
// (Structural sanity: the payloadID must appear before the node-level wildcard.)
func TestReadyTopicFilter_NotMatchOtherPayload(t *testing.T) {
	filter := ReadyTopicFilter("pub-abc", "payload-A")
	// A topic for a different payload must not satisfy the filter structurally.
	otherTopic := ReadyTopic("pub-abc", "payload-B", "node-1")
	// The filter and other topic differ in the payload segment — confirm they differ.
	if filter == otherTopic {
		t.Error("ReadyTopicFilter for payload-A should not equal ReadyTopic for payload-B")
	}
	// The wildcard (+) must be the last segment.
	segs := strings.Split(filter, "/")
	if last := segs[len(segs)-1]; last != "+" {
		t.Errorf("ReadyTopicFilter last segment should be \"+\", got %q", last)
	}
}

// ── validTopicSegment ─────────────────────────────────────────────────────────

// TestValidTopicSegment_AcceptsValidSegments verifies that normal strings
// (UUIDs, hostnames, alphanumeric identifiers) are accepted.
func TestValidTopicSegment_AcceptsValidSegments(t *testing.T) {
	valid := []string{
		"atlas.cern.ch",
		"stratum1-cern",
		"550e8400-e29b-41d4-a716-446655440000",
		"node1",
	}
	for _, v := range valid {
		if err := validTopicSegment("field", v); err != nil {
			t.Errorf("validTopicSegment(%q) returned unexpected error: %v", v, err)
		}
	}
}

// TestValidTopicSegment_RejectsSpecialChars verifies that MQTT-special
// characters are rejected.
func TestValidTopicSegment_RejectsSpecialChars(t *testing.T) {
	bad := []string{
		"a/b",       // level separator
		"a+b",       // single-level wildcard
		"a#b",       // multi-level wildcard
		"a\x00b",    // NUL byte
		"",          // empty
	}
	for _, v := range bad {
		if err := validTopicSegment("field", v); err == nil {
			t.Errorf("validTopicSegment(%q) expected error, got nil", v)
		}
	}
}

// TestAnnounceTopic_PanicsOnSpecialChars verifies that AnnounceTopic panics
// rather than silently producing a structurally broken topic when a repo name
// contains a forbidden character.
func TestAnnounceTopic_PanicsOnSpecialChars(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("AnnounceTopic with slash in repo should panic")
		}
	}()
	AnnounceTopic("repo/injected")
}

// TestReadyTopic_PanicsOnSlashInNodeID verifies the same for ReadyTopic.
func TestReadyTopic_PanicsOnSlashInNodeID(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("ReadyTopic with slash in nodeID should panic")
		}
	}()
	ReadyTopic("pub-abc", "payload-xyz", "node/injected")
}

// TestTopics_NoSlashPrefix verifies that all topic constructors return topics
// that do not start with "/" (MQTT spec: topics must not start with slash for
// compatibility with most brokers).
func TestTopics_NoSlashPrefix(t *testing.T) {
	topics := []string{
		AnnounceTopic("repo.example.com"),
		AnnounceTopicFilter(),
		PresenceTopic("node-1"),
		PresenceTopicFilter(),
		ReadyTopic("pub", "pay", "node"),
		ReadyTopicFilter("pub", "pay"),
	}
	for _, topic := range topics {
		if strings.HasPrefix(topic, "/") {
			t.Errorf("topic %q must not start with \"/\"", topic)
		}
	}
}
