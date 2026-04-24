package job

import "fmt"

// validTransitions encodes the FSM.
//
// Lease-window optimisation: the gateway lease is now acquired AFTER objects
// have been uploaded to CAS and pre-pushed to all Stratum 1 replicas.  This
// shrinks the lease window from O(minutes) to O(seconds) (just the gateway
// SubmitPayload + commit RPC), allowing other publishers to work on the same
// repository sub-path with far less contention.
//
// New order: incoming → staging → uploading → [distributing →] leased → committing → published
//                                                  ↑
//                             uploading → leased  (fast path: no Stratum 1s)
var validTransitions = map[State]map[State]bool{
	StateIncoming: {
		StateStaging: true, // pipeline starts immediately — no lease needed yet
		StateAborted: true,
		StateFailed:  true,
	},
	StateStaging: {
		StateUploading: true,
		StateAborted:   true,
		StateFailed:    true,
	},
	StateUploading: {
		StateDistributing: true,
		StateLeased:       true, // fast path: no Stratum 1s → acquire lease directly
		StateAborted:      true,
		StateFailed:       true,
	},
	StateDistributing: {
		StateLeased:  true, // lease acquired HERE — after all replicas have the objects
		StateAborted: true,
		StateFailed:  true,
	},
	StateLeased: {
		StateCommitting: true, // only the commit window needs the lease
		StateAborted:    true,
		StateFailed:     true,
	},
	StateCommitting: {
		StatePublished: true,
		StateAborted:   true,
		StateFailed:    true,
	},
	StatePublished: {},
	StateAborted:   {},
	StateFailed:    {},
}

// Transition validates that a state transition from→to is allowed.
func Transition(from, to State) error {
	allowed, exists := validTransitions[from]
	if !exists {
		return fmt.Errorf("unknown state: %s", from)
	}
	if !allowed[to] {
		return fmt.Errorf("invalid transition: %s -> %s", from, to)
	}
	return nil
}

// IsTerminal returns true if the state is a terminal state.
func IsTerminal(s State) bool {
	return s == StatePublished || s == StateAborted || s == StateFailed
}
