package job

import "testing"

// TestTransition_LocalModeFastPath verifies that the local-mode fast path
// StateIncoming → StateLeased is accepted by the FSM.
//
// In gateway mode jobs go through StateStaging and StateUploading before
// reaching StateLeased.  In local mode the pipeline is skipped entirely and
// the job jumps directly from StateIncoming to StateLeased.  The FSM must
// permit this or the first Transition call in the orchestrator's local mode
// path panics / returns an error.
func TestTransition_LocalModeFastPath(t *testing.T) {
	if err := Transition(StateIncoming, StateLeased); err != nil {
		t.Errorf("StateIncoming → StateLeased must be valid (local mode fast path): %v", err)
	}
}

// TestTransition_GatewayModePath verifies that the standard gateway mode
// path (incoming → staging → uploading → leased → committing → published)
// is accepted at every step.
func TestTransition_GatewayModePath(t *testing.T) {
	steps := []struct{ from, to State }{
		{StateIncoming, StateStaging},
		{StateStaging, StateUploading},
		{StateUploading, StateLeased},
		{StateLeased, StateCommitting},
		{StateCommitting, StatePublished},
	}
	for _, s := range steps {
		if err := Transition(s.from, s.to); err != nil {
			t.Errorf("expected valid transition %s → %s: %v", s.from, s.to, err)
		}
	}
}

// TestTransition_DistributionPath verifies the optional Stratum 1 distribution
// detour (uploading → distributing → leased).
func TestTransition_DistributionPath(t *testing.T) {
	steps := []struct{ from, to State }{
		{StateUploading, StateDistributing},
		{StateDistributing, StateLeased},
	}
	for _, s := range steps {
		if err := Transition(s.from, s.to); err != nil {
			t.Errorf("expected valid transition %s → %s: %v", s.from, s.to, err)
		}
	}
}

// TestTransition_InvalidRejectsTerminalToNonTerminal verifies that a
// terminal state cannot transition to any other state.
func TestTransition_InvalidRejectsTerminalToNonTerminal(t *testing.T) {
	terminals := []State{StatePublished, StateAborted, StateFailed}
	nonTerminals := []State{StateIncoming, StateStaging, StateUploading,
		StateDistributing, StateLeased, StateCommitting}
	for _, from := range terminals {
		for _, to := range nonTerminals {
			if err := Transition(from, to); err == nil {
				t.Errorf("expected error for %s → %s (terminal state must not transition)", from, to)
			}
		}
	}
}

// TestTransition_InvalidSkipStates verifies that skipping required intermediate
// states in gateway mode is rejected.
func TestTransition_InvalidSkipStates(t *testing.T) {
	invalid := []struct{ from, to State }{
		{StateIncoming, StateCommitting},  // skips staging, uploading, leased
		{StateIncoming, StatePublished},   // skips everything
		{StateStaging, StateLeased},       // skips uploading
		{StateStaging, StateCommitting},   // skips uploading, leased
	}
	for _, s := range invalid {
		if err := Transition(s.from, s.to); err == nil {
			t.Errorf("expected error for invalid transition %s → %s", s.from, s.to)
		}
	}
}

// TestIsTerminal verifies that all terminal states are recognised.
func TestIsTerminal(t *testing.T) {
	terminals := []State{StatePublished, StateAborted, StateFailed}
	for _, s := range terminals {
		if !IsTerminal(s) {
			t.Errorf("expected IsTerminal(%s) == true", s)
		}
	}
	nonTerminals := []State{StateIncoming, StateStaging, StateUploading,
		StateDistributing, StateLeased, StateCommitting}
	for _, s := range nonTerminals {
		if IsTerminal(s) {
			t.Errorf("expected IsTerminal(%s) == false", s)
		}
	}
}
