// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package buildset

import (
	"encoding/json"
	"testing"
)

// Conflicts reach API clients (finalize responses); keys are snake_case like
// every other field there.
func TestConflict_JSONKeys(t *testing.T) {
	b, err := json.Marshal(Conflict{Path: "x86_64/pkg/1.0", Reason: "fingerprint differs"})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"path":"x86_64/pkg/1.0","reason":"fingerprint differs"}`; string(b) != want {
		t.Errorf("got %s, want %s", b, want)
	}
}
