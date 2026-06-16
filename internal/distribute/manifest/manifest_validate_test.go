// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package manifest

import "testing"

func TestValidateRejectsUnsafeObjectHash(t *testing.T) {
	m := &Manifest{
		TransactionID: "x", Repo: "r", TargetRootHash: "t",
		BaseURLs: []string{"u"}, Generator: GeneratorPipeline,
		Objects: []ObjRef{{Hash: "../../etc/passwd"}},
	}
	if err := m.Validate(); err == nil {
		t.Fatalf("expected validation error for path-traversal hash")
	}
	// A normal hex digest with a content-type suffix letter must pass.
	m.Objects = []ObjRef{{Hash: "0123456789abcdef0123456789abcdef01234567C"}}
	if err := m.Validate(); err != nil {
		t.Fatalf("valid object hash rejected: %v", err)
	}
}
