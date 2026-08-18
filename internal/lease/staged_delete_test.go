// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"errors"
	"testing"
)

type recordingRemover struct {
	calls []string
	err   error
}

func (r *recordingRemover) DeleteSubtree(_ context.Context, repo, rel string) error {
	r.calls = append(r.calls, repo+"|"+rel)
	return r.err
}

// The staged path must be able to delete, or replace_on_conflict is inert on
// it and the three publish paths have different semantics -- which is what the
// 2026-08-16 run showed ("this publish path cannot delete a subtree").
//
// NEGATIVE CONTROL: delete the DeleteSubtree method from StagedBackend and
// this stops compiling at the subtreeRemover assertion below.
func TestStagedBackend_DelegatesTheDelete(t *testing.T) {
	rem := &recordingRemover{}
	b := NewStagedBackend(&Client{}, rem)

	if err := b.DeleteSubtree(context.Background(), "r.cern.ch", "x86_64/pkg/1.0"); err != nil {
		t.Fatalf("DeleteSubtree: %v", err)
	}
	if len(rem.calls) != 1 || rem.calls[0] != "r.cern.ch|x86_64/pkg/1.0" {
		t.Errorf("delegation = %v", rem.calls)
	}
	// It must satisfy the capability the orchestrator asserts on.
	var _ subtreeRemover = b
}

// A deployment without the ingest path cannot delete. Saying so with a
// sentinel keeps the orchestrator's decline path reachable; returning nil
// would make a conflict look remediated when nothing was removed.
func TestStagedBackend_WithoutARemoverSaysSoAndDeletesNothing(t *testing.T) {
	b := NewStagedBackend(&Client{}, nil)
	err := b.DeleteSubtree(context.Background(), "r.cern.ch", "x86_64/pkg/1.0")
	if !errors.Is(err, ErrSubtreeDeleteUnsupported) {
		t.Fatalf("err = %v, want ErrSubtreeDeleteUnsupported", err)
	}
}

// A real failure must NOT be mistaken for "unsupported": one leaves the error
// terminal, the other reports a broken deletion.
func TestStagedBackend_RealDeleteFailureIsNotUnsupported(t *testing.T) {
	boom := errors.New("cvmfs_server ingest -f: exit 1")
	b := NewStagedBackend(&Client{}, &recordingRemover{err: boom})
	err := b.DeleteSubtree(context.Background(), "r.cern.ch", "p/1.0")
	if errors.Is(err, ErrSubtreeDeleteUnsupported) {
		t.Errorf("a genuine failure was reported as unsupported: %v", err)
	}
	if !errors.Is(err, boom) {
		t.Errorf("lost the underlying error: %v", err)
	}
}

// The production caller (cmd/prepub) holds a *IngestBackend and passes it as
// the subtreeRemover interface; when --ingest-publish is off that pointer is
// nil. A nil pointer wrapped in an interface is NOT a nil interface, so without
// the constructor's guard `remover == nil` is false and DeleteSubtree
// dispatches the delete to a nil *IngestBackend receiver, which panics in
// Acquire (b.queueFor) — in the job path, which has no recover(). The earlier
// "WithoutARemover" test passes an UNTYPED nil and so does not reproduce this.
//
// NEGATIVE CONTROL: remove the reflect-based normalisation in NewStagedBackend
// and this panics (nil-receiver dereference) instead of returning the sentinel.
func TestStagedBackend_TypedNilRemoverIsUnsupportedNotAPanic(t *testing.T) {
	var ib *IngestBackend // typed nil — exactly what main.go passes when ingest is off
	b := NewStagedBackend(&Client{}, ib)
	err := b.DeleteSubtree(context.Background(), "r.cern.ch", "x86_64/pkg/1.0")
	if !errors.Is(err, ErrSubtreeDeleteUnsupported) {
		t.Fatalf("err = %v, want ErrSubtreeDeleteUnsupported "+
			"(a typed-nil remover must be treated as absent, not dispatched to)", err)
	}
}
