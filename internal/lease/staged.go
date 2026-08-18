// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"errors"
	"reflect"
)

// StagedBackend publishes a job whose content a producer prepared elsewhere.
//
// A producer running the canonical CVMFS publisher on a build node writes the
// chunked, compressed, content-addressed objects into a staging prefix and
// builds the subtree catalog. prepub promotes those objects into the store and
// then only has to graft the catalog. There is no payload to send.
//
// It is the gateway Client in every respect but two, and it delegates
// everything else — Acquire, Heartbeat, Abort, Probe — to it:
//
//   - NeedsPipeline is false. The pipeline turns a tar into CAS objects and a
//     catalog, and a staged job arrives with both already made. Saying true
//     here would make the orchestrator demand a tar that does not exist.
//   - Commit skips SubmitPayload. The objects reached the store by a
//     server-side copy before the commit (see cas.PromoteFrom), so uploading
//     them through prepub is the exact work this design removes. What remains
//     is the finalise POST, which CommitFinalizeOnly already performs — this
//     is a routing decision, not new protocol.
//
// Registered as its own publish path rather than folded into "ingest": that
// path hands a tar to `cvmfs_server ingest`, which is a different mechanism
// with a different failure mode, and a job must name the one it means.
type StagedBackend struct {
	*Client
	// remover deletes a published subtree for conflict remediation. Nil when
	// the deployment offers no path that can run `cvmfs_server`, in which case
	// DeleteSubtree reports ErrSubtreeDeleteUnsupported rather than pretending.
	remover subtreeRemover
}

// subtreeRemover is the one capability the staged path borrows for
// remediation. Deleting a published subtree is repository-level work
// (`cvmfs_server ingest -f <path> <repo>`) and has nothing to do with how the
// content originally arrived, so the staged path delegates rather than
// carrying a second copy of it.
type subtreeRemover interface {
	DeleteSubtree(ctx context.Context, repo, relPath string) error
}

// ErrSubtreeDeleteUnsupported reports that this deployment cannot delete a
// published subtree, so a conflict on the staged path stays terminal.
//
// A sentinel rather than a missing method: in Go the method set decides
// interface satisfaction, so StagedBackend either always implements
// subtreeDeleter or never does. Always, plus an honest error, keeps the
// capability visible and the failure legible.
var ErrSubtreeDeleteUnsupported = errors.New(
	"staged backend: this prepub has no publish path that can delete a " +
		"subtree (needs --ingest-publish, which provides cvmfs_server)")

// NewStagedBackend wraps an existing gateway client. The client is shared, not
// copied — its connection pools, retry budget and credentials are the same
// ones the default path uses.
//
// remover may be nil; it is the ingest backend when this prepub offers that
// path, borrowed solely so replace_on_conflict behaves the same on both.
func NewStagedBackend(c *Client, remover subtreeRemover) *StagedBackend {
	// Guard the typed-nil trap. cmd/prepub holds the ingest backend as a
	// *IngestBackend and hands it here as this interface; when --ingest-publish
	// is off that pointer is nil, and a nil pointer wrapped in an interface is
	// itself NOT nil. Left as-is it defeats DeleteSubtree's `remover == nil`
	// check and dispatches a delete to a nil receiver — a panic in the job path,
	// which has no recover(). Normalise any nil-pointer remover back to a real
	// nil interface so the downstream check stays sufficient for every caller.
	if remover != nil {
		if rv := reflect.ValueOf(remover); rv.Kind() == reflect.Ptr && rv.IsNil() {
			remover = nil
		}
	}
	return &StagedBackend{Client: c, remover: remover}
}

// DeleteSubtree removes a published path so a staged commit can be retried.
//
// The staged path cannot avoid needing this. A producer can prepare over an
// occupied path (swissknife -D with -f), but the receiver's graft is add-only
// by construction, so the commit is refused until the existing subtree is
// gone. Without this the staged path silently had weaker semantics than
// ingest, which is what MEASUREMENTS.md §25 claimed it did not.
func (b *StagedBackend) DeleteSubtree(ctx context.Context, repo, relPath string) error {
	if b.remover == nil {
		return ErrSubtreeDeleteUnsupported
	}
	return b.remover.DeleteSubtree(ctx, repo, relPath)
}

// NeedsPipeline reports false: a staged job carries no payload to process.
func (b *StagedBackend) NeedsPipeline() bool { return false }

// Commit grafts the producer's catalog without uploading anything.
//
// CommitFinalizeOnly documents its precondition as "all catalog objects have
// already been uploaded via SubmitPayload". Promotion satisfies that
// precondition by a different route — the objects are in the repository's own
// store, which is where SubmitPayload would have put them — and the gateway
// receiver fetches the catalog from stratum0 by content hash either way.
func (b *StagedBackend) Commit(ctx context.Context, req CommitRequest) error {
	return b.Client.CommitFinalizeOnly(ctx, req)
}
