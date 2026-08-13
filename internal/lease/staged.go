// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package lease

import "context"

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
type StagedBackend struct{ *Client }

// NewStagedBackend wraps an existing gateway client. The client is shared, not
// copied — its connection pools, retry budget and credentials are the same
// ones the default path uses.
func NewStagedBackend(c *Client) *StagedBackend { return &StagedBackend{Client: c} }

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
