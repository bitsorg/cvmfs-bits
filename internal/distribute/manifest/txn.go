// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package manifest

import "time"

// Phase is a three-phase-commit phase for a distribution transaction (ADR D2):
// objects are prepared on Stratum 0, replicas are warmed, then the catalog is
// committed. Abort unwinds a prepared-but-not-committed transaction.
type Phase string

const (
	PhasePrepare Phase = "prepare"
	PhaseWarm    Phase = "warm"
	PhaseCommit  Phase = "commit"
	PhaseAbort   Phase = "abort"
)

// TxnRecord is the durable journal record for a distribution transaction
// (ADR R1). It is defined here in P0; persistence via internal/spool and the
// crash-recovery reconcile are wired in P3.
type TxnRecord struct {
	TxnID          string    `json:"txn_id"`
	Repo           string    `json:"repo"`
	Phase          Phase     `json:"phase"`
	TargetRootHash string    `json:"target_root_hash"`
	GCPin          string    `json:"gc_pin,omitempty"` // pin/lease protecting objects in the prepare→commit window (ADR R2)
	At             time.Time `json:"at"`
}
