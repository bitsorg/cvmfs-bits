// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package manifest

import "time"

// Phase is a three-phase-commit phase for a distribution transaction:
// objects are prepared on Stratum 0, replicas are warmed, then the catalog is
// committed. Abort unwinds a prepared-but-not-committed transaction.
type Phase string

const (
	PhasePrepare Phase = "prepare"
	PhaseWarm    Phase = "warm"
	PhaseCommit  Phase = "commit"
	PhaseAbort   Phase = "abort"
)

// TxnRecord is the durable journal record for a distribution transaction,
// persisted by commit.Journal and replayed on restart by commit.Reconcile.
type TxnRecord struct {
	TxnID          string    `json:"txn_id"`
	Repo           string    `json:"repo"`
	Phase          Phase     `json:"phase"`
	TargetRootHash string    `json:"target_root_hash"`
	GCPin          string    `json:"gc_pin,omitempty"` // pin/lease protecting objects in the prepare→commit window
	At             time.Time `json:"at"`
}
