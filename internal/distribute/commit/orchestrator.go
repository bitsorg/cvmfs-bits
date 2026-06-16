// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package commit

import (
	"context"
	"time"

	"cvmfs.io/prepub/internal/distribute/manifest"
)

// Orchestrator drives the Stratum-0 three-phase distribution commit of ADR-0001
// (D2): Prepare (pin objects, journal, announce so receivers pull) → Warm (wait
// for an authoritative quorum of replicas to report warm) → Commit (flip the
// catalog, journal the terminal record, release the pin, tell receivers). It is
// transport-agnostic: the catalog flip, the control-plane broadcasts, and the
// GC pin are injected as interfaces, so the same logic is exercised by the MQTT
// control plane, an SSE one (P-B), or fakes in tests.
//
// Ordering is chosen for crash safety. The journal is the source of truth: a
// Prepare/Warm record is written and fsync'd before the action it guards, and
// the terminal Commit record is fsync'd before the GC pin is released. So a
// crash can only ever leave a transaction whose recovery is unambiguous (see
// Recover): if the catalog was flipped, the pin is still held and the journal
// says Warm, so the commit is simply finished idempotently; if it was not, the
// journal says Prepare and the transaction is aborted.
type Orchestrator struct {
	Journal   *Journal
	Gate      *WarmGate
	Pinner    Pinner
	Committer Committer
	Notifier  Notifier

	// WarmTimeout bounds how long Run waits for the warm quorum before
	// degrading to a timeout-commit (ADR D6/R5). 0 → 30s.
	WarmTimeout time.Duration
	// PinTTL is the GC-pin lifetime; it must outlast a normal prepare→commit
	// window so a slow warm does not expose objects to GC. 0 → 15m.
	PinTTL time.Duration

	// Log and Metrics are optional (nil-safe).
	Log     func(msg string, args ...any)
	Metrics Observer
}

// Pinner protects a transaction's objects from garbage collection during the
// prepare→commit window (ADR R2). Satisfied structurally by *serve.MemPinner;
// in production it is backed by an external, crash-surviving pin (a cvmfs_server
// named tag or a held gateway lease) so that recovery after a publisher crash
// still finds the objects intact.
type Pinner interface {
	Pin(txn string, hashes []string, ttl time.Duration)
	Release(txn string)
}

// Committer performs the catalog flip on Stratum 0 (e.g. cvmfs_server
// transaction/publish, or publishing a new signed .cvmfspublished). It MUST be
// idempotent on (repo, txn): crash recovery may invoke it a second time for a
// transaction whose first commit already landed.
type Committer interface {
	Commit(ctx context.Context, repo, txn, rootHash string) error
}

// Notifier broadcasts control-plane events to receivers. Announce triggers the
// pull (prepare); Committed signals that the catalog flipped. Both are
// best-effort: a failed broadcast never blocks or fails the commit, because
// receivers have a backstop poll of .cvmfspublished (ADR R5) to converge.
type Notifier interface {
	Announce(repo, txn, rootHash string, hashes []string) error
	Committed(repo, txn, rootHash string) error
}

// Observer receives orchestration lifecycle events for metrics/observability.
// Every method is optional in spirit — Orchestrator only calls it when Metrics
// is non-nil.
type Observer interface {
	Prepared(repo string)
	Warmed(repo string, quorum bool)
	Committed(repo string)
	Aborted(repo string)
}

// Txn describes a distribution transaction to commit.
type Txn struct {
	TxnID    string
	Repo     string
	RootHash string
	Hashes   []string // CAS object hashes to pin for the prepare→commit window
}

// Outcome reports how a Run finished.
type Outcome struct {
	Committed bool // catalog was flipped
	Warmed    bool // an authoritative quorum acked before the warm timeout
	Aborted   bool // prepared but not committed (commit error)
}

func (o *Orchestrator) warmTimeout() time.Duration {
	if o.WarmTimeout > 0 {
		return o.WarmTimeout
	}
	return 30 * time.Second
}

func (o *Orchestrator) pinTTL() time.Duration {
	if o.PinTTL > 0 {
		return o.PinTTL
	}
	return 15 * time.Minute
}

func (o *Orchestrator) log(msg string, args ...any) {
	if o.Log != nil {
		o.Log(msg, args...)
	}
}

// Run executes the three-phase commit for t. It always calls Gate.Forget(t.TxnID)
// before returning (honouring the WarmGate lifecycle contract on every path).
//
// A Prepare-journal failure is fatal: without a durable prepare record a crash
// could not be recovered, so Run releases the pin and returns the error without
// touching the catalog. A Commit-journal failure after the catalog already
// flipped is logged but not fatal — the commit is real, and recovery will
// idempotently finish the terminal record on the next start.
func (o *Orchestrator) Run(ctx context.Context, t Txn) (Outcome, error) {
	defer o.Gate.Forget(t.TxnID)

	// ── Phase 1: Prepare ─────────────────────────────────────────────────────
	// Pin first so objects cannot be GC'd between the journal write and the
	// receivers pulling them.
	o.Pinner.Pin(t.TxnID, t.Hashes, o.pinTTL())
	if err := o.append(manifest.PhasePrepare, t); err != nil {
		o.Pinner.Release(t.TxnID)
		o.log("commit: prepare journal failed — aborting before catalog flip",
			"txn", t.TxnID, "error", err)
		return Outcome{}, err
	}
	if o.Metrics != nil {
		o.Metrics.Prepared(t.Repo)
	}
	// Announce is the trigger for receivers to pull; failure only means we warm
	// cold and rely on the timeout-commit + backstop poll.
	if o.Notifier != nil {
		if err := o.Notifier.Announce(t.Repo, t.TxnID, t.RootHash, t.Hashes); err != nil {
			o.log("commit: announce failed — proceeding to degraded warm",
				"txn", t.TxnID, "error", err)
		}
	}

	// ── Phase 2: Warm ────────────────────────────────────────────────────────
	if err := o.append(manifest.PhaseWarm, t); err != nil {
		// Warm-journal failure before the catalog flip is still recoverable as a
		// prepared txn (the Prepare record is durable), so abort cleanly.
		o.append(manifest.PhaseAbort, t) //nolint:errcheck — best effort
		o.Pinner.Release(t.TxnID)
		o.log("commit: warm journal failed — aborting", "txn", t.TxnID, "error", err)
		return Outcome{Aborted: true}, err
	}
	warmed := o.Gate.WaitQuorum(ctx, t.TxnID, o.warmTimeout())
	if o.Metrics != nil {
		o.Metrics.Warmed(t.Repo, warmed)
	}
	if !warmed {
		o.log("commit: warm quorum not reached before timeout — committing degraded "+
			"(receivers converge via published broadcast and backstop poll)", "txn", t.TxnID)
	}

	// ── Phase 3: Commit ──────────────────────────────────────────────────────
	if err := o.Committer.Commit(ctx, t.Repo, t.TxnID, t.RootHash); err != nil {
		// Catalog was NOT flipped: unwind to a terminal abort and release the pin.
		o.append(manifest.PhaseAbort, t) //nolint:errcheck — best effort
		o.Pinner.Release(t.TxnID)
		if o.Metrics != nil {
			o.Metrics.Aborted(t.Repo)
		}
		o.log("commit: catalog commit failed — aborted", "txn", t.TxnID, "error", err)
		return Outcome{Warmed: warmed, Aborted: true}, err
	}
	// Durable terminal record BEFORE releasing the pin: if we crash here, recovery
	// sees Commit (terminal) and does nothing; the pin (in-memory) is moot.
	if err := o.append(manifest.PhaseCommit, t); err != nil {
		o.log("commit: terminal journal write failed after catalog flip — recovery "+
			"will idempotently re-finish this txn", "txn", t.TxnID, "error", err)
	}
	o.Pinner.Release(t.TxnID)
	if o.Metrics != nil {
		o.Metrics.Committed(t.Repo)
	}
	if o.Notifier != nil {
		if err := o.Notifier.Committed(t.Repo, t.TxnID, t.RootHash); err != nil {
			o.log("commit: committed broadcast failed (commit already durable)",
				"txn", t.TxnID, "error", err)
		}
	}
	return Outcome{Committed: true, Warmed: warmed}, nil
}

// Recover replays the journal on publisher startup and resolves every
// transaction left non-terminal by a crash (ADR R1). A transaction that had
// reached Warm is finished by re-running the (idempotent) catalog commit; one
// still at Prepare is aborted. Each resolution writes a terminal journal record
// and releases the pin, so Recover is itself idempotent across repeated restarts.
func (o *Orchestrator) Recover(ctx context.Context) error {
	recs, err := o.Journal.Records()
	if err != nil {
		return err
	}
	for _, r := range Reconcile(recs) {
		switch r.Phase {
		case manifest.PhaseWarm:
			if err := o.Committer.Commit(ctx, r.Repo, r.TxnID, r.TargetRootHash); err != nil {
				// Leave it non-terminal: a later restart retries. The pin (external,
				// crash-surviving) keeps the objects safe in the meantime.
				o.log("recover: re-commit failed — will retry next start",
					"txn", r.TxnID, "error", err)
				continue
			}
			o.appendRecord(manifest.TxnRecord{TxnID: r.TxnID, Repo: r.Repo, Phase: manifest.PhaseCommit, TargetRootHash: r.TargetRootHash, GCPin: r.GCPin})
			o.Pinner.Release(r.TxnID)
			if o.Metrics != nil {
				o.Metrics.Committed(r.Repo)
			}
			if o.Notifier != nil {
				_ = o.Notifier.Committed(r.Repo, r.TxnID, r.TargetRootHash)
			}
			o.log("recover: finished warm transaction", "txn", r.TxnID)
		case manifest.PhasePrepare:
			o.appendRecord(manifest.TxnRecord{TxnID: r.TxnID, Repo: r.Repo, Phase: manifest.PhaseAbort, TargetRootHash: r.TargetRootHash, GCPin: r.GCPin})
			o.Pinner.Release(r.TxnID)
			if o.Metrics != nil {
				o.Metrics.Aborted(r.Repo)
			}
			o.log("recover: aborted prepared-but-unwarmed transaction", "txn", r.TxnID)
		}
	}
	return nil
}

// append writes one phase record for t, stamping a GC-pin id equal to the txn id.
func (o *Orchestrator) append(phase manifest.Phase, t Txn) error {
	return o.appendRecord(manifest.TxnRecord{
		TxnID:          t.TxnID,
		Repo:           t.Repo,
		Phase:          phase,
		TargetRootHash: t.RootHash,
		GCPin:          t.TxnID,
	})
}

func (o *Orchestrator) appendRecord(rec manifest.TxnRecord) error {
	rec.At = time.Now()
	if o.Journal == nil {
		return nil
	}
	return o.Journal.Append(rec)
}
