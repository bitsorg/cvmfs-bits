<!--
SPDX-FileCopyrightText: 2026 CERN
SPDX-License-Identifier: Apache-2.0
-->

# ADR-0001 cut-over plan: push → pull

This is the rollout plan for switching the Stratum-0 → Stratum-1 data plane from
the legacy **push** model (S0 PUTs objects to each S1) to the ADR-0001 **pull**
model (S1 fetches objects from S0 when notified), and finally retiring the
inbound push path. It is deliberately staged and reversible up to the last step:
content addressing means a repository warmed by push and one warmed by pull are
byte-for-byte identical, so the two models can run side by side and be compared
object-for-object before any commitment.

## Guiding invariants (must hold at every stage)

- **Identical state.** Objects are content-addressed (CAS key = SHA-1 of the
  compressed bytes). A repo's root-catalog hash after pull equals the one after
  push for the same publish. Every stage verifies this rather than trusting it.
- **No object lost in the prepare→commit window.** The GC pin (`Pinner`) plus the
  three-phase journal (`commit.Journal` / `Orchestrator`) guarantee a crash never
  strands or collects an in-flight object; restart reconcile finishes or aborts.
- **Liveness under degradation.** If the warm quorum is not reached in time the
  publisher commits anyway (degrade) and receivers converge via the published
  broadcast and the `.cvmfspublished` backstop poll (R5). A slow or absent S1
  never blocks a publish.
- **Reversibility.** Until Stage 4, `--distribute-mode` flips between `push` and
  `pull` per process with no schema or on-disk change, so rollback is a restart.

## Preconditions (wiring that must land before Stage 0)

These are the live-integration seams left after P0–P6; all the mechanisms exist
and are unit/-race tested, but they need wiring into the running services:

1. **Publisher orchestration.** Call `commit.Orchestrator.Run` from the publish
   loop with concrete adapters: `Committer` = `cvmfs_server` transaction/publish;
   `Notifier` = the selected control plane; `Pinner` = `serve.MemPinner` backed by
   a crash-surviving external pin (a `cvmfs_server` named tag or held gateway
   lease); journal under the spool dir; call `Orchestrator.Recover` on startup.
2. **Manifest ingest.** The gateway (or pipeline) POSTs the per-transaction
   manifest to `POST /api/v1/distribute/manifests` so S1 `OnTransaction` finds it.
3. **Ack routing.** Publisher `OnReady` → `WarmGate.Ack`; receiver `OnWarmed`
   (already wired in `pull.go`) → publish a Ready over the control plane.
4. **Control-plane selection.** `--control-plane mqtt|sse` chooses `mqttPublisher`
   /`mqttReceiver` or `SSEServer`/`SSEReceiver` (both implement the same
   interfaces; SSE adapters are built and tested).
5. **Catch-up + diff.** Concrete `serve.DiffSource` over `cvmfs_server diff`;
   mount `/s1/catchup` (gated by `credential.Verifier`) and the enroll endpoints;
   provision per-node enrollment keys; receivers run `Coordinator.Catchup` on a
   schedule and as the R5 backstop.
6. **Bundling policy.** Wire `Puller.PullBundle` selection by RTT/object-count per
   `docs/design/bundling-benchmark.md` (optional; per-object is the safe default).
7. **Metrics.** Emit the health signals listed below from the orchestrator,
   warm-gate, puller, and catch-up paths.

## Health signals to watch (gates between stages)

- warm-quorum success rate and time-to-quorum per publish;
- pull success / failure / retry rate per S1; objects fetched vs skipped;
- degraded-commit rate (warm timeouts) — should be near zero in steady state;
- catch-up invocations and bytes (a spike means an S1 fell behind);
- journal reconcile events on restart (prepare-aborts vs warm-commits);
- GC-pin age / leak (pins outliving their commit), admission 429 rate;
- **parity**: pull-warmed root hash == push/ingest root hash (the hard gate).

Parity is checked with the testbed's catalog-dump diff (`make catdiff`, extended
to a `pull` label) and, in production, by comparing each S1's served
`.cvmfspublished` against S0 after each publish.

## Stages

### Stage 0 — Shadow (dual-run, pull is read-only)

Run pull **alongside** push. Push stays the source of truth and the thing that
actually satisfies the commit; pull runs in parallel on the same publishes
(receivers also pull), but the publisher does **not** yet gate commit on the warm
quorum. Measure: do all S1s reach the identical root via pull, and how fast?

- Enable: receivers run `--distribute-mode pull` (they already accept push too);
  publisher announces + serves manifests/objects but commits on the push result.
- Advance when: parity holds across all S1s for N publishes (e.g. one week / 100+
  publishes), warm latency is acceptable, pull failure rate is negligible.
- Rollback: stop announcing / set receivers back to push-only. Zero risk — pull
  was never authoritative.

### Stage 1 — Canary (commit gated on pull for one repo)

Pick a low-traffic, non-critical repo and a subset of S1s as the authoritative
quorum. Turn on warm-gate gating for that repo: the publisher waits for the
authoritative quorum (with the degrade-on-timeout safety net) before commit.
Push remains enabled as the fallback transport for any S1 not yet pulling.

- Advance when: the canary repo runs cleanly for a sustained period — quorum met
  in time, no spurious degrades, parity intact, recovery works across a forced
  publisher restart mid-publish.
- Rollback: drop the repo back to Stage 0 behaviour (commit on push), one flag.

### Stage 2 — Ramp

Extend commit-gating to more repos and bring all S1s into the authoritative set,
batch by batch. Keep push on as a fallback for stragglers (the existing
dual-write path already tolerates mixed peers). Exercise catch-up by deliberately
taking an S1 offline across a few publishes and confirming it converges via
`/s1/catchup` on return.

- Advance when: all production repos and S1s are pull-authoritative and have been
  stable for a defined soak; catch-up is proven on real deltas.
- Rollback: per-repo flag back to push; both transports still coexist.

### Stage 3 — Default flip

Make `--distribute-mode pull` the **default**. Push is now opt-in, retained only
for any explicitly-configured legacy peer. New deployments are pull by default.

- Advance when: no peer depends on push except known, listed exceptions.
- Rollback: still possible — flip the default back; the push code still exists.

### Stage 4 — Retire the inbound push path (point of no return)

Once nothing depends on push: remove the receiver's inbound data listener (the
TCP 9101 PUT endpoint and the HTTP announce/bloom push protocol), remove the
publisher-side push distributor and worker pool, and delete the dead config. This
is the only irreversible step and is taken well after Stage 3 has been stable.

- Pre-flight: confirm zero push traffic in metrics for a defined window; confirm
  no S1 advertises the push data channel; snapshot/back up configs.
- After: the inbound firewall hole for push can be closed — S1s need only
  outbound (control plane + object GET), which was a core ADR-0001 goal.

## Rollback summary

| Stage | Authoritative transport | Rollback |
|------:|-------------------------|----------|
| 0 | push | stop pull (no-op) |
| 1 | push + pull(canary repo) | un-gate the canary repo |
| 2 | pull (ramping) + push fallback | per-repo flag → push |
| 3 | pull (default) + push opt-in | flip default → push |
| 4 | pull only | **none** — push removed |

## Testbed dry-run (before each production stage)

Run the whole ladder in `cvmfs-testbed` first: `make start-pull` brings up the
pull profile; `make test-pull` proves receivers pull and warm; force a
publisher restart mid-publish to exercise `Orchestrator.Recover`; take a
`stratum1-b` offline across publishes and confirm `/s1/catchup` convergence;
`make catdiff` (push vs pull labels) for the parity gate. CI
(`validate:pull-profile`) keeps the overlay honest on every change.

## Done criteria

Pull is the default and only data plane; the inbound push listener is removed;
S1 sites require outbound-only connectivity; parity has held across the entire
rollout; recovery, catch-up, degradation, and admission have all been exercised
under load in the testbed and in production canaries.
