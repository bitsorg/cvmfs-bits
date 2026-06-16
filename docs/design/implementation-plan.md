# Implementation plan — pull-based S1 distribution (ADR-0001)

Local working doc (uncommitted, like the ADR). Realises
[ADR-0001](../adr/0001-pull-based-distribution.md) in **cvmfs-bits**, then in
**cvmfs-testbed**. Decision IDs (D1–D10, R1–R5, P-A/P-B) refer to the ADR.

## Principles

- **Push keeps working throughout.** Pull lands behind a flag/discovery selector;
  the default stays push until the benchmark (P-A) says otherwise. `BrokerURL`
  empty → legacy, unchanged.
- **Every phase is shippable and testbed-verified.** Each phase ends with a
  cvmfs-testbed run and explicit exit criteria.
- **Interfaces first** (`Fetcher`, `ControlPlane`) so MQTT/SSE and per-object/
  bundle are interchangeable leaves and never leak (D5, D7).
- **Idempotency + durable journal** are not optional add-ons; they land with the
  three-phase commit (R1–R5).

## Anchors in the current code

- Publish lifecycle / commit: `internal/api/orchestrator.go` (+ `internal/lease`
  `Acquire/Commit/Abort/Heartbeat`). P1 upload and P3 commit live here; the
  warm-gate inserts just before `lease.Commit`.
- Control plane: `internal/broker` (`Publish/Subscribe`, presence+LWT), consumed
  by `internal/distribute/distributor_mqtt.go` and
  `internal/distribute/receiver/mqtt_handler.go`.
- Data plane today (push): `internal/distribute/push.go`,
  `receiver/handler.go` (`PUT /api/v1/objects/{hash}`, temp+verify), `internal/cas`.
- Crash-recovery substrate: `internal/spool/journal.go` (`Append/Read`).
- GC: `internal/gc` (verify the pin/retention hook here for R2).
- Catalog/diff: `pkg/cvmfscatalog` (manifest, catalog walk) for D4 cold-start.

---

## Phase 0 — Interfaces & scaffolding (no behaviour change)

- Define `Fetcher` (D5): `Fetch(ctx, obj ObjRef, into io.Writer) error`;
  implementations register by name.
- Define `ControlPlane` (D7) — the six-method interface from the ADR companion
  §2.1; wrap the existing MQTT usage as `mqttControlPlane` with **no behaviour
  change** (pure refactor; existing tests must stay green).
- Define manifest types (`internal/distribute/manifest`): `Manifest`, `ObjRef`,
  generators `pipeline|diff`.
- Add journal `Entry` kinds for `{txn, phase, gc_pin}` (R1) — not yet written.
- Flags/config in `cmd/prepub/main.go`: `--distribute-mode push|pull` (default
  `push`), `--control-plane mqtt|sse` (default `mqtt`).

**Exit:** builds, full `go test ./...` green, zero functional change. Testbed
unchanged.

## Phase 1 — S0 serving side: objects, manifest, discovery, GC pin

- **Object endpoint (D5, D8):** ensure `GET /cvmfs/{repo}/data/{xx}/{rest}` serves
  CAS objects directly (reuse `internal/cas`); default `auth:public`/cacheable;
  scaffold the optional token gate (D8) behind config.
- **Manifest (D3):** emit the S0-authoritative new-object set from the publish
  pipeline (it already computes dedup) → `GET /s1/{txn}/manifest`. Small JSON now;
  NDJSON deferred to Phase 4.
- **Discovery (D10):** generate signed `GET /cvmfs/{repo}/.cvmfsbits`
  (`control_plane{type,url}`, repos, CA), signed via the repo/whitelist trust path.
- **GC pin (R2):** pin objects for a txn with TTL > prepare window in `internal/gc`;
  enforce P1 ordering (upload+fsync, *then* publish manifest); abort releases pin;
  restart sweep expires leaked pins.

**Exit:** an S1 (or curl) can fetch `.cvmfsbits`, a manifest, and every object by
hash; GC does not reap pinned objects within the window. Push still default.

## Phase 2 — Receiver pull path

- **Puller package** `internal/distribute/puller`: on announce, fetch manifest,
  compute `missing = manifest − localstore` **locally** (D3; no `AbsentHashes`
  round-trip on the critical path), fetch via `Fetcher` (per-object GET default),
  verify each hash, **atomic install** temp→verify→rename (R3, reuse the PUT
  verify logic), persist **last-synced root** (R4).
- Wire `mqttControlPlane` announce/published → puller (reuse existing
  announce/published topics).
- Keep the inbound data listener for now (push fallback); remove only after the
  default flips.

**Exit:** with `--distribute-mode pull`, an S1 warms from S0 over GET on announce;
interrupt mid-pull → restart re-pulls only the gaps. Push path untouched.

## Phase 3 — Three-phase commit, admission, recovery

- **Admission/lease (D6):** `POST /s1/{txn}/lease` (or control-plane grant) with
  rate/slots + lease TTL; revoke on heartbeat loss.
- **Warm-gate + commit (D2, D6):** in `orchestrator.go`, after P1/P2, gate
  `lease.Commit` on an **authoritative quorum** of acks or **timeout**; emit
  `committed`; release GC pin; decouple *committed* from *globally warm* (the
  latter a metric).
- **Journal + reconcile (R1):** write `{txn, phase, pin}` transitions; on restart
  reconcile idempotently (resume / commit / abort).
- **Degradation ladder (R5):** S1 backstop poll of `.cvmfspublished` when the
  control plane is dead; publishing never blocks on control-plane liveness.
- **Metrics:** per-S1 lag, fleet warmth, bytes re-sent, lease occupancy
  (`pkg/observe`).

**Exit:** publish→warm-quorum→commit works end-to-end; kill an S1, the broker, or
prepub mid-txn and the system recovers (resume / degrade / reconcile) with the
catalog never referencing absent objects.

## Phase 4 — Catch-up & scale

- **Cold/lagging S1 (D4):** generate a cumulative `old→target` manifest from
  `cvmfs_server diff` / catalog walk (`pkg/cvmfscatalog`), streamed as **NDJSON**;
  same pull path.
- Large-delta hardening: streaming manifest parse on both ends (no full buffer).

**Exit:** a fresh S1 cold-syncs the whole repo; a weeks-behind S1 catches up via
one cumulative diff.

## Phase 5 — Benchmark & bundling decision (P-A)

- **Harness** in cvmfs-testbed (below): Fetcher variants — per-object HTTP/1.1,
  HTTP/2, HTTP/3/QUIC, parallel-connection sweep, length-prefixed batch, tar/cpio,
  zstd batch — ± forward proxy.
- Run the ADR matrix on the ~90 GB / incremental / lossy workloads; collect the
  metrics; compare against the targets.
- **Decide P-A:** adopt a bundling `Fetcher` only if it beats cached per-object GET
  per the gate; else per-object GET stays default.

**Exit:** a results table in the ADR/benchmark doc and a go/no-go on bundling.

## Phase 6 — SSE control-plane backend (P-B, optional)

- Implement `sseControlPlane` (companion §2.1): `GET …/events` (hub + ring
  buffer + Last-Event-ID), `POST …/{ready,ack,lease,presence}`, heartbeat/timeout,
  mTLS authz. Select via `.cvmfsbits` `control_plane.type=sse`.
- A/B against MQTT in the testbed; decide whether to drop the broker.

**Exit:** the testbed runs identically on either control plane by flipping
`.cvmfsbits`.

## Cut-over

- After Phase 5 (and 3) prove out: flip default `--distribute-mode pull`, retire
  the inbound data listener, keep push reachable for one release as fallback, then
  remove.

---

## cvmfs-testbed implementation (parallel, per phase)

The testbed must exercise each phase; binaries are injected from the host (no
image rebuilds), so changes are mostly compose + scripts.

- **New profile `docker-compose.pull.yml`** (sibling of `docker-compose.mqtt.yml`):
  - S0/prepub: serve `/data` + `/s1/{txn}/manifest` + signed `.cvmfsbits` + control
    plane; pass `--distribute-mode pull`.
  - `stratum1-a`/`stratum1-b`: `cvmfs-prepub --mode receiver` in pull mode; **drop
    the inbound 9100 data exposure** (keep only under the legacy push profile).
  - Add a **squid forward-proxy** container in front of S0 for the caching/benchmark
    variants (Phase 5).
  - Keep the MQTT broker from the mqtt profile; add an SSE-only variant for Phase 6.
- **Scripts (`cvmfs-testbed/scripts/`):**
  - `gen-payload.sh` — synthesize a realistic small-file corpus (incremental and a
    ~90 GB cold set) for the benchmark.
  - `benchmark.sh` — drive the Fetcher variants × workloads, scrape metrics, emit a
    CSV/table (Phase 5).
  - `e2e-pull.sh` — publish on S0; assert all S1 warm **before** commit; assert a
    client sees a consistent view; fault-injection: kill an S1 mid-pull (assert
    resume), kill the broker (assert backstop-poll degradation), kill prepub
    mid-txn (assert journal reconcile).
- **Monitoring:** extend the VictoriaMetrics/vmagent dashboards with per-S1 lag,
  fleet-warmth, bytes-re-sent, lease occupancy.
- **CI:** add a `pull`-profile job to the testbed runner that boots the stack and
  runs `e2e-pull.sh` on every change.

**Testbed exit criteria mirror the phases:** Phase 1 → discovery+manifest+object
fetch reachable; Phase 2 → receiver warms over GET; Phase 3 → e2e warm-gate +
fault injection pass; Phase 4 → cold-sync + catch-up pass; Phase 5 → benchmark CSV
produced; Phase 6 → identical run on SSE.

## Sequencing / dependencies

```
P0 ──> P1 ──> P2 ──> P3 ──> P4 ──> P5 ──> (cut-over)
                      └─────────────────> P6 (optional, parallel after P3)
testbed pull profile lands with P1 and grows each phase; benchmark harness in P5.
```

## Risks to watch during implementation

- GC pin correctness (R2) — the easiest way to lose data; test the abort/TTL/leak
  paths first.
- Orchestrator commit-gate must not deadlock publishing — keep the timeout +
  minimum-warm rule (D6) front and centre.
- Manifest/`AbsentHashes` removal (D3) touches `distributor_mqtt.go` and the
  receiver Bloom path — keep Bloom as optional telemetry, don't regress push.
- Discovery-doc signing reuse — confirm the whitelist/repo key path is available
  to prepub before relying on it (D10).
