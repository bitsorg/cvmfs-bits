# cvmfs-prepub

A fast, resilient Go service for pre-processing and publishing software releases
into [CVMFS](https://cernvm.cern.ch/fs/) without holding the repository transaction
lock during file processing.

## The problem

The standard CVMFS publishing workflow acquires an exclusive lock on the Stratum 0
repository for the full duration of tar extraction, compression, hashing, and CAS
upload — serialising work that is intrinsically parallel. After the catalog is
committed, Stratum 1 replicas must fetch all new objects from scratch, causing a
lot of cache misses on worker nodes.

## What this does

`cvmfs-prepub` accepts a packaged tar file and:

1. **Unpacks and processes** files in parallel — compress, SHA-256 hash, deduplicate against the existing CAS — without touching the overlay filesystem or holding any lock.
2. **Uploads objects** to the CAS backend (S3 or local filesystem) before acquiring a gateway lease.
3. **Pre-warms Stratum 1 replicas** with the new objects before the catalog is committed (Option B), so replication becomes catalog-only after the flip.
4. **Merges catalogs directly** by fetching the current CVMFS catalog from Stratum 0, applying the new entries to the correct sub-catalog SQLite database, finalising (compress + SHA-256), and committing via the `cvmfs_gateway` lease API. No overlay filesystem is required; catalog merging is done entirely in Go using the CVMFS schema 2.5 SQLite format.
5. **Supports private share directories** — files can be published under a hidden, randomly-named path (analogous to a Google Docs share link) that is invisible in `readdir()` but accessible by direct path, using the CVMFS `kFlagHidden` catalog flag.
6. **Recovers automatically** from crashes at any stage — every state transition is an atomic filesystem rename backed by a WAL journal.
7. **Supports release tagging** — an optional `tag_name` / `tag_description` can be set on any job; the tag is embedded in the catalog commit so the published snapshot is browsable by name via `cvmfs_server tag`.

The existing `cvmfs_server publish` workflow continues to work in parallel; the
gateway lease enforces mutual exclusion at the path level.

## Deployment options

| | Local mode | Option A | Option B (HTTP) | Option B (MQTT) |
|---|---|---|---|---|
| **Pre-processes tar** | ✓ | ✓ | ✓ | ✓ |
| **Bypasses overlay FS** | ✗ | ✓ | ✓ | ✓ |
| **Pre-warms Stratum 1** | ✗ | ✗ | ✓ | ✓ |
| **Requires cvmfs_gateway** | ✗ | ✓ | ✓ | ✓ |
| **Inbound firewall rules at S1** | None | None | TCP 9100 (data push) | TCP 9100 (data push); outbound TCP 8883 to broker (control) |
| **New infrastructure** | None | None | Receiver agent on each S1 | Receiver agent + MQTT broker |
| **Phase** | 1 | 1 | 2 | 2 |

**Local mode** (`--publish-mode local`) is the simplest deployment: it drives
`cvmfs_server publish` directly on the Stratum 0 node without a gateway.
File processing still happens in parallel before the lock is acquired, but the
overlay filesystem is used for the commit step.  Use this to get started without
a `cvmfs_gateway` installation.

The MQTT variant of Option B shifts the **control plane** (announce/ready
exchange) onto a shared broker — receivers connect outbound to the broker (TCP
8883) so Stratum 1 sites need not be reachable from Stratum 0 for signalling.
The **data plane** is identical in both variants: after the ready exchange the
publisher connects directly to each receiver's HTTP endpoint (TCP 9100 inbound
on each S1) to push CAS objects.  MQTT therefore helps when S1 sites cannot
accept arbitrary inbound connections from S0, but each receiver must still
accept the data push from S0 on port 9100.

See [REFERENCE.md §5](REFERENCE.md#5-option-a--inline-pre-processor),
[§6](REFERENCE.md#6-option-b--distributed-pre-processor-with-stratum-1-pre-warming),
and [§20.11](REFERENCE.md#2011-mqtt-control-plane-optional) for full topology
diagrams, trade-off analysis, and MQTT topic schema.

## Documentation

| Document | Contents |
|---|---|
| **[REFERENCE.md](REFERENCE.md)** | Full architecture, subsystem design, Go package layout, configuration reference, security considerations, deployment roadmap, comparison with the traditional workflow, provenance/transparency log, and REST API reference |
| **[INSTALL.md](INSTALL.md)** | Build instructions, configuration, systemd setup, and smoke-test procedure |

Key sections in REFERENCE.md:

- [§4 System Overview](REFERENCE.md#4-system-overview) — topology diagram and gateway API summary
- [§7.1 Job State Machine](REFERENCE.md#71-job-state-machine-and-spool-directory-model) — spool directory layout and crash-recovery model
- [§7.2 Processing Pipeline](REFERENCE.md#72-processing-pipeline) — fan-out channel graph and single-pass compress+hash
- [§8 Lifecycle Cleanup](REFERENCE.md#8-lifecycle-cleanup-gc-subsystem) — per-repository TTL-based GC with proxy-agnostic access tracking
- [§10 Configuration Reference](REFERENCE.md#10-configuration-reference) — annotated YAML config
- [§12 Deployment Roadmap](REFERENCE.md#12-deployment-roadmap) — phased rollout with exit criteria per phase
- [§16 Security, Confidentiality, Integrity, and Traceability](REFERENCE.md#16-security-confidentiality-integrity-and-traceability) — audit trail, supply-chain fields, tamper-evident publishing
- [§16.7 Stratum 1 Distribution Security](REFERENCE.md#167-stratum-1-distribution-security) — SSRF guard, MQTT mTLS, topic ACLs, input bounds
- [§17 Comparison with Traditional `cvmfs_server publish`](REFERENCE.md#17-comparison-with-traditional-cvmfs_server-publish) — head-to-head table, where the fundamental difference lies, and when to use each approach
- [§18 Provenance and Transparency Log](REFERENCE.md#18-provenance-and-transparency-log) — four-layer attribution chain, Rekor integration, CI OIDC token validation, and offline verification workflow
- [§20.11 MQTT Control Plane](REFERENCE.md#2011-mqtt-control-plane-optional) — topic schema, flow, presence/LWT, security controls, configuration flags
- [§38 REST API Reference](REFERENCE.md#38-cvmfs-prepub-rest-api-reference) — full endpoint specification for the cvmfs-prepub HTTP API

## Repository layout

```
cvmfs-bits/
├── cmd/
│   ├── prepub/          # Main service binary; probe.go holds startup readiness checks
│   └── prepubctl/       # Admin CLI (drain, abort, status)
├── internal/
│   ├── api/             # REST server (server.go) and Orchestrator (orchestrator.go)
│   ├── job/             # Job struct, FSM state definitions, ValidateTagName
│   ├── spool/           # Atomic spool directory manager + WAL journal
│   ├── cas/             # CAS backend (local FS; S3 in roadmap)
│   ├── lease/           # cvmfs_gateway lease client (lease.go), HMAC auth (auth.go),
│   │                    #   payload commit (payload.go), lease.Backend interface
│   ├── pipeline/        # Processing stages (unpack, compress, dedup, upload, catalog)
│   ├── distribute/      # Stratum 1 pre-warmer (distributor.go); per-object and batch
│   │                    #   push logic in push.go; quorum gating
│   ├── gc/              # Lifecycle GC scheduler (Phase 3)
│   └── access/          # Pluggable access-event tracking (Phase 3)
├── pkg/
│   ├── observe/         # OTel tracing + Prometheus metrics + slog logger
│   ├── cvmfshash/       # CVMFS content hash format utilities
│   └── cvmfscatalog/    # CVMFS catalog: schema 2.5 SQLite, MD5 path encoding, merge, secret dirs
├── testutil/
│   ├── fakegateway/     # In-process cvmfs_gateway with chaos controls
│   ├── fakecas/         # In-memory CAS with latency/failure injection
│   ├── fakestratum1/    # In-process Stratum 1 receiver with partition simulation
│   ├── observe/         # In-memory OTel span recorder for test assertions
│   └── simulate/        # Cluster simulator wiring all fakes; integration tests
├── docs/                # Architecture SVG diagrams
├── REFERENCE.md         # Full design and implementation reference
├── INSTALL.md           # Build, configuration, and deployment guide
├── go.mod
└── Makefile
```

## Quick start

```sh
# Build
make build

# Run the cluster integration test (simulates a full publish in-process)
make run-sim

# Start the service (Option A, local CAS, with cvmfs_gateway)
./cvmfs-prepub \
  --gateway-url http://localhost:4929 \
  --cas-type localfs \
  --cas-root /srv/cvmfs/cas \
  --spool-root /var/spool/cvmfs-prepub \
  --listen :8080

# Start the service (local mode — no gateway required)
./cvmfs-prepub \
  --publish-mode local \
  --cvmfs-mount /cvmfs \
  --cas-type localfs \
  --cas-root /srv/cvmfs/cas \
  --spool-root /var/spool/cvmfs-prepub \
  --listen :8080
```

See [INSTALL.md](INSTALL.md) for full deployment instructions.

## Monitoring

Every significant operation emits an OpenTelemetry span. Prometheus metrics are
exposed at `/api/v1/metrics`. Structured JSON logs use `log/slog` throughout.

The `testutil/simulate` package runs the full publish pipeline in-process with
fake infrastructure components, each emitting their own spans, making distributed
traces observable in a single `go test` run without any external services.

## Requirements

- Go 1.22+
- `cvmfs_gateway` ≥ 1.2 (for the lease-and-payload API; not required in local mode)
- Write access to the CAS backend (local filesystem or S3-compatible)
- HTTP read access to the Stratum 0 CAS (required for manifest fetch and catalog download during merge)
- TCP 9100 inbound on each Stratum 1 for the CAS object data push (Option B only — both HTTP and MQTT variants)
- TCP 8883 outbound from each Stratum 1 to the MQTT broker, plus TCP 8883 inbound on the broker host (Option B MQTT variant only)
- No `cvmfs` client tools required on the pre-publisher node — catalog merging is done natively in Go
