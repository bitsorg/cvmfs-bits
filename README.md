# cvmfs-prepub

A fast, resiliant Go service for pre-processing and publishing software releases
into [CVMFS](https://cernvm.cern.ch/fs/) without holding the repository transaction
lock during file processing.

## The problem

The standard CVMFS publishing workflow acquires an exclusive lock on the Stratum 0
repository for the full duration of tar extraction, compression, hashing, and CAS
upload — serialising work that is intrinsically parallel. After the catalog is
committed, Stratum 1 replicas must fetch all new objects from scratch, causing a
lot of cache misses worker nodes.

## What this does

`cvmfs-prepub` accepts a packaged tar file and:

1. **Unpacks and processes** files in parallel — compress, SHA-256 hash, deduplicate against the existing CAS — without touching the overlay filesystem or holding any lock.
2. **Uploads objects** to the CAS backend (S3 or local filesystem) before acquiring a gateway lease.
3. **Pre-warms Stratum 1 replicas** with the new objects before the catalog is committed (Option B), so replication becomes catalog-only after the flip.
4. **Commits atomically** via the `cvmfs_gateway` lease-and-payload API, which handles manifest signing and catalog merging.
5. **Recovers automatically** from crashes at any stage — every state transition is an atomic filesystem rename backed by a WAL journal.

The existing `cvmfs_server publish` workflow continues to work in parallel; the
gateway lease enforces mutual exclusion at the path level.

## Deployment options

| | Option A | Option B (HTTP) | Option B (MQTT) |
|---|---|---|---|
| **Pre-processes tar** | ✓ | ✓ | ✓ |
| **Bypasses overlay FS** | ✓ | ✓ | ✓ |
| **Pre-warms Stratum 1** | ✗ | ✓ | ✓ |
| **Inbound firewall rules at S1** | None | TCP 9100 (data push) | TCP 9100 (data push); outbound TCP 8883 to broker (control) |
| **New infrastructure** | None | Receiver agent on each S1 | Receiver agent + MQTT broker |
| **Phase** | 1 | 2 | 2 |

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
| **[REFERENCE.md](REFERENCE.md)** | Full architecture, subsystem design, Go package layout, configuration reference, security considerations, deployment roadmap, comparison with the traditional workflow, and provenance/transparency log |
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

## Repository layout

```
cvmfs-bits/
├── cmd/
│   ├── prepub/          # Main service binary
│   └── prepubctl/       # Admin CLI (drain, abort, status)
├── internal/
│   ├── api/             # REST server and Orchestrator
│   ├── job/             # Job struct and FSM
│   ├── spool/           # Atomic spool directory manager + WAL journal
│   ├── cas/             # CAS backend (local FS; S3 in roadmap)
│   ├── lease/           # cvmfs_gateway lease client with heartbeat
│   ├── pipeline/        # Processing stages (unpack, compress, dedup, upload, catalog)
│   ├── distribute/      # Stratum 1 pre-warmer with quorum gating
│   ├── gc/              # Lifecycle GC scheduler (Phase 3)
│   └── access/          # Pluggable access-event tracking (Phase 3)
├── pkg/
│   ├── observe/         # OTel tracing + Prometheus metrics + slog logger
│   └── cvmfshash/       # CVMFS content hash format utilities
├── testutil/
│   ├── fakegateway/     # In-process cvmfs_gateway with chaos controls
│   ├── fakecas/         # In-memory CAS with latency/failure injection
│   ├── fakestratum1/    # In-process Stratum 1 receiver with partition simulation
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

# Start the service (Option A, local CAS)
./cvmfs-prepub \
  --gateway-url http://localhost:4929 \
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
- `cvmfs_gateway` ≥ 1.2 (for the lease-and-payload API)
- Write access to the CAS backend (local filesystem or S3-compatible)
- TCP 9100 inbound on each Stratum 1 for the CAS object data push (Option B only — both HTTP and MQTT variants)
- TCP 8883 outbound from each Stratum 1 to the MQTT broker, plus TCP 8883 inbound on the broker host (Option B MQTT variant only)
