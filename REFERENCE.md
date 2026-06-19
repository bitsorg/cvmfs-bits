# CVMFS Pre-Publisher — Complete Reference

A Go service (`cvmfs-prepub`, the publishing service used by **bits**) for
pre-processing, queuing, and publishing software releases into CVMFS,
complementing the existing overlay-based publishing workflow.

> **What this system is.** The service publishes a tar archive into a CVMFS
> repository through `cvmfs_gateway`, and distributes the resulting objects to
> Stratum 1 receivers using a **pull-based data path coordinated over an embedded
> MQTT-over-WebSocket/TLS control plane** with token authentication, role ACLs,
> and Ed25519-signed discovery. The pull control plane is described in
> [Chapter 8](#8-pull-distribution-and-the-control-plane) and specified in
> [Chapter 31](#31-pull-distribution-protocol).
>
> Earlier prototypes of a push data plane, an SSE control plane, an external
> mosquitto broker, and a separate coordination service have been removed from
> the current implementation; any residual code lives on the `legacy` git branch.

---

## Table of Contents

### Part I — Introduction and Context
1. Background and Motivation
2. Current Publishing Flow and Its Constraints
3. Design Objectives
4. Comparison with `cvmfs_server publish`
5. WLCG and bits-console Context

### Part II — Architecture
6. System Overview
7. Pre-Processor Architecture with Stratum 1 Pre-Warming
8. Pull Distribution and the Control Plane
9. Core Subsystems
10. Lifecycle Cleanup (GC) Subsystem
11. Multi-Build-Node Topology
12. Go Package Structure
13. Provenance Architecture
14. Security Architecture Overview

### Part III — User Guide
15. Deployment Summary
16. bits Integration — Data Flow and Deduplication
17. bits-console Pipeline Integration
18. Coexistence with `cvmfs_server publish`
19. Monitoring and Observability
20. Backward Compatibility and Fallback

### Part IV — Cookbook
21. Recipe 1: Deploy a Single Stratum 0 Node
22. Recipe 2: Add Stratum 1 Pre-Warming (Pull)
23. Recipe 3: bits-console GitLab CI Integration
24. Recipe 4: Multi-Build-Node Deployment
25. Recipe 5: Provenance and Transparency Log Setup
26. Recipe 6: Access Control and Build Authorisation
27. Recipe 7: Receiver Enrollment and Revocation
28. Infrastructure Requirements Checklist

### Part V — Reference
29. Configuration Reference — Publisher
30. Configuration Reference — Receiver
31. Pull Distribution Protocol
32. Security Reference
33. Provenance Reference and Verification
34. Infrastructure Requirements Detail

### Part VI — REST API Reference
35. cvmfs-prepub REST API Reference

### Part VII — Roadmap
36. Phased Deployment Plan
37. Open Questions and Future Work

---

# PART I — INTRODUCTION AND CONTEXT

> **Who should read this part:** Everyone new to cvmfs-prepub, and anyone who wants
> to understand the motivation before diving into architecture or configuration.
> This part explains what problem the software solves, in what WLCG and CVMFS
> context it operates, and how it compares to the traditional publishing workflow.

---

## 1. Background and Motivation

CVMFS (CernVM File System) is a read-only, HTTP-distributed filesystem optimised
for software distribution in high-energy physics and scientific computing. Its
distribution hierarchy is:

```
Stratum 0 (authoritative publisher)
    └── Stratum 1 replicas  (N sites)
            └── Squid / proxy caches
                    └── O(10,000) worker nodes
```

Software is published by acquiring an exclusive transaction lock on the Stratum 0 repository, creating an overlay filesystem, installing software into the
`/cvmfs/<repo>` namespace, closing the transaction, then processing all changed files: compression, content hashing, deduplication against the existing
content-addressable store (CAS), upload to backend storage, catalog update, and manifest signing. Only after the manifest is published can Stratum 1 servers
replicate the update.

This process is correct but has two fundamental inefficiencies:

- **The transaction lock is held for the full duration of file processing.** Any parallel publishing attempt must wait. For large software releases (tens of GB)
  this serialises work that is intrinsically parallelisable.

- **Stratum 1 replication begins only after the catalog is committed.** Clients on sites whose Stratum 1 has not yet replicated the new catalog experience a thundering-herd of cache misses against slow inter-site links.

This document describes a complementary publishing service that addresses both problems without replacing or breaking the existing workflow.

---


## 2. Current Publishing Flow and Its Constraints

```
acquire lock
  → cvmfs_server transaction
  → mount overlay fs
  → install software to /cvmfs/<repo>/...        ← often minutes, lock held
  → cvmfs_server publish
      → diff overlay vs lower
      → for each changed file:
          compress (zlib)
          content hash
          dedup check against CAS
          upload to CAS backend
          add row to SQLite catalog
      → write new root catalog
      → sign manifest (.cvmfspublished)
      → release lock
  → Stratum 1 can now snapshot
```

Constraints:

| Constraint | Impact |
|---|---|
| Lock held during file I/O | Serialises concurrent releases |
| Overlay filesystem required | Requires publisher node privileges |
| CAS upload inside transaction | Network latency adds to lock time |
| Stratum 1 pull-on-demand | Cold-start latency after catalog flip |
| No partial namespace reservation | Cannot pipeline processing of a sub-tree |

---


## 3. Design Objectives

- **Non-disruptive:** Runs alongside existing infrastructure. Does not replace
  `cvmfs_server publish` and does not require changes to Stratum 1, proxies, or
  clients.
- **Fast path from tar to published:** Start from a packaged tar file; produce a
  committed catalog update with pre-warmed Stratum 1s.
- **Crash-safe:** Every state transition is durable. A process restart at any
  point resumes from the last committed spool state with no data loss and no
  double-deletion.
- **Idempotent operations:** CAS uploads, catalog submissions, and distribution
  pushes can be safely retried.
- **Per-repository lifecycle cleanup:** Repositories may be configured to
  automatically expire and remove content that has not been accessed within a
  configurable time window.
- **No proxy-technology dependency:** Access tracking must not be coupled to any
  specific proxy or caching technology.

---


---

## 4. Comparison with `cvmfs_server publish`

`cvmfs-prepub` is designed to run alongside the traditional workflow, not as a
replacement. The table below lists **architectural** differences that follow
directly from the design and are checkable against the code and behaviour.
Performance figures (throughput, latency, "faster"/"more scalable" claims) are
deliberately omitted: the two paths have not been benchmarked head-to-head in a
controlled setting, so any such number here would be unverified.

### 4.1 Architectural differences

| Dimension | Traditional `cvmfs_server publish` | `cvmfs-prepub` service |
|---|---|---|
| **Lock scope** | Repository transaction held from file I/O through manifest sign | Gateway lease acquired only for the commit (`SubmitPayload` + `Release`); pipeline runs before the lease |
| **When processing runs** | Inside the transaction lock — compress, hash, upload all precede commit while the lock is held | Before the lease is acquired — decoupled from the lock |
| **Entry point** | Local access to the Stratum 0 node; install files via overlay mount | HTTP POST of a tar archive plus an API token |
| **Input format** | Arbitrary file tree via overlay mount | Tar archive stream |
| **Worker parallelism** | As implemented by `cvmfs_server publish` | Configurable compress/upload worker pool (`--pipeline-upload-conc`, pipeline worker count) |
| **Dedup mechanism** | CAS existence check per file inside the transaction | Direct `CAS.Exists()` per object — `os.Stat` (local FS) or `HEAD` (S3), via `cas.NativeExistsChecker` |
| **Cross-node dedup** | No built-in mechanism across separate publisher nodes | Nodes sharing one content-addressed CAS see each other's objects through `Exists()` |
| **Stratum 1 pre-warming** | Not built in; S1 replicates after the catalog flip | Optional: receivers pull objects before the catalog flip (Chapter 8) |
| **State durability** | Overlay fs is live; an interrupted publish leaves an uncommitted overlay | Each state transition is written to a crash-safe spool with a WAL journal; restarts resume |
| **Idempotency** | Not guaranteed on re-run after failure | CAS `Put` and gateway `Submit` are idempotent; retries are safe |
| **Job status** | Exit code + log file | REST API with per-job FSM state; optional SSE event stream and webhook |
| **Metrics / tracing** | Not built in | OpenTelemetry spans + Prometheus metrics per pipeline stage |
| **Gateway changes** | Is the gateway workflow | None — uses the existing lease-and-payload API as a client |
| **S1 / client changes** | None | None to the CVMFS client; receivers run the `--mode receiver` binary for pull pre-warming |
| **Coexistence** | — | Runs alongside the traditional workflow; the gateway's per-path lease arbitrates |
| **Publisher identity** | Operator/account on the Stratum 0 node, recorded in server-side logs | Structured fields (`actor`, `git_sha`, `pipeline_id`) in every job manifest |
| **Identity verification** | No built-in cryptographic check | Optional OIDC token validated against the CI provider's JWKS; `verified=true` is set only on a validated token |
| **Transparency log** | None | Optional Rekor (`hashedrekord`) submission after publish |

The differences above are structural consequences of the design; they do not by
themselves establish that one path is faster than the other for any given
workload.

### 4.2 Where the structural difference lies

In the traditional workflow the exclusive transaction is held while files are
compressed, hashed, dedup-checked, and uploaded. `cvmfs-prepub` reorders this:
the compress → hash → dedup → CAS-upload pipeline runs **before** a gateway
lease is requested, so by the time the lease is acquired every object is already
in the CAS. The lease window therefore covers only `SubmitPayload` and
`Release`. The same reordering enables Stratum 1 pre-warming: because receivers
can pull objects before the catalog flip (Chapter 8), the objects are already
present locally when clients see the new revision.

### 4.3 What the prepub service does not change

The gateway's manifest signing, catalog merging, and revision-number increment
are untouched — the prepub service is a first-class client of the existing
gateway lease-and-payload API, not a bypass of it. Stratum 1 replication
protocol, proxy behaviour, and the CVMFS client are unchanged. The
overlay-filesystem workflow continues to work in parallel; the gateway's
per-path lease enforces mutual exclusion between the two paths at the sub-path
level.

### 4.4 Hard-link handling — a concrete behavioural difference

The overlay-filesystem approach handles hard links through the Linux VFS: two
directory entries with the same inode are indistinguishable from any other files
in the tree. A tar-stream pipeline must handle them explicitly.

In the current implementation the unpack stage maintains a `seenFiles` map keyed
by cleaned entry path. When a `TypeLink` entry is encountered its data is
resolved from the map and the entry is emitted as a regular `FileEntry` with the
same byte content. Both the original path and the hard-linked path then appear in
the catalog with identical content hashes — the dedup check recognises the hash
from the earlier upload and skips the second CAS write. Forward hard-link
references (the link appearing before its target in the stream) are rejected with
a clear error, consistent with GNU tar's ordering guarantee.

### 4.5 When to use each approach

| Scenario | Recommended path |
|---|---|
| Ad-hoc manual publish by an operator on the Stratum 0 node | Traditional `cvmfs_server publish` |
| CI/CD pipeline publishing a build artifact from an external build node | `cvmfs-prepub` |
| Multiple teams publishing to different sub-paths in parallel | `cvmfs-prepub` |
| Rollout where Stratum 1 pre-warming before the catalog flip is wanted | `cvmfs-prepub` (Chapter 8) |
| Repository with sparse, infrequent, small updates | Either; traditional is simpler |
| Environment requiring a structured, externally verifiable audit trail | `cvmfs-prepub` with `--provenance` (Chapter 33) |
| Air-gapped or self-hosted transparency log required | `cvmfs-prepub` with `--rekor-server <internal-url>` |

---


---

## 5. WLCG and bits-console Context

This section describes how cvmfs-prepub fits into the broader WLCG software
distribution pipeline, specifically the bits-console build-to-edge workflow.
It covers what bits produces, how the CVMFS namespace is partitioned, and the
submission protocol between a bits CI job and cvmfs-prepub.  Data flow and
deduplication are covered in the User Guide (Chapter 16).

### 5.1 What `bits` Produces

`bits` is a build tool that cross-compiles or packages software and emits a
**self-describing tar archive** whose directory tree is rooted at the target
CVMFS namespace path.  For example, a build of the ATLAS offline software might
emit:

```
atlas/24.0.30/x86_64-el9-gcc13-opt/
    bin/
    lib/
    python/
    ...
```

The tar contains only the new or changed files for this software version; files
that are identical to a previously published version are detected at the
`bits`-side by comparing checksums against a manifest snapshot and are excluded
from the archive to keep transfer size small.

### 5.2 Path Configuration for the CVMFS Namespace

`bits` must be configured with the CVMFS repository name and the sub-path that
corresponds to the package group so that the tar root aligns with the gateway
lease path:

```toml
[publish]
repo       = "software.cern.ch"
path       = "atlas/24.0"          # gateway lease will be acquired on this sub-path
prepub_url = "https://prepub.example.org:8080"
```

The `path` field in the config becomes the `path` field of the
`POST /api/v1/jobs` request.  The gateway will issue a lease scoped to this
sub-path, allowing other build nodes to publish to different sub-paths (e.g.
`cms/`, `lhcb/`) concurrently without conflicting leases.

### 5.3 Submission Protocol

Once the tar is ready, `bits` (or the CI harness that invokes it) submits the
archive to `cvmfs-prepub` via a single HTTP call:

```
POST /api/v1/jobs
Authorization: Bearer <PREPUB_API_TOKEN>
Content-Type: multipart/form-data

  repo=software.cern.ch
  path=atlas/24.0
  tar=<binary stream or file reference in the spool staging area>
```

If the tar has already been staged to a shared spool directory (the fast path
for build nodes co-located with the prepub host), the submission body can
instead reference the staging path and the server reads directly from disk,
avoiding a network copy.

The server responds immediately with a `job_id`.  The caller can poll
`GET /api/v1/jobs/{id}` or wait for a webhook callback to learn when the
publish has completed.



# PART II — ARCHITECTURE

> **Who should read this part:** Developers, architects, and site administrators
> who want to understand how the system is built internally — job FSM, pipeline
> stages, distributor, receiver, GC, provenance chain, and security model.
> Operators deploying without customising the code can skim this part and jump
> straight to the Cookbook.

---

## 6. System Overview

The service is a single Go binary (`cvmfs-prepub`) with embedded subsystems. The
**Stratum 0 publisher** runs on a node that has write access to the CAS backend
(S3 credentials or a local CAS tree) and network access to the `cvmfs_gateway`
HTTP API. The same binary runs on each Stratum 1 in `--mode receiver`, where it
**pulls** objects from the publisher's content-addressed object store.

```
  tar file (HTTP upload / spool reference)
       │
       ▼
  ┌─────────────────────────────────────────────────┐
  │           cvmfs-prepub  (Stratum 0)              │
  │                                                  │
  │  REST API ──► Job Queue ──► Lease Manager        │
  │                                                  │
  │  Processing Pipeline:                            │
  │    Unpack ──► Compress+Hash ──► Dedup ──► Upload │
  │                                        │         │
  │                               Catalog Merge      │
  │                                        │         │
  │  Spool: incoming/leased/staging/       │         │
  │         uploading/distributing/        │         │
  │         committing/published/aborted   │         │
  │                                        ▼         │
  │                               cvmfs_gateway API  │
  │                                                  │
  │  Control plane:                                  │
  │    Embedded MQTT broker (wss)  ── announce ──┐   │
  │    Signed discovery (.cvmfsbits)             │   │
  │    Durable manifest store + object serving   │   │
  └──────────────────────────────────────────────┼──┘
       │                                          │
       ▼                              announce/published (wss)
  CAS Backend                                     │
  (S3 / local)                                    ▼
                                       Stratum 1 receivers
                                       (pull + verify objects,
                                        warm before catalog flip)
```

![CVMFS Pre-Publisher Architecture](docs/cvmfs_prepublisher_architecture.svg)

The `cvmfs_gateway` already exposes a lease-and-payload HTTP API:

| Endpoint | Purpose |
|---|---|
| `POST /api/v1/leases/{path}` | Reserve a sub-path for exclusive write |
| `PUT /api/v1/leases/{token}` | Heartbeat to renew lease TTL |
| `POST /api/v1/payloads` | Submit pre-processed catalog diff + objects |
| `DELETE /api/v1/leases/{token}` | Release lease (commit or abort) |

The pre-publisher targets this API directly, making it a first-class gateway client rather than a workaround. No gateway modifications are required.

---


## 7. Pre-Processor Architecture with Stratum 1 Pre-Warming

### 7.1 Description

A `cvmfs-prepub` publisher accepts tar uploads, runs the full processing
pipeline (unpack → compress + hash → dedup → CAS upload), merges the catalog,
and commits via the gateway lease API. To pre-warm Stratum 1 replicas it
publishes a per-transaction **announce** on its embedded control-plane broker
**before** the catalog is committed. Stratum 1 receivers learn of the
transaction, read a per-transaction manifest, and **pull** the objects they are
missing from the publisher's content-addressed object store, verifying each
object by hash. When the catalog flip occurs the objects are already present in
each receiver's local CAS, so the replication snapshot becomes catalog-only.

### 7.2 Topology

```
  pre-processor node (Stratum 0)
  ┌──────────────────────────────────────┐
  │  cvmfs-prepub                        │
  │    ├── REST API                      │
  │    ├── Worker pool                   │
  │    ├── CAS backend client            │──► S3 / local CAS (Stratum 0)
  │    ├── cvmfs_gateway client          │──► cvmfs_gateway
  │    ├── Embedded MQTT broker (wss)    │── announce/published ─┐
  │    ├── Signed discovery + manifests  │                       │
  │    └── Object serving (HTTP)         │◄── pull ──────────┐   │
  └──────────────────────────────────────┘                  │   │
         │                                                   │   │
         ▼  (catalog commit happens here)                    │   │
    cvmfs_gateway → Stratum 0 manifest signed                │   │
                                                             │   │
    Stratum 1 receivers ─────────────────────────────────────┘   │
      ◄── announce / published (wss) ────────────────────────────┘
      pull missing objects, verify by hash, warm before catalog flip
```

### 7.3 Stratum 1 Receiver (Pull)

Each Stratum 1 runs `cvmfs-prepub --mode receiver`. The receiver:

- subscribes to the publisher's control-plane broker over `wss://` and receives
  `announce` (pre-commit) and `published` (post-commit) messages for the
  repositories it follows;
- on each announce, fetches the per-transaction manifest and computes the subset
  of object hashes it does not already hold with a direct `CAS.Exists()` check
  (`os.Stat` for local FS, `HEAD` for S3) — there is no inventory filter;
- pulls the missing objects from the publisher's content-addressed object store
  over ordinary HTTP, verifying each object by hash on arrival;
- warms its local catalog (atomic flip) when it has the objects for the
  committed root.

The data path is therefore **pull** — receivers open only outbound connections
and need no inbound ports. See Chapter 8 for the control plane and Chapter 31 for
the full protocol.

### 7.4 Commit Coordination

The publisher can gate the catalog flip (or the completion of a transaction) on a
**warm quorum** of receivers reporting `ready`. A lagging receiver does not
block the release indefinitely; receivers that connect late still catch up
because the post-commit `published` message is retained on the broker. The exact
quorum/timeout behaviour is covered in Chapter 8 and Chapter 31.

---


## 8. Pull Distribution and the Control Plane

This chapter documents the distribution system as actually built: the bits
publish pipeline plus the **pull-based data path** coordinated over an embedded
**MQTT-over-WebSocket/TLS** control plane with token authentication, role ACLs,
and Ed25519-signed discovery. Chapter 31 is the field-level protocol reference;
this chapter is the architectural overview, key model, and security model.

### 8.1 Component Map

A build farm produces release artifacts and submits them to a single Stratum 0
publisher (`cvmfs-prepub`). The publisher runs the processing pipeline, commits
through `cvmfs_gateway`, and **coordinates** distribution: it announces each
transaction on an in-process broker and serves a signed manifest. Stratum 1
receivers **pull** the objects they are missing from content-addressed storage
(the CVMFS web tier / CDN), verify each by hash, and warm before the catalog flip.

```mermaid
flowchart LR
  subgraph Build["Build farm — O(10) platforms (elastic, stateless)"]
    B1[bits builder]
    B2[bits builder]
  end
  subgraph S0["Stratum 0 — cvmfs-prepub (stateful core)"]
    API["REST API :8080"]
    PIPE["Pipeline<br/>unpack -> dedup -> compress -> CAS"]
    COMMIT["Commit (gateway lease)"]
    MSTORE["Durable manifest store (disk)"]
    BROKER["Embedded broker wss :1882<br/>(token auth + role ACL)"]
    ENROLL["TLS control :8443<br/>challenge / enroll / revoke"]
    DISCO["Signed discovery<br/>GET /cvmfs/{repo}/.cvmfsbits"]
  end
  GW["cvmfs_gateway<br/>(commit authority, per-repo)"]
  subgraph S1["Stratum 1 receivers (elastic, no inbound ports)"]
    R1[receiver]
    R2[receiver]
  end
  CDN["Object serving<br/>CVMFS web / CDN (content-addressed)"]
  B1 --> API
  B2 --> API
  API --> PIPE --> COMMIT --> GW
  PIPE --> MSTORE
  COMMIT -->|announce / published| BROKER
  R1 -->|1 fetch + verify discovery| DISCO
  R2 -->|1 fetch + verify discovery| DISCO
  R1 -->|2 enroll over TLS| ENROLL
  R2 -->|2 enroll over TLS| ENROLL
  R1 -->|3 subscribe wss + token| BROKER
  R2 -->|3 subscribe wss + token| BROKER
  R1 -->|4 GET manifest| MSTORE
  R2 -->|4 GET manifest| MSTORE
  R1 -->|5 pull + verify objects| CDN
  R2 -->|5 pull + verify objects| CDN
```

The two planes are separated on purpose:

- **Control plane** (small, latency-sensitive): discovery, enrollment, the broker
  `announce` / `published` / `ready` / `presence` messages, and revocation.
- **Data plane** (large, throughput-sensitive): content-addressed objects, served
  as ordinary HTTP static content and therefore freely replicable / CDN-able.

### 8.2 Roles, Keys, and Trust Boundaries

The Stratum 0 publisher is the only trusted minting authority. Receivers are
semi-trusted: each can act only as itself. Keys are distributed so that
compromising a receiver cannot escalate to control of the plane.

| Secret / key | Held by | Purpose | If a receiver is compromised |
|---|---|---|---|
| `PREPUB_HMAC_SECRET` (master) | **S0 only** | mint/verify tokens; derive per-node keys | not exposed — receiver never has it |
| per-node key `HMAC(master, node)` | that one receiver | prove identity during enrollment | attacker can enrol only as that node |
| Ed25519 discovery **private** key | **S0 only** | sign the discovery document | not exposed |
| Ed25519 discovery **public** key | all receivers | verify discovery | cannot forge a discovery doc |
| broker server cert + key | **S0 only** | terminate `wss` / HTTPS | not exposed |
| CA certificate | S0 + receivers | verify broker + enroll endpoint | public material |
| scoped bearer token (TTL ~10m) | minted per node on demand | broker password; admin = `publisher` node | only its own receiver-scoped token |

```mermaid
flowchart TB
  subgraph Trusted["Trusted — Stratum 0"]
    M["master secret<br/>(token mint, key derivation)"]
    DK["Ed25519 private (sign discovery)"]
    SK["broker server key (wss/TLS)"]
  end
  subgraph SemiTrusted["Semi-trusted — each Stratum 1"]
    NK["per-node key (enrol as self only)"]
    DP["Ed25519 public (verify only)"]
    CA["CA cert (verify S0 only)"]
  end
  M -. "derives, never sent" .-> NK
  DK -. "public half" .-> DP
  SK -. "CA only" .-> CA
```

Receivers are provisioned only with the per-node key and the discovery *public*
key; they never hold the master secret. A single compromised receiver therefore
cannot mint a `publisher` token, forge commit notifications, revoke peers, or
impersonate another node.

### 8.3 Publish, Coordinate, Pull, Warm

The per-transaction manifest is **provisional** (its root hash is a placeholder
until commit); the objects are content-addressed, so receivers can pre-pull
before the catalog flips. The post-commit `published` message is **retained**, so
a receiver that connects late still catches up.

```mermaid
sequenceDiagram
  autonumber
  participant Bld as bits builder
  participant API as S0 API
  participant P as Pipeline
  participant M as Durable manifest store
  participant B as Broker (wss)
  participant GW as cvmfs_gateway
  participant R as Receiver
  participant O as Object serving (CDN)
  Bld->>API: POST /api/v1/jobs (tar)
  API->>P: unpack -> dedup -> compress -> upload to CAS
  P->>M: Put provisional manifest (durable, key = txn)
  P->>B: announce(repo, txn, objectHashes)   [pre-warm]
  B-->>R: announce
  R->>M: GET /s1/{txn}/manifest
  R->>O: pull missing objects (verify each hash)
  API->>GW: acquire lease -> merge catalog -> commit
  API->>B: published(repo, txn, rootHash)   [retained]
  B-->>R: published
  R->>O: pull any remaining objects
  R->>R: warm (atomic local catalog flip)
  R-->>B: ready / presence
  API->>API: quorum of ready reached -> transaction done
  API->>M: Delete manifest (warm quorum reached)
```

### 8.4 Security Model in Detail

- **Transport.** The broker listens on `wss://` (TLS); enrollment and revocation
  are on a dedicated HTTPS listener that reuses the broker's server certificate.
  Both sides verify the server certificate against the provisioned CA. The token
  is therefore never exposed on the wire (plain-HTTP enrollment is not served;
  `GET :8080/control/challenge` is 404).
- **Authentication.** Per-node challenge/response proves possession of the
  per-node key without transmitting it; the server issues a self-verifying,
  scoped, TTL-bounded HMAC token (`Minter`/`Verifier`). The token is presented as
  the MQTT CONNECT password and re-supplied on every reconnect by a credentials
  provider, so short token lifetimes do not require manual rotation.
- **Authorization.** The auth hook records the token-verified node on the
  connection object itself and enforces a role ACL on every publish: the
  publisher may write any control topic; a receiver may write only its own
  `ready`/`presence` and may not forge `announce`/`published` (which could push
  the publisher to a premature commit).
- **Discovery integrity (Ed25519).** The publisher signs the discovery document
  with its Ed25519 private key; receivers verify with the public key only
  (`--discovery-signing-key` on the publisher, `--discovery-verify-key` on the
  receiver). A symmetric signer remains only as a dev fallback when no Ed25519
  key is configured; production uses Ed25519.
- **DoS resistance (no firewall assumed).** The enrollment challenge is
  **stateless** (`nonce = ts || HMAC(serverKey, ts||node)`), so flooding
  `/control/challenge` costs an HMAC and zero memory; the redeemed-nonce replay
  set is bounded; a per-IP token-bucket rate limiter with a global ceiling guards
  the control endpoints (honouring `X-Forwarded-For` only from configured trusted
  proxies); HTTP read/idle timeouts and a connection cap bound slow-client
  attacks.
- **Revocation.** A shared denylist (consulted by both the enroll key store and
  the broker auth hook) plus an active disconnect gives immediate cut-off; by
  attrition, access also lapses within one token TTL.
- **Durability.** Per-transaction manifests are written to disk and reloaded on
  startup, so a publisher restart does not strand in-flight transactions or leave
  the durable distribution queue referencing manifests that have vanished.

### 8.5 Notes on Horizontal Scalability

For an expected load of O(10) build platforms per release and O(100)
repositories, the message rate on the broker is modest; the pressure is on
Stratum 0 pipeline throughput and on the durability of in-flight coordination
state. Components scale as follows:

- **Elastic / horizontal:** the build farm, the Stratum 1 receiver fleet, and
  object serving (content-addressed; served by the CVMFS web tier / CDN, off the
  publisher's critical path — advertise the CDN base in the manifest `base_urls`).
- **Single-writer per repository:** the commit authority and the `cvmfs_gateway`
  for a given repo — two committers for one repo is unsafe; commits to *different*
  repos parallelise.
- **Recommended scale-out:** **shard by repository** across K publisher instances,
  each owning a disjoint subset of repos and running its own embedded broker;
  discovery routes each receiver to the broker that owns the repo it follows. This
  keeps per-repo coordination local and removes any single global hub. State that
  must survive a restart / failover (the manifest store; optionally the revocation
  and rate-limit state) should be persisted or externalised per shard.

---


## 9. Core Subsystems

### 9.1 Job State Machine and Spool Directory Model

Every job is a directory under a spool root. State transitions are atomic POSIX
renames: the job directory moves from one spool subdirectory to the next. A
`fsync` of the destination directory and a WAL journal entry precede every rename,
making each transition crash-safe.

#### Spool Layout

```
/var/spool/cvmfs-prepub/
├── incoming/     <jobID>/  {manifest.json, pkg.tar}
├── leased/       <jobID>/  {manifest.json, lease.json}
├── staging/      <jobID>/  {manifest.json, objects/<hash>.Z, catalog.db}
├── uploading/    <jobID>/  {manifest.json, upload-log.jsonl}
├── distributing/ <jobID>/  {manifest.json, dist-log.jsonl}
├── committing/   <jobID>/  {manifest.json}
├── published/    <jobID>/  {manifest.json, receipt.json}
├── aborted/      <jobID>/  {manifest.json, abort-reason.json}
└── failed/       <jobID>/  {manifest.json, error.json}
```

#### State Machine

![Job State Machine — Spool Directory Lifecycle](docs/cvmfs_job_state_machine.svg)

```
incoming
  │  lease acquired from cvmfs_gateway
  ▼
leased
  │  tar extracted, files enumerated
  ▼
staging
  │  compress + hash + dedup + local write complete
  ▼
uploading
  │  all objects confirmed in CAS backend
  ▼
distributing          (announce published; receivers pull and warm)
  │  warm quorum of Stratum 1 receivers reached
  ▼
committing
  │  payload submitted and manifest signed by gateway
  ▼
published ◄──────────────────────────────────────────────────────── terminal

Any state → aborted    (lease released, uploaded objects GC-marked)
aborted   → failed     (if the abort itself fails; requires operator intervention)
```

#### Recovery on Restart

At startup the service scans all non-terminal spool directories and re-queues
each job at its current state:

| Found in | Recovery action |
|---|---|
| `incoming/` | Re-acquire lease and restart |
| `leased/` | Resume from unpack |
| `staging/` | Resume upload from the upload log |
| `uploading/` | Re-issue idempotent PUTs for unconfirmed objects |
| `distributing/` | Re-announce the transaction; receivers re-pull what they still miss |
| `committing/` | Query gateway for payload status; commit or re-submit |

#### WAL Journal

Each job carries a `journal.jsonl` in its spool directory. Every state transition
appends a record before the rename:

```jsonc
{"t":"2026-04-24T02:00:00Z","from":"uploading","to":"distributing","run":"abc123"}
{"t":"2026-04-24T02:01:00Z","op":"warm_ready","s1":"stratum1-cern.ch","n_objects":4821}
```

The journal is append-only and fsynced. It is the definitive record of what
happened to a job and is retained in `published/` or `aborted/` for audit.

### 9.2 Processing Pipeline

The pipeline runs as a bounded worker pool. The compress stage's worker count
defaults to `runtime.NumCPU()` and is clamped to a safe range
(`internal/pipeline/compress`). The pipeline is a directed graph of stages; each
stage communicates via Go channels with backpressure.

```
Unpacker
  │  chan FileEntry (path, []byte)
  ▼
Compress+Hash worker pool
  │  chan Result (path, casHash, compressedBytes [, chunks])
  ▼
Deduplicator (direct CAS.Exists() — os.Stat / S3 HEAD)
  │  only objects absent from the CAS proceed to upload
  ▼
CAS Uploader (idempotent PUT)
  │  confirmation written to upload-log.jsonl
  ▼
Catalog Accumulator
  └► catalog.db (all files, including deduped ones already in CAS)
```

**CAS key and compression.**  Each regular file is zlib-compressed and the
**CAS key is the SHA-1 of the compressed bytes** — the CVMFS CAS convention; the
C++ receiver's hash enum only recognises SHA-1, RIPEMD-160, and SHAKE-128, so
SHA-256 keys would be rejected (`pkg/cvmfshash/hash.go`,
`internal/pipeline/compress/compress.go`). Compression and hashing are done in a
single pass: the zlib output stream is fed to both an accumulation buffer and the
SHA-1 hasher via `io.MultiWriter`, with the `zlib.Writer` and `sha1.Hash`
instances pooled across files. The compression level is the zlib default
(level 6); `--pipeline-compress-level` overrides it (`0` = default/6, `1` =
fastest, `9` = best).

**File chunking (content-defined, CVMFS-compatible).**  Large files are split
into content-defined chunks using a port of the CVMFS xor32 rolling-checksum
cut-point algorithm (`internal/pipeline/chunker/xor32.go`): rolling window of 32
bytes, magic `0x7FFFFFFF` (UINT32_MAX/2), cut threshold `UINT32_MAX / avg`, and
min/avg/max chunk-size bounds. A file of size ≤ min is stored whole; a file that
yields a single piece collapses to a bulk object (matching CVMFS's sole-piece
behaviour). For a chunked file:

- each chunk is an independent CAS object keyed by `SHA-1(zlib(chunk))`, and its
  catalog hash carries the CVMFS `P` (partial) suffix;
- the file's catalog **bulk hash** is the SHA-1 of the **uncompressed** full
  file content (the CVMFS standard for chunked files), not the SHA-1 of any
  individual chunk.

Chunk sizes are controlled by `--chunk-min` / `--chunk-avg` / `--chunk-max`
(defaults 4 / 8 / 16 MiB) or the `chunking:` block in `config.yaml`; setting
`--chunk-avg 0` disables content-defined chunking.

**Deduplication.**  Each object is checked directly against the store with
`CAS.Exists()` — an `os.Stat` for the local filesystem backend or a single
`HEAD` request for S3 — via the `cas.NativeExistsChecker` interface. Objects that
already exist are recorded in the catalog but skipped by the uploader; absent
objects go to the uploader. There is no probabilistic inventory filter, no
startup CAS/catalog walk, and no in-memory inventory: the check is exact (no
false positives) and stateless.

### 9.3 Lease Management

The `cvmfs_gateway` lease has a configurable TTL (default 2 minutes). The lease
manager maintains a heartbeat goroutine per active lease:

```go
type LeaseManager struct {
    gatewayURL string
    client     *http.Client
}

func (lm *LeaseManager) Heartbeat(ctx context.Context, token string, ttl time.Duration) {
    ticker := time.NewTicker(ttl / 3)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            if err := lm.renew(ctx, token); err != nil {
                // lease lost; signal job FSM to abort
                lm.abort(token, err)
                return
            }
        }
    }
}
```

If the heartbeat fails, the job FSM transitions to `aborted`. Objects already
uploaded are GC-marked in a sidecar file; the CAS Reaper (described in §10.3)
cleans them up during the next GC pass once they are confirmed unreferenced.

### 9.4 Catalog Merge

Rather than building a standalone catalog and submitting it via the gateway
payload API, cvmfs-prepub performs a **direct SQLite catalog merge** using the
`pkg/cvmfscatalog` package.  The merge proceeds as follows:

1. **Fetch the current manifest** — `GET <stratum0_url>/<repo>/.cvmfspublished`
   to obtain the current root catalog hash (`C` field) and hash algorithm.
2. **Download the root catalog** — fetch `data/XY/<hash>[suffix]` from the
   Stratum 0 CAS and zlib-decompress it into a temporary SQLite file.
3. **Locate the target sub-catalog** — walk the `nested_catalogs` table to find
   the catalog whose root prefix covers the lease path.  If the lease path is
   covered by the root catalog, work directly on the root.
4. **Apply entries** — `Upsert` or `Remove` each `cvmfscatalog.Entry` into the
   sub-catalog, updating the `statistics` counters atomically.
5. **Finalise** — increment `revision`, set `last_modified`, VACUUM the SQLite
   file, zlib-compress it, hash the compressed bytes with the repository's hash
   algorithm (SHA-1 in the CVMFS CAS convention), and write it to
   `data/XY/<hash>C` in the CAS directory (suffix `C` = catalog).
6. **Walk parent chain** — update each parent catalog's `nested_catalogs` row
   with the new child hash; finalise each parent in turn up to the root.
7. **Return hashes** — `MergeResult.OldRootHash` (plain hex from the manifest)
   and `MergeResult.NewRootHashSuffixed` (hex plus any algorithm suffix) are
   passed to the gateway `CommitLease` call.

#### CVMFS catalog schema (version 2.5)

The `catalog` table has the following columns (key ones annotated):

| Column | Role |
|---|---|
| `md5path_1`, `md5path_2` | MD5 of the absolute path, split into two little-endian `int64`; root entry = `MD5("")` |
| `parent_1`, `parent_2` | MD5 of the parent directory path |
| `name` | Basename only (empty for the root entry) |
| `flags` | Bit-packed: `FlagDir=1`, `FlagDirNestedMount=2`, `FlagFile=4`, `FlagLink=8`, `FlagDirNestedRoot=32`, `FlagFileChunk=64`, `FlagHidden=0x8000`; hash algorithm encoded as `(algo-1) << 8` |
| `size` | File size in bytes; for symlinks, length of the link target |
| `mtime` | Unix timestamp (seconds) |
| `symlink` | Symlink target (non-empty only when `FlagLink` is set) |
| `hardlinks` | `(linkcount << 32) \| inode`; for regular files, `linkcount=1` and `inode=md5path_1 & 0x7fffffff` |

The `statistics` table tracks 14 counters (`num_files`, `num_symlinks`,
`num_dir`, `num_nested_catalogs`, `num_chunked_files`, `num_chunks`,
`file_size`, `chunked_file_size`, `num_special`, plus `num_x_*` variants for
external files) and is updated on every `Upsert`/`Remove`.

#### Secret / private directories

Files can optionally be published under a hidden path using a cryptographically
random share token, analogous to a Google Docs share link:

```
.shares/<64-hex-token>/<content-path>
```

Both `.shares/` and `.shares/<token>/` are inserted into the catalog with the
`kFlagHidden` (0x8000) flag, which causes CVMFS clients to skip them in
`readdir()` while still serving them on direct `open()` or `stat()` calls.  The
share token is 32 random bytes (256 bits) of entropy, making brute-force
enumeration infeasible.

The secret directory entries are created by `cvmfscatalog.SharesDirEntry()` and
`cvmfscatalog.TokenDirEntry(token, mtime)`.  A new token is generated by
`cvmfscatalog.NewToken()`.  The actual content entries are created under
`cvmfscatalog.SharePath(token, contentPath)` with normal (non-hidden) flags,
so that once a user knows the token, ordinary tools (`ls`, `cat`, `cvmfs_config
stat`) can access the files.

**Security note:** the obscurity is provided by the 256-bit token, not by access
control. The catalog SQLite file is publicly readable on any Stratum 0 or Stratum
1; anyone who can read the full catalog can enumerate share tokens.  This is the
same trade-off as Google Docs links — suitable for controlled-disclosure
scenarios (e.g. sharing a pre-release build with a collaborator) but not for
content that must be kept secret from site administrators.

### 9.5 CAS Backend Abstraction

All storage operations go through a minimal interface:

```go
type CASBackend interface {
    Exists(ctx context.Context, hash string) (bool, error)
    Put(ctx context.Context, hash string, r io.Reader, size int64) error
    Get(ctx context.Context, hash string) (io.ReadCloser, error)
    Delete(ctx context.Context, hash string) error
}
```

Implementations:

| Implementation | Notes |
|---|---|
| `S3Backend` | Multipart upload, ETag verification, AWS/GCS/MinIO compatible |
| `LocalFSBackend` | Direct write to CVMFS data directory structure |
| `MultiBackend` | Fan-out write to multiple backends; fan-in read (for migration) |

The `MultiBackend` is useful during a migration from local filesystem to S3: write
to both, serve reads from either, then decommission the local backend once S3 is
confirmed complete.

### 9.6 Distribution Coordinator

After all CAS objects are confirmed uploaded, the publisher announces the
transaction on the control-plane broker (Chapter 8) and tracks per-receiver
`ready` state in `dist-log.jsonl`:

```jsonc
{"s1":"stratum1-cern.ch",   "n_objects":4821, "ready":true,  "t":"..."}
{"s1":"stratum1-fnal.gov",  "n_objects":4821, "ready":false, "t":"..."}
```

Receivers pull the objects they are missing (they do not receive pushes). On
recovery the publisher re-announces and receivers re-pull whatever they still
lack. A receiver that connects late catches up from the retained `published`
message. If a warm quorum is not reached within the configured timeout, the job
proceeds rather than blocking indefinitely; lagging receivers warm later from the
retained state. See Chapter 31 for the protocol detail.

---


## 10. Lifecycle Cleanup (GC) Subsystem

![CVMFS GC and Lifecycle Cleanup Architecture](docs/cvmfs_gc_lifecycle_architecture.svg)

Certain repositories require automatic expiry and removal of content that has not
been accessed within a configurable window. This affects both the CVMFS namespace
(catalog entries) and backend CAS objects. The GC subsystem is opt-in per
repository and completely independent of the publishing pipeline.

### 10.1 Access Tracking

CVMFS clients do not report access back to the server and catalogs do not record
`atime`. Access information is reconstructed from the distribution layer.

#### AccessEvent and AccessEventSource

All access data flows through a single normalized type and a pluggable source
interface. There is no dependency on any specific proxy technology.

```go
type AccessEvent struct {
    Repo      string
    CASHash   string    // may be empty if only path is known
    Path      string    // may be empty if only hash is known
    Timestamp time.Time
    Source    string    // "stratum1", "cas-backend", "publish", "external"
}

type AccessEventSource interface {
    Name() string
    Events(ctx context.Context) (<-chan AccessEvent, <-chan error)
    Close() error
}
```

The `AccessTracker` fan-ins from all configured sources, deduplicates events
(same hash within a 1-minute window), and writes to a per-repository SQLite
database keyed by `(repo, cas_hash)`.

#### Built-in Sources

**Stratum1LogSource** — The primary default source. Tails the Stratum 1 HTTP
server access log. Format is defined by operator-supplied field mapping
(regex/grok capture groups), so it works with nginx, Apache, or any HTTP server
without technology-specific code:

```go
type LogFileSourceConfig struct {
    Path         string
    FieldMap     map[string]string  // logical field → regex capture
    TimeFormat   string
    PollInterval time.Duration
}
// Example FieldMap for a typical nginx CVMFS log line:
// hash:      `data/\w{2}/([0-9a-f]{40})`
// repo:      `cvmfs/([^/\s]+)`
// timestamp: `\[([^\]]+)\]`
```

**HTTPReceiverSource** — The pre-publisher exposes `POST /api/v1/access-events`
accepting a JSON batch of `AccessEvent` structs. Any infrastructure component that
can make an HTTP call can feed access data: a log shipper, a CDN webhook, a custom
agent. Authenticated with a pre-shared HMAC token.

**CASBackendSource** — Reads access events from the storage backend. For S3:
server access logs or S3 EventBridge notifications. For local filesystem: inotify
on the CAS data directory. Captures accesses that bypass Stratum 1 (internal
tooling, direct S3 reads).

**PublishEventSource** — The pre-publisher emits an `AccessEvent` with
`Source: "publish"` for every object it successfully publishes. This unconditionally
establishes `last_written` as a floor for `last_seen`, ensuring freshly published
content is never a GC candidate within its TTL even before any proxy logs arrive.

#### Proxy Cache Coverage

Stratum 1 only sees requests that miss the proxy cache. If a file is continuously
cache-hit at the proxy, Stratum 1 may not see it for the duration of the proxy
TTL. The `soft_delete_period` (§10.2) provides the primary buffer against this
(proxy TTLs are hours to days; the tombstone window is days to weeks). For
deployments that need stronger guarantees, configure the `HTTPReceiverSource` and
have proxy nodes periodically POST a summary of hashes served in the last N hours.
This is proxy-agnostic: it is a small script or log-shipper configuration, not a
Squid-specific integration.

### 10.2 Cleanup Policy

Each repository may optionally include a `gc` section in its configuration. The
absence of this section means GC is disabled for that repository.

```yaml
gc:
  enabled: true
  access_ttl: 180d           # expire content not seen for this long
  soft_delete_period: 14d    # tombstone window before hard CAS delete
  schedule: "0 2 * * 0"     # cron: weekly at 02:00 Sunday
  dry_run: false             # if true, produce deletion plan only
  exempt_tags: [stable, lts-2024]   # tag names never subject to expiry
  exempt_paths:
    - "/cvmfs/repo/critical/**"
    - "/cvmfs/repo/releases/latest"
  access_sources: [stratum1, cas_backend, publish]
  max_deletion_rate: 100     # CAS objects deleted per second

access_sources:
  - type: log_file
    name: stratum1-primary
    path: /var/log/nginx/cvmfs-access.log
    field_map:
      hash:      'data/\w{2}/([0-9a-f]{40})'
      repo:      'cvmfs/([^/\s]+)'
      timestamp: '\[([^\]]+)\]'
    time_format: "02/Jan/2006:15:04:05 -0700"

  - type: http_receiver
    name: external-push
    listen: ":9200"
    auth_token_env: CVMFS_ACCESS_TOKEN

  - type: cas_backend
    name: s3-primary

  - type: publish_events
```

### 10.3 GC Pipeline

GC runs are serialised per repository: no concurrent GC and publish can hold a
lease on the same path simultaneously (the gateway enforces this). A GC run
proceeds through four phases.

**Phase 1 — Reference Counter.**  
Before any deletion decision, the reference counter walks every catalog in the
repository: the current head catalog, all nested sub-catalogs, and every tagged
revision. It builds a map of `casHash → refcount`. Only objects with `refcount = 0`
are eligible for deletion. Any catalog read error aborts the entire GC run; the
system never silently skips a catalog.

```go
type RefCounter struct {
    backend CASBackend
    catalog CatalogReader
}

func (rc *RefCounter) BuildRefMap(ctx context.Context, repo string) (map[string]int, error) {
    refs := make(map[string]int)
    tags, err := rc.catalog.ListTags(ctx, repo)
    if err != nil {
        return nil, fmt.Errorf("listing tags: %w", err)  // abort on any error
    }
    for _, tag := range tags {
        if err := rc.walkCatalog(ctx, tag.RootHash, refs); err != nil {
            return nil, fmt.Errorf("walking tag %s: %w", tag.Name, err)
        }
    }
    return refs, nil
}
```

**Phase 2 — Namespace Cleaner.**  
Acquires a gateway lease on the target path subtree. Removes expired entries from
the catalog using the same catalog-diff pipeline as the publishing workflow (a
deletion diff: zero-payload rows for removed paths). Commits via
`POST /api/v1/payloads`. Directory entries are cleaned bottom-up; a directory
that becomes empty after file removal is itself removed unless protected by
`exempt_paths`.

**Phase 3 — Tombstone Manager.**  
Objects whose refcount reached zero after the namespace cleanup are written into
`tombstone.db` with a `delete_after` timestamp:

```go
type TombstoneEntry struct {
    CASHash      string
    Repo         string
    GCRunID      string
    TombstonedAt time.Time
    DeleteAfter  time.Time
    Revoked      bool    // true if re-access detected during soft-delete window
}
```

During the tombstone period, if the `AccessTracker` records a new access for a
tombstoned hash, the entry is immediately revoked. Revoked entries are never
passed to the CAS Reaper.

**Phase 4 — CAS Reaper.**  
Runs continuously, scanning `tombstone.db` for entries past their `DeleteAfter`
time. Before each deletion it re-checks `refcount = 0` against the current catalog
state (TOCTOU guard: a publish could have re-introduced the file during the
tombstone window). Deletions are batched and rate-limited by `max_deletion_rate`.
Each deletion is appended to a signed audit JSONL file retained in `published/`.

### 10.4 GC Job State Machine

GC jobs use the same crash-safe spool model as publishing jobs:

```
gc-scheduled/
  │  refcount walk complete, candidate set produced
  ▼
gc-scanning/
  │  namespace cleaner lease acquired, catalog diff committed
  ▼
ns-cleaning/
  │  tombstone entries written to tombstone.db
  ▼
tombstoned/
  │  soft_delete_period elapsed, refcount re-verified, no revocations
  ▼
gc-reaping/
  │  CAS objects deleted, audit log written
  ▼
gc-complete/ ◄──────────────────────────────────────────────────── terminal

Any state → gc-aborted/   (lease released, tombstones revoked)
```

A GC run aborted at any point revokes all tombstone entries it created, releases
any held lease, and logs the abort reason. Aborted GC runs do not result in data
loss or orphaned objects.

---


## 11. Multi-Build-Node Topology

### 11.1 Motivation

Large HEP experiments ship software on different schedules and require different
build environments.  Serialising all publishes through a single build node
creates a bottleneck.  The cvmfs-prepub architecture supports horizontal scaling
by assigning each experiment group its own build node and a scoped set of
credentials, so multiple groups can publish in parallel without interfering.

### 11.2 Per-Group Namespace Partitioning

The CVMFS gateway issues leases at sub-path granularity.  Two jobs that target
non-overlapping sub-paths can hold leases simultaneously:

```
software.cern.ch/atlas/   ← ATLAS build node holds lease while publishing
software.cern.ch/cms/     ← CMS build node holds lease simultaneously — no conflict
software.cern.ch/lhcb/    ← LHCb build node holds lease simultaneously
software.cern.ch/common/  ← shared toolchain; only one node at a time
```

The gateway enforces mutual exclusion at the sub-path level.  `cvmfs-prepub`
does not need to coordinate across build nodes: each node submits jobs
independently and the gateway arbitrates.

### 11.3 Deployment Topology

```
                       ┌─────────────────────────────────┐
                       │          cvmfs_gateway           │
                       │   (issues leases, signs manifests)│
                       └────────┬────────┬────────┬───────┘
                                │ lease  │ lease  │ lease
                      atlas/    │  cms/  │  lhcb/ │
              ┌─────────────────┘        │        └─────────────────┐
              ▼                          ▼                          ▼
  ┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
  │  ATLAS build node│      │   CMS build node │      │  LHCb build node │
  │  bits + prepub   │      │  bits + prepub   │      │  bits + prepub   │
  │  token: atlas-01 │      │  token: cms-01   │      │  token: lhcb-01  │
  └────────┬─────────┘      └────────┬─────────┘      └────────┬─────────┘
           │                         │                         │
           └─────────────────────────┼─────────────────────────┘
                                     │ commit (gateway lease)
                              ┌──────┴──────┐
                              │  Stratum 1  │
                              │  receivers  │  (pull objects, Chapter 8)
                              └─────────────┘
```

Each build node runs its own `cvmfs-prepub` process (or connects to a shared
prepub service with separate API tokens and path-scoped gateway keys).  The CAS
can be shared across nodes on a fast network filesystem (NFS, CephFS) to
maximise cross-experiment deduplication, or kept local per node if isolation is
preferred.

### 11.4 Credential Scoping

Each build node receives a distinct pair of credentials:

| Credential | Scope | Where stored |
|---|---|---|
| `PREPUB_API_TOKEN` | Controls which jobs the node can submit to this prepub instance | Environment variable / secrets manager |
| `CVMFS_GATEWAY_SECRET` | Authorises lease acquisition; scoped per key-ID to a sub-path prefix | Environment variable / secrets manager |
| Gateway key-ID | e.g. `atlas-build-01` | Config file |

The gateway's key table maps each key-ID to a permitted path prefix.  An ATLAS
key can acquire leases on `atlas/*` but will receive `403 Forbidden` if it
attempts to acquire a lease on `cms/*`.  This is the primary authorization
boundary between groups.

### 11.5 Parallel Publishing

Because the gateway serialises only within a sub-path, independent groups publish
in parallel. Distribution to Stratum 1 is pull-based (Chapter 8): receivers pull
the objects for each transaction from the publisher's object store, so the
publisher does not push to every replica per build node.

### 11.6 Monitoring Across Groups

Add a `group` label to all Prometheus metrics by injecting it via the config:

```toml
[observability]
extra_labels = { group = "atlas" }
```

This allows per-group Grafana dashboards to show job throughput, pipeline
latency, and CAS dedup ratio independently for each experiment.

---


## 12. Go Package Structure

```
cvmfs-prepub/
├── cmd/
│   ├── prepub/              # Main binary: signal handling, config load, startup
│   └── prepubctl/           # Admin CLI: drain, abort, status, gc-trigger, gc-dry-run
│
├── internal/
│   ├── api/                 # HTTP/gRPC server, auth middleware, request validation
│   │
│   ├── job/                 # Job struct, priority queue, FSM transitions
│   │   ├── job.go
│   │   ├── fsm.go
│   │   └── queue.go
│   │
│   ├── spool/               # Spool directory manager: rename, fsync, WAL journal
│   │   ├── spool.go
│   │   └── journal.go
│   │
│   ├── pipeline/            # Orchestrates processing stages
│   │   ├── pipeline.go
│   │   ├── unpack/          # Streaming tar reader, path normaliser
│   │   ├── compress/        # zlib compress + SHA-1 hash worker pool
│   │   ├── chunker/         # CVMFS-compatible content-defined (xor32) chunking
│   │   ├── upload/          # CAS object uploader, retry
│   │   └── catalog/         # Entry collector shim (delegates merge to pkg/cvmfscatalog)
│   │
│   ├── lease/               # cvmfs_gateway lease client, heartbeat goroutine
│   │
│   ├── broker/              # MQTT client, message types, topic schema (control plane)
│   ├── distribute/          # Distribution coordinator (announce / pull / warm)
│   │   ├── serve/           # Discovery, per-txn manifest, object/bundle/catchup serving
│   │   ├── commit/          # Commit orchestrator, warm-gate, admission
│   │   ├── credential/      # Token mint/verify, per-node enrollment, rate limit
│   │   └── receiver/        # --mode receiver: pull client, MQTT handler
│   │
│   ├── gc/                  # GC subsystem
│   │   ├── policy.go        # CleanupPolicy, loader, inotify watcher
│   │   ├── scheduler.go     # Cron runner, per-repo serialisation
│   │   ├── refcount.go      # Reference counter across all catalogs + tags
│   │   ├── nscleaner.go     # Namespace deletion via lease + payload
│   │   ├── tombstone.go     # TombstoneEntry, soft-delete DB
│   │   └── reaper.go        # CAS hard-delete, rate limiter, audit
│   │
│   ├── access/              # Access tracking subsystem
│   │   ├── tracker.go       # AccessTracker: fan-in, dedup, SQLite write
│   │   ├── source.go        # AccessEventSource interface + AccessEvent type
│   │   ├── logfile.go       # Configurable log file tailer (regex field mapping)
│   │   ├── httpreceiver.go  # HTTP POST endpoint source
│   │   ├── casbackend.go    # S3 event notifications / inotify
│   │   └── publish.go       # Internal publish event source
│   │
│   └── cas/                 # CAS backend abstraction
│       ├── cas.go           # CASBackend interface
│       ├── s3.go            # S3 / GCS / MinIO implementation
│       ├── localfs.go       # Local filesystem implementation
│       └── multi.go         # Fan-out multi-backend
│
└── pkg/
    ├── cvmfshash/           # CVMFS content hash encoding (base16 + chunk format)
    └── cvmfscatalog/        # CVMFS catalog: schema 2.5 SQLite, MD5 path encoding,
                             #   manifest parsing, catalog download + merge, secret dirs
        ├── entry.go         # Entry struct, MD5Path, flag constants, mode conversion
        ├── catalog.go       # Create/Open/Upsert/Remove/Finalize (compress+hash→CAS path)
        ├── manifest.go      # ParseManifest, DownloadCatalog (zlib-decompress from HTTP)
        ├── merge.go         # Merge: fetch manifest → download catalog → apply entries → finalise
        └── secret.go        # NewToken, SharePath, SharesDirEntry, TokenDirEntry (kFlagHidden)
```

Key design constraints on package boundaries:

- `gc` and `access` communicate only through the `AccessEventSource` interface.
  No GC code imports any access source implementation.
- `pipeline` stages communicate only through typed Go channels.
  No stage imports another stage's implementation.
- `spool` has no knowledge of what the job contains; it only manages directory
  transitions. Business logic lives in `job` and `pipeline`.

---


## 13. Provenance Architecture

This section describes the architecture of the provenance and transparency log
system.  For the full configuration reference and offline verification
workflow see Chapter 33; for step-by-step setup see Recipe 5 (Chapter 25).

### 13.1 Threat model and motivation

An attacker who has write access to the build system (or who can impersonate a CI
job) could publish malicious content into CVMFS.  Without structured provenance, the
only evidence is server-side access logs — mutable, not independently verifiable, and
absent if the build node is compromised.

The provenance system needs to satisfy three properties:

1. **Irrefutability** — a record that links a specific file to a specific git commit
   cannot be retroactively denied by the publisher (even a malicious insider).
2. **Offline verifiability** — any auditor can confirm the chain using only public
   keys, without relying on `cvmfs-prepub` logs remaining intact.
3. **Minimal trusted-party footprint** — the proof chain should not require trusting
   the build system itself; cryptographic attestation from the CI provider and the
   transparency log is sufficient.

---

### 13.2 The four-layer provenance chain

```
┌───────────────────────────────────────────────────────────────────┐
│  Layer 1: File → Content Hash                                     │
│  Source: CVMFS signed catalog (.cvmfspublished)                   │
│  /atlas/24.0/libAtlasROOT.so  →  <content-hash>                  │
│  Signature: Stratum 0 gateway key (existing CVMFS trust anchor)   │
└─────────────────────────┬─────────────────────────────────────────┘
                          │  reverse-index by hash
┌─────────────────────────▼─────────────────────────────────────────┐
│  Layer 2: Content Hash → Publish Job                              │
│  Source: Rekor transparency log (Sigstore)                        │
│  Entry type: hashedrekord                                         │
│  Fields: <content-hash>, job_id, catalog_hash, git_sha           │
│  Proof: Merkle inclusion proof + Signed Entry Timestamp (SET)     │
│  SET verifiable offline with Rekor's Ed25519 public key           │
└─────────────────────────┬─────────────────────────────────────────┘
                          │  job_id links manifest ↔ Rekor UUID
┌─────────────────────────▼─────────────────────────────────────────┐
│  Layer 3: Publish Job → CI Pipeline                               │
│  Source: job manifest (spool/published/<job_id>/job.json)         │
│  Fields: oidc_issuer, oidc_subject, actor, git_sha, pipeline_id  │
│  Verified: OIDC token validated against issuer's JWKS at submit   │
│  verified=true  ⟹  claims cannot be forged by the submitter      │
└─────────────────────────┬─────────────────────────────────────────┘
                          │  git_sha from verified OIDC claim
┌─────────────────────────▼─────────────────────────────────────────┐
│  Layer 4: CI Pipeline → User / Git Commit                         │
│  Source: VCS (GitHub, GitLab, …)                                 │
│  git log --show-signature <sha>                                   │
│  Fields: Author, Date, signed commit message, GPG/SSH signature   │
│  Closes chain: file → hash → job → pipeline → author             │
└───────────────────────────────────────────────────────────────────┘
```

The diagram below illustrates the same chain with the corresponding verification
commands at each step:

![CVMFS Provenance Chain](docs/cvmfs_provenance_chain.svg)

---


## 14. Security Architecture Overview

This section provides a high-level view of the security trust chain.
Full details of each security control are in Part V (Chapter 32) and, for the
pull control plane, in Chapter 8.

### 14.1 Trust Chain Overview

The end-to-end trust chain from source code to edge worker node is:

```
Source code / build script
    │  (1) Build isolation
    ▼
bits build environment (isolated container or VM)
    │  (2) Optional tar signing (SHA-256 digest + detached signature)
    ▼
Signed tar archive
    │  (3) TLS transport + API token
    ▼
cvmfs-prepub API (HTTPS, bearer token auth)
    │  (4) Pipeline integrity — content-addressed CAS (SHA-1 keys, CVMFS convention)
    ▼
CAS (content-addressed objects, immutable after write)
    │  (5) HMAC-authenticated gateway requests
    ▼
cvmfs_gateway (path-scoped lease, repository-key manifest signing)
    │  (6) Signed manifest + whitelist
    ▼
Stratum 1 receivers (pull objects over the wss-coordinated control plane,
    │                  Ed25519-signed discovery + token auth — Chapter 8)
    │  (7) Client whitelist + manifest signature verification
    ▼
Edge worker nodes (CVMFS client verifies manifest signature before mount)
```



# PART III — USER GUIDE

> **Who should read this part:** Site administrators, CI/CD engineers, and anyone
> operating cvmfs-prepub day-to-day.  This part covers deployment options,
> integration with the bits-console pipeline, monitoring, coexistence with the
> traditional workflow, and security posture.

---

## 15. Deployment Summary

There is one architecture; Stratum 1 pre-warming is optional and can be added
incrementally:

| Capability | Publisher only | Publisher + pull pre-warming |
|---|---|---|
| **Pre-processes tar** | ✓ | ✓ |
| **Bypasses overlay FS** | ✓ | ✓ |
| **Pre-warms Stratum 1 before catalog flip** | ✗ | ✓ |
| **New infrastructure** | None | `cvmfs-prepub --mode receiver` on each Stratum 1 |

**Publisher only.** The publisher runs the pipeline and commits via the gateway.
No changes are needed on Stratum 1 replicas; they replicate after the catalog
flip exactly as they do today (`cvmfs_server snapshot`).

**Publisher + pull pre-warming.** Each Stratum 1 runs `cvmfs-prepub --mode
receiver`, which subscribes to the publisher's control-plane broker and **pulls**
the objects for each transaction before the catalog flip (Chapter 8). Receivers
open only outbound connections, so no inbound firewall changes are required at
Stratum 1 sites.

See Chapter 6 (System Overview), Chapter 7 (Architecture), and Chapter 8 (Pull
Distribution and the Control Plane). See Cookbook recipes 21 and 22 for
step-by-step deployment.

---

## 16. bits Integration — Data Flow and Deduplication

### 16.1 Data Flow

```
bits build node
  │  (1) tar produced
  ▼
HTTP POST /api/v1/jobs          (multipart upload or spool reference)
  │  (2) job enters FSM: incoming → leased
  ▼
gateway lease acquire           (single RPC to cvmfs_gateway)
  │  (3) leased → staging
  ▼
tar unpack + scan
  │  (4) staging → uploading
  ▼
compress + CAS upload           (dedup eliminates already-known objects)
  │  (5) uploading → distributing
  ▼
announce on broker; receivers pull missing objects and warm (Chapter 8)
  │  (6) distributing → committing
  ▼
gateway commit + manifest sign
  │  (7) committing → published
  ▼
cvmfs client sees new revision  (after the client's catalog TTL elapses)
```

The dominant cost is normally the compress + CAS upload stage, which scales with
the volume of **new** (non-deduplicated) data. End-to-end wall-clock time depends
on release size, the number of new objects, network bandwidth to the CAS and
receivers, and the CVMFS client TTL; no fixed figures are quoted here because
they have not been measured under a defined workload.

To reduce the delay before clients see a new revision, configure a shorter
`CVMFS_MAX_TTL` for frequently-updated repositories, or trigger `cvmfs_config
reload` on workers after the gateway commits.

### 16.2 Deduplication for Incremental Packages

CAS objects are content-addressed, so files that are shared across software
versions (shared libraries, runtimes, common headers) are uploaded once and
reused on subsequent publishes. A `bits` package update that changes a small
fraction of files only uploads that fraction.

The pipeline's dedup check (§9.2) is a direct `CAS.Exists()` per object — an
`os.Stat` on the local filesystem backend or a single `HEAD` request on S3.
Objects that already exist are recorded in the catalog and skipped by the
uploader; only genuinely new objects are transferred. The check is exact (no
false positives) and requires no precomputed inventory.

---


## 17. bits-console Pipeline Integration

### 17.1 Where cvmfs-prepub Fits in the bits-console Pipeline

bits-console supports three publication backends selectable per community via
`publish_pipeline` in `ui-config.yaml`:

| Pipeline file | Backend | Runners needed |
|---|---|---|
| `.gitlab/cvmfs-publish.yml` | `cvmfs-ingest` daemon + `cvmfs_server` (three stages) | `bits-build-<arch>`, `bits-ingest`, `bits-cvmfs-publisher` |
| `.gitlab/cvmfs-local-publish.yml` | `cvmfs-local-publish` systemd daemon (single-host) | `bits-build-<arch>` (must also be the CVMFS gateway host, tagged `bits-local-cvmfs`) |
| `.gitlab/cvmfs-prepub-publish.yml` | **cvmfs-prepub REST API** (this service) | `bits-build-<arch>` only |

When `cvmfs-prepub-publish.yml` is selected the build stage compiles and
packages the software as usual, then **POSTs the resulting tarball** directly
to the `cvmfs-prepub` REST API over HTTPS.  The service handles all remaining
work — decompression, content hashing, deduplication, CAS upload, optional
Stratum 1 pull pre-warming, gateway lease acquisition, and catalog commit
— asynchronously on the pre-publisher node.  The CI job polls for the job's
terminal state and exits accordingly.

```
bits-console (browser)
       │ POST /projects/:id/trigger/pipeline
       ▼
GitLab CI — bits-console project
       │
       ├─ Stage: build   (bits-build-<arch> runner)
       │    bits build --docker … <PACKAGE>
       │    tar czf package.tar.gz <install-dir>
       │    POST https://<prepub-host>:8080/api/v1/jobs  → job_id
       │    poll  https://<prepub-host>:8080/api/v1/jobs/<job_id>
       │         until state == "published"
       │
       └─ Stage: status  (any runner)
            update cvmfs-status.json in bits-console repo
                    │
                    ▼
          cvmfs-prepub service (runs continuously on publisher node)
               unpack → compress+hash → dedup → CAS upload
               [optional: announce; Stratum 1 receivers pull and warm]
               lease → catalog commit → "published"
```

This eliminates the `bits-ingest` and `bits-cvmfs-publisher` runners and their
associated SSH key management, spool directory, and sequencing constraints.

### 17.2 Pipeline Comparison

| Aspect | cvmfs-publish.yml (three-stage) | cvmfs-prepub-publish.yml |
|---|---|---|
| **Runners required** | build + ingest + publisher (3 types) | build only |
| **Ingest mechanism** | `cvmfs-ingest` binary, rsync over SSH | REST API POST over HTTPS |
| **CVMFS transaction** | `cvmfs_server publish` on stratum-0 | `cvmfs_gateway` lease+payload API |
| **Stratum 1 pre-warming** | Not built in | Optional pull pre-warming (Chapter 8) |
| **Crash recovery** | Spool directory survives runner restart | WAL-journalled spool in cvmfs-prepub |
| **Deduplication** | Per the three-stage tooling | Direct `CAS.Exists()` — `os.Stat` / S3 `HEAD`, shared across jobs |
| **Provenance** | Not built in | Optional Rekor transparency log (Chapter 33) |
| **Parallel jobs** | Serialised by the CVMFS transaction lock | Serialised by the gateway lease; pipeline runs before the lease |
| **CI/CD variables** | `SPOOL_SSH_KEY`, `SPOOL_USER`, `SPOOL_HOST`, `SPOOL_PATH` | `PREPUB_URL`, `PREPUB_API_TOKEN` |


## 18. Coexistence with `cvmfs_server publish`

The gateway's per-path lease enforces mutual exclusion at sub-path granularity.
A `cvmfs-prepub` job on `atlas/24.0` and a traditional `cvmfs_server publish` on
`cms/` can hold leases simultaneously without conflict.  Both workflows compete
only if they target the same sub-path at the same time, in which case one must
wait — the same constraint that applies between two traditional publishes.

There is no flag day: operators can deploy `cvmfs-prepub` for new or
CI-driven repositories while continuing to use the traditional overlay workflow
for manual or infrequent publishes on other repositories.

### 18.1 What Does Not Change

| Layer | Status |
|---|---|
| `cvmfs_gateway` | Unmodified; targeted via existing lease-and-payload API |
| Stratum 1 replication daemon | Unmodified (`cvmfs_server snapshot` unchanged) |
| Stratum 0 CAS layout | Identical; objects written with the same path structure |
| Signed manifest format | Identical; gateway signs as usual |
| Proxy / Squid caches | Unmodified; cache the same object URLs |
| CVMFS client | Unmodified; verifies the same manifest signature |
| Traditional `cvmfs_server publish` | Continues to work in parallel |

For the behavioural difference in hard-link handling and guidance on when to use
each path, see §4.4 and §4.5.

---


## 19. Monitoring and Observability

Every significant operation emits an OpenTelemetry span.  Prometheus metrics
are exposed at `/api/v1/metrics` on the publisher and at `/metrics` on each
receiver.  Structured JSON logs use `log/slog` throughout.

The `testutil/simulate` package runs the full publish pipeline in-process with
fake infrastructure components, each emitting their own spans, making
distributed traces observable in a single `go test` run without any external
services.

### 19.1 Prometheus Metrics

Key publisher metrics:

| Metric | Type | Description |
|---|---|---|
| `cvmfs_prepub_jobs_active` | Gauge | Currently running jobs |
| `cvmfs_prepub_jobs_total` | Counter | Total jobs started |
| `cvmfs_prepub_pipeline_duration_seconds` | Histogram | End-to-end pipeline latency |
| `cvmfs_prepub_cas_objects_total` | Gauge | Objects in the CAS backend |
| `cvmfs_prepub_cas_bytes_used` | Gauge | Bytes used in the CAS backend |

Receiver-specific metrics (see also Chapter 31):

| Metric | Type | Description |
|---|---|---|
| `cvmfs_receiver_objects_received_total` | Counter | CAS objects stored by the receiver |
| `cvmfs_receiver_bytes_received_total` | Counter | Compressed bytes stored |

### 19.2 Structured Logging

All log output is `log/slog` JSON.  Set `--log-level` to `debug`, `info`
(default), `warn`, or `error`.  Every log line includes `job_id`, `endpoint`,
and `hash` where applicable, so logs can be correlated with OTel traces.

### 19.3 OpenTelemetry Traces

The publisher propagates W3C `traceparent` headers on outbound HTTP requests
(gateway, CAS, object serving).  Set `OTEL_EXPORTER_OTLP_ENDPOINT` to a
compatible collector.

## 20. Backward Compatibility and Fallback

`cvmfs-prepub` is a first-class client of the existing `cvmfs_gateway`
lease-and-payload API and writes the standard CVMFS CAS layout and signed
manifest format, so a repository published via `cvmfs-prepub` remains fully
compatible with the traditional `cvmfs_server publish` and `cvmfs_server
snapshot` tooling (see Chapter 18). If pull pre-warming is not deployed, Stratum 1
replication simply happens after the catalog flip exactly as it does today, with
no receiver involvement.

For provenance — a structural property of every publish job, sealed at publish
time and optionally anchored in the Rekor transparency log — see Chapter 13
(architecture) and Chapter 33 (reference).



# PART IV — COOKBOOK

> **Who should read this part:** Operators performing a concrete task — deploying a
> node, wiring up GitLab CI, adding Stratum 1 pre-warming, or setting up
> provenance signing.  Each recipe is self-contained and cross-references the
> relevant reference sections for full parameter details.

---

## 21. Recipe 1: Deploy a Single Stratum 0 Node

**Goal:** Run cvmfs-prepub in publisher mode on the Stratum 0 node, bypassing
the overlay filesystem.  No Stratum 1 changes required.

### 21.1 Summary

`cvmfs-prepub` runs alongside the existing `cvmfs_server publish` workflow
without replacing or disrupting it.  No changes to Stratum 1 servers, proxy
caches, or CVMFS clients are required for the publisher-only deployment; pull
pre-warming (Recipe 2) adds a receiver on each Stratum 1 but leaves the existing
replication machinery untouched.

### 21.2 Publisher-Only Deployment (Zero Stratum 1 Changes)

The pre-publisher is a first-class client of the existing `cvmfs_gateway`
lease-and-payload API (`POST /api/v1/leases`, `PUT /api/v1/leases/{token}`,
`POST /api/v1/payloads`).  No gateway modifications are required; gateway ≥ 1.2
is sufficient.

After the catalog is committed, Stratum 1 servers replicate it exactly as they do
today — by running `cvmfs_server snapshot` and fetching the new manifest and any
missing objects from the Stratum 0 CAS.  The signed manifest format, catalog
schema, and CAS object layout are identical to those produced by
`cvmfs_server publish`.  Stratum 1 replication, proxy caching, and CVMFS client
verification are all unaware that a different publishing path was used.

**New infrastructure required:** the `cvmfs-prepub` binary on one node with
write access to the CAS backend and network reach to the gateway.  That node can
be the Stratum 0 host or a separate build node.

### 21.3 Recommended Rollout Order

1. **Publisher on a test repository** — validate correctness alongside the
   existing workflow; no Stratum 1 involvement.
2. **Pull pre-warming on one Stratum 1 site** — deploy the receiver on a single
   replica and observe its warm behaviour before and after a publish.
3. **Pull pre-warming on remaining Stratum 1 sites** progressively as confidence
   grows.
4. **CI-driven repositories** migrated from the overlay workflow to
   `cvmfs-prepub`; manual ad-hoc publishes can remain on `cvmfs_server publish`
   indefinitely.

---


## 22. Recipe 2: Add Stratum 1 Pre-Warming (Pull)

**Goal:** Deploy the receiver on each Stratum 1 and configure the publisher's
control plane so receivers pull objects before the catalog flip.

### 22.1 How Pull Pre-Warming Works

The publisher announces each transaction on its embedded control-plane broker
*before* the catalog is committed. Each Stratum 1 receiver reads the
per-transaction manifest and **pulls** the objects it is missing from the
publisher's content-addressed object store, verifying each by hash, so the
replication snapshot after the catalog flip becomes catalog-only. Receivers open
only outbound connections — no inbound ports are needed at Stratum 1.

To act as a pull receiver, each Stratum 1 node runs the same `cvmfs-prepub`
binary in receiver mode:

```sh
cvmfs-prepub --mode receiver \
  --discovery-url   https://cvmfs-prepub:8080 \
  --discovery-verify-key /etc/cvmfs-receiver/discovery.pub \
  --broker-auth \
  --broker-ca-cert  /etc/cvmfs-receiver/ca.crt \
  --receiver-stratum0-url http://cvmfs-prepub:8080/cvmfs \
  --repos atlas.cern.ch,cms.cern.ch \
  --cas-root /srv/cvmfs/stratum1/cas
# PREPUB_NODE_KEY=<hex per-node key, provisioned on Stratum 0> in the environment
```

The receiver:
- fetches the signed discovery document and verifies it with the Ed25519 public
  key, learning its control-plane broker URL and enroll endpoint;
- enrolls over TLS (challenge/response with its per-node key) and subscribes to
  the broker over `wss://`;
- on each announce, pulls the objects it is missing and places them in the local
  CAS directory using the standard CVMFS on-disk layout;
- warms its local catalog when it has the objects for the committed root.

The existing `cvmfs_server snapshot` daemon on each Stratum 1 is unchanged: when
it runs after the catalog commit it finds the new objects already present and
fetches only the catalog.

On the publisher, enable the control plane with `--embedded-broker-ws-addr`,
`--control-plane-url`, the broker TLS cert/key, `--embedded-broker-auth`, the
enroll listener (`--enroll-tls-addr` / `--enroll-url`),
`--discovery-signing-key`, and `--pull-object-base-url` (see Chapter 29 and
Chapter 8). Key provisioning is covered in Recipe 7.

**Alternative without receivers.** If all Stratum 1s share an S3-compatible
object store as their data backend, a single CAS upload on the publisher makes
objects available to every Stratum 1 immediately; no receiver is needed.


## 23. Recipe 3: bits-console GitLab CI Integration

**Goal:** Wire cvmfs-prepub into a bits-console GitLab CI/CD pipeline so that
every tagged build automatically publishes to CVMFS.

### 23.1 GitLab CI/CD Variables

Set these in the bits-console GitLab project under **Settings → CI/CD →
Variables**.  Mark sensitive values **Masked** and **Protected** (available
only on protected branches; fork MR pipelines cannot access them).

| Variable | Protected | Masked | Description |
|---|:-:|:-:|---|
| `PREPUB_URL` | ✅ | — | Full base URL of the cvmfs-prepub API, e.g. `https://prepub.example.org:8080` |
| `PREPUB_API_TOKEN` | ✅ | ✅ | Bearer token authorising job submission.  Set the same value in the cvmfs-prepub server's `EnvironmentFile` as `PREPUB_API_TOKEN`. |
| `CVMFS_REPO` | ✅ | — | CVMFS repository name, e.g. `sft.cern.ch` (fallback if not set in `ui-config.yaml`) |

The existing `SPOOL_SSH_KEY`, `SPOOL_USER`, `SPOOL_HOST`, and `SPOOL_PATH`
variables are **not needed** when using the cvmfs-prepub pipeline.  They may
coexist in the project if some communities still use the three-stage pipeline.

### 23.2 Community ui-config.yaml Changes

In each community's `communities/<name>/ui-config.yaml`, change the
`publish_pipeline` field to select the cvmfs-prepub backend:

```yaml
# Before (three-stage ingest+publish pipeline):
publish_pipeline: .gitlab/cvmfs-publish.yml

# After (cvmfs-prepub REST API):
publish_pipeline: .gitlab/cvmfs-prepub-publish.yml
```

No other `ui-config.yaml` fields need to change.  The `cvmfs_repo`,
`cvmfs_prefix`, `cvmfs_user_prefix`, `platforms`, `admins`, and
`bits_admins` fields are all consumed by the pipeline's server-side
authorisation block exactly as before.

**Minimal working example** (community switching to cvmfs-prepub):

```yaml
title: LCG Software Console
admins:
  - pbuncic
read_token: glpat-xxxxxxxxxxxxxxxxxxxx
cvmfs_repo: sft.cern.ch
cvmfs_prefix: /cvmfs/sft.cern.ch/lcg/releases
cvmfs_user_prefix: /cvmfs/sft.cern.ch/lcg/user
platforms:
  - x86_64-el9
  - aarch64-el9
publish_pipeline: .gitlab/cvmfs-prepub-publish.yml   # ← only this line changes
```

### 23.3 The cvmfs-prepub Pipeline File

Add the file `.gitlab/cvmfs-prepub-publish.yml` to the bits-console GitLab
project.  It follows the same structure as the existing pipeline files: a
build stage that compiles with bits and a status stage that updates
`cvmfs-status.json`.  The key difference is the publish step: instead of
rsyncing to an SSH spool or writing a local daemon receipt, it submits a tarball
to the cvmfs-prepub REST API and polls for the terminal state.

```yaml
# .gitlab/cvmfs-prepub-publish.yml
# bits-console → cvmfs-prepub integration pipeline.
#
# Pipeline variables (injected by bits-console frontend, same as cvmfs-publish.yml):
#   COMMUNITY, PACKAGE, VERSION, PLATFORM, RUNNER_ARCH,
#   BITS_BUILD_ARGS, CVMFS_INSTALL_DIR
#
# CI/CD variables (Settings → CI/CD → Variables):
#   PREPUB_URL       https://prepub.example.org:8080
#   PREPUB_API_TOKEN Bearer token (Protected + Masked)
#   CVMFS_REPO       Repository name (fallback)

bits-build:
  stage: build
  timeout: 6h
  variables:
    GIT_STRATEGY: none
  tags:
    - self-hosted
    - bits-build-${RUNNER_ARCH}
  rules:
    - if: '$CI_PIPELINE_SOURCE =~ /^(api|trigger|web)$/ && $PACKAGE != ""'
  script:
    # ── Server-side authorisation (identical to cvmfs-publish.yml) ────────────
    # Fetches ui-config.yaml via CI_JOB_TOKEN, resolves EFFECTIVE_TARGET from
    # the caller's GitLab identity.  Admins → cvmfs_prefix; others → cvmfs_user_prefix.
    - |
      [[ -z "${COMMUNITY:-}" ]] && { echo "ERROR: COMMUNITY is required"; exit 1; }
      [[ -z "${PACKAGE:-}"   ]] && { echo "ERROR: PACKAGE is required";   exit 1; }
      [[ "${COMMUNITY}" =~ ^[a-zA-Z0-9_-]+$   ]] || { echo "ERROR: invalid COMMUNITY"; exit 1; }
      [[ "${PACKAGE}"   =~ ^[a-zA-Z0-9._+-]+$ ]] || { echo "ERROR: invalid PACKAGE";   exit 1; }
      export COMMUNITY_LC=$(echo "$COMMUNITY" | tr '[:upper:]' '[:lower:]')

      eval "$(curl -sf --header "JOB-TOKEN: ${CI_JOB_TOKEN}" \
        "${CI_API_V4_URL}/projects/${CI_PROJECT_ID}/repository/files/config%2Fdirs.yaml/raw?ref=${CI_COMMIT_SHA}" \
        2>/dev/null | python3 -c '
      import yaml,sys
      doc = yaml.safe_load(sys.stdin) or {}
      sw  = (doc.get("sw_dir","") or "").strip() or "/build/bits/sw"
      run = (doc.get("runner_dir","") or "").strip() or "/home/gitlab-runner"
      print(f"export _BITS_WORK_DIR={sw!r}")
      print(f"export _BITS_RUNNER_DIR={run!r}")
      ')"

      export _BITS_JOB_DIR="${_BITS_RUNNER_DIR}/jobs/${CI_JOB_ID}"
      export _BITS_AUTH="${_BITS_JOB_DIR}/.bits-auth"
      export _COMMUNITY_YAML="${_BITS_JOB_DIR}/community.yaml"
      mkdir -p "${_BITS_JOB_DIR}"

      curl -sf --header "JOB-TOKEN: ${CI_JOB_TOKEN}" \
        "${CI_API_V4_URL}/projects/${CI_PROJECT_ID}/repository/files/communities%2F${COMMUNITY}%2Fui-config.yaml/raw?ref=${CI_COMMIT_SHA}" \
        > "${_COMMUNITY_YAML}"

      python3 -c '
      import pathlib, yaml, os, sys
      cfg          = yaml.safe_load(open(os.environ["_COMMUNITY_YAML"]))
      bits_admins  = cfg.get("bits_admins", []) or []
      group_admins = cfg.get("admins",      []) or []
      prod_prefix  = (cfg.get("cvmfs_prefix",      "") or "").rstrip("/")
      user_prefix  = (cfg.get("cvmfs_user_prefix", "") or "").rstrip("/")
      cvmfs_repo   = cfg.get("cvmfs_repo", "") or ""
      login        = os.environ.get("GITLAB_USER_LOGIN", "")
      package      = os.environ["PACKAGE"]
      is_admin     = login in bits_admins or login in group_admins
      def seg(*p): return "/" + "/".join(s.strip("/") for s in p if s)
      prefix_abs = str(pathlib.PurePosixPath(
          seg(prod_prefix if is_admin else user_prefix,
              "" if is_admin else login, package)))
      mount = f"/cvmfs/{cvmfs_repo}"
      if not prefix_abs.startswith(mount + "/"):
          print(f"ERROR: prefix {prefix_abs!r} outside {mount}", file=sys.stderr); sys.exit(1)
      prefix_rel = prefix_abs[len(mount):].lstrip("/")
      group_recipes = (cfg.get("group_recipes", "") or "").strip()
      print(f"IS_ADMIN={1 if is_admin else 0}")
      print(f"CVMFS_PKG_PREFIX_ABS={prefix_abs!r}")
      print(f"CVMFS_PKG_PREFIX_REL={prefix_rel!r}")
      print(f"CVMFS_REPO_CFG={cvmfs_repo!r}")
      print(f"GROUP_RECIPES={group_recipes!r}")
      print(f"[auth] user={login!r} is_admin={is_admin}", file=sys.stderr)
      ' > "${_BITS_AUTH}"

      source "${_BITS_AUTH}"
      echo "COMMUNITY_LC=${COMMUNITY_LC}" >> "${CI_PROJECT_DIR}/build.env"

      SETUP_URL="${CI_API_V4_URL}/projects/${CI_PROJECT_ID}/repository/files/.gitlab%2Fbits-setup.sh/raw?ref=${CI_COMMIT_SHA}"
      curl -sf --header "JOB-TOKEN: ${CI_JOB_TOKEN}" "$SETUP_URL" \
        -o "${_BITS_JOB_DIR}/bits-setup.sh"

    # ── bits CLI, setup, pre-build cleanup ───────────────────────────────────
    - |
      [[ -f /etc/profile.d/bits.sh ]] && source /etc/profile.d/bits.sh
      command -v bits &>/dev/null || { echo "ERROR: bits not on PATH"; exit 1; }
      source "${_BITS_JOB_DIR}/bits-setup.sh"
      bits cleanup --min-free "${CACHE_MIN_FREE_GB:-50}" --disk-pressure-only || true

    # ── Build ─────────────────────────────────────────────────────────────────
    - |
      _BUILD_LOG="${CI_PROJECT_DIR}/bits-build.log"
      _BUILD_START=$(date +%s)
      cd "${BITS_CONF_DIR}"
      echo "[build] bits build --config-dir . ${BITS_BUILD_ARGS} ${PACKAGE}" | tee "$_BUILD_LOG"
      set +e
      bits build --config-dir . $BITS_BUILD_ARGS "$PACKAGE" 2>&1 \
        | tee -a "$_BUILD_LOG" | grep --line-buffered -v ":WARNING:.*Ignoring dangling"
      _bits_exit="${PIPESTATUS[0]}"
      set -e
      [[ "$_bits_exit" -eq 0 ]] || { echo "ERROR: bits build failed"; exit "$_bits_exit"; }

    # ── Resolve install directory and package it ──────────────────────────────
    - |
      ARCH=$(bits architecture)
      _MANIFEST=$(ls -1t "$BITS_WORK_DIR/MANIFESTS/bits-manifest-${PACKAGE}-"*.json \
                  2>/dev/null | head -1)
      [[ -z "$_MANIFEST" ]] && _MANIFEST="$BITS_WORK_DIR/MANIFESTS/bits-manifest-latest.json"
      VERSION_DIR=$(jq -r --arg pkg "$PACKAGE" \
        '.packages[] | select(.package == $pkg) |
         if ((.revision // "") != "") then "\(.version)-\(.revision)" else .version end' \
        "$_MANIFEST" | head -1)
      FAMILY=$(jq -r --arg pkg "$PACKAGE" \
        '.packages[] | select(.package == $pkg) | .pkg_family // ""' \
        "$_MANIFEST" | head -1)
      if [[ -n "$FAMILY" && "$FAMILY" != "null" ]]; then
        SOURCE_DIR="${BITS_WORK_DIR}/${ARCH}/${FAMILY}/${PACKAGE}/${VERSION_DIR}"
      else
        SOURCE_DIR="${BITS_WORK_DIR}/${ARCH}/${PACKAGE}/${VERSION_DIR}"
      fi
      [[ -d "$SOURCE_DIR" ]] || {
        _FOUND=$(find "${BITS_WORK_DIR}/${ARCH}" -mindepth 2 -maxdepth 3 -type d \
                   -name "${VERSION_DIR}" 2>/dev/null | grep -F "/${PACKAGE}/${VERSION_DIR}" | head -1)
        [[ -n "$_FOUND" ]] && SOURCE_DIR="$_FOUND" || { echo "ERROR: install dir not found"; exit 1; }
      }

      CVMFS_TARGET_REL="${CVMFS_PKG_PREFIX_REL}/${VERSION_DIR}/${CVMFS_INSTALL_DIR}"
      PKG_ID="${PACKAGE}-${VERSION_DIR}-${ARCH/\//_}"
      echo "PKG_ID=${PKG_ID}" >> "${CI_PROJECT_DIR}/build.env"
      echo "[publish] SOURCE_DIR=${SOURCE_DIR}"
      echo "[publish] CVMFS_TARGET_REL=${CVMFS_TARGET_REL}"

      # Package the install directory into a tar stream
      _TARBALL="${CI_PROJECT_DIR}/package.tar.gz"
      tar -czf "$_TARBALL" -C "$(dirname "$SOURCE_DIR")" "$(basename "$SOURCE_DIR")"
      echo "[publish] tarball size: $(du -sh "$_TARBALL" | cut -f1)"

    # ── Submit to cvmfs-prepub and poll for completion ────────────────────────
    - |
      [[ -n "${PREPUB_URL:-}" ]]       || { echo "ERROR: PREPUB_URL is not set";       exit 1; }
      [[ -n "${PREPUB_API_TOKEN:-}" ]] || { echo "ERROR: PREPUB_API_TOKEN is not set"; exit 1; }

      _PUBLISH_LOG="${CI_PROJECT_DIR}/bits-publish.log"
      _PUBLISH_START=$(date +%s)

      # Submit the job; receive a job_id.
      JOB_ID=$(curl -sf -X POST \
        -H "Authorization: Bearer ${PREPUB_API_TOKEN}" \
        -H "Content-Type: application/octet-stream" \
        -H "X-CVMFS-Repo: ${CVMFS_REPO_CFG}" \
        -H "X-CVMFS-Path: ${CVMFS_TARGET_REL}" \
        -H "X-Package: ${PACKAGE}" \
        -H "X-Version: ${VERSION_DIR}" \
        -H "X-Platform: ${PLATFORM}" \
        --data-binary @"${CI_PROJECT_DIR}/package.tar.gz" \
        "${PREPUB_URL}/api/v1/jobs" | jq -r .job_id)
      echo "[publish] submitted job ${JOB_ID}" | tee -a "$_PUBLISH_LOG"
      echo "PREPUB_JOB_ID=${JOB_ID}" >> "${CI_PROJECT_DIR}/build.env"

      # Poll until the job reaches a terminal state (timeout: 30 min).
      for i in $(seq 1 180); do
        RESPONSE=$(curl -sf \
          -H "Authorization: Bearer ${PREPUB_API_TOKEN}" \
          "${PREPUB_URL}/api/v1/jobs/${JOB_ID}")
        STATE=$(echo "$RESPONSE" | jq -r .state)
        echo "[publish] ${i}/180 state=${STATE}" | tee -a "$_PUBLISH_LOG"
        if [[ "$STATE" == "published" ]]; then
          _PUBLISH_SECS=$(( $(date +%s) - _PUBLISH_START ))
          echo "[publish] ✓ published in ${_PUBLISH_SECS}s" | tee -a "$_PUBLISH_LOG"
          break
        fi
        if [[ "$STATE" == "failed" || "$STATE" == "aborted" ]]; then
          echo "ERROR: cvmfs-prepub job ${JOB_ID} ended with state=${STATE}" | tee -a "$_PUBLISH_LOG"
          echo "$RESPONSE" | jq . >> "$_PUBLISH_LOG"
          exit 1
        fi
        sleep 10
      done
      [[ "$STATE" == "published" ]] || { echo "ERROR: poll timeout after 30 min"; exit 1; }

  after_script:
    - |
      [[ -f "${CI_PROJECT_DIR}/.bits-dirs" ]] && source "${CI_PROJECT_DIR}/.bits-dirs"
      rm -rf "${BITS_RUNNER_DIR:-/home/gitlab-runner}/jobs/${CI_JOB_ID}"

  artifacts:
    reports:
      dotenv: build.env
    paths:
      - bits-build.log
      - bits-publish.log
    when: always
    expire_in: 1 week

bits-update-status:
  stage: status
  rules:
    - if: '$CI_PIPELINE_SOURCE =~ /^(api|trigger|web)$/ && $PACKAGE != ""'
  tags:
    - self-hosted
    - bits-build-${RUNNER_ARCH}
  needs:
    - job: bits-build
      artifacts: true
  variables:
    GIT_STRATEGY: clone
    GIT_DEPTH: "1"
  script:
    - PIPELINE_LABEL=prepub python3 .gitlab/bits-update-status.py
    - git config user.name  "GitLab CI"
    - git config user.email "noreply@gitlab.com"
    - git remote set-url origin "https://ci-token:${CI_JOB_TOKEN}@${CI_SERVER_HOST}/${CI_PROJECT_PATH}.git"
    - git add cvmfs-status.json
    - 'git diff --cached --quiet || git commit -m "cvmfs-status: publish ${PKG_ID} [prepub]" && git push origin HEAD:${CI_COMMIT_BRANCH}'
```

### 23.4 Running Alongside Existing Pipelines

Different communities within the same bits-console deployment may use different
pipelines simultaneously.  The `publish_pipeline` field in each community's
`ui-config.yaml` is independent; switching one community to cvmfs-prepub does
not affect others.

Communities using `cvmfs-publish.yml` still need the `bits-ingest` and
`bits-cvmfs-publisher` runners.  Communities using `cvmfs-prepub-publish.yml`
need only the `bits-build-<arch>` runner.

Both pipelines write to `cvmfs-status.json` in the bits-console repository
(via `bits-update-status.py`).  The `PIPELINE_LABEL` variable distinguishes
their entries: `local` for `cvmfs-local-publish.yml`, `prepub` for
`cvmfs-prepub-publish.yml`, and the default (empty) for `cvmfs-publish.yml`.
The bits-console CVMFS status tab displays all entries regardless of their
source pipeline.

**Infrastructure checklist for adding cvmfs-prepub as a pipeline backend:**

1. Deploy `cvmfs-prepub` on the publisher node (INSTALL.md §2–§7).
2. Add `PREPUB_URL` and `PREPUB_API_TOKEN` to GitLab **Settings → CI/CD → Variables** (Protected + Masked).
3. Add `.gitlab/cvmfs-prepub-publish.yml` to the bits-console repository (content from §23.3).
4. For each community switching to cvmfs-prepub, change `publish_pipeline` in `communities/<name>/ui-config.yaml` and merge the MR.
5. For Stratum 1 pull pre-warming, deploy the receiver on each Stratum 1 and configure the control plane (Recipe 2, Chapter 8).

---


## 24. Recipe 4: Multi-Build-Node Deployment

**Goal:** Scale to multiple concurrent build nodes, each responsible for a
distinct set of CVMFS repository paths, with independent credentials and
parallel publishing.

The architecture and per-group credential model are covered in full in
Chapter 11 (Multi-Build-Node Topology). In summary:

1. Give each build node its own `PREPUB_API_TOKEN` and a gateway key-ID scoped
   (in the gateway's key table) to its repository sub-path prefix; a key for
   `atlas/*` receives `403 Forbidden` if it attempts a lease on `cms/*`
   (§11.2, §11.4).
2. Because the gateway serialises only within a sub-path, independent groups
   publish in parallel; distribution to Stratum 1 is pull-based, so the
   publisher does not push to every replica per build node (§11.5).
3. Optionally inject a `group` label into Prometheus metrics for per-group
   dashboards (§11.6):

```toml
[observability]
extra_labels = { group = "atlas" }
```

Share the CAS across build nodes on a fast network filesystem (NFS, CephFS) to
maximise cross-experiment deduplication, or keep it local per node for isolation.

---


## 25. Recipe 5: Provenance and Transparency Log Setup

**Goal:** Enable CI-attested provenance records for every published CVMFS
object, submitted to the Sigstore Rekor transparency log.

### 25.1 Configuration reference

| Flag | Default | Description |
|---|---|---|
| `--provenance` | `false` | Enable provenance recording and Rekor submission. Off by default — no behaviour change for existing deployments. |
| `--rekor-server` | `https://rekor.sigstore.dev` | Rekor instance URL. Override for self-hosted deployments. |
| `--rekor-signing-key` | `{spoolRoot}/provenance.key` | Path to the node's Ed25519 signing key (PEM/PKCS#8). Auto-generated on first start if absent. Back this file up — it provides the attribution link for all entries submitted by this node. |
| `--oidc-issuers` | _(empty — OIDC disabled)_ | Comma-separated list of OIDC issuer URLs to accept. Common values: `https://token.actions.githubusercontent.com` (GitHub Actions), `https://gitlab.com` (GitLab SaaS). |

Environment variables used alongside these flags:

| Variable | Purpose |
|---|---|
| `PREPUB_API_TOKEN` | Bearer token for authenticating `POST /api/v1/jobs` requests |
| `CVMFS_GATEWAY_SECRET` | HMAC secret for gateway lease operations |

No credentials for Rekor itself are required when using the public instance — it
accepts unauthenticated submissions.

---

### 25.2 Offline verification workflow

Given a suspect file at `/cvmfs/software.cern.ch/atlas/24.0/libAtlasROOT.so`:

```bash
# 1. Get the content hash from the CVMFS catalog
attr -g hash /cvmfs/software.cern.ch/atlas/24.0/libAtlasROOT.so
# → a3f9b812...

# 2. Query Rekor by hash (returns matching log entry UUIDs)
rekor-cli search --sha sha256:a3f9b812...
# → Found matching entries (listed below):
# → 24296fb24b3d...

# 3. Retrieve and verify the Rekor entry (includes Merkle inclusion proof)
rekor-cli get --uuid 24296fb24b3d...
# → LogIndex: 183047291
# → IntegratedTime: 2026-04-24T09:14:13Z
# → UUID: 24296fb24b3d...
# → Body:
#    {  "job_id": "550e8400-...",  "git_sha": "c4e9f2a", ... }

# 4. Verify the SET offline (no network required after this step)
rekor-cli verify --uuid 24296fb24b3d...
# → Current Root Hash matches verified Entry root hash

# 5. Cross-reference the job manifest (if you have access to the spool)
cat /var/spool/cvmfs-prepub/published/550e8400-.../job.json | jq .provenance
# → { "verified": true, "oidc_issuer": "...", "git_sha": "c4e9f2a", ... }

# 6. Confirm the git commit
git -C atlas-sw show c4e9f2a --show-signature
# → Author: atlas-releaser <rel@cern.ch>
# → gpg: Good signature from "ATLAS Release Team <atlas-rel@cern.ch>"
```

The chain from file to person is complete, every link independently verifiable,
and the Rekor Merkle proof means no party — including the `cvmfs-prepub` operators
themselves — can deny the publish occurred.

---


## 26. Recipe 6: Access Control and Build Authorisation

The pipeline's server-side authorisation block (identical to the existing
`cvmfs-publish.yml`) fetches `communities/<name>/ui-config.yaml` at runtime
via `CI_JOB_TOKEN`.  It computes the effective CVMFS publish path from the
caller's `GITLAB_USER_LOGIN`, which is injected by GitLab and cannot be
overridden by trigger callers:

| Caller identity | Publish target |
|---|---|
| Listed in `bits_admins:` or `admins:` | `cvmfs_prefix/<package>/<version>/…` (production) |
| Any other authenticated user | `cvmfs_user_prefix/<login>/<package>/<version>/…` (personal area) |

This path is passed in the `X-CVMFS-Path` header when submitting the job to
`cvmfs-prepub`.  The service trusts this header because the API token
(`PREPUB_API_TOKEN`) is a Protected+Masked CI variable accessible only to
runners executing pipelines from protected branches — it cannot be obtained by
fork or untrusted pipelines.


## 27. Recipe 7: Receiver Enrollment and Revocation

**Goal:** Provision a Stratum 1 receiver so it can authenticate to the
control-plane broker, and revoke a receiver immediately when needed. The full
key model and security rationale are in Chapter 8; this recipe is the operational
procedure.

### 27.1 Key Provisioning

On Stratum 0:

- generate an Ed25519 keypair — the publisher keeps the **private** key
  (`--discovery-signing-key`); the **public** key is distributed to receivers
  (`--discovery-verify-key`);
- export the master secret `PREPUB_HMAC_SECRET` (Stratum 0 only — receivers
  never hold it);
- for each receiver node `N`, compute its per-node key
  `PREPUB_NODE_KEY = HMAC-SHA256(master, N)` and hand it to that receiver over a
  separate channel;
- mint the broker server certificate and the CA; receivers mount only the CA
  cert (`--broker-ca-cert`) and the discovery public key.

The testbed `init.sh` performs all of this automatically. In production the
discovery private key and broker server key must be readable only by the
publisher process.

### 27.2 Enrollment Flow

Discovery is fetched over plain HTTP but **integrity-protected** by an Ed25519
signature; the bearer token is exchanged only over TLS.

```mermaid
sequenceDiagram
  autonumber
  participant R as Stratum 1 receiver
  participant D as S0 discovery (HTTP :8080)
  participant E as S0 control TLS (:8443)
  participant B as Embedded broker (wss :1882)
  R->>D: GET /cvmfs/{repo}/.cvmfsbits
  D-->>R: discovery doc + Ed25519 signature + control-plane URL + enroll URL
  R->>R: verify signature with discovery.pub  (mismatch => abort: possible MITM)
  R->>E: GET /control/challenge?node=R   [TLS]
  E-->>R: nonce = hex(ts || HMAC(serverKey, ts||node))   (stateless: nothing stored)
  R->>R: MAC = HMAC(nodeKey, node|nonce)
  R->>E: POST /control/enroll {node, nonce, MAC}   [TLS]
  E->>E: verify nonce freshness + MAC vs HMAC(master,node); one-time replay guard
  E-->>R: bearer token (scope "control", TTL 10m)
  R->>B: CONNECT wss, password=token; verify broker cert via CA
  B->>B: OnConnectAuthenticate: verify token -> node; bind node to the connection
  B-->>R: CONNACK; subscribe announce + published (ACL: read ok, write self only)
```

### 27.3 Revocation

Immediate cut-off combines a denylist (refuse future connects/enrollments) with
an active disconnect of any live session. Run:

```
prepub revoke <node> --enroll-url https://cvmfs-prepub:8443 --ca-cert ca.crt
```

```mermaid
sequenceDiagram
  autonumber
  participant Op as Operator: prepub revoke <node>
  participant E as S0 control TLS /control/revoke
  participant B as Broker
  participant R as Target receiver
  Op->>Op: mint publisher admin token from PREPUB_HMAC_SECRET
  Op->>E: POST /control/revoke {node} + Bearer admin token   [TLS]
  E->>E: verify token AND token.node == "publisher"  (else 403)
  E->>E: denylist.add(node)
  E->>B: DisconnectClient(live sessions for node)
  B--xR: connection closed
  R->>B: reconnect (token)
  B--xR: refused (node on denylist)
```


## 28. Infrastructure Requirements Checklist

Use this checklist before deploying.  Full detail is in Chapter 34.

**Publisher (Stratum 0):**
- [ ] Recent Go toolchain on the Stratum 0 node
- [ ] `cvmfs_gateway` ≥ 1.2 with the lease-and-payload API
- [ ] Write access to the CAS backend (local filesystem or S3-compatible)
- [ ] Outbound HTTPS from the publisher to the gateway

**Pull pre-warming (add Stratum 1 receivers):**
- [ ] Receiver binary deployed on each Stratum 1 (or a shared S3 object store)
- [ ] Outbound reachability from each receiver to the publisher's broker (`wss`),
      enroll endpoint (HTTPS), discovery endpoint, and object store — no inbound
      ports required at Stratum 1
- [ ] `PREPUB_HMAC_SECRET` (master) on the publisher **only**; per-node
      `PREPUB_NODE_KEY` provisioned to each receiver (Recipe 7)
- [ ] Ed25519 discovery keypair (private on publisher, public on receivers) and
      broker server TLS cert/key + CA
- [ ] CAS root directory on each receiver with enough headroom (default 1.2×
      estimated payload)

**Provenance / Rekor (optional):**
- [ ] Rekor server reachable from the publisher (default `https://rekor.sigstore.dev`)
- [ ] CI system issues OIDC tokens (GitHub Actions or GitLab CI)
- [ ] `--provenance` enabled and `--oidc-issuers` set


# PART V — REFERENCE

> **Who should read this part:** Anyone who needs exhaustive specification detail —
> every configuration key, every HTTP endpoint, every protocol field, every
> security guarantee.  This is the authoritative reference; the Cookbook points
> here for full tables.

---

## 29. Configuration Reference — Publisher

```yaml
# /etc/cvmfs-prepub/config.yaml

server:
  listen: ":8080"
  tls_cert: /etc/cvmfs-prepub/tls/server.crt
  tls_key:  /etc/cvmfs-prepub/tls/server.key
  # API auth: bearer token from PREPUB_API_TOKEN (or --api-token); empty = dev mode

spool_root: /var/spool/cvmfs-prepub

gateway:
  url: http://localhost:4929
  key_id: prepub-key-001
  key_secret_env: CVMFS_GATEWAY_SECRET
  lease_ttl: 120s
  heartbeat_interval: 40s     # = lease_ttl / 3

cas:
  type: s3                    # or: localfs, multi
  bucket: cvmfs-cas-primary
  region: us-east-1
  endpoint: ""                # override for MinIO/GCS

pipeline:
  workers: 0                  # 0 = runtime.NumCPU()
  compression: zlib           # zlib only; level via --pipeline-compress-level (default 6)
  upload_concurrency: 4

chunking:                     # CVMFS content-defined (xor32) chunk sizes; bytes
  min: 4194304                # 4 MiB  (--chunk-min)
  avg: 8388608                # 8 MiB  (--chunk-avg); 0 disables chunking
  max: 16777216               # 16 MiB (--chunk-max)

# Distribution (pull control plane) is configured via CLI flags / env, not a
# distribution: block — see "Pull control plane flags" below and Chapter 8.

repositories:
  - name: atlas.cern.ch
    gc:
      enabled: true
      access_ttl: 180d
      soft_delete_period: 14d
      schedule: "0 2 * * 0"
      dry_run: false
      exempt_tags: [stable, lts-2024]
      exempt_paths: ["/cvmfs/atlas.cern.ch/releases/latest"]
      max_deletion_rate: 100

  - name: cms.cern.ch
    gc:
      enabled: false           # GC disabled for this repo

access_sources:
  - type: log_file
    name: stratum1-cern
    path: /var/log/nginx/cvmfs-stratum1-access.log
    field_map:
      hash:      'data/\w{2}/([0-9a-f]{40})'
      repo:      'cvmfs/([^/\s]+)'
      timestamp: '\[([^\]]+)\]'
    time_format: "02/Jan/2006:15:04:05 -0700"

  - type: http_receiver
    name: external
    listen: ":9200"
    auth_token_env: CVMFS_ACCESS_TOKEN

  - type: cas_backend
    name: s3-primary

  - type: publish_events
```

**Pull control plane and security flags** (Chapter 8).
On Stratum 0 the publisher runs an in-process MQTT broker, serves a signed
discovery document, and serves manifests/objects for receivers to *pull*:

| CLI flag | Default | Notes |
|---|---|---|
| `--embedded-broker-ws-addr` | `` | Run an in-process MQTT broker over WebSocket on Stratum 0 (e.g. `:1882`); empty disables the embedded broker |
| `--control-plane-url` | `` | Broker URL advertised to receivers via discovery (e.g. `wss://cvmfs-prepub:1882`) |
| `--embedded-broker-tls-cert` | — | PEM server certificate for the embedded broker; enables `wss://` |
| `--embedded-broker-tls-key` | — | PEM server private key for the embedded broker |
| `--broker-ca-cert` | — | PEM CA certificate used to verify the broker (set on publisher and receivers) |
| `--embedded-broker-auth` | `false` | Require token authentication on the broker (challenge/response enrollment) |
| `--enroll-tls-addr` | `` | HTTPS listener serving challenge / enroll / revoke (e.g. `:8443`), so the bearer token never travels in plaintext |
| `--enroll-url` | `` | HTTPS enroll endpoint advertised to receivers via discovery (e.g. `https://cvmfs-prepub:8443`) |
| `--discovery-signing-key` | — | PEM Ed25519 **private** key used to sign the discovery document |
| `--pull-object-base-url` | `` | Object base URL embedded in pull manifests (e.g. `http://cvmfs-prepub:8080`) |

The publisher master secret is read from the **`PREPUB_HMAC_SECRET`**
environment variable (**Stratum 0 only**); it mints/verifies tokens and derives
the per-node keys handed to receivers. See §8.2 and Recipe 7 for the full key
model and provisioning.

**Admin subcommand.** Revoke a receiver node — denylist it for future
enrollments/connects and disconnect any live session:

```
prepub revoke <node> [--enroll-url https://cvmfs-prepub:8443] [--ca-cert ca.crt]
```

The receiver-mode flags are documented in Chapter 30.

---


## 30. Configuration Reference — Receiver

**Receiver** — started with `cvmfs-prepub --mode receiver`. The receiver is a
**pull** client of the Stratum 0 broker and object store; it opens only outbound
connections and needs no inbound ports.

| CLI flag | Default | Notes |
|---|---|---|
| `--discovery-url` | `` | Fixed Stratum-0 endpoint serving the signed discovery document; the receiver fetches `GET {url}/cvmfs/{repo}/.cvmfsbits` to learn its broker and enroll URLs |
| `--discovery-verify-key` | — | PEM Ed25519 **public** key used to verify the signed discovery document (required under `--broker-auth`) |
| `--broker-auth` | `false` | Enroll via challenge/response and authenticate to the broker with a scoped bearer token instead of connecting anonymously |
| `--broker-ca-cert` | — | PEM CA certificate used to verify the broker and enroll endpoint server certificate; empty uses the system pool |
| `--receiver-stratum0-url` | `` | Stratum-0 base URL the receiver pulls manifests and objects from on commit notification |
| `--repos` | `` | Comma-separated list of CVMFS repositories this receiver follows (e.g. `atlas.cern.ch,cms.cern.ch`) |
| `--node-id` | hostname | Stable identifier for this receiver node |
| `--cas-root` | `/var/lib/cvmfs-prepub/cas` | Local CAS root directory (e.g. `/srv/cvmfs/stratum1/cas`) |
| `--pull-concurrency` | `16` (`0`=default) | Number of parallel object transfers / bundle requests |
| `--pull-files-per-request` | `1` | Objects per request: `1` = one GET per object; `>1` = chunked bundles requested via `POST /s1/bundle` |
| `--pull-auto` | `false` | Measure RTT to Stratum 0 at startup and auto-pick `--pull-concurrency` / `--pull-files-per-request` from a latency class; any explicitly set flag overrides the auto choice |
| `--dev` | `false` | Development mode: relaxes security checks (never use in production) |
| `--log-level` | `info` | Log level: `debug`, `info`, `warn`, `error` |

The per-node enrollment key is read from the **`PREPUB_NODE_KEY`** environment
variable (hex-encoded `HMAC-SHA256(master, node)`, provisioned on Stratum 0 and
handed to the receiver out of band — see Recipe 7). **Receivers hold no master
secret.**


## 31. Pull Distribution Protocol

This chapter is the field-level specification for the pull-based distribution
path coordinated over the embedded MQTT-over-WebSocket/TLS control plane. The
architectural overview is in Chapter 8; the enrollment and revocation procedure
is in Recipe 7. The control plane carries small coordination messages; the data
plane is ordinary content-addressed HTTP GETs served from the publisher's object
store (or a CDN in front of it).

### 31.1 Endpoints (Stratum 0)

| Method / Path | Purpose |
|---|---|
| `GET /cvmfs/{repo}/.cvmfsbits` | Signed discovery document: control-plane (broker) URL, enroll URL, object base URL(s); Ed25519 signature |
| `GET /control/challenge?node={node}` (TLS :8443) | Returns a stateless enrollment nonce |
| `POST /control/enroll` (TLS :8443) | Redeems a nonce + per-node MAC; returns a scoped bearer token |
| `POST /control/revoke` (TLS :8443) | Denylists a node and disconnects live sessions (publisher-scoped admin token only) |
| `wss://...:1882` | Embedded MQTT broker: `announce` / `published` / `ready` / `presence` |
| `GET /s1/{txn}/manifest` | Per-transaction manifest: object hash list + object base URL(s) |
| `GET .../cvmfs/{repo}/data/{ab}/{rest}` | Content-addressed object fetch (one per object) |
| `POST /s1/bundle` | Optional K-object bundle fetch: `{"repo":"...","hashes":["...",...]}` |
| `GET /s1/catchup?repo={repo}&to={root}[&from={root}]` | Manifest for a late/recovering receiver to reconcile to a given root |
| `POST /s1/{txn}/lease` | Admission control for a receiver's pull of a transaction |

### 31.2 Discovery Document

A receiver's only required configuration is the fixed discovery URL plus the
Ed25519 verification key. The receiver fetches `GET /cvmfs/{repo}/.cvmfsbits`
(plain HTTP is acceptable because the document is **integrity-protected** by an
Ed25519 detached signature) and verifies it with `--discovery-verify-key`. A
verification failure aborts startup (possible MITM). The document advertises:

- the control-plane (broker) URL, with transport `mqtt` over `wss://`;
- the enroll URL (HTTPS, e.g. `https://cvmfs-prepub:8443`);
- the object base URL(s) receivers pull from.

The publisher signs the document with its Ed25519 private key
(`--discovery-signing-key`). A symmetric signer exists only as a development
fallback when no Ed25519 key is configured; production deployments use Ed25519
and receivers verify with the public key only, so no receiver holds a key capable
of forging a discovery document.

### 31.3 Enrollment and Broker Authentication

Enrollment proves possession of the per-node key
`PREPUB_NODE_KEY = HMAC-SHA256(master, node)` without transmitting it, and is
carried entirely over TLS so the issued bearer token is never exposed:

1. `GET /control/challenge?node={node}` returns a **stateless** nonce
   `hex(ts || HMAC(serverKey, ts||node))` -- the server stores nothing.
2. The receiver computes `MAC = HMAC(nodeKey, node|nonce)` and calls
   `POST /control/enroll {node, nonce, MAC}`. The server checks nonce freshness
   and verifies the MAC against `HMAC(master, node)`, with a one-time replay
   guard on the redeemed nonce.
3. On success the server issues a self-verifying, scoped, TTL-bounded HMAC bearer
   token (scope `control`, TTL ~10 minutes).
4. The receiver connects to the broker over `wss://` with the token as the MQTT
   CONNECT password and verifies the broker certificate against the CA. A
   credentials provider re-supplies the token on every reconnect, so short token
   lifetimes need no manual rotation.

The broker's auth hook records the token-verified node on the connection and
enforces a role ACL on every publish (see Sec. 31.6).

### 31.4 Control-Plane Topics and Messages

```
cvmfs/repos/{repo}/announce
    Publisher -> receivers. Payload: AnnounceMessage (JSON). Pre-commit pre-warm.

cvmfs/repos/{repo}/published
    Publisher -> receivers. Payload: PublishedMessage (JSON). Retained, so a
    late-connecting receiver still catches up.

cvmfs/receivers/{node_id}/presence
    Receiver -> observers. Payload: PresenceMessage (JSON). Retained; LWT
    publishes the same topic with online=false on unexpected disconnect.

cvmfs/publishers/{publisher_id}/ready/{txn}/{node_id}
    Receiver -> specific publisher. Payload: ReadyMessage (JSON).
```

Topic path components (`{repo}`, `{node_id}`, `{publisher_id}`, `{txn}`) are
validated at construction time to reject characters with special MQTT meaning
(`/`, `+`, `#`, NUL) and empty strings, preventing topic-injection.

**AnnounceMessage** (publisher -> receivers):

```json
{
  "txn":          "<transaction id>",
  "publisher_id": "pub-<id>",
  "repo":         "atlas.cern.ch",
  "hashes":       ["<hash>", "..."],
  "total_bytes":  1234567
}
```

`hashes` is the complete object list for the transaction. The receiver computes
the subset it does **not** already hold by calling `CAS.Exists()` (`os.Stat` /
S3 `HEAD`) on each hash directly -- there is no inventory filter and no separate
inventory-fetch round-trip. A receiver rejects an announce with more than
1 000 000 hashes, or any individual hash string longer than 256 bytes, as a
protocol error.

**ReadyMessage** (receiver -> publisher):

```json
{
  "node_id": "stratum1-cern",
  "txn":     "<transaction id>",
  "error":   ""
}
```

### 31.5 Pull Flow

```
Publisher                         Broker (wss)                  Receiver
    |                                  |                            |
    |-- publish announce(repo,txn) --->|---- deliver -------------->|
    |                                  |                            | CAS.Exists per hash
    |                                  |                            | -> absent set
    |                                  |                            |
    |                                  |            GET /s1/{txn}/manifest (S0)
    |                                  |<---------------------------|
    |                                  |            pull absent objects from
    |                                  |            object base URL (verify each hash)
    |-- commit via cvmfs_gateway       |                            |
    |-- publish published(repo,txn,root) [retained] -------------->|
    |                                  |            pull any remaining objects
    |                                  |            warm (atomic local catalog flip)
    |<-- ready / presence -------------|<---------------------------|
    | quorum of ready reached -> txn done
```

The per-transaction manifest is **provisional** (its root hash is a placeholder
until commit); because every object is content-addressed, receivers can pre-pull
before the catalog flips. The post-commit `published` message is **retained** on
the broker, so a receiver that connects late reads it on subscribe and catches up
via `/s1/catchup`.

### 31.6 Object Pull and Bundling

The data plane is ordinary HTTP. For each hash in its absent set the receiver
performs `GET .../data/{hash[:2]}/{hash[2:]}` against the object base URL and
verifies the object by recomputing its hash on arrival. Transfers run in parallel
up to `--pull-concurrency` (default 16).

To amortise round-trip latency on high-RTT links, the receiver can request a
**bundle** of K objects in one call with `POST /s1/bundle` (`{"repo":...,
"hashes":[...]}`); `--pull-files-per-request` > 1 enables this. `--pull-auto`
measures RTT to Stratum 0 at startup and picks `--pull-concurrency` /
`--pull-files-per-request` from a latency class; any explicitly set flag overrides
the auto choice.

Because objects are static, content-addressed HTTP, the object base URL can point
at a CDN or the CVMFS web tier rather than the publisher itself; advertise it in
the manifest `base_urls` (see Sec. 8.5).

### 31.7 Missing-Set Computation (Direct CAS)

There is **no inventory-filter endpoint** -- the receiver advertises nothing for
the publisher to subtract against, and there is no Bloom filter. The per-receiver
delta is computed from the announce (or pull manifest) itself: the receiver walks
the announced hash list and, for each hash, calls `CAS.Exists()` -- an `os.Stat`
for the local-filesystem backend or a single `HEAD` for S3, via the
`cas.NativeExistsChecker` interface. The hashes that return `false` form the
absent set it must pull. This is exact (no false positives, no false negatives),
requires no startup CAS walk to populate any in-memory structure, and carries no
saturation or sizing concern at scale. The cost is one cheap local existence
check per announced object, parallelised across the receiver's worker pool.

```
1.  receive announce(repo, txn, candidate_hashes)   // or read pull manifest
2.  absent = [h for h in candidate_hashes if not CAS.Exists(h)]   // os.Stat / S3 HEAD
3.  pull each h in absent from the object base URL (verify each by hash)
```

### 31.8 Presence and Liveness

On connect each receiver publishes a **retained** `PresenceMessage` with
`online=true` and configures a Last-Will-and-Testament (LWT) on the same topic
with `online=false`. If a receiver disconnects unexpectedly the broker publishes
the LWT automatically, so the publisher and monitoring tools see the node go
offline without any application-level coordination. On reconnect the auto-reconnect
loop re-establishes subscriptions (clean-session disabled) and re-publishes the
retained `online=true` presence, overwriting the LWT.

### 31.9 Quorum

The publisher collects `ReadyMessage`s until either all expected receivers reply
or a quorum timeout fires; it then treats the transaction as warm. A lagging or
absent receiver does not block the release indefinitely -- it warms later from the
retained `published` message and `/s1/catchup`.

### 31.10 Prometheus Metrics

The receiver serves Prometheus metrics at `GET /metrics`:

| Metric | Type | Description |
|---|---|---|
| `cvmfs_receiver_objects_received_total` | Counter | CAS objects stored by the receiver |
| `cvmfs_receiver_bytes_received_total` | Counter | Compressed bytes stored (on-wire size) |

The `cvmfs_prepub_*` publisher metrics (jobs, pipeline, CAS upload durations) are
served at the publisher's metrics endpoint (Sec. 35.4).

## 32. Security Reference

This section provides exhaustive detail on every security control.  See §14
(Security Architecture Overview in Part II) for the high-level trust chain, and
Chapter 8 for the pull control-plane security model.

### 32.1 Build Environment Isolation

Each experiment group's build runs in an isolated environment (OCI container,
virtual machine, or bare-metal job with a locked-down user account) so that a
compromised build of one group cannot access the source trees, credentials, or
signing keys of another group.  The `bits` tool is invoked inside this
environment and has access only to:

- The source tree for the current build.
- The `PREPUB_API_TOKEN` for its own sub-path.
- The `CVMFS_GATEWAY_SECRET` for its own key-ID.

Neither the gateway's private signing key nor the secrets of other groups are
present in the build environment.

### 32.2 Tar Signing and Integrity at Handoff

Before `bits` submits the tar to `cvmfs-prepub`, it computes a SHA-256 digest
of the archive and signs the digest with the build node's Ed25519 private key.
The signature and the signer's public key fingerprint are included in the
multipart submission as separate form fields.

`cvmfs-prepub` verifies the signature before accepting the job:

1. Retrieve the public key for the claimed key fingerprint from the trusted
   key store (a local directory populated by the operator, or a KMS).
2. Verify the Ed25519 signature over the tar digest.
3. Reject the submission with `400 Bad Request` if the signature is invalid or
   the key is unknown.

This ensures that a tar delivered over the network (even over HTTPS) cannot be
silently tampered with in transit, and that only authorised build nodes can
submit jobs.

### 32.3 Transport Security

All control-plane and publishing traffic uses TLS 1.2 or higher:

- `bits` → `cvmfs-prepub`: HTTPS (enforced; HTTP submissions rejected unless
  `--dev` is set).
- `cvmfs-prepub` → `cvmfs_gateway`: HTTPS, `MinVersion: tls.VersionTLS12`
  enforced in the lease client transport.
- Control plane: the broker listens on `wss://` (TLS) and enrollment/revocation
  on a dedicated HTTPS listener; both ends verify the server certificate against
  the provisioned CA (Chapter 8).
- Stratum 1 → edge workers: HTTPS via the CVMFS client; certificate pinned to
  the repository whitelist.

Object pulls are content-addressed and hash-verified on arrival, so the object
base URL may be plain HTTP (as CVMFS already distributes objects to clients):
substituted bytes change the hash and are rejected; dropped bytes trigger a
re-pull.

Private key material (gateway secret, API tokens, the master `PREPUB_HMAC_SECRET`,
per-node keys, the discovery private key, build-node signing keys) is never
transmitted over the wire and is never stored in the source tree or config files;
it is injected exclusively via environment variables or a secrets manager.

### 32.4 CAS Integrity — Content Addressing

Every object stored in the CAS is keyed by the SHA-1 of its zlib-compressed bytes
(the CVMFS CAS convention — see §9.2). The upload stage computes the key before
writing and the pull path verifies each object by recomputing its hash. An
attacker who gains write access to the CAS backend and modifies an object's bytes
changes its effective hash, so the modified object no longer matches the hash
referenced by the signed catalog and is rejected. The gateway includes object
hashes in the signed manifest, so clients independently verify each file.

### 32.5 Gateway Lease Scoping and Manifest Signing

The gateway issues leases scoped to a repository sub-path and a key-ID. The
HMAC signature on every gateway API request covers:

```
method + requestURI + bodyHash + unixTimestamp
```

This prevents cross-verb replay, cross-path replay, body substitution, and
replay beyond the timestamp window. After a successful commit, the gateway signs
the updated catalog tree with the repository's signing key and updates the signed
manifest. CVMFS clients verify this signature on every catalog fetch, rejecting
any manifest not signed by the known repository key.

### 32.6 Control-Plane and Distribution Security

The pull control plane's security model — Ed25519-signed discovery, per-node
challenge/response enrollment, scoped bearer tokens, the broker role ACL,
stateless DoS-resistant challenges, rate limiting, and revocation — is specified
in §8.4 (model) and Chapter 31 (protocol). In summary:

- **Discovery integrity (Ed25519).** Receivers verify the discovery document
  with the publisher's Ed25519 public key only; they hold no key able to forge a
  discovery document.
- **Authentication.** Per-node challenge/response proves possession of the
  per-node key without transmitting it; the server issues a self-verifying,
  scoped, TTL-bounded HMAC token presented as the broker password.
- **Authorization (role ACL).** The broker records the token-verified node on the
  connection and enforces a publish ACL: a receiver may write only its own
  `ready`/`presence` and may not forge `announce`/`published`.
- **Input bounds.** A receiver rejects announces with more than 1 000 000 hashes,
  or any hash string longer than 256 bytes, preventing large allocations.
- **Topic-injection prevention.** Topic path components are validated to reject
  MQTT-special characters and empty strings.
- **Revocation.** A shared denylist plus active disconnect gives immediate
  cut-off; access also lapses by attrition within one token TTL.

### 32.7 Client Verification

CVMFS clients receive the repository whitelist (a signed list of trusted
Stratum 1 hostnames and their TLS certificate fingerprints) from the Stratum 0
gateway.  On every catalog fetch, the client:

1. Verifies the TLS certificate of the Stratum 1 against the whitelist.
2. Verifies the Ed25519 signature on the downloaded manifest.
3. Rejects the catalog if either check fails.

This means a rogue Stratum 1 (or a MITM between client and Stratum 1) cannot
serve a modified software tree without being detected.

### 32.8 Confidentiality

Source code and build artefacts are not stored in the CAS in plain text; they
are compressed (zlib) before storage but not encrypted.  If the CAS backend is
a shared filesystem or object store, access to the raw objects must be
controlled by filesystem permissions (`0600` for local CAS files) or bucket
policies (private bucket, IAM-scoped read for the prepub service account only).

Build logs, job manifests, and the upload log are stored under
`/var/spool/cvmfs-prepub/` with permissions `0700` (directory) and `0600`
(files), preventing other local users from reading job history or inferring
which objects were published.

Software packages published to CVMFS are by design publicly readable by edge
workers.  If a repository contains proprietary or embargoed packages, the
repository should be configured with CVMFS client authentication tokens (the
standard CVMFS membership mechanism), which is outside the scope of
`cvmfs-prepub` but compatible with it.

### 32.9 Audit Trail and Traceability

Every publish leaves a complete, immutable audit trail that links the build
artefact to the final CVMFS manifest revision:

```
bits build_id  (e.g. CI pipeline run ID, git commit SHA)
    │  recorded in the job submission body
    ▼
cvmfs-prepub job_id  (UUID, in job manifest at spool/published/{id}/manifest.json)
    │
    ├──▶  upload log  (spool/published/{id}/upload.log)
    │        JSON-per-line: { hash, time } for every CAS object written
    │
    ├──▶  distribution log  (spool/published/{id}/dist.log)
    │        JSON-per-line: per-receiver warm/ready state for the transaction
    │
    └──▶  gateway commit response
             catalog_hash   (content hash of the signed catalog)
             manifest_revision  (monotonically increasing integer)
             commit_time
```

All fields are written to the job manifest (`manifest.json`) under
`/var/spool/cvmfs-prepub/published/{job_id}/`.  The manifest is immutable after
the job reaches `published` state.

To answer the question "which files were added by build X?", an operator can:

1. Look up the `job_id` for `build_id` X in the job manifest index.
2. Read the `upload.log` for that job to get the list of CAS object hashes.
3. Map hashes back to file paths using the catalog (or the job's staging
   directory if it has not been GC'd).
4. Verify the catalog hash against the gateway's manifest API.

For regulated environments, the job manifest directory can be exported to an
append-only audit store (e.g. an S3 bucket with Object Lock, or a Merkle-tree
audit log) by a post-publish hook.  This provides a tamper-evident record that
satisfies both internal governance requirements and external audit obligations.

---


## 33. Provenance Reference and Verification

This section covers implementation details, OIDC validation, Rekor
non-repudiation, and the offline verification workflow.  See §13
(Provenance Architecture in Part II) for the four-layer design.
See Recipe 5 (Chapter 25) for step-by-step setup.

### 33.1 How the implementation works

#### Submission time (HTTP handler → job manifest)

When a CI job POSTs to `POST /api/v1/jobs`, the server inspects two header sources:

| Source | When used | Trust level |
|---|---|---|
| `X-Provenance-*` headers | Always | Unverified (Verified=false) — accepted as caller-supplied metadata |
| `X-OIDC-Token` header (or Bearer JWT with 3 dots) | When `--oidc-issuers` is configured | Verified — OIDC issuer's JWKS validates the token signature |

When OIDC validation succeeds the token's claims (`repository`, `sha`, `actor`,
`run_id`, …) **override** the plain headers and the manifest is written with
`verified: true`.  A verified claim cannot be forged by the submitter — it is
cryptographically attested by the CI provider (GitHub Actions or GitLab CI).

The provenance fields are stored in the job manifest (`job.json`) immediately at
submission and persist through the full job lifecycle.

#### Publish time (orchestrator → Rekor)

After a job reaches `published` state the orchestrator calls `Provenance.Submit`:

1. The `Record` is serialised to JSON and signed with the node's Ed25519 key
   (auto-generated at `{spoolDir}/provenance.key`, mode 0600, on first run).
2. The signed payload is submitted to Rekor as a `hashedrekord` entry that binds
   each object hash and the catalog hash to the job manifest fields.
3. Rekor returns a UUID, log index, integrated time, and Signed Entry Timestamp
   (SET — a log-level signature over the Merkle tree node).
4. The receipt is persisted back into the job manifest:

```json
{
  "provenance": {
    "git_repo":    "atlas-release/atlas-sw",
    "git_sha":     "c4e9f2a…",
    "git_ref":     "refs/heads/main",
    "actor":       "atlas-releaser",
    "pipeline_id": "14280931847",
    "build_system": "github-actions",
    "oidc_issuer": "https://token.actions.githubusercontent.com",
    "oidc_subject": "repo:atlas-release/atlas-sw:ref:refs/heads/main",
    "verified":    true,
    "rekor_server":          "https://rekor.sigstore.dev",
    "rekor_uuid":            "24296fb24b3d…",
    "rekor_log_index":       183047291,
    "rekor_integrated_time": 1745488353,
    "rekor_set":             "MEUCIQDr…"
  }
}
```

If Rekor submission fails (network outage, rate limit), the publish is **not**
aborted — the failure is logged at Warn and the job is marked published without a
receipt.  This is a deliberate trade-off: Rekor is an audit mechanism, not a gating
dependency.

---

### 33.2 Rekor and non-repudiation

Rekor is Sigstore's append-only transparency log, backed by a Merkle tree whose root
hash is periodically published to a Certificate Transparency-style checkpoint.
Properties that make it suitable for non-repudiation:

| Property | Detail |
|---|---|
| **Append-only** | Entries cannot be deleted or modified — only appended |
| **Merkle inclusion proof** | Any auditor can confirm an entry's position in the log without trusting Rekor's responses |
| **Signed Entry Timestamp (SET)** | A signature over the Merkle node, verifiable offline with Rekor's public key (`rekor-cli verify`) |
| **Idempotent** | Submitting an identical entry returns the existing UUID (409 Conflict), preventing duplicate log pollution |
| **Public by default** | The public instance at `https://rekor.sigstore.dev` is queryable by anyone |
| **Self-hostable** | Set `--rekor-server` to an internal URL for air-gapped or regulated deployments |

An entry stored in Rekor cannot be repudiated by the publisher: even if the
`cvmfs-prepub` service is destroyed, the job manifest deleted, and the build node
re-imaged, the Rekor entry survives and its SET remains verifiable against Rekor's
published public key.

---

### 33.3 CI OIDC token validation

Modern CI platforms issue short-lived OpenID Connect (OIDC) JWTs for each pipeline
run.  These tokens are:

- **Signed** by the provider's private key and verifiable via the published JWKS
  (`GET /.well-known/openid-configuration` → `jwks_uri`).
- **Short-lived** (typically 5–10 minutes), so a leaked token is useless after the
  pipeline run.
- **Scoped** to the specific repository, ref, and run, making forgery by a different
  pipeline impossible even if the token is intercepted.

`cvmfs-prepub` performs the validation inline at request time:

```
X-OIDC-Token: eyJhbGciOiJSUzI1NiIsImtpZCI6IjE4M...
```

```
1. Decode JWT payload (without signature) → extract "iss" claim
2. Reject issuer if not in --oidc-issuers allowlist
3. Fetch /.well-known/openid-configuration for the issuer
4. Fetch JWKS from jwks_uri, locate key by "kid"
5. Verify JWT signature against the fetched public key
6. Verify exp, nbf standard claims
7. Extract repository, sha, actor, run_id, ref claims
8. Set verified=true in the job manifest
```

Steps 3–5 make outbound HTTPS calls to the CI provider.  The `--provenance-http-timeout`
(default 20 s) caps each call independently so a slow JWKS endpoint does not stall
the publish pipeline.

Supported CI providers out of the box: **GitHub Actions** and **GitLab CI** (SaaS
and self-hosted).  Other OIDC-compliant providers work if their issuer URL is added
to `--oidc-issuers` and their tokens follow the standard claims structure.

---

### 33.4 Relationship to the internal audit trail

§32.9 documents the *audit trail* features built into the job spool (journal,
manifest, upload log).  Provenance (this chapter) extends that audit trail with
two additional properties the internal trail cannot provide alone:

| Property | Internal audit trail (§32.9) | Rekor + OIDC (this chapter) |
|---|---|---|
| Tamper evidence | Manifest is on local disk — mutable by root | Rekor entry is in an append-only public log |
| External verifiability | Requires access to the spool | Anyone with the file hash can verify via Rekor |
| Identity attestation | `actor` is caller-supplied (Verified=false by default) | `verified=true` means CI provider cryptographically attests identity |
| Survives system destruction | Spool deleted → audit trail lost | Rekor entry persists independently |

Both mechanisms are complementary: the spool gives fine-grained operational
visibility; Rekor gives the durable, externally-auditable receipt.

---


## 34. Infrastructure Requirements Detail

### 34.1 Publisher (Stratum 0)

The publisher is a first-class client of the existing `cvmfs_gateway`
lease-and-payload API (`POST /api/v1/leases`, `PUT /api/v1/leases/{token}`,
`POST /api/v1/payloads`); gateway ≥ 1.2 is sufficient and no gateway
modifications are required. It needs:

- a recent Go toolchain to build (or a prebuilt binary);
- write access to the CAS backend (local filesystem or S3-compatible);
- network reach to the gateway over HTTPS.

The node may be the Stratum 0 host or a separate build node. The signed manifest
format, catalog schema, and CAS object layout are identical to those produced by
`cvmfs_server publish`, so Stratum 1 replication, proxy caching, and CVMFS client
verification are unaware that a different publishing path was used (see
Chapter 18).

### 34.2 Stratum 1 Receivers (Pull Pre-Warming)

To pre-warm before the catalog flip, each Stratum 1 runs `cvmfs-prepub --mode
receiver`. The receiver opens only **outbound** connections — to the publisher's
broker (`wss`), enroll endpoint (HTTPS), discovery endpoint, and object store —
so no inbound firewall changes are needed at Stratum 1. It places pulled objects
in the local CAS directory using the standard CVMFS on-disk layout; the existing
`cvmfs_server snapshot` daemon is unchanged. See Recipe 2 (Chapter 22) and
Chapter 8 for configuration; Recipe 7 (Chapter 27) for key provisioning.

If all Stratum 1s share an S3-compatible object store, a single CAS upload on the
publisher suffices and no receiver is needed.

### 34.3 Security Considerations

**Authentication and authorisation.** The publish API requires a bearer token
(`PREPUB_API_TOKEN`); an empty token enables dev mode. The pull control plane
authenticates receivers with per-node challenge/response enrollment and scoped
bearer tokens, and enforces a broker role ACL (Chapter 8, Chapter 31).

**Lease token storage.** The gateway lease token is written to
`leased/<jobID>/lease.json` with mode `0600`. It is never logged or emitted in
API responses.

**Object integrity.** Objects are content-addressed (SHA-1 of the compressed
bytes); the upload stage computes the key before writing and the pull path
verifies each object by recomputing its hash, so a tampered object no longer
matches the hash referenced by the signed catalog.

**Manifest signing.** The gateway signs the manifest with the repository's signing
key as part of the payload commit. The pre-publisher does not hold the private
key; it submits the catalog diff and the gateway performs signing.

**GC safety.** The reference counter is conservative: any catalog read error
aborts the GC run entirely. The TOCTOU guard (refcount re-check immediately before
CAS deletion) prevents races between a concurrent publish and a GC run that
overlapped during the tombstone window. GC and publish are also serialised at the
gateway lease level for any overlapping path.

**Audit trail.** Every publishing job and every GC run produces a signed JSONL
audit log retained in the published/gc-complete spool directory.

---


# PART VI — REST API REFERENCE

> **Who should read this part:** Operators and CI system integrators who submit
> jobs to cvmfs-prepub programmatically, or who want a precise reference for
> every endpoint.

---

## 35. cvmfs-prepub REST API Reference

This section is the authoritative specification for the HTTP API exposed by the
`cvmfs-prepub` publisher.  The API is implemented in `internal/api/server.go`.

### 35.1 Base URL and Authentication

The service listens on the address configured by `--listen` (default `:8080`).
TLS is strongly recommended in production; use a reverse proxy (nginx, Caddy) or
configure `--tls-cert` / `--tls-key` on the binary directly.

All endpoints under `/api/v1/jobs` require a bearer token:

```
Authorization: Bearer <PREPUB_API_TOKEN>
```

The token is set via the `PREPUB_API_TOKEN` environment variable (or
`--api-token` flag).  If the token is empty the service runs in **dev mode** —
all authenticated routes accept unauthenticated requests.  Never deploy dev mode
in production.

Health and metrics endpoints are unauthenticated and can be served on a separate
internal port by a reverse proxy if needed.

### 35.2 Endpoint Summary

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/api/v1/health` | None | Liveness probe |
| `GET` | `/api/v1/metrics` | None | Prometheus metrics |
| `POST` | `/api/v1/jobs` | Required | Submit a new publish job |
| `GET` | `/api/v1/jobs/{id}` | Required | Get job status |
| `POST` | `/api/v1/jobs/{id}/abort` | Required | Request job abort |
| `GET` | `/api/v1/jobs/{id}/events` | Required | Subscribe to SSE event stream |

---

### 35.3 GET /api/v1/health

Returns a liveness response.  Use this for load-balancer health checks and
startup probes.  The check is intentionally lightweight — it does not probe the
gateway or CAS.

**Request:** `GET /api/v1/health`

No headers required.

**Response — 200 OK:**

```json
{"status":"healthy"}
```

`Content-Type: application/json`

---

### 35.4 GET /api/v1/metrics

Exposes Prometheus metrics in the standard text exposition format.  Mount this
at a scrape target in your Prometheus configuration.

**Request:** `GET /api/v1/metrics`

**Response — 200 OK:** Prometheus text format.

Key metrics exposed:

| Metric | Type | Description |
|---|---|---|
| `cvmfs_prepub_jobs_submitted_total` | Counter | Jobs accepted by the API |
| `cvmfs_prepub_jobs_published_total` | Counter | Jobs that reached `published` state |
| `cvmfs_prepub_jobs_failed_total` | Counter | Jobs that reached `failed` state |
| `cvmfs_prepub_pipeline_abort_total` | Counter | Jobs explicitly aborted |
| `cvmfs_prepub_pipeline_duration_seconds` | Histogram | End-to-end job duration |
| `cvmfs_prepub_cas_upload_duration_seconds` | Histogram | CAS object upload latency |
| `cvmfs_prepub_distribute_duration_seconds` | Histogram | Distribution (announce/warm) latency |

---

### 35.5 POST /api/v1/jobs

Submits a new publish job.  Returns immediately with a `job_id`; the job runs
asynchronously.  Use `GET /api/v1/jobs/{id}` or the SSE stream to track progress.

The endpoint supports two submission modes, selected by the request
`Content-Type`.

#### 35.5.1 Multipart upload (Content-Type: multipart/form-data)

Upload the tar file directly as a form field.  This is the standard mode for CI
pipelines.

**Request fields:**

| Field | Type | Required | Description |
|---|---|---|---|
| `repo` | string | Yes | Repository name (e.g. `atlas.cern.ch`) |
| `path` | string | No | Gateway lease sub-path (e.g. `atlas/24.0`). Empty = root lease. |
| `tar` | file | Yes | The tar archive to publish (binary). Maximum 10 GiB. |
| `tar_sha256` | string | No | Hex-encoded SHA-256 of the tar. If present, verified before the job starts. Mismatch → 400. |
| `webhook_url` | string | No | URL to POST when the job reaches a terminal state. |
| `tag_name` | string | No | Snapshot tag name to embed in the catalog commit. Must match `^[A-Za-z0-9._-]+$`, max 255 chars. Empty = no tag. |
| `tag_description` | string | No | Human-readable description for the snapshot tag. |

**Example:**

```sh
curl -sf -X POST https://prepub.example.com/api/v1/jobs \
  -H "Authorization: Bearer $PREPUB_API_TOKEN" \
  -F "repo=atlas.cern.ch" \
  -F "path=atlas/24.0" \
  -F "tar=@atlas-24.0.tar;type=application/octet-stream" \
  -F "tar_sha256=$(sha256sum atlas-24.0.tar | cut -d' ' -f1)" \
  -F "tag_name=atlas-24.0.0" \
  -F "tag_description=ATLAS 24.0.0 release"
```

#### 35.5.2 Staged tar reference (Content-Type: application/json)

Use this when the tar has already been transferred to the server's staging
directory (e.g. via rsync or a prior pipeline step).  Requires `--staging-root`
to be configured on the server; submissions are rejected if it is not set.

**Request body:**

```json
{
  "repo":            "atlas.cern.ch",
  "path":            "atlas/24.0",
  "tar_path":        "/staging/atlas/atlas-24.0.tar",
  "tar_sha256":      "e3b0c44298fc1c...",
  "webhook_url":     "https://ci.example.com/hooks/cvmfs",
  "tag_name":        "atlas-24.0.0",
  "tag_description": "ATLAS 24.0.0 release"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `repo` | string | Yes | Repository name |
| `path` | string | No | Gateway lease sub-path |
| `tar_path` | string | Yes | Absolute path to the tar file; must be inside `--staging-root` |
| `tar_sha256` | string | Yes | Hex SHA-256 of the tar (mandatory in this mode; verified before moving to spool) |
| `webhook_url` | string | No | Webhook URL for terminal-state notification |
| `tag_name` | string | No | Snapshot tag name (same rules as multipart mode) |
| `tag_description` | string | No | Human-readable tag description |

The server validates that `tar_path` is within the configured `--staging-root`
(directory traversal attempts are rejected with `400`) and moves or hard-links
the file into the spool atomically.

#### 35.5.3 Response

**202 Accepted:**

```json
{"job_id": "3f7a2b1c-0000-0000-0000-aabbccddeeff"}
```

The caller should poll `GET /api/v1/jobs/{id}` or subscribe to
`GET /api/v1/jobs/{id}/events` to track the job to completion.

#### 35.5.4 Error responses

| Status | Condition |
|---|---|
| `400 Bad Request` | Missing required field, invalid JSON, `tar_sha256` mismatch, invalid `tag_name`, or `tar_path` outside staging root |
| `401 Unauthorized` | Missing or invalid bearer token |
| `413 Request Entity Too Large` | Tar file exceeds 10 GiB |
| `503 Service Unavailable` | JSON/`tar_path` mode requested but `--staging-root` not configured |
| `500 Internal Server Error` | Spool directory creation or write failure |

---

### 35.6 GET /api/v1/jobs/{id}

Returns the current state of a job.

**Request:** `GET /api/v1/jobs/{id}`

`Authorization: Bearer <token>` required.

**Response — 200 OK:**

```json
{
  "job_id":             "3f7a2b1c-0000-0000-0000-aabbccddeeff",
  "state":              "published",
  "repo":               "atlas.cern.ch",
  "path":               "atlas/24.0",
  "n_objects":          14823,
  "n_bytes_raw":        4831838208,
  "n_bytes_compressed": 1923145728,
  "error":              "",
  "created_at":         "2025-05-01T12:00:00Z",
  "updated_at":         "2025-05-01T12:04:37Z"
}
```

| Field | Description |
|---|---|
| `job_id` | Unique identifier (UUID) assigned at submission |
| `state` | Current FSM state (see §35.6.1) |
| `repo` | Repository name |
| `path` | Gateway lease sub-path (empty = root) |
| `n_objects` | Number of CAS objects written (0 until pipeline completes) |
| `n_bytes_raw` | Uncompressed size of all objects in bytes |
| `n_bytes_compressed` | Compressed size of all objects as stored in the CAS |
| `error` | Non-empty only when `state` is `failed` |
| `created_at` | UTC timestamp of job submission |
| `updated_at` | UTC timestamp of the most recent state transition |

Fields with zero/empty values are omitted from the JSON response.

#### 35.6.1 Job states

| State | Terminal | Description |
|---|---|---|
| `queued` | No | Job accepted; waiting for a worker goroutine |
| `processing` | No | Pipeline running: unpacking, hashing, deduplicating, uploading |
| `distributing` | No | Announce published; Stratum 1 receivers pull and warm (when pull pre-warming is enabled) |
| `leased` | No | Gateway lease acquired; catalog merge in progress |
| `committing` | No | Payload submitted to gateway; waiting for commit acknowledgement |
| `published` | **Yes** | Catalog committed and manifest signed; job complete |
| `failed` | **Yes** | Unrecoverable error; `error` field contains details |
| `aborted` | **Yes** | Job was cancelled via `POST /api/v1/jobs/{id}/abort` |

#### 35.6.2 Error responses

| Status | Condition |
|---|---|
| `401 Unauthorized` | Missing or invalid bearer token |
| `404 Not Found` | No job with the given ID |
| `500 Internal Server Error` | Spool read failure |

---

### 35.7 POST /api/v1/jobs/{id}/abort

Requests cancellation of a running job.  The abort is **asynchronous**: the
endpoint signals the job's context and returns `202 Accepted`; the job
transitions to `aborted` when the running stage detects cancellation.

A job that has already reached a terminal state (`published`, `failed`,
`aborted`) cannot be aborted — the endpoint returns `409 Conflict`.

**Request:** `POST /api/v1/jobs/{id}/abort`

`Authorization: Bearer <token>` required.  No request body.

**Response — 202 Accepted:**

```json
{"status":"aborting"}
```

**Error responses:**

| Status | Condition |
|---|---|
| `401 Unauthorized` | Missing or invalid bearer token |
| `404 Not Found` | No job with the given ID |
| `409 Conflict` | Job is already in a terminal state, or completed between lookup and cancel (narrow race) |
| `500 Internal Server Error` | Spool read failure |

---

### 35.8 GET /api/v1/jobs/{id}/events

Streams state-change events as a **Server-Sent Events (SSE)** stream.  The
connection stays open until the job reaches a terminal state or the client
disconnects.  Use this instead of polling to receive real-time progress updates.

**Request:** `GET /api/v1/jobs/{id}/events`

```
Authorization: Bearer <token>
Accept: text/event-stream
```

**Response — 200 OK:**

```
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
X-Accel-Buffering: no
```

Events are emitted each time the job changes state.  Each event has the form:

```
event: state_change
data: {"job_id":"...","state":"...","time":"2025-05-01T12:01:00Z","error":""}

```

(Note the blank line terminator required by the SSE protocol.)

| Field | Description |
|---|---|
| `job_id` | The job UUID |
| `state` | New state (see §35.6.1) |
| `time` | UTC timestamp of the transition |
| `error` | Non-empty only when `state` is `failed` |

The stream closes automatically once a terminal state event is delivered.

**Example (curl):**

```sh
curl -sN \
  -H "Authorization: Bearer $PREPUB_API_TOKEN" \
  -H "Accept: text/event-stream" \
  https://prepub.example.com/api/v1/jobs/$JOB/events
```

**Error responses (before streaming begins):**

| Status | Condition |
|---|---|
| `401 Unauthorized` | Missing or invalid bearer token |
| `404 Not Found` | No job with the given ID |
| `500 Internal Server Error` | Server does not support streaming (should not occur) |

---

### 35.9 Webhook notifications

When `webhook_url` is set on a submitted job, the server POSTs a JSON payload
to that URL once the job reaches a terminal state (`published`, `failed`, or
`aborted`).  The webhook delivery is best-effort: failures are logged but do not
affect the job outcome.

**Webhook request (POST to webhook_url):**

```json
{
  "job_id":     "3f7a2b1c-0000-0000-0000-aabbccddeeff",
  "state":      "published",
  "repo":       "atlas.cern.ch",
  "path":       "atlas/24.0",
  "error":      "",
  "updated_at": "2025-05-01T12:04:37Z"
}
```

The receiving server should respond with any 2xx status.  Non-2xx responses are
logged at Warn and the webhook is not retried.

---

### 35.10 Provenance headers

When provenance tracking is enabled (`--oidc-issuers` or plain
`X-Provenance-*` headers), the submitter can attach build attribution metadata
to a job.  These fields are stored in the job manifest and optionally submitted
to the Rekor transparency log after publish (see Chapter 33).

**Unverified provenance (plain headers):**

```
X-Provenance-Git-Repo:    atlas-release/atlas-sw
X-Provenance-Git-SHA:     c4e9f2a...
X-Provenance-Git-Ref:     refs/heads/main
X-Provenance-Actor:       atlas-releaser
X-Provenance-Pipeline-ID: 14280931847
```

These headers are accepted as caller-supplied metadata (`verified: false`).

**Verified provenance (OIDC token):**

```
X-OIDC-Token: eyJhbGciOiJSUzI1NiI...
```

When `--oidc-issuers` is configured, the server validates the JWT against the
issuer's JWKS.  If validation succeeds, the token's claims override the plain
headers and the record is written with `verified: true`.  Supported providers:
GitHub Actions and GitLab CI (SaaS and self-hosted).  See Chapter 33 for details.

---

### 35.11 Size limits and timeouts

| Limit | Value | Notes |
|---|---|---|
| Maximum tar size (multipart) | 10 GiB | Configurable at compile time via `maxTarSize` |
| JSON request body | 1 MiB | Applied to `tar_path` mode requests |
| Tag name length | 255 chars | Enforced by `job.ValidateTagName` |
| Tag name charset | `^[A-Za-z0-9._-]+$` | Only letters, digits, dot, underscore, hyphen |
| Graceful shutdown wait | Context deadline | `Shutdown` waits for all in-flight jobs; context caps the wait |

---

# PART VII — ROADMAP

> **Who should read this part:** Site managers and project contributors planning
> phased adoption, or anyone tracking open design questions and future work.

---

## 36. Phased Deployment Plan

### Phase 0 — Foundation (prerequisite, no user-visible change)

| Item | Description |
|---|---|
| Repository scaffolding | Go module, CI pipeline, linting, basic test harness |
| CAS backend | Implement and test `S3Backend` and `LocalFSBackend` |
| Gateway client | Implement lease acquire/heartbeat/release/payload submit against a test gateway |
| Spool manager | Implement spool directory transitions with crash-recovery tests |
| Basic config loading | YAML config, validation, inotify watcher |

Exit criterion: a tar file can be unpacked, all objects hashed and uploaded to a
test CAS, and the spool transitions to `published/` via a real `cvmfs_gateway`
instance in a local test environment.

---

### Phase 1 — Publisher in Production (milestone: eliminate overlay FS bottleneck)

| Item | Description |
|---|---|
| Full processing pipeline | Unpack → compress+hash → dedup → upload → catalog build |
| Direct CAS deduplication | Per-object `CAS.Exists()` (os.Stat / S3 HEAD) at upload time — no inventory to populate |
| API server | REST endpoint for tar submission, status query, abort |
| `prepubctl` CLI | drain, abort, status |
| Lease heartbeat | Goroutine per active lease, abort-on-loss |
| Observability | Prometheus metrics: job latency, pipeline throughput, lease events |
| Integration tests | End-to-end against a real CVMFS repository (test instance) |
| Runbook | Deployment, monitoring alerts, manual recovery procedures |

Exit criterion: software packages can be published from a tar file into a test
CVMFS repository at the same or better correctness as the existing overlay workflow,
with measurable reduction in lock-hold time.

**Parallel operation:** During Phase 1 the existing overlay-based publisher
continues to run for all production repositories. The new service publishes to a
dedicated test repository. Both workflows coexist without interference because the
gateway lease enforces path-level mutual exclusion.

---

### Phase 2 — Stratum 1 Pull Pre-Warming (milestone: warm before the catalog flip)

| Item | Description |
|---|---|
| Control plane | Embedded MQTT-over-wss broker, signed discovery, enrollment, revocation |
| Distribution coordinator | Per-transaction announce, durable manifest store, warm-gate |
| Receiver (pull) | `--mode receiver` pull client; direct-CAS missing-set computation |
| Distribution metrics | Per-receiver readiness, warm coverage |
| Alerting | Alert when a warm quorum is not reached within timeout |

Exit criterion: after a publish, the announced objects are present on the warmed
Stratum 1 receivers before the catalog flip, verified by the receivers' warm/ready
reports.

**Phased rollout:** Enable pull pre-warming for one Stratum 1 site first, then roll
out to remaining sites progressively.

---

### Phase 3 — Lifecycle Cleanup (milestone: automated content expiry)

| Item | Description |
|---|---|
| AccessTracker + LogFileSource | Log file tailer with configurable field mapping |
| HTTPReceiverSource | Ingestion endpoint for push-based access events |
| CASBackendSource | S3 event integration or inotify |
| PublishEventSource | Internal events from the pipeline |
| GC scheduler + RefCounter | Per-repo cron, catalog walk |
| Namespace Cleaner | Catalog deletion diff via lease |
| Tombstone Manager + Reaper | Soft-delete window, rate-limited hard delete |
| GC dry-run mode | Full deletion plan produced without state changes |
| GC `prepubctl` commands | `gc-trigger`, `gc-dry-run`, `gc-status`, `tombstone-list` |

Exit criterion: for at least one repository, GC runs unattended on schedule, the
dry-run plan is reviewed and confirmed correct, then live GC is enabled and
monitored through one full cycle (namespace cleanup + tombstone expiry + CAS
reap).

**Rollout order:** Enable dry-run for all GC-eligible repos first. Review plans
for two full schedule cycles. Enable live GC per-repo with explicit operator sign-
off.

---

### Phase 4 — Hardening and Scale (ongoing)

| Item | Description |
|---|---|
| Multi-repo parallelism | Pipelines for multiple repos concurrently |
| `MultiBackend` for migration | Fan-out write to enable CAS backend migration |
| Nested catalog pre-splitting | Pre-publisher-controlled catalog depth splitting |
| Rate limiting and quotas | Per-submitter rate limits on the API |
| Formal chaos testing | Kill process at each spool state; verify recovery |
| Security audit | External review of credential handling and GC safety |

---


## 37. Open Questions and Future Work

**Chunked files (implemented).** Content-defined (xor32) chunking of large files
is implemented (§9.2): each chunk is an independent CAS object with the `P`
suffix and the catalog records the bulk hash. Remaining work is tuning the
default chunk sizes against real release workloads and broadening test coverage.

**Nested catalog pre-splitting.** Delegating catalog splitting to the gateway
(§9.4) is simpler but may not work for all gateway versions or for very deep
directory trees. The pre-publisher-controlled approach requires reading the
existing catalog structure to know where splits currently exist, adding a
dependency on catalog metadata at job start time.

**Cross-repository deduplication.** The current design deduplicates within a
single repository's CAS. CVMFS supports shared CAS across repositories. If
multiple repositories share a backend, the RefCounter must account for
cross-repository references. This is a significant complexity increase and is
deferred to Phase 4.

**Client telemetry.** If a future CVMFS client version supports reporting accessed
file hashes back to the publisher (via the monitoring socket or a dedicated
endpoint), the `HTTPReceiverSource` can ingest these events directly, providing
finer-grained access tracking than proxy logs alone.

**Catalog signing key management.** The gateway currently holds the private
signing key. If the pre-publisher is deployed on a separate host, it cannot sign
its own catalogs and must rely on the gateway for signing. A future option is to
support delegated signing tokens with path-scoped validity, avoiding the need for
the private key to reside on the gateway node.

**Observability.** Phase 1 adds basic Prometheus metrics. Full observability
(distributed tracing per job, per-file pipeline latency histograms, Stratum 1
pre-warm coverage tracking) is a Phase 4 item once the critical path is stable.

---




---
