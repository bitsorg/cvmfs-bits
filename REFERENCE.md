# CVMFS Pre-Publisher — Architecture and Deployment Design

Go service for pre-processing, queuing, and publishing software releases into CVMFS, complementing the existing overlay-based publishing workflow.
---

## Table of Contents

1. [Background and Motivation](#1-background-and-motivation)
2. [Current Publishing Flow and Its Constraints](#2-current-publishing-flow-and-its-constraints)
3. [Design Objectives](#3-design-objectives)
4. [System Overview](#4-system-overview)
5. [Option A — Inline Pre-Processor](#5-option-a--inline-pre-processor)
6. [Option B — Distributed Pre-Processor with Stratum 1 Pre-Warming](#6-option-b--distributed-pre-processor-with-stratum-1-pre-warming)
7. [Core Subsystems](#7-core-subsystems)
   - 7.1 [Job State Machine and Spool Directory Model](#71-job-state-machine-and-spool-directory-model)
   - 7.2 [Processing Pipeline](#72-processing-pipeline)
   - 7.3 [Lease Management](#73-lease-management)
   - 7.4 [Catalog Builder](#74-catalog-builder)
   - 7.5 [CAS Backend Abstraction](#75-cas-backend-abstraction)
   - 7.6 [Stratum 1 Distributor](#76-stratum-1-distributor)
8. [Lifecycle Cleanup (GC) Subsystem](#8-lifecycle-cleanup-gc-subsystem)
   - 8.1 [Access Tracking](#81-access-tracking)
   - 8.2 [Cleanup Policy](#82-cleanup-policy)
   - 8.3 [GC Pipeline](#83-gc-pipeline)
   - 8.4 [GC Job State Machine](#84-gc-job-state-machine)
9. [Go Package Structure](#9-go-package-structure)
10. [Configuration Reference](#10-configuration-reference)
11. [Security Considerations](#11-security-considerations)
12. [Deployment Roadmap](#12-deployment-roadmap)
13. [Open Questions and Future Work](#13-open-questions-and-future-work)
14. [The `bits` Integration — Build-to-Edge Pipeline](#14-the-bits-integration--build-to-edge-pipeline)
15. [Multi-Build-Node Topology](#15-multi-build-node-topology)
16. [Security, Confidentiality, Integrity, and Traceability](#16-security-confidentiality-integrity-and-traceability)
17. [Comparison with Traditional `cvmfs_server publish`](#17-comparison-with-traditional-cvmfs_server-publish)
18. [Provenance and Transparency Log](#18-provenance-and-transparency-log)
19. [Infrastructure Requirements and Backward Compatibility](#19-infrastructure-requirements-and-backward-compatibility)
20. [Stratum 1 Receiver Protocol](#20-stratum-1-receiver-protocol)
    - 20.1 [Design Rationale](#201-design-rationale)
    - 20.2 [Pre-Transfer Announce (Control Channel)](#202-pre-transfer-announce-control-channel)
    - 20.3 [Object Transfer (Data Channel)](#203-object-transfer-data-channel)
    - 20.4 [HMAC Authentication](#204-hmac-authentication)
    - 20.5 [Disk Space Pre-Check](#205-disk-space-pre-check)
    - 20.6 [Session Lifecycle](#206-session-lifecycle)
    - 20.7 [Backward Compatibility and Fallback](#207-backward-compatibility-and-fallback)
    - 20.8 [Configuration Reference](#208-configuration-reference)
21. [bits-console Integration](#21-bits-console-integration)
    - 21.1 [Where cvmfs-prepub Fits in the bits-console Pipeline](#211-where-cvmfs-prepub-fits-in-the-bits-console-pipeline)
    - 21.2 [Pipeline Comparison](#212-pipeline-comparison)
    - 21.3 [GitLab CI/CD Variables](#213-gitlab-cicd-variables)
    - 21.4 [Community ui-config.yaml Changes](#214-community-ui-configyaml-changes)
    - 21.5 [The cvmfs-prepub Pipeline File](#215-the-cvmfs-prepub-pipeline-file)
    - 21.6 [Access Control and Build Authorisation](#216-access-control-and-build-authorisation)
    - 21.7 [Running Alongside Existing Pipelines](#217-running-alongside-existing-pipelines)

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
`/cvmfs/<repo>` namespace, closing the transaction, then processing all changed files: compression, SHA-256 content hashing, deduplication against the existing
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
          SHA-256 hash
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

## 4. System Overview

The service is a single Go binary (`cvmfs-prepub`) with embedded subsystems. It
sits on a node that has write access to the CAS backend (S3 credentials or NFS
mount of the local CAS tree) and network access to the `cvmfs_gateway` HTTP API.

```
  tar file (HTTP upload / S3 / NFS drop)
       │
       ▼
  ┌─────────────────────────────────────────────────┐
  │              cvmfs-prepub service                │
  │                                                  │
  │  REST/gRPC API ──► Job Queue ──► Lease Manager  │
  │                                                  │
  │  Processing Pipeline:                            │
  │    Unpack ──► Compress+Hash ──► Dedup ──► Upload │
  │                                        │         │
  │                               Catalog Builder    │
  │                                        │         │
  │  Spool: incoming/leased/staged/        │         │
  │         uploading/distributing/        │         │
  │         committing/published/aborted   │         │
  │                                        ▼         │
  │                               cvmfs_gateway API  │
  └─────────────────────────────────────────────────┘
       │                      │
       ▼                      ▼
  CAS Backend          Stratum 1 replicas
  (S3 / local)         (pre-warmed before catalog flip)
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

## 5. Option A — Inline Pre-Processor

### Description

A single `cvmfs-prepub` instance runs on or close to the Stratum 0 node. It accepts tar uploads, runs the full processing pipeline internally, uploads objects
directly to the CAS backend, builds the catalog diff in a local SQLite file, and commits via the gateway lease API.

### Topology

```
  publisher node (or co-located node)
  ┌─────────────────────────────────┐
  │  cvmfs-prepub                   │
  │    ├── REST/gRPC API            │
  │    ├── Worker pool (N CPUs)     │
  │    ├── CAS backend client       │──► S3 / local CAS
  │    └── cvmfs_gateway client     │──► cvmfs_gateway
  └─────────────────────────────────┘
```

### Properties

- Zero new infrastructure nodes required.
- Write access to CAS backend must be available on this node.
- Stratum 1 replication still begins after catalog commit (no pre-warming).
- Suitable as Phase 1 milestone.

### Limitations

- Does not reduce Stratum 1 cold-start latency.
- Single point of processing; no horizontal scaling.

---

## 6. Option B — Distributed Pre-Processor with Stratum 1 Pre-Warming

### Description

Option B extends Option A with a `Distributor` subsystem that pushes newly uploaded CAS objects to Stratum 1 replicas **before** the catalog is committed at
Stratum 0. When the catalog flip occurs, every Stratum 1 already has the objects in its data directory. Replication becomes catalog-only rather than catalog-plus-objects. The thundering herd of cache misses is eliminated.

### Topology

```
  pre-processor node
  ┌──────────────────────────────────────┐
  │  cvmfs-prepub                        │
  │    ├── REST/gRPC API                 │
  │    ├── Worker pool                   │
  │    ├── CAS backend client            │──► S3 / local CAS (Stratum 0)
  │    ├── cvmfs_gateway client          │──► cvmfs_gateway
  │    └── Distributor                   │
  │         ├── push → Stratum 1 A       │──► /data/<ab>/<hash>C  (pre-seeded)
  │         ├── push → Stratum 1 B       │──► /data/<ab>/<hash>C  (pre-seeded)
  │         └── push → Stratum 1 …N     │──► /data/<ab>/<hash>C  (pre-seeded)
  └──────────────────────────────────────┘
         │
         ▼  (catalog commit happens here, after all S1s confirm)
    cvmfs_gateway → Stratum 0 manifest signed
         │
         ▼
    Stratum 1 snapshot: catalog fetch only, objects already present
```

### Stratum 1 Object Receiver

Each Stratum 1 needs to accept authenticated object PUTs.

![Option B Two-Channel Pre-Warming Topology](docs/cvmfs_option_b_topology.svg)

**Sub-option B1 — Push via receiver agent.** The `cvmfs-prepub --mode receiver` binary on each Stratum 1 implements a two-channel pre-warming server: an HTTPS control channel (`:9100`) handles announce requests authenticated with HMAC-SHA256, and a plain-HTTP data channel (`:9101`) accepts object PUTs authenticated with a per-session bearer token and verified by SHA-256 hash. The pre-publisher waits for a configurable quorum of Stratum 1s to confirm before committing. See §20 for the full protocol specification.

**Sub-option B2 — Shared object store.** If all Stratum 1s share an S3-compatible
object store as their data backend, uploading to S3 once in Option A is
sufficient. The distributor is a no-op; all Stratum 1s see the objects
immediately via shared storage. This is the simplest path where infrastructure
permits.

**Sub-option B3 — Trigger snapshot early.** Signal each Stratum 1 to
`cvmfs_server snapshot -t <repo>` immediately after CAS upload, before catalog
commit. Stratum 1 fetches objects from Stratum 0 CAS; the catalog fetch at
commit time finds them cached. No receiver agent required, but pre-warming is
best-effort rather than confirmed.

### Commit Gating

The catalog is committed only after a configurable quorum of Stratum 1s have
confirmed receipt:

```yaml
distribution:
  quorum: 0.75          # commit after 75% of S1s confirm
  timeout: 10m          # proceed with quorum after timeout even if some S1s lag
  commit_anyway: true   # if quorum not met after timeout, commit anyway and log
```

This preserves availability: a lagging Stratum 1 does not block the release, it
just experiences the standard cold-start behaviour for that release.

---

## 7. Core Subsystems

### 7.1 Job State Machine and Spool Directory Model

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
distributing          (Option B only; skipped in Option A)
  │  quorum of Stratum 1s confirmed
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
| `distributing/` | Re-push to Stratum 1s not confirmed in dist-log |
| `committing/` | Query gateway for payload status; commit or re-submit |

#### WAL Journal

Each job carries a `journal.jsonl` in its spool directory. Every state transition
appends a record before the rename:

```jsonc
{"t":"2026-04-24T02:00:00Z","from":"uploading","to":"distributing","run":"abc123"}
{"t":"2026-04-24T02:01:00Z","op":"dist_confirm","s1":"stratum1-cern.ch","n_objects":4821}
```

The journal is append-only and fsynced. It is the definitive record of what
happened to a job and is retained in `published/` or `aborted/` for audit.

### 7.2 Processing Pipeline

The pipeline runs as a bounded worker pool. File-level parallelism is controlled
by a `semaphore.Weighted` limited to `runtime.NumCPU()`. The pipeline is a
directed graph of stages; each stage communicates via Go channels with
backpressure.

```
Unpacker
  │  chan FileEntry (path, io.Reader)
  ▼
Compress+Hash worker pool
  │  chan ProcessedFile (path, casHash, compressedBytes)
  ▼
Deduplicator (Bloom filter → CAS HEAD check)
  │  chan NewObject (casHash, compressedBytes)  [only truly new objects]
  ▼
CAS Uploader (multipart for > 100 MB, idempotent PUT)
  │  confirmation written to upload-log.jsonl
  ▼
Catalog Accumulator
  └► catalog.db (all files, including deduped ones already in CAS)
```

**Single-pass compress+hash:**  
A single read of each file produces both the compressed form and the content hash
via an `io.TeeReader`:

```go
func processFile(r io.Reader) (hash string, compressed []byte, err error) {
    h := sha256.New()
    var buf bytes.Buffer
    w, _ := zlib.NewWriterLevel(&buf, zlib.BestCompression)
    if _, err = io.Copy(w, io.TeeReader(r, h)); err != nil {
        return
    }
    w.Close()
    return hex.EncodeToString(h.Sum(nil)), buf.Bytes(), nil
}
```

**Deduplication:**  
A Bloom filter is populated from the list of existing CAS objects at job start.
Files that hit the filter are confirmed with a single CAS `HEAD` request (false
positives are harmless — one extra network round-trip). Files that miss are
guaranteed new and go directly to the uploader. This eliminates the `HEAD` cost
for the common case where most files in a release already exist in CAS.

### 7.3 Lease Management

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
uploaded are GC-marked in a sidecar file; the CAS Reaper (described in §8.3)
cleans them up during the next GC pass once they are confirmed unreferenced.

### 7.4 Catalog Builder

The sub-catalog for the published path is built as a fresh SQLite file in
`staging/<jobID>/catalog.db` using WAL mode. It is populated with one row per
file enumerated from the tar (path, hash, size, mtime, uid, gid, mode, symlink
target, flags). The schema matches the CVMFS catalog schema so it can be
submitted directly via the gateway payload API.

Nested catalog splits can be handled in two ways:

- **Gateway-delegated:** Submit a flat catalog and allow cvmfs_gateway ≥ 1.2 to
  perform sub-catalog splitting at commit time. Zero additional logic in the
  pre-publisher.
- **Pre-publisher-controlled:** Split at path depth boundaries matching the
  existing catalog structure. Required for very large releases or when gateway
  version does not support delegation.

The catalog file itself is compressed and hashed like any other CAS object, giving
it an entry in the upload log and the same idempotent upload treatment.

### 7.5 CAS Backend Abstraction

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

### 7.6 Stratum 1 Distributor

The distributor runs after all CAS objects are confirmed uploaded. It tracks per-
Stratum 1 confirmation state in `dist-log.jsonl`:

```jsonc
{"s1":"stratum1-cern.ch",   "n_objects":4821, "confirmed":true,  "t":"..."}
{"s1":"stratum1-fnal.gov",  "n_objects":4821, "confirmed":false, "t":"..."}
```

On recovery, only unconfirmed Stratum 1s are retried. Distribution is
parallelised across Stratum 1s; each Stratum 1 receives objects concurrently with
the others. Within a single Stratum 1, uploads are pipelined with a bounded
concurrency (configurable, default 8).

If quorum is not met within `distribution.timeout`, the job proceeds to
`committing` anyway, records the lagging Stratum 1s in the receipt, and logs an
alert. The lagging sites will experience standard Stratum 1 cold-start latency for
this release only.

---

## 8. Lifecycle Cleanup (GC) Subsystem

![CVMFS GC and Lifecycle Cleanup Architecture](docs/cvmfs_gc_lifecycle_architecture.svg)

Certain repositories require automatic expiry and removal of content that has not
been accessed within a configurable window. This affects both the CVMFS namespace
(catalog entries) and backend CAS objects. The GC subsystem is opt-in per
repository and completely independent of the publishing pipeline.

### 8.1 Access Tracking

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
TTL. The `soft_delete_period` (§8.2) provides the primary buffer against this
(proxy TTLs are hours to days; the tombstone window is days to weeks). For
deployments that need stronger guarantees, configure the `HTTPReceiverSource` and
have proxy nodes periodically POST a summary of hashes served in the last N hours.
This is proxy-agnostic: it is a small script or log-shipper configuration, not a
Squid-specific integration.

### 8.2 Cleanup Policy

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

### 8.3 GC Pipeline

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

### 8.4 GC Job State Machine

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

## 9. Go Package Structure

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
│   │   ├── compress/        # zlib/zstd worker pool
│   │   ├── hash/            # SHA-256 (+ optional RIPEMD-160 for legacy compat)
│   │   ├── dedup/           # Bloom filter + CAS existence check
│   │   ├── upload/          # CAS object uploader, multipart, retry
│   │   └── catalog/         # SQLite catalog builder, WAL mode
│   │
│   ├── lease/               # cvmfs_gateway lease client, heartbeat goroutine
│   │
│   ├── distribute/          # Stratum 1 distributor, quorum tracking
│   │   ├── distributor.go
│   │   └── receiver/        # Optional: small HTTP receiver agent for S1 nodes
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
    └── manifest/            # .cvmfspublished parsing and writing
```

Key design constraints on package boundaries:

- `gc` and `access` communicate only through the `AccessEventSource` interface.
  No GC code imports any access source implementation.
- `pipeline` stages communicate only through typed Go channels.
  No stage imports another stage's implementation.
- `spool` has no knowledge of what the job contains; it only manages directory
  transitions. Business logic lives in `job` and `pipeline`.

---

## 10. Configuration Reference

```yaml
# /etc/cvmfs-prepub/config.yaml

server:
  listen: ":8080"
  tls_cert: /etc/cvmfs-prepub/tls/server.crt
  tls_key:  /etc/cvmfs-prepub/tls/server.key
  auth:
    type: jwt                  # or: mtls, hmac
    jwks_url: https://auth.example.org/.well-known/jwks.json

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
  compression: zlib           # or: zstd
  chunk_size_mb: 4            # for chunked large files
  upload_concurrency: 16

distribution:                 # Option B only; omit to run Option A
  stratum1_endpoints:
    - https://stratum1-cern.ch/cvmfs
    - https://stratum1-fnal.gov/cvmfs
  quorum: 0.75
  timeout: 10m
  commit_anyway: true
  per_s1_concurrency: 8
  # HMAC secret shared with all receivers — read from env: PREPUB_HMAC_SECRET
  hmac_secret_env: PREPUB_HMAC_SECRET

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

**Receiver mode** — `cvmfs-prepub --mode receiver` CLI flags (Stratum 1 node):

```
--control-addr  :9100            # HTTPS listen address (announce)
--data-addr     :9101            # Plain HTTP listen address (object PUTs)
--data-host     stratum1.example.com  # Publicly reachable hostname for data_endpoint
--tls-cert      /etc/cvmfs-receiver/tls/server.crt
--tls-key       /etc/cvmfs-receiver/tls/server.key
--cas-root      /srv/cvmfs/stratum1/cas
--session-ttl   1h
--disk-headroom 1.2
--log-level     info
# HMAC secret: export PREPUB_HMAC_SECRET=<shared-secret>
```

---

## 11. Security Considerations

**Authentication and authorisation.** The API server requires mTLS or JWT for all
publishing requests. Access event ingestion via `HTTPReceiverSource` uses a
pre-shared HMAC token (separate from the publishing credential). Separate
credentials limit blast radius: a compromised access event token cannot trigger
publishes.

**Lease token storage.** The gateway lease token is written to
`leased/<jobID>/lease.json` with mode `0600`. It is never logged or emitted in
API responses.

**Object integrity.** After each CAS upload the response ETag (for S3) or a local
re-read (for local FS) is compared against the computed hash. A mismatch aborts
the upload and marks the job failed. The catalog only references objects that have
passed this check.

**Manifest signing.** The gateway signs the manifest with the repository's Ed25519
private key as part of the payload commit. The pre-publisher does not need access
to the private key; it submits the unsigned catalog diff and the gateway performs
signing.

**GC safety.** The reference counter is conservative: any catalog read error
aborts the GC run entirely. The TOCTOU guard (refcount re-check immediately before
CAS deletion) prevents races between a concurrent publish and a GC run that
overlapped during the tombstone window. GC and publish are also serialised at the
gateway lease level for any overlapping path.

**Audit trail.** Every publishing job and every GC run produces a signed JSONL
audit log retained in the published/gc-complete spool directory. These are the
authoritative record of what was written to or deleted from CAS.

---

## 12. Deployment Roadmap

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

### Phase 1 — Option A in Production (milestone: eliminate overlay FS bottleneck)

| Item | Description |
|---|---|
| Full processing pipeline | Unpack → compress+hash → dedup → upload → catalog build |
| Bloom filter deduplication | Populate from CAS object listing at job start |
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

### Phase 2 — Option B: Stratum 1 Pre-Warming (milestone: eliminate cold-start latency)

| Item | Description |
|---|---|
| Distributor subsystem | Per-Stratum-1 push, dist-log, recovery |
| Quorum gating | Configurable quorum + timeout before commit |
| Receiver agent (B1) | `cvmfs-s1-receiver` sidecar for Stratum 1 nodes if not using shared S3 (B2) |
| Distribution metrics | Per-S1 lag, quorum achievement rate, pre-warm coverage |
| Alerting | Alert when quorum not met within timeout |

Exit criterion: after a publish, cache miss rates on Stratum 1 in the first 10
minutes are measurably lower than baseline for the same release size.

**Phased rollout:** Enable pre-warming for one Stratum 1 site first. Measure
cache miss delta. Roll out to remaining sites progressively.

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
| Multi-repo parallelism | Option A/B pipelines for multiple repos concurrently |
| Chunked large file support | Split files > `chunk_size_mb` into content-addressed chunks |
| `MultiBackend` for migration | Fan-out write to enable CAS backend migration |
| Nested catalog pre-splitting | Pre-publisher-controlled catalog depth splitting |
| Rate limiting and quotas | Per-submitter rate limits on the API |
| Formal chaos testing | Kill process at each spool state; verify recovery |
| Security audit | External review of credential handling and GC safety |

---

## 13. Open Questions and Future Work

**Chunked files.** CVMFS supports splitting large files into content-addressed
chunks. The current design uploads each file as a single CAS object. For files
larger than a few hundred MB, chunking is important for deduplication (a 10 GB
file that changes by 1 MB should not require re-uploading 10 GB). The pipeline
architecture accommodates chunking (the `compress` stage can emit multiple
`ProcessedFile` messages per input file), but the catalog schema for chunked
files and the Bloom filter sizing need detailed design work.

**Nested catalog pre-splitting.** Delegating catalog splitting to the gateway
(§7.4) is simpler but may not work for all gateway versions or for very deep
directory trees. The pre-publisher-controlled approach requires reading the
existing catalog structure to know where splits currently exist, adding a
dependency on catalog metadata at job start time.

**Cross-repository deduplication.** The current design deduplicates within a
single repository's CAS. CVMFS supports shared CAS across repositories. If
multiple repositories share a backend, the Bloom filter and the RefCounter must
account for cross-repository references. This is a significant complexity
increase and is deferred to Phase 4.

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

## 14. The `bits` Integration — Build-to-Edge Pipeline

### 14.1 What `bits` Produces

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

### 14.2 Path Configuration for the CVMFS Namespace

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

### 14.3 Submission Protocol

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

### 14.4 Data Flow and Latency Profile

```
bits build node
  │
  │  (1) tar produced
  ▼
HTTP POST /api/v1/jobs          ← <1 s  (multipart upload or spool reference)
  │
  │  (2) job enters FSM: incoming → leased
  ▼
gateway lease acquire           ← 50–200 ms  (single RPC to cvmfs_gateway)
  │
  │  (3) leased → staging
  ▼
tar unpack + scan               ← proportional to file count, typically 1–10 s
  │
  │  (4) staging → uploading
  ▼
compress + CAS upload           ← dominant cost; ~1–5 s per 100 MB new data
  │                               (dedup eliminates already-known objects)
  │  (5) uploading → distributing
  ▼
Stratum 1 push (parallel)       ← 1–30 s depending on replica count and BW
  │
  │  (6) distributing → committing
  ▼
gateway commit + manifest sign  ← <1 s
  │
  │  (7) committing → published
  ▼
cvmfs client sees new revision  ← up to client TTL (default 4 min) after commit
```

**Typical end-to-end time** from `bits` completing the build to the new
software version being visible on edge worker nodes:

| Phase | Typical duration |
|---|---|
| bits → prepub submission | < 2 s |
| Lease + pipeline (compress, dedup, upload) | 10–60 s for a 500 MB incremental |
| Stratum 1 propagation | 5–30 s (parallel push to N replicas) |
| Client TTL expiry + catalog refresh | 0–240 s (configurable) |
| **Total (typical incremental update)** | **< 5 min** |

To minimise client TTL latency, configure workers to use a short
`CVMFS_MAX_TTL` (e.g. 60 s) for repositories that receive frequent updates, or
deploy a lightweight push-notification mechanism that triggers `cvmfs_config
reload` on workers immediately after the gateway commits.

### 14.5 Deduplication for Incremental Packages

Because CAS objects are content-addressed by SHA-256, files that are shared
across software versions (shared libraries, Python runtimes, common headers) are
uploaded exactly once.  A `bits` package update that changes only 5% of files
will only upload that 5%, even if the full software tree is 10 GB.

The pipeline's Bloom filter (§7.3) accelerates the dedup check: before issuing
a `cas.Get`, the pipeline asks the filter whether the hash is already present.
The filter provides a probabilistic answer in microseconds, avoiding a disk or
S3 round-trip for the common case.

---

## 15. Multi-Build-Node Topology

### 15.1 Motivation

Large HEP experiments ship software on different schedules and require different
build environments.  Serialising all publishes through a single build node
creates a bottleneck.  The cvmfs-prepub architecture supports horizontal scaling
by assigning each experiment group its own build node and a scoped set of
credentials, so multiple groups can publish in parallel without interfering.

### 15.2 Per-Group Namespace Partitioning

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

### 15.3 Deployment Topology

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
                                     │ push objects
                              ┌──────┴──────┐
                              │  Stratum 1  │
                              │  replicas   │
                              └─────────────┘
```

Each build node runs its own `cvmfs-prepub` process (or connects to a shared
prepub service with separate API tokens and path-scoped gateway keys).  The CAS
can be shared across nodes on a fast network filesystem (NFS, CephFS) to
maximise cross-experiment deduplication, or kept local per node if isolation is
preferred.

### 15.4 Credential Scoping

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

### 15.5 Parallel Publishing and Load Distribution

Because the gateway serialises only within a sub-path, independent groups
publish fully in parallel.  The Stratum 1 distributor pushes objects concurrently
to all replicas (controlled by `distribute.concurrency`), so adding more build
nodes does not increase per-node push time.

To prevent a single busy group from monopolising Stratum 1 bandwidth, configure
the distributor timeout and concurrency per-node so that the aggregate push rate
across all nodes stays within the Stratum 1 ingress capacity.

### 15.6 Monitoring Across Groups

Add a `group` label to all Prometheus metrics by injecting it via the config:

```toml
[observability]
extra_labels = { group = "atlas" }
```

This allows per-group Grafana dashboards to show job throughput, pipeline
latency, CAS dedup ratio, and Stratum 1 push success rate independently for
each experiment, making it easy to identify which group is experiencing issues
without noise from the others.

---

## 16. Security, Confidentiality, Integrity, and Traceability

### 16.1 Trust Chain Overview

The end-to-end trust chain from source code to edge worker node is:

```
Source code / build script
    │  (1) Build isolation
    ▼
bits build environment (isolated container or VM)
    │  (2) Tar signing
    ▼
Signed tar archive (SHA-256 + detached signature)
    │  (3) TLS transport + API token
    ▼
cvmfs-prepub API (HTTPS only, bearer token auth)
    │  (4) Pipeline integrity — SHA-256 CAS content addressing
    ▼
CAS (content-addressed objects, immutable after write)
    │  (5) HMAC-SHA256 gateway authentication
    ▼
cvmfs_gateway (path-scoped lease, Ed25519 manifest signing)
    │  (6) Signed manifest + whitelist
    ▼
Stratum 1 replicas (HTTPS push, HMAC-authenticated)
    │  (7) Client certificate / whitelist verification
    ▼
Edge worker nodes (CVMFS client verifies manifest signature before mount)
```

### 16.2 Build Environment Isolation

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

### 16.3 Tar Signing and Integrity at Handoff

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

### 16.4 Transport Security

All communication within the system uses TLS 1.2 or higher:

- `bits` → `cvmfs-prepub`: HTTPS (enforced; HTTP submissions rejected unless
  `--dev` is set).
- `cvmfs-prepub` → `cvmfs_gateway`: HTTPS, `MinVersion: tls.VersionTLS12`
  enforced in the lease client transport (§6.2).
- `cvmfs-prepub` → Stratum 1: HTTPS, same TLS minimum enforced in the shared
  distributor HTTP client (§8).
- Stratum 1 → edge workers: HTTPS via the CVMFS client; certificate pinned to
  the repository whitelist.

Private key material (gateway secret, API tokens, build node signing keys) is
never transmitted over the wire and is never stored in the source tree or config
files; it is injected exclusively via environment variables or a secrets manager.

### 16.5 CAS Integrity — Content Addressing

Every object stored in the CAS is keyed by its SHA-256 digest.  The upload
stage (`internal/pipeline/upload`) computes the digest before writing and
verifies it on read.  An attacker who gains write access to the CAS storage
backend and modifies an object's bytes will change its effective hash, causing
the pipeline to reject it on the next read as a hash mismatch.  The gateway
includes object hashes in the signed manifest, so clients can independently
verify each file they fetch.

### 16.6 Gateway Lease Scoping and Manifest Signing

The gateway issues leases scoped to a repository sub-path and a key-ID.  The
HMAC-SHA256 signature on every gateway API request (§6.2) covers:

```
method + requestURI + bodyHash + unixTimestamp
```

This prevents cross-verb replay, cross-path replay, body substitution, and
replay beyond the 60-second window.  After a successful commit, the gateway
signs the updated catalog tree with the repository's Ed25519 private key and
updates the signed manifest.  CVMFS clients verify this signature on every
catalog fetch, rejecting any manifest not signed by the known repository key.

### 16.7 Stratum 1 Distribution Security

Objects are pushed to Stratum 1 replicas over HTTPS.  The distributor validates
all endpoints before connecting (§8.2): it requires HTTPS and rejects private or
loopback IP addresses to prevent SSRF.  The quorum mechanism (§8.3) ensures that
a publish is not considered successful unless a configurable fraction of replicas
have received all objects, preventing a silent loss of availability on a subset
of workers.

### 16.8 Client Verification

CVMFS clients receive the repository whitelist (a signed list of trusted
Stratum 1 hostnames and their TLS certificate fingerprints) from the Stratum 0
gateway.  On every catalog fetch, the client:

1. Verifies the TLS certificate of the Stratum 1 against the whitelist.
2. Verifies the Ed25519 signature on the downloaded manifest.
3. Rejects the catalog if either check fails.

This means a rogue Stratum 1 (or a MITM between client and Stratum 1) cannot
serve a modified software tree without being detected.

### 16.9 Confidentiality

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

### 16.10 Audit Trail and Traceability

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
    │        JSON-per-line: { endpoint, hash, time } for every Stratum 1 push confirmed
    │
    └──▶  gateway commit response
             catalog_hash   (SHA-256 of the signed catalog)
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

## 17. Comparison with Traditional `cvmfs_server publish`

The table below compares the two publishing paths across every dimension that
affects throughput, operator experience, and client correctness.  The
traditional workflow is the reference baseline; the prepub service is designed
to be run alongside it, not as a replacement.

### 17.1 Head-to-head comparison

| Dimension | Traditional `cvmfs_server publish` | `cvmfs-prepub` service |
|---|---|---|
| **Lock scope** | Entire repository locked from first file I/O to manifest sign | Lease covers only `SubmitPayload + Release` — seconds, not minutes |
| **Lock duration** | O(minutes – hours) depending on release size | O(seconds) regardless of release size |
| **Concurrency** | Single publisher per repo; all others queue behind the lock | Multiple jobs process concurrently; lease acquired only at commit |
| **Entry point** | SSH to Stratum 0; mount overlay fs; copy files by hand or script | HTTP POST multipart tar to any prepub node — no SSH required |
| **Input format** | Arbitrary file tree via overlay mount | Tar archive stream — native to CI/CD artifact pipelines |
| **Publisher privilege** | Requires root / overlay fs capability on the Stratum 0 node | Requires HTTP reach to prepub endpoint + API token only |
| **When processing runs** | Inside the transaction lock — compress, hash, upload all block commit | Before the lease is acquired — fully decoupled from the lock |
| **Streaming** | Files must be fully on disk before publish begins | Tar streamed from HTTP body; compress+hash begins on the first byte |
| **Worker parallelism** | Single-threaded per file in `cvmfs_server`; no worker pool | Configurable worker pool; compress and catalog build overlap |
| **CAS upload timing** | Inside transaction — network latency is counted against lock hold time | Before lease acquisition — network latency is hidden from the lock window |
| **Dedup mechanism** | CAS stat per file inside transaction | Bloom filter fast path + CAS confirm; updated in-process after each upload |
| **Cross-job dedup** | Via shared CAS — each publish rescans the store at lock time | Bloom filter seeded from CAS at startup; incremental updates during the run |
| **Cross-node dedup** | No built-in mechanism across separate publisher nodes | Shared Bloom filter snapshots on NFS/CephFS (optional, §15) |
| **When S1 can replicate** | Only after manifest is signed and released | Objects pushed to S1 before catalog commit — pre-warming (Option B) |
| **Cold-start miss storm** | High — all S1 nodes fetch concurrently after catalog flip | Eliminated — objects already on S1 when catalog flips |
| **Client latency after publish** | First clients hit slow inter-site links while S1 replicates | Files available immediately from S1 upon catalog flip |
| **State durability** | Overlay fs is live; interrupted publish leaves an uncommitted overlay | Every state transition written to spool; restarts resume from last checkpoint |
| **Recovery** | Manual — abort transaction, inspect overlay, re-run publish | Automatic — crash-recovery goroutines re-run incomplete jobs on startup |
| **Idempotency** | Not guaranteed — re-running after failure may double-upload | CAS Put and gateway Submit are idempotent; safe to retry at any stage |
| **Job status** | Exit code + log file; no structured job state | REST API with per-job FSM state; SSE stream for live progress |
| **Notifications** | None built-in; CI polls or parses log output | Webhook POST on terminal state; SSE endpoint for streaming updates |
| **Metrics** | None (relies on external OS-level monitoring) | OpenTelemetry traces + Prometheus metrics per pipeline stage |
| **Horizontal scale** | One publisher node per repo; gateway serialises them | Multiple prepub nodes process concurrently; lease only at commit |
| **Throughput ceiling** | Bounded by overlay fs I/O and single-threaded `cvmfs_server publish` | Bounded by CAS backend throughput; scales with CPU count and node count |
| **Gateway changes needed** | N/A — is the gateway workflow | None — targets existing lease API as a first-class client |
| **S1 / client changes** | None | None — S1 receiver gains optional batch endpoint; falls back per-object |
| **Coexistence** | — | Runs alongside traditional workflow without conflict |
| **Publisher identity** | SSH username on Stratum 0 node — mutable server-side log only | Structured fields (`actor`, `git_sha`, `pipeline_id`) in every job manifest |
| **Identity verification** | No cryptographic check; impersonation possible if SSH key is stolen | OIDC token validated against CI provider's JWKS at submission time; `verified=true` is cryptographically attested |
| **Git commit linkage** | Not captured; must correlate timestamps manually across systems | `git_sha` + `git_ref` embedded in manifest and Rekor entry; direct `git show` traceability |
| **Transparency log** | None | Optional Rekor submission after every publish — append-only Merkle log |
| **Tamper-evident receipt** | Absent; audit relies on server-side logs being intact | Signed Entry Timestamp (SET) stored in manifest; verifiable offline without `cvmfs-prepub` |
| **Retroactive audit** | Only if access logs survive; no way to prove absence of tampering | Rekor entry persists even if spool is deleted; Merkle inclusion proof is public |
| **Regulated / SLSA compliance** | Manual correlation of logs; no machine-verifiable chain | Four-layer chain (file → hash → job → CI → author) satisfies SLSA Build L2+ requirements |

### 17.2 Where the fundamental difference lies

The root cause of the traditional workflow's throughput ceiling is that **the
exclusive lock is held for the entire duration of file processing**.  Compress,
hash, dedup-check, and CAS upload all happen while the lock is held.  A 10 GB
release can block the repository for 20 minutes; a concurrent publisher must
wait for the full duration.

The prepub service inverts this ordering.  The compress → hash → dedup → CAS
upload pipeline runs entirely before a gateway lease is requested.  By the time
`cvmfs_gateway` is asked for a lease, every object is already in the CAS and
has optionally been pushed to Stratum 1.  The lease window shrinks to the few
seconds required to call `SubmitPayload` and `Release`, so multiple jobs can be
in-flight simultaneously without contending.

The same inversion solves the Stratum 1 cold-start problem.  Because the
distributor pushes objects to each S1 receiver before the catalog flip, the
moment clients see a new catalog revision, the corresponding objects are already
in local S1 storage.  There is no thundering herd.

### 17.3 What the prepub service does not change

The gateway's manifest signing, catalog merging, and revision-number increment
are untouched — the prepub service is a first-class client of the existing
gateway lease-and-payload API, not a bypass of it.  Stratum 1 replication
protocol, proxy behaviour, and client-side CVMFS are also unchanged.  The
overlay-filesystem workflow continues to work in parallel for operators who
prefer it; the gateway's per-path lease mechanism enforces mutual exclusion
between the two paths at the sub-path level.

### 17.4 Hard-link handling — a concrete behavioural difference

The traditional overlay-filesystem approach handles hard links naturally through
the Linux VFS: two directory entries with the same inode are indistinguishable
from any other files in the tree.  A tar-stream-based pipeline must handle them
explicitly.

In the current implementation the unpack stage maintains a `seenFiles` map keyed
by cleaned entry path.  When a `TypeLink` entry is encountered, its data is
resolved from the map and the entry is emitted as a regular `FileEntry` with the
same byte content.  This means both the original path and the hard-linked path
appear in the catalog with identical content hashes, which is the correct CAS
behaviour — the dedup check will recognise the hash from the earlier upload and
skip the second CAS write.

Forward hard-link references (where the link appears before its target in the
stream) are rejected with a clear error, consistent with GNU tar's ordering
guarantee.

### 17.5 Provenance — a structural difference, not an add-on

The traditional workflow has no native concept of a publisher identity that travels
with the content.  What exists are SSH access logs, overlay-fs transaction timestamps,
and whatever the operator chooses to record externally.  These records are mutable,
siloed, and absent if the Stratum 0 node is rebuilt.

The prepub service treats provenance as a first-class output of every publish job.
Three properties make it structurally different from any add-on audit solution:

**The record is sealed at publish time.**  The `provenance` block is written into the
job manifest before the gateway lease is acquired and is not modifiable after the
`StatePublished` transition.  An operator cannot retroactively alter which commit or
CI pipeline is attributed to a given catalog revision.

**The identity is cryptographically attested, not self-reported.**  When a CI system
passes an OIDC token, `cvmfs-prepub` validates the token signature against the
provider's published JWKS.  The resulting `verified=true` flag means the CI
provider — not the submitter — is asserting the identity.  An attacker who intercepts
the API token cannot forge the OIDC subject claim without also compromising the
CI provider's signing key.

**The receipt survives the destruction of the service.**  After Rekor submission the
Signed Entry Timestamp (SET) is stored in the manifest, but the entry also exists
independently in the append-only Rekor Merkle log.  Even if every `cvmfs-prepub`
node, spool directory, and manifest file is destroyed, any auditor can query Rekor
by file hash and recover the full publish attribution chain.  The traditional
workflow offers no equivalent — once access logs are gone, the attribution is gone.

### 17.6 When to use each approach

| Scenario | Recommended path |
|---|---|
| Ad-hoc manual publish by an operator on the Stratum 0 node | Traditional `cvmfs_server publish` |
| CI/CD pipeline publishing a build artifact from an external build node | `cvmfs-prepub` |
| Large release (> 1 GB) where lock contention matters | `cvmfs-prepub` |
| Multiple teams publishing to different sub-paths in parallel | `cvmfs-prepub` |
| Rollout to O(10,000) nodes where post-publish latency matters | `cvmfs-prepub` (Option B, pre-warming) |
| Repository with sparse, infrequent, small updates | Either; traditional is simpler |
| Regulated environment requiring a structured audit trail | `cvmfs-prepub` (§16) |
| Supply-chain compliance (SLSA, SBOM, incident attribution) | `cvmfs-prepub` with `--provenance` (§18) |
| Need to prove retroactively who published a specific file version | `cvmfs-prepub` with `--provenance` — Rekor entry is permanent |
| Air-gapped or self-hosted transparency log required | `cvmfs-prepub` with `--rekor-server <internal-url>` |

---

## 18. Provenance and Transparency Log

The standard CVMFS publishing workflow guarantees *content integrity* — every file
is addressed by its SHA-256 hash and the catalog is signed by the Stratum 0 gateway
key.  What it does not answer is the *attribution* question: **given a file on a
client node, which person, commit, and CI pipeline put it there?**

§18 documents how `cvmfs-prepub` closes that gap with a four-layer provenance chain
that is irrefutable, publicly auditable, and survives the loss of any single system.

---

### 18.1 Threat model and motivation

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

### 18.2 The four-layer provenance chain

```
┌───────────────────────────────────────────────────────────────────┐
│  Layer 1: File → Content Hash                                     │
│  Source: CVMFS signed catalog (.cvmfspublished)                   │
│  /atlas/24.0/libAtlasROOT.so  →  sha256:a3f9…b812                │
│  Signature: Stratum 0 gateway key (existing CVMFS trust anchor)   │
└─────────────────────────┬─────────────────────────────────────────┘
                          │  reverse-index by hash
┌─────────────────────────▼─────────────────────────────────────────┐
│  Layer 2: Content Hash → Publish Job                              │
│  Source: Rekor transparency log (Sigstore)                        │
│  Entry type: hashedrekord                                         │
│  Fields: sha256:a3f9…b812, job_id, catalog_hash, git_sha         │
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

### 18.3 How the implementation works

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

### 18.4 Rekor and non-repudiation

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

### 18.5 CI OIDC token validation

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

### 18.6 Configuration reference

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

### 18.7 Offline verification workflow

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

### 18.8 Relationship to §16 (Security and Traceability)

§16.10 documents the *audit trail* features built into the job spool (journal,
manifest, upload log).  §18 extends that audit trail with two additional properties
that §16 cannot provide alone:

| Property | §16 (internal audit trail) | §18 (Rekor + OIDC) |
|---|---|---|
| Tamper evidence | Manifest is on local disk — mutable by root | Rekor entry is in an append-only public log |
| External verifiability | Requires access to the spool | Anyone with the file hash can verify via Rekor |
| Identity attestation | `actor` is caller-supplied (Verified=false by default) | `verified=true` means CI provider cryptographically attests identity |
| Survives system destruction | Spool deleted → audit trail lost | Rekor entry persists independently |

Both mechanisms are complementary: the spool gives fine-grained operational
visibility; Rekor gives the durable, externally-auditable receipt.

---

## 19. Infrastructure Requirements and Backward Compatibility

### 19.1 Summary

`cvmfs-prepub` is designed to run alongside the existing `cvmfs_server publish`
workflow without replacing or disrupting it.  No changes to Stratum 1 servers,
proxy caches, or CVMFS clients are required for the baseline deployment (Option A).
Option B adds a lightweight receiver agent on each Stratum 1 but leaves the
existing replication machinery untouched.

### 19.2 Option A — Single-Node Deployment (Zero Stratum 1 Changes)

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

### 19.3 Option B — Distributed Deployment with Stratum 1 Pre-Warming

Option B pushes new CAS objects to each Stratum 1 *before* the catalog is
committed, so that the replication snapshot becomes catalog-only (the objects are
already present).  This eliminates the thundering-herd cache-miss burst that
follows a catalog flip.

To accept pre-warmed objects each Stratum 1 node runs the same `cvmfs-prepub`
binary in receiver mode:

```sh
cvmfs-prepub --config /etc/cvmfs-prepub/receiver.yaml --mode receiver
```

The receiver:
- listens on a dedicated port (default `:9100`) under its own systemd unit
- places incoming objects in the Stratum 1's local CAS directory using the
  standard CVMFS on-disk layout
- authenticates every request with HMAC-SHA256; unauthenticated requests are
  rejected

The existing `cvmfs_server snapshot` daemon on each Stratum 1 is completely
unchanged.  When it runs after the catalog commit it finds the new objects already
present and fetches only the catalog, behaving as if the objects had arrived via an
earlier snapshot.

**Two alternatives avoid deploying the receiver agent entirely:**

| Sub-option | Mechanism | Receiver agent? |
|---|---|---|
| B2 — Shared S3 backend | All Stratum 1s share the same S3-compatible bucket; a single CAS upload makes objects available to every S1 immediately | No |
| B3 — Early snapshot trigger | Signal each S1 to `cvmfs_server snapshot -t <repo>` after the CAS upload; pre-warming is best-effort (not confirmed before commit) | No |

### 19.4 Coexistence with `cvmfs_server publish`

The gateway's per-path lease enforces mutual exclusion at sub-path granularity.
A `cvmfs-prepub` job on `atlas/24.0` and a traditional `cvmfs_server publish` on
`cms/` can hold leases simultaneously without conflict.  Both workflows compete
only if they target the same sub-path at the same time, in which case one must
wait — the same constraint that applies between two traditional publishes.

There is no flag day: operators can deploy `cvmfs-prepub` for new or
CI-driven repositories while continuing to use the traditional overlay workflow
for manual or infrequent publishes on other repositories.

### 19.5 What Does Not Change

| Layer | Status |
|---|---|
| `cvmfs_gateway` | Unmodified; targeted via existing lease-and-payload API |
| Stratum 1 replication daemon | Unmodified (`cvmfs_server snapshot` unchanged) |
| Stratum 0 CAS layout | Identical; objects written with the same path structure |
| Signed manifest format | Identical; gateway signs as usual |
| Proxy / Squid caches | Unmodified; cache the same object URLs |
| CVMFS client | Unmodified; verifies the same manifest signature |
| Traditional `cvmfs_server publish` | Continues to work in parallel |

### 19.6 Recommended Rollout Order

1. **Option A on a test repository** — validate correctness and lock-time
   reduction alongside the existing workflow; no Stratum 1 involvement.
2. **Option B on one Stratum 1 site** — deploy the receiver agent on a single
   replica, measure cache-miss delta before and after a publish.
3. **Option B rolled out to remaining Stratum 1 sites** progressively as
   confidence grows.
4. **CI-driven repositories** migrated from the overlay workflow to
   `cvmfs-prepub`; manual ad-hoc publishes can remain on `cvmfs_server publish`
   indefinitely.

---

## 20. Stratum 1 Receiver Protocol

### 20.1 Design Rationale

The receiver uses two separate HTTP channels with deliberately different security
properties:

| Channel | Protocol | Purpose | Why |
|---|---|---|---|
| Control | HTTPS (TLS 1.2+) | Announce + session negotiation | Protects the HMAC secret and session tokens |
| Data | Plain HTTP | Object PUT | SHA-256 hash verification provides transfer integrity without TLS overhead |

CVMFS already distributes objects to clients over plain HTTP, relying on the
content hash embedded in the signed catalog for integrity.  The receiver applies
the same principle in the push direction: a MITM who substitutes bytes is
detected by the hash check; one who drops bytes triggers the sender's retry
logic.  TLS on the data channel would encrypt gigabytes of already-compressed
objects at both ends for no additional integrity benefit.

### 20.2 Pre-Transfer Announce (Control Channel)

Before pushing any objects the sender posts a single announce request on the
HTTPS control channel.

**Request** — `POST https://<s1-host>:<control-port>/api/v1/announce`

```json
{
  "payload_id":   "550e8400-e29b-41d4-a716-446655440000",
  "object_count": 4821,
  "total_bytes":  1073741824
}
```

Headers:

| Header | Value |
|---|---|
| `Content-Type` | `application/json` |
| `X-Timestamp` | Unix timestamp (seconds) as decimal string |
| `X-Signature` | Hex-encoded HMAC-SHA256 of the canonical message (§20.4) |

**Response — 200 OK** (accepted, or idempotent re-announce for the same
`payload_id`):

```json
{
  "session_token": "a1b2c3d4…",
  "data_endpoint": "http://<s1-host>:<data-port>"
}
```

**Error responses:**

| Status | Meaning |
|---|---|
| `401 Unauthorized` | Missing, expired, or invalid HMAC signature |
| `507 Insufficient Storage` | Free disk space below `total_bytes × disk_headroom` |
| `400 Bad Request` | Malformed JSON or missing required fields |

The receiver stores the session in memory with a configurable TTL (default 1 h).
If the sender re-announces the same `payload_id` before the session expires it
receives the same `session_token`, making the announce idempotent across sender
restarts.

### 20.3 Object Transfer (Data Channel)

After a successful announce the sender PUTs each object to the plain-HTTP data
endpoint returned in the announce response.

**Request** — `PUT http://<s1-host>:<data-port>/api/v1/objects/{hash}`

| Header | Value |
|---|---|
| `Authorization` | `Bearer <session_token>` |
| `X-Content-SHA256` | Hex-encoded SHA-256 of the compressed object bytes in the body |
| `Content-Type` | `application/octet-stream` |

Body: the compressed CAS object bytes (as stored in the sender's CAS).

**Processing at the receiver:**

1. Verify the `Authorization` header against the session store; reject with
   `401` if the token is unknown or expired.
2. Validate `{hash}` is a well-formed hex string of the correct length; reject
   with `400` if not (path traversal guard).
3. Stream the body simultaneously through a SHA-256 hasher and a temporary file
   in the CAS staging area.
4. After the full body is consumed, compare the computed digest against the
   `X-Content-SHA256` header.  If they differ return `400 Bad Request` and
   delete the temporary file.
5. Derive the final CAS path from `{hash}` using the standard CVMFS layout
   (`{cas_root}/{hash[0:2]}/{hash}C`).
6. Atomically rename the temporary file to the final CAS path.
7. Return `200 OK`.  If the object is already present (idempotent re-PUT after
   a sender restart) return `200 OK` without overwriting.

**Error responses:**

| Status | Meaning |
|---|---|
| `200 OK` | Object written or already present |
| `400 Bad Request` | Hash mismatch or malformed hash in URL |
| `401 Unauthorized` | Missing, unknown, or expired session token |
| `507 Insufficient Storage` | Disk full mid-write |

### 20.4 HMAC Authentication

The HMAC on the announce request uses the same scheme as gateway lease requests
(§16.6):

```
canonical_message = METHOD + "\n"
                  + request_path + "\n"
                  + hex(SHA-256(body)) + "\n"
                  + unix_timestamp_seconds
```

```
X-Signature = hex(HMAC-SHA256(shared_secret, canonical_message))
```

The receiver rejects announce requests where `|X-Timestamp − now| > 60 s`
(replay protection), or where the HMAC does not verify.

The `shared_secret` is configured independently of the gateway secret and is
never reused between sites.

### 20.5 Disk Space Pre-Check

On receiving an announce the receiver calls `statfs` on the CAS root and
rejects with `507 Insufficient Storage` if:

```
available_bytes < total_bytes × disk_headroom
```

`disk_headroom` defaults to `1.2` (20 % margin).  If `total_bytes` is zero
(sender does not know the payload size) the disk space check is skipped
entirely; the receiver accepts and relies on the mid-transfer `ENOSPC` handling
described below.

The check is best-effort: it guards against obvious full-disk failures at
announce time but does not guarantee space will remain available for the entire
transfer duration.  Mid-transfer `ENOSPC` is handled by returning `507` on the
failing PUT and deleting the temporary file; the sender treats this as a
retriable error.

The headroom is computed using integer arithmetic to avoid `float64` precision
loss near `MaxInt64`.  The `statfs` call uses `Bavail × Bsize` (unprivileged
blocks) rather than `Bfree × Bsize` to correctly reflect space available to the
receiver process.

### 20.6 Session Lifecycle

![Stratum 1 Receiver — Two-Channel Protocol](docs/cvmfs_receiver_protocol.svg)

```
sender (distributor)             control :9100 (HTTPS)    data :9101 (HTTP)
  │                                        │                      │
  │── POST /api/v1/announce ──────────────►│                      │
  │   X-Timestamp, X-Signature (HMAC)      │ verify HMAC          │
  │   {payload_id, total_bytes}            │ check disk           │
  │                                        │ create session       │
  │◄─ 200 {session_token, data_endpoint} ──│                      │
  │                                        │                      │
  │── PUT /api/v1/objects/{h1} ────────────────────────────────►  │
  │── PUT /api/v1/objects/{h2} ────────────────────────────────►  │ verify bearer
  │── PUT /api/v1/objects/{hN} ────────────────────────────────►  │ stream → tmp
  │   Authorization: Bearer <token>        │                      │ verify SHA-256
  │   X-Content-SHA256: <sha256>           │                      │ atomic rename
  │                                        │                      │   → CAS
  │◄─ 200 OK (per object) ──────────────────────────────────────  │
  │                                        │  session: in-memory, TTL = 1 h
```

Sessions are kept in memory and swept by a background goroutine every minute.
Expired entries are also pruned inline on every `get` / `getByPayload` lookup,
so the store does not accumulate stale entries between cleanup sweeps.
A session that expires mid-transfer causes subsequent PUTs to return `401`;
the sender re-announces to obtain a fresh session and continues from where the
distribution log shows objects were last confirmed.

The session store is capped at **10,000 concurrent live sessions**.  If the
cap is reached, expired entries are pruned inline before a new session is
created.  If the store is still full after pruning, the announce request is
rejected with `429 Too Many Requests` until a slot becomes available.

The data channel uses unique per-PUT temporary files (`{finalPath}.{8-hex}.tmp`
opened with `O_EXCL`) so that concurrent PUTs targeting the same hash cannot
corrupt each other's in-progress write.  At startup the receiver performs a
background sweep of the CAS tree and removes any orphaned `.tmp` files left
behind by a previous crash before the atomic rename could complete.  The sweep
is context-cancellable so `Shutdown()` can interrupt it on a stalling
filesystem.

### 20.7 Backward Compatibility and Fallback

If the receiver does not support the announce protocol (e.g., an older deployment
or a third-party endpoint) it returns `404` on `POST /api/v1/announce`.  The
sender detects this and falls back to direct per-object PUTs without a session
token, using the existing `PUT {endpoint}/cvmfs-receiver/objects/{hash}` path
over HTTPS.  This mirrors the batch-endpoint fallback already present in the
distributor.

### 20.8 Configuration Reference

**Receiver** — started with `cvmfs-prepub --mode receiver`

| CLI flag | Default | Env var / notes |
|---|---|---|
| `--control-addr` | `:9100` | HTTPS listen address for announce requests |
| `--data-addr` | `:9101` | Plain HTTP listen address for object PUTs |
| `--data-host` | `` | Publicly reachable hostname returned to senders as the data endpoint; defaults to `localhost` if empty |
| `--tls-cert` | — | Path to TLS certificate for the control channel (required unless `--dev`) |
| `--tls-key` | — | Path to TLS private key for the control channel (required unless `--dev`) |
| `--cas-root` | `/var/lib/cvmfs-prepub/cas` | Local CAS root directory (e.g. `/srv/cvmfs/stratum1/cas`) |
| `--session-ttl` | `1h` | How long an announce session remains valid |
| `--disk-headroom` | `1.2` | Multiplier applied to `total_bytes` for the disk space pre-check |
| `--max-object-size` | `1073741824` (1 GiB) | Maximum body size in bytes for a single `PUT /api/v1/objects/{hash}`; requests exceeding this limit are rejected with `413` before any bytes touch disk |
| `--bloom-capacity` | `5000000` | Inventory Bloom filter capacity (must be consistent across all receivers the distributor compares) |
| `--bloom-fp-rate` | `0.001` | Inventory Bloom filter false-positive rate (0 < rate < 1) |
| `--coord-url` | `` | HepCDN coordination service base URL; empty disables coordination (see §22) |
| `--node-id` | hostname | Stable identifier for this receiver node reported to the coordination service |
| `--repos` | `` | Comma-separated list of CVMFS repositories served by this receiver (e.g. `atlas.cern.ch,cms.cern.ch`) |
| `--dev` | `false` | Allow plain HTTP control channel and skip HMAC verification (tests only — never use in production) |
| `--log-level` | `info` | Log level: `debug`, `info`, `warn`, `error` |

The HMAC shared secret is read from the **`PREPUB_HMAC_SECRET`** environment
variable.  It must be set to the same value on the publisher and every receiver.
In `--dev` mode the secret is not required and HMAC verification is disabled.

The coordination service bearer token is read from the **`PREPUB_COORD_TOKEN`**
environment variable.  It is required whenever `--coord-url` is set (except in
`--dev` mode, where a warning is emitted and the requests are unauthenticated).

TLS certificate and key paths are validated at startup (file existence checked
before any listener is bound); a missing file causes a clean error exit rather
than a mid-run failure.

### 20.9 Inventory Bloom Filter Endpoint

The control channel exposes the receiver's local object inventory as a
serialised Bloom filter at `GET /api/v1/bloom`.  The distributor (and the
coordination service) fetch this endpoint before pushing a batch of objects so
that they can skip objects the receiver already holds — **delta push mode**.

**Request** — `GET https://<s1-host>:<control-port>/api/v1/bloom`

| Header | Value |
|---|---|
| `Authorization` | `Bearer <token>` where `token = hex(HMAC-SHA256(HMACSecret, "bloom-read"))` |

The token is derived from the same `PREPUB_HMAC_SECRET` shared between the
publisher and receivers using a fixed label (`"bloom-read"`), so no per-request
timestamp negotiation is needed.  In `--dev` mode the check is bypassed.
A missing or invalid token returns `401 Unauthorized`.

**Response — 200 OK:**

- `Content-Type: application/octet-stream`
- Body: binary-encoded `bloom.BloomFilter` (the format produced by
  `bloom.BloomFilter.WriteTo` from `github.com/bits-and-blooms/bloom/v3`).
  Callers deserialise with `bloom.BloomFilter.ReadFrom`.

**Error responses:**

| Status | Meaning |
|---|---|
| `200 OK` | Filter ready; body is the binary-encoded Bloom filter |
| `401 Unauthorized` | Missing or invalid bearer token |
| `503 Service Unavailable` | Inventory still building (CAS walk in progress); `Retry-After: 10` header set |
| `405 Method Not Allowed` | Non-GET request |

**Inventory lifecycle:**

1. At receiver startup `populateFromCAS` walks the two-level CAS directory tree
   (`{cas_root}/{aa}/{aabbcc…}[C]`) and adds every found hash to the filter.
   While the walk is in progress `isReady()` returns `false` and the endpoint
   returns `503`.
2. After the walk completes `isReady()` becomes `true` and the endpoint returns
   the populated filter.
3. Every successful object PUT (§20.3) calls `inv.add(hash)` immediately after
   the atomic rename, keeping the filter current without waiting for a new walk.

**Delta push algorithm in the distributor:**

```
1.  announce(endpoint)              → session_token, data_endpoint
2.  GET /api/v1/bloom               → BloomFilter bf
3.  for hash in candidate_hashes:
        if bf.TestString(hash): skip   // probably already present
        else: push(hash)               // definitely absent
```

Bloom filters have **no false negatives**: if the filter reports a hash absent,
the object is definitely not at that receiver.  False positives (the filter
reports an object present when it is not) cause an object to be skipped; the
receiver will then fetch it during normal CVMFS replication as a lazy repair.
With the default parameters (5 M capacity, 0.1% FP rate) false positives are
rare enough to be operationally acceptable.

**Filter parameters** must be identical on all receivers that a single
distributor queries, otherwise the bit arrays are not comparable.  The defaults
(`--bloom-capacity 5000000`, `--bloom-fp-rate 0.001`) are suitable for
repositories with up to five million distinct objects per Stratum 1 node.

### 20.10 Prometheus Metrics Endpoint

The control channel also serves Prometheus metrics at `GET /metrics` in the
standard text exposition format.

Receiver-specific metrics:

| Metric | Type | Description |
|---|---|---|
| `cvmfs_receiver_objects_received_total` | Counter | CAS objects successfully received via PUT |
| `cvmfs_receiver_bytes_received_total` | Counter | Compressed bytes received via PUT (on-wire size) |
| `cvmfs_receiver_bloom_size` | Gauge | Approximate number of objects in the inventory Bloom filter |
| `cvmfs_receiver_heartbeat_errors_total` | Counter | Coordination service heartbeat errors |

The existing `cvmfs_prepub_*` metrics (jobs, pipeline, CAS upload durations)
are also registered and available at this endpoint.

**Sender (distributor)** — publisher-mode configuration

| Config key | Default | Description |
|---|---|---|
| `distribution.hmac_secret_env` | `PREPUB_HMAC_SECRET` | Environment variable holding the shared HMAC secret (must match receiver) |
| `distribution.session_ttl_margin` | `5m` | Re-announce this long before session expiry |

---

## 21. bits-console Integration

`bits-console` is a GitLab Pages single-page application that lets research
groups browse recipe repositories, trigger build pipelines, and monitor CVMFS
publication status.  It drives GitLab CI pipelines on self-hosted runners; the
`cvmfs-prepub` service can act as the publication backend for those pipelines,
replacing the separate `cvmfs-ingest` and `cvmfs-publisher` runner stages.

### 21.1 Where cvmfs-prepub Fits in the bits-console Pipeline

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
Stratum 1 pre-warming (Option B), gateway lease acquisition, and catalog commit
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
               [Option B: push to Stratum 1 receivers]
               lease → catalog commit → "published"
```

This eliminates the `bits-ingest` and `bits-cvmfs-publisher` runners and their
associated SSH key management, spool directory, and sequencing constraints.

### 21.2 Pipeline Comparison

| Aspect | cvmfs-publish.yml (three-stage) | cvmfs-prepub-publish.yml |
|---|---|---|
| **Runners required** | build + ingest + publisher (3 types) | build only |
| **Ingest mechanism** | `cvmfs-ingest` binary, rsync over SSH | REST API POST over HTTPS |
| **CVMFS transaction** | `cvmfs_server publish` on stratum-0 | `cvmfs_gateway` lease+payload API |
| **Stratum 1 pre-warming** | Not supported | Option B (configurable) |
| **Crash recovery** | Spool directory survives runner restart | WAL-journalled spool in cvmfs-prepub |
| **Deduplication** | None (re-uploads all blobs) | Bloom filter + CAS HEAD (cross-job) |
| **Provenance** | Not supported | Ed25519 + Rekor transparency log (§18) |
| **Parallel jobs** | Serialised by CVMFS transaction lock | Serialised by gateway lease; pipeline independent |
| **CI/CD variables** | `SPOOL_SSH_KEY`, `SPOOL_USER`, `SPOOL_HOST`, `SPOOL_PATH` | `PREPUB_URL`, `PREPUB_API_TOKEN` |

### 21.3 GitLab CI/CD Variables

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

### 21.4 Community ui-config.yaml Changes

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

### 21.5 The cvmfs-prepub Pipeline File

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

### 21.6 Access Control and Build Authorisation

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

### 21.7 Running Alongside Existing Pipelines

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
3. Add `.gitlab/cvmfs-prepub-publish.yml` to the bits-console repository (content from §21.5).
4. For each community switching to cvmfs-prepub, change `publish_pipeline` in `communities/<name>/ui-config.yaml` and merge the MR.
5. For Option B (Stratum 1 pre-warming), deploy the receiver on each Stratum 1 and add `distribution:` to the cvmfs-prepub config (INSTALL.md §6, REFERENCE.md §20).

---

## 22. Coordination Service Client Protocol

The receiver embeds a lightweight coordination service client
(`internal/distribute/receiver/coord_client.go`) that registers the node,
sends periodic heartbeats, and deregisters on shutdown.  Coordination is
disabled by default and enabled by setting `--coord-url`.

### 22.1 Purpose

The HepCDN coordination service maintains a live registry of receiver nodes,
their available repositories, and their inventory health.  It uses this
information to:

- Route pre-warm traffic to nodes that are ready and hold the relevant repository.
- Expose a topology map (`GET /route`) for WLCG site selection.
- Receive publish-announce notifications (`POST /announce`) from the publisher
  so that it can trigger pre-warming before the catalog flip.

The coordination service itself is a separate binary that is not yet part of
`cvmfs-bits`.  The client protocol described here defines the API it must
implement so that receiver deployments can register without code changes when
the service ships.

### 22.2 Registration

On startup (after both HTTP listeners are bound) the receiver POSTs to the
coordination service to register itself.

**Request** — `POST <coord_url>/api/v1/nodes`

```json
{
  "node_id":  "stratum1-cern-01",
  "repos":    ["atlas.cern.ch", "cms.cern.ch"],
  "data_url": "http://stratum1-cern-01.example.com:9101"
}
```

| Field | Description |
|---|---|
| `node_id` | Stable identifier for this node (defaults to OS hostname) |
| `repos` | Repositories this node holds; used for routing |
| `data_url` | Plain-HTTP data endpoint for object PUTs; the coord service may expose this to publishers for direct push |

| Header | Value |
|---|---|
| `Authorization` | `Bearer <PREPUB_COORD_TOKEN>` |
| `Content-Type` | `application/json` |

**Expected response:** `200 OK` or `201 Created`.

On failure the client logs an error, increments
`cvmfs_receiver_heartbeat_errors_total`, and continues.  The heartbeat loop
will retry registration implicitly (the coordination service should treat a
heartbeat from an unknown node as a re-registration trigger).

### 22.3 Heartbeat

Every 30 seconds the receiver sends a PUT to update its health state.

**Request** — `PUT <coord_url>/api/v1/nodes/{node_id}/heartbeat`

```json
{
  "bloom_size": 3821044,
  "ready":      true
}
```

| Field | Description |
|---|---|
| `bloom_size` | Approximate number of objects in the inventory Bloom filter (from `inv.approximateSize()`) |
| `ready` | `true` once the initial CAS walk has completed (`inv.isReady()`) |

| Header | Value |
|---|---|
| `Authorization` | `Bearer <PREPUB_COORD_TOKEN>` |
| `Content-Type` | `application/json` |

**Expected response:** `200 OK` or `204 No Content`.

On failure the client logs a warning and increments
`cvmfs_receiver_heartbeat_errors_total`.  The heartbeat loop continues
regardless of individual failures.

### 22.4 Deregistration

When the receiver receives a `SIGINT` or `SIGTERM` (or `Shutdown()` is called)
it sends a DELETE before closing the HTTP listeners.

**Request** — `DELETE <coord_url>/api/v1/nodes/{node_id}`

| Header | Value |
|---|---|
| `Authorization` | `Bearer <PREPUB_COORD_TOKEN>` |

**Expected response:** `200 OK`, `204 No Content`, or `404 Not Found`.

`404` is treated as success — it means the coordination service already expired
the registration (e.g., after a restart).  Any other non-success status is
logged as a warning; shutdown continues regardless.

The deregister call uses a hard 10-second deadline so that a slow coordination
service does not delay the overall receiver shutdown.

### 22.5 Client Configuration

| CLI flag | Env var | Description |
|---|---|---|
| `--coord-url` | — | Base URL of the coordination service; empty disables all coordination |
| `--node-id` | — | Stable node identifier; defaults to OS hostname |
| `--repos` | — | Comma-separated repository list sent during registration |
| — | `PREPUB_COORD_TOKEN` | Bearer token for authentication; required when `--coord-url` is set (unless `--dev`) |

### 22.6 Security Considerations

The `PREPUB_COORD_TOKEN` is read exclusively from the environment variable and
never appears in CLI flags, log lines, or the binary.  The coordination service
endpoint should be reached over HTTPS; the receiver's `sharedClient` enforces
TLS 1.2 as the minimum acceptable version for all outbound HTTPS connections.

When `--dev` is set and `PREPUB_COORD_TOKEN` is absent, the client sends
requests with an empty `Authorization: Bearer ` header.  This is logged as a
security warning and must never be used in production deployments.
