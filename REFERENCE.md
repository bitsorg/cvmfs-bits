# cvmfs-prepub Reference

This document is the reference for `cvmfs-prepub`: what each component does,
every configuration key and flag, the REST API, the Stratum 1 pull protocol,
the security model and the on-disk and wire formats. It describes the current
code and nothing else. Procedures (installing, deploying, rotating secrets,
troubleshooting) are in [INSTALL.md](INSTALL.md); the catalog format is in
[CATALOG.md](CATALOG.md).

## Contents

1. [Architecture](#1-architecture)
2. [Job lifecycle](#2-job-lifecycle)
3. [Publisher configuration](#3-publisher-configuration)
4. [Receiver configuration](#4-receiver-configuration)
5. [REST API](#5-rest-api)
6. [Pull distribution protocol](#6-pull-distribution-protocol)
7. [Security model](#7-security-model)
8. [Provenance](#8-provenance)
9. [Metrics and logs](#9-metrics-and-logs)
10. [Formats](#10-formats)

---

## 1. Architecture

`cvmfs-prepub` is one binary with two modes. In **publisher** mode (the
default) it runs next to a CVMFS Stratum 0: it accepts package tars over HTTP,
turns them into CVMFS objects and catalogs, and commits them through the
repository's `cvmfs_gateway` (or `cvmfs_server`). In **receiver** mode it runs
on a Stratum 1 and pulls a release's objects into the local store before or
right after the commit, so the replica is warm when clients ask for the new
files.

### Components

| Component | Where | Role |
|---|---|---|
| API server | publisher, `--listen` (default `:8080`, plain HTTP) | Job submission, status, SSE events, builds, reserve/published checks, measurements, health, metrics, web console; also serves objects, manifests and discovery to receivers |
| Orchestrator | publisher | Runs each job through its states, takes the gateway lease, commits, retries, recovers after a restart |
| Spool | publisher, `--spool-root` | Crash-safe job store: one directory per job, moved between per-state directories |
| Pipeline | publisher (default path, gateway mode) | Unpacks the tar, chunks and compresses files, writes new objects to the CAS, builds the catalog entries |
| CAS writer | publisher | Writes objects straight into the repository's storage (`cas.type: localfs` or `s3`) |
| Lease client | publisher | Signed calls to the `cvmfs_gateway` HTTP API |
| `cvmfs_server` | publisher host | Used by the `local` backend (`transaction`/`publish`) and by the `ingest` path (`cvmfs_server ingest`) |
| `cvmfs_swissknife ingestsql` | publisher host | Commits a whole coarse build in one transaction (finalize) |
| Embedded MQTT broker | publisher, `--embedded-broker-ws-addr` | Control plane for Stratum 1 receivers (MQTT over WebSocket, optionally TLS and token auth) |
| Enrollment endpoint | publisher, API listener or `--enroll-tls-addr` | Exchanges a receiver's node key for a short-lived broker token |
| Receiver | Stratum 1, `--mode receiver` | Subscribes to the broker, pulls objects into its local CAS; only listener is `/metrics` on `--control-addr` |
| Rekor (optional) | external | Transparency log for provenance records (`--provenance`) |

The architecture diagram is in [README.md](README.md#architecture).

### Publish backends and paths

`--publish-mode` selects the **default** backend. A job may name another
**publish path** with the `publish_path` field; a path the node does not offer
is rejected at submission (`400`), never silently replaced. The paths a node
offers are listed in the startup log and in `publish_paths` of
`GET /api/v1/health`.

| Path name | Offered when | What happens | Notes |
|---|---|---|---|
| `prepub` (default; also the empty name) with `publish_mode: gateway` | always in gateway mode | Pipeline before the lease: chunk, compress, dedup (`CAS.Exists` per object), write objects to the CAS, build the subtree catalog(s); then lease, upload catalog(s), commit | The only path that can pre-warm Stratum 1s before the commit and the only path that accumulates coarse builds |
| `prepub` with `publish_mode: local` | always in local mode | `cvmfs_server transaction <repo>`, extract the tar under `<cvmfs_mount>/<repo>/<path>`, `cvmfs_server publish <repo>` | No gateway, no CAS, no pipeline; runs on the Stratum 0 with the repository mounted |
| `ingest` | `--ingest-publish` | `cvmfs_server ingest -t <tar> -b <path> [-c] [-u <owner>] [--direct-s3 [--s3-config <file>] [--object-list]] <repo>`; the gateway does chunking, dedup and catalogs | Needs `cvmfs_server` on `PATH` and a gateway registration per repository (`cvmfs_server connect-gw`, mountless or mounted; `install.sh` does it); one gateway transaction per package; with `direct_s3`, `object_list` and `prewarm`, pre-warms right after the commit |
| `staged` | gateway mode with `cas.type: s3` | A producer has already written the objects under an S3 `staging_prefix` and built the catalog (`catalog_hash`); cvmfs-prepub promotes the objects into the store with server-side copies and grafts the catalog | No tar payload; needs a gateway with the graft endpoint; always grafts |

The commit granularity follows from the path: `ingest`, `staged` and the
`local` backend commit each package as it arrives; the gateway-mode `prepub`
path commits each package too, unless the job is part of a coarse build (see
[Coarse builds](#coarse-builds)). How to set each path up is in
[INSTALL.md](INSTALL.md#4-publish-backends-and-paths).

A gateway-mode `prepub` publish is **replace-all** for its path: the subtree
catalog is built only from the tar, so a file that was published under the
path before and is missing from the tar disappears. Unchanged files are
deduplicated by content, not by leaving them out of the tar.

### Gateway API used

All gateway requests carry `Authorization: <key_id> <HMAC-SHA256>` with the key
from `CVMFS_GATEWAY_KEY_ID` / `CVMFS_GATEWAY_SECRET`. The secret never travels.

| Request | Used for |
|---|---|
| `GET /api/v1/repos` | Startup probe |
| `POST /api/v1/leases` | Acquire a lease on `<repo>/<path>` (path in the JSON body); `path_busy` is retried every second |
| `PUT /api/v1/leases/{token}` | Lease heartbeat every 10 s; a `405` answer (stock gateway) stops the heartbeat, three consecutive other failures abort the job |
| `POST /api/v1/payloads` | Upload the subtree catalog(s) (and the nested-catalog marker object when one was synthesized) |
| `POST /api/v1/leases/{token}` | Commit (standard path: the gateway diffs the new catalog against the published one) |
| `POST /api/v1/leases/{token}/graft` | Commit by grafting the pre-built subtree catalog (DirectGraft) |
| `DELETE /api/v1/leases/{token}` | Abort/release a lease (failure paths, `POST /api/v1/reserve`) |

Data objects do **not** go through the gateway on the default path: they are
already in the repository's storage when the lease is taken. This is why
`cas.root` (localfs) must be the repository's storage, or `cas.type: s3` must
point at the repository's bucket (it is read from the repository's own
`server.conf`).

### DirectGraft

With `gateway.direct_graft: true` (the default) the commit goes to the graft
endpoint: the gateway grafts the subtree catalog cvmfs-prepub built at the lease
path instead of diffing it. The graft endpoint is not in stock
`cvmfs_gateway` releases (it comes with cvmfs PR #4296); against a stock
gateway set `gateway.direct_graft: false` (or `--gateway-direct-graft=false`).
Grafting is only correct when the lease path holds no published content yet
(a new version directory); the standard diff path is correct in every case
but slower. Staged jobs always graft, whatever this setting says.

### Coexistence with cvmfs_server publish

`cvmfs-prepub` does not take over a repository. Other publishers
(`cvmfs_server publish`, `cvmfs_server ingest`, another cvmfs-prepub instance) can
keep publishing through the same gateway; the gateway's per-path lease
arbitrates, and a cvmfs-prepub job that meets `path_busy` waits and retries.
Within one cvmfs-prepub instance, commits to the same
repository are serialised by a per-repository lock (different repositories
commit in parallel). Clients and Stratum 1s see an ordinary CVMFS repository:
the catalog format is CVMFS's own ([CATALOG.md](CATALOG.md)).

### Comparison

| | `cvmfs_server publish` | `cvmfs_server ingest` | `cvmfs-prepub` (default path) |
|---|---|---|---|
| Input | File tree in the transaction overlay | Local tar file | Tar over HTTP (multipart), or a tar already in `--staging-root` |
| Lock held during processing | Yes | Yes (its own transaction or lease) | No: the lease is taken after compress/hash/upload |
| Who needs shell access to the publisher | Release manager | Release manager | Nobody: an API secret is enough |
| Stratum 1 pre-warming | No | No | Optional, opt-in (`--prewarm` on the node, `prewarm` per job) |
| Crash-safe job queue with retries | No | No | Yes: spool journal; interrupted jobs are re-run, failed attempts retried |
| Status | Exit code, log | Exit code, log | REST API, SSE, webhooks, Prometheus metrics |
| Build identity | Unix user | Unix user | Optional OIDC-verified CI identity and Rekor record |

---

## 2. Job lifecycle

### States

A job's state is the name of the spool directory that holds it.

| State | Meaning | Terminal |
|---|---|---|
| `incoming` | Accepted; waiting for a concurrency slot, or waiting for a retry (`next_attempt_at` set) | no |
| `staging` | Pipeline running (unpack, chunk, compress, dedup, CAS upload) | no |
| `uploading` | Pipeline finished; objects are in the CAS | no |
| `distributing` | Only when the embedded broker is configured: pull manifest stored and (if pre-warming applies) announce sent. The job does not wait here | no |
| `leased` | Building the subtree catalog, holding or acquiring the gateway lease, waiting for the per-repository commit lock | no |
| `committing` | Commit request in flight (or, for a finalize job, the build's `ingestsql` commit) | no |
| `published` | Committed (or found already published, see `identity_path`) | yes |
| `accumulated` | Coarse-build member: entries recorded, the commit is left to the build's finalize | yes |
| `failed` | Failed permanently, retry window exhausted, or aborted by an operator | yes |
| `aborted` | Defined for compatibility; no current code path enters it (an operator abort ends in `failed`) | yes |

Transitions by path:

```
prepub, gateway mode:  incoming -> staging -> uploading [-> distributing] -> leased -> committing -> published
coarse member:         incoming -> staging -> uploading [-> distributing] -> accumulated
finalize job:          incoming -> committing -> published
local, ingest, staged: incoming -> leased -> committing -> published
any non-terminal state -> failed
```

A retryable failure does not end in a terminal state: the job goes back to
`incoming` with `attempts`, `last_error` and `next_attempt_at` set
([Retries](#retries)). Each transition is published to SSE subscribers
([Server-Sent Events](#server-sent-events)).

### Spool layout

```
<spool_root>/                         mode 0700
  incoming/ staging/ uploading/ distributing/ leased/
  committing/ accumulated/ published/ failed/ aborted/
    journal.jsonl                     transitions out of this state, one CRC-prefixed JSON line each
    <job-id>/
      manifest.json                   the job record (see GET /api/v1/jobs/{id}/log)
      payload.tar                     the submitted tar; deleted when the job reaches a terminal state
      upload.log                      CAS upload log of the pipeline
      catalog.db                      pipeline catalog scratch (prepub path)
      provenance-record.json          exact signed provenance record, mode 0600 (--provenance)
  builds/<build-id>/                  coarse-build accumulator
    <job-id>.json                     one member's catalog entries
    <job-id>.failed                   a member that failed
    _expect                           declared package count (build_expect or seal)
    _finalizing                       finalize claim marker
  builds/<build-id>.result.json       finalize outcome (kept after the accumulator is removed)
  manifests/<txn>.json                pull manifests served at /s1/{txn}/manifest (newest 8192 kept)
  measurements/<build-id>.ndjson      per-publish measurement records (nobuild-YYYYMMDD.ndjson without a build id)
  tmp/                                TMPDIR for this process and its children (unless TMPDIR is set and usable)
  provenance.key                      generated Rekor signing key (when --provenance and no --rekor-signing-key)
  revoked-nodes.json                  persisted receiver denylist (--embedded-broker-auth)
  .clean-shutdown                     written last on a clean exit, consumed at the next start
```

A transition appends to the source state's `journal.jsonl`, fsyncs, renames
the job directory into the target state directory, fsyncs again and rewrites
`manifest.json`. Journal lines have the form `<crc32-hex> <json>` with the
fields `t`, `job_id`, `from`, `to`, `run_id` and optionally `note`.

### Admission and concurrency

| Limit | Default | Effect |
|---|---|---|
| Dynamic job slots | `min_concurrent_jobs: 4`, `max_concurrent_jobs: 0` (= CPU count) | Effective slots = `max(min, max - load1)`, recomputed from `/proc/loadavg` every 5 s. A job costs `ceil(tar_size / 128 MiB)` slots, capped at the effective limit; waiting jobs are served largest tar first. The slot is released when the pipeline ends, before the commit. `--min-concurrent-jobs 0` disables the limiter |
| Tar look-ahead | `pipeline.prefetch: true`, `prefetch_limit: 8` | The tar scan starts at submission, before the job has a slot, within a budget of 8 x 128 MiB; over budget it runs inline under the job's slot |
| Per-repository commit lock | always | One commit per repository at a time within this instance |
| Gateway lease | gateway | `path_busy` is retried every second ([Publish backend and gateway](#publish-backend-and-gateway)) |
| Job timeout | `job_timeout: 0` (off) | When set, counted from slot acquisition; a timed-out job is failed (or retried) |

Jobs resumed by crash recovery go through the same slot limiter and tar
look-ahead as new submissions; a job waiting to retry first waits for its
`next_attempt_at`.

### Retries

A failed attempt is retried when all of these hold: `retry_window` is not
zero, the job is not a coarse member or a finalize job, it was not aborted by
an operator, the error is not permanent, and the next attempt still falls
inside `created_at + retry_window` (default 24 h).

Permanent errors: errors classified permanent, archives that break the tar
rules ([Tar archive rules](#tar-archive-rules)), commits that fail on a
`UNIQUE constraint` (content already published), and payloads `cvmfs_server`
reports as `Impossible to open the archive`. Everything else (network,
gateway, storage, timeouts, unknown tool failures) is retried.

Backoff: 1, 2, 4, 8, 16 minutes, then every 30 minutes. A waiting job sits in
`incoming/` with `next_attempt_at` set; it keeps its payload and any gateway
lease it held is released first. `cvmfs_prepub_spool_jobs_waiting_retry`
counts these jobs.

### Recovery after a restart

At startup every job in a non-terminal state is recovered:

1. A job in `incoming` with `next_attempt_at` set simply resumes waiting.
2. If the previous exit was **not** clean (no `.clean-shutdown` marker), the
   attempt is counted: after 3 such recoveries the job is failed.
3. After a clean shutdown the interruption is counted separately: after 20
   the job is failed.
4. Any recorded gateway lease is aborted, the job is moved back to
   `incoming` and run again from the start (each step is idempotent), under
   the normal [admission](#admission-and-concurrency).

On `SIGTERM`/`SIGINT` the publisher stops accepting requests, waits up to
30 s for running jobs (recovered ones included), auto-finalizes and webhook
deliveries, then writes `.clean-shutdown`. Jobs still queued for a slot stay
in `incoming`. Jobs still running at that point are recovered at the next
start as interrupted, not as crashed.

### Coarse builds

A coarse build publishes all packages of one CI run in a single commit. The
decision is made once per job at submission:

- A job with a `build_id` on the default `prepub` path is coarse unless it
  sends `coarse=false`. `coarse=true` on another path is rejected; `coarse`
  without `build_id` is rejected.
- A coarse job runs the pipeline (objects go to the CAS, and are pre-warmed if
  enabled), records its catalog entries under `builds/<build-id>/` and ends in
  `accumulated`. It is not retried.
- Accumulation needs the gateway-mode pipeline and a non-empty `path`. In
  local mode (`publish_mode: local`) no job is coarse, whether inferred from
  `build_id` or sent with `coarse=true`: each is published on arrival, keeps
  its retries, and `build_expect` is not recorded. A coarse job with a
  root-level path is published on its own.

The build is finalized by one `cvmfs_swissknife ingestsql` run against the
gateway, configured with `ingest_config_prefix`, `ingest_swissknife` and
`ingest_env`. Without `ingest_config_prefix`, or in local mode, the finalize
cannot run (`finalize_ready: false` in health; a warning at startup when the
prefix is missing). Three triggers:

| Trigger | When |
|---|---|
| Declared count | `build_expect` on the submissions, or `POST /api/v1/builds/{id}/seal`: when the number of terminal members (accumulated + failed) reaches the count, cvmfs-prepub finalizes by itself |
| Explicit | `POST /api/v1/builds/{id}/finalize` |
| Finalize job | `POST /api/v1/jobs` with `finalize=true` and `build_id` |

Rules applied by the finalize:

- If any member failed, an auto-finalize does **not** publish; it records an
  error result. `POST /api/v1/builds/{id}/finalize` publishes the partial set
  deliberately.
- Before committing, up to 200 sampled objects are checked in the CAS; if any
  is missing the finalize fails without committing.
- Two members at the same path with the same `tar_sha256` are deduplicated;
  a member at an already used path with different content is left out and
  reported as a conflict.
- On success the accumulator is removed. An auto-finalize records its outcome
  in `builds/<id>.result.json` (the `result` of `GET /api/v1/builds/{id}`). If
  it fails before committing, its claim is released and the next member to
  finish (or a new seal) triggers it again; if it fails during the commit, the
  `_finalizing` claim stays and an operator decides what to do (for example
  `POST /api/v1/builds/{id}/finalize`).
- An auto-finalize runs detached, bounded by `job_timeout` or, when that is
  0, by 2 hours. Concurrent finalizes of the same build are serialised.
- A finalize does not send the post-commit `published` broker message and
  writes no provenance record.

---

## 3. Publisher configuration

### Sources and precedence

Every setting is a command-line flag. Most also have a key in the YAML file
given with `--config` (the installed unit passes only
`--config /etc/cvmfs-prepub/config.yaml`). A few tuning flags also take their
default from an environment variable. Precedence, highest first:

1. a flag set on the command line;
2. the YAML key;
3. the environment variable (only where listed);
4. the built-in default.

YAML rules:

- Keys use the nesting shown in the tables (`server.listen` is `listen:`
  under `server:`). Unknown keys are ignored without a warning, so check
  spelling against these tables.
- An empty string or a numeric `0` counts as "not set" and leaves the flag's
  default in place. Exceptions: `retry_window: 0` and `spool_min_free_gib: 0`
  are applied (they disable retries and the free-space check); leave the key
  out to keep the default. Any other number set to zero needs the flag.
- Boolean keys are applied when present, so `false` works
  (`gateway.direct_graft: false`, `pipeline.prefetch: false`).
- List keys (`repos`, `oidc_issuers`, `allowed_publish_prefixes`,
  `ingest_env`) are YAML lists; the flag takes the same values comma-separated.
- Durations use Go syntax: `90s`, `10m`, `24h`.

Flags marked "CLI only" have no YAML key; add them to the unit's `ExecStart`.

A complete example configuration is in
[INSTALL.md](INSTALL.md#3-deploy-a-publisher).

### General

| YAML key | Flag | Env | Default | Meaning |
|---|---|---|---|---|
| `mode` | `--mode` | | `publisher` | `publisher` or `receiver` |
| `log_level` | `--log-level` | | `info` | `debug`, `info`, `warn`, `error` (unknown values mean `info`) |
| `dev` | `--dev` | | `false` | Development mode: allows an empty `PREPUB_API_TOKEN` (API unauthenticated), an unset `CVMFS_GATEWAY_SECRET` (insecure placeholder) and a plaintext gateway URL. Never in production |
| | `--config` | | | Path of the YAML file |
| `broker_ca_cert` | `--broker-ca-cert` | | system pool | PEM CA used to verify the broker's TLS certificate (the publisher's own loopback broker clients use it too). Receiver use: [section 4](#4-receiver-configuration) |

### API server

| YAML key | Flag | Env | Default | Meaning |
|---|---|---|---|---|
| `server.listen` | `--listen` | | `:8080` | API listen address. Plain HTTP; put TLS in a reverse proxy |
| `server.auth_mode` | `--auth-mode` | | `both` | `bearer`, `both` or `hmac`; see [Authentication](#authentication). An invalid value stops startup |
| `server.signature_skew` | `--signature-skew` | | `2m` | How old a signed request may be; nonces are kept for twice this. Future timestamps get a fixed 15 s |
| `server.debug_listen` | `--debug-listen` | | off | `net/http/pprof` listener (`/debug/pprof/`). Bind to loopback only: profiles contain heap contents |
| `allowed_publish_prefixes` | `--allowed-publish-prefix` | | off | Full CVMFS paths (`/cvmfs/<repo>/<group>`) a job, reserve or published check may target; others get `403`. Empty disables the check |

### Spool and uploads

| YAML key | Flag | Env | Default | Meaning |
|---|---|---|---|---|
| `spool_root` | `--spool-root` | | `/var/spool/cvmfs-prepub` | Spool directory ([Spool layout](#spool-layout)). Also the parent of `tmp/`, `builds/`, `manifests/`, `measurements/` |
| `staging_root` | `--staging-root` | | off | Directory from which JSON submissions may reference a tar (`tar_path`). Empty disables JSON submissions (`503`) |
| `max_tar_size_gib` | `--max-tar-size-gib` | | `10` | Largest tar one submission may carry; larger uploads get `413`. Also the largest single file inside a tar on the default fixed chunk grid ([Tar archive rules](#tar-archive-rules)) |
| `spool_min_free_gib` | `--spool-min-free-gib` | | `20` | Free space an upload must leave on the spool filesystem, else `507`. `0` disables the check |
| `measurements_dir` | `--measurements-dir` | | `<spool_root>/measurements` | Measurement records ([Measurements](#get-apiv1measurements)); `off` disables them |
| `catalog_cache_dir` | `--catalog-cache-dir` | | `$CACHE_DIRECTORY/catalogs` under systemd (the installed unit sets `CacheDirectory=cvmfs-prepub`), else `<spool_root>/catalog-cache` | Published catalogs downloaded for existence and hash checks, kept by hash (a catalog never changes under its hash); only the manifest is read fresh. Prefer local disk. `off` disables |
| `catalog_cache_mib` | `--catalog-cache-mib` | | `1024` | Size limit of the catalog cache; least recently used catalogs are removed beyond it |

### Publish backend and gateway

| YAML key | Flag | Env | Default | Meaning |
|---|---|---|---|---|
| `publish_mode` | `--publish-mode` | | `gateway` | Default backend: `gateway` (cvmfs_gateway API) or `local` (`cvmfs_server` on this host) |
| `gateway.url` | `--gateway-url` | | `https://localhost:4929` | Gateway base URL. Must be HTTPS unless it is loopback (`http://localhost`, `http://127.0.0.1`, `http://[::1]`) or `allow_plaintext` is set |
| `gateway.allow_plaintext` | `--gateway-allow-plaintext` | | `false` | Permit a plaintext non-loopback gateway URL on a trusted network (requests stay HMAC-signed) |
| `gateway.direct_graft` | `--gateway-direct-graft` | | `true` | Commit through the graft endpoint ([DirectGraft](#directgraft)); `false` for a stock gateway |
| | `--lease-retry-max` (CLI only) | | `0` (= 12 min) | How long to keep retrying a `path_busy` lease; set above the gateway's `max_lease_time` |
| `stratum0_url` | `--stratum0-url` | | empty | Stratum 0 HTTP base including `/cvmfs`, e.g. `http://stratum0.example.org/cvmfs`. Needed (gateway mode) to build subtree catalogs, to read the current root hash, for `POST /api/v1/published`, the already-published check of `POST /api/v1/reserve`, `identity_path` and `replace_on_conflict` |
| `repo_name` | `--repo-name` | | empty | Repository name. Used to find `server.conf` for `cas.type: s3` and as the repository listed in the discovery document. An invalid name ([Conventions](#conventions)) stops startup |
| `cvmfs_mount` | `--cvmfs-mount` | | `/cvmfs` | Repository mount root for the `local` backend and the base for `ingest -b` |
| `replace_on_conflict` | `--replace-on-conflict` | | `false` | Allow jobs that send `replace=true` to replace what another build published at their own path: when the published hash differs from `identity_hash`, delete the subtree in its own transaction, then commit. Jobs that do not ask are never replaced, and a failed commit never deletes anything. Destructive; works for the `ingest` and `staged` paths (needs `--ingest-publish` for `cvmfs_server`) |
| `prewarm` | `--prewarm` | | `false` | Make Stratum 1 pre-warming available; jobs opt in with `prewarm`. Set by `install.sh --prewarm` / `--no-prewarm` |

Gateway credentials are environment variables only:
`CVMFS_GATEWAY_KEY_ID` (default `cvmfs-prepub`) and `CVMFS_GATEWAY_SECRET`
(required in gateway mode unless `--dev`).

### CAS

| YAML key | Flag | Env | Default | Meaning |
|---|---|---|---|---|
| `cas.type` | `--cas-type` | | `localfs` | `localfs` or `s3` (gateway mode only) |
| `cas.root` | `--cas-root` | | `/var/lib/cvmfs-prepub/cas` | localfs: the repository's storage directory (objects under `data/xx/...`). Receiver: its CAS root |
| `cas.server_conf` | `--cas-server-conf` | | `/etc/cvmfs/repositories.d/<repo_name>/server.conf` | For `s3`: a `server.conf` whose `CVMFS_UPSTREAM_STORAGE` names the S3 config file that supplies endpoint, bucket, alias and credentials; `install.sh --s3-conf-from` writes prepub's own, which names itself ([INSTALL.md](INSTALL.md#step-2--repository-credentials)). The direct-S3 ingest gets that S3 config as `--s3-config` and writes objects under its `CVMFS_S3_REPO_ALIAS`, which install.sh sets to the alias; startup fails if the file names another one, or if neither this nor `repo_name` is set |
| `promote_workers` | `--promote-workers` | `PREPUB_PROMOTE_WORKERS` | `16` | Concurrent server-side copies when promoting a staged job's objects; must be >= 1, values above 256 are clamped |

### Optional paths and coarse finalize

| YAML key | Flag | Env | Default | Meaning |
|---|---|---|---|---|
| `ingest_publish` | `--ingest-publish` | | `false` | Offer the `ingest` path. Startup fails if `cvmfs_server` is unusable |
| `ingest_publish_owner` | `--ingest-publish-owner` | | tar ownership | Owner passed as `cvmfs_server ingest -u` |
| `ingest_config_prefix` | `--ingest-config-prefix` | | empty (finalize off) | `ingestsql -C` gateway-client config directory. Required for coarse finalize |
| `ingest_swissknife` | `--ingest-swissknife` | | `cvmfs_swissknife` | Path of `cvmfs_swissknife` for the finalize |
| `ingest_env` | `--ingest-env` | | empty | Extra environment for the finalize, e.g. `LD_LIBRARY_PATH=/opt/cvmfs/lib` |

### Jobs and concurrency

| YAML key | Flag | Env | Default | Meaning |
|---|---|---|---|---|
| `min_concurrent_jobs` | `--min-concurrent-jobs` | `PREPUB_MIN_CONCURRENT_JOBS` | `4` | Guaranteed job slots; `0` (flag or env) disables the limiter |
| `max_concurrent_jobs` | `--max-concurrent-jobs` | `PREPUB_MAX_CONCURRENT_JOBS` | `0` (= CPU count) | Slot ceiling ([Admission and concurrency](#admission-and-concurrency)) |
| `job_timeout` | `--job-timeout` | | `0` (off) | Wall-clock limit per job, counted from slot acquisition |
| `retry_window` | `--retry-window` | | `24h` | How long after submission retryable failures are retried; `0` disables retries |

### Pipeline

| YAML key | Flag | Env | Default | Meaning |
|---|---|---|---|---|
| `pipeline.workers` | `--pipeline-workers` | `PREPUB_PIPELINE_WORKERS` | `4` | Compress workers per job. Peak memory scales with this: on the default fixed grid each worker streams one grid block (about 2 x 6 MiB); a file is held whole in memory only with content-defined chunking, or when the unpacker kept it in memory (with `prefetch`, entries over 64 KiB are spilled to disk; without it, entries up to 1 GiB stay in memory) |
| `pipeline.upload_concurrency` | `--pipeline-upload-conc` | `PREPUB_PIPELINE_UPLOAD_CONC` | `4` | Dedup+upload workers per job |
| | `--pipeline-compress-level` (CLI only) | | `0` (= zlib 6) | zlib level 1-9 |
| `pipeline.prefetch` | `--prefetch` | | `true` | Scan the tar ahead of the job's slot; turn off on I/O-bound spool storage |
| `pipeline.prefetch_limit` | `--prefetch-limit` | | `8` | Look-ahead budget in units of 128 MiB of tar |
| `chunking.min` | `--chunk-min` | | `6291456` | Chunk size minimum, bytes |
| `chunking.avg` | `--chunk-avg` | | `6291456` | Chunk size average, bytes; `0` (flag only) disables chunking |
| `chunking.max` | `--chunk-max` | | `6291456` | Chunk size maximum, bytes |

Keep the fixed 6 MiB grid (min = avg = max) whenever coarse builds are used;
`ingestsql` requires it. Content-defined sizes are for deployments that never
publish coarse builds ([Chunking and compression](#chunking-and-compression)).

### Stratum 1 distribution

All CLI only, except `--prewarm` (config key `prewarm`). Their use is described in
[INSTALL.md](INSTALL.md#7-stratum-1-pre-warming); the protocol is in
[section 6](#6-pull-distribution-protocol).

| Flag | Default | Meaning |
|---|---|---|
| `--embedded-broker-ws-addr` | off | Run the MQTT broker (WebSocket listener) at this `host:port` or `:port` (a value without a port stops startup). The publisher's own clients connect to `ws(s)://localhost:<port>` |
| `--embedded-broker-tls-cert`, `--embedded-broker-tls-key` | off | Broker certificate and key; enables `wss://`. The certificate must also be valid for `localhost` and trusted through `--broker-ca-cert`, because the publisher connects to its own broker that way |
| `--embedded-broker-auth` | `false` | Require tokens on the broker; enables enrollment. Needs `PREPUB_HMAC_SECRET` (>= 16 bytes) |
| `--enroll-tls-addr` | off | Serve `/control/challenge`, `/control/enroll`, `/control/revoke` and `/control/unrevoke` over HTTPS at this address with the broker certificate (needs `--embedded-broker-auth` and the broker cert). Without it enrollment is served on the API listener, and revocation is available only through `POST /api/v1/control/revoke` and `POST /api/v1/control/unrevoke` |
| `--enroll-url` | empty | HTTPS base of `--enroll-tls-addr` as receivers reach it; advertised in discovery |
| `--control-plane-url` | empty | Broker URL advertised to receivers, e.g. `wss://s0.example.org:1882`. Setting it mounts the discovery document and requires `--discovery-signing-key`; a `wss://` URL requires the broker cert |
| `--discovery-signing-key` | empty | PEM PKCS#8 Ed25519 private key that signs the discovery document |
| `--pull-object-base-url` | empty | Externally reachable base for object GETs, written into pull manifests as `{url}/cvmfs/{repo}/data`. Without it no pull manifest is stored and announces cannot be acted on |
| `--prewarm` | `false` | Make pre-warming available (config key `prewarm`, set by `install.sh --prewarm`); only jobs that send `prewarm=true` are pre-warmed |

### Provenance

| YAML key | Flag | Env | Default | Meaning |
|---|---|---|---|---|
| `provenance` | `--provenance` | | `false` | Record provenance and submit it to Rekor ([section 8](#8-provenance)) |
| `rekor_server` | `--rekor-server` | | `https://rekor.sigstore.dev` | Rekor base URL |
| `rekor_signing_key` | `--rekor-signing-key` | | `<spool_root>/provenance.key` | PEM PKCS#8 Ed25519 key; generated when the file does not exist |
| `oidc_issuers` | `--oidc-issuers` | | empty | Accepted CI OIDC issuers. With issuers set, `PREPUB_OIDC_AUDIENCE` is mandatory (startup fails otherwise) |

### Environment variables

| Variable | Used by | Meaning |
|---|---|---|
| `PREPUB_API_TOKEN` | publisher, `revoke --api-url` | The API secret: compared as a bearer token and used as the HMAC key for `X-Bits-Auth`. Required unless `--dev` |
| `CVMFS_GATEWAY_SECRET` | publisher, gateway mode | Gateway HMAC secret. Required unless `--dev` |
| `CVMFS_GATEWAY_KEY_ID` | publisher, gateway mode | Gateway key id; default `cvmfs-prepub`; must match the gateway key file |
| `PREPUB_HMAC_SECRET` | publisher (`--embedded-broker-auth`), `node-key`, `revoke --enroll-url` | Control-plane master secret, >= 16 bytes: signs broker tokens and derives node keys. Never give it to a receiver |
| `S1_NODE_KEY` | receiver (`--broker-auth`) | The receiver's own enrollment key (hex), from `cvmfs-prepub node-key <node>` |
| `PREPUB_OIDC_AUDIENCE` | publisher (`--provenance` with issuers) | Audience OIDC tokens must carry |
| `PREPUB_PROMOTE_WORKERS`, `PREPUB_MIN_CONCURRENT_JOBS`, `PREPUB_MAX_CONCURRENT_JOBS`, `PREPUB_PIPELINE_WORKERS`, `PREPUB_PIPELINE_UPLOAD_CONC` | publisher | Defaults for the matching flags (integers; invalid values are ignored) |
| `TMPDIR` | publisher | Honoured if it names a writable directory; otherwise set to `<spool_root>/tmp` for the process and its children |

The installed units read secrets from `/etc/cvmfs-prepub/env`
([INSTALL.md](INSTALL.md#5-api-authentication-and-secrets)).

### Subcommands

| Command | Meaning |
|---|---|
| `cvmfs-prepub [flags]` | Run the service (`--mode publisher` or `--mode receiver`) |
| `cvmfs-prepub --version` | Print `cvmfs-prepub <version>` and exit. The version is the one `make build` stamps in, else the Go toolchain's VCS stamp (`<12-char revision>[-dirty] (<commit time>)`), else `dev`. The startup log line `starting cvmfs-prepub` carries it as `version` |
| `PREPUB_HMAC_SECRET=<master> cvmfs-prepub node-key <node>` | Print the receiver's enrollment key, `hex(HMAC-SHA256(master, node))`. `publisher` and the empty name are refused. Provision the output as that receiver's `S1_NODE_KEY` |
| `PREPUB_HMAC_SECRET=<master> cvmfs-prepub revoke [--undo] <node> [--enroll-url https://host:8443] [--ca-cert ca.pem]` | Revoke a receiver (`POST /control/revoke`) or, with `--undo`, lift the revocation (`POST /control/unrevoke`) through the TLS enroll endpoint (default `https://localhost:8443`), with a one-minute publisher token. `--ca-cert` is the only CA trusted |
| `PREPUB_API_TOKEN=<token> cvmfs-prepub revoke [--undo] <node> --api-url http://host:8080` | The same through `POST /api/v1/control/revoke` (`--undo`: `POST /api/v1/control/unrevoke`) on the API listener, signed with `X-Bits-Auth` (so `auth_mode` must be `both` or `hmac`) |

Two helper programs are built from the same module but are not part of the
service: `cmd/prepub-finalize` (runs a coarse finalize from a spool on a
release-manager host: `-spool-root`, `-build`, `-swissknife`,
`-config-prefix`, `-lease-path`, `-keep`) and `cmd/distbench` (a benchmark of
pull bundling). `make build` builds only `bin/cvmfs-prepub`, stamping the
version from `git describe --tags --always --dirty` (override with
`make build VERSION=...`).

---

## 4. Receiver configuration

A receiver is `cvmfs-prepub --mode receiver`. It has no HTTP API: its only
listener serves Prometheus metrics. It learns the broker URL from the signed
discovery document, enrolls for a token, subscribes to the repositories it
serves and pulls objects into `--cas-root`.

| YAML key | Flag | Default | Meaning |
|---|---|---|---|
| `mode` | `--mode` | `publisher` | Must be `receiver` |
| `log_level` | `--log-level` | `info` | As for the publisher |
| `control_addr` | `--control-addr` | `:9100` | Plain-HTTP listener for `GET /metrics` |
| `node_id` | `--node-id` | host name | Stable node id: MQTT client id (`<node_id>-receiver`), presence topic, enrollment identity. Must not contain `/`, `+`, `#` or NUL |
| `repos` | `--repos` | required | Repositories this receiver serves (valid names, see [Conventions](#conventions)); an empty list or an invalid name stops startup. Announces and `published` messages for other repositories are ignored. Discovery is fetched for the first one |
| `receiver_stratum0_url` | `--receiver-stratum0-url` | empty | Publisher base URL, e.g. `http://stratum0.example.org:8080`. The receiver fetches `{url}/s1/{txn}/manifest`, `{url}/s1/bundle` and, after a commit, `{url}/cvmfs/{repo}/data/...`. Without it nothing is pulled |
| `cas.root` | `--cas-root` | `/var/lib/cvmfs-prepub/cas` | Local store; objects land in `data/xx/<rest>`. Normally the Stratum 1's storage directory for the repository |
| `broker_ca_cert` | `--broker-ca-cert` | system pool | CA for the broker's `wss://` certificate; also trusted, in addition to the system pool, for the discovery fetch; the only CA trusted for an `https://` enroll URL (required in that case) |
| | `--discovery-url` (CLI only) | empty | Publisher base serving `GET {url}/cvmfs/{repo}/.cvmfsbits`. Without it the receiver never connects to a broker |
| | `--discovery-verify-key` (CLI only) | empty | PEM Ed25519 public key matching the publisher's `--discovery-signing-key`. When set, the discovery signature is checked and a bad one stops the receiver; without it a warning is logged. Required with `--broker-auth` |
| | `--broker-auth` (CLI only) | `false` | Enroll and present a token to the broker. Needs `S1_NODE_KEY`, `--discovery-url` and `--discovery-verify-key` |
| | `--pull-concurrency` (CLI only) | `0` (= 16) | Parallel object fetches or bundle requests per transaction |
| | `--pull-files-per-request` (CLI only) | `0` (= 1) | Objects per bundle request; `> 1` switches to `POST /s1/bundle` |
| | `--pull-auto` (CLI only) | `false` | Measure the RTT to `{receiver_stratum0_url}/api/v1/health` and choose unset values: < 5 ms: 32/1; < 50 ms: 16/8; < 150 ms: 8/32; otherwise 8/64 (concurrency/files per request) |
| `dev` | `--dev` | `false` | No effect in receiver mode |

Environment: `S1_NODE_KEY` (hex) with `--broker-auth`. A receiver never needs
`PREPUB_HMAC_SECRET` or `PREPUB_API_TOKEN`. Outbound HTTP honours
`HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY`.

Deprecated flags `--tls-cert`, `--tls-key`, `--data-addr`, `--data-host`,
`--session-ttl` and `--disk-headroom` are still accepted so old units start,
but they do nothing and a warning lists them at startup. Remove them.

An example `receiver.yaml` and unit are in
[INSTALL.md](INSTALL.md#7-stratum-1-pre-warming).

Runtime limits: at most 4 transactions are pulled at once (further announces
are dropped with a warning), one announce per transaction id is processed at a time, objects are
fetched with a 5-minute per-request timeout, and stale `.tmp` files from
interrupted writes are swept from the CAS at startup.

---

## 5. REST API

### Conventions

- Base URL: `http://<publisher>:8080` (`--listen`). The listener is plain
  HTTP; production deployments put TLS in front of it (reverse proxy or
  WireGuard). Up to 1024 connections are accepted at once; request headers
  must arrive within 10 s; idle keep-alive connections close after 120 s.
  There is no overall read or write timeout, so large uploads and event
  streams are not cut off.
- Request and response bodies are JSON unless stated otherwise.
- Errors carry a JSON body `{"error":"<message>"}`. Authentication failures
  are sent as `application/json`; most other API errors are sent with
  `Content-Type: text/plain` although the body is the same JSON, so clients
  should parse the body whatever the header says. The distribution endpoints
  (`/s1/...`, objects, `/control/...`) answer errors in plain text.
- A request with a method a route does not support gets `405`; an unknown
  path gets `404`.
- Repository names (the `repo` field, `repo_name`, receiver `repos`) must be
  valid CVMFS names: at most 60 characters of `A-Z a-z 0-9 . _ -`, starting
  with a letter or digit, not ending in `.` and without `..`.

### Authentication

Authenticated routes accept two credentials, both derived from the single
secret in `PREPUB_API_TOKEN`. Which ones are accepted is set by
`server.auth_mode`:

| `auth_mode` | Bearer token | `X-Bits-Auth` signature |
|---|---|---|
| `bearer` | accepted | refused (`401`) |
| `both` (default) | accepted | accepted |
| `hmac` | refused (`401`) | accepted |

The current mode is reported as `auth_mode` in `GET /api/v1/health`. When a
request carries `X-Bits-Auth` it is checked as a signature, and an
`Authorization` header on the same request is ignored. With `--dev` and an
empty token, authentication is off. Migration and rotation procedures are in
[INSTALL.md](INSTALL.md#5-api-authentication-and-secrets).

**Bearer:** `Authorization: Bearer <PREPUB_API_TOKEN>`, compared in constant
time. The secret travels on every request.

**X-Bits-Auth (HMAC):** the secret stays on both ends; each request carries a
single-use, time-limited MAC bound to its method, URI, fields and payload.

```
X-Bits-Auth: v1 key_id=prepub ts=<unix-seconds> nonce=<string> fd=<hex> bh=<hex or -> mac=<hex>
```

| Parameter | Value |
|---|---|
| `v1` | Scheme version; anything else is refused |
| `key_id` | Must be `prepub` |
| `ts` | Client time, Unix seconds. Accepted from `now - signature_skew` (default 2 min) to `now + 15 s` |
| `nonce` | Unique per request (for example 16 random bytes in hex). Reusing a nonce with the same MAC is refused as a replay |
| `fd` | Field digest (below). For any request that is not a multipart job submission: the SHA-256 of the empty string, `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `bh` | Body/payload hash: lowercase hex SHA-256, or `-` when there is none |
| `mac` | `hex(HMAC-SHA256(PREPUB_API_TOKEN, canonical))` |

The canonical string is seven lines joined by `\n`, with no trailing newline:

```
bits-hmac-v1
<METHOD in upper case>
<request URI: path and query string exactly as sent>
<fd>
<bh>
<ts>
<nonce>
```

What `fd` and `bh` must contain depends on the request:

| Request | `fd` | `bh` |
|---|---|---|
| `GET` (no body) | empty-string digest | `-` (the SHA-256 of an empty body is also accepted) |
| JSON or other non-multipart body (reserve, published, seal, finalize, `application/json` job submission, distribute manifests) | empty-string digest | SHA-256 of the exact body bytes |
| Multipart job submission with a `tar` part | digest of all non-file form fields | SHA-256 of the tar bytes; the form must then include `tar_sha256` |
| Multipart job submission without a `tar` part (finalize, staged) | digest of all non-file form fields | `-` |

Field digest: take every form part that is not the `tar` file part (for a
repeated field name only the first value counts), sort by name, and hash the
concatenation of `<len(name)>:<name>=<len(value)>:<value>\n` for each, where
lengths are in bytes; `fd` is the lowercase hex SHA-256 of that. Unknown
fields are included, so every field the server receives is covered.

The server checks the MAC, the time window and the nonce before reading the
body, then, after reading it, checks that the fields and payload match `fd`
and `bh`. Query parameters are covered only through the URI, so sign the URI
exactly as sent. Signed non-multipart bodies are limited to 1 MiB (256 MiB
for `/api/v1/distribute/manifests`).

The replay cache holds up to 50 000 nonces for twice the skew. When it is
full, signed requests are refused with `401` until entries age out; a warning
is logged at 80 % and the counter `replay_cache.rejected_full` in health
shows refusals.

Reference signer (Python):

```python
import hashlib, hmac, os, time

EMPTY = hashlib.sha256(b"").hexdigest()

def fields_digest(fields: dict) -> str:
    h = hashlib.sha256()
    for k in sorted(fields):
        kb, vb = k.encode(), fields[k].encode()
        h.update(b"%d:%s=%d:%s\n" % (len(kb), kb, len(vb), vb))
    return h.hexdigest()

def x_bits_auth(secret: str, method: str, uri: str, fd: str = EMPTY, bh: str = "-") -> str:
    ts, nonce = str(int(time.time())), os.urandom(16).hex()
    canonical = "\n".join(["bits-hmac-v1", method.upper(), uri, fd, bh, ts, nonce])
    mac = hmac.new(secret.encode(), canonical.encode(), hashlib.sha256).hexdigest()
    return f"v1 key_id=prepub ts={ts} nonce={nonce} fd={fd} bh={bh} mac={mac}"
```

For a multipart upload, sign with `fd=fields_digest(form_fields)` and
`bh=<sha256 of the tar>`, and send the same value as `tar_sha256`.

A signed GET from a shell:

```sh
URI=/api/v1/jobs/$JOB_ID; TS=$(date +%s); NONCE=$(openssl rand -hex 16)
FD=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
MAC=$(printf 'bits-hmac-v1\nGET\n%s\n%s\n-\n%s\n%s' "$URI" "$FD" "$TS" "$NONCE" \
      | openssl dgst -sha256 -hmac "$PREPUB_API_TOKEN" -hex | sed 's/^.* //')
curl -H "X-Bits-Auth: v1 key_id=prepub ts=$TS nonce=$NONCE fd=$FD bh=- mac=$MAC" \
     "http://stratum0.example.org:8080$URI"
```

`401` bodies say what was wrong (missing header, wrong kind of credential,
unknown `key_id`, expired timestamp, replay, MAC mismatch, fields or payload
differ from the signature).

### Endpoint summary

| Method and path | Auth | Available | Purpose |
|---|---|---|---|
| `GET /api/v1/health` | none | always | Liveness and node capabilities |
| `GET /api/v1/metrics` | none | always | Prometheus metrics ([section 9](#9-metrics-and-logs)) |
| `GET /`, `GET /jobs`, `GET /jobs/{id}` | none (page asks for a token) | always | Web console |
| `GET /api/v1/jobs` | yes | always | List all jobs |
| `POST /api/v1/jobs` | yes | always (JSON form needs `staging_root`) | Submit a job |
| `GET /api/v1/jobs/{id}` | yes | always | Job status |
| `POST /api/v1/jobs/{id}/abort` | yes | always | Abort a running job |
| `GET /api/v1/jobs/{id}/events` | yes | always | Server-Sent Events stream |
| `GET /api/v1/jobs/{id}/log` | yes | always | Job record and transition history |
| `POST /api/v1/reserve` | yes | always | Fail-fast namespace check |
| `POST /api/v1/published` | yes | always (`501` without `stratum0_url`) | Is a path already published, and by which build |
| `POST /api/v1/published/files` | yes | always (`501` without `stratum0_url`) | Read published `.meta.json` / `.bits-view.json` files in one batch |
| `GET /api/v1/builds/{id}` | yes | always | Coarse build status |
| `POST /api/v1/builds/{id}/seal` | yes | always | Declare a build's job count |
| `POST /api/v1/builds/{id}/finalize` | yes | always | Publish a build's accumulated packages now |
| `GET /api/v1/measurements` | none | unless `measurements_dir: off` | Build ids with records |
| `GET /api/v1/measurements/{build}` | none | unless `measurements_dir: off` | Measurement records |
| `POST`/`PUT /api/v1/distribute/manifests` | yes | always | Register a pull manifest |
| `POST /api/v1/control/revoke` | yes | with `--embedded-broker-auth` and a non-empty `PREPUB_API_TOKEN` | Revoke a receiver ([Enrollment and broker authentication](#enrollment-and-broker-authentication)) |
| `POST /api/v1/control/unrevoke` | yes | as above | Lift a receiver's revocation |

The routes receivers use (pull manifests, objects, bundles, discovery,
enrollment and revocation) are listed with their conditions in
[Publisher endpoints](#publisher-endpoints).

### GET /api/v1/health

Always `200`:

```json
{
  "status": "healthy",
  "publish_paths": ["ingest", "prepub", "staged"],
  "auth_mode": "both",
  "finalize_ready": true,
  "max_tar_size": 10737418240,
  "replace_allowed": false,
  "replay_cache": {"entries": 12, "rejected_full": 0}
}
```

| Field | Meaning |
|---|---|
| `status` | Always `healthy` when the process answers |
| `publish_paths` | Paths a job may name ([Publish backends and paths](#publish-backends-and-paths)) |
| `auth_mode` | `bearer`, `both` or `hmac` |
| `finalize_ready` | `true` when `ingest_config_prefix` is set and the default backend is gateway mode, i.e. coarse builds can be finalized; always `false` in local mode |
| `max_tar_size` | Largest accepted tar, bytes |
| `replace_allowed` | `true` when jobs may send `replace` (`replace_on_conflict` and `stratum0_url` set) |
| `replay_cache.entries`, `replay_cache.rejected_full` | Nonces held; signed requests refused because the cache was full |

The health check does not test the gateway or the CAS; those are probed once
at startup (the process exits if the probe fails).

### POST /api/v1/jobs

Submits one job. Returns `202 {"job_id":"<uuid>"}` as soon as the job is
written to the spool; the work happens in the background. Two forms, chosen
by `Content-Type`.

**Multipart (`multipart/form-data`, the normal form).** The payload is the
part named `tar` that has a filename; it is streamed straight into the spool
and hashed on the way. All other parts are fields (at most 1 MiB each, 64
parts in total).

| Field | Type | Meaning |
|---|---|---|
| `repo` | string, required | Repository name, e.g. `software.example.org` |
| `path` | string | Repository-relative target, e.g. `sw/pkg/1.0`. Empty means the repository root. Must not start with `/`, contain `..` that escapes the repository, or start with a `cvmfs/` component |
| `tar` | file | The tar. Required except for finalize and staged jobs; refused on staged jobs |
| `tar_sha256` | hex | SHA-256 of the tar; verified if present; required on signed uploads |
| `publish_path` | string | `prepub` (default), `ingest` or `staged`; must be offered by the node |
| `build_id` | string | CI run identity: groups measurements and, on the default path, makes the job a coarse-build member |
| `coarse` | bool | Override the coarse decision; `true` requires `build_id` and the default path. In local mode `coarse=true` is treated as `false`, but without `build_id` it is still refused with `400` |
| `build_expect` | integer >= 0 | Number of jobs in this build; cvmfs-prepub finalizes when that many are terminal |
| `finalize` | `true` | Finalize job for `build_id`: carries no payload (a sent tar is dropped) |
| `prewarm` | bool | Ask to pre-warm Stratum 1s for this job (default off); effective only on a node started with `--prewarm`, otherwise ignored with a log line. `true` only on the default path, or on `ingest` with `direct_s3` and `object_list` |
| `identity_path` | string | Repository-relative path, at or under `path`, whose presence means this content is already published. Checked just before the commit: if present the job ends `published` without committing |
| `identity_hash` | string | Expected `package.hash` in `<identity_path>/.meta.json`; a different hash fails the job instead of skipping, unless `replace` |
| `replace` | bool | Replace content another build published here: when `<identity_path>/.meta.json` has a hash that differs from `identity_hash`, the subtree at `path` is deleted, then this job commits (one revision without it). A path without a readable hash is never replaced. Requires `identity_path` equal to `path`, an `identity_hash`, a path other than the root, the `ingest` or `staged` path, and `replace_on_conflict` on the node; otherwise `400`. The same hash still skips |
| `tag_name` | string | Named snapshot tag for the commit; up to 255 characters of `A-Z a-z 0-9 . _ -` |
| `tag_description` | string | Tag description |
| `webhook_url` | URL | Absolute `http://` or `https://` URL with a host; called when the job is published or fails ([Webhooks](#webhooks)) |
| `preload_exe` | string | Repository-relative executable; with `preload_paths`, the pipeline writes a `.<name>.cvmfspreload` list next to it |
| `preload_paths` | JSON array of strings | Repository-relative paths the executable opens at startup |
| `direct_s3` | bool | `ingest` path only: pass `--direct-s3` to `cvmfs_server ingest`, with `--s3-config` naming prepub's own S3 config when `cas.type` is `s3` |
| `object_list` | bool | `ingest` path only, requires `direct_s3`: collect the S3 object list |
| `staging_prefix` | string | `staged` path only: S3 prefix holding the prepared objects; slash-separated segments of `[A-Za-z0-9._-]`, at most 128 bytes, last segment not `data` |
| `catalog_hash` | string | `staged` path only: the subtree catalog to graft, 40 lowercase hex characters followed by `C` |

Boolean fields accept `true`/`false`/`1`/`0` (Go `ParseBool`); anything
else is a `400`. `finalize` is true only when the value is exactly `true`.
URL query parameters are never read.

A `curl` example is in [README.md](README.md#quick-start).

**JSON (`application/json`), for a tar already on the server.** Requires
`staging_root`; the tar must be inside it. Only after every check has passed
(shape, containment, publish path, field checks, and `tar_sha256` last) is the
tar moved into the spool (rename, else hard link, else copy) and removed from
the staging directory; a refused request leaves it where it was. Body at most
1 MiB.

| Field | Meaning |
|---|---|
| `repo` (required), `path`, `publish_path` (not `staged`), `build_id`, `coarse`, `build_expect`, `prewarm`, `tag_name`, `tag_description`, `webhook_url`, `preload_exe`, `preload_paths` (array) | As in the multipart form |
| `tar_path` (required) | Absolute or relative path of the tar inside `staging_root` |
| `tar_sha256` (required) | Verified before the job is accepted |

`staging_prefix` and `catalog_hash` are refused in this form; `finalize`,
`identity_path`, `identity_hash`, `replace`, `direct_s3` and `object_list` are not read.

**Responses.**

| Status | When |
|---|---|
| `202` | Accepted: `{"job_id":"..."}` |
| `400` | Missing `repo`; invalid repository name ([Conventions](#conventions)); invalid `webhook_url`; malformed path, `identity_path`, tag, boolean or integer; missing `tar`; `tar_sha256` mismatch; publish path not offered; a field used on the wrong path (`direct_s3`, `object_list`, `staging_prefix`, `catalog_hash`, `prewarm`, `coarse`); `staging_prefix` without `catalog_hash` or the reverse; staged job with a tar; `finalize` or `coarse` without `build_id`; `replace` without what it requires (see `replace`); too many parts; duplicate `tar` part; broken multipart; invalid JSON; `tar_path` outside `staging_root` or missing; signed upload without `tar_sha256` |
| `401` | Authentication failed, or the signature does not match the fields or payload |
| `403` | Target outside `allowed_publish_prefixes` (finalize jobs are exempt) |
| `413` | Tar larger than `max_tar_size_gib` (refused from `Content-Length` before reading when possible), or a field over 1 MiB |
| `500` | Spool write failed |
| `503` | JSON form without `staging_root` |
| `507` | Upload would leave less than `spool_min_free_gib` free on the spool filesystem |

### GET /api/v1/jobs/{id}

`200` with the job's status, `404` if unknown:

| Field | Meaning |
|---|---|
| `job_id`, `state`, `repo`, `path` | Identity and current state ([States](#states)) |
| `n_objects`, `n_bytes_raw`, `n_bytes_compressed` | Pipeline counts (default path) |
| `new_root_hash` | Root catalog hash after the commit, when the backend reports one |
| `error` | Set on failure. Deliberately generic: `job processing failed — see service logs for details` |
| `attempts`, `last_error`, `next_attempt_at` | Failed attempts so far, the latest cause (truncated to 4000 characters; also the cause of a final failure) and, while waiting, when the next attempt runs |
| `created_at`, `updated_at` | RFC 3339 timestamps |

Empty fields are omitted.

### GET /api/v1/jobs

`200` with a JSON array of every job in the spool (all states), newest first;
unreadable records are skipped. There is no paging or filtering. Each entry has
`job_id`, `state`, `repo`, `path`, `created_at`, `updated_at` and, when set,
`tag_name`, `tar_name`, `tar_size`, `n_objects`, `n_new_objects`,
`n_bytes_raw`, `n_bytes_compressed`, `new_root_hash`, `error`,
`failed_at_state` (state the job was in when it failed),
`pipeline_started_at`, `pipeline_ended_at`, `leased_at`, `published_at` and
`distributing_started_at`. `tar_name` is the uploaded file name (the
multipart part's filename, or the base name of `tar_path`), reduced to a base
name without control characters and at most 255 bytes.

### GET /api/v1/jobs/{id}/log

`200` with the full job record and its transitions, `404` if unknown:

```json
{
  "job": { "...": "contents of manifest.json" },
  "transitions": [
    {"time": "2026-10-01T12:00:00Z", "from": "incoming", "to": "staging"},
    {"time": "2026-10-01T12:00:41Z", "from": "staging", "to": "uploading"}
  ]
}
```

The `job` object is the spool record. Most keys are snake_case (`build_id`,
`publish_path`, `attempts`, `provenance`, ...), but the core fields use their
Go names: `ID`, `Repo`, `Path`, `PackageName`, `TarPath`, `TarSHA256`,
`State`, `CreatedAt`, `UpdatedAt`, `LeaseToken`, `NObjects`, `NNewObjects`,
`NBytesRaw`, `NBytesCompressed`. In this response `LeaseToken` is always
empty and `webhook_url` is cut to `<scheme>://<host>/[redacted]`.

### POST /api/v1/jobs/{id}/abort

Cancels a running or queued job. The job ends in `failed`, any gateway lease
is aborted, and it is not retried.

| Status | When |
|---|---|
| `202` | `{"status":"aborting"}` |
| `404` | Unknown job |
| `409` | The job is already terminal, or is not currently running in this process |

### Server-Sent Events

`GET /api/v1/jobs/{id}/events` returns `text/event-stream` (`404` for an
unknown job). The job's current state is sent first, then each state
change, all in this form:

```
event: state_change
data: {"job_id":"…","state":"uploading","time":"2026-10-01T12:00:41.123Z"}
```

`error` is added when the job has one: on failure events, on the `incoming`
event sent when a retry is scheduled, and on the first event of a failed job.
The stream ends after a terminal state (`published`, `accumulated`, `failed`,
`aborted`), so a subscription to a finished job gets one event and closes,
or when the client disconnects. Slow subscribers may miss events (32-event
buffer). The response sets `X-Accel-Buffering: no` for nginx.

### Webhooks

When a job has `webhook_url`, cvmfs-prepub sends `POST <webhook_url>` with
`Content-Type: application/json` and the body

```json
{"job_id":"…","state":"published","time":"2026-10-01T12:03:10Z"}
```

when the job is published (including the already-published skip) and
`{"job_id":"…","state":"failed","error":"job processing failed — see service logs for details","time":"…"}`
when it fails. No webhook is sent for `accumulated` or for retries. Delivery
is one attempt with a 10 s timeout (TLS 1.2 or later for `https://`); the
request is not signed; failures and `4xx`/`5xx` answers are only logged.

### POST /api/v1/reserve

Fail-fast check that a target can be published, before a build spends time on
it. Body `{"repo":"<repo>","path":"<path>"}` (path may be empty).

1. Containment against `allowed_publish_prefixes`.
2. In local mode: answers `204` (there is no gateway lease to conflict on).
3. If `stratum0_url` is set and `path` is not empty: if the path already
   exists in the published catalogs, `409` (a lookup error is logged and
   ignored).
4. Takes a single-attempt gateway lease on the path and releases it at once.

| Status | When |
|---|---|
| `204` | Free |
| `400` | Invalid JSON, missing or invalid `repo` |
| `403` | Outside `allowed_publish_prefixes` |
| `409` | Already published, or another publisher holds the lease |
| `502` | Gateway error |

### POST /api/v1/published

Is a path published, and by which build? Body `{"repo":"<repo>","path":"<path>"}`.
Answers `200 {"exists":false}` or `200 {"exists":true,"hash":"<hash>"}`, where
`hash` is `package.hash` from `<path>/.meta.json` (omitted when there is no
such file).

| Status | When |
|---|---|
| `200` | Answer as above |
| `400` | Invalid JSON, missing or invalid `repo`/`path` |
| `403` | Outside `allowed_publish_prefixes` |
| `501` | `stratum0_url` not configured |
| `502` | The published catalogs or `.meta.json` could not be read |

### POST /api/v1/published/files

Reads the metadata files bits keeps in what it publishes, so that a producer can
rebuild a tree from what is already there (a release's merged view from its
members' `.bits-view.json`). Body `{"repo":"<repo>","paths":["<path>",...]}`,
each a canonical repository-relative path ending in `/.meta.json` or
`/.bits-view.json`; no other file can be read. All paths are read from the same
published revision, and sizes are checked from the catalog before any download. Answers
`200 {"files":{"<path>":<content>|null},"invalid":["<path>"]}`: a file's JSON
as it is published, `null` when it is not published, and `null` plus an entry
in `invalid` when it is not valid JSON or larger than 16 MiB. A repeated path
is answered once.

| Status | When |
|---|---|
| `200` | Answer as above |
| `400` | Invalid JSON, missing or invalid `repo`, no paths, or a path that is not canonical, is invalid or names another file |
| `403` | A path outside `allowed_publish_prefixes` (nothing is read) |
| `413` | More than 512 paths, or more than 64 MiB of files: ask in smaller batches |
| `501` | `stratum0_url` not configured |
| `502` | The published catalogs or a file could not be read |

### Builds

Coarse builds are described in [Coarse builds](#coarse-builds).

**`GET /api/v1/builds/{id}`** always answers `200`:

```json
{
  "build_id": "pipeline-123",
  "expect": 42,
  "accumulated": 40,
  "failed": ["<job-id>"],
  "finalizing": true,
  "result": {"build_id": "pipeline-123", "repo": "software.example.org",
             "packages": 40, "published": 40, "at": "2026-10-01T12:30:00Z"}
}
```

`expect` is 0 when no count was declared; `finalizing` means the finalize has
been claimed (running, finished or crashed); `result` appears once a finalize
outcome has been recorded and has `error` when it failed. In local mode the
status also has `"per_package": true`: packages are published on arrival and
nothing accumulates. An unknown build id returns zeros.

**`POST /api/v1/builds/{id}/seal`** with body `{"expect": N}` declares that
the producer has submitted N jobs for the build. If they are all terminal
already, the finalize starts now; otherwise it starts when the last one
finishes. Re-sealing with the same count is harmless. In local mode a seal
is a no-op: it is answered `200` with the build status (`per_package: true`)
before the body is read, and nothing is recorded or finalized.

| Status | When |
|---|---|
| `200` | Local mode: no-op, body is the build status |
| `202` | Recorded; body is the build status as above |
| `400` | Invalid JSON, or `expect` not a positive integer |
| `409` | `expect` is below the number of jobs already terminal, or below an earlier declaration (a seal may not shrink a build) |
| `500` | The count could not be written |

**`POST /api/v1/builds/{id}/finalize`** (no body) publishes the accumulated
packages now, in one commit, even if some members failed. It returns when the
commit has finished.

| Status | Body |
|---|---|
| `200` | `{"build_id","repo","packages","published","conflicts"}` |
| `400` | `{"build_id","error"}`: nothing was published (finalize not configured, no accumulated packages, packages from several repositories, objects missing from the CAS) |
| `500` | `{"build_id","error","packages","published","conflicts","output"}`: the commit ran and failed; `output` is the `ingestsql` output |

`conflicts` is a list of `{"path": "...", "reason": "..."}` for packages left
out because another member at the same path had different content.

### GET /api/v1/measurements

Unauthenticated. `200` with the build ids that have measurement records,
newest first (`["pipeline-123", "nobuild-20261001", ...]`). `404` when
measurements are disabled.

`GET /api/v1/measurements/{build}` returns the records of one build as a JSON
array (`latest` selects the most recently written build). Query parameters:

| Parameter | Effect |
|---|---|
| `job=<id>` | Only that job's records |
| `path=<publish path>` | Only records of `prepub`, `ingest` or `staged` |
| `summary=1` | A summary object instead of the records |

Record fields: `ts`, `build_id`, `job_id`, `repo`, `path`, `publish_path`,
`host` (the prepub node that wrote it), `direct_s3` (always present; absent only
in records written before it existed), `object_list`,
`outcome` (`published`, `already_published`, `failed`, `retry`, or
`incomplete:<state>` for a job that ended elsewhere, e.g. an accumulated
member), `total_s`, `queued_s`, `lock_wait_s` (waiting for the repository's
commit lock, which serialises its publishes), `commit_s`, `backend_s`, `pipeline_s`,
`precheck_s` (the already-published check, made under the commit lock just
before `commit_s` starts), `ancestors_s` (the part of `commit_s` before the
publish tool runs that creates the target's parent directories; the delete
before a replace is in neither), `tar_bytes`, `objects`, `objects_exact`, `bytes_raw`, `bytes_compressed`,
`conflicted`, `replaced`, `error` (the real cause, truncated). Times are
seconds; absent values are omitted.

Summary fields: `build_id`, `repo`, `publish_paths` (count per path), `jobs`,
`published`, `failed`, `incomplete`, `conflicted`, `replaced`, `first`,
`last`, `window_s`, `backend_s`, `total_s` and `lock_wait_s` (each `{n, sum,
mean, median, p90, p99, max}`), `tar_bytes`, `objects`, `objects_partial`.

`404` for an unknown build, when nothing has been recorded yet (`latest`), or
when measurements are disabled; `500` when the directory cannot be read.

### Provenance headers

With `--provenance`, a submission may carry these headers; they end up in the
job's `provenance` record ([section 8](#8-provenance)):

| Header | Field |
|---|---|
| `X-Provenance-Git-Repo` | `git_repo` |
| `X-Provenance-Git-SHA` | `git_sha` |
| `X-Provenance-Git-Ref` | `git_ref` |
| `X-Provenance-Actor` | `actor` |
| `X-Provenance-Pipeline-ID` | `pipeline_id` |
| `X-Provenance-Build-System` | `build_system` |
| `X-OIDC-Token` | CI OIDC token (JWT). A JWT-shaped `Authorization: Bearer` value is also tried |

These headers are not covered by `X-Bits-Auth`. Only a validated OIDC token
sets `verified: true`; it then replaces all of these header values
([What is recorded](#what-is-recorded)). Header values alone are recorded as
unverified.

### Limits

| Limit | Value | Status when exceeded |
|---|---|---|
| Tar size | `max_tar_size_gib`, default 10 GiB | `413` |
| Free spool space after upload | `spool_min_free_gib`, default 20 GiB | `507` |
| Multipart field | 1 MiB | `413` |
| Multipart parts | 64 | `400` |
| JSON bodies (submission, reserve, published) | 1 MiB | `400` |
| Seal body | 64 KiB | `400` |
| Signed body (non-multipart) | 1 MiB; 256 MiB for distribute manifests | `401` |
| Single file inside a tar (default path) | `max_tar_size_gib` on the default fixed chunk grid, otherwise 1 GiB | job fails ([Tar archive rules](#tar-archive-rules)) |
| Manifest ingest body | 256 MiB | `400` |
| Bundle request | 8 MiB body, 100 000 hashes | `400` / `413` |
| Concurrent connections | 1024 | queued by the kernel |

The free-space check is made per upload against the space free at the time,
so concurrent uploads can together go below the floor.

---

## 6. Pull distribution protocol

Stratum 1 pre-warming is optional and best effort. Receivers pull from the
publisher; nothing is pushed to them, and the commit never waits for them.
Setup steps are in [INSTALL.md](INSTALL.md#7-stratum-1-pre-warming).

### Overview

| Plane | Transport | Carries |
|---|---|---|
| Control | MQTT over WebSocket (`ws://` or `wss://`) to the embedded broker on the publisher | Small JSON messages: announce, published, presence |
| Discovery and enrollment | HTTP(S) on the publisher | Signed discovery document; node-key challenge/response for a broker token |
| Data | HTTP GET/POST on the publisher API listener | Pull manifests, single objects, object bundles |

What a receiver does:

- On an **announce** for a repository it serves, it fetches the transaction's
  pull manifest, checks each object against its CAS and fetches the missing
  ones, verifying each by hash. This happens while the publisher is still
  building catalogs and committing.
- On a **published** message (retained by the broker, so also delivered when
  a receiver connects later) it fetches the new root catalog object.

A receiver does not fetch nested catalogs, does not write a
`.cvmfspublished` and does not replace Stratum 1 replication: the regular
`cvmfs_server snapshot` still runs; the pre-pulled objects are then already
in the Stratum 1's store.

Announces are sent only when pre-warming applies (`--prewarm` on the node and
the job's `prewarm` field) and the transaction manifest was stored, which needs
`--pull-object-base-url` (otherwise there is nothing to fetch), for two kinds
of job: on the gateway-mode `prepub` path
before the commit, and on `ingest` with `direct_s3` and `object_list` right
after the commit, listing the data objects the publisher reported as stored.
Receivers fetch the objects from the publisher's CAS, so on the ingest path
the CAS must be the repository's S3 storage (`--cas-type s3`).
The `published` message is sent after every commit that reports a new root
hash, on any path, whenever the embedded broker runs; coarse-build finalizes
do not send it.

### Publisher endpoints

| Endpoint | Listener | Available | Auth | Purpose |
|---|---|---|---|---|
| `GET /cvmfs/{repo}/.cvmfsbits` | API | with `--control-plane-url` | none | Discovery document |
| `GET /control/challenge?node=<node>` | API, or TLS enroll listener | with `--embedded-broker-auth`; on the API listener only without `--enroll-tls-addr` | none | Enrollment nonce |
| `POST /control/enroll` | API, or TLS enroll listener | as above | node key (MAC) | Redeem nonce and MAC for a broker token |
| `POST /control/revoke` | TLS enroll listener | with `--enroll-tls-addr` | publisher token | Revoke a node |
| `POST /control/unrevoke` | TLS enroll listener | with `--enroll-tls-addr` | publisher token | Lift a node's revocation |
| `POST /api/v1/control/revoke` | API | with `--embedded-broker-auth` and a non-empty `PREPUB_API_TOKEN` | API secret | Revoke a node |
| `POST /api/v1/control/unrevoke` | API | as above | API secret | Lift a node's revocation |
| `ws(s)://<host>:<port>` | `--embedded-broker-ws-addr` | when set | token with `--embedded-broker-auth` | MQTT broker |
| `GET /s1/{txn}/manifest` | API | always | none | Pull manifest for transaction `{txn}` (the job id) |
| `GET`/`HEAD /cvmfs/{repo}/data/{xx}/{rest}` | API | gateway mode | none | One object from the CAS |
| `POST /s1/bundle` | API | gateway mode | none | Many objects in one response |
| `POST`/`PUT /api/v1/distribute/manifests` | API | always | API secret | Register a pull manifest ([Pull manifest](#pull-manifest)) |

Discovery and enrollment endpoints are rate-limited per client IP (5
requests/s, burst 10) and globally (100/s, burst 200); excess requests get
`429`. The data endpoints are not rate-limited and need no authentication.

### Discovery document

`GET /cvmfs/{repo}/.cvmfsbits` (`Cache-Control: no-cache`):

```json
{
  "repos": ["software.example.org"],
  "control_plane": {"type": "mqtt", "url": "wss://stratum0.example.org:1882"},
  "enroll_url": "https://stratum0.example.org:8443",
  "signature": "<base64 Ed25519 signature>"
}
```

| Field | Meaning |
|---|---|
| `repos` | `[repo_name]`, or the requested repository when `repo_name` is empty |
| `control_plane.type`, `control_plane.url` | Always `mqtt`; the `--control-plane-url` value |
| `enroll_url` | The `--enroll-url` value, present only with `--enroll-tls-addr` |
| `signature` | Ed25519 signature (standard base64) over the compact JSON encoding of the document with `signature` removed, in the field order shown |

The receiver fetches the document for the first repository in `--repos`,
retrying for up to 60 s (1 s backoff doubling to 8 s), and exits if it cannot
get it. Whenever `--discovery-verify-key` is set it verifies the signature
and exits on failure (the key is required with `--broker-auth`). It refuses a
transport other than `mqtt` and an empty URL. An `https://` discovery URL is
verified against the system CA pool plus the `--broker-ca-cert` CA, and the
proxy environment (`HTTPS_PROXY`, `NO_PROXY`) applies.

### Enrollment and broker authentication

With `--embedded-broker-auth` every broker connection needs a token. Keys:

- Master secret `PREPUB_HMAC_SECRET` (publisher only).
- Node key `HMAC-SHA256(master, node_id)`, printed by
  `cvmfs-prepub node-key <node_id>` and given to that receiver as
  `S1_NODE_KEY` (hex). The node id `publisher` is reserved.

Flow:

1. `GET {enroll}/control/challenge?node=<node_id>` returns
   `{"nonce":"<hex>"}`: 8 bytes of timestamp and a 16-byte MAC, valid for
   2 minutes, nothing stored on the server.
2. `POST {enroll}/control/enroll` with
   `{"node":"<node_id>","nonce":"<nonce>","mac":"<hex HMAC-SHA256(node key, node_id + "|" + nonce)>"}`.
   The nonce can be redeemed once. Unknown or revoked nodes and bad MACs get
   `401`.
3. The answer is `{"token":"<token>","exp_unix":<unix>,"scope":"control"}`,
   valid 10 minutes.
4. The receiver connects to the broker with user name `<node_id>` and the
   token as password, verifying the broker certificate with
   `--broker-ca-cert`. A fresh token is obtained on every reconnect.

`{enroll}` is the discovery document's `enroll_url` when present (HTTPS; then
`--broker-ca-cert` is required on the receiver), otherwise `--discovery-url`
(the API listener, plain HTTP unless a proxy adds TLS).

Token format: `base64url(payload) "." base64url(HMAC-SHA256(master, base64url(payload)))`
without padding, where the payload is
`{"node":"…","scope":"control","exp":<unix>,"jti":"<random>"}`. Expiry has
30 s of leeway. The publisher's own broker clients use tokens for the node
`publisher`.

Broker ACL (with `--embedded-broker-auth`):

| Client | Subscribe | Publish |
|---|---|---|
| `publisher` | any topic | any topic |
| a receiver node | any topic | only `cvmfs/receivers/<its node_id>/presence` |

Without `--embedded-broker-auth` the broker accepts every connection and
every publish.

Revocation: `cvmfs-prepub revoke <node>` posts `{"node":"<node>"}` to
`POST /control/revoke` on the TLS enroll listener (publisher token), or with
`--api-url` to `POST /api/v1/control/revoke` (API secret; the route exists
only when `PREPUB_API_TOKEN` is set). The node is denied new tokens and
broker connections, and its live sessions are disconnected. Answer:
`{"revoked":"<node>","sessions_dropped":<n>}`. The denylist is saved to
`<spool_root>/revoked-nodes.json` and survives restarts; if it cannot be
saved the answer is `500` and the revocation holds only until the next
restart. `cvmfs-prepub revoke --undo <node>` posts the same body to the
matching unrevoke route (`POST /control/unrevoke` or
`POST /api/v1/control/unrevoke`), which answers `{"unrevoked":"<node>"}`; if
the list cannot be saved the answer is `500` and the node stays revoked. The
command fails unless the answer confirms the action for that node, so an
older publisher without the unrevoke route (`404`) is reported, not
silently treated as a revoke. The body names exactly one node; other fields
are refused. Errors: `403` without a valid publisher token (TLS listener) or
`401` without valid API credentials; `400` for `publisher`, an unknown
field, or a node name that is not a valid node id (empty, or containing `/`,
`+`, `#` or NUL).

### Topics and messages

All messages are JSON, QoS 1. `{repo}` is a valid repository name
([Conventions](#conventions)); `{node_id}` may not be empty or contain `/`,
`+`, `#` or NUL.

| Topic | Direction | Retained | Payload |
|---|---|---|---|
| `cvmfs/repos/{repo}/announce` | publisher to receivers | no | `{"payload_id":"<job id>","publisher_id":"pub-<job id>","repo":"…","total_bytes":<compressed bytes>}` |
| `cvmfs/repos/{repo}/published` | publisher to receivers | yes | `{"repo":"…","new_root_hash":"<40 hex>","published_at":"<RFC 3339>"}` |
| `cvmfs/receivers/{node_id}/presence` | receiver | yes | `{"node_id":"…","repos":["…"],"online":true,"ready":true}` |

Subscriptions: a receiver with one repository subscribes to that
repository's announce topic, otherwise to `cvmfs/repos/+/announce`; it always
subscribes to `cvmfs/repos/+/published`. Messages for repositories not in
`--repos` are ignored. Sessions are persistent (clean session off) with a
30 s keep-alive and automatic reconnect.

Presence: on connect a receiver publishes a retained `online: true` message
and registers a last will with `online: false`, which the broker publishes if
the connection drops; on a clean shutdown it publishes `online: false` itself.
Nothing in the publisher consumes presence; it is for monitoring.

### Pull manifest

The publisher stores a manifest per pre-warmed transaction when the job
enters `distributing` (and `--pull-object-base-url` is set); producers can
also register one with `POST /api/v1/distribute/manifests`. `GET
/s1/{txn}/manifest` returns `application/json`, or NDJSON with
`?stream=1` or `Accept: application/x-ndjson` (first line the header without
`objects`, then one object per line). `404` for an unknown transaction.

```json
{
  "transaction_id": "<job id>",
  "repo": "software.example.org",
  "base_root_hash": "",
  "target_root_hash": "<job id until the commit>",
  "base_urls": ["http://stratum0.example.org:8080/cvmfs/software.example.org/data"],
  "generator": "pipeline",
  "auth": "public",
  "created_at": "2026-10-01T12:00:41Z",
  "total_size": 123456789,
  "objects": [{"hash": "<40 hex + suffix>", "size": 0}]
}
```

Validation (on ingest and on the receiver): `transaction_id`, `repo`,
`target_root_hash` and at least one `base_urls` entry are required;
`generator` is `pipeline` or `diff`; `auth` is `public`, `token` or empty;
each object hash is at least 3 characters of `[0-9A-Za-z]`. The pipeline's
manifest lists every content object of the job (catalogs are not included)
with `size` 0 (unknown).

`POST`/`PUT /api/v1/distribute/manifests` (authenticated) takes the same JSON,
or NDJSON with `Content-Type: application/x-ndjson`, up to 256 MiB, and
answers `201 {"transaction_id":"…"}`; `400` for an invalid manifest. The
newest 8192 manifests are kept (in memory and under `<spool_root>/manifests/`);
older ones are deleted.

### Pull flow

```
publisher                               receiver
  pipeline done, job -> distributing
  store manifest /s1/<job>/manifest
  announce(repo, payload_id) ---------->  serves repo? at most 4 pulls at once
                                          GET /s1/<job>/manifest
                                          CAS.Exists per object -> missing set
                                          GET base_url/xx/rest  (or POST /s1/bundle)
                                          verify SHA-1, store in --cas-root
  lease, catalogs, commit
  published(repo, root) [retained] ---->  GET {stratum0}/cvmfs/<repo>/data/<xx>/<rest>C
```

Each object is fetched from the manifest's `base_urls` in order until one
works. The SHA-1 of the received bytes must match the hash in the object
name (ignoring the suffix); a mismatch is a failed object. A transaction with
failed objects counts as `failed` in `cvmfs_receiver_pull_transactions_total`;
there is no automatic re-pull.

### Objects and bundles

`GET /cvmfs/{repo}/data/{xx}/{rest}` serves the object `{xx}{rest}` from the
publisher's CAS (`200` with `Content-Type: application/octet-stream`,
`Cache-Control: public, max-age=31536000, immutable`, `ETag` = the object
name; `404` if absent; `400` for a malformed name). The `{repo}` segment is not
checked against the CAS: one CAS serves every name. Being content-addressed,
these URLs can be served through ordinary HTTP caches.

`POST /s1/bundle` with `{"repo":"…","hashes":["<object name>", …]}` (up to
100 000 names, 8 MiB body) answers `200` with
`Content-Type: application/x-cvmfs-bundle`: for each requested name in
order, a line `<name> <size>\n` followed by exactly `<size>` bytes, or
`<name> -1\n` when the object is missing (`invalid -1\n` for a malformed
name). Receivers use it when `--pull-files-per-request` is greater than 1,
splitting the missing set into requests of that many names.

---

## 7. Security model

This section lists what each part trusts and what it protects. How to set up
the secrets is in [INSTALL.md](INSTALL.md#5-api-authentication-and-secrets);
a summary is in [README.md](README.md#security).

### Secrets

| Secret | Holder | Grants |
|---|---|---|
| `PREPUB_API_TOKEN` | publisher and every CI runner that publishes | Full use of the authenticated API: publish to any repository and path the gateway key allows (narrowed only by `allowed_publish_prefixes`), abort jobs, finalize builds |
| `CVMFS_GATEWAY_SECRET` | publisher | Gateway leases and commits within the key's scope in the gateway's key file |
| `PREPUB_HMAC_SECRET` | publisher only | Signs broker tokens (including publisher tokens) and derives all node keys |
| `S1_NODE_KEY` | one receiver | Broker tokens for that node only: subscribe, and publish its own presence |
| Discovery signing key | publisher | Signing the discovery document |
| Rekor signing key | publisher | Signing provenance records |

There is one API secret per instance and no per-job or per-user scoping. To
give communities separate rights, use separate gateway keys and
`allowed_publish_prefixes`, or separate instances
([INSTALL.md](INSTALL.md#9-several-communities-on-one-instance)).

### Publisher API

- The listener is plain HTTP. With `auth_mode: bearer` or `both`, the secret
  is on the wire on every bearer request; without TLS anyone who can observe
  one request can publish. `auth_mode: hmac` keeps it off the wire: an
  observed request yields no reusable credential and cannot be replayed
  (single-use nonce, short time window). TLS (reverse proxy or WireGuard) is still
  needed for confidentiality and to authenticate responses.
- The signature binds method, URI with query, every form field and the
  payload. Provenance headers are not bound.
- Unauthenticated routes: health, metrics, the web console pages, measurements,
  and the distribution data routes (objects, bundles, pull manifests,
  discovery). Enrollment needs the node key; revocation needs a publisher
  token or the API secret. Objects and manifests of every repository in the
  CAS can be read by anyone who reaches the port; for repositories that are not public,
  restrict these routes in the reverse proxy. Measurement records include
  repository paths and the real error text of failed publishes.
- The web console is static; the browser stores the token in `localStorage`
  (`prepub_token`) and sends it as a bearer token, so the console cannot list
  jobs when `auth_mode` is `hmac`.
- Path containment: job paths must be repository-relative; with
  `allowed_publish_prefixes`, every target (submit, reserve, published) must
  fall under a listed `/cvmfs/<repo>/<group>` root after `path.Clean`.
- `tar_path` submissions can only use files under `staging_root`.
- `webhook_url` is called from the publisher host to whatever URL a submitter
  gives; restrict outbound traffic if that matters on your network.
- `GET /api/v1/jobs/{id}/log` returns the full record except the gateway
  lease token, and with the `webhook_url` path and query redacted.
- `--dev` turns off the API token and gateway secret requirements and the
  HTTPS requirement for the gateway.

### Gateway

Requests are HMAC-signed with `CVMFS_GATEWAY_SECRET`; the secret never
travels. HTTPS is required for a non-loopback gateway URL unless
`gateway.allow_plaintext` is set, in which case publish contents and gateway
responses are exposed to the network path but the credential is not.

### Content integrity

Objects are content-addressed (SHA-1 of the compressed bytes). Receivers
verify every pulled object against its name. Clients verify the repository as
always: the new manifest is signed by the gateway (by `cvmfs_server` in local
mode); cvmfs-prepub does not hold the repository's signing key. Tars on the default
path are checked against the [Tar archive rules](#tar-archive-rules).

### Control plane

- Use `wss://` (`--embedded-broker-tls-cert`) and `--embedded-broker-auth` on
  any network you do not fully trust. Without auth, anyone who reaches the
  broker port can send announces and `published` messages to receivers and
  impersonate presence.
- Serve enrollment over TLS (`--enroll-tls-addr`) so tokens do not travel in
  clear text.
- Receivers hold only their node key. A receiver cannot mint tokens for
  other nodes or for the publisher, and can publish only its own presence.
- The discovery document is Ed25519-signed so a receiver does not need a
  shared secret to trust the broker URL. The signature is checked whenever
  the receiver has `--discovery-verify-key` (required with `--broker-auth`).
- Revocations persist across restarts in `<spool_root>/revoked-nodes.json`
  (see [Enrollment and broker authentication](#enrollment-and-broker-authentication)).
- A manipulated announce or manifest can at most make a receiver fetch and
  store objects whose bytes match their names; it cannot change what clients
  see, which is decided by the signed repository manifest.

### Host

- Spool directories are created `0700`; job records can contain lease tokens.
- Temporary files go to `<spool_root>/tmp` (`0700`), not `/tmp`.
- `--debug-listen` exposes heap profiles (which can contain secrets and
  payload bytes); bind it to `127.0.0.1` only. A non-loopback address logs a
  warning.

---

## 8. Provenance

With `--provenance` the publisher records who built each published package
and submits a signed record to a Rekor transparency log. It is off by
default. Provenance never fails a publish: errors are logged.

### What is recorded

At submission, the request's provenance headers
([Provenance headers](#provenance-headers)) are stored in the job's
`provenance` block. If an OIDC token is present and `oidc_issuers` is set,
the token is validated (issuer in the list, signature against the issuer's
JWKS, audience equal to `PREPUB_OIDC_AUDIENCE`); on success all header
values are discarded, the record holds only what the token's claims provide
(a field the token lacks stays empty), and `verified` is `true`:

| Record field | GitHub Actions claim | GitLab CI claim |
|---|---|---|
| `git_repo` | `repository` | `project_path` |
| `git_sha` | `sha` | `sha` |
| `git_ref` | `ref` | `ref` |
| `actor` | `actor` | `user_login` |
| `pipeline_id` | `run_id` | `pipeline_id` |
| `build_system` | `github-actions` (when `workflow` is set) | `gitlab-ci` (when `ci_config_ref_uri` is set) |
| `oidc_issuer`, `oidc_subject` | `iss`, `sub` | `iss`, `sub` |

When a token carries both, the GitHub claim wins. A token that fails
validation is logged and the header values are kept with `verified: false`.

### Rekor submission

After a job is committed individually (not for coarse-build finalizes or for
jobs skipped as already published), the publisher builds a record:

```json
{
  "job_id": "…", "repo": "…", "path": "…", "published_at": "…",
  "catalog_hash": "<subtree root catalog hash>",
  "object_hashes": ["<content objects>", "…", "<catalog hashes>"],
  "git_repo": "…", "git_sha": "…", "git_ref": "…", "actor": "…",
  "pipeline_id": "…", "build_system": "…",
  "oidc_issuer": "…", "oidc_subject": "…", "verified": true,
  "rekor_server": "https://rekor.sigstore.dev"
}
```

`catalog_hash` and `object_hashes` are filled only on the default gateway-mode
path; on other paths they are empty. The hashes are CVMFS object names
(SHA-1, see [Hashing and object names](#hashing-and-object-names)).

The JSON is signed with the Ed25519 key (`rekor_signing_key`, generated on
first use) and submitted to `POST {rekor_server}/api/v1/log/entries` as a
`hashedrekord` entry whose hash is the SHA-256 of the record JSON. The
returned UUID, log index, integrated time and Signed Entry Timestamp are
stored in the job's `provenance` block as `rekor_server`, `rekor_uuid`,
`rekor_log_index`, `rekor_integrated_time` and `rekor_set`. The exact signed
bytes are kept in `<job dir>/provenance-record.json` (mode 0600, moves with
the job directory); the block names it in `signed_record_file` and holds its
SHA-256 (the hash in the Rekor entry) as `signed_record_sha256`.

### Chain and verification

The chain is: published file -> CVMFS object hash (in the catalog) -> job
(the hash appears in `object_hashes` of that job's record) -> CI run
(`git_*`, `pipeline_id`, OIDC issuer and subject) -> commit.

Limits to keep in mind when verifying:

- Rekor stores only the SHA-256 of the record and the signature, not the
  record itself, so Rekor cannot be searched by a file's content hash. The
  full record, including `catalog_hash`, `object_hashes` and
  `published_at`, is in the job's `provenance-record.json`; its SHA-256 must
  equal `signed_record_sha256` and the hash in the Rekor entry.
- What can be checked: fetch the entry by `rekor_uuid`
  (`rekor-cli get --uuid <uuid>`), confirm the log index and integrated time,
  that the public key in the entry is this publisher's provenance key, and
  verify the SET offline with Rekor's public key. The identity claims come
  from the job record and are trustworthy to the extent `verified` is `true`.
- Records go to the public `rekor.sigstore.dev` unless `rekor_server` points
  elsewhere; they contain repository names, paths and CI identities.

---

## 9. Metrics and logs

### Metrics

The publisher serves Prometheus metrics at `GET /api/v1/metrics` on the API
listener; a receiver at `GET /metrics` on `--control-addr`. Both use their
own registry: there are no Go runtime or process metrics. Monitoring setup is
in [README.md](README.md#monitoring).

Publisher metrics that are updated:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `cvmfs_prepub_jobs_submitted_total` | counter | | Accepted submissions |
| `cvmfs_prepub_jobs_completed_total` | counter | | Jobs published individually (including already-published skips; not finalizes) |
| `cvmfs_prepub_published_bytes_total` | counter | | Payload bytes of published jobs: the submitted tar, else the pipeline's uncompressed content (staged jobs count 0; skips and finalizes are not counted) |
| `cvmfs_prepub_jobs_failed_total` | counter | | Jobs that ended in `failed` |
| `cvmfs_prepub_job_failures_by_class_total` | counter | `class` = `transient`, `permanent`, `internal` | Failures by class |
| `cvmfs_prepub_jobs_recovered_total` | counter | | Jobs reset to `incoming` by recovery at startup |
| `cvmfs_prepub_pipeline_abort_count_total` | counter | | Accepted abort requests |
| `cvmfs_prepub_spool_transitions_total` | counter | `from`, `to` | State transitions |
| `cvmfs_prepub_job_phase_seconds` | histogram | `phase` = `pipeline`, `subtree_build`, `submit_payload`, `manifest_fetch`, `commit`, `total_s0` | Phase durations (buckets 0.1 s to about 27 min) |
| `cvmfs_prepub_pipeline_files_processed_total` | counter | | Files compressed |
| `cvmfs_prepub_pipeline_bytes_compressed_total` | counter | | Compressed bytes produced |
| `cvmfs_prepub_pipeline_dedup_hits_total` | counter | | Objects already in the CAS (not uploaded) |
| `cvmfs_prepub_cas_upload_duration_seconds` | histogram | | CAS object writes |
| `cvmfs_prepub_lease_acquire_duration_seconds` | histogram | | Gateway lease acquisition |
| `cvmfs_prepub_lease_heartbeat_errors_total` | counter | | Failed lease renewals (not counting a `405` from a stock gateway) |
| `cvmfs_prepub_spool_jobs` | gauge | `state` | Jobs in each spool state |
| `cvmfs_prepub_spool_jobs_waiting_retry` | gauge | | `incoming` jobs waiting for a retry |
| `cvmfs_prepub_spool_fs_size_bytes`, `cvmfs_prepub_spool_fs_avail_bytes` | gauge | | Spool filesystem size and free space |
| `cvmfs_prepub_host_load1`, `cvmfs_prepub_host_cpus` | gauge | | Load average and CPU count |
| `cvmfs_prepub_host_memory_total_bytes`, `cvmfs_prepub_host_memory_available_bytes` | gauge | | Host memory |

Receiver metrics (pre-warm pulls triggered by announces):

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `cvmfs_receiver_pull_transactions_total` | counter | `result` = `warmed`, `failed` | Transactions pulled |
| `cvmfs_receiver_pull_objects_total` | counter | `result` = `fetched`, `skipped`, `failed` | Objects fetched, already present, or failed |
| `cvmfs_receiver_pull_duration_seconds` | histogram | | Time to pull one transaction |

Pulls triggered by `published` messages are logged but not counted.

### Logs

Logs are written to stderr as `log/slog` text lines (`time=… level=… msg=…
key=value …`); under systemd they go to the journal. `--log-level` sets the
minimum level. Lines about a job carry `job_id`. Useful messages:

| Message | Level | Meaning |
|---|---|---|
| `publish paths available` | info | Startup: the paths this node offers |
| `coarse-publish finalize is NOT configured …` | warn | Startup: `ingest_config_prefix` is unset |
| `startup probe failed` | error | Gateway or CAS unreachable; the process exits |
| `rejected unauthenticated request` | warn | `401`; `reason` says why |
| `job attempt failed — will retry` | warn | Retry scheduled; `next_attempt_at` |
| `job failed` | error | Terminal failure with the real error and `class` |
| `ingest backend: timeline` | info, warn on failure | One per publish: the non-blank output lines of the `cvmfs_server ingest` call with the seconds since it started (`+4.1s …`, or `+5.0s..+605.0s …` for a line that took that long to finish), so the time can be split between opening the transaction, swissknife and closing it. At most the first 40 and last 20 lines, each cut at 300 bytes; the ancestors transaction and the delete before a replace are not included |
| `lease abort failed — stale lease left on gateway` | error | The lease stays until the gateway expires it |
| `build will NOT be auto-published: some jobs failed` | error | A sealed build with failed members |
| `replay cache is filling up …` | warn | The nonce cache is at 80 % |
| `ignoring deprecated flags; remove them from the unit` | warn | Receiver started with removed flags |

Tracing spans are created internally but not exported.

---

## 10. Formats

### Hashing and object names

| Item | Rule |
|---|---|
| Object key | SHA-1 of the zlib-compressed object bytes, 40 lowercase hex characters |
| Suffixes | none: whole-file object; `P`: file chunk; `C`: catalog |
| Store layout | `data/<first 2 hex>/<remaining 38 hex><suffix>` under `cas.root` (localfs) or the bucket (s3), the standard CVMFS layout |
| Root hash | `new_root_hash` and `published.new_root_hash` are the 40 hex characters without the `C` |

cvmfs-prepub writes SHA-1 keys only. CVMFS also knows RIPEMD-160 and SHAKE-128
keys; see [CATALOG.md](CATALOG.md#4-content-hash-conventions) and
[CATALOG.md](CATALOG.md#8-cas-storage-layout).

### Chunking and compression

- Compression: zlib, level 6 unless `--pipeline-compress-level` is set.
- Default chunking: a fixed 6 MiB grid (`chunking.min = avg = max =
  6291456`). Every regular file, including files smaller than one chunk and
  empty files, is stored as chunk objects with the `P` suffix; the catalog
  records the chunk list. The fixed grid is what coarse finalize
  (`ingestsql`) expects.
- Content-defined chunking: when min, avg and max differ, files are cut with
  CVMFS's xor32 chunker within those bounds.
- `--chunk-avg 0`: no chunking; each file is one object without suffix.
- Deduplication: before writing an object the pipeline checks `CAS.Exists`
  (a `stat` on localfs, a `HEAD` on S3); existing objects are not uploaded
  again.

### Tar archive rules

Applied by the default path's unpacker; a violation fails the job
permanently.

| Entry | Rule |
|---|---|
| Paths | No absolute paths; no `..` component |
| Regular files | At most `max_tar_size_gib` per file on the default fixed chunk grid (larger files are spilled to disk under the spool, not held in memory); 1 GiB with content-defined chunking or `--chunk-avg 0`, as those files are read whole into memory. Negative or inconsistent sizes are refused |
| Symlinks | Target must be relative, non-empty and stay inside the archive |
| Hard links | Target must be an earlier entry of the archive |
| Duplicates | The same path twice is refused |
| Devices, FIFOs | Skipped |
| Extended attributes | PAX `SCHILY.xattr.*` records are carried into the catalog |

The `ingest` and `local` paths hand the tar to `cvmfs_server`, which applies
its own rules.

### Catalogs

The default path builds a fresh subtree catalog for the job's path from the
tar (replace-all, see [Publish backends and paths](#publish-backends-and-paths)),
splitting it into nested catalogs where the tar contains `.cvmfscatalog`
markers or a `.cvmfsdirtab` asks for them. The schema, flags, statistics and
splitting rules are in [CATALOG.md](CATALOG.md). Only the default path uses
this catalog builder; `ingest` and `local` use `cvmfs_server`, `staged` uses
the producer's catalog, and coarse finalize uses `ingestsql`.
