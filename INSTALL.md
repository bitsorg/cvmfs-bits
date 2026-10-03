# cvmfs-prepub — Installation and Operations Guide

This guide takes an operator from a fresh host to a working cvmfs-prepub
publisher, optionally with Stratum 1 pre-warming and bits-console integration,
and covers day-to-day operation, upgrades and removal. It is task oriented:
configuration keys, flags and API fields are listed once, in
[REFERENCE.md](REFERENCE.md), and linked from here. For what the service does
and why, start with [README.md](README.md).

## Contents

1. [Roles and ports](#1-roles-and-ports)
2. [Build and install](#2-build-and-install)
3. [Deploy a publisher](#3-deploy-a-publisher)
4. [Publish backends and paths](#4-publish-backends-and-paths)
5. [API authentication and secrets](#5-api-authentication-and-secrets)
6. [Verify the installation](#6-verify-the-installation)
7. [Stratum 1 pre-warming](#7-stratum-1-pre-warming)
8. [bits-console integration](#8-bits-console-integration)
9. [Several communities on one instance](#9-several-communities-on-one-instance)
10. [Operations and troubleshooting](#10-operations-and-troubleshooting)
11. [Upgrading](#11-upgrading)
12. [Uninstalling](#12-uninstalling)

---

## 1. Roles and ports

### 1.1 Roles

- **Publisher**: `cvmfs-prepub` in publisher mode (unit `cvmfs-prepub`) with the
  REST API, pipeline, spool and optional embedded control-plane broker. It may
  run on the gateway host or on its own host.
- **Gateway mode** also needs `cvmfs_gateway`, the Stratum 0 web server that
  serves `.cvmfspublished` and catalogs, and the repository's object store (a
  local directory or its S3 bucket).
- **Stratum 1 receivers** (pre-warming only): `cvmfs-prepub --mode receiver`
  (unit `cvmfs-prepub-receiver`).
- **Producers**: build runners (bits, bits-console CI) that submit tars over HTTP.

### 1.2 Prerequisites

- Build host: Go 1.24 or later (see `go.mod`) and `make`. Publisher: Linux with
  systemd; `curl` and `jq` for the checks in this guide.
- Gateway mode: a reachable `cvmfs_gateway` and a gateway key for the
  repositories. The default commit path (direct graft) needs a gateway with the
  graft endpoint (cvmfs PR #4296); with a stock gateway set
  `gateway.direct_graft: false` ([section 4.1](#41-gateway-mode-default)).
- Local mode, the ingest path or the coarse-publish finalize: the
  `cvmfs-server` package (`cvmfs_server`, `cvmfs_swissknife`) on the publisher.
- Spool disk: job state plus the unpacked content of the packages in progress.
  Size it for the largest package times the concurrent jobs, plus
  `spool_min_free_gib` (default 20 GiB; `0` disables the check), below which
  uploads are refused.

### 1.3 Ports

| Listener | Default | Flag / key | Who connects | Notes |
|---|---|---|---|---|
| REST API, web console, discovery, pull manifests and objects | `:8080` | `--listen` / `server.listen` | producers, Stratum 1 receivers, monitoring | plain HTTP ([section 5.4](#54-tls-in-front-of-the-api)) |
| Embedded broker (MQTT over WebSocket) | off, e.g. `:1882` | `--embedded-broker-ws-addr` | Stratum 1 receivers | pre-warming only; `wss://` with a certificate |
| Enroll / revoke (HTTPS) | off, e.g. `:8443` | `--enroll-tls-addr` | Stratum 1 receivers, `cvmfs-prepub revoke` | pre-warming only; revoke also works on the API port |
| pprof debug listener | off, e.g. `127.0.0.1:6060` | `--debug-listen` / `server.debug_listen` | an operator on the host | keep on loopback |
| Receiver `/metrics` | `:9100` | `--control-addr` / `control_addr` | Prometheus | plain HTTP, receiver mode |

The publisher connects out to the gateway (usually `:4929`), the S3 endpoint,
the Stratum 0 HTTP server (`stratum0_url`) and any webhook URL a producer gives.
Stratum 1 receivers connect out to the publisher's API, broker and enroll ports;
the publisher never connects to a Stratum 1.

---

## 2. Build and install

### 2.1 Build

```sh
git clone https://github.com/bitsorg/cvmfs-bits.git
cd cvmfs-bits
make build            # -> bin/cvmfs-prepub
```

`make build` stamps the version from `git describe --tags --always --dirty`
(override with `make build VERSION=...`); `bin/cvmfs-prepub --version` prints
it.

Other targets: `make test` (unit tests with the race detector), `make lint`
(`go fmt`, `go vet`), `make run-sim` (in-process cluster simulation) and
`make clean`. To build for another platform: `GOOS=linux GOARCH=amd64 make build`.

### 2.2 Run install.sh

`install.sh` installs, updates or removes the service. It must run as root and
takes the binary from `./bin` (or `--bin-dir`).

```sh
sudo ./install.sh --dry-run                        # preview, change nothing
sudo ./install.sh --skip-service                   # publisher (default mode)
sudo ./install.sh --mode receiver --skip-service   # on a Stratum 1
```

The action is `install` (default), `update` ([section 11](#11-upgrading)) or
`uninstall` ([section 12](#12-uninstalling)). When the run writes
`config.yaml` or `receiver.yaml` from the template, it enables that unit but
does not start it: configure it, then `systemctl start` it. An install over an
existing configuration starts the unit. The script then checks each unit it
started: the publisher's `/api/v1/health` on `server.listen` (default `:8080`;
an error if it does not answer), and the receiver's `/metrics` on
`control_addr` (default `:9100`), retried for up to 60 s because the receiver
opens it only after discovery succeeds (a warning, not an error, if it stays
down; check `journalctl`).

| Option | Actions | Meaning |
|---|---|---|
| `--mode publisher\|receiver\|all` | all | role; `all` installs both on one host (testing) |
| `--bin-dir DIR` | install, update | directory with the built binary (default `./bin`) |
| `--skip-service` | install | install files but do not enable or start units |
| `--user NAME` | install, update | run the services as NAME ([section 2.4](#24-service-account-and-spool-location)) |
| `--spool-dir DIR` | install, update | spool root ([section 2.4](#24-service-account-and-spool-location)) |
| `--purge-legacy` | install, uninstall | remove the legacy bits-console spool daemon without asking ([section 2.5](#25-legacy-bits-console-spool-daemon)) |
| `--legacy-spool DIR` | install, uninstall | legacy spool location (default `/mnt/build/bits/spool`) |
| `--keep-spool`, `--keep-user` | uninstall | preserve the spool, the account |
| `--purge-cas` | uninstall | also delete the CAS directory (kept by default; `--keep-cas` is accepted and does nothing) |
| `--dry-run` | all | print the actions only |
| `--yes`, `-y` | all | no confirmation prompts |
| `--help` | all | full usage |

### 2.3 What install.sh creates

| Path | Owner | Mode | Installed for |
|---|---|---|---|
| `/usr/local/bin/cvmfs-prepub` | root | 0755 | both modes |
| `/etc/cvmfs-prepub/` and `/etc/cvmfs-prepub/tls/` | `root:cvmfs-prepub` | 0750 | both modes |
| `/etc/cvmfs-prepub/config.yaml` (template; never overwritten) | `root:cvmfs-prepub` | 0640 | publisher |
| `/etc/cvmfs-prepub/receiver.yaml` (template; never overwritten) | `root:cvmfs-prepub` | 0640 | receiver |
| `/etc/cvmfs-prepub/env` (secrets skeleton; never overwritten): `CVMFS_GATEWAY_SECRET`, `PREPUB_API_TOKEN`, `CVMFS_GATEWAY_KEY_ID` for a publisher, `S1_NODE_KEY` (with the `node-key` command) for a receiver, both for `--mode all` | `root:cvmfs-prepub` | 0600 | both modes |
| spool (default `/var/spool/cvmfs-prepub`) and its `tmp/` | service user | 0700 | publisher |
| publisher CAS directory (`cas.root`, default `/srv/cvmfs/cas`) | service user | 0750 | publisher |
| receiver CAS directory (`cas.root`, default `/srv/cvmfs/stratum1/cas`) | service user | 0750 | receiver |
| `/etc/systemd/system/cvmfs-prepub.service` | root | 0644 | publisher |
| `/etc/systemd/system/cvmfs-prepub-receiver.service` | root | 0644 | receiver |

It also creates the `cvmfs-prepub` system account (no login shell, home = the
spool) and group, and adds the service user to the `cvmfs` group when that group
exists (local mode needs it). The publisher unit runs `cvmfs-prepub --config
/etc/cvmfs-prepub/config.yaml` with `EnvironmentFile=/etc/cvmfs-prepub/env`,
`TMPDIR=<spool>/tmp`, `MemoryHigh=2G`, `MemoryMax=3G` and systemd hardening; the
receiver unit runs `--config /etc/cvmfs-prepub/receiver.yaml --mode receiver`
with the same env file (`systemctl cat` shows them). To install by hand,
reproduce this table and take the unit text from `write_units_to` in
`install.sh`.

Add flags and limits with a drop-in (`systemctl edit`), not by editing the unit
file: `update` replaces a unit file whose content differs from the shipped one
(keeping a backup) but leaves drop-ins alone. A drop-in that changes the command
line must first clear it with an empty `ExecStart=` ([section 7.2](#72-publisher-flags)).

### 2.4 Service account and spool location

By default the services run as `cvmfs-prepub`. To use an existing account (for
example the repository owner, which `cvmfs_server ingest` may need) or a spool on
another volume:

```sh
sudo ./install.sh --user cvbits --spool-dir /mnt/cvmfs-prepub --skip-service
```

The account must exist; it joins the `cvmfs-prepub` group, which can read the
config and credential files, and `uninstall` never removes it. Without `--user`,
`install` and `update` keep the user the installed unit runs as (drop-ins
included). Without `--spool-dir` the spool is `spool_root` from an existing
`config.yaml`, else `/var/spool/cvmfs-prepub`; a `--spool-dir` that disagrees
with `spool_root` is refused. A spool path through a symlink is resolved, since
systemd may refuse a symlink under SELinux (`226/NAMESPACE`).

### 2.5 Legacy bits-console spool daemon

`install` looks for the earlier bits-console spool publisher
(`cvmfs-local-publish.service`, its scripts, `/etc/cvmfs-local-publish.conf` and
the legacy spool) and offers to remove it; `--purge-legacy` removes it without
asking. Removal deletes the legacy spool, so do it once cvmfs-prepub is
publishing, and never run both publishers against the same repository.

---

## 3. Deploy a publisher

This is the full procedure for a production publisher in gateway mode with an S3
CAS and coarse (whole-build) publishing, which is what bits-console uses by
default. Variations (local CAS, local mode, ingest and staged paths) are in
[section 4](#4-publish-backends-and-paths).

### Step 1 — packages and install

```sh
sudo dnf install -y cvmfs-server     # for the finalize, local mode or the ingest path
make build
sudo ./install.sh --dry-run
sudo ./install.sh --skip-service
```

The coarse-publish finalize runs `cvmfs_swissknife ingestsql`, and its
`ingestsql` must write objects through the repository's own
`CVMFS_UPSTREAM_STORAGE` (local, S3 or gateway) rather than a built-in S3-only
definition. Released cvmfs packages do not do this; the change ("ingestsql:
object spooler follows the repo upstream") is in the
[bitsorg/cvmfs](https://github.com/bitsorg/cvmfs) fork, for example its
`server/ingest-direct-s3` branch. Install that build alongside the packaged one
and note the paths of its `cvmfs_swissknife` and libraries for
`ingest_swissknife` and `ingest_env`.

### Step 2 — repository credentials

The S3 CAS reads the repository's own configuration, so these files must exist
on the publisher (copy them from the gateway host):

| File | Used for |
|---|---|
| `/etc/cvmfs/repositories.d/<repo>/server.conf` | `cas.server_conf`; its `CVMFS_UPSTREAM_STORAGE` names the S3 config |
| the S3 config it names (usually `/etc/cvmfs/keys/<repo>.s3.conf`) | endpoint, bucket, credentials, repository alias |
| `<ingest_config_prefix>/<repo>/{config,gatewaykey,pubkey}` | coarse-publish finalize: the `ingestsql` gateway client configuration (`config` sets `CVMFS_GATEWAY`, `CVMFS_STRATUM0`, `CVMFS_HTTP_PROXY`, `CVMFS_UPSTREAM_STORAGE`) |

```sh
sudo chown root:cvmfs-prepub /etc/cvmfs/keys/<repo>.s3.conf
sudo chmod 0640              /etc/cvmfs/keys/<repo>.s3.conf
```

The service refuses an S3 config that is world-accessible or group-writable.
S3 credentials come only from that file, never from `AWS_*` environment
variables or an instance role. Protect the finalize prefix the same way:
`gatewaykey` is a secret.

### Step 3 — configure

Edit `/etc/cvmfs-prepub/config.yaml`:

```yaml
server:
  listen: ":8080"
  auth_mode: both                  # see section 5

spool_root: /var/spool/cvmfs-prepub

publish_mode: gateway
gateway:
  url: http://gateway.example.org:4929
  allow_plaintext: true            # non-loopback http://, trusted network only
  # direct_graft: false            # stock gateway without the graft endpoint

stratum0_url: http://stratum0.example.org/cvmfs   # includes /cvmfs; not the gateway port
repo_name: software.example.org

cas:
  type: s3
  server_conf: /etc/cvmfs/repositories.d/software.example.org/server.conf

# Coarse-publish finalize (bits-console's default mode needs it).
ingest_config_prefix: /etc/cvmfs-prepub/ingest
ingest_swissknife: /opt/cvmfs/bin/cvmfs_swissknife   # the build from step 1
ingest_env:
  - LD_LIBRARY_PATH=/opt/cvmfs/lib

pipeline:
  workers: 2                       # peak memory scales with workers (section 10.8)
  upload_concurrency: 4

allowed_publish_prefixes:
  - /cvmfs/software.example.org/lcg
```

Every key is optional: an absent key, an empty string or a zero keeps the flag
default (except `retry_window` and `spool_min_free_gib`, where `0` disables),
and a command-line flag overrides the file (which is read because the unit
passes `--config`). Unknown keys are ignored without a warning, so check
spelling against [REFERENCE.md](REFERENCE.md#3-publisher-configuration), which
lists every key with its flag, environment variable and default. For a local CAS
use `cas: {type: localfs, root: /srv/cvmfs/cas}`, pointing at the store the
repository is served from.

A plaintext gateway URL is accepted without a flag only on loopback. Gateway
requests are HMAC-signed and the secret never travels, but plaintext exposes
what is published and lets an on-path attacker forge responses; prefer HTTPS
off-host.

Without `ingest_config_prefix`, a build submitted with `build_id` uploads,
accumulates and is never committed, and the producer, which has usually exited,
is not told. Startup warns, and `finalize_ready` in the health response is
`false`.

### Step 4 — secrets

```sh
sudo tee /etc/cvmfs-prepub/env >/dev/null <<'ENVEOF'
CVMFS_GATEWAY_KEY_ID=<gateway key id>
CVMFS_GATEWAY_SECRET=<gateway key secret>
PREPUB_API_TOKEN=<output of: openssl rand -hex 32>
ENVEOF
sudo chown root:cvmfs-prepub /etc/cvmfs-prepub/env
sudo chmod 0600 /etc/cvmfs-prepub/env
```

`CVMFS_GATEWAY_KEY_ID` defaults to `cvmfs-prepub` and must be a key the gateway
associates with the repository (on the gateway, `/etc/cvmfs/keys/<repo>.gw`
holds `plain_text <key_id> <secret>`). Do not put a comment on the same line as
a value: systemd keeps it as part of the value. The other secrets are described
in [section 5](#5-api-authentication-and-secrets).

### Step 5 — network and start

Open the API port to the producers (and, with pre-warming, to the Stratum 1s)
and allow the outbound connections in [section 1.3](#13-ports). Request signing
authenticates producers but does not encrypt; restrict the port to the networks
that need it.

```sh
sudo systemctl enable --now cvmfs-prepub
journalctl -u cvmfs-prepub -n 50 --no-pager
```

At startup the service checks that it can write to the CAS and reach the
gateway (a signed `GET /api/v1/repos`), or in local mode that `cvmfs_server` is
on `PATH`, and exits if not. The log then states the publish paths offered, the
auth mode, the temp directory, the upload limits and whether the finalize is
configured.

Then run [section 6](#6-verify-the-installation): health with
`finalize_ready: true`, metrics, and a smoke-test publish into a scratch path.

### Step 6 — cut over from an existing publisher

Skip this for a first installation. A coarse build in progress lives in the
spool of the host that received it, so drain the old host first:

1. On the old host, check that nothing is in flight or accumulating:
   `<spool_root>/builds/` should be empty, and `GET /api/v1/jobs` should list no
   job outside `published`, `failed` and `accumulated`.
2. Point the producers at the new host (`PREPUB_URL` in bits-console,
   [section 8](#8-bits-console-integration)) and publish one real build.
3. Remove the old installation, keeping its history for as long as you need it:
   `sudo ./install.sh uninstall --keep-spool`.

### Step 7 — tighten authentication

When every producer signs its requests, set `server.auth_mode: hmac`, restart,
and rotate `PREPUB_API_TOKEN` on both sides
([section 5.2](#52-moving-to-signed-requests-and-rotating-the-token)).

---

## 4. Publish backends and paths

`publish_mode` selects the backend for the default path. A job may name another
path the node offers (`publish_path` form field); a path the node does not offer
is rejected with 400. The startup log and `publish_paths` in the health response
list what a node offers:

- `prepub` (default): always offered. In gateway mode cvmfs-prepub unpacks,
  compresses and uploads, then takes a short gateway lease for the commit; in
  local mode the tar is extracted inside `cvmfs_server transaction`.
- `ingest`: offered with `ingest_publish: true` ([section 4.3](#43-the-ingest-path)).
- `staged`: offered in gateway mode with an S3 CAS
  ([section 4.4](#44-the-staged-path)).

Coarse builds and pre-warming exist only on the `prepub` path in gateway mode;
a request for either on another path is rejected with 400. What each path does
is described in [REFERENCE.md](REFERENCE.md#1-architecture).

### 4.1 Gateway mode (default)

The pipeline runs before any lease is taken; the gateway lease covers only the
commit. It needs `gateway.url` (HTTPS, loopback, or `gateway.allow_plaintext`;
`--dev` also permits plaintext but drops the secret requirements, so never use
it in production), the gateway key in the env file, `stratum0_url`, and a CAS:
`cas.type: localfs` with `cas.root`, or `cas.type: s3` with `cas.server_conf`
(or `repo_name`, from which `/etc/cvmfs/repositories.d/<repo_name>/server.conf`
is derived). On a gateway without the graft endpoint (cvmfs PR #4296) set
`gateway.direct_graft: false`.

When another publisher holds the lease, acquisition keeps retrying for up to
`--lease-retry-max` (default 12 minutes; set it above the gateway's
`max_lease_time`). `cvmfs_server publish` and other gateway clients keep working
alongside: the gateway lease serialises them.

### 4.2 Local mode

For a Stratum 0 without a gateway, `publish_mode: local` runs
`cvmfs_server transaction`, extracts the tar under `cvmfs_mount` (default
`/cvmfs`) and runs `cvmfs_server publish`. The pipeline, the CAS settings and the
gateway secrets are not used.

The service user must be allowed to run `cvmfs_server` for the repository
(`install.sh` adds it to the `cvmfs` group when the group exists). Jobs on one
repository are serialised; different repositories publish in parallel. Coarse
publishing and pre-warming need the pipeline and therefore gateway mode. Local
mode publishes every job on arrival even when it carries `build_id` or
`coarse=true`, and answers a build seal with a harmless `200` (`per_package:
true`), so producers need no change.

### 4.3 The ingest path

```yaml
ingest_publish: true
ingest_publish_owner: cvmfs        # optional: cvmfs_server ingest -u <owner>
```

Once per repository on the publisher, register a mountless gateway publisher
(`-P`: no FUSE mount, no overlay):

```sh
sudo mkdir -p /etc/cvmfs/keys
echo "plain_text <key_id> <secret>" | sudo tee /etc/cvmfs/keys/<repo>.gw >/dev/null
sudo cvmfs_server connect-gw -P -K \
    -u http://<gateway>:4929/api/v1 \
    -w <stratum0-url>/<repo> \
    -o <owner> <repo>
```

`cvmfs_server` must be on `PATH` (the service exits at startup otherwise), and
the service user must be allowed to run `cvmfs_server ingest` for the
repository, which often means running as the repository owner (`--user`,
[section 2.4](#24-service-account-and-spool-location)). `connect-gw` state
belongs to this host and does not travel with the config. `publish_mode: local`
with `ingest_publish: true` is supported; jobs on one repository are still
serialised.

Direct S3 is chosen per job: a job submitted with `direct_s3=true` (in
bits-console, the Build dialog's direct-S3 option) runs
`cvmfs_server ingest --direct-s3`, which writes data objects straight to S3,
reading `/etc/cvmfs/<repo>.s3.conf`, and sends only catalogs through the
gateway. The file's presence alone does not enable it, and the installed
`cvmfs_server` must support `--direct-s3`.

A mountless publisher cannot create the parent directories of a new target, so
the gateway must: set `CVMFS_GW_MKDIR_PARENTS=true` in the gateway's
`/etc/cvmfs/repositories.d/<repo>/server.conf`. Without it the first publish
into a new area fails with "failed to graft nested catalog".

### 4.4 The staged path

Offered in gateway mode with `cas.type: s3` (the objects are promoted by
server-side copy inside the store); without an S3 CAS it is not offered and a
staged submission gets `400`. The producer prepares the package itself and
submits `publish_path=staged` with `staging_prefix` and `catalog_hash` and no
tar. Producer-side requirements for bits-console are listed in the CI template
([section 8](#8-bits-console-integration)). `promote_workers` (default 16) sets
the copy concurrency ([section 10.8](#108-tuning)).

### 4.5 Coarse publish and the finalize

Coarse publishing is the default on the `prepub` path: a job that carries
`build_id` accumulates (state `accumulated`) instead of committing, unless it
says `coarse=false`, and the whole build is committed once by a finalize that
runs `cvmfs_swissknife ingestsql`. bits-console sends `build_id` on every job and
seals the build, after which the publisher finalizes on its own.

Prerequisites on the publisher: `ingest_config_prefix` (empty disables the
finalize), `ingest_swissknife` and `ingest_env` pointing at the build from
[section 3](#3-deploy-a-publisher) step 1, and a CAS that the repository's
upstream storage reads (in a multi-host deployment, the shared S3 store).
`GET /api/v1/health` reports `finalize_ready`, and `GET /api/v1/builds/{id}`
shows a build's progress and result. How a build is sealed and finalized, and
what happens when one of its jobs fails, is described in
[REFERENCE.md](REFERENCE.md#2-job-lifecycle).

---

## 5. API authentication and secrets

### 5.1 The API token and auth modes

Every write endpoint and the job endpoints require `PREPUB_API_TOKEN`; the
publisher refuses to start without it (only `--dev` allows that, for
development). `server.auth_mode` selects `bearer` (the token travels on every
request), `both` (default; bearer or signed) or `hmac` (signed requests only, the
token never travels). Signed requests are valid only for a short time window, so
keep producer and publisher clocks synchronised (NTP). Modes, signature format
and the endpoints that need no token are described in
[REFERENCE.md](REFERENCE.md#5-rest-api).

### 5.2 Moving to signed requests and rotating the token

1. Run with `auth_mode: both` while producers are updated. The bits-console
   pipeline signs by default (`PREPUB_SIGN` unset or `true`).
2. When every producer signs, set `server.auth_mode: hmac` and restart. Requests
   with only a bearer token are now refused with 401.
3. Rotate the token once, because until now it travelled on the wire: generate
   a new value, put it in `/etc/cvmfs-prepub/env` and in every producer (the
   bits-console CI variable), and restart the service. Requests signed with the
   old value fail with 401 until the producers have the new one.

Rotate the same way whenever the token may have leaked. Under `auth_mode: hmac`
the web console can no longer list or show jobs, because it authenticates with a
bearer token, and the curl examples in this guide need a signing client instead;
health, metrics and measurements stay open.

### 5.3 Other secrets

All secrets go in `/etc/cvmfs-prepub/env` (mode 0600), never in a YAML file:
`PREPUB_API_TOKEN`, `CVMFS_GATEWAY_KEY_ID` and `CVMFS_GATEWAY_SECRET` on every
publisher, `PREPUB_HMAC_SECRET` on a pre-warming publisher, and `S1_NODE_KEY` on
each Stratum 1 ([section 7](#7-stratum-1-pre-warming)). What each one protects
is listed in [REFERENCE.md](REFERENCE.md#7-security-model).

### 5.4 TLS in front of the API

The API listener is plain HTTP. To encrypt it, terminate TLS in a reverse proxy
(or use WireGuard) and give producers the proxy URL. Behind a path prefix (for
example `https://host/prepub`) signing clients must sign the prefixed path;
bits-console derives it from the URL it calls.

---

## 6. Verify the installation

### 6.1 Health

```sh
curl -s http://localhost:8080/api/v1/health | jq
```

```json
{"status":"healthy","publish_paths":["prepub","staged"],"auth_mode":"both",
 "finalize_ready":true,"max_tar_size":10737418240,
 "replay_cache":{"entries":0,"rejected_full":0}}
```

Check `publish_paths`, `finalize_ready` (must be `true` for coarse builds) and
`auth_mode`; a non-zero `replay_cache.rejected_full` means signed requests are
being refused for capacity reasons, which producers see as 401s.

### 6.2 Metrics

```sh
curl -s http://localhost:8080/api/v1/metrics | grep '^cvmfs_prepub_'
```

Useful for alerting: `cvmfs_prepub_job_failures_by_class_total` (label `class`:
`transient`, `permanent`, `internal`), `cvmfs_prepub_spool_jobs` (per state),
`cvmfs_prepub_spool_jobs_waiting_retry` and `cvmfs_prepub_spool_fs_avail_bytes`;
the full list is in [REFERENCE.md](REFERENCE.md#9-metrics-and-logs). Logs go to
the journal as `key=value` text (`log_level: debug` for more).

### 6.3 Smoke test

Publish a small tar into a scratch path:

```sh
export PREPUB_API_TOKEN=<token>          # bearer: needs auth_mode bearer or both
mkdir -p /tmp/smoke/hello && echo "hello cvmfs" > /tmp/smoke/hello/hello.txt
tar -cf /tmp/smoke.tar -C /tmp/smoke .

JOB=$(curl -sf -X POST http://localhost:8080/api/v1/jobs \
  -H "Authorization: Bearer $PREPUB_API_TOKEN" \
  -F "repo=software.example.org" \
  -F "path=test/smoke" \
  -F "tar=@/tmp/smoke.tar;type=application/octet-stream" | jq -r .job_id)

for i in $(seq 1 60); do
  STATE=$(curl -sf -H "Authorization: Bearer $PREPUB_API_TOKEN" \
    http://localhost:8080/api/v1/jobs/$JOB | jq -r .state)
  echo "$STATE"
  case "$STATE" in published|failed) break ;; esac
  sleep 5
done
```

Without `build_id` the job commits on its own. A `failed` job shows its error in
`GET /api/v1/jobs/$JOB` and its log in `GET /api/v1/jobs/$JOB/log`. Check the
result on a client: `ls /cvmfs/software.example.org/test/smoke/hello`. The path
must lie inside `allowed_publish_prefixes` if that is set; otherwise the
submission is refused with 403.

### 6.4 Web console and job list

The read-only web console is at `http://<host>:8080/` (`/jobs`, `/jobs/{id}`).
The page itself is public; to show jobs it asks for the API token, keeps it in
the browser, and sends it as a bearer token, so it needs `auth_mode` `bearer` or
`both`. The job list is also available as JSON, newest first:

```sh
curl -s -H "Authorization: Bearer $PREPUB_API_TOKEN" \
  http://localhost:8080/api/v1/jobs | jq -r '.[] | "\(.state)\t\(.repo)/\(.path)\t\(.job_id)"'
```

---

## 7. Stratum 1 pre-warming

Pre-warming lets Stratum 1s pull a build's new objects from the publisher before
its catalog is committed, so their next replication downloads little more than
catalogs. The commit never waits for receivers; they also catch up after each
commit. Protocol and trust model:
[REFERENCE.md](REFERENCE.md#6-pull-distribution-protocol),
[REFERENCE.md](REFERENCE.md#7-security-model).

Pre-warming needs gateway mode and the default `prepub` path. Replace
`s0.example.org` below with the publisher's public name.

### 7.1 Keys and certificates on the publisher

`/etc/cvmfs-prepub/tls` is not readable by ordinary users, so run every command
with `sudo` and absolute paths:

```sh
T=/etc/cvmfs-prepub/tls
# A CA for the broker and enroll certificate (or use your site CA).
sudo openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 \
  -subj "/CN=cvmfs-prepub CA" -keyout $T/ca.key -out $T/ca.crt
# Server certificate for the broker and enroll listeners: the public name that
# receivers use, plus localhost for the publisher's own broker clients.
sudo openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -subj "/CN=s0.example.org" -keyout $T/broker.key -out $T/broker.csr
printf 'subjectAltName=DNS:s0.example.org,DNS:localhost\n' | sudo tee $T/broker.ext >/dev/null
sudo openssl x509 -req -in $T/broker.csr -CA $T/ca.crt -CAkey $T/ca.key -CAcreateserial \
  -days 825 -extfile $T/broker.ext -out $T/broker.crt
# Ed25519 key pair that signs the discovery document.
sudo openssl genpkey -algorithm ed25519 -out $T/discovery.key
sudo openssl pkey -in $T/discovery.key -pubout -out $T/discovery.pub
sudo chown root:cvmfs-prepub $T/broker.key $T/discovery.key
sudo chmod 0640 $T/broker.key $T/discovery.key
# Master secret (publisher only).
echo "PREPUB_HMAC_SECRET=$(openssl rand -hex 32)" | sudo tee -a /etc/cvmfs-prepub/env >/dev/null
```

The publisher's own announce and notification clients connect to its broker as
`wss://localhost:<port>` and verify the certificate against `--broker-ca-cert`,
which is why the certificate names `localhost` as well. Keep `ca.key` off the
publisher once the certificate is issued. Receivers get only `ca.crt` and
`discovery.pub`.

### 7.2 Publisher flags

The control-plane settings are command-line flags with no YAML keys; add them in
a drop-in (`sudo systemctl edit cvmfs-prepub`):

```ini
[Service]
ExecStart=
ExecStart=/usr/local/bin/cvmfs-prepub --config /etc/cvmfs-prepub/config.yaml \
  --prewarm \
  --embedded-broker-ws-addr :1882 \
  --control-plane-url wss://s0.example.org:1882 \
  --embedded-broker-tls-cert /etc/cvmfs-prepub/tls/broker.crt \
  --embedded-broker-tls-key /etc/cvmfs-prepub/tls/broker.key \
  --embedded-broker-auth \
  --broker-ca-cert /etc/cvmfs-prepub/tls/ca.crt \
  --enroll-tls-addr :8443 --enroll-url https://s0.example.org:8443 \
  --discovery-signing-key /etc/cvmfs-prepub/tls/discovery.key \
  --pull-object-base-url http://s0.example.org:8080
```

`--prewarm` makes pre-warming the node default, which a job can override with
its `prewarm` field (`PREPUB_PREWARM` in bits-console); without it no pre-commit
announce is sent, but receivers still converge after each commit.
`--embedded-broker-auth` (needs `PREPUB_HMAC_SECRET`) admits only enrolled nodes,
and `--enroll-tls-addr` serves enrollment and revocation over HTTPS with the
broker certificate, so the enrollment token never travels in plaintext.
`--broker-ca-cert` lets the publisher's own clients verify the broker.
Receivers are told `--control-plane-url` and `--enroll-url`, and fetch objects
from `--pull-object-base-url` (the API base URL). Open ports 1882, 8443 and the
API port to the Stratum 1s.

Restart and check the log for `embedded broker: token authentication enabled`,
`control-plane: TLS enroll/revoke listener started` and
`control-plane: discovery advertising broker`.

### 7.3 Provision a receiver key

Each receiver authenticates with its own key, derived from the master secret and
its node id (the receiver's `node_id`, by default its hostname; `publisher` is
reserved). Print it on the publisher and hand it to the Stratum 1 operator over
a secure channel:

```sh
sudo sh -c 'set -a; . /etc/cvmfs-prepub/env; /usr/local/bin/cvmfs-prepub node-key stratum1-a'
```

### 7.4 Install and configure the receiver

On the Stratum 1:

```sh
sudo ./install.sh --mode receiver --skip-service
sudo cp ca.crt discovery.pub /etc/cvmfs-prepub/tls/      # from the publisher
echo "S1_NODE_KEY=<hex from node-key>" | sudo tee /etc/cvmfs-prepub/env >/dev/null
```

`/etc/cvmfs-prepub/receiver.yaml`:

```yaml
control_addr: ":9100"                  # plain-HTTP /metrics
node_id: stratum1-a                    # must match the node-key argument
repos:
  - software.example.org
receiver_stratum0_url: http://s0.example.org:8080   # the publisher's API base URL
broker_ca_cert: /etc/cvmfs-prepub/tls/ca.crt
cas:
  root: /srv/cvmfs/stratum1/cas
```

Discovery and authentication flags have no YAML keys; add them in a drop-in
(`sudo systemctl edit cvmfs-prepub-receiver`):

```ini
[Service]
ExecStart=
ExecStart=/usr/local/bin/cvmfs-prepub --config /etc/cvmfs-prepub/receiver.yaml --mode receiver \
  --discovery-url http://s0.example.org:8080 \
  --discovery-verify-key /etc/cvmfs-prepub/tls/discovery.pub \
  --broker-auth
```

Then `sudo systemctl enable --now cvmfs-prepub-receiver`.

`repos` is required (the receiver does not start without it): the receiver
fetches discovery for the first repository listed and acts only on announcements
for the listed repositories. The discovery signature is checked whenever
`--discovery-verify-key` is set, and `--broker-auth` requires it.
`broker_ca_cert` is also trusted, besides the system CAs, for an `https://`
discovery URL. Pulled objects are stored under `cas.root` in the CVMFS data
layout. The receiver's only secret is `S1_NODE_KEY`. Transfer tuning
(`--pull-concurrency`, `--pull-files-per-request`, `--pull-auto`) and all
receiver keys are in [REFERENCE.md](REFERENCE.md#4-receiver-configuration).

### 7.5 Verify

- Receiver log: `control-plane: broker URL learned from discovery`, then
  `receiver ready`.
- Publish a package with pre-warming on. The job passes through `distributing`,
  and the publisher logs `pull: transaction manifest stored`.
- Receiver metrics: `curl -s http://<stratum1>:9100/metrics | grep cvmfs_receiver_pull`
  shows `cvmfs_receiver_pull_transactions_total{result="warmed"}` increasing.

### 7.6 Revoke a receiver

Through the API port (needs `PREPUB_API_TOKEN` set on the publisher and
`auth_mode` `both` or `hmac`):

```sh
sudo sh -c 'set -a; . /etc/cvmfs-prepub/env; /usr/local/bin/cvmfs-prepub revoke stratum1-a \
  --api-url http://localhost:8080'
```

or through the enroll listener (`--enroll-tls-addr`), signed with
`PREPUB_HMAC_SECRET`:

```sh
sudo sh -c 'set -a; . /etc/cvmfs-prepub/env; /usr/local/bin/cvmfs-prepub revoke stratum1-a \
  --enroll-url https://s0.example.org:8443 --ca-cert /etc/cvmfs-prepub/tls/ca.crt'
```

The node is put on a denylist and its live broker sessions are closed. The
denylist is saved in `<spool_root>/revoked-nodes.json` and survives restarts
(an uninstall without `--keep-spool` deletes it). To readmit the node, run
the same command with `--undo` (`cvmfs-prepub revoke --undo stratum1-a ...`);
it uses the separate unrevoke route (`/api/v1/control/unrevoke` or
`/control/unrevoke`) and fails against a publisher that predates it.
Command and answers are in
[REFERENCE.md](REFERENCE.md#enrollment-and-broker-authentication).

---

## 8. bits-console integration

bits-console publishes through its CI template
[`.gitlab/cvmfs-prepub-publish.yml`](https://gitlab.cern.ch/buncic/bits-console/-/blob/main/.gitlab/cvmfs-prepub-publish.yml).
Its header documents every pipeline variable; this section covers what the
publisher operator has to set and check.

### 8.1 Configure bits-console

1. In the bits-console project, **Settings → CI/CD → Variables**, add
   `PREPUB_URL` (the publisher's API base URL, for example
   `http://prepub.example.org:8080`, or the TLS proxy URL) and
   `PREPUB_API_TOKEN` (the same value as on the publisher), both protected and
   masked. They may instead come from the runner's `config.toml` environment.
2. Each community selects the pipeline in `communities/<community>/ui-config.yaml`
   with `publish_pipeline: .gitlab/cvmfs-prepub-publish.yml`. An optional
   `prepub_url:` there overrides `PREPUB_URL` for that community.
3. Build runners need the tags `self-hosted` and `bits-build-<arch>` (for example
   `bits-build-x86_64`; bits-console may pin a build with `bits-host-<name>`).
   Runners need no CVMFS privileges, except for the staged path, whose runner
   requirements are listed under `PREPUB_PUBLISH_PATH` in the template.

### 8.2 Defaults that affect the publisher

| Variable | Default | Effect on the publisher |
|---|---|---|
| `PREPUB_COARSE` | `true` | every job carries `build_id` (the CI pipeline id) and accumulates; one finalize commits the build. Needs `finalize_ready: true` ([section 4.5](#45-coarse-publish-and-the-finalize)) |
| `PREPUB_WAIT` | `false` | the CI job uploads, seals the build and exits; the publisher finalizes on its own, so a green pipeline does not yet mean "published" |
| `PREPUB_SIGN` | `true` | requests are signed (`X-Bits-Auth`); works with `auth_mode` `both` or `hmac` |
| `PREPUB_PUBLISH_PATH` | `prepub` | `ingest` and `staged` require the node to offer that path ([section 4](#4-publish-backends-and-paths)) |
| `PREPUB_PREWARM` | off | `true` sends `prewarm=true` (prepub path only) |

### 8.3 Verify

Run one pipeline for a test package. The `bits-prepub-build` job log shows
`[publish] auth: PREPUB_API_TOKEN present`, the publish path or coarse mode in
use, and, with the defaults, that the build was sealed. Then follow it on the
publisher:

```sh
curl -s -H "Authorization: Bearer $PREPUB_API_TOKEN" \
  http://localhost:8080/api/v1/builds/<CI pipeline id> | jq
```

The build should reach a result with no failed members and the files should
appear on a CVMFS client. For 401s or a build that never finalizes, see
[section 10.9](#109-common-problems).

---

## 9. Several communities on one instance

One publisher can serve every community and repository it has credentials for;
the target of each job is its `repo` and `path` fields. Spool, CAS and limits
are shared.

- **Gateway key scope.** The gateway key in `CVMFS_GATEWAY_KEY_ID` must be
  allowed, in the gateway's repository access configuration, on every path the
  communities publish to. The gateway refuses a lease outside that scope.
- **Containment.** `allowed_publish_prefixes` (flag `--allowed-publish-prefix`,
  comma-separated) lists the group roots this instance may publish into, for
  example `/cvmfs/software.example.org/lcg`. A submission or reservation outside
  every root is refused with 403. List a group's root, not its `releases/`
  directory, when its user area is a sibling.
- **One API token.** All producers share `PREPUB_API_TOKEN`; deciding who may
  publish where is bits-console's job, and containment bounds the damage.
- **Monitoring.** Metrics have no per-community labels. Filter the per-publish
  measurement records by `repo` and `path` instead; they are grouped per build
  (`GET /api/v1/measurements` lists builds, `latest` is only the newest), e.g.
  `curl -s http://localhost:8080/api/v1/measurements/<build> | jq '[.[] | select(.path | startswith("lcg/"))]'`.

---

## 10. Operations and troubleshooting

### 10.1 Job lifecycle at a glance

A job moves `incoming → staging → uploading → [distributing] → leased →
committing → published`; a coarse-build member ends in `accumulated`, and a
failed or aborted job in `failed`. Each job is a directory under
`<spool_root>/<state>/` ([REFERENCE.md](REFERENCE.md#2-job-lifecycle)).

### 10.2 Retries

A failed attempt is retried with backoff until `retry_window` (default 24 h from
submission) runs out, unless the failure is permanent (for example a conflict
with already published content) or the job was aborted. Coarse-build members
and finalize jobs are not retried. `GET /api/v1/jobs/{id}` shows `attempts`,
`last_error` and `next_attempt_at`, and `cvmfs_prepub_spool_jobs_waiting_retry`
counts waiting jobs. Turn retries off with `retry_window: 0` (or
`--retry-window=0`). The schedule is in
[REFERENCE.md](REFERENCE.md#2-job-lifecycle).

### 10.3 Restarts and recovery

A restart is safe: at startup every job in a non-terminal state is resumed under
the normal concurrency limit, a clean stop does not count against the job, and a
job that repeatedly crashes the service is eventually failed instead of
crash-looping it ([REFERENCE.md](REFERENCE.md#2-job-lifecycle)). On stop the
service waits up to 30 s for requests to drain. The unit has no reload action;
configuration changes need a restart.

### 10.4 Aborting a job

```sh
curl -s -X POST -H "Authorization: Bearer $PREPUB_API_TOKEN" \
  http://localhost:8080/api/v1/jobs/<id>/abort
```

Abort applies to a queued or running job (202), including one waiting for a
concurrency slot; the job ends in `failed` and is not retried. A job that is
already terminal answers 409.

### 10.5 Spool space and payload cleanup

A job's `payload.tar` is deleted when the job reaches a final state; the failure
cause stays in its manifest, its log and the measurements. Job directories are
kept. Uploads that would leave less than `spool_min_free_gib` free are refused
with 507, and tars above `max_tar_size_gib` with 413. Watch
`cvmfs_prepub_spool_fs_avail_bytes`, and prune old `published/` and `failed/`
job directories when you no longer need their history. Measurement records live
in `<spool>/measurements` unless `measurements_dir` says otherwise (`off`
disables them).

### 10.6 Timeouts

`job_timeout` bounds a whole job from the moment it gets a concurrency slot. It
is off by default (startup warns): a wall clock cannot tell a slow job from a
stuck one, so size it against the largest package on your storage, or leave it
off. Lease acquisition on a busy path gives up after `--lease-retry-max` (12
min); S3 requests time out after 2 min without a response header and 15 min per
operation.

### 10.7 Diagnosing a stalled publisher

A pipeline stage that stops returning parks the others at zero CPU with no log
output. Enable the debug listener (`server.debug_listen: 127.0.0.1:6060`,
restart) and take a goroutine dump:

```sh
curl -s 'http://127.0.0.1:6060/debug/pprof/goroutine?debug=2' > goroutines.txt
```

Use this instead of `SIGQUIT`, which kills the process and pushes thousands of
lines through the rate-limited journal. Keep the listener on loopback: profiles
contain heap contents, including credentials. Then check the storage:
`vmstat 1` (high `b` and `wa`) and `iostat -x 1` (`%util`, `r_await`). If the
spool device is saturated, no concurrency setting helps.

### 10.8 Tuning

| Setting (YAML) | Default | When to change |
|---|---|---|
| `pipeline.workers` | 4 (template: 2) | memory lever; see below |
| `pipeline.upload_concurrency` | 4 | dedup is one `HEAD` per object on S3; raise when re-publishing mostly existing content is slow |
| `pipeline.prefetch` | on | turn off on I/O-bound storage, where the look-ahead doubles disk I/O |
| `promote_workers` | 16 | staged path only; mind the 256-connection pool per S3 host shared with uploads |

Peak memory scales with `pipeline.workers`: on the default fixed chunk grid each
worker streams one 6 MiB block at a time. With `pipeline.prefetch` off, or a
job over the prefetch budget, the inline path keeps every file of up to 1 GiB
in memory for the whole job, i.e. roughly the unpacked package; with
content-defined chunking each worker also holds a whole file
([REFERENCE.md](REFERENCE.md#pipeline)). The unit's `MemoryHigh=2G` and
`MemoryMax=3G` suit `workers: 2` on an 8 GB host shared with a gateway; with
more workers and files held whole, a large package can exceed `MemoryMax` and
systemd kills the service. On a dedicated host raise the workers and both limits
together, in a drop-in (`[Service]`, `MemoryHigh=6G`, `MemoryMax=8G`). Leave
`chunking` at its fixed 6 MiB: coarse publish requires it. Job-slot limits,
prefetch budget and the environment-variable equivalents are in
[REFERENCE.md](REFERENCE.md#3-publisher-configuration); the startup line
`publisher tuning` shows the values in effect.

### 10.9 Common problems

| Symptom | Likely cause | Fix |
|---|---|---|
| Service exits at start: `PREPUB_API_TOKEN environment variable must be set` | env file missing or not readable | [section 3](#3-deploy-a-publisher) step 4 |
| `gateway URL must use HTTPS` | non-loopback `http://` gateway | HTTPS or `gateway.allow_plaintext: true` |
| `startup probe failed` or `failed to load S3 settings` | CAS not writable or S3 config missing or too permissive; gateway unreachable or key rejected; `cvmfs_server` missing | read the error; [section 3](#3-deploy-a-publisher) steps 2 and 4 |
| CI green but nothing published | finalize not configured | set `ingest_config_prefix`; check `finalize_ready` |
| Every submission 400 naming a publish path | node does not offer that path | `ingest_publish: true`; `staged` needs gateway mode with `cas.type: s3` |
| 401 on signed requests | token mismatch, clock skew, or a proxy path prefix | compare tokens, check NTP, sign the prefixed path |
| 403 on submit | target outside `allowed_publish_prefixes` | extend the list or fix the path |
| Commit fails with a graft error | gateway without the graft endpoint | `gateway.direct_graft: false` |
| Service killed during large publishes | `MemoryMax` below what `pipeline.workers` needs | lower workers or raise the limits |

---

## 11. Upgrading

```sh
git pull && make build
sudo ./install.sh update --dry-run     # shows exactly what would change
sudo ./install.sh update
curl -s http://localhost:8080/api/v1/health | jq     # publisher
curl -s http://localhost:9100/metrics | head        # receiver (control_addr)
```

Without `--mode`, `update` works on the roles whose unit files are installed in
`/etc/systemd/system` (both units → `all`, one → that role) and refuses to run
when neither is installed; a unit for a role that is not installed is added
only when `--mode` names it. `update` prints the installed and the new version
(`cvmfs-prepub --version`; `(unknown)` for an old binary without the flag), and
the next steps for the roles updated. It refuses to run on a host that is not
installed and never writes configuration. It preserves
`config.yaml`, `env`, `receiver.yaml`, TLS material, spool, CAS, the service
account and each unit's enabled and running state. It replaces the binary, and a
unit file only if its content differs from the shipped template, after copying
the old one to `<unit>.bak-<timestamp>`. Running services are stopped for the
swap and restarted; stopped ones stay stopped. If a shipped config template
has top-level keys your `config.yaml` (publisher) or `receiver.yaml` (receiver)
lacks, `update` lists them; they are optional.

In-flight jobs survive the restart ([section 10.3](#103-restarts-and-recovery)).
There is no drain command; on a busy publisher, wait until `GET /api/v1/jobs`
shows nothing in `incoming` to `committing` before updating. Without
`install.sh`: install the new binary and `systemctl restart` the units.

### Rolling back

`update` does not keep the previous binary. To go back, build the previous
version and update to it:

```sh
git checkout <previous release tag or commit>
make build
sudo ./install.sh update
```

Unit files that `update` replaced are kept next to them as
`/etc/systemd/system/<unit>.service.bak-<YYYYmmddHHMMSS>`; rolling back writes
the older template and backs up the current file the same way. Remove drop-in
flags the older version does not define first, or it exits with
`flag provided but not defined`.

### Notes for this release

- Receiver flags `--tls-cert`, `--tls-key`, `--data-addr`, `--data-host`,
  `--session-ttl` and `--disk-headroom` are accepted but ignored, with the
  warning `ignoring deprecated flags; remove them from the unit`. Remove them
  now; a later release will reject them.
- Flags of removed features (push distribution, an external MQTT broker with
  client certificates, TLS on the API listener, `--api-token`) are no longer
  defined, and the service exits with `flag provided but not defined`. Remove
  them from units and drop-ins before updating.
- Configuration keys of removed features are ignored silently. Delete
  `gateway.key_id`, `gateway.key_secret_env`, `gateway.lease_ttl`,
  `gateway.heartbeat_interval`, `pipeline.compression`, `repositories`, the
  server TLS keys and any `distribution:` block. The gateway key id now comes
  from `CVMFS_GATEWAY_KEY_ID`.
- The admin CLI `prepubctl` is no longer built or installed; `update` and
  `uninstall` remove a leftover `/usr/local/bin/prepubctl`. Use the job API and
  the web console ([section 10.4](#104-aborting-a-job)).
- Older versions kept every job's payload. Reclaim the space once with
  `sudo find <spool_root>/{published,accumulated,failed,aborted} -mindepth 2 -maxdepth 2 -name payload.tar -delete`.

---

## 12. Uninstalling

```sh
sudo ./install.sh uninstall --dry-run                  # preview
sudo ./install.sh uninstall --keep-spool                 # installed roles, keep data
sudo ./install.sh uninstall --mode receiver              # only the receiver
sudo ./install.sh uninstall --mode all --purge-cas --yes # everything, no prompts
```

Without `--mode`, `uninstall` removes the roles whose unit files are installed
(both units → `all`, one → that role); with neither unit installed it refuses
and asks for `--mode`.

| Removed | publisher | receiver |
|---|---|---|
| unit (stopped and disabled first) | `cvmfs-prepub.service` | `cvmfs-prepub-receiver.service` |
| `/usr/local/bin/cvmfs-prepub` | yes* | yes* |
| `/etc/cvmfs-prepub/` (config, env, TLS material) | yes* | yes* |
| spool (all job history, the receiver denylist) | unless `--keep-spool` | no |
| CAS directory from `cas.root` | only with `--purge-cas` | only with `--purge-cas` |
| `cvmfs-prepub` account | unless `--keep-user`; never an account given with `--user` | same |

\* Only when the other role's unit is not installed. Otherwise only this
role's unit, its configuration file (`config.yaml` or `receiver.yaml`) and,
with `--purge-cas`, its CAS are removed; the binary, `env`, `tls/` and the
account stay.

Without `--yes` the script lists what it will remove and asks for `yes`. The
CAS is kept by default because on a local-filesystem publisher or a Stratum 1
it is the live object store: `--purge-cas` deletes published objects. A
leftover `/usr/local/bin/prepubctl` is removed too. Legacy bits-console spool
artifacts found during uninstall are removed with `--purge-legacy` or
`--yes`.

Files outside the installation (`/etc/cvmfs/keys/*`, `connect-gw` state, the
ingest config prefix) are not touched.
