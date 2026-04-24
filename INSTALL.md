# cvmfs-prepub — Installation and Deployment Guide

## Contents

1. [Prerequisites](#1-prerequisites)
2. [Building from Source](#2-building-from-source)
3. [Directory Layout and Permissions](#3-directory-layout-and-permissions)
4. [Configuration](#4-configuration)
5. [Option A — Single-Node Deployment](#5-option-a--single-node-deployment)
6. [Option B — Distributed Deployment with Stratum 1 Pre-Warming](#6-option-b--distributed-deployment-with-stratum-1-pre-warming)
7. [Systemd Setup](#7-systemd-setup)
8. [Health Check and Smoke Test](#8-health-check-and-smoke-test)
9. [Upgrading](#9-upgrading)
10. [Uninstalling](#10-uninstalling)

---

## 1. Prerequisites

**Required on the pre-publisher node:**

- Go 1.22 or later (`go version`)
- `cvmfs_gateway` ≥ 1.2 reachable from the pre-publisher node
  (for the lease-and-payload API: `POST /api/v1/leases`, `POST /api/v1/payloads`)
- Write access to the CAS backend:
  - *Local filesystem:* the directory must be on the same host as the Stratum 0 CAS (`/srv/cvmfs/cas` or equivalent)
  - *S3-compatible:* credentials with `s3:PutObject`, `s3:HeadObject`, `s3:ListObjectsV2` on the CAS bucket
- `make` and standard POSIX shell tools

**Required for Option B only:**

- Each Stratum 1 node must run the receiver agent (see §6)
- Network connectivity from the pre-publisher to every configured Stratum 1 HTTPS endpoint

**Not required:**

- `cvmfs` client tools on the pre-publisher node
- Squid or any proxy — access tracking is proxy-agnostic (see REFERENCE.md §8.1)

---

## 2. Building from Source

```sh
git clone https://github.com/your-org/cvmfs-bits.git
cd cvmfs-bits

# Download Go module dependencies
go mod download

# Build both binaries: cvmfs-prepub (service) and prepubctl (admin CLI)
make build

# Binaries are placed in bin/
ls -l bin/
# bin/cvmfs-prepub
# bin/prepubctl
```

To cross-compile for a Linux target from macOS:

```sh
GOOS=linux GOARCH=amd64 make build
```

To run the in-process cluster integration test (no external services required):

```sh
make run-sim
```

This exercises the full publish pipeline — unpack, compress, dedup, CAS upload, gateway
commit, and Stratum 1 distribution — using in-process fakes with configurable chaos.

Install binaries system-wide:

```sh
sudo install -m 755 bin/cvmfs-prepub  /usr/local/bin/
sudo install -m 755 bin/prepubctl     /usr/local/bin/
```

---

## 3. Directory Layout and Permissions

```sh
# Spool directory — owned by the service account, mode 0700
sudo mkdir -p /var/spool/cvmfs-prepub
sudo chown cvmfs-prepub:cvmfs-prepub /var/spool/cvmfs-prepub
sudo chmod 0700 /var/spool/cvmfs-prepub

# Config directory
sudo mkdir -p /etc/cvmfs-prepub/tls
sudo chown root:cvmfs-prepub /etc/cvmfs-prepub
sudo chmod 0750 /etc/cvmfs-prepub

# Config file — readable by service account only
sudo install -m 0640 -o root -g cvmfs-prepub config.yaml /etc/cvmfs-prepub/config.yaml

# Local CAS root (Option A, local filesystem backend)
sudo mkdir -p /srv/cvmfs/cas
sudo chown cvmfs-prepub:cvmfs-prepub /srv/cvmfs/cas
```

Create a dedicated system account if one does not exist:

```sh
sudo useradd -r -s /sbin/nologin -d /var/spool/cvmfs-prepub cvmfs-prepub
```

---

## 4. Configuration

The service reads a YAML config file. A minimal working config for Option A with a
local CAS is shown below; for the full annotated reference see
[REFERENCE.md §10](REFERENCE.md#10-configuration-reference).

```yaml
# /etc/cvmfs-prepub/config.yaml

server:
  listen: ":8080"
  # TLS and auth are strongly recommended in production; omit for local testing only
  # tls_cert: /etc/cvmfs-prepub/tls/server.crt
  # tls_key:  /etc/cvmfs-prepub/tls/server.key

spool_root: /var/spool/cvmfs-prepub

gateway:
  url: http://localhost:4929
  key_id: prepub-key-001
  key_secret_env: CVMFS_GATEWAY_SECRET   # export in the environment or set in the unit file
  lease_ttl: 120s
  heartbeat_interval: 40s

cas:
  type: localfs
  root: /srv/cvmfs/cas

pipeline:
  workers: 0            # 0 = runtime.NumCPU()
  compression: zlib
  upload_concurrency: 16

repositories:
  - name: atlas.cern.ch
    gc:
      enabled: false
```

**Secrets** — never put the gateway secret directly in the config file. Set it as an
environment variable in the systemd unit `EnvironmentFile` (see §7), or inject it
from a secrets manager.

For S3-compatible CAS replace the `cas:` block with:

```yaml
cas:
  type: s3
  bucket: cvmfs-cas-primary
  region: us-east-1
  endpoint: ""     # leave empty for AWS; set to e.g. http://minio:9000 for MinIO
```

S3 credentials are read from the standard AWS SDK chain (environment variables,
`~/.aws/credentials`, EC2 instance role, etc.).

---

## 5. Option A — Single-Node Deployment

Option A runs the pre-publisher on the same host as the Stratum 0, using a local
CAS. No Stratum 1 receiver agent is needed.

```
[client]  ──POST /api/v1/jobs──►  [cvmfs-prepub :8080]
                                         │
                               unpack / compress / hash
                                         │
                                    local CAS write
                                         │
                              cvmfs_gateway lease + payload
                                         │
                                   manifest commit
```

1. Build and install binaries (§2).
2. Create directories and accounts (§3).
3. Write `/etc/cvmfs-prepub/config.yaml` with `cas.type: localfs` and no
   `distribution:` block (§4).
4. Register and start the systemd service (§7).
5. Run the smoke test (§8).

The existing `cvmfs_server publish` workflow continues to work in parallel; the
gateway lease enforces mutual exclusion at the path level.

---

## 6. Option B — Distributed Deployment with Stratum 1 Pre-Warming

Option B adds a lightweight receiver agent on each Stratum 1 node. The pre-publisher
pushes new CAS objects to every configured Stratum 1 before committing the catalog,
eliminating the thundering-herd cache-miss burst on the first replication.

### 6.1 Stratum 1 receiver agent

The receiver is embedded in the same binary. On each Stratum 1 node:

```sh
sudo install -m 755 bin/cvmfs-prepub /usr/local/bin/

# Minimal config for receiver-only mode
cat > /etc/cvmfs-prepub/receiver.yaml <<'EOF'
server:
  listen: ":9100"
  tls_cert: /etc/cvmfs-prepub/tls/server.crt
  tls_key:  /etc/cvmfs-prepub/tls/server.key

cas:
  type: localfs
  root: /srv/cvmfs/stratum1/cas
EOF
```

Start with the `--mode receiver` flag (or add `mode: receiver` to the config):

```sh
cvmfs-prepub --config /etc/cvmfs-prepub/receiver.yaml --mode receiver
```

### 6.2 Pre-publisher node config

Add a `distribution:` block to the pre-publisher config on the Stratum 0 node:

```yaml
distribution:
  stratum1_endpoints:
    - https://stratum1-site-a.example.org:9100/cvmfs
    - https://stratum1-site-b.example.org:9100/cvmfs
  quorum: 0.75          # commit after 75 % of S1s acknowledge
  timeout: 10m
  commit_anyway: true   # proceed with gateway commit even if quorum not met
  per_s1_concurrency: 8
```

For a full topology diagram see [REFERENCE.md §6](REFERENCE.md#6-option-b--distributed-pre-processor-with-stratum-1-pre-warming).

---

## 7. Systemd Setup

### Service unit — pre-publisher

```ini
# /etc/systemd/system/cvmfs-prepub.service

[Unit]
Description=CVMFS Pre-Publisher Service
After=network.target

[Service]
Type=simple
User=cvmfs-prepub
Group=cvmfs-prepub
ExecStart=/usr/local/bin/cvmfs-prepub --config /etc/cvmfs-prepub/config.yaml
Restart=on-failure
RestartSec=5s

# Secrets — never put these in config.yaml
EnvironmentFile=/etc/cvmfs-prepub/env
# /etc/cvmfs-prepub/env should contain (mode 0600, owned by cvmfs-prepub):
#   CVMFS_GATEWAY_SECRET=<your-gateway-key-secret>
#   AWS_ACCESS_KEY_ID=<key>          # S3 only
#   AWS_SECRET_ACCESS_KEY=<secret>   # S3 only

# Hardening
NoNewPrivileges=true
ProtectSystem=full
PrivateTmp=true
ReadWritePaths=/var/spool/cvmfs-prepub /srv/cvmfs/cas

[Install]
WantedBy=multi-user.target
```

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now cvmfs-prepub
sudo systemctl status cvmfs-prepub
```

### Service unit — Stratum 1 receiver (Option B)

```ini
# /etc/systemd/system/cvmfs-prepub-receiver.service

[Unit]
Description=CVMFS Pre-Publisher Stratum 1 Receiver
After=network.target

[Service]
Type=simple
User=cvmfs-prepub
ExecStart=/usr/local/bin/cvmfs-prepub \
    --config /etc/cvmfs-prepub/receiver.yaml \
    --mode receiver
Restart=on-failure
RestartSec=5s
NoNewPrivileges=true
ProtectSystem=full
PrivateTmp=true
ReadWritePaths=/srv/cvmfs/stratum1/cas

[Install]
WantedBy=multi-user.target
```

---

## 8. Health Check and Smoke Test

### Health check

```sh
curl -sf http://localhost:8080/api/v1/health | jq .
# {"status":"ok","version":"0.1.0"}
```

### Prometheus metrics

```sh
curl -sf http://localhost:8080/api/v1/metrics | grep cvmfs_prepub
```

### Smoke test — submit a job and poll to completion

```sh
# Create a small test tar
mkdir -p /tmp/smoke/usr/share/test
echo "hello cvmfs" > /tmp/smoke/usr/share/test/hello.txt
tar -czf /tmp/smoke.tar.gz -C /tmp/smoke .

# Submit the job
JOB=$(curl -sf -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/octet-stream" \
  --data-binary @/tmp/smoke.tar.gz \
  -d '{"repo":"atlas.cern.ch","path":"/test/smoke"}' | jq -r .job_id)
echo "job: $JOB"

# Poll until terminal state
for i in $(seq 1 30); do
  STATE=$(curl -sf http://localhost:8080/api/v1/jobs/$JOB | jq -r .state)
  echo "$i: $STATE"
  [[ "$STATE" == "published" || "$STATE" == "failed" || "$STATE" == "aborted" ]] && break
  sleep 2
done
```

### Admin CLI

```sh
# Show all active jobs
prepubctl status

# Drain the queue — wait for in-flight jobs to finish, refuse new ones
prepubctl drain --wait

# Abort a stuck job
prepubctl abort --job $JOB
```

---

## 9. Upgrading

In-flight jobs survive a service restart: each state transition is an atomic
filesystem rename preceded by a WAL journal fsync, so the service picks up where
it left off. For a zero-downtime upgrade:

```sh
# 1. Drain — stop accepting new jobs and wait for in-flight ones to complete
prepubctl drain --wait

# 2. Replace the binary
sudo install -m 755 bin/cvmfs-prepub /usr/local/bin/

# 3. Restart
sudo systemctl restart cvmfs-prepub

# 4. Verify
curl -sf http://localhost:8080/api/v1/health | jq .
```

If the new version changes the spool directory schema, a migration note will appear
in the release changelog. Migrations are run automatically on startup; no manual
action is required unless a breaking schema change is explicitly called out.

---

## 10. Uninstalling

```sh
# Stop and disable the service
sudo systemctl stop cvmfs-prepub
sudo systemctl disable cvmfs-prepub

# Remove binaries
sudo rm /usr/local/bin/cvmfs-prepub /usr/local/bin/prepubctl

# Remove config and spool (CAUTION: deletes all job history)
sudo rm -rf /etc/cvmfs-prepub /var/spool/cvmfs-prepub

# Remove the system account
sudo userdel cvmfs-prepub
```

---

## 11. bits-console Integration

[bits-console](https://gitlab.cern.ch/hep-software/bits-console) is the
GitLab-based CI/CD front-end used to compile and publish HEP software to CVMFS.
It manages build runners, enforces access control, and drives publication through
configurable pipeline files.  `cvmfs-prepub` replaces the two-step
`bits-ingest` + `bits-cvmfs-publisher` runner flow with a single REST API call.

### 11.1 Prerequisites

Before wiring bits-console to cvmfs-prepub, confirm the following:

- `cvmfs-prepub` is installed, has a valid gateway key, and is reachable from
  the bits-console build runners over HTTPS (§2–§6).
- The bits-console GitLab project exists and at least one build runner tagged
  `self-hosted` + `bits-build-<arch>-<os>` is registered (see the bits-console
  INSTALL.txt runner registration guide for `bits-build` runner setup).
- No `bits-ingest` or `bits-publisher` runners are required for the
  cvmfs-prepub path — those are only needed for the legacy three-stage pipeline.

### 11.2 Step 1 — Add CI/CD Variables to bits-console

In the bits-console GitLab project go to **Settings → CI/CD → Variables** and
add two protected, masked variables:

| Variable | Example value | Notes |
|---|---|---|
| `PREPUB_URL` | `https://prepub.example.org:8080` | Base URL of the cvmfs-prepub API; no trailing slash |
| `PREPUB_API_TOKEN` | `<random 32-byte base64 string>` | Same token configured in the cvmfs-prepub `EnvironmentFile` |

Generate the token with:

```sh
openssl rand -base64 32
```

Set the matching value in the cvmfs-prepub server's environment file and reload:

```sh
# /etc/cvmfs-prepub/env (on the prepub host)
PREPUB_API_TOKEN=<same token as above>
```

```sh
sudo systemctl reload cvmfs-prepub
```

### 11.3 Step 2 — Add the Pipeline File

Create `.gitlab/cvmfs-prepub-publish.yml` in the bits-console repository.
This file is selected per-community via `publish_pipeline` in
`ui-config.yaml` (see §11.4).

```yaml
# .gitlab/cvmfs-prepub-publish.yml
#
# Replaces the bits-ingest + bits-cvmfs-publisher two-stage flow.
# The build runner compiles with bits, packages a tar, POSTs to cvmfs-prepub,
# then polls until the job reaches "published".

stages:
  - compile-and-publish

compile_and_publish:
  stage: compile-and-publish
  tags:
    - self-hosted
    - bits-build-${ARCHITECTURE}-${PLATFORM}
  variables:
    GIT_STRATEGY: fetch
  script:
    # 1. Fetch community config to determine the publish path
    - >
      ui_cfg=$(curl -fsSL --header "JOB-TOKEN: $CI_JOB_TOKEN"
      "${CI_API_V4_URL}/projects/${CI_PROJECT_ID}/repository/files/communities%2F${COMMUNITY}%2Fui-config.yaml/raw?ref=${CI_COMMIT_REF_NAME}"
      | python3 -c "import sys,yaml; c=yaml.safe_load(sys.stdin); print(c.get('cvmfs_prefix',''))")
    # 2. Determine per-user or admin path
    - |
      ADMINS_FILE=$(curl -fsSL --header "JOB-TOKEN: $CI_JOB_TOKEN" \
        "${CI_API_V4_URL}/projects/${CI_PROJECT_ID}/repository/files/communities%2F${COMMUNITY}%2Fui-config.yaml/raw?ref=${CI_COMMIT_REF_NAME}" \
        | python3 -c "import sys,yaml; c=yaml.safe_load(sys.stdin); print(' '.join(c.get('admins',[])))")
      if echo "$ADMINS_FILE" | grep -qw "$GITLAB_USER_LOGIN"; then
        PUBLISH_PATH="${ui_cfg}"
      else
        USER_PREFIX=$(cat communities/${COMMUNITY}/ui-config.yaml \
          | python3 -c "import sys,yaml; c=yaml.safe_load(sys.stdin); print(c.get('cvmfs_user_prefix',''))")
        PUBLISH_PATH="${USER_PREFIX}/${GITLAB_USER_LOGIN}"
      fi
    # 3. Build with bits
    - bits build --architecture $ARCHITECTURE --platform $PLATFORM
    # 4. Package the build output as a tar
    - tar -czf /tmp/build-output.tar.gz -C /tmp/bits-output .
    # 5. Submit to cvmfs-prepub
    - |
      JOB_ID=$(curl -fsSL -X POST \
        -H "Authorization: Bearer $PREPUB_API_TOKEN" \
        -H "X-Cvmfs-Path: $PUBLISH_PATH" \
        -H "Content-Type: application/x-tar" \
        --data-binary @/tmp/build-output.tar.gz \
        "${PREPUB_URL}/api/v1/jobs" | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])")
      echo "Submitted cvmfs-prepub job: $JOB_ID"
    # 6. Poll until published (timeout 30 min)
    - |
      for i in $(seq 1 180); do
        STATE=$(curl -fsSL \
          -H "Authorization: Bearer $PREPUB_API_TOKEN" \
          "${PREPUB_URL}/api/v1/jobs/${JOB_ID}" | python3 -c "import sys,json; print(json.load(sys.stdin)['state'])")
        echo "[${i}] Job ${JOB_ID} state: ${STATE}"
        [ "$STATE" = "published" ] && exit 0
        [ "$STATE" = "failed" ] && { echo "Job failed"; exit 1; }
        sleep 10
      done
      echo "Timeout waiting for job $JOB_ID"
      exit 1
  artifacts:
    when: always
    paths:
      - /tmp/bits-output/
    expire_in: 1 week
  rules:
    - if: '$CI_PIPELINE_SOURCE == "web"'
    - if: '$CI_PIPELINE_SOURCE == "api"'
```

Commit this file to the bits-console repository and push.

### 11.4 Step 3 — Set `publish_pipeline` in `ui-config.yaml`

For each community that should publish via cvmfs-prepub, open
`communities/<community>/ui-config.yaml` and change (or add) the
`publish_pipeline` key:

```yaml
# communities/LCG/ui-config.yaml  (example)
cvmfs_prefix: /cvmfs/software.cern.ch/lcg
cvmfs_user_prefix: /cvmfs/software.cern.ch/user

# Change from:
#   publish_pipeline: .gitlab/cvmfs-local-publish.yml
# To:
publish_pipeline: .gitlab/cvmfs-prepub-publish.yml

admins:
  - alice
  - bob
```

Commit and push.  From this point any build triggered for that community will
use the new pipeline.

### 11.5 Step 4 — Runner Requirements

The cvmfs-prepub pipeline needs only the `bits-build` runner — it handles
compilation, packaging, API submission, and polling in a single job.  No
dedicated `bits-ingest` or `bits-publisher` runner is required.

Each `bits-build` runner must carry two tags so GitLab can schedule the job on
the right architecture and OS:

```
self-hosted
bits-build-x86_64-el9       ← replace with actual arch-os pair
```

Register runners following the bits-console INSTALL.txt runner registration
guide (section "Build runner").  The runner user needs no special CVMFS
privileges — all CVMFS writes happen server-side inside cvmfs-prepub.

### 11.6 Step 5 — Verify the Integration

Trigger a test build from the bits-console web UI:

1. Open the bits-console GitLab project → **CI/CD → Pipelines → Run pipeline**.
2. Set the `COMMUNITY` variable to the community configured in §11.4.
3. Set `ARCHITECTURE` and `PLATFORM` to match a registered runner.
4. Click **Run pipeline** and watch the `compile_and_publish` job log.

The job log should show:

```
Submitted cvmfs-prepub job: <uuid>
[1] Job <uuid> state: uploading
[2] Job <uuid> state: distributing
...
[N] Job <uuid> state: published
```

Confirm the files are visible on CVMFS:

```sh
ls /cvmfs/software.cern.ch/lcg/
```

If the job reaches `failed`, retrieve the server-side error from the prepub API:

```sh
curl -s -H "Authorization: Bearer $PREPUB_API_TOKEN" \
    https://prepub.example.org:8080/api/v1/jobs/<uuid> | python3 -m json.tool
```

---

## 12. Multi-Community Deployment

A single `cvmfs-prepub` instance can serve all bits-console communities
simultaneously.  Access control between communities is enforced by two
independent mechanisms: the CVMFS gateway (via namespace-scoped leases) and the
bits-console pipeline itself (via `GITLAB_USER_LOGIN` checked against the
community's `admins` list in `ui-config.yaml`).

### 12.1 Namespace Isolation via the Gateway

Each community publishes to a distinct sub-path of the CVMFS repository.  The
gateway key used by cvmfs-prepub must be scoped to cover all community prefixes:

```sh
# Allow cvmfs-prepub to acquire leases anywhere under /cvmfs/software.cern.ch:
cvmfs_gateway key add prepub-service /cvmfs/software.cern.ch
```

A single broad key is appropriate when cvmfs-prepub is the only publisher and
enforces per-community path boundaries itself.  If other publishers also use the
gateway, use narrower keys (one per community sub-path) and run a separate
cvmfs-prepub instance per community.

### 12.2 Community `ui-config.yaml` Settings

Each community declares its own paths and admin list.  The bits-console pipeline
reads these at CI job runtime via the GitLab API (using `CI_JOB_TOKEN`) and
applies them server-side before calling cvmfs-prepub:

| `ui-config.yaml` field | Purpose |
|---|---|
| `cvmfs_prefix` | Publish path for admin users |
| `cvmfs_user_prefix` | Prefix for per-user sandbox paths |
| `publish_pipeline` | Pipeline file selected for this community |
| `admins` | GitLab login names with admin-path write access |

A community with:

```yaml
cvmfs_prefix: /cvmfs/software.cern.ch/lcg
cvmfs_user_prefix: /cvmfs/software.cern.ch/user
admins: [alice, bob]
```

will publish alice's builds to `/cvmfs/software.cern.ch/lcg/` and all other
users' builds to `/cvmfs/software.cern.ch/user/<login>/`.

### 12.3 Single cvmfs-prepub Instance for All Communities

No per-community cvmfs-prepub instances are needed.  The publish path is passed
by the bits-console pipeline in the `X-Cvmfs-Path` HTTP header; cvmfs-prepub
treats each path independently within the same spool and CAS:

```
Community A build  ──┐
Community B build  ──┼──▶  cvmfs-prepub :8080  ──▶  cvmfs_gateway  ──▶  Stratum 1
Community C build  ──┘         (shared)
```

The systemd unit from §3 requires no changes.  The gateway key must be broad
enough to cover all community prefixes (see §12.1).

### 12.4 Runner Tagging for Multiple Communities

If different communities target different architectures or OS platforms,
register multiple `bits-build` runners, each tagged accordingly:

```
self-hosted + bits-build-x86_64-el9    ← EL9 x86_64 (LCG, ATLAS, CMS …)
self-hosted + bits-build-aarch64-el9   ← EL9 ARM64
self-hosted + bits-build-x86_64-el8    ← EL8 (legacy communities)
```

GitLab's runner matching (`tags:` in the pipeline YAML) routes each
`compile_and_publish` job to the correct host automatically.  No changes to
the cvmfs-prepub server are required when adding new runners.

### 12.5 Monitoring Across Communities

The single cvmfs-prepub instance exposes per-job metrics with the path label
set to the `X-Cvmfs-Path` value, allowing Grafana dashboards to show
per-community throughput, failure rates, and publish latencies without running
separate instances.

Key metrics:

| Metric | What to watch |
|---|---|
| `cvmfs_prepub_jobs_submitted_total` | Build cadence (label: `path`) |
| `cvmfs_prepub_pipeline_dedup_hits_total` | Cross-community dedup effectiveness |
| `cvmfs_prepub_cas_upload_duration_seconds` | CAS upload performance |
| `cvmfs_prepub_distribution_duration_seconds` | Stratum 1 push latency (Option B) |
| `cvmfs_prepub_jobs_recovered_total` | Crash recovery events |
| `cvmfs_prepub_job_failures_by_class_total` | Failure classification |

Set an alert on `job_failures_by_class_total{class="permanent"}` to detect
misconfiguration (wrong gateway URL, revoked token, malformed tar) before it
affects users.
