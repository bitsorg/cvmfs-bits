#!/usr/bin/env bash
# run.sh – end-to-end gateway publish integration test for cvmfs-bits.
#
# Brings up a mountless cvmfs_gateway backed by S3 (Garage) via
# docker-compose.yml, then drives a full publish through cvmfs-prepub in
# gateway mode:
#
#   1. build + start the Garage + mountless-gateway stack (needs CVMFS_SRC)
#   2. build cvmfs-prepub and run it on the host in gateway mode
#   3. submit a publish job (a small tar) via the prepub HTTP API
#   4. poll the job to a terminal state; require "published"
#   5. confirm the repository's .cvmfspublished is served from the S3 backend
#
# The publisher is cvmfs-prepub itself (it speaks the gateway lease/payload/
# commit API directly) — there is no cvmfs_server publisher container.
#
# Environment
# -----------
#   CVMFS_SRC   (required) path to a cvmfs checkout on the devel branch
#   KEEP_UP     (optional) if set to 1, leave the stack + prepub running on exit
#
# Usage
# -----
#   CVMFS_SRC=/path/to/cvmfs ./run.sh

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration (matches docker-compose.yml)
# ---------------------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HERE}/../../.." && pwd)"

REPO_NAME="test.repo.org"
PUBLISH_PATH="hello"                 # a brand-new subtree → direct-graft valid
GATEWAY_URL="http://localhost:4929"
GARAGE_WEB="http://localhost:3902"
GARAGE_WEB_HOST="cvmfs.web.garage.internal"

# Gateway lease key.  The gateway entrypoint writes "plain_text mykey mysecret"
# to /etc/cvmfs/keys/<repo>.gw and the mounted repo.json associates that key
# with test.repo.org, so prepub must sign with the same id/secret.  (These are
# fixed dev/test credentials, matching docker-compose.yml's GW_KEY_* env.)
export CVMFS_GATEWAY_KEY_ID="mykey"
export CVMFS_GATEWAY_SECRET="mysecret"

# Bearer token guarding the prepub HTTP API.
export PREPUB_API_TOKEN="integration-test-token"

PREPUB_LISTEN="127.0.0.1:8080"
PREPUB_API="http://${PREPUB_LISTEN}"

: "${CVMFS_SRC:?set CVMFS_SRC to a cvmfs checkout (devel branch)}"
export CVMFS_SRC

COMPOSE=(docker compose -f "${HERE}/docker-compose.yml")

WORKDIR="$(mktemp -d)"
PREPUB_PID=""

log() { printf '\n\033[1;34m[run.sh]\033[0m %s\n' "$*"; }
err() { printf '\n\033[1;31m[run.sh] ERROR:\033[0m %s\n' "$*" >&2; }

# ---------------------------------------------------------------------------
# Teardown – always dump gateway logs and tear the stack down (unless KEEP_UP)
# ---------------------------------------------------------------------------
cleanup() {
    local rc=$?
    if [[ -n "${PREPUB_PID}" ]] && kill -0 "${PREPUB_PID}" 2>/dev/null; then
        kill "${PREPUB_PID}" 2>/dev/null || true
        wait "${PREPUB_PID}" 2>/dev/null || true
    fi
    if [[ "${rc}" -ne 0 ]]; then
        err "failed (exit ${rc}) — dumping diagnostics"
        [[ -f "${WORKDIR}/prepub.log" ]] && { echo "──── prepub.log ────"; tail -n 100 "${WORKDIR}/prepub.log"; }
        echo "──── gateway logs ────"; "${COMPOSE[@]}" logs --no-color --tail 100 gateway 2>/dev/null || true
    fi
    if [[ "${KEEP_UP:-0}" != "1" ]]; then
        log "tearing down stack"
        "${COMPOSE[@]}" down -v --remove-orphans 2>/dev/null || true
        rm -rf "${WORKDIR}"
    else
        log "KEEP_UP=1 — leaving stack up; artifacts in ${WORKDIR}"
    fi
    exit "${rc}"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Poll a URL until it responds (2xx/4xx) or times out.
# ---------------------------------------------------------------------------
wait_for_http() {
    local url="$1" name="$2" tries="${3:-60}"
    log "waiting for ${name} at ${url}"
    for ((i = 0; i < tries; i++)); do
        if curl -s -o /dev/null --max-time 3 "${url}"; then
            log "${name} is up"
            return 0
        fi
        sleep 2
    done
    err "${name} did not become ready at ${url}"
    return 1
}

# ---------------------------------------------------------------------------
# 1. Build + start the gateway stack
# ---------------------------------------------------------------------------
log "building + starting Garage + mountless gateway (CVMFS_SRC=${CVMFS_SRC})"
"${COMPOSE[@]}" up --build -d

# The gateway entrypoint runs `cvmfs_server mkfs` on first boot, which takes a
# while; poll the lease API root until it answers.
wait_for_http "${GATEWAY_URL}/api/v1" "gateway lease API" 120

# ---------------------------------------------------------------------------
# 2. Build + start cvmfs-prepub (gateway mode) on the host
# ---------------------------------------------------------------------------
log "building cvmfs-prepub"
( cd "${REPO_ROOT}" && go build -o "${WORKDIR}/cvmfs-prepub" ./cmd/prepub )

mkdir -p "${WORKDIR}/spool" "${WORKDIR}/cas"

log "starting cvmfs-prepub against ${GATEWAY_URL}"
# --stratum0-url is REQUIRED for gateway publishing: prepub only builds the
# subtree catalog (BuildSubtree, which produces new_root_hash) when a stratum0
# URL is configured.  Without it the commit sends a null new_root_hash and the
# receiver rejects the DirectGraft with "merge_error" ("DirectGraft requires a
# catalog hash").  We point it at Garage's web endpoint on localhost; because
# Garage routes buckets by Host header, a bare localhost request for a fresh
# repo returns 404, which prepub correctly treats as "first publish, no existing
# manifest" (empty old_root_hash) — exactly right here.  The receiver fetches
# the real base manifest itself over the compose network, so an empty
# old_root_hash does not affect the graft.
"${WORKDIR}/cvmfs-prepub" \
    --dev \
    --publish-mode gateway \
    --gateway-url "${GATEWAY_URL}" \
    --gateway-direct-graft=true \
    --stratum0-url "${GARAGE_WEB}" \
    --listen "${PREPUB_LISTEN}" \
    --spool-root "${WORKDIR}/spool" \
    --cas-type localfs \
    --cas-root "${WORKDIR}/cas" \
    --repo-name "${REPO_NAME}" \
    > "${WORKDIR}/prepub.log" 2>&1 &
PREPUB_PID=$!

wait_for_http "${PREPUB_API}/api/v1/health" "prepub API" 30

# ---------------------------------------------------------------------------
# 3. Submit a publish job (a small tar at a brand-new subtree)
# ---------------------------------------------------------------------------
log "building payload tar"
mkdir -p "${WORKDIR}/payload/${PUBLISH_PATH}"
echo "hello from cvmfs-bits gateway integration test" \
    > "${WORKDIR}/payload/${PUBLISH_PATH}/greeting.txt"
tar -C "${WORKDIR}/payload/${PUBLISH_PATH}" -cf "${WORKDIR}/payload.tar" .

log "submitting publish job (repo=${REPO_NAME} path=${PUBLISH_PATH})"
submit_resp="$(curl -sf \
    -H "Authorization: Bearer ${PREPUB_API_TOKEN}" \
    -F "repo=${REPO_NAME}" \
    -F "path=${PUBLISH_PATH}" \
    -F "tar=@${WORKDIR}/payload.tar" \
    "${PREPUB_API}/api/v1/jobs")"
echo "submit response: ${submit_resp}"

job_id="$(printf '%s' "${submit_resp}" | sed -n 's/.*"job_id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')"
[[ -n "${job_id}" ]] || { err "no job_id in submit response"; exit 1; }
log "job id: ${job_id}"

# ---------------------------------------------------------------------------
# 4. Poll the job to a terminal state
# ---------------------------------------------------------------------------
log "polling job ${job_id}"
state=""
for ((i = 0; i < 90; i++)); do
    job_json="$(curl -sf -H "Authorization: Bearer ${PREPUB_API_TOKEN}" \
        "${PREPUB_API}/api/v1/jobs/${job_id}" || true)"
    state="$(printf '%s' "${job_json}" | sed -n 's/.*"state"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')"
    case "${state}" in
        published)
            log "job published"
            echo "${job_json}"
            break
            ;;
        failed|aborted)
            err "job reached terminal state '${state}'"
            echo "${job_json}"
            exit 1
            ;;
    esac
    sleep 2
done
[[ "${state}" == "published" ]] || { err "job did not publish (last state='${state:-none}')"; exit 1; }

# ---------------------------------------------------------------------------
# 5. Confirm the published manifest is readable from the S3 backend
# ---------------------------------------------------------------------------
log "verifying .cvmfspublished on the S3 backend"
if curl -sf --max-time 10 -H "Host: ${GARAGE_WEB_HOST}" \
        "${GARAGE_WEB}/${REPO_NAME}/.cvmfspublished" -o "${WORKDIR}/cvmfspublished"; then
    root_hash="$(sed -n 's/^C\(.*\)/\1/p' "${WORKDIR}/cvmfspublished" | head -1)"
    log "published root catalog hash: ${root_hash:-<unparsed>}"
else
    err ".cvmfspublished not readable from ${GARAGE_WEB}/${REPO_NAME}/"
    exit 1
fi

log "SUCCESS: cvmfs-prepub published ${REPO_NAME}:/${PUBLISH_PATH} through the gateway"
