#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: Apache-2.0

# install.sh — Install or uninstall cvmfs-prepub on this host.
#
# Usage:
#   sudo ./install.sh [ACTION] [OPTIONS]
#
# ACTION (default: install):
#   install      Install cvmfs-prepub binaries, config, spool, and systemd
#                units.  Detects legacy bits-console spool services and offers
#                to clean them up.
#   update       Upgrade an EXISTING installation in place: replaces the
#                binaries (and any systemd unit whose content changed, backing
#                up the previous one first) while PRESERVING config.yaml, env
#                secrets, receiver.yaml, TLS material, spool and CAS.  Services
#                are stopped for the swap and restarted only if they were
#                running.  Fails if the host is not already installed.
#   uninstall    Remove a cvmfs-prepub installation from this host.
#
# ── INSTALL OPTIONS ────────────────────────────────────────────────────────────
#   --mode MODE         Role to install on this host:
#                         publisher  (default) — pre-publisher + REST API
#                                    service on the Stratum-0/gateway node
#                         receiver   — Stratum-1 receiver agent only
#                         all        — both publisher and receiver on the same
#                                    node (testing / single-host deployments)
#   --bin-dir DIR       Directory containing pre-built binaries
#                       (default: ./bin relative to this script)
#   --skip-service      Install files but do not enable or start systemd units
#   --purge-legacy      Automatically stop and remove legacy bits-console spool
#                       services (cvmfs-local-publish daemon, cvmfs-spool-submit,
#                       config, and spool directory) if detected.  Without this
#                       flag the script warns and prompts interactively.
#   --legacy-spool DIR  Path to legacy spool root (default: /mnt/build/bits/spool)
#
# ── UNINSTALL OPTIONS ──────────────────────────────────────────────────────────
#   --mode MODE         What to uninstall:
#                         publisher  (default)
#                         receiver
#                         all
#   --keep-spool        Preserve /var/spool/cvmfs-prepub (job history + WAL).
#   --keep-cas          Preserve the local CAS data directory.
#   --keep-user         Preserve the cvmfs-prepub system account.
#
# ── COMMON OPTIONS ─────────────────────────────────────────────────────────────
#   --dry-run           Print every action that would be taken; make no changes.
#   --yes               Skip all interactive confirmation prompts.
#   --help              Show this message.
#
# Examples:
#   # Install the publisher service (builds must already be in ./bin/)
#   sudo ./install.sh
#
#   # Install publisher + auto-remove legacy spool daemon if found
#   sudo ./install.sh --purge-legacy
#
#   # Install receiver agent on a Stratum-1 node
#   sudo ./install.sh --mode receiver
#
#   # Preview install — no changes made
#   sudo ./install.sh --dry-run
#
#   # Upgrade binaries after a rebuild, keeping all configuration
#   sudo ./install.sh update
#
#   # Preview exactly what an upgrade would change
#   sudo ./install.sh update --dry-run
#
#   # Remove publisher (keep job history and CAS objects)
#   sudo ./install.sh uninstall --keep-spool --keep-cas
#
#   # Remove receiver on a Stratum-1 node
#   sudo ./install.sh uninstall --mode receiver
#
#   # Full removal without prompts (automation / CI)
#   sudo ./install.sh uninstall --mode all --yes

set -euo pipefail

# ── constants ─────────────────────────────────────────────────────────────────
readonly PROG="$(basename "$0")"
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# cvmfs-prepub install targets
readonly BINARY_DIR="/usr/local/bin"
readonly CONFIG_DIR="/etc/cvmfs-prepub"
readonly SPOOL_DIR="/var/spool/cvmfs-prepub"
readonly DEFAULT_CAS_PUB="/srv/cvmfs/cas"
readonly DEFAULT_CAS_RCV="/srv/cvmfs/stratum1/cas"
readonly SERVICE_USER="cvmfs-prepub"
readonly SVC_PUB="cvmfs-prepub"
readonly SVC_RCV="cvmfs-prepub-receiver"
readonly UNIT_DIR="/etc/systemd/system"

# Legacy bits-console spool artifacts (pre-cvmfs-prepub deployment)
readonly LEGACY_SVC="cvmfs-local-publish"
readonly LEGACY_DAEMON_BIN="/usr/local/sbin/cvmfs-local-publish.sh"
readonly LEGACY_SUBMIT_BIN="/usr/local/bin/cvmfs-spool-submit.sh"
readonly LEGACY_CONF="/etc/cvmfs-local-publish.conf"
readonly LEGACY_SPOOL_DEFAULT="/mnt/build/bits/spool"

# ── defaults ──────────────────────────────────────────────────────────────────
ACTION="install"
MODE="publisher"
DRY_RUN=false
YES=false

# install-specific
BIN_DIR="${SCRIPT_DIR}/bin"
SKIP_SERVICE=false
PURGE_LEGACY=false
LEGACY_SPOOL_DIR="$LEGACY_SPOOL_DEFAULT"

# uninstall-specific
KEEP_SPOOL=false
KEEP_CAS=false
KEEP_USER=false

# ── counters ──────────────────────────────────────────────────────────────────
DONE=0
SKIPPED=0
ERRS=0
NEED_DAEMON_RELOAD=false

# ── colour (suppressed when output is not a terminal) ─────────────────────────
if [ -t 1 ]; then
    RED='\033[0;31m'  YELLOW='\033[1;33m'  GREEN='\033[0;32m'
    BOLD='\033[1m'    DIM='\033[2m'        RESET='\033[0m'
else
    RED='' YELLOW='' GREEN='' BOLD='' DIM='' RESET=''
fi

# ── output helpers ────────────────────────────────────────────────────────────
usage() {
    # Print every header comment line (strip leading "# ?" prefix).
    # Stop at the first line that is NOT a comment (i.e. "set -euo pipefail").
    awk 'NR==1{next} /^[^#]/{exit} {sub(/^# ?/,""); print}' "$0"
    exit 0
}

die()     { printf "${RED}ERROR:${RESET} %s\n" "$*" >&2; exit 1; }
header()  { printf "\n${BOLD}── %s %s${RESET}\n" "$1" \
                   "$(printf '%.0s─' {1..50} | head -c $((52 - ${#1})))"; }
ok()      { printf "  ${GREEN}✓${RESET}  %s\n"  "$*";  DONE=$((DONE + 1)); }
skip()    { printf "  ${DIM}-  %s${RESET}\n"    "$*";  SKIPPED=$((SKIPPED + 1)); }
warn()    { printf "  ${YELLOW}!${RESET}  %s\n" "$*"; }
info()    { printf "  ${BOLD}»${RESET}  %s\n"   "$*"; }
dry()     { printf "  ${YELLOW}[dry-run]${RESET}  %s\n" "$*"; }
err()     { printf "  ${RED}✗${RESET}  %s\n"    "$*" >&2; ERRS=$((ERRS + 1)); }

# run DESCRIPTION CMD [ARGS...]
# Execute CMD or print it in dry-run mode.
run() {
    local desc="$1"; shift
    if $DRY_RUN; then
        dry "$desc"
        return 0
    fi
    if "$@" 2>/dev/null; then
        ok "$desc"
    else
        local rc=$?
        err "Failed ($rc): $desc"
    fi
}

# confirm PROMPT — ask for explicit "yes"; return 1 if declined.
confirm() {
    local prompt="${1:-Continue?}"
    if $YES; then return 0; fi
    printf "\n  %s\n  Type 'yes' to continue, anything else to abort: " "$prompt"
    local ans; read -r ans
    [[ "${ans:-}" == "yes" ]]
}

# ── systemd helpers ───────────────────────────────────────────────────────────
has_systemd()    { command -v systemctl &>/dev/null; }
unit_file()      { echo "${UNIT_DIR}/${1}.service"; }
unit_exists()    { [ -f "$(unit_file "$1")" ]; }
svc_active()     { has_systemd && systemctl is-active --quiet "${1}.service" 2>/dev/null; }
svc_enabled()    { has_systemd && systemctl is-enabled --quiet "${1}.service" 2>/dev/null; }

stop_disable() {
    local svc="$1"
    if ! has_systemd; then
        skip "systemctl not available — skipping service management"
        return
    fi
    if ! unit_exists "$svc" && ! svc_active "$svc"; then
        skip "Service ${svc}.service — not installed"
        return
    fi
    if svc_active "$svc"; then
        run "Stop ${svc}.service" systemctl stop "${svc}.service"
    else
        skip "Service ${svc}.service — already stopped"
    fi
    if svc_enabled "$svc"; then
        run "Disable ${svc}.service" systemctl disable "${svc}.service"
    else
        skip "Service ${svc}.service — already disabled"
    fi
}

remove_unit() {
    local svc="$1"
    local f; f="$(unit_file "$svc")"
    if [ -f "$f" ]; then
        run "Remove unit file $f" rm -f "$f"
        NEED_DAEMON_RELOAD=true
    else
        skip "Unit file $f — not found"
    fi
}

maybe_daemon_reload() {
    if ! has_systemd; then return; fi
    if $NEED_DAEMON_RELOAD || $DRY_RUN; then
        run "Reload systemd daemon" systemctl daemon-reload
        NEED_DAEMON_RELOAD=false
    fi
}

# ── filesystem helpers ────────────────────────────────────────────────────────
remove_file() {
    local path="$1" label="${2:-}"
    local desc="${label:-$path}"
    if [ -f "$path" ] || [ -L "$path" ]; then
        run "Remove $desc" rm -f "$path"
    else
        skip "$desc — not found"
    fi
}

remove_dir() {
    local path="$1" label="$2" warn_data="${3:-false}"
    if [ ! -d "$path" ]; then
        skip "$label — not found"
        return
    fi
    if $warn_data && ! $DRY_RUN; then
        warn "DATA LOSS: removing $path — this cannot be undone."
    fi
    if $DRY_RUN; then
        local suffix=""
        $warn_data && suffix="  *** DATA LOSS ***"
        dry "Remove $label${suffix}"
    else
        run "Remove $label" rm -rf "$path"
    fi
}

ensure_dir() {
    local path="$1" owner="${2:-root:root}" mode="${3:-755}"
    if [ -d "$path" ]; then
        skip "Directory $path — already exists"
    else
        run "Create directory $path" mkdir -p "$path"
    fi
    run "Set ownership $path → $owner" chown "$owner" "$path"
    run "Set mode $path → $mode"       chmod "$mode"  "$path"
}

# read_cas_root CFG DEFAULT — extract cas.root from a YAML config file.
read_cas_root() {
    local cfg="$1" default="$2"
    if [ ! -f "$cfg" ]; then echo "$default"; return; fi
    local val
    val=$(awk '/^cas[[:space:]]*:/{in_cas=1; next}
               in_cas && /^[^ ]/{in_cas=0}
               in_cas && /root[[:space:]]*:/{
                   sub(/.*root[[:space:]]*:[[:space:]]*/,""); print; exit
               }' "$cfg" 2>/dev/null || true)
    echo "${val:-$default}"
}

# ── argument parsing ──────────────────────────────────────────────────────────
# Consume optional positional action (install / uninstall) first.
if [[ $# -gt 0 ]]; then
    case "$1" in
        install|update|uninstall) ACTION="$1"; shift ;;
    esac
fi

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)         DRY_RUN=true ;;
        --yes|-y)          YES=true ;;
        --mode)            shift; MODE="${1:-}" ;;
        # install options
        --bin-dir)         shift; BIN_DIR="${1:-}" ;;
        --skip-service)    SKIP_SERVICE=true ;;
        --purge-legacy)    PURGE_LEGACY=true ;;
        --legacy-spool)    shift; LEGACY_SPOOL_DIR="${1:-}" ;;
        # uninstall options
        --keep-spool)      KEEP_SPOOL=true ;;
        --keep-cas)        KEEP_CAS=true ;;
        --keep-user)       KEEP_USER=true ;;
        --help|-h)         usage ;;
        # allow bare --uninstall / --install as synonyms
        --uninstall)       ACTION="uninstall" ;;
        --install)         ACTION="install" ;;
        --update)          ACTION="update" ;;
        *) die "Unknown option: '$1'.  Run '${PROG} --help' for usage." ;;
    esac
    shift
done

case "$MODE" in
    publisher|receiver|all) ;;
    *) die "Unknown --mode '$MODE'.  Valid values: publisher, receiver, all." ;;
esac

# ── privilege check ───────────────────────────────────────────────────────────
[[ $EUID -eq 0 ]] || die "This script must be run as root.  Try: sudo $PROG $*"

# ═════════════════════════════════════════════════════════════════════════════
# LEGACY DETECTION AND REMOVAL
# Identifies bits-console spool-daemon artifacts and optionally removes them.
# Called during install to offer a clean migration; also available standalone.
# ═════════════════════════════════════════════════════════════════════════════

# legacy_present -- return 0 if any legacy artifact is found on this host.
legacy_present() {
    unit_exists "$LEGACY_SVC"     ||
    svc_active  "$LEGACY_SVC"     ||
    [ -f "$LEGACY_DAEMON_BIN" ]   ||
    [ -f "$LEGACY_SUBMIT_BIN" ]   ||
    [ -f "$LEGACY_CONF"       ]   ||
    [ -d "$LEGACY_SPOOL_DIR"  ]
}

# remove_legacy -- stop and remove all legacy bits-console spool artifacts.
remove_legacy() {
    header "Legacy bits-console spool service"
    info "Detected: cvmfs-local-publish (bits-console spool daemon)"
    info "This is superseded by cvmfs-prepub — removing legacy artifacts."

    stop_disable "$LEGACY_SVC"
    remove_unit  "$LEGACY_SVC"
    maybe_daemon_reload

    remove_file "$LEGACY_DAEMON_BIN" "cvmfs-local-publish.sh (daemon binary)"
    remove_file "$LEGACY_SUBMIT_BIN" "cvmfs-spool-submit.sh (submit helper)"
    remove_file "$LEGACY_CONF"       "cvmfs-local-publish.conf"

    # Read custom spool path from legacy config if present
    local legacy_conf_spool=""
    if [ -f "$LEGACY_CONF" ]; then
        legacy_conf_spool=$(awk -F= '/^SPOOL_DIR/{print $2}' "$LEGACY_CONF" \
                            | tr -d ' "' | head -1 || true)
    fi
    local spool_to_remove="${legacy_conf_spool:-$LEGACY_SPOOL_DIR}"

    if [ -d "$spool_to_remove" ]; then
        warn "Legacy spool at $spool_to_remove contains job history."
        warn "Removing it is safe once you have verified cvmfs-prepub is working."
        remove_dir "$spool_to_remove" "legacy spool $spool_to_remove" true
    else
        skip "Legacy spool $spool_to_remove — not found"
    fi
}

# ═════════════════════════════════════════════════════════════════════════════
# INSTALL
# ═════════════════════════════════════════════════════════════════════════════

# install_prereq_check -- verify binaries exist before attempting install.
install_prereq_check() {
    header "Prerequisites"
    local missing=0

    for bin in cvmfs-prepub prepubctl; do
        local path="${BIN_DIR}/${bin}"
        if [ -f "$path" ] && [ -x "$path" ]; then
            ok "Binary found: $path"
        else
            err "Binary not found or not executable: $path"
            info "Build it first:  make build"
            missing=$((missing + 1))
        fi
    done

    if ! command -v systemctl &>/dev/null; then
        warn "systemctl not found — service management will be skipped"
    fi

    if command -v python3 &>/dev/null; then
        ok "python3 found"
    else
        warn "python3 not found — required by bits-console pipeline scripts on the runner"
    fi

    if [[ $missing -gt 0 ]]; then
        die "$missing required binary/binaries missing in ${BIN_DIR}/ — run 'make build' first."
    fi
}

# install_account -- create the service account if not present.
install_account() {
    header "Service Account"
    if id "$SERVICE_USER" &>/dev/null 2>&1; then
        skip "Account '${SERVICE_USER}' — already exists"
    else
        run "Create system account '${SERVICE_USER}'" \
            useradd -r -s /sbin/nologin \
                    -d "$SPOOL_DIR" \
                    -c "cvmfs-prepub service" \
                    "$SERVICE_USER"
    fi
    # For local publish mode: add to cvmfs group so cvmfs_server can be called
    if getent group cvmfs &>/dev/null; then
        if id -nG "$SERVICE_USER" 2>/dev/null | grep -qw "cvmfs"; then
            skip "Account '${SERVICE_USER}' already in group 'cvmfs'"
        else
            run "Add '${SERVICE_USER}' to group 'cvmfs' (required for local publish mode)" \
                usermod -aG cvmfs "$SERVICE_USER"
        fi
    else
        skip "Group 'cvmfs' not present — skip (needed only when publish_mode: local)"
    fi
}

# install_dirs -- create spool, config, and CAS directories.
install_dirs() {
    header "Directories"

    case "$MODE" in
        publisher|all)
            ensure_dir "$SPOOL_DIR"   "${SERVICE_USER}:${SERVICE_USER}" "0700"
            # Temporaries live on the spool volume, never on /tmp: catalog
            # downloads and finalize work dirs are far larger than a typical
            # /tmp, which under systemd PrivateTmp may even be RAM-backed.
            ensure_dir "${SPOOL_DIR}/tmp" "${SERVICE_USER}:${SERVICE_USER}" "0700"
            ensure_dir "$CONFIG_DIR"  "root:${SERVICE_USER}"            "0750"
            ensure_dir "${CONFIG_DIR}/tls" "root:${SERVICE_USER}"       "0750"
            ensure_dir "$DEFAULT_CAS_PUB" "${SERVICE_USER}:${SERVICE_USER}" "0750"
            ;;
    esac
    case "$MODE" in
        receiver|all)
            ensure_dir "$CONFIG_DIR"  "root:${SERVICE_USER}"            "0750"
            ensure_dir "${CONFIG_DIR}/tls" "root:${SERVICE_USER}"       "0750"
            ensure_dir "$DEFAULT_CAS_RCV" "${SERVICE_USER}:${SERVICE_USER}" "0750"
            ;;
    esac
}

# install_binaries -- copy pre-built binaries to BINARY_DIR.
install_binaries() {
    header "Binaries"
    for bin in cvmfs-prepub prepubctl; do
        local src="${BIN_DIR}/${bin}"
        local dst="${BINARY_DIR}/${bin}"
        run "Install ${bin} → ${dst}" install -m 755 "$src" "$dst"
    done
}

# install_config_template -- write a starter config if none is present.
# write_config_template_to -- render the CURRENT publisher config template to
# path $1. Shared by install (writes it when absent) and update (renders to a
# temp file purely to DIFF against the live config — update never writes it).
write_config_template_to() {
    cat > "$1" <<'CFGEOF'
# /etc/cvmfs-prepub/config.yaml  — generated by install.sh
# Edit before starting the service.  See INSTALL.md §4 for the full reference.

server:
  listen: ":8080"
  # tls_cert: /etc/cvmfs-prepub/tls/server.crt
  # tls_key:  /etc/cvmfs-prepub/tls/server.key

spool_root: /var/spool/cvmfs-prepub

# ── Publish mode ──────────────────────────────────────────────────────────────
# "gateway" (default) — use cvmfs_gateway lease + payload API
# "local"             — call cvmfs_server transaction/publish directly
#                       (no gateway required; service must be in 'cvmfs' group)
# publish_mode: local
# cvmfs_mount: /cvmfs

# ── Gateway (only used when publish_mode != local) ────────────────────────────
gateway:
  url: http://localhost:4929
  key_id: prepub-key-001
  key_secret_env: CVMFS_GATEWAY_SECRET   # set in /etc/cvmfs-prepub/env
  lease_ttl: 120s
  heartbeat_interval: 40s

# ── Stratum 0 HTTP endpoint (for manifest fetch and catalog download) ──────────
# Required for the direct catalog merge (pkg/cvmfscatalog).
# Use the public HTTP URL of the Stratum 0 CAS — NOT the gateway port (4929).
stratum0_url: http://localhost:8000   # e.g. http://stratum0.example.org

# ── CAS backend ───────────────────────────────────────────────────────────────
cas:
  type: localfs
  root: /srv/cvmfs/cas
  # type: s3
  # bucket: cvmfs-cas-primary
  # region: us-east-1

pipeline:
  # These are now READ by the service. Earlier versions shipped this block while
  # the config parser ignored it, so an upgrade makes whatever is written here
  # take effect — keep the values conservative and matched to MemoryMax in the
  # unit file. Peak RSS scales with workers x largest-file.
  workers: 2            # unset/0 keeps the built-in default (4)
  compression: zlib
  upload_concurrency: 4

repositories:
  - name: your-repo.example.org   # replace with your CVMFS repository name
    gc:
      enabled: false
CFGEOF
}

install_config_template() {
    header "Configuration"

    # Publisher config
    if [[ "$MODE" == "publisher" || "$MODE" == "all" ]]; then
        local cfg="${CONFIG_DIR}/config.yaml"
        if [ -f "$cfg" ]; then
            skip "${cfg} — already exists (not overwritten)"
        elif $DRY_RUN; then
            dry "Write config template → ${cfg}"
        else
            write_config_template_to "$cfg"
            chown "root:${SERVICE_USER}" "$cfg"
            chmod 0640 "$cfg"
            ok "Config template written: ${cfg}"
        fi
    fi

    # Receiver config
    if [[ "$MODE" == "receiver" || "$MODE" == "all" ]]; then
        local rcfg="${CONFIG_DIR}/receiver.yaml"
        if [ -f "$rcfg" ]; then
            skip "${rcfg} — already exists (not overwritten)"
        elif $DRY_RUN; then
            dry "Write receiver config template → ${rcfg}"
        else
            cat > "$rcfg" <<'EOF'
# /etc/cvmfs-prepub/receiver.yaml  — generated by install.sh
# Edit before starting the receiver service.

server:
  listen: ":9100"
  tls_cert: /etc/cvmfs-prepub/tls/server.crt
  tls_key:  /etc/cvmfs-prepub/tls/server.key

cas:
  type: localfs
  root: /srv/cvmfs/stratum1/cas
EOF
            chown "root:${SERVICE_USER}" "$rcfg"
            chmod 0640 "$rcfg"
            ok "Receiver config template written: ${rcfg}"
        fi
    fi

    # Secrets env file skeleton
    local env_file="${CONFIG_DIR}/env"
    if [ -f "$env_file" ]; then
        skip "${env_file} — already exists (not overwritten)"
    elif $DRY_RUN; then
        dry "Write secrets env skeleton → ${env_file}"
    else
        cat > "$env_file" <<'EOF'
# /etc/cvmfs-prepub/env — sourced by systemd EnvironmentFile=
# Mode 0600; owned by root or cvmfs-prepub.
# NEVER commit this file to version control.

# Gateway secret (gateway publish mode only)
# CVMFS_GATEWAY_SECRET=

# Shared secret for the publish API. Used as a bearer token, or as the HMAC
# key for signed requests, depending on server.auth_mode (see ADR-0008 D3):
#   bearer — the token travels on every request
#   both   — either is accepted (default; use while publishers migrate)
#   hmac   — signed requests only, so the token stops travelling
# After switching to hmac, ROTATE this value once: until then it was on the wire.
# PREPUB_API_TOKEN=

# S3 credentials (S3 CAS backend only)
# AWS_ACCESS_KEY_ID=
# AWS_SECRET_ACCESS_KEY=

# Override gateway key_id at runtime (optional; value in config.yaml takes precedence)
# CVMFS_GATEWAY_KEY_ID=
EOF
        chown "root:${SERVICE_USER}" "$env_file"
        chmod 0600 "$env_file"
        ok "Secrets env skeleton written: ${env_file}"
    fi
}

# install_units -- write systemd unit files.
# write_units_to -- render the CURRENT unit templates into directory $1 as
# <name>.service. Single source of truth shared by install (writes them into
# place) and update (renders to a temp dir to diff against what is installed),
# so the two can never drift.
write_units_to() {
    local dir="$1"

    if [[ "$MODE" == "publisher" || "$MODE" == "all" ]]; then
        cat > "${dir}/${SVC_PUB}.service" <<EOF
[Unit]
Description=CVMFS Pre-Publisher Service
After=network.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_USER}
ExecStart=${BINARY_DIR}/cvmfs-prepub --config ${CONFIG_DIR}/config.yaml
Restart=on-failure
RestartSec=5s
# Keep temporaries off /tmp (small, and RAM-backed when PrivateTmp is on).
# Set BEFORE EnvironmentFile so an operator can still override TMPDIR there;
# systemd applies these in unit order and the last assignment wins. The service
# also sets it itself at startup, which covers child processes we exec.
Environment=TMPDIR=${SPOOL_DIR}/tmp
EnvironmentFile=${CONFIG_DIR}/env
# Memory containment. Peak RSS scales with --pipeline-workers x largest-file,
# because each compress worker holds a whole file plus its compressed chunks.
# Without a cap the KERNEL picks the OOM victim, and on a host shared with
# cvmfs_gateway that victim may be the gateway rather than this service.
# MemoryHigh throttles and reclaims first; MemoryMax is the hard stop.
# Tune both to the host (these suit an 8 GB node shared with the gateway).
# IMPORTANT: this cap only holds with a matching pipeline.workers in
# config.yaml — the generated template sets 2. At the built-in default of 4 the
# observed peak was 6.7 GB, i.e. above MemoryMax, which turns an occasional
# kernel OOM into a systemd SIGKILL on every large publish.
MemoryHigh=2G
MemoryMax=3G
NoNewPrivileges=true
ProtectSystem=full
PrivateTmp=true
ReadWritePaths=${SPOOL_DIR} ${DEFAULT_CAS_PUB}

[Install]
WantedBy=multi-user.target
EOF
    fi

    if [[ "$MODE" == "receiver" || "$MODE" == "all" ]]; then
        cat > "${dir}/${SVC_RCV}.service" <<EOF
[Unit]
Description=CVMFS Pre-Publisher Stratum-1 Receiver
After=network.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_USER}
ExecStart=${BINARY_DIR}/cvmfs-prepub --config ${CONFIG_DIR}/receiver.yaml --mode receiver
Restart=on-failure
RestartSec=5s
EnvironmentFile=${CONFIG_DIR}/env
NoNewPrivileges=true
ProtectSystem=full
PrivateTmp=true
ReadWritePaths=${DEFAULT_CAS_RCV}

[Install]
WantedBy=multi-user.target
EOF
    fi
}

install_units() {
    header "Systemd Units"

    if ! has_systemd; then
        skip "systemctl not available — skipping unit installation"
        return
    fi

    if $DRY_RUN; then
        [[ "$MODE" == "publisher" || "$MODE" == "all" ]] && dry "Write unit $(unit_file "$SVC_PUB")"
        [[ "$MODE" == "receiver"  || "$MODE" == "all" ]] && dry "Write unit $(unit_file "$SVC_RCV")"
        maybe_daemon_reload
        return
    fi

    local tmp; tmp="$(mktemp -d)"
    write_units_to "$tmp"
    for name in "$SVC_PUB" "$SVC_RCV"; do
        local src="${tmp}/${name}.service"
        [ -f "$src" ] || continue
        install -m 644 "$src" "$(unit_file "$name")"
        NEED_DAEMON_RELOAD=true
        ok "Unit written: $(unit_file "$name")"
    done
    rm -rf "$tmp"

    maybe_daemon_reload
}

# enable_start_services -- enable and start the installed units.
enable_start_services() {
    if $SKIP_SERVICE; then
        skip "Service enable/start — skipped (--skip-service)"
        return
    fi
    if ! has_systemd; then
        skip "systemctl not available — start services manually"
        return
    fi

    header "Service Activation"

    local units=()
    [[ "$MODE" == "publisher" || "$MODE" == "all" ]] && units+=("${SVC_PUB}.service")
    [[ "$MODE" == "receiver"  || "$MODE" == "all" ]] && units+=("${SVC_RCV}.service")

    for unit in "${units[@]}"; do
        run "Enable ${unit}" systemctl enable "$unit"
        run "Start ${unit}"  systemctl start  "$unit"
        if ! $DRY_RUN; then
            sleep 1
            if systemctl is-active --quiet "$unit" 2>/dev/null; then
                ok "${unit} is running"
            else
                err "${unit} failed to start — check: journalctl -u ${unit} -n 30"
            fi
        fi
    done
}

# install_health_check -- verify the service responds on the health endpoint.
install_health_check() {
    if $SKIP_SERVICE || $DRY_RUN; then
        skip "Health check — skipped"
        return
    fi
    header "Health Check"
    if ! command -v curl &>/dev/null; then
        skip "curl not found — skipping health check"
        return
    fi
    sleep 2   # give the service a moment to start
    local resp rc=0
    resp=$(curl -sf --max-time 5 http://localhost:8080/api/v1/health 2>/dev/null) || rc=$?
    if [[ $rc -eq 0 ]]; then
        ok "Health endpoint responded: $resp"
    else
        warn "Health endpoint not yet reachable on :8080 — the service may still be starting."
        warn "Verify manually:  curl http://localhost:8080/api/v1/health"
        ERRS=$((ERRS + 1))
    fi
}

# ═════════════════════════════════════════════════════════════════════════════
# UPDATE
# ═════════════════════════════════════════════════════════════════════════════
#
# Replace the binaries (and, if they changed, the systemd units) on a host that
# is ALREADY installed, without touching operator state:
#
#   preserved: config.yaml, env (secrets), receiver.yaml, TLS material,
#              spool, CAS, the service account, and the enabled/disabled +
#              active/inactive state of every unit
#   replaced:  cvmfs-prepub, prepubctl, and unit files whose content differs
#              (the previous unit is backed up first)
#
# The service is stopped for the binary swap and restarted only if it was
# running before — an update must never silently start a service the operator
# had deliberately stopped, nor leave a running publisher down.

# update_prereq_check -- refuse to "update" a host that was never installed,
# rather than half-installing one.
update_prereq_check() {
    header "Prerequisites"
    local missing=0

    for bin in cvmfs-prepub prepubctl; do
        local path="${BIN_DIR}/${bin}"
        if [ -f "$path" ] && [ -x "$path" ]; then
            ok "New binary found: $path"
        else
            err "Binary not found or not executable: $path"
            info "Build it first:  make build"
            missing=$((missing + 1))
        fi
    done

    if [ ! -d "$CONFIG_DIR" ]; then
        err "${CONFIG_DIR} does not exist — this host is not installed"
        info "Run a full install first:  sudo ${PROG} install --mode ${MODE}"
        missing=$((missing + 1))
    else
        ok "Existing installation found: ${CONFIG_DIR}"
    fi

    [ "$missing" -gt 0 ] && die "Prerequisites not met — nothing was changed."
    return 0
}

# binary_version -- best-effort version string for before/after reporting.
binary_version() {
    local path="$1"
    [ -x "$path" ] || { echo "(absent)"; return; }
    "$path" --version 2>/dev/null | head -1 && return
    echo "(unknown)"
}

# update_units -- refresh unit files only when their content actually changed,
# backing up the existing one first. Operators do edit units (ExecStart flags,
# resource limits); silently overwriting that is how an update loses a
# production tuning nobody remembers making.
update_units() {
    header "Systemd Units"
    if ! has_systemd; then
        skip "systemctl not available — skipping unit refresh"
        return
    fi

    local tmp; tmp="$(mktemp -d)"
    # write_units_to writes the CURRENT templates into $1 (same content
    # install_units would install).
    write_units_to "$tmp"

    local names=()
    [[ "$MODE" == "publisher" || "$MODE" == "all" ]] && names+=("$SVC_PUB")
    [[ "$MODE" == "receiver"  || "$MODE" == "all" ]] && names+=("$SVC_RCV")

    for name in "${names[@]}"; do
        local new="${tmp}/${name}.service"
        local cur; cur="$(unit_file "$name")"
        [ -f "$new" ] || continue

        if [ ! -f "$cur" ]; then
            run "Install missing unit ${cur}" install -m 644 "$new" "$cur"
            NEED_DAEMON_RELOAD=true
        elif cmp -s "$new" "$cur"; then
            skip "${cur} — unchanged"
        else
            local bak="${cur}.bak-$(date +%Y%m%d%H%M%S)"
            warn "${cur} differs from the shipped template (local edits?)"
            run "Back up existing unit → ${bak}" cp -p "$cur" "$bak"
            run "Update unit ${cur}"             install -m 644 "$new" "$cur"
            NEED_DAEMON_RELOAD=true
        fi
    done

    rm -rf "$tmp"
}

# update_config_report -- never rewrite config; just point out keys the shipped
# template has that the live config lacks, so a new release's settings are not
# silently missed.
update_config_report() {
    header "Configuration (preserved)"

    local cfg="${CONFIG_DIR}/config.yaml"
    local envf="${CONFIG_DIR}/env"
    for f in "$cfg" "$envf" "${CONFIG_DIR}/receiver.yaml"; do
        [ -f "$f" ] && ok "$(basename "$f") — preserved, not modified"
    done

    [ -f "$cfg" ] || { warn "${cfg} is missing — the service will use flag defaults"; return; }

    # Compare top-level keys only: enough to flag a new config section without
    # pretending to be a YAML parser.
    local tmp; tmp="$(mktemp -d)"
    write_config_template_to "${tmp}/config.yaml" 2>/dev/null || { rm -rf "$tmp"; return; }
    local newkeys; newkeys="$(grep -oE '^[a-z_]+:' "${tmp}/config.yaml" 2>/dev/null | sort -u)"
    local curkeys; curkeys="$(grep -oE '^[a-z_]+:' "$cfg"                2>/dev/null | sort -u)"
    local added;   added="$(comm -23 <(echo "$newkeys") <(echo "$curkeys"))"
    rm -rf "$tmp"

    if [ -n "$added" ]; then
        warn "This release ships config keys your ${cfg} does not set:"
        while read -r k; do [ -n "$k" ] && warn "    ${k}"; done <<<"$added"
        info "They are optional (flag defaults apply); see INSTALL.md §4."
    else
        ok "No new top-level config keys in this release"
    fi
}

# do_update -- orchestrate the update flow.
do_update() {
    if $DRY_RUN; then
        printf "\n${BOLD}cvmfs-prepub update  [DRY RUN]  mode=%s${RESET}\n" "$MODE"
        printf "${DIM}No changes will be made.  Configuration is never modified.${RESET}\n"
    else
        printf "\n${BOLD}cvmfs-prepub update  mode=%s${RESET}\n" "$MODE"
        info "Configuration, secrets, spool and CAS are preserved."
        confirm "Update cvmfs-prepub binaries on this host?" || { printf "Aborted.\n"; exit 0; }
    fi

    update_prereq_check

    header "Current Version"
    info "installed: $(binary_version "${BINARY_DIR}/cvmfs-prepub")"
    info "new:       $(binary_version "${BIN_DIR}/cvmfs-prepub")"

    # Record what is running/enabled so the same state can be restored.
    local units=()
    [[ "$MODE" == "publisher" || "$MODE" == "all" ]] && units+=("$SVC_PUB")
    [[ "$MODE" == "receiver"  || "$MODE" == "all" ]] && units+=("$SVC_RCV")

    local was_active=()
    for name in "${units[@]}"; do
        if svc_active "$name"; then
            was_active+=("$name")
            info "${name}: running — will be restarted after the update"
        else
            info "${name}: not running — will be left stopped"
        fi
    done

    # Stop before swapping binaries: a publisher mid-job would otherwise keep
    # the old binary mapped while the new one is already on disk, and an
    # in-flight publish could span two versions.
    if [ ${#was_active[@]} -gt 0 ]; then
        header "Stopping Services"
        for name in "${was_active[@]}"; do
            run "Stop ${name}" systemctl stop "${name}.service"
        done
    fi

    install_dirs        # idempotent; restores a missing directory
    install_binaries    # the actual update
    update_units
    update_config_report
    maybe_daemon_reload

    if [ ${#was_active[@]} -gt 0 ]; then
        header "Restarting Services"
        for name in "${was_active[@]}"; do
            run "Start ${name}" systemctl start "${name}.service"
        done
        if ! $DRY_RUN; then
            sleep 1
            for name in "${was_active[@]}"; do
                if svc_active "$name"; then
                    ok "${name} is running"
                else
                    err "${name} failed to start after the update"
                    info "Inspect:  journalctl -u ${name} -n 50 --no-pager"
                fi
            done
        fi
    fi

    header "Next Steps"
    if $DRY_RUN; then
        info "Re-run without --dry-run to apply the above changes."
    else
        info "1. Verify health:  curl http://localhost:8080/api/v1/health"
        info "2. Check the log:  journalctl -u ${SVC_PUB} -n 30 --no-pager"
        info "Configuration was not modified; no secrets were touched."
    fi
}

# do_install -- orchestrate the full install flow.
do_install() {
    if $DRY_RUN; then
        printf "\n${BOLD}cvmfs-prepub install  [DRY RUN]  mode=%s${RESET}\n" "$MODE"
        printf "${DIM}No changes will be made.${RESET}\n"
    else
        printf "\n${BOLD}cvmfs-prepub install  mode=%s${RESET}\n" "$MODE"
        confirm "Install cvmfs-prepub on this host?" || { printf "Aborted.\n"; exit 0; }
    fi

    # 1. Check we have binaries to install
    install_prereq_check

    # 2. Detect and handle legacy bits-console spool daemon
    if legacy_present; then
        header "Legacy Service Detection"
        warn "Found legacy bits-console spool daemon artifacts on this host:"
        unit_exists  "$LEGACY_SVC"     && warn "  • systemd unit:  $(unit_file "$LEGACY_SVC")"
        svc_active   "$LEGACY_SVC"     && warn "  • service is RUNNING"
        [ -f "$LEGACY_DAEMON_BIN" ]    && warn "  • binary: ${LEGACY_DAEMON_BIN}"
        [ -f "$LEGACY_SUBMIT_BIN" ]    && warn "  • binary: ${LEGACY_SUBMIT_BIN}"
        [ -f "$LEGACY_CONF" ]          && warn "  • config: ${LEGACY_CONF}"
        [ -d "$LEGACY_SPOOL_DIR" ]     && warn "  • spool:  ${LEGACY_SPOOL_DIR}"
        warn ""
        warn "cvmfs-prepub replaces these components.  They should be removed"
        warn "to avoid conflicting CVMFS transactions."

        if $PURGE_LEGACY; then
            remove_legacy
        else
            warn ""
            warn "Use --purge-legacy to remove them automatically, or run:"
            warn "  sudo $PROG uninstall    (after cvmfs-prepub is confirmed working)"
            if ! $DRY_RUN && ! $YES; then
                if confirm "Remove legacy spool artifacts now and continue installing?"; then
                    remove_legacy
                else
                    warn "Skipping legacy removal — proceeding with install."
                    warn "You should remove legacy services manually before going live."
                fi
            fi
        fi
    fi

    # 3. Install cvmfs-prepub
    install_account
    install_dirs
    install_binaries
    install_config_template
    install_units
    enable_start_services
    install_health_check

    # 4. Post-install guidance
    header "Next Steps"
    if $DRY_RUN; then
        info "Re-run without --dry-run to apply the above changes."
    else
        info "1. Edit /etc/cvmfs-prepub/config.yaml — set gateway URL, key_id, stratum0_url, and repos."
        info "2. Set secrets in /etc/cvmfs-prepub/env (mode 0600): CVMFS_GATEWAY_SECRET."
        info "3. Restart the service:  systemctl restart ${SVC_PUB}"
        info "4. Verify health:        curl http://localhost:8080/api/v1/health"
        info "5. Run the smoke test from INSTALL.md §8."
        info "6. In bits-console ui-config.yaml set:"
        info "     publish_pipeline: .gitlab/cvmfs-prepub-publish.yml"
        info "     # prepub_url: https://<this-host>:8080"
    fi
}

# ═════════════════════════════════════════════════════════════════════════════
# UNINSTALL
# ═════════════════════════════════════════════════════════════════════════════

# Resolve CAS paths from installed config before config dir might be removed.
CAS_PUB="$(read_cas_root "${CONFIG_DIR}/config.yaml"   "${DEFAULT_CAS_PUB}")"
CAS_RCV="$(read_cas_root "${CONFIG_DIR}/receiver.yaml" "${DEFAULT_CAS_RCV}")"

do_publisher_uninstall() {
    header "Services (publisher)"
    stop_disable "$SVC_PUB"
    remove_unit  "$SVC_PUB"
    maybe_daemon_reload

    header "Binaries"
    remove_file "${BINARY_DIR}/cvmfs-prepub" "cvmfs-prepub"
    remove_file "${BINARY_DIR}/prepubctl"    "prepubctl"

    header "Configuration"
    remove_dir "$CONFIG_DIR" "config directory ${CONFIG_DIR}"

    header "Spool (job state + WAL journal)"
    if $KEEP_SPOOL; then
        skip "Spool ${SPOOL_DIR} — preserved (--keep-spool)"
    else
        remove_dir "$SPOOL_DIR" "spool ${SPOOL_DIR}" true
    fi

    header "Publisher CAS"
    if $KEEP_CAS; then
        skip "Publisher CAS ${CAS_PUB} — preserved (--keep-cas)"
    else
        remove_dir "$CAS_PUB" "publisher CAS ${CAS_PUB}" true
    fi
}

do_receiver_uninstall() {
    header "Services (receiver)"
    stop_disable "$SVC_RCV"
    remove_unit  "$SVC_RCV"
    maybe_daemon_reload

    # In "receiver" mode the publisher was never set up on this host.
    # In "all" mode binaries and config were already removed by do_publisher_uninstall;
    # the helpers are idempotent (they skip if already gone).
    if [[ "$MODE" == "receiver" ]]; then
        header "Binaries"
        remove_file "${BINARY_DIR}/cvmfs-prepub" "cvmfs-prepub"
        remove_file "${BINARY_DIR}/prepubctl"    "prepubctl"

        header "Configuration"
        remove_dir "$CONFIG_DIR" "config directory ${CONFIG_DIR}"
    fi

    header "Receiver CAS"
    if $KEEP_CAS; then
        skip "Receiver CAS ${CAS_RCV} — preserved (--keep-cas)"
    else
        remove_dir "$CAS_RCV" "receiver CAS ${CAS_RCV}" true
    fi
}

do_account_uninstall() {
    header "System Account"
    if $KEEP_USER; then
        skip "Account '${SERVICE_USER}' — preserved (--keep-user)"
        return
    fi
    if id "$SERVICE_USER" &>/dev/null 2>&1; then
        run "Remove system account '${SERVICE_USER}'" userdel "$SERVICE_USER"
    else
        skip "Account '${SERVICE_USER}' — not found"
    fi
}

do_uninstall() {
    # Build a removal manifest for the confirmation prompt.
    local manifest=() warn_items=()
    case "$MODE" in
        publisher|all)
            manifest+=("Binaries: ${BINARY_DIR}/{cvmfs-prepub,prepubctl}")
            manifest+=("Systemd unit: $(unit_file "${SVC_PUB}")")
            manifest+=("Config: ${CONFIG_DIR}/")
            if ! $KEEP_SPOOL; then
                warn_items+=("Spool + WAL journal: ${SPOOL_DIR}/  [ALL JOB HISTORY]")
            else
                manifest+=("Spool ${SPOOL_DIR}/ — PRESERVED (--keep-spool)")
            fi
            if ! $KEEP_CAS; then
                warn_items+=("Publisher CAS: ${CAS_PUB}/  [ALL CAS OBJECTS]")
            else
                manifest+=("Publisher CAS ${CAS_PUB}/ — PRESERVED (--keep-cas)")
            fi
            ;;&
        receiver|all)
            [[ "$MODE" == "receiver" ]] && \
                manifest+=("Binaries: ${BINARY_DIR}/{cvmfs-prepub,prepubctl}")
            manifest+=("Systemd unit: $(unit_file "${SVC_RCV}")")
            [[ "$MODE" == "receiver" ]] && \
                manifest+=("Config: ${CONFIG_DIR}/")
            if ! $KEEP_CAS; then
                warn_items+=("Receiver CAS: ${CAS_RCV}/  [ALL CACHED OBJECTS]")
            else
                manifest+=("Receiver CAS ${CAS_RCV}/ — PRESERVED (--keep-cas)")
            fi
            ;;
    esac
    if ! $KEEP_USER; then
        manifest+=("System account: ${SERVICE_USER}")
    fi

    if legacy_present; then
        warn_items+=("Legacy spool daemon artifacts (cvmfs-local-publish) — also detected")
    fi

    if $DRY_RUN; then
        printf "\n${BOLD}cvmfs-prepub uninstall  [DRY RUN]  mode=%s${RESET}\n" "$MODE"
        printf "${DIM}No changes will be made.${RESET}\n"
    else
        printf "\n${BOLD}cvmfs-prepub uninstall  mode=%s${RESET}\n" "$MODE"
        if ! $YES; then
            printf "\nThe following will be removed from this host:\n"
            for item in "${manifest[@]}"; do
                printf "  • %s\n" "$item"
            done
            if [[ ${#warn_items[@]} -gt 0 ]]; then
                printf "\n${RED}Permanent data loss (cannot be undone):${RESET}\n"
                for item in "${warn_items[@]}"; do
                    printf "  ${RED}• %s${RESET}\n" "$item"
                done
                printf "\n  Use --keep-spool / --keep-cas to preserve these.\n"
            fi
            printf "\n  Run with --dry-run to preview each command first.\n\n"
            read -r -p "Type 'yes' to continue, anything else to abort: " _confirm
            [[ "${_confirm:-}" == "yes" ]] || { printf "Aborted.\n"; exit 0; }
        fi
    fi

    case "$MODE" in
        publisher) do_publisher_uninstall; do_account_uninstall ;;
        receiver)  do_receiver_uninstall;  do_account_uninstall ;;
        all)       do_publisher_uninstall; do_receiver_uninstall; do_account_uninstall ;;
    esac

    # Also clean up any legacy spool artifacts found during uninstall
    if legacy_present; then
        warn ""
        warn "Legacy bits-console spool artifacts are still present on this host."
        if $PURGE_LEGACY || $YES || $DRY_RUN; then
            remove_legacy
        else
            warn "Run with --purge-legacy to remove them, or:"
            warn "  sudo $PROG uninstall --purge-legacy"
        fi
    fi
}

# ═════════════════════════════════════════════════════════════════════════════
# SUMMARY + DISPATCH
# ═════════════════════════════════════════════════════════════════════════════

print_summary() {
    header "Summary"
    if $DRY_RUN; then
        printf "\n  ${YELLOW}Dry-run complete — no changes were made.${RESET}\n"
        printf "  Re-run without --dry-run to apply.\n\n"
    elif [[ $ERRS -eq 0 ]]; then
        printf "\n  ${GREEN}Done.${RESET}  %d action(s) performed, %d skipped.\n\n" \
               "$DONE" "$SKIPPED"
    else
        printf "\n  ${YELLOW}Finished with %d error(s).${RESET}  %d action(s) performed, %d skipped.\n\n" \
               "$ERRS" "$DONE" "$SKIPPED"
        exit 1
    fi
}

case "$ACTION" in
    install)   do_install   ;;
    update)    do_update    ;;
    uninstall) do_uninstall ;;
esac

print_summary
