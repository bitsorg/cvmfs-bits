#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: Apache-2.0
#
# Tests install.sh's set_s3_conf (--s3-conf-from) without root: the function and
# its helpers are extracted from install.sh and run against a scratch CONFIG_DIR,
# with chown stubbed. Run: bash test/install-s3-conf.sh
set -euo pipefail
here=$(cd "$(dirname "$0")/.." && pwd)
W=$(mktemp -d); trap 'rm -rf "$W"' EXIT
fails=0
check() { if eval "$2"; then echo "ok   $1"; else echo "FAIL $1"; fails=$((fails + 1)); fi; }

{
    echo 'set -euo pipefail'
    echo "CONFIG_DIR=$W/etc; ACCESS_GROUP=$(id -gn); MODE=\${MODE:-publisher}; ACTION=update"
    echo 'DRY_RUN=false; SVC_PUB=cvmfs-prepub; S3_CONF_FROM="${S3_CONF_FROM:-}"; ERRS=0'
    echo 'ok(){ echo "OK: $*"; }; err(){ echo "ERR: $*"; ERRS=$((ERRS+1)); }; warn(){ echo "WARN: $*"; }'
    echo 'dry(){ echo "DRY: $*"; }; run(){ shift; "$@"; }; svc_active(){ return 1; }; chown(){ :; }'
    echo 'install(){ local a=("$@"); command cp "${a[-2]}" "${a[-1]}"; chmod 0640 "${a[-1]}"; }   # no root: owner skipped'
    sed -n '/^yaml_scalar() {/,/^}/p; /^read_yaml_key() {/,/^}/p' "$here/install.sh"
    sed -n '/^S3_TUNING_MARK=/p; /^S3_SOURCE_TAG=/p; /^S3_TUNING_KEYS=/p' "$here/install.sh"
    sed -n '/^set_s3_conf() {/,/^}/p' "$here/install.sh"
    echo 'set_s3_conf; exit $(( ERRS > 0 ))'
} > "$W/run.sh"
run() { bash "$W/run.sh" > "$W/out" 2>&1 || true; }

mkdir -p "$W/etc" "$W/keys"
D="$W/etc/r.s3.server.conf"
cfg() { printf 'repo_name: r\ncas:\n  type: %s\n  server_conf: %s\n' "${2:-s3}" "$1" > "$W/etc/config.yaml"; }
printf 'CVMFS_S3_HOST=s3.example\nCVMFS_S3_BUCKET=b\nCVMFS_S3_ACCESS_KEY=AK1\nCVMFS_S3_SECRET_KEY=SK1' > "$W/keys/r.s3.conf"
printf '# Created by cvmfs_server.\nCVMFS_UPSTREAM_STORAGE=S3,/var/spool/cvmfs/r/tmp,cvmfs/r@/etc/cvmfs/keys/r.s3.conf\n' > "$D"
cfg "$D"

# 1. Converting a copied server.conf: alias and temp dir kept, keys copied, default tuning.
(cd "$W" && S3_CONF_FROM=keys/r.s3.conf run)
check "written"                 "grep -q '^OK: S3 config' $W/out"
check "self-referencing upstream" "grep -qx 'CVMFS_UPSTREAM_STORAGE=S3,/var/spool/cvmfs/r/tmp,cvmfs/r@$D' $D"
check "keys copied"             "grep -qx 'CVMFS_S3_SECRET_KEY=SK1' $D"
check "default tuning"          "grep -qx 'CVMFS_S3_MAX_NUMBER_OF_PARALLEL_CONNECTIONS=64' $D"
check "source recorded absolute" "grep -qx '# source: $W/keys/r.s3.conf' $D"
check "mode 0640"               "[ \$(stat -c %a $D) = 640 ]"
check "previous kept as .orig, 0640" "[ \$(stat -c %a $D.orig) = 640 ]"

# 2. Idempotent.
cp "$D" "$W/before"; run
check "refresh is byte-identical" "cmp -s $D $W/before"

# 3. Key rotation and tuning edits: keys refreshed, allowed tuning kept, the rest dropped.
printf 'CVMFS_S3_HOST=s3.example\nCVMFS_S3_BUCKET=b\nCVMFS_S3_ACCESS_KEY=AK2\nCVMFS_S3_SECRET_KEY=SK2\n' > "$W/keys/r.s3.conf"
sed -i 's/=64$/=32/' "$D"
printf '# note\nCVMFS_S3_TIMEOUT=60\nCVMFS_S3_SECRET_KEY=STALE\nCVMFS_UPSTREAM_STORAGE=S3,/t,evil@/tmp/x\n' >> "$D"
run
check "rotated key"             "grep -qx 'CVMFS_S3_SECRET_KEY=SK2' $D && ! grep -q 'SK1\|STALE' $D"
check "allowed tuning kept"     "grep -qx 'CVMFS_S3_MAX_NUMBER_OF_PARALLEL_CONNECTIONS=32' $D && grep -qx 'CVMFS_S3_TIMEOUT=60' $D && grep -qx '# note' $D"
check "planted upstream dropped" "! grep -q evil $D && grep -q 'dropped from the tuning block' $W/out"
check "alias unchanged"         "grep -qx 'CVMFS_UPSTREAM_STORAGE=S3,/var/spool/cvmfs/r/tmp,cvmfs/r@$D' $D"

# 4. Refusals.
S3_CONF_FROM="$D" run;               check "source = destination refused" "grep -q 'is cas.server_conf itself' $W/out"
cp "$D" "$W/keys/prepub-copy"; S3_CONF_FROM="$W/keys/prepub-copy" run
check "a prepub file as source refused" "grep -q 'is a prepub S3 config' $W/out"
cfg "$W/etc/../keys/r.s3.server.conf"; S3_CONF_FROM="$W/keys/r.s3.conf" run
check "'..' in cas.server_conf refused" "grep -q 'not a canonical' $W/out && [ ! -e $W/keys/r.s3.server.conf ]"
cfg "$W/keys/elsewhere.conf"; S3_CONF_FROM="$W/keys/r.s3.conf" run
check "outside CONFIG_DIR refused" "grep -q 'is outside' $W/out"
cfg "$W/etc/new.conf"; S3_CONF_FROM="$W/keys/r.s3.conf" run
check "no alias refused"        "grep -q 'no S3 alias' $W/out && [ ! -e $W/etc/new.conf ]"
cfg "$D" localfs; S3_CONF_FROM="$W/keys/r.s3.conf" run
check "cas.type localfs refused" "grep -q 'needs cas.type: s3' $W/out"
MODE=receiver S3_CONF_FROM="$W/keys/r.s3.conf" run
check "receiver: ignored with a warning" "grep -q 'only applies to the publisher' $W/out"

[ "$fails" -eq 0 ] && echo "all passed" || { echo "$fails failed"; exit 1; }
