#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: Apache-2.0
#
# Tests install.sh's configuration steps without root: set_s3_conf
# (--s3-conf-from), repo_service_user and connect_gw are extracted from
# install.sh and run against a scratch CONFIG_DIR and CVMFS_ETC, with chown and
# cvmfs_server stubbed. Run: bash test/install-sh.sh
set -euo pipefail
here=$(cd "$(dirname "$0")/.." && pwd)
W=$(mktemp -d); trap 'rm -rf "$W"' EXIT
fails=0
check() { if eval "$2"; then echo "ok   $1"; else echo "FAIL $1"; fails=$((fails + 1)); fi; }

{
    echo 'set -euo pipefail'
    echo "CONFIG_DIR=$W/etc; CVMFS_ETC=$W/cvmfs; ACCESS_GROUP=$(id -gn); MODE=\${MODE:-publisher}; ACTION=update"
    echo 'DRY_RUN=${DRY_RUN:-false}; SVC_PUB=cvmfs-prepub; S3_CONF_FROM="${S3_CONF_FROM:-}"; ERRS=0'
    echo 'GW_MOUNTED=${GW_MOUNTED:-false}; SERVICE_USER=${SERVICE_USER:-cvbits}; SERVICE_GROUP=cvbits; STEP=${STEP:-set_s3_conf}'
    echo "PATH=$W/bin:\$PATH"
    echo 'ok(){ echo "OK: $*"; }; err(){ echo "ERR: $*"; ERRS=$((ERRS+1)); }; warn(){ echo "WARN: $*"; }; info(){ echo "INFO: $*"; }'
    echo 'skip(){ echo "SKIP: $*"; }; dry(){ echo "DRY: $*"; }; run(){ shift; "$@"; }; svc_active(){ return 1; }; chown(){ :; }'
    echo 'install(){ local a=("$@"); command cp "${a[-2]}" "${a[-1]}"; chmod 0640 "${a[-1]}"; }   # no root: owner skipped'
    sed -n '/^yaml_scalar() {/,/^}/p; /^read_yaml_key() {/,/^}/p' "$here/install.sh"
    sed -n '/^S3_TUNING_MARK=/p; /^S3_SOURCE_TAG=/p; /^S3_TUNING_KEYS=/p' "$here/install.sh"
    for f in set_s3_conf repo_service_user config_repo connect_gw; do sed -n "/^$f() {/,/^}/p" "$here/install.sh"; done
    echo 'case "$STEP" in user) repo_service_user ;; *) "$STEP" ;; esac; exit $(( ERRS > 0 ))'
} > "$W/run.sh"
run() { bash "$W/run.sh" > "$W/out" 2>&1 || true; }

mkdir -p "$W/etc" "$W/keys" "$W/cvmfs/keys" "$W/bin"
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
check "direct-S3 prefix = alias" "grep -qx 'CVMFS_S3_REPO_ALIAS=cvmfs/r' $D"
check "default tuning"          "grep -qx 'CVMFS_S3_MAX_NUMBER_OF_PARALLEL_CONNECTIONS=64' $D"
check "source recorded absolute" "grep -qx '# source: $W/keys/r.s3.conf' $D"
check "mode 0640"               "[ \$(stat -c %a $D) = 640 ]"
check "previous kept as .orig, 0640" "[ \$(stat -c %a $D.orig) = 640 ]"

# 2. Idempotent.
cp "$D" "$W/before"; run
check "refresh is byte-identical" "cmp -s $D $W/before"

# 3. Key rotation and tuning edits: keys refreshed, allowed tuning kept, the rest dropped.
printf 'CVMFS_S3_HOST=s3.example\nCVMFS_S3_BUCKET=b\nCVMFS_S3_ACCESS_KEY=AK2\nCVMFS_S3_SECRET_KEY=SK2\nCVMFS_S3_REPO_ALIAS=wrong\n' > "$W/keys/r.s3.conf"
sed -i 's/=64$/=32/' "$D"
printf '# note\nCVMFS_S3_TIMEOUT=60\nCVMFS_S3_SECRET_KEY=STALE\nCVMFS_UPSTREAM_STORAGE=S3,/t,evil@/tmp/x\n' >> "$D"
run
check "rotated key"             "grep -qx 'CVMFS_S3_SECRET_KEY=SK2' $D && ! grep -q 'SK1\|STALE' $D"
check "allowed tuning kept"     "grep -qx 'CVMFS_S3_MAX_NUMBER_OF_PARALLEL_CONNECTIONS=32' $D && grep -qx 'CVMFS_S3_TIMEOUT=60' $D && grep -qx '# note' $D"
check "planted upstream dropped" "! grep -q evil $D && grep -q 'dropped from the tuning block' $W/out"
check "alias unchanged"         "grep -qx 'CVMFS_UPSTREAM_STORAGE=S3,/var/spool/cvmfs/r/tmp,cvmfs/r@$D' $D"
check "source's REPO_ALIAS replaced" "[ \$(grep -c CVMFS_S3_REPO_ALIAS $D) = 1 ] && grep -qx 'CVMFS_S3_REPO_ALIAS=cvmfs/r' $D"

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


# 5. Default source: /etc/cvmfs/keys/<repo>.s3.conf converts a copied server.conf without the option.
printf 'CVMFS_S3_HOST=s3.example\nCVMFS_S3_BUCKET=b\nCVMFS_S3_ACCESS_KEY=AK3\nCVMFS_S3_SECRET_KEY=SK3\n' > "$W/cvmfs/keys/r.s3.conf"
printf 'CVMFS_USER=cvbits\nCVMFS_UPSTREAM_STORAGE=S3,/var/spool/cvmfs/r/tmp,cvmfs/r@/elsewhere\n' > "$W/etc/fresh.conf"
cfg "$W/etc/fresh.conf"; run
check "default source used" "grep -qx '# source: $W/cvmfs/keys/r.s3.conf' $W/etc/fresh.conf && grep -qx 'CVMFS_S3_SECRET_KEY=SK3' $W/etc/fresh.conf"
check "repository owner kept" "grep -qx 'CVMFS_USER=cvbits' $W/etc/fresh.conf"
printf 'CVMFS_UPSTREAM_STORAGE=S3,/t,cvmfs/r@%s\n' "$W/keys/named.s3.conf" > "$W/etc/named.conf"
printf 'CVMFS_S3_HOST=h\nCVMFS_S3_BUCKET=b\nCVMFS_S3_ACCESS_KEY=NAMED\nCVMFS_S3_SECRET_KEY=s\n' > "$W/keys/named.s3.conf"
cfg "$W/etc/named.conf"; run
check "the S3 config the copy names comes first" "grep -qx 'CVMFS_S3_ACCESS_KEY=NAMED' $W/etc/named.conf"
ln -sf "$W/etc/named.conf" "$W/etc/link.conf"; cfg "$W/etc/link.conf"; run
check "symlinked destination left alone" "[ -L $W/etc/link.conf ]"
cfg "$W/etc/none.conf"; mv "$W/cvmfs/keys/r.s3.conf" "$W/cvmfs/keys/r.s3.conf.off"; run
check "nothing to do: silent" "! grep -q 'ERR' $W/out && [ ! -e $W/etc/none.conf ]"
mv "$W/cvmfs/keys/r.s3.conf.off" "$W/cvmfs/keys/r.s3.conf"

# 6. Service user from /etc/cvmfs: the repository's server.conf wins over the copied S3 one;
#    root and accounts unknown here are never used.
me=$(id -un); other=$(getent passwd | awk -F: '$3 >= 1 && $3 < 1000 {print $1; exit}')
printf 'CVMFS_USER=%s\n' "$other" > "$W/etc/copy.conf"; cfg "$W/etc/copy.conf"
check "user from cas.server_conf" "[ \"\$(STEP=user bash $W/run.sh 2>/dev/null)\" = $other ]"
mkdir -p "$W/cvmfs/repositories.d/r"; printf 'CVMFS_USER="%s"\nCVMFS_UPSTREAM_STORAGE=gw,/srv/cvmfs/r/data/txn,http://gw:4929/api/v1\n' "$me" > "$W/cvmfs/repositories.d/r/server.conf"
check "user from repositories.d" "[ \"\$(STEP=user bash $W/run.sh 2>/dev/null)\" = $me ]"
sed -i "s/CVMFS_USER=.*/CVMFS_USER=root/" "$W/cvmfs/repositories.d/r/server.conf"
check "root refused" "[ -z \"\$(STEP=user bash $W/run.sh 2>/dev/null)\" ]"
sed -i "s/CVMFS_USER=.*/CVMFS_USER=no-such-account-x/" "$W/cvmfs/repositories.d/r/server.conf"
check "unknown account refused" "[ -z \"\$(STEP=user bash $W/run.sh 2>/dev/null)\" ]"

# 7. Gateway registration.
printf '#!/bin/sh\necho "$*" > %s\n' "$W/argv" > "$W/bin/cvmfs_server"; chmod +x "$W/bin/cvmfs_server"
gwcfg() { printf 'repo_name: r\npublish_mode: gateway\ningest_publish: %s\nstratum0_url: http://s0.example/cvmfs/\ngateway:\n  url: http://gw.example:4929\n' "$1" > "$W/etc/config.yaml"; }
gwcfg true; STEP=connect_gw run
check "registered repository left alone" "grep -q 'SKIP: r already registered' $W/out && [ ! -e $W/argv ]"
rm -r "$W/cvmfs/repositories.d/r"
STEP=connect_gw run
check "missing gateway key: warning, no call" "grep -q 'r.gw is missing' $W/out && [ ! -e $W/argv ]"
printf 'plain_text k s\n' > "$W/cvmfs/keys/r.gw"
STEP=connect_gw run
check "mountless registration" "[ \"\$(cat $W/argv)\" = 'connect-gw -P -K -u http://gw.example:4929/api/v1 -w http://s0.example/cvmfs/r -o cvbits r' ]"
rm -f "$W/argv"; GW_MOUNTED=true STEP=connect_gw run
check "--mounted drops -P" "[ \"\$(cat $W/argv)\" = 'connect-gw -K -u http://gw.example:4929/api/v1 -w http://s0.example/cvmfs/r -o cvbits r' ]"
rm -f "$W/argv"; DRY_RUN=true STEP=connect_gw run
check "dry run prints, does not call" "grep -q 'DRY: cvmfs_server connect-gw -P' $W/out && [ ! -e $W/argv ]"
gwcfg false; STEP=connect_gw run
check "no ingest path: nothing" "[ ! -s $W/out ] && [ ! -e $W/argv ]"

[ "$fails" -eq 0 ] && echo "all passed" || { echo "$fails failed"; exit 1; }
