#!/usr/bin/env bash
# Clean-host smoke for the supported local_delta candidate archive.
# Installs extension + CLI only from CANDIDATE_DIR; never cargo-builds and has
# no external backup dependency.
set -Eeuo pipefail

CANDIDATE_DIR="${CANDIDATE_DIR:?CANDIDATE_DIR is required}"
PG_BIN="${PG_BIN:?PG_BIN is required}"
KEEP="${KEEP:-0}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
WORK="${CLEAN_HOST_WORK:-/tmp/pgfb-clean-host-$RUN_ID}"
# Persistent by default: transient WORK is removed on a completed, non-KEEP
# run, so the evidence file itself must not live under WORK.
RESULT_JSON="${CLEAN_HOST_RESULT:-$ROOT/target/qualification/clean-host-smoke-$RUN_ID.json}"
MANIFEST="$CANDIDATE_DIR/MANIFEST.json"
PORT=$((36000 + ($$ % 20000)))
SOCKET="/tmp/pgfb-ch-$RUN_ID"
DATA="$WORK/data"
INSTALL_ROOT="$WORK/install"
PRIMARY_STARTED=0
PREFIX_INSTALLED=0
RUN_COMPLETE=0
PASSED=0

log() { printf '[clean-host-smoke] %s %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { log "FAIL: $*"; exit 1; }
pass() { PASSED=$((PASSED + 1)); log "PASS[$PASSED]: $1"; }
sha() { sha256sum "$1" | awk '{print $1}'; }

for exe in initdb pg_ctl psql pg_config; do
    [[ -x "$PG_BIN/$exe" ]] || die "missing $PG_BIN/$exe"
done
command -v jq >/dev/null || die "jq is required"
[[ -f "$MANIFEST" ]] || die "MANIFEST.json missing"

SOURCE_COMMIT="$(jq -r '.provenance.source_commit' "$MANIFEST")"
SOURCE_TREE="$(jq -r '.provenance.source_tree' "$MANIFEST")"
ARCH="$(jq -r '.provenance.arch' "$MANIFEST")"
PG_MAJOR="$(jq -r '.provenance.pg_major' "$MANIFEST")"
EXT_ARCHIVE="$(jq -r '.artifacts.extension_archive.name' "$MANIFEST")"
PACKAGE_SHA="$(jq -r '.artifacts.package_sha256' "$MANIFEST")"
EXT_SHA="$(jq -r '.artifacts.extension_binary_sha256' "$MANIFEST")"
CLI_SHA="$(jq -r '.artifacts.cli_binary_sha256' "$MANIFEST")"
[[ -f "$CANDIDATE_DIR/$EXT_ARCHIVE" ]] || die "extension archive missing"
(
    cd "$CANDIDATE_DIR"
    sha256sum -c SHA256SUMS >/dev/null
    echo "$PACKAGE_SHA  $EXT_ARCHIVE" | sha256sum -c - >/dev/null
) || die "candidate digest verification failed"

PKGLIB="$("$PG_BIN/pg_config" --pkglibdir)"
SHARE_EXT="$("$PG_BIN/pg_config" --sharedir)/extension"
STASH="$WORK/prefix-stash"

stash_file() {
    local path=$1 rel=$2
    mkdir -p "$STASH/$(dirname "$rel")"
    if [[ -e "$path" ]]; then
        cp -a "$path" "$STASH/$rel"
        printf 'present\n' >"$STASH/$rel.state"
    else
        printf 'absent\n' >"$STASH/$rel.state"
    fi
}

restore_prefix() {
    local state rel dest
    [[ -d "$STASH" ]] || return 0
    while IFS= read -r -d '' state; do
        rel="${state#"$STASH/"}"
        rel="${rel%.state}"
        case "$rel" in
            lib/*) dest="$PKGLIB/${rel#lib/}" ;;
            share/*) dest="$SHARE_EXT/${rel#share/}" ;;
            *) continue ;;
        esac
        if [[ "$(cat "$state")" == present ]]; then
            cp -a "$STASH/$rel" "$dest"
        else
            rm -f "$dest"
        fi
    done < <(find "$STASH" -type f -name '*.state' -print0)
}

write_result() {
    local rc=$1 status=failed
    [[ "$rc" == 0 && "$RUN_COMPLETE" == 1 ]] && status=passed
    mkdir -p "$(dirname "$RESULT_JSON")"
    jq -n \
        --arg status "$status" \
        --arg source_commit "$SOURCE_COMMIT" \
        --arg source_tree "$SOURCE_TREE" \
        --arg package_sha256 "$PACKAGE_SHA" \
        --arg extension_sha256 "$EXT_SHA" \
        --arg cli_sha256 "$CLI_SHA" \
        --arg arch "$ARCH" \
        --arg pg_major "$PG_MAJOR" \
        --argjson assertions "$PASSED" \
        '{
          qualification_kind:"clean_host_local_delta",
          status:$status,
          source_commit:$source_commit,
          source_tree:$source_tree,
          package_sha256:$package_sha256,
          extension_binary_sha256:$extension_sha256,
          cli_binary_sha256:$cli_sha256,
          arch:$arch,
          pg_major:$pg_major,
          install_source:"candidate_archives_only",
          assertions_passed:$assertions,
          external_backup_dependency:false
        }' >"$RESULT_JSON"
}

cleanup() {
    local rc=$?
    trap - EXIT INT TERM
    set +e
    if [[ "$PRIMARY_STARTED" == 1 ]]; then
        "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1 || true
    fi
    if [[ "$PREFIX_INSTALLED" == 1 ]]; then restore_prefix || rc=1; fi
    write_result "$rc"
    rm -rf "$SOCKET"
    if [[ "$RUN_COMPLETE" == 1 && "$KEEP" != 1 ]]; then
        rm -rf "$WORK"
    else
        log "artifacts kept at $WORK"
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir -p "$INSTALL_ROOT" "$SOCKET" "$WORK/log"
chmod 700 "$SOCKET"
tar -C "$INSTALL_ROOT" -xzf "$CANDIDATE_DIR/$EXT_ARCHIVE"
EXT_ROOT="$(find "$INSTALL_ROOT" -maxdepth 1 -type d -name 'pg_flashback-candidate-*' -print -quit)"
[[ -n "$EXT_ROOT" ]] || die "unexpected archive layout"
CLI="$EXT_ROOT/bin/pg_flashback"
[[ -x "$CLI" ]] || die "packaged CLI missing"
[[ "$(sha "$EXT_ROOT/lib/pg_flashback.so")" == "$EXT_SHA" ]] || die "extension hash mismatch"
[[ "$(sha "$CLI")" == "$CLI_SHA" ]] || die "CLI hash mismatch"
pass "candidate archive and binary hashes verified"

stash_file "$PKGLIB/pg_flashback.so" lib/pg_flashback.so
stash_file "$SHARE_EXT/pg_flashback.control" share/pg_flashback.control
for sql in "$EXT_ROOT"/share/extension/pg_flashback--*.sql; do
    stash_file "$SHARE_EXT/$(basename "$sql")" "share/$(basename "$sql")"
done
install -m 0755 "$EXT_ROOT/lib/pg_flashback.so" "$PKGLIB/pg_flashback.so"
install -m 0644 "$EXT_ROOT/share/extension/pg_flashback.control" \
    "$EXT_ROOT"/share/extension/pg_flashback--*.sql "$SHARE_EXT/"
PREFIX_INSTALLED=1
[[ "$(sha "$PKGLIB/pg_flashback.so")" == "$EXT_SHA" ]] || die "installed extension hash mismatch"
pass "installed extension and CLI from candidate archive only"

"$PG_BIN/initdb" -D "$DATA" --no-locale --encoding=UTF8 --auth=trust >/dev/null
cat >>"$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_flashback'
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
unix_socket_directories = '$SOCKET'
port = $PORT
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
pg_flashback.worker_interval_ms = 50
pg_flashback.target_databases = 'postgres'
pg_flashback.local_max_snapshot_bytes = 8GB
pg_flashback.local_max_restore_peak_bytes = 16GB
pg_flashback.local_min_filesystem_bytes = 64MB
pg_flashback.local_safety_reserve_bytes = 16MB
pg_flashback.allow_unaudited_restore = on
EOF
"$PG_BIN/pg_ctl" -D "$DATA" -l "$WORK/log/postgresql.log" start -w >/dev/null
PRIMARY_STARTED=1

export PGHOST="$SOCKET" PGPORT="$PORT" PGDATABASE=postgres
q() { "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -qAtc "$1"; }
fingerprint_of() {
    local rel=$1
    q "SELECT count(*)::text || '|' ||
              COALESCE(bit_xor(hashtextextended(row_to_json(t)::text, 0)), 0)::text
       FROM $rel AS t;"
}
q "CREATE EXTENSION pg_flashback;"
for _ in $(seq 1 200); do
    [[ "$(q "SELECT admission_state FROM flashback_worker_readiness();")" == ready ]] && break
    sleep 0.05
done
[[ "$(q "SELECT admission_state FROM flashback_worker_readiness();")" == ready ]] || die "workers not ready"
"$CLI" doctor >/dev/null
pass "fresh CREATE EXTENSION and doctor"

q "CREATE ROLE smoke_reader;
   CREATE TABLE public.orders(
     id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
     note text NOT NULL,
     payload jsonb NOT NULL
   );
   CREATE INDEX orders_note_idx ON public.orders(note);
   GRANT SELECT ON public.orders TO smoke_reader;"
"$CLI" protect public.orders >/dev/null
for _ in $(seq 1 300); do
    [[ "$(q "SELECT health FROM flashback_health() WHERE table_name='public.orders';")" == healthy ]] && break
    sleep 0.05
done
[[ "$(q "SELECT health FROM flashback_health() WHERE table_name='public.orders';")" == healthy ]] \
    || die "coverage not healthy"
q "INSERT INTO public.orders(note,payload)
   SELECT 'row-'||g, jsonb_build_object('v',g) FROM generate_series(1,100) g;"
for _ in $(seq 1 300); do
    [[ "$(q "SELECT count(*) FROM flashback.delta_log
             WHERE table_name='public.orders' AND event_type='INSERT';")" -ge 100 ]] && break
    sleep 0.05
done
FP="$(fingerprint_of public.orders)"
q "DROP TABLE public.orders;"
for _ in $(seq 1 300); do
    [[ "$(q "SELECT count(*) FROM flashback_disaster_points('public.orders', interval '1 hour')
             WHERE event_type='DROP' AND status='restorable';")" -ge 1 ]] && break
    sleep 0.05
done
"$CLI" recover public.orders --latest-drop --yes >/dev/null
[[ "$(fingerprint_of public.orders)" == "$FP" ]] \
    || die "recovered fingerprint mismatch"
[[ "$(q "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='public.orders'::regclass;")" == "$(id -un)" ]] \
    || die "owner mismatch"
[[ "$(q "SELECT has_table_privilege('smoke_reader','public.orders','SELECT');")" == t ]] \
    || die "ACL mismatch"
pass "protect, WAL capture, exact DROP recovery, fingerprint, owner and ACL"

RUN_COMPLETE=1
log "PASS: local-only clean-host smoke ($PASSED assertions)"
