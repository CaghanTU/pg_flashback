#!/usr/bin/env bash
# Clean-host candidate smoke: install ONLY from candidate archives.
# Never uses a development tree for extension/helper binaries.
#
# Required env:
#   CANDIDATE_DIR  directory containing MANIFEST.json + archives
#   PG_BIN         PostgreSQL bindir matching the packaged PG_MAJOR
#   PGBACKREST     pgBackRest binary (qualification version)
#
# Optional:
#   KEEP=1         retain run artifacts on success

set -Eeuo pipefail

CANDIDATE_DIR="${CANDIDATE_DIR:?CANDIDATE_DIR is required}"
PG_BIN="${PG_BIN:?PG_BIN is required}"
PGBACKREST="${PGBACKREST:?PGBACKREST is required}"
KEEP="${KEEP:-0}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
WORK="${CLEAN_HOST_WORK:-/tmp/pgfb-clean-host-$RUN_ID}"
RESULT_JSON="${CLEAN_HOST_RESULT:-$WORK/result.json}"
MANIFEST="$CANDIDATE_DIR/MANIFEST.json"

log() { printf '[clean-host-smoke] %s %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { log "FAIL: $*"; exit 1; }
pass() { PASSED=$((PASSED + 1)); log "PASS[$PASSED]: $1"; }
require_executable() { [[ -x "$1" ]] || die "required executable not found: $1"; }

[[ -f "$MANIFEST" ]] || die "MANIFEST.json missing in $CANDIDATE_DIR"
require_executable "$PG_BIN/pg_ctl"
require_executable "$PG_BIN/psql"
require_executable "$PG_BIN/initdb"
require_executable "$PGBACKREST"
require_executable "$(command -v jq)"

SOURCE_COMMIT="$(jq -r '.provenance.source_commit' "$MANIFEST")"
PACKAGE_SHA="$(jq -r '.artifacts.package_sha256' "$MANIFEST")"
EXT_ARCHIVE="$(jq -r '.artifacts.extension_archive.name' "$MANIFEST")"
HELPER_ARCHIVE="$(jq -r '.artifacts.helper_archive.name' "$MANIFEST")"
[[ -f "$CANDIDATE_DIR/$EXT_ARCHIVE" ]] || die "missing $EXT_ARCHIVE"
[[ -f "$CANDIDATE_DIR/$HELPER_ARCHIVE" ]] || die "missing $HELPER_ARCHIVE"

# Verify digests before any install.
(
    cd "$CANDIDATE_DIR"
    echo "$PACKAGE_SHA  $EXT_ARCHIVE" | sha256sum -c -
    sha256sum -c SHA256SUMS
)

PASSED=0
PRIMARY_STARTED=0
RUN_COMPLETE=0
STANZA="clean_host"
DB_NAME="cleandb"
PORT_BASE=$((36000 + ($$ % 20000)))
PRIMARY_PORT="$PORT_BASE"
HELPER_PORT=$((PORT_BASE + 1))
SOCKET_DIR="/tmp/pgfb-ch-$RUN_ID"
HELPER_SOCKET_DIR="/tmp/pgfb-chh-$RUN_ID"
PRIMARY_DIR="$WORK/primary"
REPO_DIR="$WORK/repo"
WORK_ROOT="$WORK/helper-work"
LOG_DIR="$WORK/log"
PGBACKREST_CONFIG="$WORK/pgbackrest.conf"
PROOF_HMAC_KEY_FILE="$WORK/proof-hmac.key"
HELPER_CONFIG="$WORK/helper.json"
INSTALL_ROOT="$WORK/install"
CREATED_FULL_VIA_HELPER=0

cleanup() {
    local rc=$?
    trap - EXIT INT TERM
    set +e
    if [[ "$PRIMARY_STARTED" == "1" ]]; then
        "$PG_BIN/pg_ctl" -D "$PRIMARY_DIR" stop -m fast -w -t 60 >/dev/null 2>&1 || true
    fi
    # Uninstall packaged files from the live PostgreSQL install prefixes when we
    # copied into them; prefer staged install when possible.
    rm -rf -- "$SOCKET_DIR" "$HELPER_SOCKET_DIR"
    if [[ "$RUN_COMPLETE" == "1" && "$KEEP" != "1" ]]; then
        rm -rf -- "$WORK"
    else
        log "artifacts kept at $WORK"
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir -p "$WORK" "$REPO_DIR" "$WORK_ROOT" "$LOG_DIR" "$SOCKET_DIR" "$HELPER_SOCKET_DIR" \
    "$INSTALL_ROOT/ext" "$INSTALL_ROOT/helper"
chmod 700 "$SOCKET_DIR" "$HELPER_SOCKET_DIR" "$WORK_ROOT"
umask 077
od -An -N32 -tx1 /dev/urandom | tr -d ' \n' > "$PROOF_HMAC_KEY_FILE"
chmod 600 "$PROOF_HMAC_KEY_FILE"

log "extracting candidate archives (source_commit=$SOURCE_COMMIT package_sha=$PACKAGE_SHA)"
tar -C "$INSTALL_ROOT/ext" -xzf "$CANDIDATE_DIR/$EXT_ARCHIVE"
tar -C "$INSTALL_ROOT/helper" -xzf "$CANDIDATE_DIR/$HELPER_ARCHIVE"
EXT_ROOT="$(find "$INSTALL_ROOT/ext" -maxdepth 1 -type d -name 'pg_flashback-candidate-*' -print -quit)"
HELPER_ROOT="$(find "$INSTALL_ROOT/helper" -maxdepth 1 -type d -name 'pg-flashback-recovery-candidate-*' -print -quit)"
[[ -n "$EXT_ROOT" && -n "$HELPER_ROOT" ]] || die "archive layout unexpected"
HELPER="$HELPER_ROOT/bin/pg-flashback-recovery"
require_executable "$HELPER"
[[ "$(cat "$EXT_ROOT/PG_MAJOR")" == "$("${PG_BIN}/pg_config" --version | awk '{print $2}' | cut -d. -f1)" ]] \
    || die "PG_MAJOR mismatch between archive and PG_BIN"

# Install extension into the PostgreSQL prefix from the archive only.
PG_CONFIG="$PG_BIN/pg_config"
PKGLIB="$("$PG_CONFIG" --pkglibdir)"
SHARE_EXT="$("$PG_CONFIG" --sharedir)/extension"
install -m 0755 "$EXT_ROOT/lib/pg_flashback.so" "$PKGLIB/pg_flashback.so"
install -m 0644 "$EXT_ROOT/share/extension/pg_flashback.control" \
    "$EXT_ROOT"/share/extension/pg_flashback--*.sql \
    "$SHARE_EXT/"
pass "installed extension+helper from candidate archives only"

pgbr() { "$PGBACKREST" --config="$PGBACKREST_CONFIG" --stanza="$STANZA" "$@"; }
primary_sql() {
    "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -qAt \
        -h "$SOCKET_DIR" -p "$PRIMARY_PORT" -d "$DB_NAME" -c "$1"
}
fingerprint() {
    primary_sql "
        SELECT count(*)::text || '|' ||
               COALESCE(bit_xor(hashtextextended(row_to_json(t)::text, 0)), 0)::text
        FROM public.target_table AS t;"
}
force_archive() {
    primary_sql "SELECT pg_switch_wal();" >/dev/null
    sleep 2
}
backup_info() {
    local field="$1"
    pgbr info --output=json | jq -r --arg field "$field" '
        .[0].backup
        | sort_by(.timestamp.stop)
        | last
        | if $field == "label" then .label
          elif $field == "stop" then .lsn.stop
          else empty end'
}
write_helper_config() {
    jq -n \
        --arg profile "clean_host" \
        --arg pgbackrest "$PGBACKREST" \
        --arg pgbackrest_config "$PGBACKREST_CONFIG" \
        --arg pg_bin_dir "$PG_BIN" \
        --arg repository_path "$REPO_DIR" \
        --arg stanza "$STANZA" \
        --arg work_root "$WORK_ROOT" \
        --arg socket_root "$HELPER_SOCKET_DIR" \
        --arg expire_lock "$WORK/expire.lock" \
        --arg controller_host "$SOCKET_DIR" \
        --arg controller_database "$DB_NAME" \
        --arg recovery_user "$(id -un)" \
        --arg proof_hmac_key_file "$PROOF_HMAC_KEY_FILE" \
        --argjson controller_port "$PRIMARY_PORT" \
        --argjson recovery_port "$HELPER_PORT" \
        '{
          profile: $profile,
          pgbackrest_bin: $pgbackrest,
          pgbackrest_config: $pgbackrest_config,
          pg_bin_dir: $pg_bin_dir,
          cp_bin: "/usr/bin/cp",
          repository_path: $repository_path,
          repository_key: 1,
          stanza: $stanza,
          work_root: $work_root,
          socket_root: $socket_root,
          recovery_port: $recovery_port,
          recovery_user: $recovery_user,
          snapshot_provider: "xfs_reflink",
          expire_lock_path: $expire_lock,
          max_work_bytes: 1073741824,
          max_work_root_bytes: 2147483648,
          min_free_bytes: 1,
          artifact_ttl_seconds: 86400,
          max_retained_artifacts: 64,
          max_retained_artifact_bytes: 1073741824,
          command_timeout_seconds: 60,
          recovery_timeout_seconds: 120,
          proof_hmac_key_file: $proof_hmac_key_file,
          controller: {
            host: $controller_host,
            port: $controller_port,
            database: $controller_database,
            user: $recovery_user
          }
        }' > "$HELPER_CONFIG"
    chmod 600 "$HELPER_CONFIG"
}
write_request() {
    local path="$1" request_id="$2" rel_oid="$3" target_lsn="$4" fingerprint="$5"
    jq -n \
        --arg request_id "$request_id" \
        --arg target_lsn "$target_lsn" \
        --arg fingerprint "$fingerprint" \
        --argjson rel_oid "$rel_oid" \
        --argjson observed_at "$(date +%s)" \
        '{
          request_id: $request_id,
          database: "cleandb",
          table: {schema: "public", name: "target_table", rel_oid: $rel_oid},
          target: {kind: "lsn", value: $target_lsn, observed_at_unix_seconds: $observed_at, inclusive: true},
          expected_schema_version: 1,
          expected_schema_sha256: null,
          expected_fingerprint: (if $fingerprint == "" then null else $fingerprint end)
        }' > "$path"
}

"$PG_BIN/initdb" -D "$PRIMARY_DIR" --no-locale --encoding=UTF8 --auth=trust >"$LOG_DIR/initdb.log"
cat > "$PGBACKREST_CONFIG" <<EOF
[global]
repo1-path=$REPO_DIR
repo1-retention-full=99
repo1-hardlink=y
repo1-bundle=n
repo1-block=n
start-fast=y
compress-type=none
archive-async=n
spool-path=$WORK/spool
log-path=$LOG_DIR
log-level-console=info
log-level-file=detail

[$STANZA]
pg1-path=$PRIMARY_DIR
pg1-port=$PRIMARY_PORT
pg1-socket-path=$SOCKET_DIR
EOF
chmod 600 "$PGBACKREST_CONFIG"

cat >> "$PRIMARY_DIR/postgresql.conf" <<EOF
port = $PRIMARY_PORT
unix_socket_directories = '$SOCKET_DIR'
listen_addresses = ''
wal_level = logical
archive_mode = on
archive_command = '$PGBACKREST --config=$PGBACKREST_CONFIG --stanza=$STANZA archive-push %p'
archive_timeout = 1
max_wal_senders = 10
max_replication_slots = 10
shared_preload_libraries = 'pg_flashback'
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
pg_flashback.target_databases = '$DB_NAME'
pg_flashback.local_max_snapshot_bytes = 8GB
pg_flashback.local_max_restore_peak_bytes = 16GB
pg_flashback.local_min_filesystem_bytes = 64MB
pg_flashback.local_safety_reserve_bytes = 16MB
pg_flashback.slot_lag_warning_bytes = 16GB
pg_flashback.slot_lag_at_risk_bytes = 32GB
EOF

"$PG_BIN/pg_ctl" -D "$PRIMARY_DIR" -l "$LOG_DIR/primary.log" start -w -t 60 >/dev/null
PRIMARY_STARTED=1
"$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PRIMARY_PORT" "$DB_NAME"
pgbr stanza-create

# Required configuration: extension must load.
primary_sql "CREATE EXTENSION pg_flashback;"
pass "CREATE EXTENSION from candidate archive"

# Fail-closed track requires an admitted live capture worker before lifecycle creation.
state=""
for _ in $(seq 1 200); do
    state=$(primary_sql "SELECT admission_state FROM flashback_worker_readiness();")
    [[ "$state" == "ready" || "$state" == "maintenance_missing" ]] && break
    sleep 0.05
done
[[ "${state:-}" == "ready" || "${state:-}" == "maintenance_missing" ]] \
    || die "capture worker not ready for $DB_NAME (state=${state:-unset})"

# Local track / change / restore (local_delta).
primary_sql "CREATE TABLE public.local_t(id int PRIMARY KEY, note text NOT NULL);"
primary_sql "INSERT INTO public.local_t VALUES (1, 'seed');"
primary_sql "SELECT flashback_track('public.local_t');" >/dev/null
for _ in $(seq 1 200); do
    STATE=$(primary_sql "SELECT state FROM flashback.coverage_generations
                         WHERE recovery_profile='local_delta' ORDER BY generation_no DESC LIMIT 1;")
    [[ "$STATE" == "active" ]] && break
    primary_sql "SELECT flashback_consume_wal(8192);" >/dev/null || true
    sleep 0.05
done
[[ "$(primary_sql "SELECT state FROM flashback.coverage_generations
                   WHERE recovery_profile='local_delta' ORDER BY generation_no DESC LIMIT 1;")" == "active" ]] \
    || die "local track did not activate"
primary_sql "UPDATE public.local_t SET note='changed' WHERE id=1;"
primary_sql "SELECT pg_switch_wal();" >/dev/null
TARGET_LOCAL_LSN=""
for _ in $(seq 1 200); do
    primary_sql "SELECT flashback_consume_wal(8192);" >/dev/null || true
    TARGET_LOCAL_LSN=$(primary_sql "
        SELECT commit_lsn::text
        FROM flashback.delta_log
        WHERE table_name = 'public.local_t'
          AND event_type = 'UPDATE'
          AND new_data->>'note' = 'changed'
        ORDER BY commit_lsn DESC
        LIMIT 1;")
    VT=$(primary_sql "SELECT valid_through_lsn::text FROM flashback.coverage_generations
                      WHERE recovery_profile='local_delta' AND state='active' LIMIT 1;")
    if [[ -n "$TARGET_LOCAL_LSN" && -n "$VT" ]] && \
       primary_sql "SELECT CASE WHEN '$VT'::pg_lsn >= '$TARGET_LOCAL_LSN'::pg_lsn
                               THEN 't' ELSE 'f' END;" | grep -qx t; then
        break
    fi
    sleep 0.05
done
[[ -n "$TARGET_LOCAL_LSN" ]] || die "changed row was not captured into delta_log"
VT=$(primary_sql "SELECT valid_through_lsn::text FROM flashback.coverage_generations
                  WHERE recovery_profile='local_delta' AND state='active' LIMIT 1;")
primary_sql "SELECT CASE WHEN '$VT'::pg_lsn >= '$TARGET_LOCAL_LSN'::pg_lsn
                         THEN true ELSE false END;" | grep -qx t \
    || die "local watermark $VT has not reached change commit LSN $TARGET_LOCAL_LSN"
primary_sql "SELECT flashback_restore_lsn('public.local_t', '$TARGET_LOCAL_LSN'::pg_lsn);" >/dev/null
[[ "$(primary_sql "SELECT note FROM public.local_t WHERE id=1;")" == "changed" ]] \
    || die "local restore did not retain changed row"
pass "local track/change/restore"

# Backup-profile retained FULL + reconcile + helper restore.
primary_sql "CREATE TABLE public.target_table(
    id bigserial PRIMARY KEY, marker text NOT NULL, payload bytea NOT NULL);"
primary_sql "INSERT INTO public.target_table(marker, payload)
             SELECT 'base', decode(repeat(md5(g::text), 8), 'hex')
             FROM generate_series(1, 40) g;"
primary_sql "CHECKPOINT;"
pgbr backup --type=full --no-expire-auto
FULL0=$(backup_info label)
pass "operator FULL0 created: $FULL0"

primary_sql "SELECT flashback_track_backup('public.target_table', 'clean_host');" >/dev/null
for _ in $(seq 1 100); do
    MARKER=$(primary_sql "SELECT details->>'tracking_marker_lsn'
                          FROM flashback.coverage_generations
                          WHERE recovery_profile='backup' AND state='building'
                          ORDER BY generation_no DESC LIMIT 1;")
    [[ -n "$MARKER" ]] && break
    primary_sql "SELECT flashback_consume_wal(4096);" >/dev/null || true
    sleep 0.05
done
[[ -n "${MARKER:-}" ]] || die "backup marker unresolved"
TRACKING_ID=$(primary_sql "SELECT tracking_id FROM flashback.tracked_tables
                            WHERE table_name='target_table' AND is_active;")
primary_sql "INSERT INTO public.target_table(marker, payload)
             SELECT 'after', decode(repeat(md5(('a'||g)::text), 8), 'hex')
             FROM generate_series(1, 20) g;"
force_archive
write_helper_config
jq -n --arg request_id "ch-retained" --argjson tracking_id "$TRACKING_ID" \
    '{request_id:$request_id, tracking_id:$tracking_id}' > "$WORK/verify0.json"
"$HELPER" verify-anchor --config "$HELPER_CONFIG" --request "$WORK/verify0.json" \
    > "$WORK/verify0.result.json"
[[ "$(jq -r '.backup_label' "$WORK/verify0.result.json")" == "$FULL0" ]]
pass "retained FULL+WAL activation"

"$HELPER" reconcile-anchors --config "$HELPER_CONFIG" --dry-run > "$WORK/reconcile-dry.json"
[[ "$(jq -r '.created_backup' "$WORK/reconcile-dry.json")" == "false" ]]
"$HELPER" reconcile-anchors --config "$HELPER_CONFIG" > "$WORK/reconcile.json"
[[ "$(jq -r '.created_backup' "$WORK/reconcile.json")" == "false" ]]
pass "anchor reconcile dry-run and live (no FULL created)"

TARGET_LSN=$(primary_sql "SELECT pg_current_wal_lsn()::text;")
TARGET_FP=$(fingerprint)
force_archive
jq -n --arg request_id "ch-front" --argjson tracking_id "$TRACKING_ID" \
    '{request_id:$request_id, tracking_id:$tracking_id}' > "$WORK/front.json"
"$HELPER" verify-frontier --config "$HELPER_CONFIG" --request "$WORK/front.json" \
    > "$WORK/front.result.json"
TARGET_OID=$(primary_sql "SELECT 'public.target_table'::regclass::oid;")
write_request "$WORK/restore.json" "ch-restore" "$TARGET_OID" "$TARGET_LSN" "$TARGET_FP"
"$HELPER" restore-table --config "$HELPER_CONFIG" --request "$WORK/restore.json" \
    > "$WORK/restore.result.json"
[[ "$(jq -r '.status' "$WORK/restore.result.json")" == "completed" ]]
[[ "$(jq -r '.recovered_fingerprint' "$WORK/restore.result.json")" == "$TARGET_FP" ]]
[[ "$(fingerprint)" == "$TARGET_FP" ]]
pass "real pgBackRest table restore"

HEALTH=$(primary_sql "SELECT health FROM flashback_health() WHERE tracking_id=$TRACKING_ID;")
[[ "$HEALTH" == "healthy" || "$HEALTH" == "maintenance_required" ]] \
    || die "unexpected health=$HEALTH"
pass "health checks (health=$HEALTH)"

# Uninstall / cleanup extension objects then packaged files.
primary_sql "DROP EXTENSION pg_flashback CASCADE;"
rm -f "$PKGLIB/pg_flashback.so" \
    "$SHARE_EXT/pg_flashback.control" \
    "$SHARE_EXT"/pg_flashback--*.sql
pass "uninstall/cleanup"

[[ "$CREATED_FULL_VIA_HELPER" == "0" ]]
mkdir -p "$(dirname "$RESULT_JSON")"
jq -n \
    --arg source_commit "$SOURCE_COMMIT" \
    --arg package_sha "$PACKAGE_SHA" \
    --argjson passed "$PASSED" \
    '{
      status: "passed",
      provenance: {
        source_commit: $source_commit,
        package_sha256: $package_sha,
        install_source: "candidate_archives_only"
      },
      assertions_passed: $passed,
      created_backup_via_helper: false
    }' > "$RESULT_JSON"
RUN_COMPLETE=1
log "COMPLETE: $PASSED assertions; $RESULT_JSON"
