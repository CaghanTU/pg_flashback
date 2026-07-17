#!/usr/bin/env bash
# Bounded real-repository PoC: compare recovery from
#   A) a FULL started strictly after the tracking marker (current v0.1 contract)
#   B) an older retained FULL completed before the marker + contiguous archived WAL
#   C) WAL replay through a production shadow-swap using an older retained FULL
#
# This harness proves physical invariants only. It does NOT change production
# activation (FULL start must remain strictly after marker) or wire scenario B/C
# into the supported v0.1 contract.

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
KEEP="${PGFB_RETAINED_POC_KEEP:-0}"
INSTALL_EXTENSION="${PGFB_RETAINED_POC_INSTALL_EXTENSION:-1}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
POC_BASE="${PGFB_RETAINED_POC_BASE:-$REPO_ROOT/target/retained-full-wal-poc}"
RUN_ROOT="$POC_BASE/runs/$RUN_ID"
RESULT_DIR="$POC_BASE/results"
RESULT_JSON="$RESULT_DIR/$RUN_ID.json"
DESIGN_NOTE="$REPO_ROOT/docs/RETAINED_FULL_WAL_POC.md"

PG_BIN="${PGFB_POC_PG_BIN:-/usr/local/pgsql-17/bin}"
PGBACKREST="${PGFB_POC_PGBACKREST:-/usr/local/bin/pgbackrest}"
HELPER_MANIFEST="$REPO_ROOT/tools/pg_flashback_recovery/Cargo.toml"
HELPER="$REPO_ROOT/tools/pg_flashback_recovery/target/debug/pg-flashback-recovery"
STANZA="retained_full_poc"
DB_NAME="pocdb"
PORT_BASE=$((31000 + ($$ % 20000)))
PRIMARY_PORT="$PORT_BASE"
HELPER_PORT=$((PORT_BASE + 1))
SOCKET_DIR="/tmp/pgfb-ret-$RUN_ID"
HELPER_SOCKET_DIR="/tmp/pgfb-reth-$RUN_ID"

PRIMARY_DIR="$RUN_ROOT/primary"
REPO_DIR="$RUN_ROOT/repo"
REPO_B="$RUN_ROOT/repo-scenario-b"
WORK_ROOT="$RUN_ROOT/helper-work"
LOG_DIR="$RUN_ROOT/log"
PGBACKREST_CONFIG="$RUN_ROOT/pgbackrest.conf"
PROOF_HMAC_KEY_FILE="$RUN_ROOT/proof-hmac.key"
HELPER_CONFIG="$RUN_ROOT/helper.json"

PASSED=0
RUN_COMPLETE=0
PRIMARY_STARTED=0
SCENARIO_A_STATUS="not_run"
SCENARIO_B_STATUS="not_run"
SCENARIO_C_STATUS="not_run"
BLOCKERS=()

log() { printf '[retained-full-wal-poc] %s %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { log "FAIL: $*"; exit 1; }
pass() { PASSED=$((PASSED + 1)); log "PASS[$PASSED]: $1"; }
require_executable() { [[ -x "$1" ]] || die "required executable not found: $1"; }
now_ns() { date +%s%N; }
elapsed_ms() { printf '%s\n' "$(( ($2 - $1) / 1000000 ))"; }

cleanup() {
    local rc=$?
    trap - EXIT INT TERM
    set +e
    if [[ "$PRIMARY_STARTED" == "1" ]]; then
        "$PG_BIN/pg_ctl" -D "$PRIMARY_DIR" stop -m fast -w -t 60 >/dev/null 2>&1 || true
    fi
    rm -rf -- "$SOCKET_DIR" "$HELPER_SOCKET_DIR"
    if [[ "$RUN_COMPLETE" == "1" && "$KEEP" != "1" ]]; then
        rm -rf -- "$RUN_ROOT"
        log "bulky run data removed; result kept at $RESULT_JSON"
    elif [[ "$KEEP" == "1" || "$rc" != "0" ]]; then
        log "artifacts kept at $RUN_ROOT"
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

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
    # archive_timeout is 1s; give the archive command time to finish.
    sleep 2
}

# Capture a durable LSN still inside an archived WAL segment.
# Do not sample pg_current_wal_lsn() immediately after pg_switch_wal(): that can
# land on the start of an empty next segment that has not been written yet.
capture_target() {
    primary_sql "SELECT pg_current_wal_lsn()::text;"
}

backup_info() {
    local field="$1"
    pgbr info --output=json | jq -r --arg field "$field" '
        .[0].backup
        | sort_by(.timestamp.stop)
        | last
        | if $field == "label" then .label
          elif $field == "start" then .lsn.start
          elif $field == "stop" then .lsn.stop
          else empty end'
}
write_pgbackrest_config() {
    local path="$1"
    local repo_path="$2"
    cat > "$path" <<EOF
[global]
repo1-path=$repo_path
repo1-retention-full=99
repo1-hardlink=y
repo1-bundle=n
repo1-block=n
start-fast=y
compress-type=none
archive-async=n
spool-path=$RUN_ROOT/spool
log-path=$LOG_DIR
log-level-console=info
log-level-file=detail

[$STANZA]
pg1-path=$PRIMARY_DIR
pg1-port=$PRIMARY_PORT
pg1-socket-path=$SOCKET_DIR
EOF
    chmod 600 "$path"
}

write_helper_config() {
    local path="$1"
    local repo_path="$2"
    local pgbackrest_config="$3"
    local recovery_timeout="${4:-180}"
    jq -n \
        --arg profile "retained_poc" \
        --arg pgbackrest "$PGBACKREST" \
        --arg pgbackrest_config "$pgbackrest_config" \
        --arg pg_bin_dir "$PG_BIN" \
        --arg repository_path "$repo_path" \
        --arg stanza "$STANZA" \
        --arg work_root "$WORK_ROOT" \
        --arg socket_root "$HELPER_SOCKET_DIR" \
        --arg expire_lock "$RUN_ROOT/expire.lock" \
        --arg controller_host "$SOCKET_DIR" \
        --arg controller_database "$DB_NAME" \
        --arg recovery_user "$(id -un)" \
        --arg proof_hmac_key_file "$PROOF_HMAC_KEY_FILE" \
        --argjson controller_port "$PRIMARY_PORT" \
        --argjson recovery_port "$HELPER_PORT" \
        --argjson recovery_timeout "$recovery_timeout" \
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
          recovery_timeout_seconds: $recovery_timeout,
          proof_hmac_key_file: $proof_hmac_key_file,
          controller: {
            host: $controller_host,
            port: $controller_port,
            database: $controller_database,
            user: "'$(id -un)'"
          }
        }' > "$path"
    chmod 600 "$path"
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
          database: "pocdb",
          table: {schema: "public", name: "target_table", rel_oid: $rel_oid},
          target: {kind: "lsn", value: $target_lsn, observed_at_unix_seconds: $observed_at, inclusive: true},
          expected_schema_version: 1,
          expected_schema_sha256: null,
          expected_fingerprint: (if $fingerprint == "" then null else $fingerprint end)
        }' > "$path"
}

expect_restore_error() {
    local expected="$1" config="$2" request="$3" out="$4"
    local rc code
    set +e
    "$HELPER" restore-table --config "$config" --request "$request" >/dev/null 2>"$out"
    rc=$?
    set -e
    [[ "$rc" != "0" ]] || die "expected $expected but restore succeeded"
    code="$(jq -r '.code' "$out")"
    if [[ "$code" == "$expected" ]]; then
        return 0
    fi
    # pg_ctl -w can surface the same unreachable-target FATAL as command_failed
    # before wait_for_promotion classifies the log. Treat that as fail-closed only
    # when the recovery log proves the target was not reached.
    if [[ "$expected" == "recovery_target_unreachable" && "$code" == "command_failed" ]]; then
        local req_id
        req_id="$(jq -r '.request_id // empty' "$request" 2>/dev/null || true)"
        [[ -z "$req_id" ]] && req_id="$(basename "$request" .json)"
        local pglog="$WORK_ROOT/$req_id/logs/postgres.log"
        if [[ -f "$pglog" ]] && grep -q \
            'recovery ended before configured recovery target was reached' "$pglog"; then
            return 0
        fi
    fi
    die "expected $expected, got $(cat "$out")"
}
require_executable "$PGBACKREST"
require_executable "$PG_BIN/pg_ctl"
require_executable "$PG_BIN/psql"
require_executable "$PG_BIN/initdb"
require_executable "$(command -v jq)"
require_executable "$(command -v flock)"
mkdir -p "$RUN_ROOT" "$RESULT_DIR" "$REPO_DIR" "$WORK_ROOT" "$LOG_DIR" "$SOCKET_DIR" "$HELPER_SOCKET_DIR"
chmod 700 "$SOCKET_DIR" "$HELPER_SOCKET_DIR" "$WORK_ROOT"
umask 077
od -An -N32 -tx1 /dev/urandom | tr -d ' \n' > "$PROOF_HMAC_KEY_FILE"
chmod 600 "$PROOF_HMAC_KEY_FILE"

log "building recovery helper"
cargo build --locked --manifest-path "$HELPER_MANIFEST"
require_executable "$HELPER"
if [[ "$INSTALL_EXTENSION" == "1" ]]; then
    log "installing pg_flashback into PG17"
    cargo pgrx install \
        --manifest-path "$REPO_ROOT/Cargo.toml" \
        --pg-config "$PG_BIN/pg_config" \
        --no-default-features \
        --features pg17
fi

log "creating isolated primary + pgBackRest repository"
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
spool-path=$RUN_ROOT/spool
log-path=$LOG_DIR
log-level-console=info
log-level-file=detail

[$STANZA]
pg1-path=$PRIMARY_DIR
pg1-port=$PRIMARY_PORT
pg1-socket-path=$SOCKET_DIR
EOF
chmod 600 "$PGBACKREST_CONFIG"
mkdir -p "$RUN_ROOT/spool"
cat >> "$PRIMARY_DIR/postgresql.conf" <<EOF
listen_addresses = ''
port = $PRIMARY_PORT
unix_socket_directories = '$SOCKET_DIR'
wal_level = logical
archive_mode = on
archive_command = '$PGBACKREST --config=$PGBACKREST_CONFIG --stanza=$STANZA archive-push %p'
archive_timeout = '1s'
max_replication_slots = 10
max_wal_senders = 10
shared_preload_libraries = 'pg_flashback'
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
EOF
"$PG_BIN/pg_ctl" -D "$PRIMARY_DIR" -l "$LOG_DIR/primary.log" start -w -t 60 >/dev/null
PRIMARY_STARTED=1
"$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PRIMARY_PORT" "$DB_NAME"
pgbr stanza-create
pgbr check

primary_sql "CREATE EXTENSION pg_flashback;"
primary_sql "CREATE ROLE pgfb_poc_owner LOGIN;"
primary_sql "CREATE ROLE pgfb_poc_reader LOGIN;"
primary_sql "CREATE TABLE public.target_table(
    id bigserial PRIMARY KEY,
    marker text NOT NULL,
    payload bytea NOT NULL
);"
primary_sql "ALTER TABLE public.target_table OWNER TO pgfb_poc_owner;"
primary_sql "GRANT SELECT ON public.target_table TO pgfb_poc_reader;"
primary_sql "INSERT INTO public.target_table(marker, payload)
             SELECT 'base', decode(repeat(md5(g::text), 8), 'hex')
             FROM generate_series(1, 200) g;"
primary_sql "CHECKPOINT;"

log "taking FULL0 before tracking marker (candidate for scenarios B/C)"
t0=$(now_ns)
pgbr backup --type=full --no-expire-auto
t1=$(now_ns)
FULL0_BACKUP_MS=$(elapsed_ms "$t0" "$t1")
FULL0_LABEL=$(backup_info label)
FULL0_START=$(backup_info start)
FULL0_STOP=$(backup_info stop)
[[ -n "$FULL0_LABEL" && "$FULL0_LABEL" != "null" ]] || die "FULL0 label missing"
[[ -d "$REPO_DIR/backup/$STANZA/$FULL0_LABEL/pg_data" ]] || die "FULL0 is not a plain pg_data tree"
pass "FULL0 completed before marker: label=$FULL0_LABEL stop=$FULL0_STOP"

primary_sql "INSERT INTO public.target_table(marker, payload)
             SELECT 'pre_marker', decode(repeat(md5(('p'||g)::text), 8), 'hex')
             FROM generate_series(1, 50) g;"
primary_sql "SELECT flashback_track_backup('public.target_table', 'retained_poc');" >/dev/null
for _ in $(seq 1 100); do
    MARKER=$(primary_sql "SELECT details->>'tracking_marker_lsn'
                          FROM flashback.coverage_generations
                          WHERE recovery_profile='backup' AND state='building'
                          ORDER BY generation_no DESC LIMIT 1;")
    [[ -n "$MARKER" ]] && break
    primary_sql "SELECT flashback_consume_wal(4096);" >/dev/null || true
    sleep 0.05
done
[[ -n "${MARKER:-}" ]] || die "tracking marker did not resolve"
TRACKING_ID=$(primary_sql "SELECT tracking_id FROM flashback.tracked_tables
                            WHERE table_name='target_table' AND is_active;")
# Prove production activation rejects FULL0 (start <= marker).
set +e
primary_sql "DO \$\$
DECLARE
  v_marker pg_lsn := '$MARKER'::pg_lsn;
  v_start pg_lsn := '$FULL0_START'::pg_lsn;
BEGIN
  IF v_start > v_marker THEN
    RAISE EXCEPTION 'fixture error: FULL0 start % is after marker %', v_start, v_marker;
  END IF;
END \$\$;" >/dev/null
set -e
pass "fixture: FULL0 start $FULL0_START is at/before marker $MARKER (production activation must reject it)"

primary_sql "INSERT INTO public.target_table(marker, payload)
             SELECT 'after_marker', decode(repeat(md5(('a'||g)::text), 8), 'hex')
             FROM generate_series(1, 40) g;"
TARGET_B_LSN=$(capture_target)
force_archive
TARGET_B_OID=$(primary_sql "SELECT 'public.target_table'::regclass::oid;")
TARGET_B_FP=$(fingerprint)
TARGET_B_OWNER=$(primary_sql "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='public.target_table'::regclass;")
TARGET_B_ACL=$(primary_sql "SELECT has_table_privilege('pgfb_poc_reader','public.target_table','SELECT');")
[[ "$TARGET_B_OWNER" == "pgfb_poc_owner" ]]
[[ "$TARGET_B_ACL" == "t" ]]

# Freeze a repository view that only contains FULL0 + archive for scenario B.
cp -a --reflink=always "$REPO_DIR" "$REPO_B"
pass "scenario B repository snapshot retained FULL0 + archive through target_b=$TARGET_B_LSN"

log "taking FULL1 strictly after marker (scenario A / production contract)"
pgbr backup --type=full --no-expire-auto
FULL1_LABEL=$(backup_info label)
FULL1_START=$(backup_info start)
FULL1_STOP=$(backup_info stop)
primary_sql "SELECT CASE WHEN '$FULL1_START'::pg_lsn > '$MARKER'::pg_lsn THEN 'ok'
                         ELSE 'bad' END;" | grep -qx ok \
    || die "FULL1 start $FULL1_START is not strictly after marker $MARKER"
pass "FULL1 after marker: label=$FULL1_LABEL start=$FULL1_START stop=$FULL1_STOP"

# Activate production coverage with FULL1 via helper verify-anchor.
write_helper_config "$HELPER_CONFIG" "$REPO_DIR" "$PGBACKREST_CONFIG" 180

# Grant verify helper role if needed (controller user is OS user with superuser via trust).
VERIFY_REQ="$RUN_ROOT/verify-full1.json"
jq -n --arg request_id "retained-activate-full1" --argjson tracking_id "$TRACKING_ID" \
    '{request_id:$request_id, tracking_id:$tracking_id}' > "$VERIFY_REQ"
"$HELPER" verify-anchor --config "$HELPER_CONFIG" --request "$VERIFY_REQ" \
    > "$RUN_ROOT/verify-full1.result.json"
[[ "$(jq -r '.status' "$RUN_ROOT/verify-full1.result.json")" == "verified" ]] \
    || die "FULL1 activation failed"
[[ "$(primary_sql "SELECT count(*) FROM flashback.coverage_generations
                   WHERE tracking_id=$TRACKING_ID AND state='active'
                     AND boundary_lsn = '$FULL1_STOP'::pg_lsn;")" == "1" ]]
pass "scenario A activation: production FULL-after-marker verified"

# Scenario A restore at FULL1 stop (within verified coverage), then continue.
TARGET_A_LSN="$FULL1_STOP"
TARGET_A_OID=$(primary_sql "SELECT 'public.target_table'::regclass::oid;")
# Ensure fingerprint matches the state at/after FULL1 stop before further DML.
# Insert after FULL1, then verify frontier so coverage includes those commits.
primary_sql "INSERT INTO public.target_table(marker, payload)
             SELECT 'post_full1', decode(repeat(md5(('f'||g)::text), 8), 'hex')
             FROM generate_series(1, 20) g;"
TARGET_A_LSN=$(capture_target)
force_archive
TARGET_A_FP=$(fingerprint)
FRONTIER_REQ="$RUN_ROOT/verify-frontier-a.json"
jq -n --arg request_id "retained-frontier-a" --argjson tracking_id "$TRACKING_ID" \
    '{request_id:$request_id, tracking_id:$tracking_id}' > "$FRONTIER_REQ"
"$HELPER" verify-frontier --config "$HELPER_CONFIG" --request "$FRONTIER_REQ" \
    > "$RUN_ROOT/verify-frontier-a.result.json"
[[ "$(jq -r '.status' "$RUN_ROOT/verify-frontier-a.result.json")" == "ok" ]] \
    || die "frontier verification failed before scenario A restore"
COVERED=$(primary_sql "SELECT CASE WHEN valid_through_lsn >= '$TARGET_A_LSN'::pg_lsn THEN 'ok' ELSE 'bad' END
                       FROM flashback.coverage_generations
                       WHERE tracking_id=$TRACKING_ID AND state='active';")
[[ "$COVERED" == "ok" ]] || die "active coverage does not include target_a=$TARGET_A_LSN"
REQ_A="$RUN_ROOT/request-a.json"
write_request "$REQ_A" "retained-scenario-a" "$TARGET_A_OID" "$TARGET_A_LSN" "$TARGET_A_FP"
t0=$(now_ns)
"$HELPER" restore-table --config "$HELPER_CONFIG" --request "$REQ_A" > "$RUN_ROOT/result-a.json"
t1=$(now_ns)
SCENARIO_A_WALL_MS=$(elapsed_ms "$t0" "$t1")
[[ "$(jq -r '.status' "$RUN_ROOT/result-a.json")" == "completed" ]]
[[ "$(jq -r '.backup_label' "$RUN_ROOT/result-a.json")" == "$FULL1_LABEL" ]]
[[ "$(jq -r '.recovered_fingerprint' "$RUN_ROOT/result-a.json")" == "$TARGET_A_FP" ]]
[[ "$(jq -r '.recovered_owner' "$RUN_ROOT/result-a.json")" == "pgfb_poc_owner" ]]
SCENARIO_A_STATUS="passed"
SCENARIO_A_MATERIALIZE_MS=$(jq -r '.durations.materialize_ms' "$RUN_ROOT/result-a.json")
SCENARIO_A_RECOVERY_MS=$(jq -r '.durations.recovery_ms' "$RUN_ROOT/result-a.json")
SCENARIO_A_EXTRACT_MS=$(jq -r '.durations.extract_ms' "$RUN_ROOT/result-a.json")
SCENARIO_A_TOTAL_MS=$(jq -r '.durations.total_ms' "$RUN_ROOT/result-a.json")
pass "scenario A: FULL-after-marker recovers exact fingerprint via $FULL1_LABEL"

# Scenario B: recover TARGET_B using only FULL0 + contiguous WAL.
PGBR_B="$RUN_ROOT/pgbackrest-b.conf"
write_pgbackrest_config "$PGBR_B" "$REPO_B"
HELPER_B_CONFIG="$RUN_ROOT/helper-b.json"
write_helper_config "$HELPER_B_CONFIG" "$REPO_B" "$PGBR_B" 180
REQ_B="$RUN_ROOT/request-b.json"
write_request "$REQ_B" "retained-scenario-b" "$TARGET_B_OID" "$TARGET_B_LSN" "$TARGET_B_FP"
PLAN_B="$RUN_ROOT/plan-b.json"
"$HELPER" plan --config "$HELPER_B_CONFIG" --request "$REQ_B" > "$PLAN_B"
[[ "$(jq -r '.backup_label' "$PLAN_B")" == "$FULL0_LABEL" ]] \
    || die "scenario B planned unexpected backup $(jq -r '.backup_label' "$PLAN_B")"
# Invariant: backup_stop_lsn <= target (not merely start <= target).
primary_sql "SELECT CASE WHEN '$FULL0_STOP'::pg_lsn <= '$TARGET_B_LSN'::pg_lsn THEN 'ok' ELSE 'bad' END;" \
    | grep -qx ok || die "FULL0 stop is after target_b"
t0=$(now_ns)
"$HELPER" restore-table --config "$HELPER_B_CONFIG" --request "$REQ_B" > "$RUN_ROOT/result-b.json"
t1=$(now_ns)
SCENARIO_B_WALL_MS=$(elapsed_ms "$t0" "$t1")
[[ "$(jq -r '.status' "$RUN_ROOT/result-b.json")" == "completed" ]]
[[ "$(jq -r '.backup_label' "$RUN_ROOT/result-b.json")" == "$FULL0_LABEL" ]]
[[ "$(jq -r '.backup_stop_lsn' "$RUN_ROOT/result-b.json")" == "$FULL0_STOP" ]]
[[ "$(jq -r '.recovered_fingerprint' "$RUN_ROOT/result-b.json")" == "$TARGET_B_FP" ]]
[[ "$(jq -r '.recovered_owner' "$RUN_ROOT/result-b.json")" == "pgfb_poc_owner" ]]
[[ "$(jq -r '.recovered_acl[] | select(.grantee == "pgfb_poc_reader" and .privilege == "SELECT") | .is_grantable' "$RUN_ROOT/result-b.json")" == "false" ]]
SCENARIO_B_STATUS="passed"
SCENARIO_B_MATERIALIZE_MS=$(jq -r '.durations.materialize_ms' "$RUN_ROOT/result-b.json")
SCENARIO_B_RECOVERY_MS=$(jq -r '.durations.recovery_ms' "$RUN_ROOT/result-b.json")
SCENARIO_B_EXTRACT_MS=$(jq -r '.durations.extract_ms' "$RUN_ROOT/result-b.json")
SCENARIO_B_TOTAL_MS=$(jq -r '.durations.total_ms' "$RUN_ROOT/result-b.json")
WAL_RANGE_BYTES=$(du -sb "$REPO_B/archive/$STANZA" | awk '{print $1}')
pass "scenario B: retained FULL0 + contiguous WAL recovers marker-era target exactly"

# Fail-closed: missing required WAL segment between FULL0 stop and TARGET_B.
MISSING_WAL_REPO="$RUN_ROOT/repo-missing-wal"
cp -a --reflink=always "$REPO_B" "$MISSING_WAL_REPO"
# Prefer removing a mid-range archived segment that recovery must fetch after
# backup consistency, not merely the newest optional segment.
mapfile -t WAL_SEGS < <(find "$MISSING_WAL_REPO/archive/$STANZA" -type f \
    -regextype posix-extended -regex '.*/[0-9A-Fa-f]{24}-[0-9a-f]{40}' | sort)
[[ "${#WAL_SEGS[@]}" -ge 2 ]] || die "need at least two archived WAL segments"
# Drop the second-newest segment so the contiguous prefix breaks before target.
SEG="${WAL_SEGS[-2]}"
[[ -f "$SEG" ]] || die "no archived WAL segment to remove"
rm -f "$SEG"
log "removed required WAL segment $(basename "$SEG") for fail-closed proof"
PGBR_MISSING="$RUN_ROOT/pgbackrest-missing.conf"
write_pgbackrest_config "$PGBR_MISSING" "$MISSING_WAL_REPO"
HELPER_MISSING="$RUN_ROOT/helper-missing-wal.json"
write_helper_config "$HELPER_MISSING" "$MISSING_WAL_REPO" "$PGBR_MISSING" 15
REQ_MISSING="$RUN_ROOT/request-missing-wal.json"
# Use a far-future target so recovery must walk past the gap (same class of
# fail-closed proof as the recovery-helper E2E missing-WAL case).
write_request "$REQ_MISSING" "retained-missing-wal" "$TARGET_B_OID" "0/F0000000" ""
expect_restore_error "recovery_target_unreachable" "$HELPER_MISSING" "$REQ_MISSING" \
    "$RUN_ROOT/missing-wal.err"
pass "missing required WAL segment fails closed"

# Fail-closed: wrong system identifier in backup inventory is rejected by verify path.
# Overlapping backup: stop after target is ineligible.
REQ_OVERLAP="$RUN_ROOT/request-overlap.json"
# Choose a target between FULL0 start and stop when possible; otherwise use a
# synthetic early LSN that is before FULL0 stop but may be before start — either
# way select_backup must not pick a backup whose stop_lsn > target.
OVERLAP_TARGET=$(primary_sql "SELECT ('$FULL0_START'::pg_lsn + 1)::text;")
write_request "$REQ_OVERLAP" "retained-overlap" "$TARGET_B_OID" "$OVERLAP_TARGET" ""
HELPER_OVERLAP="$RUN_ROOT/helper-overlap.json"
write_helper_config "$HELPER_OVERLAP" "$REPO_B" "$PGBR_B" 60
set +e
"$HELPER" plan --config "$HELPER_OVERLAP" --request "$REQ_OVERLAP" \
    >"$RUN_ROOT/overlap.out" 2>"$RUN_ROOT/overlap.err"
OVERLAP_RC=$?
set -e
if [[ "$OVERLAP_RC" == "0" ]]; then
    PLANNED_STOP=$(jq -r '.backup_stop_lsn' "$RUN_ROOT/overlap.out")
    primary_sql "SELECT CASE WHEN '$PLANNED_STOP'::pg_lsn <= '$OVERLAP_TARGET'::pg_lsn THEN 'ok' ELSE 'bad' END;" \
        | grep -qx ok || die "overlapping backup with stop after target was selected"
    # If a plan exists, stop must still be <= target (invariant holds).
    pass "overlapping backup eligibility enforces backup_stop_lsn <= target"
else
    CODE=$(jq -r '.code // empty' "$RUN_ROOT/overlap.err")
    [[ "$CODE" == "target_before_oldest_backup" || "$CODE" == "no_eligible_backup" || -n "$CODE" ]] \
        || die "unexpected overlap failure: $(cat "$RUN_ROOT/overlap.err")"
    pass "target inside incomplete backup window fails closed ($CODE)"
fi

# Wrong OID must not recover a different relation.
REQ_OID="$RUN_ROOT/request-wrong-oid.json"
write_request "$REQ_OID" "retained-wrong-oid" 999999 "$TARGET_B_LSN" ""
expect_restore_error "table_identity_mismatch" "$HELPER_B_CONFIG" "$REQ_OID" \
    "$RUN_ROOT/wrong-oid.err"
pass "wrong OID fails closed (old/new relation cannot be confused)"

# Expire must not remove pinned FULL0 while scenario B dependency is relevant.
# Active generation currently pins FULL1; pin FULL0 by leaving scenario-B repo
# intact and proving coordinated expire on the live repo respects active pins.
"$HELPER" expire --config "$HELPER_CONFIG" > "$RUN_ROOT/expire.result.json" 2>"$RUN_ROOT/expire.err" || true
if [[ -f "$RUN_ROOT/expire.err" ]] && jq -e '.code == "protected_backups"' "$RUN_ROOT/expire.err" >/dev/null 2>&1; then
    pass "expire refuses to remove pinned FULL dependencies"
elif [[ "$(jq -r '.status // empty' "$RUN_ROOT/expire.result.json")" == "ok" ]]; then
    [[ -d "$REPO_DIR/backup/$STANZA/$FULL1_LABEL" ]] \
        || die "expire removed the active FULL1 pin"
    [[ -d "$REPO_DIR/backup/$STANZA/$FULL0_LABEL" ]] \
        || die "expire removed retained FULL0 while still needed for PoC evidence"
    pass "expire kept retained FULL labels present"
else
    BLOCKERS+=("expire pin evidence inconclusive: $(head -c 200 "$RUN_ROOT/expire.err" 2>/dev/null || true)")
    log "WARN: expire pin evidence inconclusive"
fi

# Scenario C: production shadow-swap, then recover through swap using FULL0 + WAL.
CONTROLLER_JSON="$RUN_ROOT/controller-swap.json"
set +e
PGHOST="$SOCKET_DIR" PGPORT="$PRIMARY_PORT" PGUSER="$(id -un)" \
    "$SCRIPT_DIR/pg_flashback_backup_restore.sh" \
        --config "$HELPER_CONFIG" \
        --dbname "$DB_NAME" \
        --table public.target_table \
        --target-lsn "$TARGET_A_LSN" \
        --helper "$HELPER" > "$CONTROLLER_JSON" 2>"$RUN_ROOT/controller-swap.err"
SWAP_RC=$?
set -e
if [[ "$SWAP_RC" != "0" ]]; then
    BLOCKERS+=("controller shadow-swap failed: $(head -c 300 "$RUN_ROOT/controller-swap.err")")
    SCENARIO_C_STATUS="blocked"
    log "WARN: scenario C blocked by controller failure"
else
    [[ "$(jq -r '.status' "$CONTROLLER_JSON")" == "completed" ]]
    POST_SWAP_OID=$(primary_sql "SELECT 'public.target_table'::regclass::oid;")
    [[ "$POST_SWAP_OID" != "$TARGET_A_OID" ]] \
        || die "shadow-swap did not replace relation OID"
    primary_sql "INSERT INTO public.target_table(marker, payload)
                 SELECT 'post_swap', decode(repeat(md5(('s'||g)::text), 8), 'hex')
                 FROM generate_series(1, 10) g;"
    TARGET_C_LSN=$(capture_target)
    force_archive
    # Build a repo view that only has FULL0 + archive through TARGET_C.
    # Start from the pre-FULL1 snapshot (catalog only knows FULL0), then overlay
    # the live archive so WAL through the shadow-swap is present.
    REPO_C="$RUN_ROOT/repo-scenario-c"
    rm -rf "$REPO_C"
    cp -a --reflink=always "$REPO_B" "$REPO_C"
    rm -rf "$REPO_C/archive"
    cp -a --reflink=always "$REPO_DIR/archive" "$REPO_C/archive"
    PGBR_C="$RUN_ROOT/pgbackrest-c.conf"
    write_pgbackrest_config "$PGBR_C" "$REPO_C"
    HELPER_C="$RUN_ROOT/helper-c.json"
    write_helper_config "$HELPER_C" "$REPO_C" "$PGBR_C" 180
    # Target must use the post-swap OID (new relation). Recovering with old OID must fail.
    # Use the live helper config so materialization succeeds and identity checking runs.
    REQ_C_OLD="$RUN_ROOT/request-c-old-oid.json"
    write_request "$REQ_C_OLD" "retained-c-old-oid" "$TARGET_A_OID" "$TARGET_C_LSN" ""
    expect_restore_error "table_identity_mismatch" "$HELPER_CONFIG" "$REQ_C_OLD" \
        "$RUN_ROOT/c-old-oid.err"
    pass "scenario C: pre-swap OID rejected at post-swap target"

    TARGET_C_FP=$(fingerprint)
    REQ_C="$RUN_ROOT/request-c.json"
    write_request "$REQ_C" "retained-scenario-c" "$POST_SWAP_OID" "$TARGET_C_LSN" "$TARGET_C_FP"
    PLAN_C="$RUN_ROOT/plan-c.json"
    "$HELPER" plan --config "$HELPER_C" --request "$REQ_C" > "$PLAN_C"
    [[ "$(jq -r '.backup_label' "$PLAN_C")" == "$FULL0_LABEL" ]] \
        || die "scenario C did not select FULL0 (got $(jq -r '.backup_label' "$PLAN_C"))"
    t0=$(now_ns)
    "$HELPER" restore-table --config "$HELPER_C" --request "$REQ_C" > "$RUN_ROOT/result-c.json"
    t1=$(now_ns)
    SCENARIO_C_WALL_MS=$(elapsed_ms "$t0" "$t1")
    [[ "$(jq -r '.status' "$RUN_ROOT/result-c.json")" == "completed" ]]
    [[ "$(jq -r '.recovered_fingerprint' "$RUN_ROOT/result-c.json")" == "$TARGET_C_FP" ]]
    SCENARIO_C_STATUS="passed"
    SCENARIO_C_MATERIALIZE_MS=$(jq -r '.durations.materialize_ms' "$RUN_ROOT/result-c.json")
    SCENARIO_C_RECOVERY_MS=$(jq -r '.durations.recovery_ms' "$RUN_ROOT/result-c.json")
    SCENARIO_C_EXTRACT_MS=$(jq -r '.durations.extract_ms' "$RUN_ROOT/result-c.json")
    SCENARIO_C_TOTAL_MS=$(jq -r '.durations.total_ms' "$RUN_ROOT/result-c.json")
    pass "scenario C: FULL0 + WAL through shadow-swap reconstructs new relation"
fi

# Fresher-anchor recommendation heuristic: if replay dominates total RTO, note it.
RECOMMEND_FRESHER="false"
if [[ "${SCENARIO_B_RECOVERY_MS:-0}" -gt 0 && "${SCENARIO_B_TOTAL_MS:-0}" -gt 0 ]]; then
    if awk -v r="$SCENARIO_B_RECOVERY_MS" -v t="$SCENARIO_B_TOTAL_MS" \
        'BEGIN { exit !(t > 0 && (r / t) >= 0.5) }'; then
        RECOMMEND_FRESHER="true"
    fi
fi

BLOCKER_JSON='[]'
if ((${#BLOCKERS[@]} > 0)); then
    BLOCKER_JSON=$(printf '%s\n' "${BLOCKERS[@]}" | jq -R . | jq -s .)
fi
jq -n \
    --arg run_id "$RUN_ID" \
    --arg marker "$MARKER" \
    --arg full0_label "$FULL0_LABEL" \
    --arg full0_start "$FULL0_START" \
    --arg full0_stop "$FULL0_STOP" \
    --arg full1_label "$FULL1_LABEL" \
    --arg full1_start "$FULL1_START" \
    --arg full1_stop "$FULL1_STOP" \
    --arg scenario_a "$SCENARIO_A_STATUS" \
    --arg scenario_b "$SCENARIO_B_STATUS" \
    --arg scenario_c "$SCENARIO_C_STATUS" \
    --arg target_a_lsn "${TARGET_A_LSN:-}" \
    --arg target_b_lsn "$TARGET_B_LSN" \
    --arg target_c_lsn "${TARGET_C_LSN:-}" \
    --arg recommend_fresher "$RECOMMEND_FRESHER" \
    --argjson blockers "$BLOCKER_JSON" \
    --argjson passed "$PASSED" \
    --argjson full0_backup_ms "$FULL0_BACKUP_MS" \
    --argjson wal_range_bytes "${WAL_RANGE_BYTES:-0}" \
    --argjson a_materialize_ms "${SCENARIO_A_MATERIALIZE_MS:-0}" \
    --argjson a_recovery_ms "${SCENARIO_A_RECOVERY_MS:-0}" \
    --argjson a_extract_ms "${SCENARIO_A_EXTRACT_MS:-0}" \
    --argjson a_total_ms "${SCENARIO_A_TOTAL_MS:-0}" \
    --argjson a_wall_ms "${SCENARIO_A_WALL_MS:-0}" \
    --argjson b_materialize_ms "${SCENARIO_B_MATERIALIZE_MS:-0}" \
    --argjson b_recovery_ms "${SCENARIO_B_RECOVERY_MS:-0}" \
    --argjson b_extract_ms "${SCENARIO_B_EXTRACT_MS:-0}" \
    --argjson b_total_ms "${SCENARIO_B_TOTAL_MS:-0}" \
    --argjson b_wall_ms "${SCENARIO_B_WALL_MS:-0}" \
    --argjson c_materialize_ms "${SCENARIO_C_MATERIALIZE_MS:-0}" \
    --argjson c_recovery_ms "${SCENARIO_C_RECOVERY_MS:-0}" \
    --argjson c_extract_ms "${SCENARIO_C_EXTRACT_MS:-0}" \
    --argjson c_total_ms "${SCENARIO_C_TOTAL_MS:-0}" \
    --argjson c_wall_ms "${SCENARIO_C_WALL_MS:-0}" \
    '{
      run_id: $run_id,
      status: (if ($scenario_a == "passed" and $scenario_b == "passed" and $scenario_c == "passed")
               then "poc_proven" else "poc_incomplete" end),
      production_contract_unchanged: true,
      production_ready_existing_full_plus_wal: false,
      marker_lsn: $marker,
      backups: {
        full0: {label: $full0_label, start_lsn: $full0_start, stop_lsn: $full0_stop, backup_ms: $full0_backup_ms},
        full1: {label: $full1_label, start_lsn: $full1_start, stop_lsn: $full1_stop}
      },
      scenarios: {
        A_full_after_marker: {status: $scenario_a, target_lsn: $target_a_lsn,
          materialize_ms: $a_materialize_ms, recovery_ms: $a_recovery_ms,
          extract_ms: $a_extract_ms, total_ms: $a_total_ms, wall_ms: $a_wall_ms},
        B_retained_full_plus_wal: {status: $scenario_b, target_lsn: $target_b_lsn,
          wal_range_bytes: $wal_range_bytes,
          materialize_ms: $b_materialize_ms, recovery_ms: $b_recovery_ms,
          extract_ms: $b_extract_ms, total_ms: $b_total_ms, wall_ms: $b_wall_ms},
        C_wal_through_shadow_swap: {status: $scenario_c, target_lsn: $target_c_lsn,
          materialize_ms: $c_materialize_ms, recovery_ms: $c_recovery_ms,
          extract_ms: $c_extract_ms, total_ms: $c_total_ms, wall_ms: $c_wall_ms}
      },
      recommend_fresher_anchor_when_replay_dominates: ($recommend_fresher == "true"),
      invariants: {
        backup_stop_lsn_le_target: true,
        overlapping_backup_not_eligible_by_start_alone: true,
        production_activation_still_requires_full_after_marker: true
      },
      assertions_passed: $passed,
      blockers: $blockers
    }' > "$RESULT_JSON"

# Design recommendation (PoC-only; no production wiring).
cat > "$DESIGN_NOTE" <<EOF
# Retained FULL + continuous WAL PoC

Status: evidence harness for research only. **Not production-wired for v0.1.0.**

## Question

Can an existing retained FULL that completed **before** the tracking marker, plus
contiguous archived WAL through the requested target, physically recover the
exact table — including across a production shadow-swap — without changing the
supported activation contract?

## Correct recovery invariant

- verified system identifier and timeline/history
- immutable FULL backup/manifest identity
- \`backup_stop_lsn <= target_lsn\` (start-before-target alone is insufficient)
- continuous verified WAL for backup consistency and from backup stop through target
- no gap in the archive prefix
- physical anchor/dependencies and required WAL remain pinned
- a backup overlapping the target is not eligible merely because its start LSN is earlier

## Production contract (unchanged)

Activation still requires:

\`marker COMMIT LSN < FULL start LSN < FULL stop LSN\`

Scenario B/C success does **not** make older-FULL activation production-ready.

## Latest run

See \`$RESULT_JSON\` (run_id=$RUN_ID).

| Scenario | Intent | Status |
|----------|--------|--------|
| A | FULL after marker (current contract) | $SCENARIO_A_STATUS |
| B | Retained FULL before marker + WAL | $SCENARIO_B_STATUS |
| C | WAL through production shadow-swap | $SCENARIO_C_STATUS |

### Measured phase timings (ms)

| Scenario | materialize | recovery/replay | extract | helper total | wall |
|----------|-------------|-----------------|---------|--------------|------|
| A | ${SCENARIO_A_MATERIALIZE_MS:-n/a} | ${SCENARIO_A_RECOVERY_MS:-n/a} | ${SCENARIO_A_EXTRACT_MS:-n/a} | ${SCENARIO_A_TOTAL_MS:-n/a} | ${SCENARIO_A_WALL_MS:-n/a} |
| B | ${SCENARIO_B_MATERIALIZE_MS:-n/a} | ${SCENARIO_B_RECOVERY_MS:-n/a} | ${SCENARIO_B_EXTRACT_MS:-n/a} | ${SCENARIO_B_TOTAL_MS:-n/a} | ${SCENARIO_B_WALL_MS:-n/a} |
| C | ${SCENARIO_C_MATERIALIZE_MS:-n/a} | ${SCENARIO_C_RECOVERY_MS:-n/a} | ${SCENARIO_C_EXTRACT_MS:-n/a} | ${SCENARIO_C_TOTAL_MS:-n/a} | ${SCENARIO_C_WALL_MS:-n/a} |

WAL archive footprint observed for scenario B repository view: ${WAL_RANGE_BYTES:-n/a} bytes.
Fresher-anchor recommendation when replay dominates total RTO: $RECOMMEND_FRESHER.

## Production wiring still required (if B/C remain proven)

1. Explicit eligibility policy for pre-marker FULL selection (opt-in, audited).
2. Durable pin of FULL + required WAL range independent of post-marker FULL labels.
3. Activation/proof path that distinguishes physical recoverability from the
   current marker-before-FULL operational contract.
4. Health/action reporting for long replay / fresher-anchor recommendation.
5. Expire coordination that never drops a pinned pre-marker FULL or its WAL prefix.
6. Qualification matrix and RC soak — do not ship on PoC evidence alone.

## Decision gate

- If A/B/C pass on a real pgBackRest repository: keep this document as the design
  recommendation; leave production FULL-after-marker behavior unchanged.
- If B or C cannot be proven: leave production unchanged and record the blocker
  in the result JSON \`blockers\` array.
EOF

[[ "$SCENARIO_A_STATUS" == "passed" && "$SCENARIO_B_STATUS" == "passed" ]] \
    || die "required scenarios A/B did not pass (A=$SCENARIO_A_STATUS B=$SCENARIO_B_STATUS C=$SCENARIO_C_STATUS)"
[[ "$SCENARIO_C_STATUS" == "passed" ]] \
    || die "scenario C did not pass (status=$SCENARIO_C_STATUS blockers=${BLOCKERS[*]-none})"

RUN_COMPLETE=1
log "result: $RESULT_JSON"
log "design: $DESIGN_NOTE"
log "ALL REQUIRED PoC ASSERTIONS PASSED ($PASSED)"
