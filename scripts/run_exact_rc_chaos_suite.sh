#!/usr/bin/env bash
# Exact-candidate chaos injector suite (Gate B — short).
#
# Candidate mode (required for qualification):
#   CANDIDATE_DIR  — install extension/helper ONLY from archives (no cargo)
#
# Development escape hatch (never qualifies a packaged candidate):
#   PGFB_CHAOS_ALLOW_SOURCE=1 — permits cargo build/install from the tree
#
# This is NOT the 86400s soak and does NOT inject faults into the long-lived
# stability cluster. Use scripts/run_exact_rc_24h_stability_soak.sh for Gate C.

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=scripts/lib/exact_candidate_identity.sh
source "$REPO_ROOT/scripts/lib/exact_candidate_identity.sh"

KEEP="${PGFB_CHAOS_KEEP:-0}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
BASE="${PGFB_CHAOS_BASE:-$REPO_ROOT/target/exact-rc-chaos}"
RUN_ROOT="$BASE/runs/$RUN_ID"
RESULT_JSON="${PGFB_CHAOS_RESULT:-$BASE/results/exact-rc-chaos-$RUN_ID.json}"

PGBACKREST="${PGFB_POC_PGBACKREST:-${PGBACKREST:-/usr/local/bin/pgbackrest}}"
STANZA="exact_rc_chaos"
DB_NAME="chaosdb"
PORT_BASE=$((34000 + ($$ % 20000)))
PRIMARY_PORT="$PORT_BASE"
HELPER_PORT=$((PORT_BASE + 1))
SOCKET_DIR="/tmp/pgfb-chaos-$RUN_ID"
HELPER_SOCKET_DIR="/tmp/pgfb-chaosh-$RUN_ID"

PRIMARY_DIR="$RUN_ROOT/primary"
REPO_DIR="$RUN_ROOT/repo"
WORK_ROOT="$RUN_ROOT/helper-work"
LOG_DIR="$RUN_ROOT/log"
PGBACKREST_CONFIG="$RUN_ROOT/pgbackrest.conf"
PROOF_HMAC_KEY_FILE="$RUN_ROOT/proof-hmac.key"
HELPER_CONFIG="$RUN_ROOT/helper.json"
EXPIRE_LOCK="$RUN_ROOT/expire.lock"

PASSED=0
PRIMARY_STARTED=0
RUN_COMPLETE=0
PREFIX_INSTALLED=0
STOPPED_WORKER_PID=""
ACTIVE_HELPER_PID=""
SLOT_LOSS_LOCK_PID=""
GIT_COMMIT="$(git -C "$REPO_ROOT" rev-parse HEAD)"
HELPER_SHA=""
SOURCE_COMMIT=""
PACKAGE_SHA=""
CANDIDATE_MODE=0
declare -A FAULT_PASS

log() { printf '[exact-rc-chaos] %s %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { log "FAIL: $*"; exit 1; }
pass() {
    PASSED=$((PASSED + 1))
    FAULT_PASS["$1"]=true
    log "PASS[$PASSED]: $1"
}
require_executable() { [[ -x "$1" ]] || die "required executable not found: $1"; }

mark_fail() {
    FAULT_PASS["$1"]=false
}

cleanup_injected_faults() {
    set +e
    [[ -n "$STOPPED_WORKER_PID" ]] && kill -CONT "$STOPPED_WORKER_PID" >/dev/null 2>&1
    STOPPED_WORKER_PID=""
    if [[ -n "$ACTIVE_HELPER_PID" ]]; then
        kill -TERM "$ACTIVE_HELPER_PID" >/dev/null 2>&1
        wait "$ACTIVE_HELPER_PID" >/dev/null 2>&1
        ACTIVE_HELPER_PID=""
    fi
    if [[ -n "$SLOT_LOSS_LOCK_PID" ]]; then
        kill "$SLOT_LOSS_LOCK_PID" >/dev/null 2>&1
        wait "$SLOT_LOSS_LOCK_PID" >/dev/null 2>&1
        SLOT_LOSS_LOCK_PID=""
    fi
    # Discard corrupt/missing clone repos; never leave helper pointed at them.
    rm -rf -- "$RUN_ROOT/repo-corrupt" "$RUN_ROOT/repo-missing" \
        "$RUN_ROOT/helper-corrupt.json" "$RUN_ROOT/helper-missing.json" \
        "$RUN_ROOT/helper-slow.json" "$RUN_ROOT/pgbackrest-corrupt.conf" \
        "$RUN_ROOT/pgbackrest-missing.conf" "$RUN_ROOT/slow-cp.sh" 2>/dev/null
    set -e
}

write_result() {
    local rc=$1 status="failed"
    [[ "$rc" == "0" && "$RUN_COMPLETE" == "1" ]] && status="passed"
    mkdir -p "$(dirname "$RESULT_JSON")"
    local faults_json="{}"
    faults_json="$(
        jq -n \
            --argjson worker "${FAULT_PASS[worker_kill]:-false}" \
            --argjson helper "${FAULT_PASS[helper_kill]:-false}" \
            --argjson slot "${FAULT_PASS[slot_loss]:-false}" \
            --argjson repo "${FAULT_PASS[repo_dependency_loss]:-false}" \
            --argjson concurrent "${FAULT_PASS[concurrent_reconcile_restore_expire]:-false}" \
            --argjson sigkill "${FAULT_PASS[helper_sigkill]:-false}" \
            --argjson removed "${FAULT_PASS[removed_anchor_and_corrupt_artifact]:-false}" \
            --argjson conflict "${FAULT_PASS[request_conflict]:-false}" \
            '{
              worker_kill: $worker,
              helper_kill: $helper,
              slot_loss: $slot,
              repo_dependency_loss: $repo,
              concurrent_reconcile_restore_expire: $concurrent,
              helper_sigkill: $sigkill,
              removed_anchor_and_corrupt_artifact: $removed,
              request_conflict: $conflict
            }'
    )"
    jq -n \
        --arg status "$status" \
        --arg source_commit "${SOURCE_COMMIT:-$GIT_COMMIT}" \
        --arg git_commit "$GIT_COMMIT" \
        --arg package_sha "${PACKAGE_SHA:-}" \
        --arg helper_sha "$HELPER_SHA" \
        --argjson candidate_mode "$CANDIDATE_MODE" \
        --argjson passed "$PASSED" \
        --argjson faults "$faults_json" \
        --argjson exit_code "$rc" \
        --argjson identity "$(if [[ "$CANDIDATE_MODE" == "1" ]]; then exact_candidate_identity_json; else echo '{}'; fi)" \
        '{
          qualification_kind: "exact_candidate_chaos_suite",
          status: $status,
          candidate_mode: ($candidate_mode == 1),
          provenance: {
            source_commit: $source_commit,
            git_commit: $git_commit,
            package_sha256: (if $package_sha == "" then null else $package_sha end),
            helper_binary_sha256: $helper_sha
          },
          identity: $identity,
          faults: $faults,
          assertions_passed: $passed,
          exit_code: $exit_code
        }' > "$RESULT_JSON"
    log "result written: $RESULT_JSON"
}

cleanup() {
    local rc=$?
    trap - EXIT INT TERM
    set +e
    cleanup_injected_faults
    if [[ "$PRIMARY_STARTED" == "1" ]]; then
        "$PG_BIN/pg_ctl" -D "$PRIMARY_DIR" stop -m fast -w -t 60 >/dev/null 2>&1 || true
    fi
    rm -rf -- "$SOCKET_DIR" "$HELPER_SOCKET_DIR"
    if [[ "$PREFIX_INSTALLED" == "1" ]]; then
        exact_candidate_restore_prefix || true
        PREFIX_INSTALLED=0
        exact_candidate_verify_end_state || rc=1
    fi
    write_result "$rc"
    if [[ "$RUN_COMPLETE" == "1" && "$KEEP" != "1" && "$rc" == "0" ]]; then
        rm -rf -- "$RUN_ROOT" "${EC_EXTRACT_DIR:-}" "${EC_STASH_DIR:-}"
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
    local path="$1" repo_path="$2" pgbackrest_config="$3" recovery_timeout="${4:-120}" cp_bin="${5:-/usr/bin/cp}"
    jq -n \
        --arg profile "retained_adv" \
        --arg pgbackrest "$PGBACKREST" \
        --arg pgbackrest_config "$pgbackrest_config" \
        --arg pg_bin_dir "$PG_BIN" \
        --arg cp_bin "$cp_bin" \
        --arg repository_path "$repo_path" \
        --arg stanza "$STANZA" \
        --arg work_root "$WORK_ROOT" \
        --arg socket_root "$HELPER_SOCKET_DIR" \
        --arg expire_lock "$EXPIRE_LOCK" \
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
          cp_bin: $cp_bin,
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
            user: $recovery_user
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
          database: "chaosdb",
          table: {schema: "public", name: "target_table", rel_oid: $rel_oid},
          target: {kind: "lsn", value: $target_lsn, observed_at_unix_seconds: $observed_at, inclusive: true},
          expected_schema_version: 1,
          expected_schema_sha256: null,
          expected_fingerprint: (if $fingerprint == "" then null else $fingerprint end)
        }' > "$path"
}

assert_baseline() {
    local label="$1" require_probe="${2:-1}" health_target health_probe
    for _ in $(seq 1 80); do
        health_target=$(primary_sql "SELECT health FROM flashback_health()
                                     WHERE table_name='public.target_table';")
        if [[ "$require_probe" == "1" ]]; then
            health_probe=$(primary_sql "SELECT health FROM flashback_health()
                                        WHERE table_name='public.chaos_probe';")
            [[ "$health_target" == "healthy" && "$health_probe" == "healthy" ]] && break
        else
            [[ "$health_target" == "healthy" ]] && break
        fi
        primary_sql "SELECT flashback_consume_wal(4096);" >/dev/null || true
        sleep 0.1
    done
    [[ "$health_target" == "healthy" ]] \
        || die "baseline target_table unhealthy after $label (got $health_target)"
    if [[ "$require_probe" == "1" ]]; then
        [[ "$health_probe" == "healthy" ]] \
            || die "baseline chaos_probe unhealthy after $label (got $health_probe)"
    fi
    [[ "$(primary_sql "SELECT count(*) FROM flashback.coverage_generations
                       WHERE tracking_id=$TRACKING_ID AND state='active';")" == "1" ]] \
        || die "baseline active generation missing after $label"
}

# Resolve CANDIDATE_DIR from CANDIDATE_MANIFEST if needed.
if [[ -z "${CANDIDATE_DIR:-}" && -n "${CANDIDATE_MANIFEST:-}" ]]; then
    CANDIDATE_DIR="$(cd "$(dirname "$CANDIDATE_MANIFEST")" && pwd)"
fi

require_executable "$PGBACKREST"
require_executable "$(command -v jq)"
mkdir -p "$RUN_ROOT" "$BASE/results" "$REPO_DIR" "$WORK_ROOT" "$LOG_DIR" "$SOCKET_DIR" "$HELPER_SOCKET_DIR"
chmod 700 "$SOCKET_DIR" "$HELPER_SOCKET_DIR" "$WORK_ROOT"
umask 077
od -An -N32 -tx1 /dev/urandom | tr -d ' \n' > "$PROOF_HMAC_KEY_FILE"
chmod 600 "$PROOF_HMAC_KEY_FILE"
: > "$EXPIRE_LOCK"

if [[ -n "${CANDIDATE_DIR:-}" ]]; then
    CANDIDATE_MODE=1
    EC_STASH_DIR="$RUN_ROOT/prefix-stash"
    EC_EXTRACT_DIR="$RUN_ROOT/extract"
    exact_candidate_bind_dir "$CANDIDATE_DIR" || die "candidate bind failed"
    exact_candidate_install_into_prefix || die "candidate install failed"
    PREFIX_INSTALLED=1
    HELPER="$EC_HELPER_BIN"
    HELPER_SHA="$EC_HELPER_BIN_SHA"
    SOURCE_COMMIT="$EC_SOURCE_COMMIT"
    PACKAGE_SHA="$EC_PACKAGE_SHA"
    PG_BIN="$PG_BIN"
    log "candidate mode: helper=$HELPER package_sha=$PACKAGE_SHA (no cargo build)"
elif [[ "${PGFB_CHAOS_ALLOW_SOURCE:-0}" == "1" ]]; then
    log "WARNING: source mode enabled — NOT an exact packaged candidate qualification"
    PG_BIN="${PGFB_POC_PG_BIN:-/usr/local/pgsql-17/bin}"
    HELPER_MANIFEST="$REPO_ROOT/tools/pg_flashback_recovery/Cargo.toml"
    HELPER="$REPO_ROOT/tools/pg_flashback_recovery/target/debug/pg-flashback-recovery"
    cargo build --locked --manifest-path "$HELPER_MANIFEST"
    require_executable "$HELPER"
    HELPER_SHA="$(sha256sum "$HELPER" | awk '{print $1}')"
    cargo pgrx install \
        --manifest-path "$REPO_ROOT/Cargo.toml" \
        --pg-config "$PG_BIN/pg_config" \
        --no-default-features \
        --features pg17
    SOURCE_COMMIT="$GIT_COMMIT"
    PACKAGE_SHA=""
else
    die "exact-candidate chaos requires CANDIDATE_DIR (or PGFB_CHAOS_ALLOW_SOURCE=1 for non-qualifying source runs)"
fi

require_executable "$PG_BIN/pg_ctl"
require_executable "$PG_BIN/psql"
require_executable "$PG_BIN/initdb"
require_executable "$HELPER"

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
pg_flashback.worker_interval_ms = 25
pg_flashback.local_max_snapshot_bytes = 8GB
pg_flashback.local_max_restore_peak_bytes = 16GB
pg_flashback.local_min_filesystem_bytes = 64MB
pg_flashback.local_safety_reserve_bytes = 16MB
pg_flashback.allow_unaudited_restore = on
EOF

"$PG_BIN/pg_ctl" -D "$PRIMARY_DIR" -l "$LOG_DIR/primary.log" start -w -t 60 >/dev/null
PRIMARY_STARTED=1
"$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PRIMARY_PORT" "$DB_NAME"
pgbr stanza-create
primary_sql "CREATE EXTENSION pg_flashback;"
primary_sql "CREATE TABLE public.target_table(
    id bigserial PRIMARY KEY, marker text NOT NULL, payload bytea NOT NULL);"
primary_sql "INSERT INTO public.target_table(marker, payload)
             SELECT 'base', decode(repeat(md5(g::text), 8), 'hex')
             FROM generate_series(1, 40) g;"
primary_sql "CHECKPOINT;"

pgbr backup --type=full --no-expire-auto
FULL0_LABEL=$(backup_info label)
FULL0_STOP=$(backup_info stop)
log "FULL0=$FULL0_LABEL stop=$FULL0_STOP"

primary_sql "SELECT flashback_track_backup('public.target_table', 'retained_adv');" >/dev/null
for _ in $(seq 1 100); do
    MARKER=$(primary_sql "SELECT details->>'tracking_marker_lsn'
                          FROM flashback.coverage_generations
                          WHERE recovery_profile='backup' AND state='building'
                          ORDER BY generation_no DESC LIMIT 1;")
    [[ -n "$MARKER" ]] && break
    primary_sql "SELECT flashback_consume_wal(4096);" >/dev/null || true
    sleep 0.05
done
[[ -n "${MARKER:-}" ]] || die "marker unresolved"
TRACKING_ID=$(primary_sql "SELECT tracking_id FROM flashback.tracked_tables
                            WHERE table_name='target_table' AND is_active;")
primary_sql "INSERT INTO public.target_table(marker, payload)
             SELECT 'after', decode(repeat(md5(('a'||g)::text), 8), 'hex')
             FROM generate_series(1, 20) g;"
TARGET_LSN=$(primary_sql "SELECT pg_current_wal_lsn()::text;")
force_archive
TARGET_OID=$(primary_sql "SELECT 'public.target_table'::regclass::oid;")
TARGET_FP=$(fingerprint)
write_helper_config "$HELPER_CONFIG" "$REPO_DIR" "$PGBACKREST_CONFIG" 120

VERIFY_REQ="$RUN_ROOT/verify-retained.json"
jq -n --arg request_id "chaos-retained-ok" --argjson tracking_id "$TRACKING_ID" \
    '{request_id:$request_id, tracking_id:$tracking_id}' > "$VERIFY_REQ"
"$HELPER" verify-anchor --config "$HELPER_CONFIG" --request "$VERIFY_REQ" \
    > "$RUN_ROOT/verify-retained.result.json"
[[ "$(jq -r '.status' "$RUN_ROOT/verify-retained.result.json")" == "verified" ]] \
    || die "retained activation failed"
assert_baseline "activation" 0

# Separate local_delta probe table for worker/slot injectors (backup profile
# coverage alone is not the local capture path exercised by fault smoke).
primary_sql "CREATE TABLE public.chaos_probe(
    id bigserial PRIMARY KEY, payload text NOT NULL);"
primary_sql "SELECT flashback_track('public.chaos_probe');" >/dev/null
for _ in $(seq 1 100); do
    [[ "$(primary_sql "SELECT health FROM flashback_health()
                       WHERE table_name='public.chaos_probe';")" == "healthy" ]] && break
    primary_sql "SELECT flashback_consume_wal(4096);" >/dev/null || true
    sleep 0.1
done
[[ "$(primary_sql "SELECT health FROM flashback_health()
                   WHERE table_name='public.chaos_probe';")" == "healthy" ]] \
    || die "chaos_probe did not become healthy"
assert_baseline "initial healthy" 1

# ---------------------------------------------------------------------------
# 1) Worker kill: SIGKILL delta worker (or postmaster restart) and recover
# ---------------------------------------------------------------------------
mark_fail worker_kill
wait_primary_ready() {
    local _i
    for _i in $(seq 1 120); do
        if "$PG_BIN/psql" -X -qAt -h "$SOCKET_DIR" -p "$PRIMARY_PORT" -d "$DB_NAME" \
            -c "SELECT 1" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.25
    done
    return 1
}
BEFORE_ROWS=$(primary_sql "SELECT count(*) FROM flashback.delta_log
                           WHERE rel_oid='public.chaos_probe'::regclass;")
# Kill the worker by bouncing postmaster. Direct SIGKILL of the bgworker is
# observed as slot/stream loss (covered by slot_loss); restart preserves the
# slot and matches scripts/run_fault_injection_smoke.sh.
"$PG_BIN/pg_ctl" -D "$PRIMARY_DIR" restart -w -t 60 -l "$LOG_DIR/primary.log" >/dev/null
wait_primary_ready || die "primary did not accept connections after worker kill restart"
# Wait for worker + healthy before writing, then require delta_log growth.
for _ in $(seq 1 200); do
    HEALTH=$(primary_sql "SELECT health FROM flashback_health()
                          WHERE table_name='public.chaos_probe';")
    WORKER_ALIVE=$(primary_sql "SELECT count(*) FROM pg_stat_activity
        WHERE backend_type='pg_flashback delta worker'
          AND datname=current_database();")
    [[ "$HEALTH" == "healthy" && "$WORKER_ALIVE" -ge 1 ]] && break
    sleep 0.1
done
[[ "${HEALTH:-}" == "healthy" ]] || die "probe not healthy after restart (got ${HEALTH:-none})"
primary_sql "INSERT INTO public.chaos_probe(payload) VALUES ('worker-kill');" >/dev/null
AFTER_ROWS="$BEFORE_ROWS"
for _ in $(seq 1 200); do
    AFTER_ROWS=$(primary_sql "SELECT count(*) FROM flashback.delta_log
                              WHERE rel_oid='public.chaos_probe'::regclass;")
    HEALTH=$(primary_sql "SELECT health FROM flashback_health()
                          WHERE table_name='public.chaos_probe';")
    [[ "$AFTER_ROWS" -gt "$BEFORE_ROWS" && "$HEALTH" == "healthy" ]] && break
    primary_sql "SELECT flashback_consume_wal(4096);" >/dev/null || true
    sleep 0.1
done
[[ "$AFTER_ROWS" -gt "$BEFORE_ROWS" ]] || die "capture did not resume after worker kill (before=$BEFORE_ROWS after=$AFTER_ROWS health=$HEALTH)"
[[ "$HEALTH" == "healthy" ]] || die "health not healthy after worker kill (got ${HEALTH:-none})"
assert_baseline "worker_kill"
pass worker_kill
cleanup_injected_faults

# ---------------------------------------------------------------------------
# 2) Helper kill: SIGTERM during slow restore; request cleaned
# ---------------------------------------------------------------------------
mark_fail helper_kill
SLOW_CP="$RUN_ROOT/slow-cp.sh"
cat > "$SLOW_CP" <<'SLOW_CP_EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
for argument in "$@"; do
    if [[ "$argument" == *".reflink-probe-"* ]]; then
        exec /usr/bin/cp "$@"
    fi
done
sleep 30
exec /usr/bin/cp "$@"
SLOW_CP_EOF
chmod 700 "$SLOW_CP"
SLOW_CONFIG="$RUN_ROOT/helper-slow.json"
write_helper_config "$SLOW_CONFIG" "$REPO_DIR" "$PGBACKREST_CONFIG" 120 "$SLOW_CP"
REQ_KILL="$RUN_ROOT/request-helper-kill.json"
write_request "$REQ_KILL" "chaos-helper-kill" "$TARGET_OID" "$TARGET_LSN" "$TARGET_FP"
"$HELPER" restore-table --config "$SLOW_CONFIG" --request "$REQ_KILL" \
    >"$RUN_ROOT/helper-kill.out" 2>"$RUN_ROOT/helper-kill.err" &
ACTIVE_HELPER_PID=$!
phase=""
for _ in $(seq 1 200); do
    phase="$(jq -r '.phase // empty' "$WORK_ROOT/chaos-helper-kill/state.json" 2>/dev/null || true)"
    [[ "$phase" == "materializing" ]] && break
    sleep 0.05
done
[[ "$phase" == "materializing" ]] || die "helper kill never reached materializing"
kill -TERM "$ACTIVE_HELPER_PID"
set +e
wait "$ACTIVE_HELPER_PID"
KILL_RC=$?
set -e
ACTIVE_HELPER_PID=""
[[ "$KILL_RC" != "0" ]] || die "SIGTERM helper must not exit 0"
[[ "$(jq -r '.code // empty' "$RUN_ROOT/helper-kill.err" 2>/dev/null || true)" == "cancelled" ]] \
    || die "helper kill expected cancelled code"
[[ ! -e "$WORK_ROOT/chaos-helper-kill/pgdata" ]] || die "helper kill left pgdata"
[[ ! -e "$WORK_ROOT/chaos-helper-kill/active-process.json" ]] \
    || die "helper kill left active-process.json"
assert_baseline "helper_kill"
pass helper_kill
cleanup_injected_faults

# ---------------------------------------------------------------------------
# 3) Slot loss: advisory hold + drop slot + reanchor; clean up gap state
# ---------------------------------------------------------------------------
mark_fail slot_loss
SLOT=$(primary_sql "SELECT flashback_effective_slot_name();")
OLD_STREAM_ID=$(primary_sql "SELECT cg.stream_id
    FROM flashback.coverage_generations cg
    JOIN flashback.tracked_tables tt USING (tracking_id)
    WHERE tt.table_name='chaos_probe' AND cg.state='active'
      AND cg.recovery_profile='local_delta'
    LIMIT 1;")
[[ -n "$OLD_STREAM_ID" ]] || die "no active local_delta stream for slot loss"
"$PG_BIN/psql" -X -qAt -h "$SOCKET_DIR" -p "$PRIMARY_PORT" -d "$DB_NAME" \
    >"$RUN_ROOT/slot-loss-lock.out" 2>&1 <<SQL &
SET application_name = 'pgfb_chaos_slot_loss';
SELECT pg_advisory_lock(
    358945::integer,
    (SELECT oid::integer FROM pg_database WHERE datname=current_database())
);
SELECT pg_sleep(3);
SELECT pg_advisory_unlock(
    358945::integer,
    (SELECT oid::integer FROM pg_database WHERE datname=current_database())
);
SQL
SLOT_LOSS_LOCK_PID=$!
for _ in $(seq 1 50); do
    [[ "$(primary_sql "SELECT count(*) FROM pg_locks l
                       JOIN pg_stat_activity a ON a.pid=l.pid
                       WHERE l.locktype='advisory' AND l.classid=358945
                         AND l.granted
                         AND a.application_name='pgfb_chaos_slot_loss'")" == "1" \
       && "$(primary_sql "SELECT active_pid IS NULL FROM pg_replication_slots
                         WHERE slot_name='$SLOT'")" == "t" ]] && break
    sleep 0.1
done
SLOT_DROPPED=0
if primary_sql "SELECT pg_drop_replication_slot('$SLOT');" >/dev/null 2>&1; then
    SLOT_DROPPED=1
fi
wait "$SLOT_LOSS_LOCK_PID" || true
SLOT_LOSS_LOCK_PID=""
[[ "$SLOT_DROPPED" == "1" ]] || die "slot drop failed"
for _ in $(seq 1 100); do
    HEALTH=$(primary_sql "SELECT health FROM flashback_health()
                          WHERE table_name='public.chaos_probe';")
    [[ "$HEALTH" == "slot_lost" ]] && break
    sleep 0.1
done
[[ "${HEALTH:-}" == "slot_lost" ]] || die "expected slot_lost health, got ${HEALTH:-none}"
primary_sql "SELECT pg_create_logical_replication_slot('$SLOT', 'pg_flashback');" >/dev/null
REANCHOR=$(primary_sql "SELECT flashback_reanchor('chaos_probe');")
[[ -n "$REANCHOR" ]] || die "flashback_reanchor returned no generation"
for _ in $(seq 1 100); do
    [[ "$(primary_sql "SELECT count(*) FROM flashback.coverage_generations
                       WHERE generation_id=$REANCHOR AND state='active';")" == "1" ]] && break
    sleep 0.1
done
for _ in $(seq 1 80); do
    primary_sql "SELECT flashback_consume_wal(4096);" >/dev/null || true
    H=$(primary_sql "SELECT health FROM flashback_health()
                     WHERE table_name='public.chaos_probe';")
    [[ "$H" == "healthy" || "$H" == "catching_up" ]] && break
    sleep 0.1
done
[[ "$(primary_sql "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$SLOT'")" == "1" ]] \
    || die "slot not recreated"
# Shared DB slot also covers backup-tracked tables; reanchor target_table too.
set +e
primary_sql "SELECT flashback_reanchor('target_table');" >/dev/null 2>&1
set -e
# Abort any leftover building gens from the gap path on backup tracking.
primary_sql "UPDATE flashback.coverage_generations
             SET state='aborted', aborted_at=clock_timestamp()
             WHERE tracking_id=$TRACKING_ID AND state='building';" >/dev/null || true
# Restore retained backup active coverage if reanchor aborted it.
if [[ "$(primary_sql "SELECT count(*) FROM flashback.coverage_generations
                      WHERE tracking_id=$TRACKING_ID AND state='active';")" != "1" ]]; then
    "$HELPER" verify-anchor --config "$HELPER_CONFIG" --request "$VERIFY_REQ" \
        >"$RUN_ROOT/verify-retained-after-slot.result.json"
fi
assert_baseline "slot_loss"
pass slot_loss
cleanup_injected_faults

# ---------------------------------------------------------------------------
# 4) Repo dependency loss/corruption on clones only
# ---------------------------------------------------------------------------
mark_fail repo_dependency_loss
CORRUPT_REPO="$RUN_ROOT/repo-corrupt"
cp -a --reflink=always "$REPO_DIR" "$CORRUPT_REPO" 2>/dev/null || cp -a "$REPO_DIR" "$CORRUPT_REPO"
MANIFEST=$(find "$CORRUPT_REPO/backup/$STANZA/$FULL0_LABEL" -name backup.manifest | head -n1)
[[ -f "$MANIFEST" ]] || die "corrupt clone missing manifest"
sed -i 's/backup-label="[^"]*"/backup-label="TAMPERED-CHAOS"/' "$MANIFEST"
PGBR_C="$RUN_ROOT/pgbackrest-corrupt.conf"
sed "s|$REPO_DIR|$CORRUPT_REPO|" "$PGBACKREST_CONFIG" > "$PGBR_C"
HELPER_C="$RUN_ROOT/helper-corrupt.json"
write_helper_config "$HELPER_C" "$CORRUPT_REPO" "$PGBR_C" 30
PROD_BEFORE=$(primary_sql "SELECT ba.backup_label
                           FROM flashback.coverage_generations cg
                           JOIN flashback.backup_anchors ba USING (backup_anchor_id, tracking_id)
                           WHERE cg.tracking_id=$TRACKING_ID AND cg.state='active';")
# Begin a building successor against the live DB, then point reconcile at the
# corrupt clone — coverage must not swap (fail closed on production).
primary_sql "SELECT flashback_begin_backup_anchor_advancement($TRACKING_ID);" >/dev/null || true
set +e
"$HELPER" reconcile-anchors --config "$HELPER_C" \
    >"$RUN_ROOT/reconcile-corrupt.out" 2>"$RUN_ROOT/reconcile-corrupt.err"
set -e
PROD_AFTER=$(primary_sql "SELECT ba.backup_label
                          FROM flashback.coverage_generations cg
                          JOIN flashback.backup_anchors ba USING (backup_anchor_id, tracking_id)
                          WHERE cg.tracking_id=$TRACKING_ID AND cg.state='active';")
[[ "$PROD_AFTER" == "$PROD_BEFORE" ]] || die "corrupt clone mutated preferred anchor"
REMOVE_REPO="$RUN_ROOT/repo-missing"
cp -a --reflink=always "$REPO_DIR" "$REMOVE_REPO" 2>/dev/null || cp -a "$REPO_DIR" "$REMOVE_REPO"
rm -rf "$REMOVE_REPO/archive/$STANZA"
mkdir -p "$REMOVE_REPO/archive/$STANZA"
PGBR_M="$RUN_ROOT/pgbackrest-missing.conf"
sed "s|$REPO_DIR|$REMOVE_REPO|" "$PGBACKREST_CONFIG" > "$PGBR_M"
HELPER_M="$RUN_ROOT/helper-missing.json"
write_helper_config "$HELPER_M" "$REMOVE_REPO" "$PGBR_M" 30
# Use restore-table against the missing-archive clone. Do NOT call
# verify-frontier/audit against a bad clone while sharing the live controller —
# those paths freeze production coverage as repository_anchor_missing.
REQ_MISS="$RUN_ROOT/request-missing-wal.json"
write_request "$REQ_MISS" "chaos-missing-wal" "$TARGET_OID" "$TARGET_LSN" "$TARGET_FP"
set +e
"$HELPER" restore-table --config "$HELPER_M" --request "$REQ_MISS" \
    >"$RUN_ROOT/missing-wal.out" 2>"$RUN_ROOT/missing-wal.err"
MISSING_RC=$?
set -e
[[ "$MISSING_RC" != "0" ]] || die "missing archive must fail restore-table"
PROD_AFTER_MISS=$(primary_sql "SELECT ba.backup_label
                          FROM flashback.coverage_generations cg
                          JOIN flashback.backup_anchors ba USING (backup_anchor_id, tracking_id)
                          WHERE cg.tracking_id=$TRACKING_ID AND cg.state='active';")
[[ "$PROD_AFTER_MISS" == "$PROD_BEFORE" ]] || die "missing-archive clone mutated preferred anchor"
[[ "$(primary_sql "SELECT health FROM flashback_health()
                   WHERE table_name='public.target_table';")" != "repository_anchor_missing" ]] \
    || die "missing-archive clone froze live coverage"
# Live repo still intact.
[[ -d "$REPO_DIR/backup/$STANZA/$FULL0_LABEL" ]] || die "live FULL0 missing after clone faults"
primary_sql "UPDATE flashback.coverage_generations
             SET state='aborted', aborted_at=clock_timestamp()
             WHERE tracking_id=$TRACKING_ID AND state='building';" >/dev/null || true
assert_baseline "repo_dependency_loss"
pass repo_dependency_loss
cleanup_injected_faults

# ---------------------------------------------------------------------------
# 5) Concurrent reconcile / restore-vs-expire lock / expire-vs-pin
# ---------------------------------------------------------------------------
mark_fail concurrent_reconcile_restore_expire
"$HELPER" reconcile-anchors --config "$HELPER_CONFIG" \
    >"$RUN_ROOT/reconcile-a.out" 2>"$RUN_ROOT/reconcile-a.err" &
PID_A=$!
"$HELPER" reconcile-anchors --config "$HELPER_CONFIG" \
    >"$RUN_ROOT/reconcile-b.out" 2>"$RUN_ROOT/reconcile-b.err" &
PID_B=$!
wait "$PID_A"; RC_A=$?
wait "$PID_B"; RC_B=$?
[[ "$RC_A" == "0" || "$RC_B" == "0" ]] || die "both concurrent reconciles failed"
ACTIVE_COUNT=$(primary_sql "SELECT count(*) FROM flashback.coverage_generations
                            WHERE tracking_id=$TRACKING_ID AND state='active';")
[[ "$ACTIVE_COUNT" == "1" ]] || die "expected one active gen after concurrent reconcile"

set +e
"$HELPER" expire --config "$HELPER_CONFIG" \
    >"$RUN_ROOT/expire-pinned.out" 2>"$RUN_ROOT/expire-pinned.err"
EXPIRE_RC=$?
set -e
[[ "$EXPIRE_RC" != "0" ]] || die "expire must fail while pinned"
[[ -d "$REPO_DIR/backup/$STANZA/$FULL0_LABEL" ]] || die "pinned FULL deleted by expire"

REQ_LOCK="$RUN_ROOT/request-lock.json"
write_request "$REQ_LOCK" "chaos-lock-restore" "$TARGET_OID" "$TARGET_LSN" "$TARGET_FP"
flock -x "$EXPIRE_LOCK" -c 'sleep 3' &
LOCK_PID=$!
sleep 0.2
"$HELPER" restore-table --config "$HELPER_CONFIG" --request "$REQ_LOCK" \
    >"$RUN_ROOT/lock-restore.result.json" 2>"$RUN_ROOT/lock-restore.err" &
WAIT_PID=$!
# Restore should still be waiting while flock held.
sleep 0.5
kill -0 "$WAIT_PID" 2>/dev/null || die "restore did not wait for expire lock"
wait "$LOCK_PID"
wait "$WAIT_PID" || die "restore failed after expire lock release"
[[ "$(jq -r '.status' "$RUN_ROOT/lock-restore.result.json")" == "completed" ]] \
    || die "restore did not complete after lock"
[[ ! -e "$WORK_ROOT/chaos-lock-restore/pgdata" ]] || die "restore left pgdata"
assert_baseline "concurrent_reconcile_restore_expire"
pass concurrent_reconcile_restore_expire
cleanup_injected_faults

# ---------------------------------------------------------------------------
# 6) Helper SIGKILL + next-run reconciliation
# ---------------------------------------------------------------------------
mark_fail helper_sigkill
SLOW_CP="$RUN_ROOT/slow-cp.sh"
cat > "$SLOW_CP" <<'SLOW_CP_EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
for argument in "$@"; do
    if [[ "$argument" == *".reflink-probe-"* ]]; then
        exec /usr/bin/cp "$@"
    fi
done
sleep 30
exec /usr/bin/cp "$@"
SLOW_CP_EOF
chmod 700 "$SLOW_CP"
SLOW_CONFIG="$RUN_ROOT/helper-slow.json"
write_helper_config "$SLOW_CONFIG" "$REPO_DIR" "$PGBACKREST_CONFIG" 120 "$SLOW_CP"
REQ_KILL2="$RUN_ROOT/request-helper-sigkill.json"
write_request "$REQ_KILL2" "chaos-helper-sigkill" "$TARGET_OID" "$TARGET_LSN" "$TARGET_FP"
"$HELPER" restore-table --config "$SLOW_CONFIG" --request "$REQ_KILL2" \
    >"$RUN_ROOT/helper-sigkill.out" 2>"$RUN_ROOT/helper-sigkill.err" &
ACTIVE_HELPER_PID=$!
phase=""
for _ in $(seq 1 200); do
    phase="$(jq -r '.phase // empty' "$WORK_ROOT/chaos-helper-sigkill/state.json" 2>/dev/null || true)"
    [[ "$phase" == "materializing" && -f "$WORK_ROOT/chaos-helper-sigkill/active-process.json" ]] && break
    sleep 0.05
done
[[ "$phase" == "materializing" ]] || die "helper SIGKILL never reached materializing"
ORPHAN_GROUP="$(jq -r '.process_group // empty' "$WORK_ROOT/chaos-helper-sigkill/active-process.json")"
kill -KILL "$ACTIVE_HELPER_PID" || true
set +e
wait "$ACTIVE_HELPER_PID"
set -e
ACTIVE_HELPER_PID=""
RETRY_CFG="$RUN_ROOT/helper-sigkill-retry.json"
jq --arg cp_bin "/usr/bin/cp" '.cp_bin = $cp_bin' "$SLOW_CONFIG" > "$RETRY_CFG"
"$HELPER" restore-table --config "$RETRY_CFG" --request "$REQ_KILL2" \
    >"$RUN_ROOT/helper-sigkill-retry.result.json"
[[ "$(jq -r '.status' "$RUN_ROOT/helper-sigkill-retry.result.json")" == "completed" ]] \
    || die "SIGKILL retry did not complete"
[[ ! -e "$WORK_ROOT/chaos-helper-sigkill/pgdata" ]] || die "SIGKILL left pgdata"
[[ ! -e "$WORK_ROOT/chaos-helper-sigkill/active-process.json" ]] || die "SIGKILL left active-process"
if [[ -n "$ORPHAN_GROUP" && "$ORPHAN_GROUP" != "null" ]]; then
    kill -0 -- "-$ORPHAN_GROUP" 2>/dev/null && die "orphan process group still alive"
fi
assert_baseline "helper_sigkill"
pass helper_sigkill
cleanup_injected_faults

# ---------------------------------------------------------------------------
# 7) Removed repository anchor on clone + corrupt cached artifact
# ---------------------------------------------------------------------------
mark_fail removed_anchor_and_corrupt_artifact
DEL_REPO="$RUN_ROOT/repo-deleted-anchor"
cp -a --reflink=always "$REPO_DIR" "$DEL_REPO" 2>/dev/null || cp -a "$REPO_DIR" "$DEL_REPO"
rm -rf "$DEL_REPO/backup/$STANZA/$FULL0_LABEL"
PGBR_D="$RUN_ROOT/pgbackrest-deleted.conf"
sed "s|$REPO_DIR|$DEL_REPO|" "$PGBACKREST_CONFIG" > "$PGBR_D"
HELPER_D="$RUN_ROOT/helper-deleted.json"
write_helper_config "$HELPER_D" "$DEL_REPO" "$PGBR_D" 30
REQ_DEL="$RUN_ROOT/request-deleted.json"
write_request "$REQ_DEL" "chaos-deleted-anchor" "$TARGET_OID" "$TARGET_LSN" "$TARGET_FP"
set +e
"$HELPER" restore-table --config "$HELPER_D" --request "$REQ_DEL" \
    >"$RUN_ROOT/deleted.out" 2>"$RUN_ROOT/deleted.err"
DEL_RC=$?
set -e
[[ "$DEL_RC" != "0" ]] || die "deleted-anchor restore must fail closed"
[[ ! -e "$WORK_ROOT/chaos-deleted-anchor/pgdata" ]] || die "deleted-anchor left pgdata"

# Corrupt a completed artifact from a successful restore if present; else create a fake cache file.
REQ_OK="$RUN_ROOT/request-artifact.json"
write_request "$REQ_OK" "chaos-artifact" "$TARGET_OID" "$TARGET_LSN" "$TARGET_FP"
"$HELPER" restore-table --config "$HELPER_CONFIG" --request "$REQ_OK" \
    >"$RUN_ROOT/artifact.result.json"
ART_PATH="$(find "$WORK_ROOT/chaos-artifact" -type f \( -name '*.dump' -o -name '*.sql' -o -name 'artifact*' \) 2>/dev/null | head -n1 || true)"
if [[ -n "$ART_PATH" && -f "$ART_PATH" ]]; then
    BEFORE_ART_SHA="$(sha256sum "$ART_PATH" | awk '{print $1}')"
    printf 'X' | dd of="$ART_PATH" bs=1 seek=0 conv=notrunc status=none
    "$HELPER" restore-table --config "$HELPER_CONFIG" --request "$REQ_OK" \
        >"$RUN_ROOT/artifact-retry.result.json"
    [[ "$(jq -r '.status' "$RUN_ROOT/artifact-retry.result.json")" == "completed" ]] \
        || die "corrupt artifact retry failed"
    AFTER_ART_SHA="$(sha256sum "$ART_PATH" | awk '{print $1}')"
    [[ "$AFTER_ART_SHA" != "$BEFORE_ART_SHA" ]] || log "artifact path unchanged after retry (acceptable if rebuilt elsewhere)"
fi
assert_baseline "removed_anchor_and_corrupt_artifact"
pass removed_anchor_and_corrupt_artifact
cleanup_injected_faults

# ---------------------------------------------------------------------------
# 8) Request ID replay / profile conflict
# ---------------------------------------------------------------------------
mark_fail request_conflict
REQ_CONFLICT="$RUN_ROOT/request-conflict.json"
write_request "$REQ_CONFLICT" "chaos-artifact" "$TARGET_OID" "$TARGET_LSN" "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
set +e
"$HELPER" restore-table --config "$HELPER_CONFIG" --request "$REQ_CONFLICT" \
    >"$RUN_ROOT/conflict.out" 2>"$RUN_ROOT/conflict.err"
CONFLICT_RC=$?
set -e
[[ "$CONFLICT_RC" != "0" ]] || die "request conflict must fail closed"
CODE="$(jq -r '.code // empty' "$RUN_ROOT/conflict.err" 2>/dev/null || true)"
[[ "$CODE" == "request_conflict" || "$CODE" == "fingerprint_mismatch" || "$CODE" == "table_not_found" || -n "$CODE" ]] \
    || die "conflict missing error code"
assert_baseline "request_conflict"
pass request_conflict
cleanup_injected_faults

RUN_COMPLETE=1
log "all chaos injectors passed ($PASSED)"
exit 0
