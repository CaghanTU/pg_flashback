#!/usr/bin/env bash
# Worker admission + kill/restart/catch-up + multi-DB max_workers regressions.
#
# Complements run_capture_maintenance_isolation_slo.sh. Proves:
#   * maintenance kill does not stall capture
#   * capture kill + automatic restart consumes retained slot WAL exactly once
#   * missing workers never project as healthy
#   * databases beyond max_workers are fail-closed for track
#   * exact event counts and fingerprints match after recovery
#   * slot lag drains to a bounded target
#
# Prerequisite: cargo pgrx install --pg-config "$PG_CONFIG"

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PG_CONFIG="${PG_CONFIG:-/usr/local/pgsql-17/bin/pg_config}"
# shellcheck source=scripts/qualification_provenance.sh
source "$ROOT/scripts/qualification_provenance.sh"
qualification_provenance_init "$ROOT" "$PG_CONFIG"
PG_BIN="$("$PG_CONFIG" --bindir)"
SHARE_DIR="$("$PG_CONFIG" --sharedir)"
PSQL="$PG_BIN/psql"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
PORT="${PG_FLASHBACK_WORKER_ADMISSION_PORT:-28937}"
DRAIN_TIMEOUT_SECONDS="${PG_FLASHBACK_WORKER_ADMISSION_DRAIN_TIMEOUT_SECONDS:-45}"
RESTART_TIMEOUT_SECONDS="${PG_FLASHBACK_WORKER_ADMISSION_RESTART_TIMEOUT_SECONDS:-30}"
WORK_ROOT="${PG_FLASHBACK_WORKER_ADMISSION_WORK_ROOT:-$ROOT/target/worker-admission-isolation/$RUN_ID}"
DATA="$WORK_ROOT/data"
SOCKET="/tmp/pgfb-wadm-$RUN_ID"
RESULT_DIR="${PG_FLASHBACK_WORKER_ADMISSION_RESULT_DIR:-$ROOT/target/qualification}"
RESULT_JSON="$RESULT_DIR/worker-admission-isolation-$RUN_ID.json"
DB_A=wadm_a
DB_B=wadm_b
DB_C=wadm_c

require_file() {
    [[ -f "$1" ]] || {
        echo "FAIL: required extension artifact not found: $1" >&2
        echo "Run: cargo pgrx install --pg-config $PG_CONFIG" >&2
        exit 2
    }
}

cleanup() {
    local rc=$?
    set +e
    "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1 || true
    rm -rf "$DATA" "$SOCKET"
    exit "$rc"
}
trap cleanup EXIT

require_file "$PG_CONFIG"
require_file "$SHARE_DIR/extension/pg_flashback.control"
mkdir -p "$WORK_ROOT" "$SOCKET" "$RESULT_DIR"
"$PG_BIN/initdb" -D "$DATA" --locale=C.UTF-8 -A trust >/dev/null
# Start without target_databases so we can create DBs before workers connect.
cat >>"$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_flashback'
wal_level = logical
max_replication_slots = 16
max_wal_senders = 16
max_worker_processes = 16
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
pg_flashback.worker_interval_ms = 50
pg_flashback.max_workers = 2
pg_flashback.target_database = postgres
pg_flashback.local_max_snapshot_bytes = 8GB
pg_flashback.local_max_restore_peak_bytes = 16GB
pg_flashback.local_min_filesystem_bytes = 64MB
pg_flashback.local_safety_reserve_bytes = 16MB
EOF
"$PG_BIN/pg_ctl" -D "$DATA" -l "$WORK_ROOT/postgresql.log" \
    -o "-p $PORT -k $SOCKET" start -w >/dev/null

q() {
    local db=$1
    shift
    "$PSQL" -X -h "$SOCKET" -p "$PORT" -d "$db" -v ON_ERROR_STOP=1 -qAtc "$1"
}
qp() { q postgres "$1"; }

qp "CREATE DATABASE $DB_A;"
qp "CREATE DATABASE $DB_B;"
qp "CREATE DATABASE $DB_C;"
for db in "$DB_A" "$DB_B" "$DB_C"; do
    q "$db" "CREATE EXTENSION pg_flashback;"
done

# Admit A and B only (C remains configured-but-beyond-max_workers after restart).
qp "ALTER SYSTEM SET pg_flashback.max_workers = 2;"
qp "ALTER SYSTEM SET pg_flashback.target_databases = '$DB_A,$DB_B,$DB_C';"
"$PG_BIN/pg_ctl" -D "$DATA" -l "$WORK_ROOT/postgresql.log" -m fast -w restart >/dev/null

# Process identity + database identity for admitted pair on DB_A.
wait_workers() {
    local db=$1 role=$2 deadline=$(( $(date +%s) + RESTART_TIMEOUT_SECONDS ))
    local pid=""
    while (( $(date +%s) <= deadline )); do
        if [[ "$role" == capture ]]; then
            pid=$(q "$db" "SELECT flashback_capture_worker_pid();")
        else
            pid=$(q "$db" "SELECT flashback_maintenance_worker_pid();")
        fi
        if [[ -n "$pid" && "$pid" != "" ]]; then
            printf '%s\n' "$pid"
            return 0
        fi
        sleep 0.1
    done
    return 1
}

wait_workers "$DB_A" capture >/dev/null || { echo "FAIL: DB_A capture worker missing" >&2; exit 1; }
MAIN_A=$(wait_workers "$DB_A" maintenance) || { echo "FAIL: DB_A maintenance worker missing" >&2; exit 1; }
wait_workers "$DB_B" capture >/dev/null || { echo "FAIL: DB_B capture worker missing" >&2; exit 1; }
wait_workers "$DB_B" maintenance >/dev/null || { echo "FAIL: DB_B maintenance worker missing" >&2; exit 1; }

# DB_C is configured but beyond max_workers=2.
STATE_C=$(q "$DB_C" "SELECT admission_state FROM flashback_worker_readiness();")
[[ "$STATE_C" == "beyond_max_workers" ]] || {
    echo "FAIL: expected beyond_max_workers for $DB_C, got $STATE_C" >&2
    exit 1
}
q "$DB_C" "CREATE TABLE public.beyond(id int PRIMARY KEY, payload text);"
RC_C=0
q "$DB_C" "SELECT flashback_track('public.beyond');" >/dev/null 2>"$WORK_ROOT/beyond.err" || RC_C=$?
[[ "$RC_C" != 0 ]] || { echo "FAIL: beyond-max_workers track succeeded" >&2; exit 1; }
grep -q "beyond pg_flashback.max_workers" "$WORK_ROOT/beyond.err"
[[ "$(q "$DB_C" "SELECT count(*) FROM flashback.tracked_tables;")" == "0" ]] || {
    echo "FAIL: beyond-max_workers track leaked lifecycle metadata" >&2
    exit 1
}
beyond_max_workers_rejected=true

# Track on admitted DB_A and seed events.
q "$DB_A" "CREATE TABLE public.events(id integer PRIMARY KEY, payload text NOT NULL);
           CREATE TABLE public.sidecar(id integer PRIMARY KEY, payload text NOT NULL);"
q "$DB_A" "SELECT flashback_track('public.events');" >/dev/null
q "$DB_A" "SELECT flashback_track('public.sidecar');" >/dev/null
for _ in $(seq 1 100); do
    active=$(q "$DB_A" "SELECT count(*) FROM flashback.coverage_generations
                        WHERE state='active' AND recovery_profile='local_delta';")
    [[ "$active" == "2" ]] && break
    sleep 0.1
done
[[ "${active:-0}" == "2" ]] || { echo "FAIL: anchors not active" >&2; exit 1; }

SEED=50
for i in $(seq 1 "$SEED"); do
    q "$DB_A" "INSERT INTO public.events VALUES ($i, repeat('e', 16));" >/dev/null
done
SLOT=$(q "$DB_A" "SELECT flashback_effective_slot_name();")
drain_deadline=$(( $(date +%s) + DRAIN_TIMEOUT_SECONDS ))
while (( $(date +%s) <= drain_deadline )); do
    rows=$(q "$DB_A" "SELECT count(*) FROM flashback.delta_log
                      WHERE rel_oid='public.events'::regclass AND event_type='INSERT';")
    [[ "$rows" == "$SEED" ]] && break
    sleep 0.05
done
[[ "${rows:-0}" == "$SEED" ]] || { echo "FAIL: seed inserts not captured ($rows)" >&2; exit 1; }
HEALTH_OK=$(q "$DB_A" "SELECT health FROM flashback_health()
                       WHERE table_name='public.events';")
[[ "$HEALTH_OK" == "healthy" ]] || {
    echo "FAIL: expected healthy before kill, got $HEALTH_OK" >&2
    exit 1
}

# Kill maintenance once: capture must continue; health must not stay falsely healthy.
kill -TERM "$MAIN_A"
false_healthy_during_maint_gap=false
saw_maint_missing=false
observe_deadline=$(( $(date +%s) + 5 ))
while (( $(date +%s) <= observe_deadline )); do
    state_now=$(q "$DB_A" "SELECT admission_state FROM flashback_worker_readiness();")
    health_now=$(q "$DB_A" "SELECT health FROM flashback_health()
                            WHERE table_name='public.events';")
    if [[ "$state_now" == "maintenance_missing" ]]; then
        saw_maint_missing=true
        [[ "$health_now" != "healthy" ]] || false_healthy_during_maint_gap=true
        break
    fi
    cur_main=$(q "$DB_A" "SELECT COALESCE(flashback_maintenance_worker_pid()::text, '');")
    if [[ -z "$cur_main" ]]; then
        saw_maint_missing=true
        [[ "$health_now" != "healthy" ]] || false_healthy_during_maint_gap=true
        break
    fi
    sleep 0.05
done
[[ "$saw_maint_missing" == true ]] || {
    echo "FAIL: never observed maintenance_missing after kill" >&2
    exit 1
}
[[ "$false_healthy_during_maint_gap" == false ]] || {
    echo "FAIL: health reported healthy while maintenance worker missing" >&2
    exit 1
}

POST_MAIN_INSERTS=40
capture_while_maint_down=false
# Insert while maintenance may still be restarting; capture must not stall.
for i in $(seq 1 "$POST_MAIN_INSERTS"); do
    q "$DB_A" "INSERT INTO public.sidecar VALUES ($i, repeat('s', 8));" >/dev/null
done
drain_deadline=$(( $(date +%s) + DRAIN_TIMEOUT_SECONDS ))
while (( $(date +%s) <= drain_deadline )); do
    side=$(q "$DB_A" "SELECT count(*) FROM flashback.delta_log
                      WHERE rel_oid='public.sidecar'::regclass AND event_type='INSERT';")
    [[ "$side" == "$POST_MAIN_INSERTS" ]] && { capture_while_maint_down=true; break; }
    sleep 0.05
done
[[ "$capture_while_maint_down" == true ]] || {
    echo "FAIL: capture stalled while maintenance was interrupted" >&2
    exit 1
}
# Wait for maintenance restart (bounded).
MAIN_A2=$(wait_workers "$DB_A" maintenance) || {
    echo "FAIL: maintenance worker did not restart" >&2
    exit 1
}

# Kill capture: events remain in slot; restart must consume exactly once.
CAP_BEFORE_KILL=$(q "$DB_A" "SELECT flashback_capture_worker_pid();")
kill -TERM "$CAP_BEFORE_KILL"
# Observe a non-healthy window before automatic restart, with a bounded wait.
false_healthy_during_capture_gap=false
saw_capture_missing=false
observe_deadline=$(( $(date +%s) + 5 ))
while (( $(date +%s) <= observe_deadline )); do
    state_down=$(q "$DB_A" "SELECT admission_state FROM flashback_worker_readiness();")
    health_down=$(q "$DB_A" "SELECT health FROM flashback_health()
                             WHERE table_name='public.events';")
    if [[ "$state_down" == "capture_missing" ]]; then
        saw_capture_missing=true
        [[ "$health_down" != "healthy" ]] || false_healthy_during_capture_gap=true
        break
    fi
    # If restart already completed, ensure health is not healthy with a stale dead pid.
    cur=$(q "$DB_A" "SELECT COALESCE(flashback_capture_worker_pid()::text,'');")
    if [[ -z "$cur" ]]; then
        [[ "$health_down" != "healthy" ]] || false_healthy_during_capture_gap=true
        saw_capture_missing=true
        break
    fi
    sleep 0.05
done
[[ "$saw_capture_missing" == true ]] || {
    echo "FAIL: never observed capture_missing after kill (restart too fast to sample; retrying with STOP)" >&2
    CAP_NOW=$(q "$DB_A" "SELECT flashback_capture_worker_pid();")
    kill -STOP "$CAP_NOW"
    state_down=$(q "$DB_A" "SELECT admission_state FROM flashback_worker_readiness();")
    # STOP keeps the process visible in pg_stat_activity; terminate instead and
    # block restart briefly by holding the name — fall back to TERM + immediate check.
    kill -CONT "$CAP_NOW" 2>/dev/null || true
    kill -KILL "$CAP_NOW" 2>/dev/null || true
    sleep 0.05
    state_down=$(q "$DB_A" "SELECT admission_state FROM flashback_worker_readiness();")
    health_down=$(q "$DB_A" "SELECT health FROM flashback_health() WHERE table_name='public.events';")
    if [[ "$state_down" == "capture_missing" || -z "$(q "$DB_A" "SELECT COALESCE(flashback_capture_worker_pid()::text,'');")" ]]; then
        saw_capture_missing=true
        [[ "$health_down" != "healthy" ]] || false_healthy_during_capture_gap=true
    fi
}
[[ "$saw_capture_missing" == true ]] || {
    echo "FAIL: could not observe capture worker absence" >&2
    exit 1
}
[[ "$false_healthy_during_capture_gap" == false ]] || {
    echo "FAIL: false healthy while capture missing" >&2
    exit 1
}

events_before_catchup=$(q "$DB_A" "SELECT count(*) FROM flashback.delta_log
                                    WHERE rel_oid='public.events'::regclass
                                      AND event_type='INSERT';")
CATCHUP=60
for i in $(seq $((SEED + 1)) $((SEED + CATCHUP))); do
    q "$DB_A" "INSERT INTO public.events VALUES ($i, repeat('e', 16));" >/dev/null
done

CAP_A2=$(wait_workers "$DB_A" capture) || {
    echo "FAIL: capture worker did not restart" >&2
    exit 1
}
[[ "$CAP_A2" != "$CAP_BEFORE_KILL" ]] || {
    echo "FAIL: capture pid did not change after kill" >&2
    exit 1
}

EXPECTED_EVENTS=$((SEED + CATCHUP))
drain_deadline=$(( $(date +%s) + DRAIN_TIMEOUT_SECONDS ))
drain_ok=false
final_lag=0
while (( $(date +%s) <= drain_deadline )); do
    events_after=$(q "$DB_A" "SELECT count(*) FROM flashback.delta_log
                              WHERE rel_oid='public.events'::regclass
                                AND event_type='INSERT';")
    final_lag=$(q "$DB_A" "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn),0)::bigint
                           FROM pg_replication_slots WHERE slot_name='$SLOT';")
    if [[ "$events_after" == "$EXPECTED_EVENTS" ]] && (( final_lag <= 65536 )); then
        drain_ok=true
        break
    fi
    sleep 0.05
done
[[ "$events_after" == "$EXPECTED_EVENTS" ]] || {
    echo "FAIL: expected $EXPECTED_EVENTS INSERT events, got $events_after (before catchup=$events_before_catchup)" >&2
    exit 1
}
# Exactly-once: no duplicate ids in delta_log for events table inserts.
dups=$(q "$DB_A" "SELECT count(*) FROM (
                    SELECT (new_data->>'id')::int AS id, count(*)
                    FROM flashback.delta_log
                    WHERE rel_oid='public.events'::regclass AND event_type='INSERT'
                    GROUP BY 1 HAVING count(*) > 1
                  ) d;")
[[ "$dups" == "0" ]] || { echo "FAIL: duplicate INSERT events after catch-up" >&2; exit 1; }
# Fingerprint of live table must reflect catch-up inserts.
FP_LIVE=$(q "$DB_A" "SELECT md5(string_agg(id::text || ':' || payload, ',' ORDER BY id))
                     FROM public.events;")
EXPECTED_FP=$(q "$DB_A" "SELECT md5(string_agg(id::text || ':' || repeat('e',16), ',' ORDER BY id))
                         FROM generate_series(1,$EXPECTED_EVENTS) AS id;")
[[ "$FP_LIVE" == "$EXPECTED_FP" ]] || {
    echo "FAIL: live table fingerprint mismatch" >&2
    exit 1
}
DELTA_FP=$(q "$DB_A" "SELECT md5(string_agg((new_data->>'id') || ':' || (new_data->>'payload'), ','
                          ORDER BY (new_data->>'id')::int))
                      FROM flashback.delta_log
                      WHERE rel_oid='public.events'::regclass AND event_type='INSERT';")
[[ "$DELTA_FP" == "$EXPECTED_FP" ]] || {
    echo "FAIL: delta fingerprint mismatch after catch-up" >&2
    exit 1
}

health_final=$(q "$DB_A" "SELECT health FROM flashback_health() WHERE table_name='public.events';")
state_final=$(q "$DB_A" "SELECT admission_state FROM flashback_worker_readiness();")
[[ "$state_final" == "ready" ]] || {
    echo "FAIL: readiness not ready after recovery ($state_final)" >&2
    exit 1
}
[[ "$health_final" == "healthy" ]] || {
    echo "FAIL: health not healthy after recovery ($health_final)" >&2
    exit 1
}
[[ "$false_healthy_during_capture_gap" == false ]] || {
    echo "FAIL: false healthy during capture gap" >&2
    exit 1
}
[[ "$drain_ok" == true ]] || {
    echo "FAIL: slot lag did not drain (lag=$final_lag)" >&2
    exit 1
}

# DB_B still has its own workers (isolation across databases).
CAP_B2=$(q "$DB_B" "SELECT flashback_capture_worker_pid();")
[[ -n "$CAP_B2" ]] || { echo "FAIL: DB_B capture lost during DB_A churn" >&2; exit 1; }

status=PASS
cat >"$RESULT_JSON" <<EOF
{
  "run_id": "$RUN_ID",
$(qualification_provenance_json "$(date -u +%Y-%m-%dT%H:%M:%SZ)"),
  "status": "$status",
  "beyond_max_workers_rejected": $beyond_max_workers_rejected,
  "capture_progressed_while_maintenance_interrupted": $capture_while_maint_down,
  "false_healthy_during_maintenance_gap": $false_healthy_during_maint_gap,
  "false_healthy_during_capture_gap": $false_healthy_during_capture_gap,
  "seed_inserts": $SEED,
  "catchup_inserts": $CATCHUP,
  "exact_insert_events": $events_after,
  "duplicate_insert_events": $dups,
  "delta_fingerprint": "$DELTA_FP",
  "live_fingerprint": "$FP_LIVE",
  "final_slot_lag_bytes": $final_lag,
  "drain_ok": $drain_ok,
  "capture_pid_before_kill": $CAP_BEFORE_KILL,
  "capture_pid_after_restart": $CAP_A2,
  "maintenance_pid_after_restart": $MAIN_A2,
  "db_b_capture_pid": $CAP_B2,
  "final_health": "$health_final",
  "final_admission_state": "$state_final"
}
EOF

echo "Worker admission isolation: $status ($RESULT_JSON)"
[[ "$status" == "PASS" ]]
