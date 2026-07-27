#!/usr/bin/env bash
# Worker shutdown/restart lifecycle regressions (scenarios A-E). Every case
# ties its outcome to the PostgreSQL log for this run: SIGSEGV, SIGBUS,
# "signal 7", "signal 11" and "abnormal database system shutdown" are hard
# failures regardless of what the harness's own exit code would otherwise say.
#
#   A. SIGTERM only the delta worker, postmaster alive: must restart within a
#      bounded time; capture must continue afterward.
#   B. SIGTERM only the maintenance worker, postmaster alive: must restart
#      within a bounded time.
#   C. pg_ctl stop -m fast on an idle, healthy cluster: clean shutdown, no
#      hard-failure pattern, no worker restart attempt after the shutdown
#      request.
#   D. pg_ctl restart -m fast: clean restart, each worker type comes back
#      exactly once, coverage/capture remain correct afterward.
#   E. pg_ctl stop -m fast while a genuine wal_status=lost condition is
#      active: no hard-failure pattern, no restart storm, and the durable
#      slot_lost/gap state survives the restart.
#
# Usage: ./scripts/run_worker_shutdown_restart_e2e.sh
# Env:   PG_CONFIG, PGFB_WSR_PORT, PGFB_WSR_KEEP=1

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PG_CONFIG="${PG_CONFIG:-/usr/local/pgsql-17/bin/pg_config}"
PG_BIN="$("$PG_CONFIG" --bindir)"
SHARE_DIR="$("$PG_CONFIG" --sharedir)"
PSQL="$PG_BIN/psql"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
PORT="${PGFB_WSR_PORT:-28968}"
WORK_ROOT="${PGFB_WSR_WORK_ROOT:-$ROOT/target/worker-shutdown-restart/$RUN_ID}"
DATA="$WORK_ROOT/data"
SOCKET="/tmp/pgfb-wsr-$RUN_ID"
RESULT_JSON="${PGFB_WSR_RESULT:-$ROOT/target/qualification/worker-shutdown-restart-$RUN_ID.json}"
LOG="$WORK_ROOT/postgresql.log"
DB=postgres
PASSED=0
FAILED=0
declare -a CASE_RESULTS=()

die() { echo "FAIL: $*" >&2; FAILED=$((FAILED + 1)); CASE_RESULTS+=("{\"name\":$(jq -Rn --arg s "$*" '$s'),\"pass\":false}"); exit 1; }
pass() { echo "  PASS: $*"; PASSED=$((PASSED + 1)); CASE_RESULTS+=("{\"name\":$(jq -Rn --arg s "$*" '$s'),\"pass\":true}"); }

cleanup() {
    local rc=$?
    set +e
    if [[ "${PGFB_WSR_KEEP:-0}" != "1" ]]; then
        "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1
        rm -rf "$DATA" "$SOCKET"
    else
        echo "PGFB_WSR_KEEP=1: leaving $WORK_ROOT" >&2
    fi
    mkdir -p "$(dirname "$RESULT_JSON")"
    local arr
    arr=$(printf '%s\n' "${CASE_RESULTS[@]:-}" | jq -s '.')
    jq -n \
        --arg run_id "$RUN_ID" --argjson passed "$PASSED" --argjson failed "$FAILED" \
        --argjson cases "$arr" --argjson exit_code "$rc" --arg log "$LOG" \
        '{run_id:$run_id,passed:$passed,failed:$failed,cases:$cases,exit_code:$exit_code,
          postgresql_log:$log,
          status:(if $failed==0 and $exit_code==0 then "PASS" else "FAIL" end)}' \
        >"$RESULT_JSON"
    echo "worker shutdown/restart E2E: $([[ $FAILED -eq 0 && $rc -eq 0 ]] && echo PASS || echo FAIL) ($RESULT_JSON)"
    [[ $FAILED -eq 0 && $rc -eq 0 ]] || exit 1
    exit 0
}
trap cleanup EXIT

[[ -x "$PSQL" ]] || die "psql not found via $PG_CONFIG"
[[ -f "$SHARE_DIR/extension/pg_flashback.control" ]] || die "extension not installed in $SHARE_DIR"

mkdir -p "$WORK_ROOT" "$SOCKET"
"$PG_BIN/initdb" -D "$DATA" --locale=C.UTF-8 -A trust >/dev/null
cat >>"$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_flashback'
wal_level = logical
max_replication_slots = 16
max_wal_senders = 16
max_worker_processes = 16
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
"$PG_BIN/pg_ctl" -D "$DATA" -l "$LOG" -o "-p $PORT -k $SOCKET" start -w >/dev/null

q() { "$PSQL" -X -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAtc "$1"; }
PGUSER="$(id -un)"
export PGHOST="$SOCKET" PGPORT="$PORT" PGUSER PGDATABASE="$DB"

hard_fail_scan() { # start_line -> prints matches, empty if clean
    local start=$1
    tail -n "+${start}" "$LOG" | grep -inE \
        "segmentation fault|sigsegv|bus error|sigbus|signal 7|signal 11|abnormal database system shutdown" \
        || true
}
log_lines() { wc -l < "$LOG" 2>/dev/null || echo 0; }

worker_pid() { # backend_type
    q "SELECT pid FROM pg_stat_activity WHERE backend_type='$1' AND datname=current_database()"
}

q "CREATE EXTENSION pg_flashback" >/dev/null
q "CREATE TABLE public.wsr_t (id int primary key, note text)" >/dev/null
q "SELECT flashback_track('public.wsr_t')" >/dev/null
q "INSERT INTO public.wsr_t VALUES (1, 'seed')" >/dev/null

# ==================== A. SIGTERM only the delta worker ====================
START_A=$(log_lines)
OLD_PID_A=$(worker_pid "pg_flashback delta worker")
[[ -n "$OLD_PID_A" ]] || die "A: delta worker not found before SIGTERM"
kill -TERM "$OLD_PID_A"
NEW_PID_A=""
for _ in $(seq 1 100); do
    NEW_PID_A=$(worker_pid "pg_flashback delta worker")
    [[ -n "$NEW_PID_A" && "$NEW_PID_A" != "$OLD_PID_A" ]] && break
    sleep 0.1
done
[[ -n "$NEW_PID_A" && "$NEW_PID_A" != "$OLD_PID_A" ]] || die "A: delta worker did not restart within bound (old=$OLD_PID_A)"
HITS_A=$(hard_fail_scan "$((START_A + 1))")
[[ -z "$HITS_A" ]] || die "A: hard-failure pattern after manual delta-worker SIGTERM: $HITS_A"
q "INSERT INTO public.wsr_t VALUES (2, 'after-A')" >/dev/null
CAPTURED_A=0
for _ in $(seq 1 100); do
    [[ "$(q "SELECT count(*) FROM flashback.delta_log dl JOIN flashback.tracked_tables tt USING (tracking_id) WHERE tt.table_name='wsr_t' AND dl.event_type='INSERT'")" -ge 1 ]] && { CAPTURED_A=1; break; }
    sleep 0.1
done
[[ "$CAPTURED_A" == "1" ]] || die "A: capture did not resume after delta-worker restart"
pass "A: manual SIGTERM to delta worker alone restarts within bound, capture resumes, no hard-failure"

# ==================== B. SIGTERM only the maintenance worker ====================
START_B=$(log_lines)
OLD_PID_B=$(worker_pid "pg_flashback maintenance worker")
[[ -n "$OLD_PID_B" ]] || die "B: maintenance worker not found before SIGTERM"
kill -TERM "$OLD_PID_B"
NEW_PID_B=""
for _ in $(seq 1 100); do
    NEW_PID_B=$(worker_pid "pg_flashback maintenance worker")
    [[ -n "$NEW_PID_B" && "$NEW_PID_B" != "$OLD_PID_B" ]] && break
    sleep 0.1
done
[[ -n "$NEW_PID_B" && "$NEW_PID_B" != "$OLD_PID_B" ]] || die "B: maintenance worker did not restart within bound (old=$OLD_PID_B)"
HITS_B=$(hard_fail_scan "$((START_B + 1))")
[[ -z "$HITS_B" ]] || die "B: hard-failure pattern after manual maintenance-worker SIGTERM: $HITS_B"
pass "B: manual SIGTERM to maintenance worker alone restarts within bound, no hard-failure"

# ==================== C. pg_ctl stop -m fast, idle+healthy ====================
START_C=$(log_lines)
"$PG_BIN/pg_ctl" -D "$DATA" stop -m fast -w -t 30
HITS_C=$(hard_fail_scan "$((START_C + 1))")
[[ -z "$HITS_C" ]] || die "C: hard-failure pattern during clean fast shutdown: $HITS_C"
grep -q "database system is shut down" <(tail -n "+$((START_C + 1))" "$LOG") \
    || die "C: fast shutdown did not report clean 'database system is shut down'"
grep -qi "abnormal database system shutdown" <(tail -n "+$((START_C + 1))" "$LOG") \
    && die "C: fast shutdown reported abnormal database system shutdown"
# No restart attempt: no worker "started" line after "received fast shutdown request".
SHUTDOWN_LINE=$(tail -n "+$((START_C + 1))" "$LOG" | grep -n "received fast shutdown request" | head -1 | cut -d: -f1)
[[ -n "$SHUTDOWN_LINE" ]] || die "C: no 'received fast shutdown request' line found"
POST_SHUTDOWN_RESTARTS=$(tail -n "+$((START_C + 1))" "$LOG" | tail -n "+$((SHUTDOWN_LINE + 1))" | grep -c "worker 0 started" || true)
[[ "$POST_SHUTDOWN_RESTARTS" == "0" ]] || die "C: worker restart attempt(s) logged after shutdown request began"
pass "C: pg_ctl stop -m fast is clean, no hard-failure, no worker restart attempts"

"$PG_BIN/pg_ctl" -D "$DATA" -l "$LOG" -o "-p $PORT -k $SOCKET" start -w >/dev/null
for _ in $(seq 1 60); do q "SELECT 1" >/dev/null 2>&1 && break; sleep 0.5; done

# ==================== D. pg_ctl restart -m fast ====================
insert_count_wsr_t() { q "SELECT count(*) FROM flashback.delta_log dl JOIN flashback.tracked_tables tt USING (tracking_id) WHERE tt.table_name='wsr_t' AND dl.event_type='INSERT'"; }
q "INSERT INTO public.wsr_t VALUES (3, 'before-D')" >/dev/null
BEFORE_D_COUNT=0
for _ in $(seq 1 100); do
    BEFORE_D_COUNT=$(insert_count_wsr_t)
    [[ "$BEFORE_D_COUNT" -ge 1 ]] && break
    sleep 0.1
done
[[ "$BEFORE_D_COUNT" -ge 1 ]] || die "D: pre-restart marker row was never captured"
START_D=$(log_lines)
"$PG_BIN/pg_ctl" -D "$DATA" restart -m fast -w -t 60 -l "$LOG" >/dev/null
for _ in $(seq 1 60); do q "SELECT 1" >/dev/null 2>&1 && break; sleep 0.5; done
HITS_D=$(hard_fail_scan "$((START_D + 1))")
[[ -z "$HITS_D" ]] || die "D: hard-failure pattern during restart -m fast: $HITS_D"
DELTA_STARTS_D=$(tail -n "+$((START_D + 1))" "$LOG" | grep -c "pg_flashback delta worker 0 started" || true)
MAINT_STARTS_D=$(tail -n "+$((START_D + 1))" "$LOG" | grep -c "pg_flashback maintenance worker 0 started" || true)
[[ "$DELTA_STARTS_D" == "1" ]] || die "D: delta worker started $DELTA_STARTS_D times after restart (expected exactly 1)"
[[ "$MAINT_STARTS_D" == "1" ]] || die "D: maintenance worker started $MAINT_STARTS_D times after restart (expected exactly 1)"
HEALTH_D=$(q "SELECT health FROM flashback_health() WHERE table_name='public.wsr_t'")
[[ "$HEALTH_D" != "slot_lost" ]] || die "D: coverage shows slot_lost after a clean restart"
q "INSERT INTO public.wsr_t VALUES (4, 'after-D')" >/dev/null
CAPTURED_D=0
for _ in $(seq 1 100); do
    [[ "$(insert_count_wsr_t)" -gt "$BEFORE_D_COUNT" ]] && { CAPTURED_D=1; break; }
    sleep 0.1
done
[[ "$CAPTURED_D" == "1" ]] || die "D: capture did not resume after restart"
pass "D: pg_ctl restart -m fast is clean, each worker starts exactly once, coverage/capture correct after"

# ==================== E. fast shutdown while wal_status=lost is active ====================
q "CREATE TABLE public.wsr_bulk (id int, payload text)" >/dev/null
OLD_STREAM_E=$(q "SELECT stream_id FROM flashback.capture_streams WHERE state='active'")
"$PSQL" -X -h "$SOCKET" -p "$PORT" -d postgres -qAtc "ALTER SYSTEM SET max_slot_wal_keep_size = '256kB'" >/dev/null
"$PSQL" -X -h "$SOCKET" -p "$PORT" -d postgres -qAtc "SELECT pg_reload_conf()" >/dev/null

DELTA_PID_E=""
for _ in $(seq 1 200); do
    DELTA_PID_E=$(q "SELECT pid FROM pg_stat_activity WHERE backend_type='pg_flashback delta worker' AND datname=current_database() AND wait_event_type='Extension'")
    [[ -n "$DELTA_PID_E" ]] && break
    sleep 0.05
done
[[ -n "$DELTA_PID_E" ]] || die "E: could not find idle delta worker to freeze"
kill -STOP "$DELTA_PID_E"
[[ "$(ps -o stat= -p "$DELTA_PID_E")" == T* ]] || die "E: delta worker did not actually stop"

q "INSERT INTO public.wsr_bulk SELECT g, repeat('z', 800) FROM generate_series(1, 300000) g" >/dev/null
q "CHECKPOINT" >/dev/null
WAL_STATUS_E=""
for _ in $(seq 1 100); do
    WAL_STATUS_E=$(q "SELECT wal_status FROM pg_replication_slots WHERE slot_name LIKE 'pg_flashback_%'")
    [[ "$WAL_STATUS_E" == "lost" ]] && break
    sleep 0.1
done
[[ "$WAL_STATUS_E" == "lost" ]] || die "E: could not force genuine wal_status=lost"

kill -CONT "$DELTA_PID_E"
for _ in $(seq 1 100); do
    [[ "$(q "SELECT count(*) FROM flashback.capture_streams WHERE stream_id=$OLD_STREAM_E AND state='broken'")" == "1" ]] && break
    sleep 0.1
done
[[ "$(q "SELECT state FROM flashback.capture_streams WHERE stream_id=$OLD_STREAM_E")" == "broken" ]] \
    || die "E: stream did not break on genuine slot loss before shutdown"

START_E=$(log_lines)
"$PG_BIN/pg_ctl" -D "$DATA" stop -m fast -w -t 30
HITS_E=$(hard_fail_scan "$((START_E + 1))")
[[ -z "$HITS_E" ]] || die "E: hard-failure pattern during fast shutdown under active slot-loss: $HITS_E"
grep -qi "abnormal database system shutdown" <(tail -n "+$((START_E + 1))" "$LOG") \
    && die "E: fast shutdown under slot-loss reported abnormal database system shutdown"

"$PG_BIN/pg_ctl" -D "$DATA" -l "$LOG" -o "-p $PORT -k $SOCKET" start -w >/dev/null
for _ in $(seq 1 60); do q "SELECT 1" >/dev/null 2>&1 && break; sleep 0.5; done
[[ "$(q "SELECT state FROM flashback.capture_streams WHERE stream_id=$OLD_STREAM_E")" == "broken" ]] \
    || die "E: broken/slot_lost state did not survive the restart"
[[ "$(q "SELECT health FROM flashback_health() WHERE table_name='public.wsr_t'")" == "slot_lost" ]] \
    || die "E: health did not report slot_lost after restart"
POST_E_PID_1=$(worker_pid "pg_flashback delta worker")
[[ -n "$POST_E_PID_1" ]] || die "E: no delta worker after restart"
STORM_E=0
for _ in $(seq 1 40); do
    sleep 0.1
    P=$(worker_pid "pg_flashback delta worker")
    [[ "$P" != "$POST_E_PID_1" ]] && { STORM_E=1; break; }
done
[[ "$STORM_E" == "0" ]] || die "E: delta worker restart storm after restart under slot-loss"
pass "E: fast shutdown under active slot-loss is clean, no restart storm, durable state survives restart"

echo "worker shutdown/restart: $PASSED passed, $FAILED failed"
