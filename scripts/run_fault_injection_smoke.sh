#!/usr/bin/env bash
# Bounded development fault smoke for worker recovery and shutdown cleanup.

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PG_CONFIG="${PG_CONFIG:-/usr/local/pgsql-17/bin/pg_config}"
PG_BIN="$("$PG_CONFIG" --bindir)"
SHARE_DIR="$("$PG_CONFIG" --sharedir)"
PSQL="$PG_BIN/psql"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
PORT="${PG_FLASHBACK_FAULT_PORT:-28947}"
WORK_ROOT="${PG_FLASHBACK_FAULT_WORK_ROOT:-$ROOT/target/fault-smoke/$RUN_ID}"
DATA="$WORK_ROOT/data"
SOCKET="/tmp/pgfb-fault-$RUN_ID"
RESULT_DIR="${PG_FLASHBACK_FAULT_RESULT_DIR:-$ROOT/target/qualification}"
RESULT_JSON="$RESULT_DIR/fault-injection-smoke-$RUN_ID.json"
DB=postgres
RESTART_PASS=false
FAST_STOP_PASS=false
CLEANUP_OK=false

require_file() {
    [[ -f "$1" ]] || { echo "FAIL: required file not found: $1" >&2; exit 2; }
}

write_result() {
    local rc=$1
    mkdir -p "$RESULT_DIR"
    cat >"$RESULT_JSON" <<EOF
{
  "run_id": "$RUN_ID",
  "commit": "$(git -C "$ROOT" rev-parse HEAD)",
  "status": "$([[ "$RESTART_PASS" == true && "$FAST_STOP_PASS" == true && "$CLEANUP_OK" == true ]] && echo PASS || echo FAIL)",
  "faults": [
    {"name": "postmaster_restart_worker_recovery", "pass": $RESTART_PASS},
    {"name": "fast_shutdown_restart_recovery", "pass": $FAST_STOP_PASS}
  ],
  "cleanup": {"postgres_stopped": $CLEANUP_OK, "data_directory_removed": $CLEANUP_OK},
  "exit_code": $rc
}
EOF
}

cleanup() {
    local rc=$?
    set +e
    "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1
    if ! "$PG_BIN/pg_ctl" -D "$DATA" status >/dev/null 2>&1; then
        rm -rf "$DATA" "$SOCKET"
        [[ ! -e "$DATA" && ! -e "$SOCKET" ]] && CLEANUP_OK=true
    fi
    write_result "$rc"
    if [[ "$RESTART_PASS" != true || "$FAST_STOP_PASS" != true || "$CLEANUP_OK" != true ]]; then rc=1; fi
    echo "Fault injection smoke: $([[ "$rc" == 0 ]] && echo PASS || echo FAIL) ($RESULT_JSON)"
    exit "$rc"
}
trap cleanup EXIT

require_file "$PG_CONFIG"
require_file "$SHARE_DIR/extension/pg_flashback.control"
mkdir -p "$WORK_ROOT" "$SOCKET" "$RESULT_DIR"
"$PG_BIN/initdb" -D "$DATA" --locale=C.UTF-8 -A trust >/dev/null
cat >>"$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_flashback'
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
pg_flashback.worker_interval_ms = 25
EOF
"$PG_BIN/pg_ctl" -D "$DATA" -l "$WORK_ROOT/postgresql.log" \
    -o "-p $PORT -k $SOCKET" start -w >/dev/null
q() { "$PSQL" -X -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAtc "$1"; }
wait_health() {
    local expected=$1
    for _ in $(seq 1 200); do
        [[ "$(q "SELECT health FROM flashback_health()
                     WHERE table_name = 'public.fault_probe';")" == "$expected" ]] && return 0
        sleep 0.1
    done
    return 1
}

q "CREATE EXTENSION pg_flashback;
   CREATE TABLE public.fault_probe(id integer PRIMARY KEY, payload text NOT NULL);"
q "SELECT flashback_track('public.fault_probe');" >/dev/null
wait_health healthy || { echo "FAIL: initial coverage did not become healthy" >&2; exit 1; }

"$PG_BIN/pg_ctl" -D "$DATA" restart -w -t 30 -l "$WORK_ROOT/postgresql.log" >/dev/null
q "INSERT INTO public.fault_probe VALUES (1, 'after-restart');" >/dev/null
wait_health healthy || { echo "FAIL: health did not recover after restart" >&2; exit 1; }
for _ in $(seq 1 200); do
    worker_rows="$(q "SELECT count(*) FROM flashback.delta_log
                       WHERE rel_oid = 'public.fault_probe'::regclass;")"
    [[ "$worker_rows" -ge 1 ]] && break
    sleep 0.1
done
[[ "${worker_rows:-0}" -ge 1 ]] && RESTART_PASS=true

"$PG_BIN/pg_ctl" -D "$DATA" stop -m fast -w >/dev/null
"$PG_BIN/pg_ctl" -D "$DATA" -l "$WORK_ROOT/postgresql.log" \
    -o "-p $PORT -k $SOCKET" start -w >/dev/null
q "INSERT INTO public.fault_probe VALUES (2, 'after-fast-stop');" >/dev/null
if wait_health healthy; then
    for _ in $(seq 1 200); do
        worker_rows="$(q "SELECT count(*) FROM flashback.delta_log
                           WHERE rel_oid = 'public.fault_probe'::regclass;")"
        [[ "$worker_rows" -ge 2 ]] && break
        sleep 0.1
    done
    [[ "${worker_rows:-0}" -ge 2 ]] && FAST_STOP_PASS=true
fi

[[ "$RESTART_PASS" == true && "$FAST_STOP_PASS" == true ]]
