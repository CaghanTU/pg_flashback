#!/usr/bin/env bash
# Development qualification for M3 capture/maintenance isolation.
#
# Starts an isolated PostgreSQL cluster, tracks two local_delta tables, then
# holds one lifecycle lock while the other table receives WAL. The JSON result
# is intentionally machine-readable so CI or an overnight run can retain the
# observed slot lag and visibility samples.
#
# Prerequisite: install the current extension for this PostgreSQL build first:
#   cargo pgrx install --pg-config /usr/local/pgsql-17/bin/pg_config

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PG_CONFIG="${PG_CONFIG:-/usr/local/pgsql-17/bin/pg_config}"
PG_BIN="$("$PG_CONFIG" --bindir)"
SHARE_DIR="$("$PG_CONFIG" --sharedir)"
PSQL="$PG_BIN/psql"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
PORT="${PG_FLASHBACK_ISOLATION_PORT:-28927}"
HOLD_SECONDS="${PG_FLASHBACK_ISOLATION_HOLD_SECONDS:-30}"
WORK_ROOT="${PG_FLASHBACK_ISOLATION_WORK_ROOT:-$ROOT/target/capture-maintenance-isolation-slo/$RUN_ID}"
DATA="$WORK_ROOT/data"
SOCKET="/tmp/pgfb-m3-$RUN_ID"
RESULT_DIR="${PG_FLASHBACK_ISOLATION_RESULT_DIR:-$ROOT/target/qualification}"
RESULT_JSON="$RESULT_DIR/capture-maintenance-isolation-$RUN_ID.json"
DB=postgres
LOCK_PID=""

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
    [[ -n "$LOCK_PID" ]] && kill "$LOCK_PID" >/dev/null 2>&1
    "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1 || true
    rm -rf "$DATA" "$SOCKET"
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
pg_flashback.worker_interval_ms = 50
pg_flashback.maintenance_every_n_cycles = 1
pg_flashback.maintenance_lock_timeout_ms = 100
pg_flashback.maintenance_statement_timeout_ms = 1000
EOF
"$PG_BIN/pg_ctl" -D "$DATA" -l "$WORK_ROOT/postgresql.log" \
    -o "-p $PORT -k $SOCKET" start -w >/dev/null

q() {
    "$PSQL" -X -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAtc "$1"
}

q "CREATE EXTENSION pg_flashback;"
q "CREATE TABLE public.m3_locked(id integer PRIMARY KEY, payload text NOT NULL);
   CREATE TABLE public.m3_open(id integer PRIMARY KEY, payload text NOT NULL);"
# flashback_track deliberately requires its own transaction before any other
# write, so each tracked lifecycle is initialized in a separate psql call.
q "SELECT flashback_track('public.m3_locked');" >/dev/null
q "SELECT flashback_track('public.m3_open');" >/dev/null

# Wait for both lifecycle anchors to become active before injecting workload.
for _ in $(seq 1 100); do
    active=$(q "SELECT count(*) FROM flashback.coverage_generations
                WHERE state = 'active' AND recovery_profile = 'local_delta';")
    [[ "$active" == "2" ]] && break
    sleep 0.1
done
[[ "${active:-0}" == "2" ]] || {
    echo "FAIL: local_delta anchors did not become active" >&2
    exit 1
}

TRACKING_ID=$(q "SELECT tracking_id FROM flashback.tracked_tables
                 WHERE rel_oid = 'public.m3_locked'::regclass AND is_active;")
SLOT=$(q "SELECT flashback_effective_slot_name();")
PGAPPNAME=pgfb_m3_lifecycle_hold \
    "$PSQL" -X -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAt <<SQL >/dev/null &
BEGIN;
SELECT pg_advisory_xact_lock(358944::integer, hashint8($TRACKING_ID));
SELECT pg_sleep($HOLD_SECONDS);
COMMIT;
SQL
LOCK_PID=$!

for _ in $(seq 1 100); do
    holder=$(q "SELECT count(*)
                 FROM pg_locks l JOIN pg_stat_activity a USING (pid)
                 WHERE l.locktype = 'advisory' AND l.classid = 358944
                   AND l.objid = hashint8($TRACKING_ID)
                   AND l.granted AND a.application_name = 'pgfb_m3_lifecycle_hold';")
    [[ "$holder" == "1" ]] && break
    sleep 0.05
done
[[ "${holder:-0}" == "1" ]] || { echo "FAIL: lifecycle lock was not acquired" >&2; exit 1; }

samples=""
rows_before=$(q "SELECT count(*) FROM flashback.delta_log
                  WHERE rel_oid = 'public.m3_open'::regclass;")
rows_last_during_hold="$rows_before"
capture_progressed=false
for i in $(seq 1 "$HOLD_SECONDS"); do
    q "INSERT INTO public.m3_open
       SELECT $((i * 1000)) + g, repeat('x', 32)
       FROM generate_series(1, 100) AS g;" >/dev/null
    visible=$(q "SELECT count(*) FROM flashback.delta_log
                 WHERE rel_oid = 'public.m3_open'::regclass;")
    rows_last_during_hold="$visible"
    [[ "$visible" -gt "$rows_before" ]] && capture_progressed=true
    slot_lag=$(q "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
                  FROM pg_replication_slots WHERE slot_name = '$SLOT';")
    samples+="${samples:+,}{\"second\":$i,\"visible_rows\":$visible,\"slot_lag_bytes\":$slot_lag}"
    sleep 1
done
wait "$LOCK_PID"
LOCK_PID=""

rows_during=$(q "SELECT count(*) FROM flashback.delta_log
                  WHERE rel_oid = 'public.m3_open'::regclass;")
status="FAIL"
[[ "$capture_progressed" == true ]] && status="PASS"

cat >"$RESULT_JSON" <<EOF
{
  "run_id": "$RUN_ID",
  "status": "$status",
  "hold_seconds": $HOLD_SECONDS,
  "tracked_tables": ["public.m3_locked", "public.m3_open"],
  "locked_tracking_id": $TRACKING_ID,
  "capture_progressed_on_unblocked_table_during_hold": $capture_progressed,
  "unblocked_delta_rows_before_hold": $rows_before,
  "unblocked_delta_rows_last_sample_during_hold": $rows_last_during_hold,
  "unblocked_delta_rows_at_hold_end": $rows_during,
  "samples": [$samples],
  "p95_visibility_target_ms": 2000,
  "p95_visibility_ms": null,
  "visibility_measurement_status": "PARTIAL: one-second polling samples prove progress but do not provide per-commit p95 latency."
}
EOF

echo "M3 isolation qualification: $status ($RESULT_JSON)"
[[ "$status" == "PASS" ]]
