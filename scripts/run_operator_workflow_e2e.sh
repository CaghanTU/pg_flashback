#!/usr/bin/env bash
# Real-session E2E for the supported local operator workflow:
# doctor → advise → track → healthy → DROP → disaster discovery → restore → healthy
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PG_CONFIG="${PG_CONFIG:-/usr/local/pgsql-17/bin/pg_config}"
# shellcheck source=scripts/qualification_provenance.sh
source "$ROOT/scripts/qualification_provenance.sh"
qualification_provenance_init "$ROOT" "$PG_CONFIG"
PG_BIN="$("$PG_CONFIG" --bindir)"
SHARE_DIR="$("$PG_CONFIG" --sharedir)"
PSQL="$PG_BIN/psql"
CTL="$ROOT/scripts/pg_flashbackctl"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
PORT="${PG_FLASHBACK_OPERATOR_PORT:-28947}"
WORK_ROOT="${PG_FLASHBACK_OPERATOR_WORK_ROOT:-$ROOT/target/operator-workflow-e2e/$RUN_ID}"
DATA="$WORK_ROOT/data"
SOCKET="/tmp/pgfb-op-$RUN_ID"
RESULT_DIR="${PG_FLASHBACK_OPERATOR_RESULT_DIR:-$ROOT/target/qualification}"
RESULT_JSON="$RESULT_DIR/operator-workflow-$RUN_ID.json"
DB=postgres
export PGHOST="$SOCKET" PGPORT="$PORT" PGDATABASE="$DB"

cleanup() {
    local rc=$?
    set +e
    "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1 || true
    rm -rf "$DATA" "$SOCKET"
    exit "$rc"
}
trap cleanup EXIT

[[ -x "$CTL" ]] || { echo "FAIL: $CTL missing/executable" >&2; exit 2; }
[[ -f "$SHARE_DIR/extension/pg_flashback.control" ]] || {
    echo "FAIL: install extension first" >&2; exit 2; }

mkdir -p "$WORK_ROOT" "$SOCKET" "$RESULT_DIR"
"$PG_BIN/initdb" -D "$DATA" --locale=C.UTF-8 -A trust >/dev/null
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
pg_flashback.target_database = postgres
pg_flashback.local_max_snapshot_bytes = 8GB
pg_flashback.local_max_restore_peak_bytes = 16GB
pg_flashback.local_min_filesystem_bytes = 64MB
pg_flashback.local_safety_reserve_bytes = 16MB
EOF
"$PG_BIN/pg_ctl" -D "$DATA" -l "$WORK_ROOT/postgresql.log" start -w >/dev/null

q() { "$PSQL" -X -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAtc "$1"; }

q "CREATE EXTENSION pg_flashback;"
# Wait for workers
for _ in $(seq 1 100); do
    state=$(q "SELECT admission_state FROM flashback_worker_readiness();")
    [[ "$state" == "ready" ]] && break
    sleep 0.1
done
[[ "$state" == "ready" ]] || { echo "FAIL: workers not ready ($state)" >&2; exit 1; }

"$CTL" doctor >/dev/null
doctor_rc=0
"$CTL" doctor >/tmp/pgfb-doctor.out 2>&1 || doctor_rc=$?
[[ "$doctor_rc" == "0" ]] || { echo "FAIL: doctor exit $doctor_rc"; cat /tmp/pgfb-doctor.out; exit 1; }

q "CREATE TABLE public.op_flow(id integer PRIMARY KEY, payload text NOT NULL);
   CREATE TABLE public.\"Weird Name\"(id integer PRIMARY KEY, note text);
   CREATE TABLE public.medium_op(id bigint PRIMARY KEY, payload text NOT NULL);"
q "INSERT INTO public.op_flow VALUES (1,'a'),(2,'b');"
q "INSERT INTO public.medium_op SELECT g, repeat(md5(g::text), 8)
   FROM generate_series(1,5000) g;"

"$CTL" advise public.op_flow | tee "$WORK_ROOT/advise.txt" >/dev/null
grep -Eq 'local profile|backup profile|configure local' "$WORK_ROOT/advise.txt"

"$CTL" track public.op_flow
"$CTL" track 'public."Weird Name"'
"$CTL" track public.medium_op

health=$(q "SELECT string_agg(health, ',' ORDER BY table_name)
            FROM flashback_health()
            WHERE table_name IN ('public.op_flow','public.\"Weird Name\"','public.medium_op');")
[[ "$health" == "healthy,healthy,healthy" ]] || {
    echo "FAIL: expected all healthy, got $health" >&2
    exit 1
}

q "INSERT INTO public.op_flow VALUES (3,'c');
   UPDATE public.op_flow SET payload='b2' WHERE id=2;
   DELETE FROM public.op_flow WHERE id=1;"
# Wait for coverage
for _ in $(seq 1 200); do
    n=$(q "SELECT count(*) FROM flashback.delta_log
           WHERE rel_oid='public.op_flow'::regclass;")
    (( n >= 3 )) && break
    sleep 0.05
done

fp=$(q "SELECT md5(string_agg(id::text||':'||payload, ',' ORDER BY id)) FROM public.op_flow;")
owner=$(q "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='public.op_flow'::regclass;")
acl=$(q "SELECT COALESCE(array_to_string(relacl,','),'') FROM pg_class WHERE oid='public.op_flow'::regclass;")

# Same-transaction DML+DROP
q "BEGIN;
   INSERT INTO public.op_flow VALUES (99,'gone');
   DROP TABLE public.op_flow;
   COMMIT;"

# Discover disaster point (no pre-recorded LSN)
safe_lsn=""
for _ in $(seq 1 200); do
    q "SELECT flashback_consume_wal(8192);" >/dev/null || true
    safe_lsn=$(q "SELECT safe_target_lsn::text
                  FROM flashback_disaster_points('public.op_flow', interval '1 hour')
                  WHERE event_type='DROP' AND status='restorable'
                  ORDER BY disaster_time DESC LIMIT 1;")
    [[ -n "$safe_lsn" ]] && break
    sleep 0.05
done
[[ -n "$safe_lsn" ]] || {
    echo "FAIL: disaster_points did not yield a restorable DROP target" >&2
    q "SELECT * FROM flashback_disaster_points('public.op_flow', interval '1 hour');" >&2 || true
    exit 1
}

"$CTL" disasters public.op_flow '1 hour' >"$WORK_ROOT/disasters.txt"
grep -q "$safe_lsn" "$WORK_ROOT/disasters.txt"

# Restore requires --yes
restore_rc=0
"$CTL" restore public.op_flow "$safe_lsn" >/tmp/pgfb-restore-deny.out 2>&1 || restore_rc=$?
[[ "$restore_rc" != "0" ]] || { echo "FAIL: restore without --yes succeeded" >&2; exit 1; }

"$CTL" restore public.op_flow "$safe_lsn" --yes

[[ "$(q "SELECT to_regclass('public.op_flow') IS NOT NULL;")" == "t" ]]
fp2=$(q "SELECT md5(string_agg(id::text||':'||payload, ',' ORDER BY id)) FROM public.op_flow;")
[[ "$fp2" == "$fp" ]] || { echo "FAIL: fingerprint mismatch after restore ($fp2 vs $fp)" >&2; exit 1; }
[[ "$(q "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='public.op_flow'::regclass;")" == "$owner" ]]
[[ "$(q "SELECT COALESCE(array_to_string(relacl,','),'') FROM pg_class WHERE oid='public.op_flow'::regclass;")" == "$acl" ]]
[[ "$(q "SELECT count(*) FROM pg_index WHERE indrelid='public.op_flow'::regclass AND indisprimary;")" == "1" ]]

# Repeated DROP/recreate cycle via disasters API
for cycle in 1 2; do
    q "UPDATE public.op_flow SET payload='c$cycle' WHERE id=2;" >/dev/null
    for _ in $(seq 1 100); do
        q "SELECT flashback_consume_wal(8192);" >/dev/null || true
        covered=$(q "SELECT EXISTS(
            SELECT 1 FROM flashback.delta_log
            WHERE rel_oid='public.op_flow'::regclass AND event_type='UPDATE'
              AND (new_data->>'payload')='c$cycle' AND commit_lsn IS NOT NULL);")
        [[ "$covered" == "t" ]] && break
        sleep 0.05
    done
    fp_c=$(q "SELECT md5(string_agg(id::text||':'||payload, ',' ORDER BY id)) FROM public.op_flow;")
    q "DROP TABLE public.op_flow;" >/dev/null
    safe=""
    for _ in $(seq 1 200); do
        q "SELECT flashback_consume_wal(8192);" >/dev/null || true
        safe=$(q "SELECT safe_target_lsn::text
                  FROM flashback_disaster_points('public.op_flow', interval '1 hour')
                  WHERE event_type='DROP' AND status='restorable'
                  ORDER BY disaster_time DESC LIMIT 1;")
        [[ -n "$safe" ]] && break
        sleep 0.05
    done
    [[ -n "$safe" ]] || { echo "FAIL: cycle $cycle disaster discovery failed" >&2; exit 1; }
    "$CTL" restore public.op_flow "$safe" --yes
    fp_r=$(q "SELECT md5(string_agg(id::text||':'||payload, ',' ORDER BY id)) FROM public.op_flow;")
    [[ "$fp_r" == "$fp_c" ]] || { echo "FAIL: cycle $cycle fingerprint mismatch" >&2; exit 1; }
done

# Capacity rejection before lock: tiny budgets
q "ALTER SYSTEM SET pg_flashback.local_max_snapshot_bytes = '1kB';"
q "ALTER SYSTEM SET pg_flashback.local_max_restore_peak_bytes = '1kB';"
q "SELECT pg_reload_conf();" >/dev/null
cap_rc=0
q "SELECT flashback_admit_local_capacity('public.medium_op'::regclass, 'restore');" \
    >/tmp/pgfb-cap.err 2>&1 || cap_rc=$?
[[ "$cap_rc" != "0" ]] || { echo "FAIL: capacity rejection did not fire" >&2; cat /tmp/pgfb-cap.err >&2; exit 1; }
grep -Eiq 'capacity|budget|filesystem|snapshot|restore_peak|admit|exceeds' /tmp/pgfb-cap.err
q "ALTER SYSTEM RESET pg_flashback.local_max_snapshot_bytes;"
q "ALTER SYSTEM RESET pg_flashback.local_max_restore_peak_bytes;"
q "SELECT pg_reload_conf();" >/dev/null

# Quoted identifier still healthy
[[ "$(q "SELECT health FROM flashback_health() WHERE table_name='public.\"Weird Name\"';")" == "healthy" ]]

# Cleanup lifecycles
"$PSQL" -X -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAtc \
    "SELECT flashback_untrack('public.op_flow');" >/dev/null
"$PSQL" -X -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAtc \
    "SELECT flashback_untrack('public.\"Weird Name\"');" >/dev/null
"$PSQL" -X -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAtc \
    "SELECT flashback_untrack('public.medium_op');" >/dev/null
[[ "$(q "SELECT count(*) FROM flashback.tracked_tables WHERE is_active;")" == "0" ]]
[[ "$(q "SELECT count(*) FROM pg_replication_slots WHERE database=current_database();")" == "0" ]] \
    || q "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database=current_database();" >/dev/null || true

status=PASS
cat >"$RESULT_JSON" <<EOF
{
  "run_id": "$RUN_ID",
$(qualification_provenance_json "$(date -u +%Y-%m-%dT%H:%M:%SZ)"),
  "status": "$status",
  "safe_target_lsn": "$safe_lsn",
  "workflow": ["doctor","advise","track","healthy","drop","disasters","restore","successor_healthy"]
}
EOF
echo "Operator workflow E2E: $status ($RESULT_JSON)"
[[ "$status" == "PASS" ]]
