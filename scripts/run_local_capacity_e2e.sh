#!/usr/bin/env bash
# Adversarial local capacity / write-stall E2E against a live PostgreSQL.
# Exercises lock-timeout residue, budget rejection before CTAS, and bounded
# success. Intended for a development instance that already has pg_flashback
# in shared_preload_libraries.
set -euo pipefail

BINDIR="${1:-}"
if [[ -z "$BINDIR" ]]; then
    for cand in /usr/local/pgsql-17/bin /usr/pgsql-17/bin /usr/bin; do
        if [[ -x "$cand/psql" && -x "$cand/pg_ctl" ]]; then BINDIR="$cand"; break; fi
    done
fi
[[ -n "$BINDIR" ]] || { echo "FAIL: psql/pg_ctl not found"; exit 1; }

PSQL="$BINDIR/psql"
DB="fb_capacity_e2e_$$"
export PGHOST="${PGHOST:-/tmp}"
export PGPORT="${PGPORT:-5432}"

q() { "$PSQL" -v ON_ERROR_STOP=1 -d "$DB" -Atqc "$1"; }
qp() { "$PSQL" -v ON_ERROR_STOP=1 -d postgres -Atqc "$1"; }

cleanup() {
    local rc=$?
    qp "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='$DB' AND pid <> pg_backend_pid()" >/dev/null 2>&1 || true
    qp "DROP DATABASE IF EXISTS $DB WITH (FORCE)" >/dev/null 2>&1 || true
    exit "$rc"
}
trap cleanup EXIT

echo "━━━ local capacity E2E ━━━"
qp "DROP DATABASE IF EXISTS $DB WITH (FORCE)" >/dev/null
qp "CREATE DATABASE $DB" >/dev/null
q "CREATE EXTENSION pg_flashback" >/dev/null
q "ALTER SYSTEM SET pg_flashback.local_max_snapshot_bytes='8GB'" >/dev/null || true
q "SELECT set_config('pg_flashback.local_max_snapshot_bytes','8GB', false)" >/dev/null
q "SELECT set_config('pg_flashback.local_max_restore_peak_bytes','16GB', false)" >/dev/null
q "SELECT set_config('pg_flashback.local_min_filesystem_bytes','64MB', false)" >/dev/null
q "SELECT set_config('pg_flashback.local_safety_reserve_bytes','16MB', false)" >/dev/null
q "SELECT set_config('pg_flashback.capture_mode','wal', false)" >/dev/null

q "CREATE TABLE capacity_lock(id int PRIMARY KEY, payload text);
   INSERT INTO capacity_lock VALUES (1,'seed')" >/dev/null

echo "── budget rejection before track copy ──"
q "SELECT set_config('pg_flashback.local_max_snapshot_bytes','64', false)" >/dev/null
TRACK_RC=0
q "SELECT flashback_admit_local_capacity('capacity_lock'::regclass, 'track')" \
    >/tmp/pg_flashback_capacity_track_reject.out 2>&1 || TRACK_RC=$?
[[ "$TRACK_RC" != "0" ]] || { echo "FAIL: undersized track budget was admitted"; exit 1; }
grep -q "capacity admission failed" /tmp/pg_flashback_capacity_track_reject.out
echo "  ok: track rejected before copy"

echo "── lock timeout leaves no snapshot residue ──"
q "SELECT set_config('pg_flashback.local_max_snapshot_bytes','8GB', false)" >/dev/null
q "SELECT set_config('pg_flashback.local_boundary_write_stall_ms','200', false)" >/dev/null
# Hold ACCESS EXCLUSIVE in a background session while a waiter times out.
"$PSQL" -d "$DB" -v ON_ERROR_STOP=1 -c "
BEGIN;
LOCK TABLE capacity_lock IN ACCESS EXCLUSIVE MODE;
SELECT pg_sleep(5);
COMMIT;
" >/tmp/pg_flashback_capacity_lock_holder.out 2>&1 &
HOLDER_PID=$!
sleep 0.3
TRACK_RC=0
"$PSQL" -d "$DB" -v ON_ERROR_STOP=1 -c "
SELECT set_config('pg_flashback.capture_mode','wal', false);
SELECT set_config('pg_flashback.local_max_snapshot_bytes','8GB', false);
SELECT set_config('pg_flashback.local_max_restore_peak_bytes','16GB', false);
SELECT set_config('pg_flashback.local_min_filesystem_bytes','64MB', false);
SELECT set_config('pg_flashback.local_boundary_write_stall_ms','200', false);
SELECT flashback_apply_local_boundary_lock_timeout();
LOCK TABLE capacity_lock IN SHARE ROW EXCLUSIVE MODE;
" >/tmp/pg_flashback_capacity_lock_waiter.out 2>&1 || TRACK_RC=$?
wait "$HOLDER_PID" || true
[[ "$TRACK_RC" != "0" ]] || { echo "FAIL: lock waiter did not time out"; exit 1; }
grep -Eqi 'lock (timeout|not available)|write stall|canceling statement due to lock timeout' \
    /tmp/pg_flashback_capacity_lock_waiter.out
SNAP_COUNT=$(q "SELECT count(*) FROM flashback.snapshots")
GEN_COUNT=$(q "SELECT count(*) FROM flashback.coverage_generations")
[[ "$SNAP_COUNT" == "0" ]] || { echo "FAIL: snapshot residue after lock timeout"; exit 1; }
[[ "$GEN_COUNT" == "0" ]] || { echo "FAIL: generation residue after lock timeout"; exit 1; }
echo "  ok: lock timeout left no payload/generation residue"

echo "── privileged override is visible ──"
q "SELECT set_config('pg_flashback.local_max_snapshot_bytes','64', false)" >/dev/null
q "SELECT set_config('pg_flashback.local_capacity_override','on', false)" >/dev/null
q "SELECT flashback_admit_local_capacity('capacity_lock'::regclass, 'track')" >/dev/null
OVERRIDE=$(q "SELECT capacity_override FROM flashback_advise('capacity_lock'::regclass)")
[[ "$OVERRIDE" == "t" ]] || { echo "FAIL: override not visible in advise"; exit 1; }
q "SELECT set_config('pg_flashback.local_capacity_override','off', false)" >/dev/null
echo "  ok: override visible"

echo "── bounded success path ──"
q "SELECT set_config('pg_flashback.local_max_snapshot_bytes','8GB', false)" >/dev/null
q "SELECT set_config('pg_flashback.local_boundary_write_stall_ms','60000', false)" >/dev/null
q "SELECT flashback_admit_local_capacity('capacity_lock'::regclass, 'track')" >/dev/null
q "SELECT flashback_admit_local_capacity('capacity_lock'::regclass, 'restore')" >/dev/null
echo "  ok: bounded track/restore admission succeeded"

echo "╔══════════════════════════════════════════╗"
echo "║   LOCAL CAPACITY E2E: PASSED ✔           ║"
echo "╚══════════════════════════════════════════╝"
