#!/usr/bin/env bash
# Maintain lifecycle + storage-budget freeze E2E against a live PostgreSQL.
#
# Exercises:
#   1. flashback_maintain_plan -> not_needed on a fresh healthy lifecycle.
#   2. flashback_maintain_begin -> flashback_maintain_finalize two-phase
#      reanchor: predecessor generation ends up sealed (not deleted), successor
#      active, table data intact throughout.
#   3. flashback_storage_freeze_scan() freezing a lifecycle once its retained
#      payload is over a (deliberately tiny, test-only) hard budget: the
#      lifecycle's health becomes coverage_frozen_storage_exhausted, its
#      storage metrics report status=blocked, flashback_maintain_plan reports
#      maintenance_status=blocked, and other lifecycles are unaffected.
#
# flashback_track() admission requires the target database to already be
# listed in pg_flashback.target_databases/target_database AND for that
# change to have taken effect via a PostgreSQL restart (see
# flashback_worker_readiness()) — a throwaway CREATE DATABASE per run can
# never satisfy that, so this operates against an already-admitted database
# (default: the caller's PGDATABASE, else 'postgres') using unique,
# self-cleaning table names instead of a unique database. Requires
# pg_flashback already in shared_preload_libraries with wal_level=logical
# and the target database admitted (restart already done).
set -euo pipefail

BINDIR="${1:-}"
if [[ -z "$BINDIR" ]]; then
    for cand in /usr/local/pgsql-17/bin /usr/pgsql-17/bin /usr/bin; do
        if [[ -x "$cand/psql" && -x "$cand/pg_ctl" ]]; then BINDIR="$cand"; break; fi
    done
fi
[[ -n "$BINDIR" ]] || { echo "FAIL: psql/pg_ctl not found"; exit 1; }

PSQL="$BINDIR/psql"
DB="${PGDATABASE:-postgres}"
export PGHOST="${PGHOST:-/tmp}"
export PGPORT="${PGPORT:-5432}"

q() { "$PSQL" -v ON_ERROR_STOP=1 -d "$DB" -Atqc "$1"; }
qp() { "$PSQL" -v ON_ERROR_STOP=1 -d postgres -Atqc "$1"; }

cleanup() {
    local rc=$?
    qp "ALTER SYSTEM RESET pg_flashback.local_max_retained_payload_bytes" >/dev/null 2>&1 || true
    qp "ALTER SYSTEM RESET pg_flashback.local_retained_payload_soft_bytes" >/dev/null 2>&1 || true
    qp "SELECT pg_reload_conf()" >/dev/null 2>&1 || true
    q "SELECT flashback_unprotect('public.ml_main')" >/dev/null 2>&1 || true
    q "SELECT flashback_unprotect('public.ml_other')" >/dev/null 2>&1 || true
    q "DROP TABLE IF EXISTS public.ml_main, public.ml_other CASCADE" >/dev/null 2>&1 || true
    exit "$rc"
}
trap cleanup EXIT

wait_healthy() {
    local table=$1 h=""
    for _ in $(seq 1 240); do
        h=$(q "SELECT flashback_lifecycle_health('$table')")
        [[ "$h" == "healthy" ]] && return 0
        sleep 0.25
    done
    return 1
}

echo "--- maintain lifecycle E2E (database: $DB) ---"
q "SELECT flashback_unprotect('public.ml_main')" >/dev/null 2>&1 || true
q "SELECT flashback_unprotect('public.ml_other')" >/dev/null 2>&1 || true
q "DROP TABLE IF EXISTS public.ml_main, public.ml_other CASCADE" >/dev/null
q "CREATE EXTENSION IF NOT EXISTS pg_flashback" >/dev/null
q "SELECT set_config('pg_flashback.local_max_snapshot_bytes','256MB', false)" >/dev/null
q "SELECT set_config('pg_flashback.local_max_restore_peak_bytes','512MB', false)" >/dev/null
q "SELECT set_config('pg_flashback.local_min_filesystem_bytes','64MB', false)" >/dev/null
q "SELECT set_config('pg_flashback.local_safety_reserve_bytes','16MB', false)" >/dev/null

echo "-- setup: two independent lifecycles --"
# flashback_track() must be the first write in its own transaction; psql -c
# runs a whole multi-statement string as one implicit transaction, so the
# CREATE TABLE/INSERT and the track() call must be separate q invocations.
q "CREATE TABLE ml_main(id int PRIMARY KEY, v text NOT NULL);
   INSERT INTO ml_main SELECT g, 'x'||g FROM generate_series(1,200) g;" >/dev/null
q "SELECT flashback_track('public.ml_main');" >/dev/null
q "CREATE TABLE ml_other(id int PRIMARY KEY, v text NOT NULL);
   INSERT INTO ml_other VALUES (1,'a');" >/dev/null
q "SELECT flashback_track('public.ml_other');" >/dev/null
wait_healthy public.ml_main || { echo "FAIL: ml_main not healthy after protect"; exit 1; }
wait_healthy public.ml_other || { echo "FAIL: ml_other not healthy after protect"; exit 1; }
echo "  ok: both lifecycles healthy"

echo "-- 1. plan says not_needed on a healthy fresh lifecycle --"
PLAN=$(q "SELECT flashback_maintain_plan('public.ml_main')")
[[ "$(printf '%s' "$PLAN" | jq -r '.status')" == "ok" ]] || { echo "FAIL: plan status not ok: $PLAN"; exit 1; }
[[ "$(printf '%s' "$PLAN" | jq -r '.maintenance_status')" == "not_needed" ]] \
    || { echo "FAIL: expected maintenance_status=not_needed, got: $PLAN"; exit 1; }
echo "  ok: maintenance_status=not_needed"

echo "-- 2. begin -> finalize two-phase reanchor --"
PRED_GEN=$(q "SELECT generation_id FROM flashback.coverage_generations
              WHERE tracking_id = (SELECT tracking_id FROM flashback.tracked_tables
                                    WHERE table_name='ml_main' AND is_active)
                AND state = 'active'")
BEGIN_JSON=$(q "SELECT flashback_maintain_begin('public.ml_main')")
OP=$(printf '%s' "$BEGIN_JSON" | jq -r '.operation_id')
NEWGEN=$(printf '%s' "$BEGIN_JSON" | jq -r '.new_generation_id')
[[ -n "$OP" && "$OP" != "null" ]] || { echo "FAIL: maintain_begin did not return operation_id: $BEGIN_JSON"; exit 1; }
[[ "$NEWGEN" != "$PRED_GEN" ]] || { echo "FAIL: maintain_begin did not create a new generation"; exit 1; }

FSTATE=""
for _ in $(seq 1 120); do
    FINAL_JSON=$(q "SELECT flashback_maintain_finalize($OP)")
    FSTATE=$(printf '%s' "$FINAL_JSON" | jq -r '.status')
    [[ "$FSTATE" == "sealed" ]] && break
    sleep 1
done
[[ "$FSTATE" == "sealed" ]] || { echo "FAIL: maintain never finalized to sealed (state=$FSTATE)"; exit 1; }
[[ "$(q "SELECT state FROM flashback.coverage_generations WHERE generation_id=$PRED_GEN")" == "sealed" ]] \
    || { echo "FAIL: predecessor generation $PRED_GEN not sealed"; exit 1; }
[[ "$(q "SELECT state FROM flashback.coverage_generations WHERE generation_id=$NEWGEN")" == "active" ]] \
    || { echo "FAIL: successor generation $NEWGEN not active"; exit 1; }
# Predecessor payload is retained, not deleted.
[[ "$(q "SELECT to_regclass(snapshot_table) IS NOT NULL FROM flashback.coverage_generations cg
         JOIN flashback.snapshots s ON s.snapshot_id = cg.boundary_snapshot_id AND s.tracking_id = cg.tracking_id
         WHERE cg.generation_id = $PRED_GEN")" == "t" ]] \
    || { echo "FAIL: predecessor snapshot payload was removed, expected retained"; exit 1; }
[[ "$(q "SELECT count(*) FROM public.ml_main")" == "200" ]] || { echo "FAIL: data lost across maintain"; exit 1; }
echo "  ok: predecessor sealed+retained, successor active, data intact"

echo "-- 3. storage budget freeze (deliberately tiny hard budget) --"
# The hard budget is a single cluster-wide GUC, not per-lifecycle, so it must
# be picked between ml_other's and ml_main's *current* retained payload sizes
# (ml_main is larger here: it now carries a sealed predecessor generation
# plus its active successor from step 2) rather than an arbitrary constant —
# otherwise a too-small constant freezes both lifecycles and a too-large one
# freezes neither, and either would make step "other lifecycles continue
# unaffected" below meaningless.
OTHER_BYTES=$(q "SELECT COALESCE((flashback_lifecycle_storage_metrics('public.ml_other')->>'retained_local_payload_bytes')::bigint, 0)")
MAIN_BYTES=$(q "SELECT COALESCE((flashback_lifecycle_storage_metrics('public.ml_main')->>'retained_local_payload_bytes')::bigint, 0)")
[[ "$MAIN_BYTES" -gt "$OTHER_BYTES" ]] \
    || { echo "FAIL: expected ml_main ($MAIN_BYTES bytes) retained payload > ml_other ($OTHER_BYTES bytes) after reanchor"; exit 1; }
THRESHOLD=$(( (OTHER_BYTES + MAIN_BYTES) / 2 ))
echo "  ml_other=${OTHER_BYTES}B ml_main=${MAIN_BYTES}B -> hard budget=${THRESHOLD}B"
qp "ALTER SYSTEM SET pg_flashback.local_max_retained_payload_bytes = '${THRESHOLD}B'" >/dev/null
qp "SELECT pg_reload_conf()" >/dev/null
sleep 0.2

# The live delta worker also calls flashback_storage_freeze_scan() every WAL
# consume tick (see consume_wal_changes() in src/storage/worker.rs), so it
# can win the race and freeze ml_main before this explicit call runs — that
# is the intended "worker must not silently skip" behavior, not a failure.
# Poll flashback_lifecycle_storage_metrics()'s frozen flag (true whichever
# caller froze it) instead of asserting on one explicit scan's return count.
FROZEN_OK=0
for _ in $(seq 1 40); do
    q "SELECT flashback_storage_freeze_scan()" >/dev/null
    if [[ "$(q "SELECT flashback_lifecycle_storage_metrics('public.ml_main')" | jq -r '.frozen')" == "true" ]]; then
        FROZEN_OK=1
        break
    fi
    sleep 0.25
done
[[ "$FROZEN_OK" -eq 1 ]] || { echo "FAIL: ml_main never froze under a 1kB hard budget"; exit 1; }

METRICS=$(q "SELECT flashback_lifecycle_storage_metrics('public.ml_main')")
[[ "$(printf '%s' "$METRICS" | jq -r '.status')" == "blocked" ]] \
    || { echo "FAIL: ml_main storage metrics not blocked after freeze: $METRICS"; exit 1; }
HEALTH=$(q "SELECT health FROM flashback_health() WHERE table_name='public.ml_main'")
[[ "$HEALTH" == "coverage_frozen_storage_exhausted" ]] \
    || { echo "FAIL: ml_main health not coverage_frozen_storage_exhausted (got $HEALTH)"; exit 1; }
PLAN2=$(q "SELECT flashback_maintain_plan('public.ml_main')")
[[ "$(printf '%s' "$PLAN2" | jq -r '.maintenance_status')" == "blocked" ]] \
    || { echo "FAIL: maintain_plan not blocked after freeze: $PLAN2"; exit 1; }
echo "  ok: ml_main storage=blocked, health=coverage_frozen_storage_exhausted, maintain_plan=blocked"

echo "-- recover targeting beyond the freeze point is rejected --"
q "DROP TABLE public.ml_main"
DROP_RESTORABLE=""
for _ in $(seq 1 120); do
    DROP_RESTORABLE=$(q "SELECT status FROM flashback_disaster_points('public.ml_main', interval '1 day') WHERE event_type='DROP' ORDER BY disaster_commit_lsn DESC LIMIT 1")
    [[ -n "$DROP_RESTORABLE" ]] && break
    sleep 0.25
done
[[ "$DROP_RESTORABLE" != "restorable" ]] \
    || { echo "FAIL: DROP after freeze point unexpectedly restorable"; exit 1; }
echo "  ok: DROP beyond the freeze point is non_restorable ($DROP_RESTORABLE)"

echo "-- other lifecycles continue unaffected --"
wait_healthy public.ml_other || { echo "FAIL: ml_other health disturbed by ml_main freeze"; exit 1; }
q "INSERT INTO public.ml_other VALUES (2,'b')" >/dev/null
wait_healthy public.ml_other || { echo "FAIL: ml_other unhealthy after DML post-freeze"; exit 1; }
echo "  ok: ml_other unaffected"

echo "-- maintenance worker cadence --"
INITIAL_PID=$(q "SELECT pid FROM pg_stat_activity WHERE backend_type = 'pg_flashback maintenance worker' LIMIT 1")
[[ -n "$INITIAL_PID" ]] || { echo "FAIL: maintenance worker not running"; exit 1; }

# Find the active postgresql.log for the worker
LOG_PATH="${PGFB_LOG_FILE:-}"
if [[ -n "$LOG_PATH" && -f "$LOG_PATH" ]]; then
    ERR_BEFORE=$(grep -c "flashback_internal_checkpoint" "$LOG_PATH" || true)
else
    ERR_BEFORE=0
fi

sleep 2
CURRENT_PID=$(q "SELECT pid FROM pg_stat_activity WHERE backend_type = 'pg_flashback maintenance worker' LIMIT 1")
[[ "$CURRENT_PID" == "$INITIAL_PID" ]] || { echo "FAIL: maintenance worker restarted (crashed?) $INITIAL_PID -> $CURRENT_PID"; exit 1; }

if [[ -n "$LOG_PATH" && -f "$LOG_PATH" ]]; then
    ERR_AFTER=$(grep -c "flashback_internal_checkpoint" "$LOG_PATH" || true)
    if [[ "$ERR_AFTER" -gt "$ERR_BEFORE" ]]; then
        echo "FAIL: checkpoint exception found in postgresql log!"
        exit 1
    fi
fi
echo "  ok: maintenance worker survived"

echo "PASS: maintain lifecycle E2E"
