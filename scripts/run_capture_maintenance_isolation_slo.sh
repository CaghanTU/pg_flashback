#!/usr/bin/env bash
# Development qualification for M3 capture/maintenance isolation.
#
# Starts an isolated PostgreSQL cluster, tracks two local_delta tables, then
# holds one lifecycle lock while the other table receives WAL. Capture and
# maintenance deliberately still share one background-worker loop: its cadence,
# timeouts, and maintenance attempts are configured below. This is therefore an
# isolation measurement, not evidence that they run in separate workers.
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
HOLD_SECONDS="${HOLD_SECONDS:-${PG_FLASHBACK_ISOLATION_HOLD_SECONDS:-30}}"
COMMIT_SAMPLES="${PG_FLASHBACK_ISOLATION_COMMIT_SAMPLES:-200}"
POLL_SECONDS="${PG_FLASHBACK_ISOLATION_POLL_SECONDS:-0.02}"
DRAIN_TIMEOUT_SECONDS="${PG_FLASHBACK_ISOLATION_DRAIN_TIMEOUT_SECONDS:-30}"
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
[[ "$HOLD_SECONDS" =~ ^[0-9]+$ && "$HOLD_SECONDS" -gt 0 ]] || {
    echo "FAIL: HOLD_SECONDS must be a positive integer" >&2; exit 2; }
[[ "$COMMIT_SAMPLES" =~ ^[0-9]+$ && "$COMMIT_SAMPLES" -ge 200 ]] || {
    echo "FAIL: PG_FLASHBACK_ISOLATION_COMMIT_SAMPLES must be at least 200" >&2; exit 2; }
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

monotonic_ns() {
    python3 -c 'import time; print(time.monotonic_ns())'
}

rows_before=$(q "SELECT count(*) FROM flashback.delta_log
                  WHERE rel_oid = 'public.m3_open'::regclass;")
start_lag=$(q "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
               FROM pg_replication_slots WHERE slot_name = '$SLOT';")
declare -a ACK_NS VISIBLE_NS
emitted=0
for i in $(seq 1 "$COMMIT_SAMPLES"); do
    # One row and one psql invocation make this a distinct committed
    # transaction. Record monotonic time after psql returns its commit ack.
    q "INSERT INTO public.m3_open VALUES ($i, repeat('x', 32));" >/dev/null
    ACK_NS[$i]=$(monotonic_ns)
    emitted=$i
done

# delta_log's new_data identifies each insert. Poll at 20 ms and use its
# contiguous inserted-id watermark to stamp the first observation for every
# committed transaction. This measures commit acknowledgement to first
# observable capture, with poll-resolution uncertainty only.
visible_through=0
polls=0
# Keep every observation inside the actual advisory-lock hold. The lock holder
# may finish before a slow machine emits all samples, in which case this run
# truthfully reports only the commits observed during the hold.
while (( visible_through < emitted )); do
    holder=$(q "SELECT count(*)
                 FROM pg_locks l JOIN pg_stat_activity a USING (pid)
                 WHERE l.locktype = 'advisory' AND l.classid = 358944
                   AND l.objid = hashint8($TRACKING_ID)
                   AND l.granted AND a.application_name = 'pgfb_m3_lifecycle_hold';")
    [[ "$holder" == "1" ]] || break
    visible=$(q "SELECT COALESCE(max((new_data->>'id')::integer), 0)
                 FROM flashback.delta_log
                 WHERE rel_oid = 'public.m3_open'::regclass
                   AND event_type = 'INSERT'
                   AND (new_data->>'id')::integer BETWEEN 1 AND $emitted;")
    if (( visible > visible_through )); then
        now_ns=$(monotonic_ns)
        (( visible > emitted )) && visible=$emitted
        for i in $(seq $((visible_through + 1)) "$visible"); do
            VISIBLE_NS[$i]=$now_ns
        done
        visible_through=$visible
    fi
    polls=$((polls + 1))
    sleep "$POLL_SECONDS"
done
rows_last_during_hold=$(q "SELECT count(*) FROM flashback.delta_log
                           WHERE rel_oid = 'public.m3_open'::regclass;")
capture_progressed=false
(( visible_through > 0 )) && capture_progressed=true

latencies_file="$WORK_ROOT/visibility-latencies-ms.txt"
samples_file="$WORK_ROOT/visibility-samples.jsonl"
: >"$latencies_file"
: >"$samples_file"
for i in $(seq 1 "$emitted"); do
    if [[ -n "${VISIBLE_NS[$i]:-}" ]]; then
        latency_ms=$(( (VISIBLE_NS[$i] - ACK_NS[$i]) / 1000000 ))
        printf '%s\n' "$latency_ms" >>"$latencies_file"
        printf '{"id":%s,"commit_ack_monotonic_ns":%s,"first_visible_monotonic_ns":%s,"visibility_latency_ms":%s}\n' \
            "$i" "${ACK_NS[$i]}" "${VISIBLE_NS[$i]}" "$latency_ms" >>"$samples_file"
    fi
done
observed_samples=$(wc -l <"$latencies_file" | tr -d ' ')
visibility_samples_json=$(paste -sd, "$samples_file")
if (( observed_samples > 0 )); then
    sort -n "$latencies_file" -o "$latencies_file"
    p50=$(awk -v n="$observed_samples" 'NR == int(n * .50 + .999999) { print; exit }' "$latencies_file")
    p95=$(awk -v n="$observed_samples" 'NR == int(n * .95 + .999999) { print; exit }' "$latencies_file")
    p99=$(awk -v n="$observed_samples" 'NR == int(n * .99 + .999999) { print; exit }' "$latencies_file")
    max_ms=$(awk 'END { print }' "$latencies_file")
else
    p50=null; p95=null; p99=null; max_ms=null
fi

wait "$LOCK_PID"
LOCK_PID=""

rows_during=$(q "SELECT count(*) FROM flashback.delta_log
                  WHERE rel_oid = 'public.m3_open'::regclass;")
drain_deadline=$(( $(date +%s) + DRAIN_TIMEOUT_SECONDS ))
drain_target=$(( start_lag + 65536 ))
(( drain_target < 65536 )) && drain_target=65536
drain_ok=false
while (( $(date +%s) <= drain_deadline )); do
    q "SELECT flashback_consume_wal(8192);" >/dev/null || true
    # Catch-up means confirmed_flush has reached the last observed open-table commit.
    catchup=$(q "SELECT CASE
        WHEN max(d.commit_lsn) IS NULL THEN false
        WHEN s.confirmed_flush_lsn >= max(d.commit_lsn) THEN true
        ELSE false
      END
      FROM flashback.delta_log d
      CROSS JOIN pg_replication_slots s
      WHERE d.rel_oid = 'public.m3_open'::regclass
        AND s.slot_name = '$SLOT'
      GROUP BY s.confirmed_flush_lsn;")
    final_lag=$(q "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
                   FROM pg_replication_slots WHERE slot_name = '$SLOT';")
    if [[ "$catchup" == "t" ]]; then
        drain_ok=true
        break
    fi
    sleep 0.05
done
final_lag="${final_lag:-$(q "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint FROM pg_replication_slots WHERE slot_name = '$SLOT';")}"

# Exit PASS only when all SLO gates hold; otherwise PARTIAL (progress observed)
# or FAIL. Never claim PASS with null p95.
status="FAIL"
if [[ "$capture_progressed" == true ]] && (( observed_samples >= COMMIT_SAMPLES )) &&
   [[ "$p95" != null ]] && (( p95 < 2000 )) && (( max_ms < 5000 )) &&
   [[ "$drain_ok" == true ]]; then
    status="PASS"
elif [[ "$capture_progressed" == true ]] && (( observed_samples > 0 )) && [[ "$p95" != null ]]; then
    status="PARTIAL"
fi

cat >"$RESULT_JSON" <<EOF
{
  "run_id": "$RUN_ID",
  "status": "$status",
  "hold_seconds": $HOLD_SECONDS,
  "measurement_note": "Capture and maintenance share one background-worker loop; this measures progress despite a held unrelated lifecycle lock.",
  "shared_background_worker_loop": true,
  "worker_interval_ms": 50,
  "tracked_tables": ["public.m3_locked", "public.m3_open"],
  "locked_tracking_id": $TRACKING_ID,
  "capture_progressed_on_unblocked_table_during_hold": $capture_progressed,
  "commits_emitted_during_hold": $emitted,
  "commits_observed_during_hold": $observed_samples,
  "unblocked_delta_rows_before_hold": $rows_before,
  "unblocked_delta_rows_last_sample_during_hold": $rows_last_during_hold,
  "unblocked_delta_rows_at_hold_end": $rows_during,
  "start_slot_lag_bytes": $start_lag,
  "final_slot_lag_bytes": $final_lag,
  "slot_lag_drain_target_bytes": $drain_target,
  "drain_ok": $drain_ok,
  "visibility_poll_seconds": $POLL_SECONDS,
  "visibility_poll_count": $polls,
  "p95_visibility_target_ms": 2000,
  "max_visibility_target_ms": 5000,
  "p50_visibility_ms": $p50,
  "p95_visibility_ms": $p95,
  "p99_visibility_ms": $p99,
  "max_visibility_ms": $max_ms,
  "visibility_samples": [$visibility_samples_json],
  "visibility_samples_jsonl": "$samples_file",
  "visibility_measurement_status": "per-commit monotonic commit-ack to first delta_log visibility, sampled every $POLL_SECONDS seconds"
}
EOF
cp "$RESULT_JSON" "$ROOT/docs/qualification/capture-maintenance-isolation-latest.json"

echo "M3 isolation qualification: $status ($RESULT_JSON)"
[[ "$status" == "PASS" ]]
