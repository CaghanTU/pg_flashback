#!/usr/bin/env bash
# Bounded development soak for WAL local_delta capture.
#
# Creates a disposable cluster, cycles a bounded table through insert/update/
# delete work, and records coverage and slot-lag samples as JSON.

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PG_CONFIG="${PG_CONFIG:-/usr/local/pgsql-17/bin/pg_config}"
PG_BIN="$("$PG_CONFIG" --bindir)"
SHARE_DIR="$("$PG_CONFIG" --sharedir)"
PSQL="$PG_BIN/psql"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
PORT="${PG_FLASHBACK_SOAK_PORT:-28937}"
SOAK_SECONDS="${PG_FLASHBACK_SOAK_SECONDS:-90}"
TARGET_MIB="${PG_FLASHBACK_SOAK_TARGET_MIB:-2048}"
WORK_ROOT="${PG_FLASHBACK_SOAK_WORK_ROOT:-$ROOT/target/dev-soak/$RUN_ID}"
DATA="$WORK_ROOT/data"
SOCKET="/tmp/pgfb-soak-$RUN_ID"
RESULT_DIR="${PG_FLASHBACK_SOAK_RESULT_DIR:-$ROOT/target/qualification}"
RESULT_JSON="$RESULT_DIR/dev-soak-$RUN_ID.json"
DB=postgres
STATUS=FAIL
CLEANUP_OK=false
CYCLES=0
INSERTED_ROWS=0
UPDATED_ROWS=0
DELETED_ROWS=0
SAMPLES=""
START_SLOT_LAG=0
FINAL_SLOT_LAG=0
DRAIN_OK=false
ACTUAL_INSERTS=0
ACTUAL_UPDATES=0
ACTUAL_DELETES=0
EVENT_COUNTS_OK=false
CATCHUP_OK=false
FINGERPRINT_OK=false
LAG_NEAR_START=false
LAG_NEAR_START_SLACK_BYTES="${PG_FLASHBACK_SOAK_LAG_NEAR_START_SLACK_BYTES:-1048576}"
SLOT_LAG_STABLE_OR_DECREASING=false
START_EPOCH="$(date +%s)"
COMMIT="$(git -C "$ROOT" rev-parse HEAD)"

require_file() {
    [[ -f "$1" ]] || {
        echo "FAIL: required file not found: $1" >&2
        exit 2
    }
}

write_result() {
    local rc=$1 elapsed
    elapsed=$(( $(date +%s) - START_EPOCH ))
    mkdir -p "$RESULT_DIR"
    cat >"$RESULT_JSON" <<EOF
{
  "run_id": "$RUN_ID",
  "commit": "$COMMIT",
  "qualification_kind": "development_soak",
  "status": "$STATUS",
  "exit_code": $rc,
  "duration_seconds": $elapsed,
  "configured_duration_seconds": $SOAK_SECONDS,
  "workload_target_mib": $TARGET_MIB,
  "cycles": $CYCLES,
  "expected_event_counts": {"INSERT": $INSERTED_ROWS, "UPDATE": $UPDATED_ROWS, "DELETE": $DELETED_ROWS},
  "actual_event_counts": {"INSERT": $ACTUAL_INSERTS, "UPDATE": $ACTUAL_UPDATES, "DELETE": $ACTUAL_DELETES},
  "event_count_tolerance": 0,
  "event_counts_ok": $EVENT_COUNTS_OK,
  "start_slot_lag_bytes": $START_SLOT_LAG,
  "final_slot_lag_bytes": $FINAL_SLOT_LAG,
  "drain_ok": $DRAIN_OK,
  "catchup_ok": $CATCHUP_OK,
  "fingerprint_ok": $FINGERPRINT_OK,
  "lag_near_start": $LAG_NEAR_START,
  "lag_near_start_slack_bytes": $LAG_NEAR_START_SLACK_BYTES,
  "slot_lag_stable_or_decreasing": $SLOT_LAG_STABLE_OR_DECREASING,
  "slot_lag_samples": [$SAMPLES],
  "cleanup": {"postgres_stopped": $CLEANUP_OK, "data_directory_removed": $CLEANUP_OK, "socket_directory_removed": $CLEANUP_OK}
}
EOF
}

cleanup() {
    local rc=$?
    set +e
    "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1
    if "$PG_BIN/pg_ctl" -D "$DATA" status >/dev/null 2>&1; then
        CLEANUP_OK=false
    elif ps -eo args= | awk -v data="$DATA" \
        'index($0, data) && /[p]ostgres/ {found=1} END {exit !found}'; then
        echo "FAIL: PostgreSQL process still references this run's data directory" >&2
        CLEANUP_OK=false
    else
        rm -rf "$DATA" "$SOCKET"
        [[ ! -e "$DATA" && ! -e "$SOCKET" ]] && CLEANUP_OK=true
    fi
    [[ "$rc" == 0 && "$CLEANUP_OK" == true ]] || STATUS=FAIL
    write_result "$rc"
    [[ "$STATUS" == PASS && "$CLEANUP_OK" == true ]] || rc=1
    echo "Development soak: $STATUS ($RESULT_JSON)"
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

require_file "$PG_CONFIG"
require_file "$SHARE_DIR/extension/pg_flashback.control"
[[ "$SOAK_SECONDS" =~ ^[0-9]+$ && "$SOAK_SECONDS" -gt 0 ]] || {
    echo "FAIL: PG_FLASHBACK_SOAK_SECONDS must be a positive integer" >&2; exit 2; }
[[ "$TARGET_MIB" =~ ^[0-9]+$ && "$TARGET_MIB" -ge 2 ]] || {
    echo "FAIL: PG_FLASHBACK_SOAK_TARGET_MIB must be at least 2" >&2; exit 2; }
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

q() {
    "$PSQL" -X -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAtc "$1"
}

q "CREATE EXTENSION pg_flashback;
   CREATE TABLE public.soak_delta(id integer PRIMARY KEY, payload text NOT NULL);"
q "SELECT flashback_track('public.soak_delta');" >/dev/null
for _ in $(seq 1 200); do
    [[ "$(q "SELECT health FROM flashback_health()
             WHERE table_name = 'public.soak_delta';")" == healthy ]] && break
    sleep 0.1
done
[[ "$(q "SELECT health FROM flashback_health()
         WHERE table_name = 'public.soak_delta';")" == healthy ]] || {
    echo "FAIL: initial coverage did not become healthy" >&2; exit 1; }

SLOT="$(q "SELECT flashback_effective_slot_name();")"
START_SLOT_LAG="$(q "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
                       FROM pg_replication_slots WHERE slot_name = '$SLOT';")"
deadline=$(( START_EPOCH + SOAK_SECONDS ))
# One cycle writes ~8 MiB of row payload, then deletes it so live table size
# stays bounded. TARGET_MIB is the intended cumulative churn budget for the
# run; the loop always honors SOAK_SECONDS so short smokes and longer soaks
# both remain time-bounded.
while (( $(date +%s) < deadline )); do
    base=$((CYCLES * 1024))
    q "INSERT INTO public.soak_delta
       SELECT $base + g, repeat(md5(($base + g)::text), 256)
       FROM generate_series(1, 1024) AS g;" >/dev/null
    q "UPDATE public.soak_delta SET payload = reverse(payload);" >/dev/null
    q "DELETE FROM public.soak_delta;" >/dev/null
    CYCLES=$((CYCLES + 1))
    INSERTED_ROWS=$((INSERTED_ROWS + 1024))
    UPDATED_ROWS=$((UPDATED_ROWS + 1024))
    DELETED_ROWS=$((DELETED_ROWS + 1024))

    health="$(q "SELECT health FROM flashback_health()
                 WHERE table_name = 'public.soak_delta';")"
    lag="$(q "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
               FROM pg_replication_slots WHERE slot_name = '$SLOT';")"
    visible="$(q "SELECT count(*) FROM flashback.delta_log
                   WHERE rel_oid = 'public.soak_delta'::regclass;")"
    SAMPLES+="${SAMPLES:+,}{\"cycle\":$CYCLES,\"health\":\"$health\",\"slot_lag_bytes\":$lag,\"delta_rows\":$visible}"
    [[ "$health" == healthy ]] || {
        echo "FAIL: coverage became $health; refusing silent capture loss" >&2; exit 1; }
    # Soft ceiling: stop early once cumulative cycle churn reaches the budget.
    if (( CYCLES * 8 >= TARGET_MIB )); then
        break
    fi
done

[[ "$(q "SELECT count(*) FROM public.soak_delta;")" == 0 ]] || {
    echo "FAIL: cyclic workload cleanup left table rows" >&2; exit 1; }
[[ "$CYCLES" -gt 0 ]] || { echo "FAIL: no soak cycles completed" >&2; exit 1; }

# Draining is a qualification condition, not an informational postscript.
# Require exact decoded DML counts and prove the slot confirmed_flush has
# caught up to this table's last captured commit LSN. Global slot lag may
# remain elevated when filtered non-output WAL is present (consume returns
# early without advancing across empty peeks); that alone must not hide
# silent capture loss, and must not force FAIL when every expected event is
# durable and confirmed_flush is past the table's max commit_lsn.
DRAIN_TIMEOUT_SECONDS="${PG_FLASHBACK_SOAK_DRAIN_TIMEOUT_SECONDS:-120}"
LAG_NEAR_START_SLACK_BYTES="${PG_FLASHBACK_SOAK_LAG_NEAR_START_SLACK_BYTES:-1048576}"
drain_deadline=$(( $(date +%s) + DRAIN_TIMEOUT_SECONDS ))
last_lag=-1
EVENT_COUNTS_OK=false
DRAIN_OK=false
CATCHUP_OK=false
SLOT_LAG_STABLE_OR_DECREASING=false
LAG_NEAR_START=false
FINGERPRINT_OK=false
FINAL_SLOT_LAG=0
while (( $(date +%s) <= drain_deadline )); do
    # Larger batches reduce empty-peek stalls when filtered catalog WAL sits
    # ahead of the remaining user-table changes.
    q "SELECT flashback_consume_wal(65536);" >/dev/null || true
    counts="$(q "SELECT
        count(*) FILTER (WHERE event_type = 'INSERT'),
        count(*) FILTER (WHERE event_type = 'UPDATE'),
        count(*) FILTER (WHERE event_type = 'DELETE')
      FROM flashback.delta_log
      WHERE rel_oid = 'public.soak_delta'::regclass;")"
    IFS='|' read -r ACTUAL_INSERTS ACTUAL_UPDATES ACTUAL_DELETES <<<"$counts"
    if (( ACTUAL_INSERTS == INSERTED_ROWS && ACTUAL_UPDATES == UPDATED_ROWS &&
          ACTUAL_DELETES == DELETED_ROWS )); then
        EVENT_COUNTS_OK=true
    else
        EVENT_COUNTS_OK=false
    fi
    FINAL_SLOT_LAG="$(q "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
                         FROM pg_replication_slots WHERE slot_name = '$SLOT';")"
    catchup="$(q "SELECT CASE
        WHEN max(d.commit_lsn) IS NULL THEN false
        WHEN s.confirmed_flush_lsn >= max(d.commit_lsn) THEN true
        ELSE false
      END
      FROM flashback.delta_log d
      CROSS JOIN pg_replication_slots s
      WHERE d.rel_oid = 'public.soak_delta'::regclass
        AND s.slot_name = '$SLOT'
      GROUP BY s.confirmed_flush_lsn;")"
    CATCHUP_OK=false
    [[ "$catchup" == "t" ]] && CATCHUP_OK=true
    SLOT_LAG_STABLE_OR_DECREASING=false
    (( last_lag < 0 || FINAL_SLOT_LAG <= last_lag )) && SLOT_LAG_STABLE_OR_DECREASING=true
    LAG_NEAR_START=false
    (( FINAL_SLOT_LAG <= START_SLOT_LAG + LAG_NEAR_START_SLACK_BYTES )) && LAG_NEAR_START=true
    if [[ "$EVENT_COUNTS_OK" == true && "$CATCHUP_OK" == true ]]; then
        DRAIN_OK=true
        break
    fi
    last_lag=$FINAL_SLOT_LAG
    sleep 0.05
done

# Spot-check silent loss: every inserted id must appear as an INSERT in delta_log.
if [[ "$EVENT_COUNTS_OK" == true ]]; then
    missing="$(q "SELECT count(*) FROM generate_series(1, $INSERTED_ROWS) AS g(id)
                  WHERE NOT EXISTS (
                    SELECT 1 FROM flashback.delta_log d
                    WHERE d.rel_oid = 'public.soak_delta'::regclass
                      AND d.event_type = 'INSERT'
                      AND (d.new_data->>'id')::integer = g.id
                  );")"
    [[ "$missing" == "0" ]] && FINGERPRINT_OK=true
fi

[[ "$EVENT_COUNTS_OK" == true && "$DRAIN_OK" == true && "$FINGERPRINT_OK" == true ]] || {
    echo "FAIL: capture did not drain to expected event counts and slot catch-up" >&2
    echo "  event_counts_ok=$EVENT_COUNTS_OK catchup_ok=$CATCHUP_OK drain_ok=$DRAIN_OK fingerprint_ok=$FINGERPRINT_OK final_lag=$FINAL_SLOT_LAG lag_near_start=$LAG_NEAR_START" >&2
    exit 1
}
STATUS=PASS
