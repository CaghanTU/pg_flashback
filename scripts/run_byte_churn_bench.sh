#!/usr/bin/env bash
# CHURN MODE: measures post-protect WAL/DML stress in isolation from
# table-size cost (see run_byte_table_size_bench.sh for that). A base table
# is created at a small fixed size and protected first; each churn tier then
# applies its own DML pattern against a fresh copy of that base table and is
# measured independently.
#
# Tiers (PG_FLASHBACK_CHURN_TIERS, default all five):
#   pct1, pct10, pct100   - a single UPDATE touching that percent of rows
#   small_batches         - the pct10 row volume split across many small
#                           committed transactions (throughput under frequent
#                           small commits)
#   single_large_tx       - the pct10 row volume applied as one large
#                           transaction (mixed INSERT/UPDATE/DELETE, one COMMIT)
#
# Per tier, records: WAL bytes generated, capture throughput (events/s and WAL
# MiB/s), a slot-lag sample series with its trend, catch-up time, restore
# (DROP+recover) time, disk usage, exact row fingerprint, worker
# restart/crash count observed during the tier, and the flashback_health()
# result at tier end.
#
# A failing tier is reported on its own; it does NOT block other churn tiers
# and does NOT imply anything about table-size mode (they are scored and
# gated separately).
#
# Usage:
#   PGHOST=... PGDATABASE=... ./scripts/run_byte_churn_bench.sh
#   PG_FLASHBACK_CHURN_BASE_SIZE=10MiB PG_FLASHBACK_CHURN_TIERS="pct1 pct10 pct100" \
#     ./scripts/run_byte_churn_bench.sh

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULT_DIR="${PG_FLASHBACK_BENCH_RESULT_DIR:-$ROOT/target/bench}"
RUN_ID="${PG_FLASHBACK_BENCH_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
RUN_ID_SAFE="$(printf '%s' "$RUN_ID" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9_' '_' | cut -c1-24)"
OUT_JSON="$RESULT_DIR/byte-churn-$RUN_ID.json"
BASE_SIZE="${PG_FLASHBACK_CHURN_BASE_SIZE:-10MiB}"
TIERS="${PG_FLASHBACK_CHURN_TIERS:-pct1 pct10 pct100 small_batches single_large_tx}"
TABLE_TIMEOUT_SECS="${PG_FLASHBACK_BENCH_TABLE_TIMEOUT_SECS:-600}"
CATCHUP_TIMEOUT_SECS="${PG_FLASHBACK_BENCH_CATCHUP_TIMEOUT_SECS:-1800}"
SAMPLE_INTERVAL_SECS="${PG_FLASHBACK_CHURN_SAMPLE_INTERVAL_SECS:-1}"
mkdir -p "$RESULT_DIR"
# Every one of these is read inside run_churn_tier/sample_lag_until_drained,
# which run in a separate bash -c child process (spawned by the outer
# `timeout ... bash -c '...'` below) -- a fresh process only inherits
# exported variables, not this script's plain top-level ones.
export ROOT RUN_ID RUN_ID_SAFE RESULT_DIR BASE_SIZE CATCHUP_TIMEOUT_SECS SAMPLE_INTERVAL_SECS

# shellcheck source=scripts/lib/byte_bench_common.sh
source "$ROOT/scripts/lib/byte_bench_common.sh"

TIMEOUT_BIN="$(command -v timeout || true)"
export TIMEOUT_BIN TABLE_TIMEOUT_SECS

trap cleanup_all_created EXIT

bench_preflight || die "preflight failed"
host_meta="$(host_meta_json)"
printf '%s' "$host_meta" | jq -e . >/dev/null 2>&1 || die "host metadata is not valid JSON"

SOURCE_HEAD="$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
EXT_SHA256="$(extension_sha256)"
BASE_BYTES="$(size_to_bytes "$BASE_SIZE")"
export BASE_BYTES

# Sample slot lag once a second while a background PID (the churn statement)
# runs, then continue sampling until lag returns near baseline. Emits a JSON
# array of {t_ms, lag_bytes} and the wall-clock catch-up duration.
sample_lag_until_drained() {
    local threshold=$1 deadline lag t0 now samples='[]'
    t0=$(date +%s%N)
    deadline=$(( $(date +%s) + CATCHUP_TIMEOUT_SECS ))
    while (( $(date +%s) <= deadline )); do
        lag="$(slot_lag_bytes)"
        now=$(( ($(date +%s%N) - t0) / 1000000 ))
        samples=$(jq -n --argjson acc "$samples" --argjson t "$now" --argjson l "$lag" '$acc + [{t_ms:$t, lag_bytes:$l}]')
        if (( lag <= threshold )); then
            echo "$samples" > "$LAG_SAMPLES_TMP"
            echo $(( ($(date +%s%N) - t0) / 1000000 ))
            return 0
        fi
        psqlq -c "SELECT flashback_consume_wal(65536);" >/dev/null 2>&1 || true
        sleep "$SAMPLE_INTERVAL_SECS"
    done
    echo "$samples" > "$LAG_SAMPLES_TMP"
    die "slot lag did not drain to threshold ${threshold}B within ${CATCHUP_TIMEOUT_SECS}s"
}

lag_trend() {
    # "decreasing" if the second half of samples averages lower than the
    # first half; "flat" if within 5%; otherwise "increasing".
    jq -c '
      (length) as $n |
      if $n < 4 then "insufficient_samples" else
        (.[0:($n/2|floor)] | map(.lag_bytes) | add / length) as $first_avg |
        (.[($n/2|floor):] | map(.lag_bytes) | add / length) as $second_avg |
        if $second_avg <= $first_avg * 0.95 then "decreasing"
        elif $second_avg >= $first_avg * 1.05 then "increasing"
        else "flat" end
      end'
}

set_phase() {
    [[ -n "${PHASE_FILE:-}" ]] && printf '%s' "$1" > "$PHASE_FILE" 2>/dev/null
    return 0
}
export -f set_phase

run_churn_tier() {
    local tier=$1 outfile=$2
    set_phase "setup"
    local rel="public.bc_${RUN_ID_SAFE}_${tier}"
    local ddl rows tid=""
    ddl="CREATE TABLE $rel (id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v int NOT NULL, note text);"
    rows=$(( BASE_BYTES / 32 ))

    # Scales every bounded wait in this function with base size (see the
    # matching comment in run_byte_table_size_bench.sh's run_one_table).
    local catchup_timeout_secs size_based_secs
    catchup_timeout_secs=$CATCHUP_TIMEOUT_SECS
    size_based_secs=$(( 300 + (BASE_BYTES / (1024 * 1024)) * 2 ))
    if (( size_based_secs > catchup_timeout_secs )); then size_based_secs=$catchup_timeout_secs; fi

    psqlq -c "DROP TABLE IF EXISTS $rel CASCADE;" >/dev/null 2>&1 || true
    psqlq -c "$ddl"
    psqlq -c "INSERT INTO $rel(v,note) SELECT g, 'base' FROM generate_series(1,$rows) g;"
    wait_slot_lag_near_start "before-protect-${rel}"

    set_phase "protect"
    local h protect_deadline
    psqlq -c "SELECT flashback_track('$rel');"
    tid="$(psqlq -c "SELECT tracking_id FROM flashback.tracked_tables
                    WHERE is_active AND format('%I.%I', schema_name, table_name) = '$rel'
                    ORDER BY tracking_id DESC LIMIT 1;")"
    protect_deadline=$(( $(date +%s) + size_based_secs ))
    h=""
    while (( $(date +%s) <= protect_deadline )); do
      h=$(psqlq -c "SELECT flashback_lifecycle_health('$rel');")
      [[ "$h" == "healthy" ]] && break
      sleep 0.25
    done
    [[ "$h" == "healthy" ]] || { echo "FAIL $rel: never reached healthy boundary after protect within ${size_based_secs}s (last=$h)" >&2; exit 1; }

    local disk_before churn_rows churn_sql events_before events_after lsn_before lsn_after
    disk_before=$(psqlq -c "SELECT COALESCE((flashback_disk_retention_status())->>'used_bytes','0')::bigint;")
    events_before=$(psqlq -c "SELECT count(*) FROM flashback.delta_log dl
                               JOIN flashback.tracked_tables tt USING (tracking_id)
                               WHERE tt.table_name = '${rel#public.}';")

    case "$tier" in
      pct1)  churn_rows=$(( rows / 100 )); (( churn_rows > 0 )) || churn_rows=1
             churn_sql="UPDATE $rel SET v = v + 1 WHERE id IN (SELECT id FROM $rel ORDER BY id LIMIT $churn_rows);" ;;
      pct10) churn_rows=$(( rows / 10 )); (( churn_rows > 0 )) || churn_rows=1
             churn_sql="UPDATE $rel SET v = v + 1 WHERE id IN (SELECT id FROM $rel ORDER BY id LIMIT $churn_rows);" ;;
      pct100) churn_rows=$rows
             churn_sql="UPDATE $rel SET v = v + 1;" ;;
      small_batches)
             churn_rows=$(( rows / 10 )); (( churn_rows > 0 )) || churn_rows=1
             churn_sql="" ;;
      single_large_tx)
             churn_rows=$(( rows / 10 )); (( churn_rows > 0 )) || churn_rows=1
             churn_sql="BEGIN;
                        UPDATE $rel SET v = v + 1 WHERE id IN (SELECT id FROM $rel ORDER BY id LIMIT $((churn_rows/2)));
                        DELETE FROM $rel WHERE id IN (SELECT id FROM $rel ORDER BY id DESC LIMIT $((churn_rows/4)));
                        INSERT INTO $rel(v,note) SELECT g, 'churn' FROM generate_series($((rows+1)), $((rows + churn_rows/4))) g;
                        COMMIT;" ;;
      *) echo "unknown churn tier $tier" >&2; exit 2 ;;
    esac

    set_phase "churn"
    local delta_pid_before
    delta_pid_before=$(psqlq -c "SELECT pid FROM pg_stat_activity WHERE backend_type='pg_flashback delta worker' AND datname=current_database();")

    lsn_before=$(psqlq -c "SELECT pg_current_wal_lsn();")
    local t_churn0 t_churn_apply_ms
    t_churn0=$(date +%s%N)
    if [[ "$tier" == "small_batches" ]]; then
        local batch_n=50
        local per_batch=$(( churn_rows / batch_n ))
        (( per_batch > 0 )) || per_batch=1
        local b
        for (( b = 0; b < batch_n; b++ )); do
            psqlq -c "UPDATE $rel SET v = v + 1 WHERE id IN (
                        SELECT id FROM $rel WHERE id % $batch_n = $b ORDER BY id LIMIT $per_batch);" >/dev/null
        done
    else
        psqlq -c "$churn_sql" >/dev/null
    fi
    t_churn_apply_ms=$(( ($(date +%s%N) - t_churn0) / 1000000 ))
    lsn_after=$(psqlq -c "SELECT pg_current_wal_lsn();")
    local wal_bytes
    wal_bytes=$(psqlq -c "SELECT pg_wal_lsn_diff('$lsn_after','$lsn_before')::bigint;")

    local fp_before rows_before
    fp_before=$(psqlq -c "SELECT md5(count(*)::text || ':' || coalesce(sum(hashtext(t::text)),0)::text) FROM $rel t;")
    rows_before=$(psqlq -c "SELECT count(*) FROM $rel;")

    # ---- slot-lag sample series + catch-up time (capture throughput window) ----
    set_phase "churn_catchup"
    LAG_SAMPLES_TMP="$(mktemp "${RESULT_DIR}/lag-samples.XXXXXX.json")"
    local threshold=$(( START_SLOT_LAG + LAG_NEAR_START_SLACK_BYTES ))
    (( threshold > MAX_START_LAG_BYTES )) && threshold=$MAX_START_LAG_BYTES
    local catchup_ms
    catchup_ms=$(sample_lag_until_drained "$threshold")
    local lag_samples trend
    lag_samples="$(cat "$LAG_SAMPLES_TMP")"
    trend="$(printf '%s' "$lag_samples" | lag_trend)"
    rm -f "$LAG_SAMPLES_TMP"

    # A different delta-worker PID at the end of the churn+catchup window than
    # at its start means the postmaster relaunched it -- a crash or restart
    # during this tier's own churn, not a pre-existing/unrelated event.
    local delta_pid_after delta_restarts=0
    delta_pid_after=$(psqlq -c "SELECT pid FROM pg_stat_activity WHERE backend_type='pg_flashback delta worker' AND datname=current_database();")
    if [[ -n "$delta_pid_before" && -n "$delta_pid_after" && "$delta_pid_before" != "$delta_pid_after" ]]; then
        delta_restarts=1
    fi

    events_after=$(psqlq -c "SELECT count(*) FROM flashback.delta_log dl
                              JOIN flashback.tracked_tables tt USING (tracking_id)
                              WHERE tt.table_name = '${rel#public.}';")
    local events_captured events_per_sec wal_mib_per_sec
    events_captured=$(( events_after - events_before ))
    if (( catchup_ms > 0 )); then
        events_per_sec=$(awk -v e="$events_captured" -v ms="$catchup_ms" 'BEGIN{printf "%.2f", e/(ms/1000.0)}')
        wal_mib_per_sec=$(awk -v b="$wal_bytes" -v ms="$catchup_ms" 'BEGIN{printf "%.4f", (b/1048576.0)/(ms/1000.0)}')
    else
        events_per_sec="0"
        wal_mib_per_sec="0"
    fi

    local health_after_churn
    health_after_churn=$(psqlq -c "SELECT health FROM flashback_health() WHERE table_name='${rel#public.}';")

    # ---- DROP + recover (restore time) ----
    # (size_based_secs computed once at the top of this function)
    set_phase "drop_discovery"

    local t1 t_discover st
    t1=$(date +%s%N)
    psqlq -c "DROP TABLE $rel;"
    local catchup_deadline catchup_i
    catchup_deadline=$(( $(date +%s) + size_based_secs ))
    st=""
    catchup_i=0
    while (( $(date +%s) <= catchup_deadline )); do
      st=$(psqlq -c "SELECT status FROM flashback_disaster_points('$rel', interval '1 day') WHERE event_type='DROP' ORDER BY disaster_commit_lsn DESC LIMIT 1;")
      [[ "$st" == "restorable" ]] && break
      if (( catchup_i % 8 == 0 )); then
        psqlq -c "SELECT flashback_consume_wal(65536);" >/dev/null 2>&1 || true
      fi
      catchup_i=$((catchup_i + 1))
      sleep 0.5
    done
    t_discover=$(( ($(date +%s%N) - t1) / 1000000 ))
    [[ "$st" == "restorable" ]] || { echo "FAIL $rel: DROP not restorable after ${size_based_secs}s" >&2; exit 1; }

    set_phase "plan"
    local plan plan_status token plan_code
    catchup_deadline=$(( $(date +%s) + size_based_secs ))
    plan=""
    plan_status=""
    catchup_i=0
    while (( $(date +%s) <= catchup_deadline )); do
      plan=$(psqlq -c "SELECT flashback_recover_plan('$rel', interval '1 day');")
      plan_status=$(printf '%s' "$plan" | jq -r '.status // empty')
      [[ "$plan_status" == "restorable" ]] && break
      plan_code=$(printf '%s' "$plan" | jq -r '.code // empty')
      if [[ "$plan_status" == "error" && "$plan_code" != "capture_catchup_pending" && "$plan_code" != "manifest_pending" ]]; then
        echo "FAIL plan $rel: $plan" >&2
        exit 1
      fi
      if (( catchup_i % 8 == 0 )); then
        psqlq -c "SELECT flashback_consume_wal(65536);" >/dev/null 2>&1 || true
      fi
      catchup_i=$((catchup_i + 1))
      sleep 0.5
    done
    token=$(printf '%s' "$plan" | jq -r '.plan_token // empty')
    [[ "$plan_status" == "restorable" && -n "$token" && "$token" != "null" ]] \
      || { echo "FAIL plan $rel after ${size_based_secs}s catch-up: $plan" >&2; exit 1; }

    set_phase "recover"
    local t3 op exec_err exec_rc t_restore fp_after ok disk_peak
    t3=$(date +%s%N)
    catchup_deadline=$(( $(date +%s) + size_based_secs ))
    exec_rc=1
    while (( $(date +%s) <= catchup_deadline )); do
      op=$(psqlq -c "SELECT flashback_recover_begin('$rel', '$token', interval '1 day')->>'operation_id';")
      set +e
      exec_err=$(psqlq -c "SELECT flashback_recover_execute('$rel', '$token', interval '1 day', NULL, NULL, NULL, $op);" 2>&1)
      exec_rc=$?
      set -e
      [[ $exec_rc -eq 0 ]] && break
      if printf '%s' "$exec_err" | grep -qi 'logical slot is .* bytes behind'; then
        psqlq -c "SELECT flashback_consume_wal(65536);" >/dev/null 2>&1 || true
        sleep 0.5
        plan=$(psqlq -c "SELECT flashback_recover_plan('$rel', interval '1 day');")
        token=$(printf '%s' "$plan" | jq -r '.plan_token // empty')
        [[ -n "$token" && "$token" != "null" ]] || { echo "FAIL replan $rel: $plan" >&2; exit 1; }
        continue
      fi
      echo "FAIL recover_execute $rel: $exec_err" >&2
      exit 1
    done
    [[ $exec_rc -eq 0 ]] || { echo "FAIL recover_execute $rel: WAL never drained in ${size_based_secs}s (last: $exec_err)" >&2; exit 1; }
    local verify_deadline
    verify_deadline=$(( $(date +%s) + size_based_secs ))
    while (( $(date +%s) <= verify_deadline )); do
      st=$(psqlq -c "SELECT COALESCE(flashback_operation_state($op),'missing');")
      [[ "$st" == "verified" || "$st" == "failed" ]] && break
      sleep 0.5
    done
    t_restore=$(( ($(date +%s%N) - t3) / 1000000 ))
    fp_after=$(psqlq -c "SELECT md5(count(*)::text || ':' || coalesce(sum(hashtext(t::text)),0)::text) FROM $rel t;")
    ok=$([[ "$fp_before" == "$fp_after" && "$st" == "verified" ]] && echo true || echo false)
    disk_peak=$(psqlq -c "SELECT COALESCE((flashback_disk_retention_status())->>'used_bytes','0')::bigint;")

    set_phase "cleanup"
    local t5 t_cleanup
    t5=$(date +%s%N)
    cleanup_table_lifecycle "$rel" "$tid"
    t_cleanup=$(( ($(date +%s%N) - t5) / 1000000 ))

    set_phase "done"
    jq -n \
      --arg tier "$tier" --arg rel "$rel" --arg base_size "$BASE_SIZE" --argjson base_bytes "$BASE_BYTES" \
      --argjson churn_rows "$churn_rows" --argjson wal_bytes "$wal_bytes" \
      --argjson t_churn_apply_ms "$t_churn_apply_ms" --argjson catchup_ms "$catchup_ms" \
      --argjson events_captured "$events_captured" --argjson events_per_sec "$events_per_sec" \
      --argjson wal_mib_per_sec "$wal_mib_per_sec" --argjson lag_samples "$lag_samples" \
      --arg lag_trend "$trend" --arg health_after_churn "$health_after_churn" \
      --argjson delta_worker_restarts "$delta_restarts" \
      --argjson discover_ms "$t_discover" --argjson restore_ms "$t_restore" \
      --argjson rows "$rows_before" --argjson disk_before "$disk_before" --argjson disk_peak "$disk_peak" \
      --argjson cleanup_ms "$t_cleanup" --argjson ok "$ok" --arg op_state "$st" --arg status "ran" \
      '{
        tier:$tier, table:$rel, base_size_label:$base_size, base_target_bytes:$base_bytes,
        churn_rows:$churn_rows, wal_bytes_generated:$wal_bytes, churn_apply_ms:$t_churn_apply_ms,
        catchup_ms:$catchup_ms, events_captured:$events_captured,
        capture_events_per_sec:$events_per_sec, capture_wal_mib_per_sec:$wal_mib_per_sec,
        slot_lag_samples:$lag_samples, slot_lag_trend:$lag_trend,
        health_after_churn:$health_after_churn, delta_worker_restarts_observed:$delta_worker_restarts,
        drop_discovery_ms:$discover_ms, restore_ms:$restore_ms,
        rows:$rows, disk_before_bytes:$disk_before, disk_peak_bytes:$disk_peak,
        cleanup_ms:$cleanup_ms, fingerprint_ok:$ok, operation_state:$op_state, status:$status
      }' > "$outfile"
}
export -f run_churn_tier sample_lag_until_drained lag_trend

RESULTS='[]'
for tier in $TIERS; do
  CREATED_TABLES+=("public.bc_${RUN_ID_SAFE}_${tier}")
  echo "== churn tier: $tier (base=$BASE_SIZE) =="

  resfile="$(mktemp "${RESULT_DIR}/one-churn-result.XXXXXX.json")"
  PHASE_FILE="$(mktemp "${RESULT_DIR}/one-churn-phase.XXXXXX.txt")"
  export PHASE_FILE
  stderr_file="$(mktemp "${RESULT_DIR}/one-churn-stderr.XXXXXX.log")"
  : > "$PHASE_FILE"
  timed_out=0
  rc=0
  if [[ -n "$TIMEOUT_BIN" ]]; then
    if ! "$TIMEOUT_BIN" "${TABLE_TIMEOUT_SECS}s" bash -c 'set -Eeuo pipefail; run_churn_tier "$1" "$2"' _ "$tier" "$resfile" \
        2>"$stderr_file"; then
      rc=$?
      [[ $rc -eq 124 ]] && timed_out=1
    fi
  else
    run_churn_tier "$tier" "$resfile" 2>"$stderr_file" || rc=$?
  fi
  cat "$stderr_file" >&2
  last_phase="$(cat "$PHASE_FILE" 2>/dev/null || echo unknown)"
  [[ -n "$last_phase" ]] || last_phase="unknown"
  stderr_tail="$(tail -c 4000 "$stderr_file" 2>/dev/null || echo "")"

  rel="public.bc_${RUN_ID_SAFE}_${tier}"
  if [[ $timed_out -eq 1 ]]; then
    echo "TIMEOUT churn tier $tier after ${TABLE_TIMEOUT_SECS}s (phase=$last_phase)" >&2
    tid="$(psqlq -c "SELECT tracking_id FROM flashback.tracked_tables
                     WHERE format('%I.%I', schema_name, table_name) = '$rel'
                     ORDER BY tracking_id DESC LIMIT 1;" 2>/dev/null || true)"
    cleanup_table_lifecycle "$rel" "$tid"
    remove_from_created "$rel"
    RESULTS=$(jq -n --argjson acc "$RESULTS" --arg tier "$tier" --arg rel "$rel" --argjson timeout "$TABLE_TIMEOUT_SECS" \
      --arg phase "$last_phase" --arg reason "timed out after ${TABLE_TIMEOUT_SECS}s in phase $last_phase" \
      --arg stderr_tail "$stderr_tail" \
      '$acc + [{tier:$tier, table:$rel, status:"timeout", timeout_secs:$timeout,
                 phase:$phase, reason:$reason, stderr_tail:$stderr_tail, fingerprint_ok:false}]')
  elif [[ -s "$resfile" ]] && jq -e . >/dev/null 2>&1 < "$resfile"; then
    RESULTS=$(jq -n --argjson acc "$RESULTS" --argjson one "$(cat "$resfile")" '$acc + [$one]')
    remove_from_created "$rel"
  else
    echo "FAIL churn tier $tier: no result produced (rc=$rc, phase=$last_phase)" >&2
    tid="$(psqlq -c "SELECT tracking_id FROM flashback.tracked_tables
                     WHERE format('%I.%I', schema_name, table_name) = '$rel'
                     ORDER BY tracking_id DESC LIMIT 1;" 2>/dev/null || true)"
    cleanup_table_lifecycle "$rel" "$tid"
    remove_from_created "$rel"
    RESULTS=$(jq -n --argjson acc "$RESULTS" --arg tier "$tier" --arg rel "$rel" --argjson exit_code "$rc" \
      --arg phase "$last_phase" --arg reason "no result produced (exit_code=$rc) in phase $last_phase" \
      --arg stderr_tail "$stderr_tail" \
      '$acc + [{tier:$tier, table:$rel, status:"error", exit_code:$exit_code,
                 phase:$phase, reason:$reason, stderr_tail:$stderr_tail, fingerprint_ok:false}]')
  fi
  rm -f "$resfile" "$PHASE_FILE" "$stderr_file"
  unset PHASE_FILE
done

trap - EXIT
cleanup_all_created

[[ -n "$START_SLOT_LAG" && "$START_SLOT_LAG" =~ ^[0-9]+$ ]] || START_SLOT_LAG=0
[[ -n "$PREFLIGHT_JSON" ]] || PREFLIGHT_JSON='{}'
printf '%s' "$PREFLIGHT_JSON" | jq -e . >/dev/null 2>&1 || PREFLIGHT_JSON='{}'
printf '%s' "$RESULTS" | jq -e . >/dev/null 2>&1 || die "results accumulator is not valid JSON"

jq -n \
  --argjson host "$host_meta" \
  --argjson results "$RESULTS" \
  --arg run "$RUN_ID" \
  --arg run_id_safe "$RUN_ID_SAFE" \
  --arg source_head "$SOURCE_HEAD" \
  --arg ext_sha256 "$EXT_SHA256" \
  --argjson preflight "$PREFLIGHT_JSON" \
  --argjson start_slot_lag "$START_SLOT_LAG" \
  --arg base_size "$BASE_SIZE" \
  --arg slot_name "$SLOT_NAME" \
  '{
    schema_version: 1,
    mode: "churn",
    run_id: $run,
    run_id_safe: $run_id_safe,
    source_head: $source_head,
    extension_binary_sha256: $ext_sha256,
    worker_preflight: $preflight,
    start_slot_lag_bytes: $start_slot_lag,
    base_size_label: $base_size,
    slot_name: $slot_name,
    host: $host,
    results: $results,
    note: "Measures post-protect WAL/DML churn in isolation; a churn-tier failure does not imply table-size mode failure and vice versa (see run_byte_table_size_bench.sh)."
  }' > "$OUT_JSON"
echo "Wrote $OUT_JSON"
jq -r '.results[] | "\(.tier)\t\(.status // "ran")\twal=\(.wal_bytes_generated // "-")B events_per_sec=\(.capture_events_per_sec // "-") trend=\(.slot_lag_trend // "-") restore=\(.restore_ms // "-")ms ok=\(.fingerprint_ok // false)"' "$OUT_JSON"

failed_n="$(jq '[.results[] | select(.fingerprint_ok == false or .status == "error" or .status == "timeout")] | length' "$OUT_JSON")"
if (( failed_n > 0 )); then
  echo "FAIL: $failed_n churn tier(s) failed" >&2
  exit 1
fi
echo "byte_churn_bench PASS"
