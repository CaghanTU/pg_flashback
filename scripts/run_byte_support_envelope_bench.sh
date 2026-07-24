#!/usr/bin/env bash
# Byte-based local_delta support-envelope benchmark.
# Measures protect/DROP/recover cost by table size + shape, not whole-DB size.
#
# Requires a live cluster with pg_flashback installed (PG* / libpq).
# Results go under target/ (never commit). Trap cleans only this run's tables.
#
# Staged tiers: sizes run smallest-first as a gate. All shapes in a tier must
# pass before the next larger tier runs; a failing tier blocks larger tiers as
# BLOCKED (not run).
#
# Usage:
#   PGHOST=... PGDATABASE=... ./scripts/run_byte_support_envelope_bench.sh
#   PG_FLASHBACK_BENCH_SIZES="10MiB 100MiB" ./scripts/run_byte_support_envelope_bench.sh

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PG_CONFIG="${PG_CONFIG:-/usr/local/pgsql-17/bin/pg_config}"
RESULT_DIR="${PG_FLASHBACK_BENCH_RESULT_DIR:-$ROOT/target/bench}"
RUN_ID="${PG_FLASHBACK_BENCH_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
RUN_ID_SAFE="$(printf '%s' "$RUN_ID" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9_' '_' | cut -c1-24)"
OUT_JSON="$RESULT_DIR/byte-support-envelope-$RUN_ID.json"
SIZES="${PG_FLASHBACK_BENCH_SIZES:-10MiB 100MiB 500MiB 1GiB}"
SHAPES="${PG_FLASHBACK_BENCH_SHAPES:-narrow indexed toast churn}"
MAX_TEMP_BYTES="${PG_FLASHBACK_BENCH_MAX_TEMP_BYTES:-$((8 * 1024 * 1024 * 1024))}"
TABLE_TIMEOUT_SECS="${PG_FLASHBACK_BENCH_TABLE_TIMEOUT_SECS:-600}"
FS_RESERVE_BYTES="${PG_FLASHBACK_BENCH_FS_RESERVE_BYTES:-$((1024 * 1024 * 1024))}"
MAX_START_LAG_BYTES="${PG_FLASHBACK_BENCH_MAX_START_LAG_BYTES:-$((16 * 1024 * 1024))}"
LAG_WAIT_TIMEOUT_SECS="${PG_FLASHBACK_BENCH_LAG_WAIT_TIMEOUT_SECS:-600}"
LAG_NEAR_START_SLACK_BYTES="${PG_FLASHBACK_BENCH_LAG_NEAR_START_SLACK_BYTES:-1048576}"
UNPROTECT_POLL_MAX="${PG_FLASHBACK_BENCH_UNPROTECT_POLL_MAX:-120}"
mkdir -p "$RESULT_DIR"

PSQL_BIN="${PSQL_BIN:-psql}"
export PSQL_BIN PG_CONFIG ROOT RUN_ID RUN_ID_SAFE MAX_START_LAG_BYTES
export LAG_WAIT_TIMEOUT_SECS LAG_NEAR_START_SLACK_BYTES UNPROTECT_POLL_MAX

psqlq() { "$PSQL_BIN" -X -v ON_ERROR_STOP=1 -qAt "$@"; }
export -f psqlq

remove_from_created() {
    local target=$1 t
    local -a kept=()
    for t in "${CREATED_TABLES[@]+"${CREATED_TABLES[@]}"}"; do
        [[ "$t" == "$target" ]] || kept+=("$t")
    done
    CREATED_TABLES=("${kept[@]+"${kept[@]}"}")
}

CREATED_TABLES=()
START_SLOT_LAG=0
SLOT_NAME=""
TIMEOUT_BIN="$(command -v timeout || true)"
export TIMEOUT_BIN TABLE_TIMEOUT_SECS

die() { echo "FAIL: $*" >&2; exit 1; }

# Always abort the script: `cmd || preflight_fail` is an OR-list, so a plain
# `return 1` would not stop bench_preflight under `set -e`.
preflight_fail() { die "$*"; }

extension_sha256() {
    local so
    so="$("$PG_CONFIG" --pkglibdir)/pg_flashback.so"
    [[ -f "$so" ]] || die "pg_flashback.so not found at $so (PG_CONFIG=$PG_CONFIG)"
    sha256sum "$so" | awk '{print $1}'
}

slot_lag_bytes() {
    psqlq -c "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
              FROM pg_replication_slots WHERE slot_name = '$SLOT_NAME';"
}

wait_slot_lag_near_start() {
    local label=${1:-between-shapes} deadline lag threshold
    deadline=$(( $(date +%s) + LAG_WAIT_TIMEOUT_SECS ))
    threshold=$(( START_SLOT_LAG + LAG_NEAR_START_SLACK_BYTES ))
    if (( threshold > MAX_START_LAG_BYTES )); then
        threshold=$MAX_START_LAG_BYTES
    fi
    while (( $(date +%s) <= deadline )); do
        lag="$(slot_lag_bytes)"
        if (( lag <= threshold )); then
            echo "slot lag ${lag}B <= threshold ${threshold}B ($label)" >&2
            return 0
        fi
        psqlq -c "SELECT flashback_consume_wal(65536);" >/dev/null 2>&1 || true
        sleep 0.5
    done
    die "slot lag did not return to start threshold within ${LAG_WAIT_TIMEOUT_SECS}s ($label; last=${lag}B threshold=${threshold}B)"
}
export -f wait_slot_lag_near_start slot_lag_bytes

catalog_cleanup_tracking_id() {
    local tid=$1
    [[ -n "$tid" && "$tid" != "null" ]] || return 0
    psqlq <<SQL >/dev/null 2>&1 || true
BEGIN;
SET LOCAL pg_flashback.enabled = on;
DELETE FROM flashback.delta_log WHERE tracking_id = ${tid}::bigint;
DELETE FROM flashback.snapshots WHERE tracking_id = ${tid}::bigint;
DELETE FROM flashback.coverage_generations WHERE tracking_id = ${tid}::bigint;
UPDATE flashback.tracked_tables
   SET is_active = false, protection_state = 'cleaned'
 WHERE tracking_id = ${tid}::bigint;
COMMIT;
SQL
}

cleanup_table_lifecycle() {
    local rel=$1 tid=${2:-}
    if [[ -z "$tid" || "$tid" == "null" ]]; then
        tid="$(psqlq -c "SELECT tracking_id FROM flashback.tracked_tables
                          WHERE is_active AND format('%I.%I', schema_name, table_name) = '$rel'
                          ORDER BY tracking_id DESC LIMIT 1;" 2>/dev/null || true)"
    fi
    psqlq -c "SELECT flashback_unprotect('$rel');" >/dev/null 2>&1 || true
    local st2 _ i
    for (( i = 1; i <= UNPROTECT_POLL_MAX; i++ )); do
        if [[ -n "$tid" && "$tid" != "null" ]]; then
            st2="$(psqlq -c "SELECT COALESCE((SELECT protection_state FROM flashback.tracked_tables
                              WHERE tracking_id = ${tid}::bigint LIMIT 1), 'gone');" 2>/dev/null || echo gone)"
        else
            st2="$(psqlq -c "SELECT COALESCE((SELECT e->>'protection_state'
                              FROM jsonb_array_elements(flashback_list_lifecycles()) e
                              WHERE e->>'table_name' = '$rel' LIMIT 1), 'gone');" 2>/dev/null || echo gone)"
        fi
        [[ "$st2" == "unprotected" || "$st2" == "gone" || "$st2" == "inactive" || "$st2" == "cleaned" ]] && break
        psqlq -c "SELECT flashback_finalize_unprotect_operations();" >/dev/null 2>&1 || true
        sleep 0.5
    done
    if [[ -n "$tid" && "$tid" != "null" ]]; then
        set +e
        psqlq -c "BEGIN; SET LOCAL pg_flashback.enabled = on; SELECT flashback_cleanup(${tid}::bigint, false); COMMIT;" >/dev/null 2>&1
        local crc=$?
        set -e
        if (( crc != 0 )); then
            catalog_cleanup_tracking_id "$tid"
        fi
    fi
    set +e
    psqlq -c "BEGIN; SET LOCAL pg_flashback.enabled = on; DROP TABLE IF EXISTS $rel CASCADE; COMMIT;" >/dev/null 2>&1 \
        || psqlq -c "DROP TABLE IF EXISTS $rel CASCADE;" >/dev/null 2>&1
    set -e
}
export -f cleanup_table_lifecycle catalog_cleanup_tracking_id

cleanup_all_created() {
    local rel tid
    for rel in "${CREATED_TABLES[@]+"${CREATED_TABLES[@]}"}"; do
        tid="$(psqlq -c "SELECT tracking_id FROM flashback.tracked_tables
                          WHERE format('%I.%I', schema_name, table_name) = '$rel'
                          ORDER BY tracking_id DESC LIMIT 1;" 2>/dev/null || true)"
        cleanup_table_lifecycle "$rel" "$tid"
    done
    CREATED_TABLES=()
}
trap cleanup_all_created EXIT

size_to_bytes() {
    case "$1" in
        10MiB) echo $((10 * 1024 * 1024)) ;;
        100MiB) echo $((100 * 1024 * 1024)) ;;
        500MiB) echo $((500 * 1024 * 1024)) ;;
        1GiB) echo $((1024 * 1024 * 1024)) ;;
        *) echo "$1" | awk '/^[0-9]+$/ {print; exit} {exit 1}' || { echo "bad size $1" >&2; exit 2; } ;;
    esac
}
export -f size_to_bytes

bench_preflight() {
    local lag leftovers cap_ok workers_json
    workers_json="$(psqlq -c "SELECT row_to_json(w)::text FROM flashback_worker_readiness() w LIMIT 1;")"
    [[ -n "$workers_json" ]] || preflight_fail "flashback_worker_readiness() returned no row"
    local admission admitted capture_ok maint_ok
    admission="$(printf '%s' "$workers_json" | jq -r '.admission_state // empty')"
    admitted="$(printf '%s' "$workers_json" | jq -r '.admitted // false')"
    capture_ok="$(printf '%s' "$workers_json" | jq -r '.capture_running // false')"
    maint_ok="$(printf '%s' "$workers_json" | jq -r '.maintenance_running // false')"
    [[ "$admitted" == "true" || "$admitted" == "t" ]] || preflight_fail "database not admitted in pg_flashback.target_databases ($(printf '%s' "$workers_json" | jq -r '.reason // empty'))"
    [[ "$admission" == "ready" ]] || preflight_fail "flashback_worker_readiness admission_state=$admission (need ready; capture_running=$capture_ok maintenance_running=$maint_ok)"
    [[ "$capture_ok" == "true" || "$capture_ok" == "t" ]] || preflight_fail "capture worker not running for current database"
    [[ "$maint_ok" == "true" || "$maint_ok" == "t" ]] || preflight_fail "maintenance worker not running for current database"

    local slots_required max_wp
    slots_required="$(printf '%s' "$workers_json" | jq -r '.bgworker_slots_required // 0')"
    max_wp="$(printf '%s' "$workers_json" | jq -r '.max_worker_processes // 0')"
    (( slots_required <= max_wp )) || preflight_fail "max_worker_processes=$max_wp insufficient for admitted pairs (need $slots_required slots)"

    [[ "$(psqlq -c "SELECT current_setting('pg_flashback.capture_mode');")" == "wal" ]] \
        || preflight_fail "pg_flashback.capture_mode must be wal"
    [[ "$(psqlq -c "SELECT current_setting('pg_flashback.enabled');")" == "on" ]] \
        || preflight_fail "pg_flashback.enabled must be on"

    cap_ok="$(psqlq -c "SELECT
        pg_size_bytes(current_setting('pg_flashback.local_max_snapshot_bytes', true)) > 0
        AND pg_size_bytes(current_setting('pg_flashback.local_max_restore_peak_bytes', true)) > 0
        AND pg_size_bytes(current_setting('pg_flashback.local_min_filesystem_bytes', true)) > 0;")"
    [[ "$cap_ok" == "t" ]] || preflight_fail "capacity GUCs local_max_snapshot_bytes/local_max_restore_peak_bytes/local_min_filesystem_bytes must be > 0"

    SLOT_NAME="$(psqlq -c "SELECT flashback_effective_slot_name();")"
    [[ -n "$SLOT_NAME" ]] || preflight_fail "flashback_effective_slot_name() returned empty"
    export SLOT_NAME

    # Slot must exist for this database before any bench DDL. pg_replication_slots.active
    # is only true during short capture bursts (idle backoff), so require existence +
    # at least one observed active sample (or confirmed_flush progress) while the
    # capture worker is already admitted/running above.
    local slot_deadline slot_active slot_exists seen_active=0
    slot_deadline=$(( $(date +%s) + 90 ))
    slot_active=f
    while (( $(date +%s) <= slot_deadline )); do
        slot_exists="$(psqlq -c "SELECT EXISTS (
            SELECT 1 FROM pg_replication_slots
             WHERE slot_name = '$SLOT_NAME' AND database = current_database());")"
        [[ "$slot_exists" == "t" ]] || { sleep 0.2; continue; }
        slot_active="$(psqlq -c "SELECT COALESCE(
            (SELECT active::text FROM pg_replication_slots
              WHERE slot_name = '$SLOT_NAME' AND database = current_database()), 'f');")"
        if [[ "$slot_active" == "t" ]]; then
            seen_active=1
            break
        fi
        # Nudge consume so a healthy capture path advances / attaches.
        psqlq -c "SELECT flashback_consume_wal(65536);" >/dev/null 2>&1 || true
        sleep 0.2
    done
    [[ "$slot_exists" == "t" ]] || preflight_fail "logical replication slot $SLOT_NAME missing for current database (qualification bootstrap must create it before bench)"
    if (( seen_active == 0 )); then
        # Accept a healthy idle slot when capture is running and lag is readable:
        # active flickers false between decode cycles and may be missed.
        lag="$(slot_lag_bytes)"
        [[ -n "$lag" && "$lag" =~ ^[0-9]+$ ]] \
            || preflight_fail "logical replication slot $SLOT_NAME never observed active and lag is unreadable"
    fi

    lag="$(slot_lag_bytes)"
    [[ -n "$lag" && "$lag" =~ ^[0-9]+$ ]] || preflight_fail "could not read slot lag for $SLOT_NAME"
    START_SLOT_LAG=$lag
    export START_SLOT_LAG
    (( lag <= MAX_START_LAG_BYTES )) || preflight_fail "slot lag ${lag}B exceeds PG_FLASHBACK_BENCH_MAX_START_LAG_BYTES=${MAX_START_LAG_BYTES}B"

    leftovers="$(psqlq -c "SELECT count(*) FROM flashback.tracked_tables tt
        WHERE tt.is_active AND (
          tt.table_name LIKE 'bench\_%' ESCAPE '\'
          OR (tt.table_name LIKE 'b\_%' ESCAPE '\' AND tt.table_name NOT LIKE 'b_${RUN_ID_SAFE}_%')
        );")"
    (( leftovers == 0 )) || preflight_fail "found $leftovers active bench/b_* tracked_tables from prior runs (not matching RUN_ID $RUN_ID_SAFE)"

    PREFLIGHT_JSON="$workers_json"
    return 0
}
PREFLIGHT_JSON=""
bench_preflight || die "preflight failed"

host_meta="$(psqlq <<'SQL'
SELECT jsonb_build_object(
  'postgresql_version', version(),
  'database', current_database(),
  'port', current_setting('port'),
  'data_directory', current_setting('data_directory'),
  'server_encoding', current_setting('server_encoding'),
  'shared_buffers', current_setting('shared_buffers'),
  'max_worker_processes', current_setting('max_worker_processes'),
  'max_slot_wal_keep_size', current_setting('max_slot_wal_keep_size'),
  'pg_flashback_enabled', current_setting('pg_flashback.enabled'),
  'pg_flashback_capture_mode', current_setting('pg_flashback.capture_mode'),
  'local_max_snapshot_bytes', current_setting('pg_flashback.local_max_snapshot_bytes', true),
  'local_max_restore_peak_bytes', current_setting('pg_flashback.local_max_restore_peak_bytes', true),
  'local_min_filesystem_bytes', current_setting('pg_flashback.local_min_filesystem_bytes', true),
  'filesystem_available_bytes', flashback_tablespace_filesystem_available_bytes(0)
);
SQL
)"

SOURCE_HEAD="$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
EXT_SHA256="$(extension_sha256)"

run_one_table() {
    local size_label=$1 bytes=$2 shape=$3 rel=$4 outfile=$5
    local ddl rows load churn tid=""
    case "$shape" in
      narrow)
        ddl="CREATE TABLE $rel (id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v int NOT NULL);"
        rows=$(( bytes / 16 ))
        load="INSERT INTO $rel(v) SELECT g FROM generate_series(1,$rows) g;"
        churn=""
        ;;
      indexed)
        ddl="CREATE TABLE $rel (
              id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
              a int NOT NULL, b int NOT NULL, c text NOT NULL,
              UNIQUE (a), CHECK (b >= 0));
             CREATE INDEX ON $rel (b); CREATE INDEX ON $rel (c);"
        rows=$(( bytes / 64 ))
        load="INSERT INTO $rel(a,b,c) SELECT g, g%1000, 'x'||g FROM generate_series(1,$rows) g;"
        churn=""
        ;;
      toast)
        ddl="CREATE TABLE $rel (id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, blob text NOT NULL);"
        rows=$(( bytes / 2048 ))
        load="INSERT INTO $rel(blob) SELECT repeat('z', 2048) FROM generate_series(1,$rows) g;"
        churn=""
        ;;
      churn)
        ddl="CREATE TABLE $rel (id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v int NOT NULL, note text);"
        rows=$(( bytes / 32 ))
        load="INSERT INTO $rel(v,note) SELECT g, 'n' FROM generate_series(1,$rows) g;"
        churn="UPDATE $rel SET v = v + 1 WHERE id % 3 = 0;
               DELETE FROM $rel WHERE id % 11 = 0;
               INSERT INTO $rel(v,note) SELECT g, 'c' FROM generate_series($((rows+1)), $((rows + rows/10))) g;"
        ;;
      *) echo "unknown shape $shape" >&2; exit 2 ;;
    esac

    psqlq -c "DROP TABLE IF EXISTS $rel CASCADE;" >/dev/null 2>&1 || true
    local t0 t_protect h
    t0=$(date +%s%N)
    psqlq -c "$ddl"
    psqlq -c "SELECT flashback_track('$rel');"
    tid="$(psqlq -c "SELECT tracking_id FROM flashback.tracked_tables
                    WHERE is_active AND format('%I.%I', schema_name, table_name) = '$rel'
                    ORDER BY tracking_id DESC LIMIT 1;")"
    for _ in $(seq 1 240); do
      h=$(psqlq -c "SELECT flashback_lifecycle_health('$rel');")
      [[ "$h" == "healthy" ]] && break
      sleep 0.25
    done
    t_protect=$(( ($(date +%s%N) - t0) / 1000000 ))

    local disk_before disk_after_dml
    disk_before=$(psqlq -c "SELECT COALESCE((flashback_disk_retention_status())->>'used_bytes','0')::bigint;")
    psqlq -c "$load"
    if [[ -n "$churn" ]]; then
      psqlq -c "$churn"
    fi
    for _ in $(seq 1 240); do
      h=$(psqlq -c "SELECT flashback_lifecycle_health('$rel');")
      [[ "$h" == "healthy" ]] && break
      sleep 0.25
    done
    disk_after_dml=$(psqlq -c "SELECT COALESCE((flashback_disk_retention_status())->>'used_bytes','0')::bigint;")
    local fp rows_live
    fp=$(psqlq -c "SELECT md5(count(*)::text || ':' || coalesce(sum(hashtext(t::text)),0)::text) FROM $rel t;")
    rows_live=$(psqlq -c "SELECT count(*) FROM $rel;")

    # Large inserts leave the slot far behind the DROP LSN; scale catch-up wait
    # with target bytes (floor 300s + ~2s/MiB, ceiling CATCHUP_TIMEOUT).
    local catchup_timeout_secs catchup_deadline size_based_secs
    catchup_timeout_secs="${PG_FLASHBACK_BENCH_CATCHUP_TIMEOUT_SECS:-1800}"
    if (( catchup_timeout_secs < 300 )); then catchup_timeout_secs=300; fi
    size_based_secs=$(( 300 + (bytes / (1024 * 1024)) * 2 ))
    if (( size_based_secs > catchup_timeout_secs )); then size_based_secs=$catchup_timeout_secs; fi

    local t1 t_discover st
    t1=$(date +%s%N)
    psqlq -c "DROP TABLE $rel;"
    catchup_deadline=$(( $(date +%s) + size_based_secs ))
    st=""
    local catchup_i=0
    while (( $(date +%s) <= catchup_deadline )); do
      st=$(psqlq -c "SELECT status FROM flashback_disaster_points('$rel', interval '1 day') WHERE event_type='DROP' ORDER BY disaster_commit_lsn DESC LIMIT 1;")
      [[ "$st" == "restorable" ]] && break
      # Avoid fighting the capture worker for the logical slot: nudge rarely.
      if (( catchup_i % 8 == 0 )); then
        psqlq -c "SELECT flashback_consume_wal(65536);" >/dev/null 2>&1 || true
      fi
      catchup_i=$((catchup_i + 1))
      sleep 0.5
    done
    t_discover=$(( ($(date +%s%N) - t1) / 1000000 ))

    local t2 t_dry plan plan_status token plan_code
    t2=$(date +%s%N)
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
    t_dry=$(( ($(date +%s%N) - t2) / 1000000 ))
    token=$(printf '%s' "$plan" | jq -r '.plan_token // empty')
    [[ "$plan_status" == "restorable" && -n "$token" && "$token" != "null" ]] \
      || { echo "FAIL plan $rel after ${size_based_secs}s catch-up: $plan" >&2; exit 1; }

    local t3 op exec_err exec_rc t_recover fp2 ok disk_peak
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
    t_recover=$(( ($(date +%s%N) - t3) / 1000000 ))
    fp2=$(psqlq -c "SELECT md5(count(*)::text || ':' || coalesce(sum(hashtext(t::text)),0)::text) FROM $rel t;")
    ok=$([[ "$fp" == "$fp2" && "$st" == "verified" ]] && echo true || echo false)
    disk_peak=$(psqlq -c "SELECT COALESCE((flashback_disk_retention_status())->>'used_bytes','0')::bigint;")

    local t4 t_maintain_plan
    t4=$(date +%s%N)
    psqlq -c "SELECT flashback_maintain_plan('$rel');" >/dev/null || true
    t_maintain_plan=$(( ($(date +%s%N) - t4) / 1000000 ))

    local t5 t_cleanup
    t5=$(date +%s%N)
    cleanup_table_lifecycle "$rel" "$tid"
    t_cleanup=$(( ($(date +%s%N) - t5) / 1000000 ))

    jq -n \
      --arg size "$size_label" --argjson bytes "$bytes" --arg shape "$shape" \
      --arg rel "$rel" --argjson protect_ms "$t_protect" --argjson discover_ms "$t_discover" \
      --argjson dry_run_ms "$t_dry" --argjson recover_ms "$t_recover" \
      --argjson maintain_plan_ms "$t_maintain_plan" --argjson cleanup_ms "$t_cleanup" \
      --argjson rows "$rows_live" --argjson disk_before "$disk_before" \
      --argjson disk_after_dml "$disk_after_dml" --argjson disk_peak "$disk_peak" \
      --argjson ok "$ok" --arg op_state "$st" --arg status "ran" \
      '{
        size_label:$size, target_bytes:$bytes, shape:$shape, table:$rel,
        protect_ms:$protect_ms, drop_discovery_ms:$discover_ms, dry_run_ms:$dry_run_ms,
        recover_ms:$recover_ms, maintain_plan_ms:$maintain_plan_ms, cleanup_ms:$cleanup_ms,
        rows:$rows, disk_before_bytes:$disk_before, disk_after_dml_bytes:$disk_after_dml,
        disk_peak_bytes:$disk_peak, fingerprint_ok:$ok, operation_state:$op_state, status:$status
      }' > "$outfile"
}
export -f run_one_table

RESULTS='[]'
tier_blocked=0
tier_block_reason=""
shape_index=0

for size_label in $SIZES; do
  bytes="$(size_to_bytes "$size_label")"

  if (( tier_blocked )); then
    for shape in $SHAPES; do
      echo "BLOCKED $size_label/$shape: earlier smaller tier did not pass ($tier_block_reason)" >&2
      RESULTS=$(jq -n --argjson acc "$RESULTS" --arg size "$size_label" --argjson bytes "$bytes" \
        --arg shape "$shape" --arg reason "$tier_block_reason" \
        '$acc + [{size_label:$size, target_bytes:$bytes, shape:$shape, status:"blocked", reason:$reason}]')
    done
    continue
  fi

  need=$((bytes * 4))
  avail="$(printf '%s' "$host_meta" | jq -r '.filesystem_available_bytes // 0')"
  if (( need > MAX_TEMP_BYTES )); then
    echo "SKIP $size_label: projected temp $need exceeds MAX_TEMP_BYTES=$MAX_TEMP_BYTES" >&2
    for shape in $SHAPES; do
      RESULTS=$(jq -n --argjson acc "$RESULTS" --arg size "$size_label" --argjson bytes "$bytes" --arg shape "$shape" \
        '$acc + [{size_label:$size, target_bytes:$bytes, shape:$shape, status:"skipped", reason:"exceeds_max_temp_bytes"}]')
    done
    continue
  fi
  if (( avail > 0 && (need + FS_RESERVE_BYTES) > avail )); then
    echo "SKIP $size_label: need~$need + reserve=$FS_RESERVE_BYTES but filesystem_available=$avail" >&2
    for shape in $SHAPES; do
      RESULTS=$(jq -n --argjson acc "$RESULTS" --arg size "$size_label" --argjson bytes "$bytes" --arg shape "$shape" \
        '$acc + [{size_label:$size, target_bytes:$bytes, shape:$shape, status:"skipped", reason:"insufficient_filesystem_reserve"}]')
    done
    continue
  fi

  tier_had_failure=0
  shape_index=0
  for shape in $SHAPES; do
    if (( shape_index > 0 )); then
      wait_slot_lag_near_start "after-${shape_index}"
    fi
    shape_index=$((shape_index + 1))

    size_slug=$(printf '%s' "$size_label" | tr '[:upper:]' '[:lower:]')
    rel="public.b_${RUN_ID_SAFE}_${shape}_${size_slug}"
    CREATED_TABLES+=("$rel")
    echo "== $rel =="

    resfile="$(mktemp "${RESULT_DIR}/one-result.XXXXXX.json")"
    timed_out=0
    rc=0
    if [[ -n "$TIMEOUT_BIN" ]]; then
      if ! "$TIMEOUT_BIN" "${TABLE_TIMEOUT_SECS}s" bash -c \
          'run_one_table "$1" "$2" "$3" "$4" "$5"' \
          _ "$size_label" "$bytes" "$shape" "$rel" "$resfile"; then
        rc=$?
        [[ $rc -eq 124 ]] && timed_out=1
      fi
    else
      run_one_table "$size_label" "$bytes" "$shape" "$rel" "$resfile" || rc=$?
    fi

    if [[ $timed_out -eq 1 ]]; then
      echo "TIMEOUT $rel after ${TABLE_TIMEOUT_SECS}s" >&2
      tid="$(psqlq -c "SELECT tracking_id FROM flashback.tracked_tables
                       WHERE format('%I.%I', schema_name, table_name) = '$rel'
                       ORDER BY tracking_id DESC LIMIT 1;" 2>/dev/null || true)"
      cleanup_table_lifecycle "$rel" "$tid"
      remove_from_created "$rel"
      RESULTS=$(jq -n --argjson acc "$RESULTS" --arg size "$size_label" --argjson bytes "$bytes" \
        --arg shape "$shape" --arg rel "$rel" --argjson timeout "$TABLE_TIMEOUT_SECS" \
        '$acc + [{size_label:$size, target_bytes:$bytes, shape:$shape, table:$rel, status:"timeout", timeout_secs:$timeout, fingerprint_ok:false}]')
      tier_had_failure=1
    elif [[ -s "$resfile" ]] && jq -e . >/dev/null 2>&1 < "$resfile"; then
      RESULTS=$(jq -n --argjson acc "$RESULTS" --argjson one "$(cat "$resfile")" '$acc + [$one]')
      remove_from_created "$rel"
      [[ "$(jq -r '.fingerprint_ok' "$resfile")" == "true" ]] || tier_had_failure=1
    else
      echo "FAIL $rel: no result produced (rc=$rc)" >&2
      tid="$(psqlq -c "SELECT tracking_id FROM flashback.tracked_tables
                       WHERE format('%I.%I', schema_name, table_name) = '$rel'
                       ORDER BY tracking_id DESC LIMIT 1;" 2>/dev/null || true)"
      cleanup_table_lifecycle "$rel" "$tid"
      remove_from_created "$rel"
      RESULTS=$(jq -n --argjson acc "$RESULTS" --arg size "$size_label" --argjson bytes "$bytes" \
        --arg shape "$shape" --arg rel "$rel" \
        '$acc + [{size_label:$size, target_bytes:$bytes, shape:$shape, table:$rel, status:"error", fingerprint_ok:false}]')
      tier_had_failure=1
    fi
    rm -f "$resfile"
  done

  if (( tier_had_failure )); then
    tier_blocked=1
    tier_block_reason="tier $size_label had a failing/timed-out shape"
  fi
done

trap - EXIT
cleanup_all_created

[[ -n "$START_SLOT_LAG" && "$START_SLOT_LAG" =~ ^[0-9]+$ ]] || START_SLOT_LAG=0
[[ -n "$PREFLIGHT_JSON" ]] || PREFLIGHT_JSON='{}'
printf '%s' "$PREFLIGHT_JSON" | jq -e . >/dev/null 2>&1 || PREFLIGHT_JSON='{}'
printf '%s' "$RESULTS" | jq -e . >/dev/null 2>&1 || die "results accumulator is not valid JSON"
printf '%s' "$host_meta" | jq -e . >/dev/null 2>&1 || die "host metadata is not valid JSON"

jq -n \
  --argjson host "$host_meta" \
  --argjson results "$RESULTS" \
  --arg run "$RUN_ID" \
  --arg run_id_safe "$RUN_ID_SAFE" \
  --arg source_head "$SOURCE_HEAD" \
  --arg ext_sha256 "$EXT_SHA256" \
  --argjson preflight "$PREFLIGHT_JSON" \
  --argjson start_slot_lag "$START_SLOT_LAG" \
  --argjson max_start_lag_bytes "$MAX_START_LAG_BYTES" \
  --arg slot_name "$SLOT_NAME" \
  '{
    schema_version: 2,
    run_id: $run,
    run_id_safe: $run_id_safe,
    source_head: $source_head,
    extension_binary_sha256: $ext_sha256,
    worker_preflight: $preflight,
    start_slot_lag_bytes: $start_slot_lag,
    max_start_lag_bytes: $max_start_lag_bytes,
    slot_name: $slot_name,
    host: $host,
    results: $results,
    note: "Support envelope is host/config specific; staged tiers block larger sizes after failure."
  }' > "$OUT_JSON"
echo "Wrote $OUT_JSON"
jq -r '.results[] | "\(.size_label)\t\(.shape)\t\(.status // "ran")\tprotect=\(.protect_ms // "-")ms recover=\(.recover_ms // "-")ms ok=\(.fingerprint_ok // false)"' "$OUT_JSON"

failed_n="$(jq '[.results[] | select(
    (.fingerprint_ok == false and (.status // "") != "blocked" and (.status // "") != "skipped")
    or (.status == "error") or (.status == "timeout")
  )] | length' "$OUT_JSON")"
blocked_after_fail_n="$(jq '[.results[] | select(.status == "blocked")] | length' "$OUT_JSON")"
if (( failed_n > 0 )); then
  echo "FAIL: $failed_n envelope shape(s) failed (plus $blocked_after_fail_n blocked)" >&2
  exit 1
fi
echo "byte_support_envelope_bench PASS"
