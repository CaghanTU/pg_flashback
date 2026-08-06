#!/usr/bin/env bash
# shellcheck disable=SC2034  # globals are read by the sourcing caller script
# Shared preflight/cleanup/slot-lag helpers for the byte-benchmark scripts.
# Sourced by run_byte_table_size_bench.sh and run_byte_churn_bench.sh. Never
# invoked directly. Requires: ROOT, RUN_ID_SAFE already set by the caller.
#
# Public entrypoints:
#   bench_common_init                  (sets PSQL_BIN/PG_CONFIG exports, traps)
#   psqlq <psql args...>
#   size_to_bytes <label>
#   extension_sha256
#   slot_lag_bytes
#   wait_slot_lag_near_start <label>
#   bench_preflight                    (sets PREFLIGHT_JSON, SLOT_NAME, START_SLOT_LAG)
#   cleanup_table_lifecycle <rel> [tid]
#   remove_from_created <rel>
#   host_meta_json

PG_CONFIG="${PG_CONFIG:-/usr/local/pgsql-17/bin/pg_config}"
PSQL_BIN="${PSQL_BIN:-psql}"
MAX_START_LAG_BYTES="${PG_FLASHBACK_BENCH_MAX_START_LAG_BYTES:-$((16 * 1024 * 1024))}"
LAG_WAIT_TIMEOUT_SECS="${PG_FLASHBACK_BENCH_LAG_WAIT_TIMEOUT_SECS:-600}"
LAG_NEAR_START_SLACK_BYTES="${PG_FLASHBACK_BENCH_LAG_NEAR_START_SLACK_BYTES:-1048576}"
UNPROTECT_POLL_MAX="${PG_FLASHBACK_BENCH_UNPROTECT_POLL_MAX:-120}"
export PSQL_BIN PG_CONFIG MAX_START_LAG_BYTES LAG_WAIT_TIMEOUT_SECS
export LAG_NEAR_START_SLACK_BYTES UNPROTECT_POLL_MAX

CREATED_TABLES=()
START_SLOT_LAG=0
SLOT_NAME=""
PREFLIGHT_JSON=""

die() { echo "FAIL: $*" >&2; exit 1; }
export -f die
preflight_fail() { die "$*"; }
export -f preflight_fail

psqlq() { "$PSQL_BIN" -X -v ON_ERROR_STOP=1 -qAt "$@"; }
export -f psqlq

size_to_bytes() {
    case "$1" in
        10MiB) echo $((10 * 1024 * 1024)) ;;
        100MiB) echo $((100 * 1024 * 1024)) ;;
        500MiB) echo $((500 * 1024 * 1024)) ;;
        1GiB) echo $((1024 * 1024 * 1024)) ;;
        10GiB) echo $((10 * 1024 * 1024 * 1024)) ;;
        25GiB) echo $((25 * 1024 * 1024 * 1024)) ;;
        50GiB) echo $((50 * 1024 * 1024 * 1024)) ;;
        *) echo "$1" | awk '/^[0-9]+$/ {print; exit} {exit 1}' || { echo "bad size $1" >&2; exit 2; } ;;
    esac
}
export -f size_to_bytes

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
export -f slot_lag_bytes

wait_slot_lag_near_start() {
    local label=${1:-between-tables} deadline lag threshold
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
export -f wait_slot_lag_near_start

catalog_cleanup_tracking_id() {
    local tid=$1
    [[ -n "$tid" && "$tid" != "null" ]] || return 0
    # flashback.snapshots is an append-only ledger: any DELETE is rejected by
    # flashback_guard_snapshot_artifact() regardless of payload_state, so a
    # stuck row must be retired in place through the real state-authority API
    # first, never deleted directly.
    psqlq -c "SELECT public.flashback_internal_snapshot_retire(snapshot_id, ${tid}::bigint, 'missing')
              FROM flashback.snapshots
              WHERE tracking_id = ${tid}::bigint
                AND payload_state NOT IN ('retired', 'missing', 'aborted');" >/dev/null 2>&1 || true
    # flashback.coverage_generations is also an append-only ledger (its own
    # guard trigger rejects any DELETE unconditionally); only delta_log rows
    # are ever safe to delete directly. Leaving historical generation/snapshot
    # rows in place is correct and harmless -- only tracked_tables.is_active
    # controls whether this tracking_id counts as active going forward.
    psqlq <<SQL >/dev/null 2>&1 || true
BEGIN;
SET LOCAL pg_flashback.enabled = on;
DELETE FROM flashback.delta_log WHERE tracking_id = ${tid}::bigint;
UPDATE flashback.tracked_tables
   SET is_active = false, protection_state = 'cleaned'
 WHERE tracking_id = ${tid}::bigint;
COMMIT;
SQL
}
export -f catalog_cleanup_tracking_id

cleanup_table_lifecycle() {
    local rel=$1 tid=${2:-}
    if [[ -z "$tid" || "$tid" == "null" ]]; then
        tid="$(psqlq -c "SELECT tracking_id FROM flashback.tracked_tables
                          WHERE is_active AND format('%I.%I', schema_name, table_name) = '$rel'
                          ORDER BY tracking_id DESC LIMIT 1;" 2>/dev/null || true)"
    fi
    psqlq -c "SELECT flashback_unprotect('$rel');" >/dev/null 2>&1 || true
    local st2 i
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
export -f cleanup_table_lifecycle

remove_from_created() {
    local target=$1 t
    local -a kept=()
    for t in "${CREATED_TABLES[@]+"${CREATED_TABLES[@]}"}"; do
        [[ "$t" == "$target" ]] || kept+=("$t")
    done
    CREATED_TABLES=("${kept[@]+"${kept[@]}"}")
}

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
        psqlq -c "SELECT flashback_consume_wal(65536);" >/dev/null 2>&1 || true
        sleep 0.2
    done
    [[ "$slot_exists" == "t" ]] || preflight_fail "logical replication slot $SLOT_NAME missing for current database (qualification bootstrap must create it before bench)"
    if (( seen_active == 0 )); then
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
          OR (tt.table_name LIKE 'bts\_%' ESCAPE '\' AND tt.table_name NOT LIKE 'bts_${RUN_ID_SAFE}_%')
          OR (tt.table_name LIKE 'bc\_%' ESCAPE '\' AND tt.table_name NOT LIKE 'bc_${RUN_ID_SAFE}_%')
        );")"
    (( leftovers == 0 )) || preflight_fail "found $leftovers active bench/b_*/bts_*/bc_* tracked_tables from prior runs (not matching RUN_ID $RUN_ID_SAFE)"

    PREFLIGHT_JSON="$workers_json"
    return 0
}

host_meta_json() {
    psqlq <<'SQL'
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
  'snapshot_storage_backend', current_setting('pg_flashback.snapshot_storage_backend', true),
  'external_snapshot_root', current_setting('pg_flashback.external_snapshot_root', true),
  'filesystem_available_bytes', flashback_tablespace_filesystem_available_bytes(0)
);
SQL
}

# Relation-size breakdown (heap/TOAST/index) for a table-size-mode measurement.
relation_size_json() {
    local rel=$1
    psqlq -c "SELECT jsonb_build_object(
        'pg_relation_size', pg_relation_size('$rel'),
        'pg_table_size', pg_table_size('$rel'),
        'pg_total_relation_size', pg_total_relation_size('$rel'),
        'heap_bytes', pg_relation_size('$rel', 'main'),
        'toast_bytes', COALESCE((SELECT pg_total_relation_size(reltoastrelid)
                                  FROM pg_class WHERE oid = '$rel'::regclass AND reltoastrelid <> 0), 0),
        'index_bytes', pg_indexes_size('$rel')
    );"
}
export -f relation_size_json

table_size_bytes() {
    psqlq -c "SELECT pg_table_size('$1');"
}
export -f table_size_bytes

# Per-shape batch inserters: each appends exactly $3 rows starting right
# after row id $2 (0 for the first batch). Used only by
# load_to_target_table_size, which measures real on-disk bytes/row and
# adapts batch size instead of assuming a fixed row width.
insert_batch_narrow() {
    local rel=$1 start=$2 n=$3
    psqlq -c "INSERT INTO $rel(v) SELECT g FROM generate_series($((start + 1)), $((start + n))) g;"
}
export -f insert_batch_narrow

insert_batch_indexed() {
    local rel=$1 start=$2 n=$3
    psqlq -c "INSERT INTO $rel(a,b,c) SELECT g, g%1000, 'x'||g FROM generate_series($((start + 1)), $((start + n))) g;"
}
export -f insert_batch_indexed

# Deterministic but incompressible payload: 16 distinct sha512 digests
# concatenated as hex (128 hex chars each = 2048 bytes/row). A repeated
# character (e.g. repeat('z', 2048)) collapses to a handful of bytes under
# TOAST compression and silently defeats a byte-size target; concatenated
# hash digests are high-entropy and deterministic in the row id alone.
insert_batch_toast() {
    local rel=$1 start=$2 n=$3
    psqlq -c "INSERT INTO $rel(blob)
              SELECT string_agg(encode(sha512((g::text || ':' || i::text)::bytea), 'hex'), '')
              FROM generate_series($((start + 1)), $((start + n))) g
              CROSS JOIN generate_series(1, 16) i
              GROUP BY g;"
}
export -f insert_batch_toast

# Scale shape: roughly 8 KiB of deterministic, poorly-compressible payload per
# row. It keeps 50 GiB qualification at a tractable row count while still
# exercising TOAST, a primary key, a unique constraint, a CHECK, an expression
# index, comments and a non-empty ACL. The whole row remains below the default
# 64 KiB WAL capture ceiling during the 1% post-protect churn.
insert_batch_wide() {
    local rel=$1 start=$2 n=$3
    psqlq -c "INSERT INTO $rel(marker,payload)
              SELECT g,
                     string_agg(
                         encode(sha512((g::text || ':' || i::text)::bytea), 'hex'),
                         '' ORDER BY i
                     )
              FROM generate_series($((start + 1)), $((start + n))) g
              CROSS JOIN generate_series(1, 64) i
              GROUP BY g;"
}
export -f insert_batch_wide

# Loads $rel (already CREATEd empty) up to within +/-10% of $target_bytes as
# measured by pg_table_size (heap+TOAST, excludes indexes -- callers record
# index_bytes separately). Inserts via $insert_fn (one of the insert_batch_*
# above), starting with a small calibration batch to learn real bytes/row for
# this shape, then sizing subsequent batches from that measurement aimed at
# 97% of target to leave headroom against overshoot.
#
# Sets LOAD_TOTAL_ROWS / LOAD_FINAL_BYTES / LOAD_ITERATIONS as plain globals.
# Callers MUST invoke this as a bare statement (`load_to_target_table_size ...`),
# never inside `$(...)`: command substitution forks a subshell, and any
# variable this function sets would be invisible to the caller once that
# subshell exits -- silently discarding the very values callers need.
load_to_target_table_size() {
    local rel=$1 target_bytes=$2 insert_fn=$3
    local tol_low=$(( target_bytes * 90 / 100 ))
    local aim_bytes=$(( target_bytes * 97 / 100 ))
    # Initial batch is deliberately tiny (not sized off any assumed row
    # width): a fixed 1000-row calibration batch overshot a 1MiB toast target
    # by ~2.8x in a single iteration (toast rows run ~2-3KB each), so the
    # first batch must be small enough to stay safe for the *largest*
    # plausible per-row shape, not just the smallest.
    local total_rows=0 current_bytes=0 batch_rows=20 iter=0 max_iterations=80
    while (( current_bytes < tol_low && iter < max_iterations )); do
        iter=$((iter + 1))
        (( batch_rows > 0 )) || batch_rows=1
        "$insert_fn" "$rel" "$total_rows" "$batch_rows" >/dev/null
        total_rows=$((total_rows + batch_rows))
        # `x=$(cmd)` does not trip `set -e` on cmd's own failure (a documented
        # bash gotcha: only the assignment's own exit status matters, and
        # bash does not propagate the substitution's failure through it), so
        # an empty/non-numeric read from psql must be checked explicitly --
        # otherwise a transient connection failure reads as current_bytes=0
        # forever and this loop keeps doubling batch_rows without bound.
        current_bytes=$(table_size_bytes "$rel")
        [[ "$current_bytes" =~ ^[0-9]+$ ]] \
            || die "load_to_target_table_size: pg_table_size('$rel') returned non-numeric '$current_bytes' (connection lost?)"
        if (( total_rows > 0 )); then
            local bytes_per_row=$(( current_bytes / total_rows ))
            (( bytes_per_row < 1 )) && bytes_per_row=1
            if (( current_bytes < aim_bytes )); then
                local remaining=$(( aim_bytes - current_bytes ))
                batch_rows=$(( remaining / bytes_per_row ))
                (( batch_rows < 1 )) && batch_rows=1
            else
                batch_rows=0
            fi
        fi
    done
    LOAD_TOTAL_ROWS=$total_rows
    LOAD_FINAL_BYTES=$current_bytes
    LOAD_ITERATIONS=$iter
}
export -f load_to_target_table_size

# Worker restart/crash count over an observation window, from pg_stat_activity
# PID stability -- a changed PID for the same backend_type between two samples
# means the postmaster relaunched it (crash or manual restart).
worker_restart_count() {
    local backend_type=$1 seconds=$2 pid_prev pid_now restarts=0 elapsed=0
    pid_prev=$(psqlq -c "SELECT pid FROM pg_stat_activity WHERE backend_type='$backend_type' AND datname=current_database();")
    while (( elapsed < seconds )); do
        sleep 1
        elapsed=$((elapsed + 1))
        pid_now=$(psqlq -c "SELECT pid FROM pg_stat_activity WHERE backend_type='$backend_type' AND datname=current_database();")
        if [[ -n "$pid_prev" && -n "$pid_now" && "$pid_now" != "$pid_prev" ]]; then
            restarts=$((restarts + 1))
        fi
        pid_prev="$pid_now"
    done
    echo "$restarts"
}
export -f worker_restart_count
