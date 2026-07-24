#!/usr/bin/env bash
# Local PG17 qualification instance: isolated cluster + matrix/bench orchestration.
#
# Usage: ./scripts/run_local_qualification_instance.sh
# Env:   PG_CONFIG, PGFB_QUAL_PORT, PGFB_QUAL_KEEP=1, PGFB_QUAL_MODE=bench|matrices|all

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PG_CONFIG="${PG_CONFIG:-/usr/local/pgsql-17/bin/pg_config}"
PG_BIN="$("$PG_CONFIG" --bindir)"
SHARE_DIR="$("$PG_CONFIG" --sharedir)"
PSQL="$PG_BIN/psql"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
MODE="${PGFB_QUAL_MODE:-all}"
WORK_ROOT="$ROOT/target/qual/$RUN_ID"
DATA="$WORK_ROOT/data"
LOG_DIR="$WORK_ROOT/log"
LOG="$LOG_DIR/postgresql.log"
SOCKET="/tmp/pgfb-qual-$RUN_ID"
SUMMARY_JSON="$WORK_ROOT/summary.json"
DB_NAME=pgfb_qual
FAILED=0
declare -a STEP_RESULTS=()

die() { echo "FAIL: $*" >&2; exit 1; }

port_is_free() {
    local p=$1
    # Return 0 when nothing is listening on TCP port $p.
    if command -v ss >/dev/null 2>&1; then
        if ss -ltnH 2>/dev/null | awk -v p=":$p" '
            $4 ~ p"$" || $4 ~ p"]$" { found=1; exit }
            END { exit found ? 0 : 1 }
        '; then
            return 1 # listener present → not free
        fi
        return 0
    fi
    if (echo >/dev/tcp/127.0.0.1/"$p") 2>/dev/null; then
        return 1
    fi
    return 0
}

choose_port() {
    local p="${PGFB_QUAL_PORT:-39117}"
    if port_is_free "$p"; then
        echo "$p"
        return 0
    fi
    local _
    for _ in $(seq 1 50); do
        p=$((40000 + RANDOM % 20000))
        if port_is_free "$p"; then
            echo "$p"
            return 0
        fi
    done
    die "could not find a free TCP port for qualification instance"
}

PORT="$(choose_port)"

record_step() {
    local name=$1 status=$2 detail=${3:-}
    STEP_RESULTS+=("$(jq -n --arg n "$name" --arg s "$status" --arg d "$detail" \
        '{name:$n,status:$s,detail:$d}')")
}

cleanup() {
    local rc=$?
    set +e
    # Keep-alive mode leaves the instance running for follow-on benches.
    if [[ "${PGFB_QUAL_KEEP:-0}" != "1" && -x "$PG_BIN/pg_ctl" && -d "$DATA" ]]; then
        "$PG_BIN/pg_ctl" -D "$DATA" stop -m fast -w >/dev/null 2>&1 || \
            "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1 || true
    fi
    local steps_json overall sticky
    steps_json=$(printf '%s\n' "${STEP_RESULTS[@]:-}" | jq -s '.')
    overall=FAIL
    [[ $FAILED -eq 0 && $rc -eq 0 ]] && overall=PASS
    mkdir -p "$WORK_ROOT" "$ROOT/target/qual"
    sticky="$ROOT/target/qual/summary-$RUN_ID.json"
    jq -n \
        --arg run_id "$RUN_ID" \
        --arg mode "$MODE" \
        --arg status "$overall" \
        --argjson failed "$FAILED" \
        --argjson exit_code "$rc" \
        --arg port "$PORT" \
        --arg socket "$SOCKET" \
        --arg data_dir "$DATA" \
        --arg summary_path "$sticky" \
        --argjson steps "$steps_json" \
        '{run_id:$run_id,mode:$mode,status:$status,failed_steps:$failed,exit_code:$exit_code,
          cluster:{port:($port|tonumber),socket:$socket,data_directory:$data_dir,database:"pgfb_qual"},
          summary_path:$summary_path,steps:$steps}' >"$sticky" 2>/dev/null || true
    cp -f "$sticky" "$SUMMARY_JSON" 2>/dev/null || true
    if [[ "${PGFB_QUAL_KEEP:-0}" != "1" ]]; then
        # Keep sticky summary; remove instance data/socket only.
        rm -rf "$DATA" "$LOG_DIR" "$SOCKET"
        # Drop empty run dir if possible; sticky lives under target/qual/.
        rmdir "$WORK_ROOT" 2>/dev/null || true
    else
        echo "PGFB_QUAL_KEEP=1: leaving $WORK_ROOT and $SOCKET" >&2
    fi
    echo "Qualification instance: $overall ($sticky)" >&2
    [[ $FAILED -eq 0 && $rc -eq 0 ]] || exit 1
    exit 0
}
trap cleanup EXIT

[[ -x "$PSQL" ]] || die "psql not found via $PG_CONFIG"
[[ -f "$SHARE_DIR/extension/pg_flashback.control" ]] || die "extension not installed in $SHARE_DIR"

mkdir -p "$WORK_ROOT" "$LOG_DIR" "$SOCKET"
"$PG_BIN/initdb" -D "$DATA" --locale=C.UTF-8 -A trust >/dev/null
cat >>"$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_flashback'
wal_level = logical
max_replication_slots = 16
max_wal_senders = 16
max_worker_processes = 16
max_slot_wal_keep_size = '4GB'
track_commit_timestamp = on
fsync = on
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
pg_flashback.worker_interval_ms = 25
pg_flashback.max_workers = 4
pg_flashback.target_databases = 'postgres'
pg_flashback.local_max_snapshot_bytes = 8GB
pg_flashback.local_max_restore_peak_bytes = 16GB
pg_flashback.local_min_filesystem_bytes = 64MB
pg_flashback.local_safety_reserve_bytes = 16MB
pg_flashback.allow_unaudited_restore = on
EOF
"$PG_BIN/pg_ctl" -D "$DATA" -l "$LOG" -o "-p $PORT -k $SOCKET" start -w >/dev/null

q() {
    local db=${1:?}; shift
    "$PSQL" -X -h "$SOCKET" -p "$PORT" -d "$db" -v ON_ERROR_STOP=1 -qAtc "$*"
}

restart_pg() {
    "$PG_BIN/pg_ctl" -D "$DATA" restart -w -t 120 -l "$LOG" >/dev/null
    local _
    for _ in $(seq 1 60); do
        q postgres "SELECT 1" >/dev/null 2>&1 && return 0
        sleep 0.5
    done
    return 1
}

wait_flashback_ready() {
    local db=$1 timeout_s=${2:-120}
    local deadline state cap maint
    deadline=$(( $(date +%s) + timeout_s ))
    while (( $(date +%s) <= deadline )); do
        state="$(q "$db" "SELECT admission_state FROM flashback_worker_readiness();")"
        cap="$(q "$db" "SELECT capture_running FROM flashback_worker_readiness();")"
        maint="$(q "$db" "SELECT maintenance_running FROM flashback_worker_readiness();")"
        if [[ "$state" == "ready" && "$cap" == "t" && "$maint" == "t" ]]; then
            return 0
        fi
        sleep 0.5
    done
    return 1
}

q postgres "CREATE DATABASE $DB_NAME;" >/dev/null
q "$DB_NAME" "CREATE EXTENSION pg_flashback;" >/dev/null
q postgres "ALTER SYSTEM SET pg_flashback.target_databases = '$DB_NAME';" >/dev/null
restart_pg || die "restart after target_databases"
wait_flashback_ready "$DB_NAME" 120 || die "flashback_worker_readiness not ready for $DB_NAME"

# Product creates the logical slot on first protect. Do a tiny warmup lifecycle
# so bench preflight sees an existing slot, capture attachment, and near-zero lag
# with no leftover tracked tables.
warmup_logical_slot() {
    local db=$1
    local rel=public._pgfb_qual_warmup_$$
    local slot tid st lag
    q "$db" "CREATE TABLE ${rel} (id int PRIMARY KEY);" >/dev/null
    # flashback_track must be the first write in its transaction.
    q "$db" "SELECT flashback_track('${rel}');" >/dev/null
    slot="$(q "$db" "SELECT flashback_effective_slot_name();")"
    [[ -n "$slot" ]] || return 1
    q "$db" "INSERT INTO ${rel} VALUES (1);" >/dev/null
    tid="$(q "$db" "SELECT tracking_id FROM flashback.tracked_tables
                     WHERE format('%I.%I', schema_name, table_name) = '${rel}'
                     ORDER BY tracking_id DESC LIMIT 1;")"
    [[ -n "$tid" ]] || return 1
    for _ in $(seq 1 60); do
        q "$db" "SELECT flashback_consume_wal(65536);" >/dev/null 2>&1 || true
        lag="$(q "$db" "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn),0)::bigint
                        FROM pg_replication_slots
                        WHERE slot_name = '${slot}' AND database = current_database();")"
        [[ -n "$lag" && "$lag" -le 1048576 ]] && break
        sleep 0.25
    done
    q "$db" "SELECT flashback_unprotect('${rel}');" >/dev/null 2>&1 || true
    for _ in $(seq 1 80); do
        q "$db" "SELECT flashback_finalize_unprotect_operations();" >/dev/null 2>&1 || true
        st="$(q "$db" "SELECT COALESCE((SELECT protection_state FROM flashback.tracked_tables
                         WHERE tracking_id = ${tid}::bigint LIMIT 1), 'gone');")"
        [[ "$st" == "unprotected" || "$st" == "gone" || "$st" == "cleaned" || "$st" == "inactive" ]] && break
        sleep 0.25
    done
    q "$db" "BEGIN; SET LOCAL pg_flashback.enabled = on; SELECT flashback_cleanup(${tid}::bigint, false); COMMIT;" \
        >/dev/null 2>&1 || true
    q "$db" "BEGIN; SET LOCAL pg_flashback.enabled = on; DROP TABLE IF EXISTS ${rel} CASCADE; COMMIT;" \
        >/dev/null 2>&1 || true
    q "$db" "SELECT EXISTS (
        SELECT 1 FROM pg_replication_slots
         WHERE slot_name = '${slot}' AND database = current_database());" | grep -qx t
}
warmup_logical_slot "$DB_NAME" || die "warmup logical slot lifecycle failed"

export PGHOST="$SOCKET" PGPORT="$PORT" PGDATABASE="$DB_NAME"
PGUSER="$(id -un)"
export PGUSER
record_step "cluster_bootstrap" "pass" "port=$PORT socket=$SOCKET"

run_child() {
    local name=$1; shift
    echo "== $name ==" >&2
    set +e
    "$@"
    local crc=$?
    set -e
    if (( crc == 0 )); then
        record_step "$name" "pass" "exit=0"
    else
        record_step "$name" "fail" "exit=$crc"
        FAILED=$((FAILED + 1))
    fi
    return 0
}

# Matrices first (fast correctness), then staged byte envelope (slow).
if [[ "$MODE" == "matrices" || "$MODE" == "all" ]]; then
    run_child "exact_manifest_matrix" \
        env REQUIRE_LIVE=1 PG_CONFIG="$PG_CONFIG" "$ROOT/scripts/run_exact_manifest_matrix.sh"
    run_child "independent_restore_proof_matrix" \
        env REQUIRE_LIVE=1 PG_CONFIG="$PG_CONFIG" "$ROOT/scripts/run_independent_restore_proof_matrix.sh"
    run_child "metadata_failpoint_matrix" \
        env REQUIRE_LIVE=1 PG_CONFIG="$PG_CONFIG" "$ROOT/scripts/run_metadata_failpoint_matrix.sh"
    run_child "local_compatibility_matrix" \
        env REQUIRE_LIVE=1 PG_CONFIG="$PG_CONFIG" "$ROOT/scripts/run_local_compatibility_matrix.sh"
    run_child "maintain_lifecycle_e2e" \
        env REQUIRE_LIVE=1 PGDATABASE="$DB_NAME" PGHOST="$SOCKET" PGPORT="$PORT" \
            "$ROOT/scripts/run_maintain_lifecycle_e2e.sh" "$PG_BIN"
fi

if [[ "$MODE" == "bench" || "$MODE" == "all" ]]; then
    BENCH_SIZES="${PG_FLASHBACK_BENCH_SIZES:-10MiB 100MiB 500MiB 1GiB}"
    BENCH_SHAPES="${PG_FLASHBACK_BENCH_SHAPES:-narrow indexed toast churn}"
    run_child "byte_support_envelope_bench" \
        env PG_FLASHBACK_BENCH_RUN_ID="$RUN_ID" \
            PG_FLASHBACK_BENCH_SIZES="$BENCH_SIZES" \
            PG_FLASHBACK_BENCH_SHAPES="$BENCH_SHAPES" \
            PG_FLASHBACK_BENCH_RESULT_DIR="$WORK_ROOT/bench" \
            PG_CONFIG="$PG_CONFIG" \
            "$ROOT/scripts/run_byte_support_envelope_bench.sh"
fi

[[ "$MODE" == "bench" || "$MODE" == "matrices" || "$MODE" == "all" ]] \
    || die "unknown PGFB_QUAL_MODE=$MODE (use bench|matrices|all)"

if (( FAILED > 0 )); then
    die "$FAILED qualification step(s) failed"
fi
echo "Qualification instance PASS ($WORK_ROOT)" >&2
