#!/usr/bin/env bash
# Local PG qualification instance: isolated cluster + matrix/bench orchestration
# against an exact release-candidate archive (never cargo-builds mid-run).
#
# Usage: CANDIDATE_DIR=... ./scripts/run_local_qualification_instance.sh
# Env:   CANDIDATE_DIR (mandatory), PG_BIN (optional override),
#        PGFB_QUAL_PORT, PGFB_QUAL_KEEP=1, PGFB_QUAL_MODE=bench|matrices|all
#
# Evidence contract: the sticky summary at target/qual/summary-$RUN_ID.json is
# PASS only when every step this mode is expected to run is present and
# recorded as pass, the process was not interrupted by a signal, and the
# process's own exit code is 0. A run cut short by SIGHUP/SIGINT/SIGTERM (own
# process or ancestor session) always leaves a FAIL summary with the
# interrupted step and the still-missing steps named explicitly -- it never
# silently reports PASS for a mode that did not finish.

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

[[ -n "${CANDIDATE_DIR:-}" ]] || { echo "FAIL: CANDIDATE_DIR is mandatory" >&2; exit 2; }
# shellcheck source=scripts/lib/exact_candidate_identity.sh
source "$ROOT/scripts/lib/exact_candidate_identity.sh"
# shellcheck source=scripts/lib/qualification_step_tracker.sh
source "$ROOT/scripts/lib/qualification_step_tracker.sh"

PG_CONFIG="${PG_CONFIG:-}"
if [[ -z "$PG_CONFIG" ]]; then
    # exact_candidate_bind_dir derives PG_BIN from the candidate's own
    # manifest pg_major when PG_BIN is not already set; resolve PG_CONFIG
    # from that after binding instead of guessing a PG major up front.
    :
else
    PG_BIN="$("$PG_CONFIG" --bindir)"
fi

exact_candidate_bind_dir "$CANDIDATE_DIR" || exit 1
exact_candidate_install_into_prefix || exit 1
PG_CONFIG="$PG_BIN/pg_config"
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

die() { echo "FAIL: $*" >&2; exit 1; }

case "$MODE" in
    matrices)
        qst_init cluster_bootstrap exact_manifest_matrix independent_restore_proof_matrix \
            metadata_failpoint_matrix local_compatibility_matrix maintain_lifecycle_e2e local_capacity_e2e
        ;;
    bench)
        qst_init cluster_bootstrap byte_support_envelope_bench
        ;;
    all)
        qst_init cluster_bootstrap exact_manifest_matrix independent_restore_proof_matrix \
            metadata_failpoint_matrix local_compatibility_matrix maintain_lifecycle_e2e local_capacity_e2e \
            byte_support_envelope_bench
        ;;
    *)
        die "unknown PGFB_QUAL_MODE=$MODE (use bench|matrices|all)"
        ;;
esac

port_is_free() {
    local p=$1
    if command -v ss >/dev/null 2>&1; then
        if ss -ltnH 2>/dev/null | awk -v p=":$p" '
            $4 ~ p"$" || $4 ~ p"]$" { found=1; exit }
            END { exit found ? 0 : 1 }
        '; then
            return 1
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

trap 'qst_on_signal HUP' HUP
trap 'qst_on_signal INT' INT
trap 'qst_on_signal TERM' TERM

cleanup() {
    local rc=$?
    set +e

    local identity_json='{}'
    if [[ "${EC_BOUND:-0}" == "1" ]]; then
        identity_json="$(exact_candidate_identity_json 2>/dev/null || echo '{}')"
    fi
    local extra_json
    extra_json="$(jq -n --arg port "$PORT" --arg socket "$SOCKET" --arg data_dir "$DATA" \
        --argjson identity "$identity_json" \
        '{cluster:{port:($port|tonumber),socket:$socket,data_directory:$data_dir,database:"pgfb_qual"},
          identity:$identity}')"

    mkdir -p "$WORK_ROOT" "$ROOT/target/qual"
    local sticky="$ROOT/target/qual/summary-$RUN_ID.json"
    local summary_json
    summary_json="$(qst_compute_summary_json "$RUN_ID" "$MODE" "$rc" "$extra_json")"
    qst_write_summary_atomic "$sticky" "$summary_json"
    local overall
    overall="$(jq -r '.status' "$sticky" 2>/dev/null || echo FAIL)"
    cp -f "$sticky" "$SUMMARY_JSON" 2>/dev/null || true

    # Restore the shared prefix regardless of outcome; the candidate .so must
    # never be left installed as a side effect of this script's own failure.
    if [[ "${EC_BOUND:-0}" == "1" && ( "$overall" != "PASS" || "${PGFB_QUAL_KEEP:-0}" != "1" ) ]]; then
        exact_candidate_restore_prefix 2>/dev/null || echo "WARN: prefix restore failed" >&2
    fi

    if [[ "$overall" == "PASS" && "${PGFB_QUAL_KEEP:-0}" != "1" ]]; then
        "$PG_BIN/pg_ctl" -D "$DATA" stop -m fast -w >/dev/null 2>&1 || \
            "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1 || true
        rm -rf "$DATA" "$LOG_DIR" "$SOCKET"
        rmdir "$WORK_ROOT" 2>/dev/null || true
    elif [[ "$overall" == "PASS" ]]; then
        echo "PGFB_QUAL_KEEP=1: leaving $WORK_ROOT and $SOCKET" >&2
    else
        # FAIL or interrupted: stop only this run's own cluster by its own
        # DATA directory; never touch other PostgreSQL processes. Evidence
        # (logs, data dir) is retained, not deleted.
        if [[ -f "$DATA/postmaster.pid" ]]; then
            "$PG_BIN/pg_ctl" -D "$DATA" stop -m fast -w >/dev/null 2>&1 || \
                "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1 || true
        fi
        echo "Evidence retained at $WORK_ROOT (status=$overall)" >&2
    fi

    echo "Qualification instance: $overall ($sticky)" >&2
    [[ "$overall" == "PASS" ]] && exit 0
    exit 1
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
fsync = on
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
pg_flashback.worker_interval_ms = 50
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
qst_mark_step "cluster_bootstrap" "pass" "port=$PORT socket=$SOCKET"

# Reads BENCH_SIZES/BENCH_SHAPES/BENCH_EXPECTED_JSON globals (set just before
# the qst_run_verified_child call below); qst_run_verified_child invokes
# verifiers with no arguments, so this can't take the path as a parameter.
verify_bench_result() {
    local expected_json="$BENCH_EXPECTED_JSON"
    if [[ ! -f "$expected_json" ]]; then
        echo "bench result JSON missing: $expected_json" >&2
        return 1
    fi
    if [[ ! -s "$expected_json" ]]; then
        echo "bench result JSON is empty: $expected_json" >&2
        return 1
    fi
    if ! jq -e . "$expected_json" >/dev/null 2>&1; then
        echo "bench result JSON does not parse: $expected_json" >&2
        return 1
    fi

    local expected_cases actual_cases missing_cases
    expected_cases="$(for sz in $BENCH_SIZES; do for sh in $BENCH_SHAPES; do echo "$sz|$sh"; done; done | sort -u)"
    actual_cases="$(jq -r '.results[]? | "\(.size_label)|\(.shape)"' "$expected_json" | sort -u)"
    missing_cases="$(comm -23 <(printf '%s\n' "$expected_cases") <(printf '%s\n' "$actual_cases") || true)"
    if [[ -n "$missing_cases" ]]; then
        echo "bench result missing requested size/shape case(s):" >&2
        echo "$missing_cases" >&2
        return 1
    fi

    local unverdicted
    unverdicted="$(jq -r '
        .results[]? | select(((.status // "ran") == "ran") and (.fingerprint_ok == null))
        | "\(.size_label)/\(.shape)"
    ' "$expected_json")"
    if [[ -n "$unverdicted" ]]; then
        echo "bench result case(s) ran without a fingerprint_ok verdict: $unverdicted" >&2
        return 1
    fi

    local bad_n
    bad_n="$(jq '[.results[]? | select(
        (.fingerprint_ok == false and ((.status // "") != "blocked") and ((.status // "") != "skipped"))
        or (.status == "error") or (.status == "timeout")
    )] | length' "$expected_json")"
    if [[ "$bad_n" -gt 0 ]]; then
        echo "bench result has $bad_n failing/errored/timed-out case(s)" >&2
        return 1
    fi
    return 0
}

# Matrices first (fast correctness), then staged byte envelope (slow).
if [[ "$MODE" == "matrices" || "$MODE" == "all" ]]; then
    qst_run_child "exact_manifest_matrix" \
        env REQUIRE_LIVE=1 PG_CONFIG="$PG_CONFIG" "$ROOT/scripts/run_exact_manifest_matrix.sh"
    qst_run_child "independent_restore_proof_matrix" \
        env REQUIRE_LIVE=1 PG_CONFIG="$PG_CONFIG" "$ROOT/scripts/run_independent_restore_proof_matrix.sh"
    qst_run_child "metadata_failpoint_matrix" \
        env REQUIRE_LIVE=1 PG_CONFIG="$PG_CONFIG" "$ROOT/scripts/run_metadata_failpoint_matrix.sh"
    qst_run_child "local_compatibility_matrix" \
        env REQUIRE_LIVE=1 PG_CONFIG="$PG_CONFIG" "$ROOT/scripts/run_local_compatibility_matrix.sh"
    qst_run_child "maintain_lifecycle_e2e" \
        env REQUIRE_LIVE=1 PGDATABASE="$DB_NAME" PGHOST="$SOCKET" PGPORT="$PORT" PGFB_LOG_FILE="$LOG" \
            "$ROOT/scripts/run_maintain_lifecycle_e2e.sh" "$PG_BIN"
    qst_run_child "local_capacity_e2e" \
        env REQUIRE_LIVE=1 PGDATABASE="$DB_NAME" PGHOST="$SOCKET" PGPORT="$PORT" \
            "$ROOT/scripts/run_local_capacity_e2e.sh" "$PG_BIN"
fi

if [[ "$MODE" == "bench" || "$MODE" == "all" ]]; then
    BENCH_SIZES="${PG_FLASHBACK_BENCH_SIZES:-10MiB 100MiB 500MiB 1GiB}"
    BENCH_SHAPES="${PG_FLASHBACK_BENCH_SHAPES:-narrow indexed toast churn}"
    BENCH_RESULT_DIR="$WORK_ROOT/bench"
    BENCH_EXPECTED_JSON="$BENCH_RESULT_DIR/byte-support-envelope-$RUN_ID.json"
    qst_run_verified_child "byte_support_envelope_bench" verify_bench_result \
        env PG_FLASHBACK_BENCH_RUN_ID="$RUN_ID" \
            PG_FLASHBACK_BENCH_SIZES="$BENCH_SIZES" \
            PG_FLASHBACK_BENCH_SHAPES="$BENCH_SHAPES" \
            PG_FLASHBACK_BENCH_RESULT_DIR="$BENCH_RESULT_DIR" \
            PG_CONFIG="$PG_CONFIG" \
            "$ROOT/scripts/run_byte_support_envelope_bench.sh"
fi

if (( QST_FAILED > 0 )); then
    die "$QST_FAILED qualification step(s) failed"
fi
echo "Qualification instance PASS ($WORK_ROOT)" >&2
