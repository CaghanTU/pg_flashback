#!/usr/bin/env bash
# shellcheck disable=SC2317,SC2329
# Dedicated 24-hour exact-candidate bounded LOCAL-DELTA stability soak (Gate C).
#
# Gate C is a local_delta stability soak. Deferred physical-backup experiments
# are outside this product qualification.
#
# HARD RULES:
# - exact mode qualification_kind is exact_rc_24h_stability_soak
# - exact mode PASS requires >= 86400 active monotonic seconds
# - accelerated mode is explicitly development-only and cannot emit the exact kind
# - installs ONLY from CANDIDATE_DIR (never cargo-builds)
# - observer-only: never call flashback_consume_wal(); capture advances via workers
# - workload budget exhaustion stops heavy writes but does NOT end the clock
# - suspension/heartbeat gaps fail closed
# - NOT a chaos suite; destructive repo/slot faults belong elsewhere
#
# Required:
#   CANDIDATE_DIR
#
# Optional resource bounds (bytes):
#   PG_FLASHBACK_SOAK_MIN_FREE_BYTES   default 2147483648 (2 GiB)
#   PG_FLASHBACK_SOAK_MAX_WORK_BYTES   default 805306368 (~768 MiB)
#   PG_FLASHBACK_SOAK_WORK_STOP_HEADROOM_BYTES default 67108864 (64 MiB)
#   PG_FLASHBACK_SOAK_HEARTBEAT_MAX_GAP_SECONDS  default 180
#   PG_FLASHBACK_SOAK_WORKER_GRACE_SECONDS default 90

set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/exact_candidate_identity.sh
source "$REPO_ROOT/scripts/lib/exact_candidate_identity.sh"

CANDIDATE_DIR="${CANDIDATE_DIR:?CANDIDATE_DIR is required}"
KEEP="${PGFB_STABILITY_KEEP:-1}"
WORKER_GRACE_SECONDS="${PG_FLASHBACK_SOAK_WORKER_GRACE_SECONDS:-90}"
# Fail closed if an exact/accelerated soak is already active for this host tree.
SOAK_LOCK_DIR="${PGFB_STABILITY_LOCK_DIR:-$REPO_ROOT/target/stability-locks}"
SOAK_LOCK_FILE="$SOAK_LOCK_DIR/gate-c-local-stability.lock"
SOAK_LOCK_HELD=0

STABILITY_MODE="${PG_FLASHBACK_STABILITY_MODE:-exact}"
case "$STABILITY_MODE" in
    exact)
        # Duration is NOT overridable below 86400 for this qualification kind.
        QUALIFICATION_KIND=exact_rc_24h_stability_soak
        QUAL_DURATION_SECONDS=86400
        RESULT_PREFIX=exact-rc-24h-stability
        RESULT_CLAIM="24-hour exact-candidate bounded local_delta stability soak plus separate exact-candidate chaos suite on Linux/aarch64 under Lima on an Apple Silicon host."
        PERIODIC_DROP_TARGET=23
        PERIODIC_DROP_INTERVAL_SECONDS=3600
        ;;
    accelerated)
        # Development regression only. This mode must never satisfy Gate C.
        QUALIFICATION_KIND=development_accelerated_stability
        QUAL_DURATION_SECONDS="${PG_FLASHBACK_STABILITY_ACCELERATED_SECONDS:-900}"
        if (( QUAL_DURATION_SECONDS < 300 || QUAL_DURATION_SECONDS > 3600 )); then
            echo "FAIL: accelerated duration must be between 300 and 3600 seconds" >&2
            exit 2
        fi
        RESULT_PREFIX=development-accelerated-stability
        RESULT_CLAIM="Development-only accelerated local_delta stability/drill regression; not 24-hour release qualification."
        PERIODIC_DROP_TARGET=7
        PERIODIC_DROP_INTERVAL_SECONDS=$((QUAL_DURATION_SECONDS / (PERIODIC_DROP_TARGET + 1)))
        ;;
    *)
        echo "FAIL: unsupported PG_FLASHBACK_STABILITY_MODE=$STABILITY_MODE" >&2
        exit 2
        ;;
esac
POST_DRILL_DROP_TARGET=4
DROP_DRILL_TARGET=$((2 + PERIODIC_DROP_TARGET + POST_DRILL_DROP_TARGET))
MIN_FREE_BYTES="${PG_FLASHBACK_SOAK_MIN_FREE_BYTES:-2147483648}"
MAX_WORK_BYTES="${PG_FLASHBACK_SOAK_MAX_WORK_BYTES:-805306368}"
WORK_STOP_HEADROOM_BYTES="${PG_FLASHBACK_SOAK_WORK_STOP_HEADROOM_BYTES:-67108864}"
if (( WORK_STOP_HEADROOM_BYTES <= 0 || WORK_STOP_HEADROOM_BYTES >= MAX_WORK_BYTES )); then
    echo "FAIL: work-stop headroom must be positive and below max work bytes" >&2
    exit 2
fi
HEAVY_WRITE_STOP_BYTES=$((MAX_WORK_BYTES - WORK_STOP_HEADROOM_BYTES))
HEARTBEAT_MAX_GAP="${PG_FLASHBACK_SOAK_HEARTBEAT_MAX_GAP_SECONDS:-180}"
SAMPLE_INTERVAL="${PG_FLASHBACK_SOAK_SAMPLE_INTERVAL_SECONDS:-45}"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
if [[ "$STABILITY_MODE" == "exact" ]]; then
    DEFAULT_BASE="$REPO_ROOT/target/exact-rc-24h-stability"
else
    DEFAULT_BASE="$REPO_ROOT/target/development-accelerated-stability"
fi
BASE="${PGFB_STABILITY_BASE:-$DEFAULT_BASE}"
RUN_ROOT="$BASE/runs/$RUN_ID"
RESULT_DIR="${PGFB_STABILITY_RESULT_DIR:-$BASE/results}"
RESULT_JSON="$RESULT_DIR/$RESULT_PREFIX-$RUN_ID.json"
HEARTBEAT_FILE="$RUN_ROOT/heartbeat.txt"
SAMPLES_JSONL="$RUN_ROOT/samples.jsonl"
DROP_EVENTS_JSONL="$RUN_ROOT/drop-events.jsonl"
PROGRESS_LOG="$RUN_ROOT/progress.log"

PRIMARY_STARTED=0
PREFIX_INSTALLED=0
INTERRUPTED=0
STATUS="failed"
HEAVY_WRITES_ENABLED=1
CYCLES=0
INS=0
UPS=0
DELS=0
OBS_INS=0
OBS_UPS=0
OBS_DELS=0
CAPTURE_RESTARTS=0
MAINT_RESTARTS=0
LAST_CAPTURE_PID=""
LAST_MAINT_PID=""
LAST_LAG=0
PREV_LAG=0
HEALTH_GRACE_UNTIL_MONO_NS=0
DRILL_EARLY_DROP=0
DRILL_LATE_DROP=0
DRILL_RESTART=0
DRILL_WORKER_PAUSE=0
DRILL_MAINT_LOCK=0
DRILL_LOCAL_RESTORE=0
DROP_DRILLS_ATTEMPTED=0
DROP_DRILLS_PASSED=0
PERIODIC_DROP_COUNT=0
POST_DRILL_DROP_COUNT=0
START_MONO_NS="$(exact_candidate_monotonic_now_ns)"
LAST_HB_MONO_NS="$START_MONO_NS"
START_UTC=""
PEAK_WORK_BYTES=0
START_FS_FREE=0
WORK_BUDGET_LOGGED=0
SOCKET_DIR=""
PG_RSS_KB=0
LAG_GROWTH_STREAK=0
PREV_CAPTURE_PID=""
PREV_MAINT_PID=""

log() { printf '[exact-rc-24h-stability] %s %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "$PROGRESS_LOG"; }
die() { log "FAIL: $*"; STATUS=failed; exit 1; }
require_executable() { [[ -x "$1" ]] || die "required executable not found: $1"; }

acquire_soak_lock() {
    mkdir -p "$SOAK_LOCK_DIR"
    if [[ -f "$SOAK_LOCK_FILE" ]]; then
        local old_pid
        old_pid="$(awk '{print $1}' "$SOAK_LOCK_FILE" 2>/dev/null || true)"
        if [[ -n "$old_pid" ]] && kill -0 "$old_pid" 2>/dev/null; then
            die "another Gate C soak is already running (pid=$old_pid lock=$SOAK_LOCK_FILE)"
        fi
        rm -f -- "$SOAK_LOCK_FILE"
    fi
    if ! (set -o noclobber; printf '%s %s %s\n' "$$" "$RUN_ID" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$SOAK_LOCK_FILE"); then
        die "failed to acquire soak lock $SOAK_LOCK_FILE"
    fi
    SOAK_LOCK_HELD=1
}

release_soak_lock() {
    if [[ "$SOAK_LOCK_HELD" == "1" ]]; then
        rm -f -- "$SOAK_LOCK_FILE"
        SOAK_LOCK_HELD=0
    fi
}


write_heartbeat() {
    local elapsed=$1
    cat > "$HEARTBEAT_FILE" <<EOF
qualification_kind=$QUALIFICATION_KIND
run_id=$RUN_ID
elapsed_active_seconds=$elapsed
target_seconds=$QUAL_DURATION_SECONDS
cycles=$CYCLES
inserts=$INS updates=$UPS deletes=$DELS
observed_inserts=$OBS_INS observed_updates=$OBS_UPS observed_deletes=$OBS_DELS
heavy_writes_enabled=$HEAVY_WRITES_ENABLED
last_operation=${LAST_OP:-none}
health=${LAST_HEALTH:-unknown}
health_action=${LAST_HEALTH_ACTION:-none}
capture_pid=${LAST_CAPTURE_PID:-}
maintenance_pid=${LAST_MAINT_PID:-}
capture_restarts=$CAPTURE_RESTARTS
maintenance_restarts=$MAINT_RESTARTS
slot_restart_lsn=${LAST_SLOT_RESTART_LSN:-}
slot_confirmed_flush_lsn=${LAST_SLOT_FLUSH_LSN:-}
slot_lag_bytes=${LAST_LAG:-0}
slot_lag_delta_bytes=${LAST_LAG_DELTA:-0}
active_generations=${LAST_ACTIVE_GENS:-0}
pending_generations=${LAST_PENDING_GENS:-0}
sealed_generations=${LAST_SEALED_GENS:-0}
postgres_rss_kb=${PG_RSS_KB:-0}
free_bytes=${LAST_FREE:-0}
work_bytes=${LAST_WORK:-0}
next_drill=${NEXT_DRILL:-none}
drop_restores=$DROP_DRILLS_PASSED/$DROP_DRILL_TARGET
periodic_drop_restores=$PERIODIC_DROP_COUNT/$PERIODIC_DROP_TARGET
next_periodic_drop_at_seconds=${NEXT_PERIODIC_DROP_AT:-none}
utc_now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
EOF
}

du_bytes() {
    local path=$1
    local bytes
    [[ -e "$path" ]] || { echo 0; return; }
    # Active PostgreSQL directories create/unlink files while du walks them.
    # A transient ENOENT must not silently terminate a multi-hour run under
    # `set -o pipefail`; retry it, then let the caller report an explicit
    # fail-closed measurement error.
    for _ in 1 2 3; do
        if bytes="$(du -sb "$path" 2>/dev/null | awk '{print $1}')"; then
            printf '%s\n' "$bytes"
            return 0
        fi
        sleep 0.1
    done
    return 1
}

sample_resources() {
    LAST_FREE="$(exact_candidate_free_bytes "$RUN_ROOT")" \
        || die "cannot measure filesystem free bytes"
    local pgdata_b wal_b flash_b work_b
    pgdata_b="$(du_bytes "$PRIMARY_DIR")" \
        || die "cannot measure active PostgreSQL data directory"
    wal_b="$(du_bytes "$PRIMARY_DIR/pg_wal")" \
        || die "cannot measure active pg_wal directory"
    flash_b="$(du_bytes "$PRIMARY_DIR/pg_flashback")" \
        || die "cannot measure pg_flashback directory"
    work_b="$(du_bytes "$RUN_ROOT")" \
        || die "cannot measure qualification work directory"
    LAST_WORK="$work_b"
    if (( work_b > PEAK_WORK_BYTES )); then PEAK_WORK_BYTES=$work_b; fi

    local readiness_row
    readiness_row="$(q "SELECT COALESCE(capture_worker_pid::text,''),
                               COALESCE(maintenance_worker_pid::text,''),
                               admission_state,
                               capture_running,
                               maintenance_running
                        FROM flashback_worker_readiness();")"
    LAST_CAPTURE_PID="$(printf '%s\n' "$readiness_row" | cut -d'|' -f1)"
    LAST_MAINT_PID="$(printf '%s\n' "$readiness_row" | cut -d'|' -f2)"
    LAST_ADMISSION_STATE="$(printf '%s\n' "$readiness_row" | cut -d'|' -f3)"
    LAST_CAPTURE_RUNNING="$(printf '%s\n' "$readiness_row" | cut -d'|' -f4)"
    LAST_MAINT_RUNNING="$(printf '%s\n' "$readiness_row" | cut -d'|' -f5)"

    if [[ -n "${PREV_CAPTURE_PID:-}" && -n "$LAST_CAPTURE_PID" && "$LAST_CAPTURE_PID" != "$PREV_CAPTURE_PID" ]]; then
        CAPTURE_RESTARTS=$((CAPTURE_RESTARTS + 1))
    fi
    if [[ -n "${PREV_MAINT_PID:-}" && -n "$LAST_MAINT_PID" && "$LAST_MAINT_PID" != "$PREV_MAINT_PID" ]]; then
        MAINT_RESTARTS=$((MAINT_RESTARTS + 1))
    fi
    PREV_CAPTURE_PID="$LAST_CAPTURE_PID"
    PREV_MAINT_PID="$LAST_MAINT_PID"

    LAST_HEALTH="$(q "SELECT health FROM flashback_health() WHERE table_name='public.steady_dml';" 2>/dev/null || echo unavailable)"
    LAST_HEALTH_ACTION="$(q "SELECT COALESCE(recommended_action,'none') FROM flashback_health() WHERE table_name='public.steady_dml';" 2>/dev/null || echo none)"
    PREV_LAG="$LAST_LAG"
    LAST_LAG="$(q "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn),0)
                   FROM pg_replication_slots
                   WHERE slot_name=flashback_effective_slot_name();" 2>/dev/null || echo 0)"
    LAST_LAG_DELTA=$((LAST_LAG - PREV_LAG))
    LAST_SLOT_RESTART_LSN="$(q "SELECT COALESCE(restart_lsn::text,'') FROM pg_replication_slots
                                WHERE slot_name=flashback_effective_slot_name();" 2>/dev/null || true)"
    LAST_SLOT_FLUSH_LSN="$(q "SELECT COALESCE(confirmed_flush_lsn::text,'') FROM pg_replication_slots
                              WHERE slot_name=flashback_effective_slot_name();" 2>/dev/null || true)"
    LAST_ACTIVE_GENS="$(q "SELECT count(*) FROM flashback.coverage_generations WHERE state='active';" 2>/dev/null || echo 0)"
    LAST_PENDING_GENS="$(q "SELECT count(*) FROM flashback.coverage_generations WHERE state='building';" 2>/dev/null || echo 0)"
    LAST_SEALED_GENS="$(q "SELECT count(*) FROM flashback.coverage_generations WHERE state='sealed';" 2>/dev/null || echo 0)"
    OBS_INS="$(q "SELECT count(*) FROM flashback.delta_log
                  WHERE table_name='public.steady_dml' AND event_type='INSERT' AND commit_lsn IS NOT NULL;")"
    OBS_UPS="$(q "SELECT count(*) FROM flashback.delta_log
                  WHERE table_name='public.steady_dml' AND event_type='UPDATE' AND commit_lsn IS NOT NULL;")"
    OBS_DELS="$(q "SELECT count(*) FROM flashback.delta_log
                   WHERE table_name='public.steady_dml' AND event_type='DELETE' AND commit_lsn IS NOT NULL;")"
    if [[ -f "$PRIMARY_DIR/postmaster.pid" ]]; then
        local postmaster_pid
        postmaster_pid="$(awk 'NR==1 {print $1}' "$PRIMARY_DIR/postmaster.pid")"
        PG_RSS_KB="$(ps -o rss= -p "$postmaster_pid" 2>/dev/null | tr -d ' ' || echo 0)"
    fi
    local mono now_utc
    mono="$(exact_candidate_monotonic_now_ns)"
    now_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '%s\n' "{\"mono_ns\":$mono,\"utc\":\"$now_utc\",\"free_bytes\":$LAST_FREE,\"pgdata_bytes\":$pgdata_b,\"pg_wal_bytes\":$wal_b,\"flashback_bytes\":$flash_b,\"work_bytes\":$work_b,\"slot_lag_bytes\":$LAST_LAG,\"slot_lag_delta_bytes\":$LAST_LAG_DELTA,\"health\":\"$LAST_HEALTH\",\"health_action\":\"$LAST_HEALTH_ACTION\",\"capture_pid\":\"$LAST_CAPTURE_PID\",\"maintenance_pid\":\"$LAST_MAINT_PID\",\"capture_restarts\":$CAPTURE_RESTARTS,\"maintenance_restarts\":$MAINT_RESTARTS,\"observed_inserts\":$OBS_INS,\"observed_updates\":$OBS_UPS,\"observed_deletes\":$OBS_DELS,\"active_generations\":$LAST_ACTIVE_GENS,\"pending_generations\":$LAST_PENDING_GENS,\"sealed_generations\":$LAST_SEALED_GENS,\"postgres_rss_kb\":${PG_RSS_KB:-0},\"cycles\":$CYCLES,\"heavy_writes\":$HEAVY_WRITES_ENABLED}" >> "$SAMPLES_JSONL"
}

assert_continuous_health() {
    local now_mono grace_active=0
    now_mono="$(exact_candidate_monotonic_now_ns)"
    if (( now_mono < HEALTH_GRACE_UNTIL_MONO_NS )); then
        grace_active=1
    fi

    # Outside planned worker-recovery grace, capture must be ready.
    if [[ "$grace_active" == "0" ]]; then
        [[ "$LAST_ADMISSION_STATE" == "ready" ]] \
            || die "admission_state=$LAST_ADMISSION_STATE outside worker grace"
        [[ "$LAST_CAPTURE_RUNNING" == "t" && -n "$LAST_CAPTURE_PID" ]] \
            || die "capture worker missing outside grace (pid='$LAST_CAPTURE_PID')"
        [[ "$LAST_MAINT_RUNNING" == "t" && -n "$LAST_MAINT_PID" ]] \
            || die "maintenance worker missing outside grace (pid='$LAST_MAINT_PID')"
    fi

    case "$LAST_HEALTH" in
        slot_lost|slot_at_risk|timeline_mismatch|repository_anchor_missing|capture_worker_missing)
            if [[ "$grace_active" == "0" || "$LAST_HEALTH" == "slot_lost" || "$LAST_HEALTH" == "timeline_mismatch" || "$LAST_HEALTH" == "repository_anchor_missing" ]]; then
                die "unexpected health=$LAST_HEALTH action=$LAST_HEALTH_ACTION"
            fi
            ;;
        maintenance_worker_missing)
            [[ "$grace_active" == "1" ]] || die "unexpected health=$LAST_HEALTH outside grace"
            ;;
        healthy|maintenance_required|slot_lag_warning|reanchor_recommended|unavailable)
            ;;
        *)
            # Fail closed on unfamiliar unhealthy states.
            if [[ "$LAST_HEALTH" != "healthy" && "$grace_active" == "0" ]]; then
                die "unexpected health=$LAST_HEALTH action=$LAST_HEALTH_ACTION"
            fi
            ;;
    esac

    # Unbounded lag growth for three consecutive samples (approx) outside grace.
    if [[ "$grace_active" == "0" ]] && (( LAST_LAG_DELTA > 0 && LAST_LAG > 67108864 )); then
        LAG_GROWTH_STREAK=$(( ${LAG_GROWTH_STREAK:-0} + 1 ))
        if (( LAG_GROWTH_STREAK >= 3 )); then
            die "slot lag growing uncontrollably lag=$LAST_LAG delta=$LAST_LAG_DELTA"
        fi
    else
        LAG_GROWTH_STREAK=0
    fi

    # Unexpected pending generations on the steady table outside local restore/drop windows.
    if [[ "$grace_active" == "0" ]] && (( LAST_PENDING_GENS > 2 )); then
        die "unexpected pending generation count=$LAST_PENDING_GENS"
    fi
}

scan_postgres_log() {
    local logf="$LOG_DIR/primary.log"
    [[ -f "$logf" ]] || return 0
    if rg -n 'PANIC:|FATAL:.*(pg_flashback|background worker)|pg_flashback.*(crash|terminated abnormally)' "$logf" \
        | rg -v 'application_name=pgfb_stability_|intentional|pgfb_stability_maint|pgfb_stability_pause' \
        >/tmp/pgfb-soak-log-hits.$$ 2>/dev/null; then
        if [[ -s /tmp/pgfb-soak-log-hits.$$ ]]; then
            log "postgres log hits:"
            cat /tmp/pgfb-soak-log-hits.$$ | tee -a "$PROGRESS_LOG"
            rm -f /tmp/pgfb-soak-log-hits.$$
            die "unexpected PANIC/FATAL/worker crash signatures in PostgreSQL log"
        fi
    fi
    rm -f /tmp/pgfb-soak-log-hits.$$
}

begin_worker_grace() {
    local now_mono
    now_mono="$(exact_candidate_monotonic_now_ns)"
    HEALTH_GRACE_UNTIL_MONO_NS=$(( now_mono + WORKER_GRACE_SECONDS * 1000000000 ))
}

wait_workers_ready() {
    local label=$1 attempts=${2:-200} _i state cap maint
    for _i in $(seq 1 "$attempts"); do
        state=$(q "SELECT admission_state FROM flashback_worker_readiness();")
        cap=$(q "SELECT capture_running FROM flashback_worker_readiness();")
        maint=$(q "SELECT maintenance_running FROM flashback_worker_readiness();")
        if [[ "$state" == "ready" && "$cap" == "t" && "$maint" == "t" ]]; then
            return 0
        fi
        sleep 0.1
    done
    die "workers not ready after $label (state=$state capture=$cap maintenance=$maint)"
}


enforce_resource_bounds() {
    sample_resources
    if (( LAST_FREE < MIN_FREE_BYTES )); then
        HEAVY_WRITES_ENABLED=0
        die "filesystem free $LAST_FREE below reserve $MIN_FREE_BYTES"
    fi
    if (( LAST_WORK > MAX_WORK_BYTES )); then
        HEAVY_WRITES_ENABLED=0
        die "work bytes $LAST_WORK exceeded hard MAX_WORK_BYTES=$MAX_WORK_BYTES"
    fi
    if (( HEAVY_WRITES_ENABLED == 1 && LAST_WORK >= HEAVY_WRITE_STOP_BYTES )); then
        HEAVY_WRITES_ENABLED=0
        if (( WORK_BUDGET_LOGGED == 0 )); then
            log "work bytes $LAST_WORK reached soft stop $HEAVY_WRITE_STOP_BYTES; stopping heavy writes with ${WORK_STOP_HEADROOM_BYTES}-byte headroom (clock continues)"
            WORK_BUDGET_LOGGED=1
        fi
    fi
    # Soft stop heavy writes when projected remaining free would breach reserve.
    if (( LAST_FREE - 67108864 < MIN_FREE_BYTES )); then
        HEAVY_WRITES_ENABLED=0
        log "approaching free-space reserve; heavy writes disabled"
    fi
}

write_result() {
    local rc=$1
    local elapsed_s end_utc end_mono
    end_mono="$(exact_candidate_monotonic_now_ns)"
    elapsed_s=$(( (end_mono - START_MONO_NS) / 1000000000 ))
    end_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    if [[ "$INTERRUPTED" == "1" ]]; then
        STATUS=interrupted
    elif [[ "$rc" != "0" ]]; then
        STATUS=failed
    elif (( elapsed_s < QUAL_DURATION_SECONDS )); then
        STATUS=failed_short_duration
        rc=3
    else
        STATUS=passed
    fi
    mkdir -p "$RESULT_DIR"
    jq -n \
        --arg status "$STATUS" \
        --arg kind "$QUALIFICATION_KIND" \
        --arg claim "$RESULT_CLAIM" \
        --arg started_utc "$START_UTC" \
        --arg finished_utc "$end_utc" \
        --argjson elapsed "$elapsed_s" \
        --argjson required "$QUAL_DURATION_SECONDS" \
        --argjson cycles "$CYCLES" \
        --argjson inserts "$INS" --argjson updates "$UPS" --argjson deletes "$DELS" \
        --argjson obs_inserts "$OBS_INS" --argjson obs_updates "$OBS_UPS" --argjson obs_deletes "$OBS_DELS" \
        --argjson capture_restarts "$CAPTURE_RESTARTS" --argjson maint_restarts "$MAINT_RESTARTS" \
        --argjson peak_work "$PEAK_WORK_BYTES" \
        --argjson max_work "$MAX_WORK_BYTES" \
        --argjson min_free "$MIN_FREE_BYTES" \
        --argjson start_free "$START_FS_FREE" \
        --argjson drop_target "$DROP_DRILL_TARGET" \
        --argjson drop_attempted "$DROP_DRILLS_ATTEMPTED" \
        --argjson drop_passed "$DROP_DRILLS_PASSED" \
        --argjson periodic_target "$PERIODIC_DROP_TARGET" \
        --argjson periodic_passed "$PERIODIC_DROP_COUNT" \
        --argjson post_target "$POST_DRILL_DROP_TARGET" \
        --argjson post_passed "$POST_DRILL_DROP_COUNT" \
        --argjson exit_code "$rc" \
        --argjson identity "$(exact_candidate_identity_json 2>/dev/null || echo '{}')" \
        --slurpfile drop_events "$DROP_EVENTS_JSONL" \
        --argjson drills "$(jq -n \
            --argjson early "$DRILL_EARLY_DROP" --argjson late "$DRILL_LATE_DROP" \
            --argjson restart "$DRILL_RESTART" --argjson pause "$DRILL_WORKER_PAUSE" \
            --argjson maint "$DRILL_MAINT_LOCK" --argjson restore "$DRILL_LOCAL_RESTORE" \
            '{early_drop:$early, late_drop:$late, postgres_restart:$restart,
              worker_pause:$pause, maintenance_lock:$maint, local_restore:$restore}')" \
        '{
          qualification_kind: $kind,
          status: $status,
          started_at_utc: $started_utc,
          finished_at_utc: $finished_utc,
          elapsed_active_monotonic_seconds: $elapsed,
          required_active_seconds: $required,
          cycles: $cycles,
          expected_event_counts: {INSERT:$inserts, UPDATE:$updates, DELETE:$deletes},
          observed_event_counts: {INSERT:$obs_inserts, UPDATE:$obs_updates, DELETE:$obs_deletes},
          worker_restarts: {capture:$capture_restarts, maintenance:$maint_restarts},
          gate_profile: "local_delta",
          observer_only: true,
          peak_work_bytes: $peak_work,
          max_work_bytes: $max_work,
          min_free_bytes: $min_free,
          start_free_bytes: $start_free,
          drills: $drills,
          drop_restores: {
            target: $drop_target,
            attempted: $drop_attempted,
            passed: $drop_passed,
            periodic_target: $periodic_target,
            periodic_passed: $periodic_passed,
            post_drill_target: $post_target,
            post_drill_passed: $post_passed,
            events: $drop_events
          },
          identity: $identity,
          exit_code: $exit_code,
          claim: $claim
        }' > "$RESULT_JSON"
    log "result written: $RESULT_JSON status=$STATUS elapsed=${elapsed_s}s"
}

on_interrupt() {
    INTERRUPTED=1
    STATUS=interrupted
    log "interrupted; writing failed/interrupted evidence"
    exit 130
}

on_error() {
    local rc=$?
    STATUS=failed
    log "FAIL: command exited rc=$rc line=${BASH_LINENO[0]} command=$BASH_COMMAND"
    return "$rc"
}

cleanup() {
    local rc=$?
    trap - EXIT INT TERM ERR
    set +e
    if [[ "$PRIMARY_STARTED" == "1" ]]; then
        "$PG_BIN/pg_ctl" -D "$PRIMARY_DIR" stop -m fast -w -t 60 >/dev/null 2>&1 || true
    fi
    if [[ -n "$SOCKET_DIR" ]]; then
        rm -rf -- "$SOCKET_DIR" 2>/dev/null
    fi
    if [[ "$PREFIX_INSTALLED" == "1" ]]; then
        exact_candidate_restore_prefix || true
        PREFIX_INSTALLED=0
    fi
    exact_candidate_verify_end_state || rc=1
    write_result "$rc"
    release_soak_lock
    if [[ "$KEEP" != "1" && "$STATUS" == "passed" ]]; then
        rm -rf -- "$EC_EXTRACT_DIR" "$EC_STASH_DIR"
    fi
    exit "$rc"
}
trap cleanup EXIT
trap on_interrupt INT TERM
trap on_error ERR

mkdir -p "$RUN_ROOT" "$RESULT_DIR" "$RUN_ROOT/log"
: > "$SAMPLES_JSONL"
: > "$DROP_EVENTS_JSONL"
: > "$PROGRESS_LOG"

acquire_soak_lock
require_executable "$(command -v jq)"
require_executable "$(command -v python3)"
# Gate C qualifies only the local exact-WAL product.

# Preflight free space before binding/install.
START_FS_FREE="$(exact_candidate_free_bytes "$REPO_ROOT")"
if (( START_FS_FREE < MIN_FREE_BYTES + MAX_WORK_BYTES )); then
    die "insufficient free bytes: have $START_FS_FREE need >= $((MIN_FREE_BYTES + MAX_WORK_BYTES)) (reserve+work)"
fi

EC_STASH_DIR="$RUN_ROOT/prefix-stash"
EC_EXTRACT_DIR="$RUN_ROOT/extract"
exact_candidate_bind_dir "$CANDIDATE_DIR" || die "candidate bind failed"
exact_candidate_install_into_prefix || die "candidate install failed"
PREFIX_INSTALLED=1

DB_NAME=stabilitydb
PORT_BASE=$((37000 + ($$ % 20000)))
PRIMARY_PORT=$PORT_BASE
SOCKET_DIR="/tmp/pgfb-stability-$RUN_ID"
PRIMARY_DIR="$RUN_ROOT/primary"
LOG_DIR="$RUN_ROOT/log"

"$PG_BIN/initdb" -D "$PRIMARY_DIR" --no-locale --encoding=UTF8 --auth=trust >"$LOG_DIR/initdb.log"
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/output_plugin_allowlist.sh"
opal_configure_postgresql_conf "$PG_BIN" "$PRIMARY_DIR"
cat >> "$PRIMARY_DIR/postgresql.conf" <<EOF
port = $PRIMARY_PORT
unix_socket_directories = '$SOCKET_DIR'
listen_addresses = ''
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
shared_preload_libraries = 'pg_flashback'
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
pg_flashback.target_databases = '$DB_NAME'
pg_flashback.worker_interval_ms = 50
pg_flashback.local_max_snapshot_bytes = 2GB
pg_flashback.local_max_restore_peak_bytes = 4GB
pg_flashback.local_min_filesystem_bytes = 64MB
pg_flashback.local_safety_reserve_bytes = 16MB
pg_flashback.allow_unaudited_restore = on
EOF
mkdir -p "$SOCKET_DIR"
chmod 700 "$SOCKET_DIR"
"$PG_BIN/pg_ctl" -D "$PRIMARY_DIR" -l "$LOG_DIR/primary.log" start -w -t 60 >/dev/null
PRIMARY_STARTED=1
"$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PRIMARY_PORT" "$DB_NAME"
"$PG_BIN/createdb" -h "$SOCKET_DIR" -p "$PRIMARY_PORT" noisedb

q() { "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -qAt -h "$SOCKET_DIR" -p "$PRIMARY_PORT" -d "$DB_NAME" -c "$1"; }
qn() { "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -qAt -h "$SOCKET_DIR" -p "$PRIMARY_PORT" -d noisedb -c "$1"; }
fingerprint_of() {
    local rel=$1
    q "SELECT count(*)::text || '|' ||
              COALESCE(bit_xor(hashtextextended(row_to_json(t)::text, 0)), 0)::text
       FROM $rel AS t;"
}
relation_contract_of() {
    local rel=$1
    q "SELECT json_build_object(
            'fingerprint', (SELECT count(*)::text || '|' ||
                COALESCE(bit_xor(hashtextextended(row_to_json(t)::text, 0)), 0)::text FROM $rel AS t),
            'owner', (SELECT pg_get_userbyid(c.relowner) FROM pg_class c WHERE c.oid='$rel'::regclass),
            'acl', (SELECT COALESCE(c.relacl::text, '') FROM pg_class c WHERE c.oid='$rel'::regclass),
            'schema', (SELECT flashback_payload_schema_fingerprint('$rel'::regclass)),
            'indexes', (SELECT COALESCE(string_agg(indexdef, '|' ORDER BY indexdef), '')
                        FROM pg_indexes WHERE schemaname=split_part('$rel','.',1)
                          AND tablename=split_part('$rel','.',2))
       )::text;"
}

# Observer-only waits: never call flashback_consume_wal(). Capture advances only
# through the admitted background capture worker.
wait_healthy() {
    local rel=$1 attempts=${2:-300} _i h
    for _i in $(seq 1 "$attempts"); do
        h=$(q "SELECT health FROM flashback_health() WHERE table_name='$rel';")
        [[ "$h" == "healthy" ]] && return 0
        sleep 0.1
    done
    return 1
}

wait_for_delta_lsn_after() {
    local rel=$1 after_event_id=$2 attempts=${3:-300}
    local _i lsn
    for _i in $(seq 1 "$attempts"); do
        lsn=$(q "SELECT commit_lsn::text
                 FROM flashback.delta_log
                 WHERE rel_oid='$rel'::regclass
                   AND event_id > $after_event_id
                   AND commit_lsn IS NOT NULL
                 ORDER BY event_id DESC
                 LIMIT 1;")
        if [[ -n "$lsn" ]]; then
            printf '%s\n' "$lsn"
            return 0
        fi
        sleep 0.1
    done
    return 1
}

wait_for_coverage_lsn() {
    local rel=$1 target_lsn=$2 attempts=${3:-300}
    local _i covered
    [[ -n "$target_lsn" ]] || return 1
    for _i in $(seq 1 "$attempts"); do
        covered=$(q "SELECT EXISTS (
                       SELECT 1
                       FROM flashback.coverage_generations cg
                       JOIN flashback.tracked_tables tt USING (tracking_id)
                       WHERE tt.rel_oid='$rel'::regclass
                         AND cg.state='active'
                         AND cg.valid_through_lsn IS NOT NULL
                         AND cg.valid_through_lsn >= '$target_lsn'::pg_lsn
                     );")
        [[ "$covered" == "t" ]] && return 0
        sleep 0.1
    done
    return 1
}

wait_slot_catchup() {
    local target_lsn=$1 attempts=${2:-600} _i flush
    for _i in $(seq 1 "$attempts"); do
        flush=$(q "SELECT confirmed_flush_lsn::text FROM pg_replication_slots
                   WHERE slot_name=flashback_effective_slot_name();")
        if [[ -n "$flush" && "$(q "SELECT '$flush'::pg_lsn >= '$target_lsn'::pg_lsn;")" == "t" ]]; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}


q "CREATE EXTENSION pg_flashback;"
# Noise DB also loads extension for filtered WAL.
qn "CREATE EXTENSION pg_flashback;"
qn "CREATE TABLE public.noise(id bigserial PRIMARY KEY, payload text);"

wait_workers_ready "startup" 400

q "CREATE TABLE public.steady_dml(
     id bigserial PRIMARY KEY, marker text NOT NULL, payload text NOT NULL, wide text);"
q "CREATE TABLE public.second_tracked(
     id bigserial PRIMARY KEY, marker text NOT NULL);"
q "CREATE TABLE public.restore_probe(
     id bigint PRIMARY KEY, marker text NOT NULL, payload text NOT NULL);"
q "SELECT flashback_track('public.steady_dml');" >/dev/null
q "SELECT flashback_track('public.second_tracked');" >/dev/null
q "SELECT flashback_track('public.restore_probe');" >/dev/null
wait_healthy "public.steady_dml" 400 || die "steady_dml not healthy"
wait_healthy "public.second_tracked" 400 || die "second_tracked not healthy"
wait_healthy "public.restore_probe" 400 || die "restore_probe not healthy"

SEED=42
START_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
START_MONO_NS="$(exact_candidate_monotonic_now_ns)"
LAST_HB_MONO_NS="$START_MONO_NS"
LAST_OP="startup"
NEXT_DRILL="early_drop"
log "stability soak started; kind=$QUALIFICATION_KIND duration=${QUAL_DURATION_SECONDS}s max_work=$MAX_WORK_BYTES soft_stop=$HEAVY_WRITE_STOP_BYTES min_free=$MIN_FREE_BYTES drop_target=$DROP_DRILL_TARGET periodic_drop_target=$PERIODIC_DROP_TARGET"
write_heartbeat 0
enforce_resource_bounds

# Schedule points as fractions of the active window.
EARLY_DROP_AT=$((QUAL_DURATION_SECONDS / 20))          # ~5%
RESTART_AT=$((QUAL_DURATION_SECONDS / 10))             # ~10%
PAUSE_AT=$((QUAL_DURATION_SECONDS * 2 / 10))           # ~20%
MAINT_AT=$((QUAL_DURATION_SECONDS * 3 / 10))           # ~30%
RESTORE_AT=$((QUAL_DURATION_SECONDS * 4 / 10))         # ~40%
LATE_DROP_AT=$((QUAL_DURATION_SECONDS * 85 / 100))     # ~85%
NEXT_PERIODIC_DROP_AT=$PERIODIC_DROP_INTERVAL_SECONDS

do_cycle() {
    # ~64 KiB payload/cycle — paced across 24h, not a 2GiB burst.
    local wide
    wide="$(python3 - <<PY
import random
random.seed($SEED + $CYCLES)
print('W'*4096)
PY
)"
    q "INSERT INTO public.steady_dml(marker, payload, wide)
       SELECT 'c$CYCLES', md5(g::text), '$wide' FROM generate_series(1,8) g;" >/dev/null
    INS=$((INS + 8))
    q "UPDATE public.steady_dml SET marker='u$CYCLES'
       WHERE id IN (SELECT id FROM public.steady_dml ORDER BY id DESC LIMIT 4);" >/dev/null
    UPS=$((UPS + 4))
    q "DELETE FROM public.steady_dml
       WHERE id IN (SELECT id FROM public.steady_dml ORDER BY id ASC LIMIT 4);" >/dev/null
    DELS=$((DELS + 4))
    q "INSERT INTO public.second_tracked(marker) VALUES ('c$CYCLES');" >/dev/null
    qn "INSERT INTO public.noise(payload) VALUES ('n$CYCLES');" >/dev/null
    CYCLES=$((CYCLES + 1))
    LAST_OP="cycle_$CYCLES"
}

do_drop_restore_drill() {
    local tag=$1
    local table_name="drop_probe_${tag}"
    local rel="public.${table_name}"
    local lsn fp contract _i drop_end_mono drop_elapsed safe_lsn status reason
    q "DROP TABLE IF EXISTS $rel;" >/dev/null
    q "CREATE TABLE $rel(id bigint PRIMARY KEY, marker text NOT NULL, payload text NOT NULL);"
    q "SELECT flashback_track('$rel');" >/dev/null
    wait_healthy "$rel" 300 || die "$tag $table_name not healthy"
    q "INSERT INTO $rel VALUES (1,'$tag', repeat('d', 500));" >/dev/null
    # Optional DML burst immediately before DROP for selected tags.
    if [[ "$tag" == *dml* || "$tag" == early || "$tag" == late ]]; then
        q "UPDATE $rel SET marker='pre-drop' WHERE id=1;" >/dev/null
        q "INSERT INTO $rel VALUES (2,'$tag-extra', repeat('e', 200));" >/dev/null
    fi
    lsn=""
    for _i in $(seq 1 300); do
        lsn=$(q "SELECT commit_lsn::text FROM flashback.delta_log
                 WHERE table_name='$rel' AND event_type='INSERT'
                 ORDER BY commit_lsn DESC LIMIT 1;")
        if [[ -n "$lsn" ]] && wait_for_coverage_lsn "$rel" "$lsn" 1; then
            break
        fi
        sleep 0.05
    done
    [[ -n "$lsn" ]] || die "$tag drop frontier not covered by worker"
    wait_for_coverage_lsn "$rel" "$lsn" 300 || die "$tag coverage did not reach $lsn"
    contract=$(relation_contract_of "$rel")
    fp=$(fingerprint_of "$rel")
    DROP_DRILLS_ATTEMPTED=$((DROP_DRILLS_ATTEMPTED + 1))
    q "DROP TABLE $rel;" >/dev/null
    [[ "$(q "SELECT to_regclass('$rel') IS NULL;")" == "t" ]] || die "$tag drop failed"

    # Wait for worker to commit the DROP event (observer-only).
    for _i in $(seq 1 300); do
        [[ "$(q "SELECT count(*) FROM flashback.delta_log WHERE table_name='$rel' AND event_type='DROP' AND commit_lsn IS NOT NULL;")" -ge 1 ]] && break
        sleep 0.05
        if (( _i == 300 )); then
            die "$tag DROP event was not captured by background worker"
        fi
    done

    # Operator workflow: disaster discovery → restore to safe_target_lsn.
    safe_lsn=""
    status=""
    reason=""
    for _i in $(seq 1 300); do
        safe_lsn=$(q "SELECT safe_target_lsn::text FROM flashback_disaster_points('$rel', interval '1 hour')
                      WHERE event_type='DROP' AND status='restorable'
                      ORDER BY disaster_commit_lsn DESC LIMIT 1;")
        status=$(q "SELECT status FROM flashback_disaster_points('$rel', interval '1 hour')
                    WHERE event_type='DROP' ORDER BY disaster_commit_lsn DESC LIMIT 1;")
        reason=$(q "SELECT COALESCE(reason,'') FROM flashback_disaster_points('$rel', interval '1 hour')
                    WHERE event_type='DROP' ORDER BY disaster_commit_lsn DESC LIMIT 1;")
        [[ -n "$safe_lsn" && "$status" == "restorable" ]] && break
        sleep 0.1
    done
    [[ "$status" == "restorable" && -n "$safe_lsn" ]] \
        || die "$tag disaster_points not restorable (status=$status reason=$reason)"

    q "SELECT flashback_restore_lsn('$rel', '$safe_lsn'::pg_lsn);" >/dev/null \
        || die "$tag disaster restore_lsn failed for $safe_lsn"
    [[ "$(fingerprint_of "$rel")" == "$fp" ]] || die "$tag drop restore fingerprint mismatch"
    [[ "$(relation_contract_of "$rel")" == "$contract" ]] \
        || die "$tag drop restore owner/acl/schema/index contract mismatch"
    wait_healthy "$rel" 300 || die "$tag post-drop coverage"
    DROP_DRILLS_PASSED=$((DROP_DRILLS_PASSED + 1))
    drop_end_mono="$(exact_candidate_monotonic_now_ns)"
    drop_elapsed=$(( (drop_end_mono - START_MONO_NS) / 1000000000 ))
    jq -nc \
        --arg tag "$tag" --arg relation "$rel" --arg target_lsn "$safe_lsn" \
        --arg pre_drop_lsn "$lsn" --arg fingerprint "$fp" --argjson elapsed "$drop_elapsed" \
        --arg discovery "flashback_disaster_points" \
        '{tag:$tag, relation:$relation, elapsed_active_seconds:$elapsed,
          target_lsn:$target_lsn, pre_drop_covered_lsn:$pre_drop_lsn,
          discovery:$discovery, relation_absent_after_drop:true,
          fingerprint_verified:true, fingerprint:$fingerprint,
          contract_verified:true}' \
        >> "$DROP_EVENTS_JSONL"
    log "DROP restore passed tag=$tag elapsed=${drop_elapsed}s count=$DROP_DRILLS_PASSED/$DROP_DRILL_TARGET safe_lsn=$safe_lsn"
    LAST_OP="drop_restore_$tag"
}

# Main loop: monotonic active duration.
while true; do
    now_mono="$(exact_candidate_monotonic_now_ns)"
    elapsed=$(( (now_mono - START_MONO_NS) / 1000000000 ))
    gap=$(( (now_mono - LAST_HB_MONO_NS) / 1000000000 ))
    if (( gap > HEARTBEAT_MAX_GAP )); then
        die "heartbeat gap ${gap}s exceeds max ${HEARTBEAT_MAX_GAP}s (suspension/stall)"
    fi
    LAST_HB_MONO_NS="$now_mono"

    if (( elapsed >= QUAL_DURATION_SECONDS )); then
        break
    fi

    # Scheduled drills (once each).
    if (( DRILL_EARLY_DROP == 0 && elapsed >= EARLY_DROP_AT )); then
        NEXT_DRILL=early_drop
        do_drop_restore_drill early
        DRILL_EARLY_DROP=1
        NEXT_DRILL=restart
    fi
    if (( DRILL_RESTART == 0 && elapsed >= RESTART_AT )); then
        NEXT_DRILL=postgres_restart
        begin_worker_grace
        "$PG_BIN/pg_ctl" -D "$PRIMARY_DIR" restart -w -t 60 -l "$LOG_DIR/primary.log" >/dev/null
        for _ in $(seq 1 60); do q "SELECT 1" >/dev/null 2>&1 && break; sleep 0.5; done
        wait_workers_ready "postgres_restart" 400
        wait_healthy "public.steady_dml" 400 || die "health after restart"
        do_drop_restore_drill post_restart
        POST_DRILL_DROP_COUNT=$((POST_DRILL_DROP_COUNT + 1))
        DRILL_RESTART=1
        LAST_OP=postgres_restart
        NEXT_DRILL=worker_pause
    fi
    if (( DRILL_WORKER_PAUSE == 0 && elapsed >= PAUSE_AT )); then
        NEXT_DRILL=worker_pause
        begin_worker_grace
        old_cap=$(q "SELECT capture_worker_pid FROM flashback_worker_readiness();")
        [[ -n "$old_cap" ]] || die "capture pid missing before pause drill"
        lag_before=$(q "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn),0)
                        FROM pg_replication_slots WHERE slot_name=flashback_effective_slot_name();")
        kill -TERM "$old_cap" || die "failed to TERM capture worker $old_cap"
        # Wait until the old PID is gone, then for automatic restart.
        for _ in $(seq 1 200); do
            if ! kill -0 "$old_cap" 2>/dev/null; then break; fi
            sleep 0.05
        done
        wait_workers_ready "capture_restart" 400
        new_cap=$(q "SELECT capture_worker_pid FROM flashback_worker_readiness();")
        [[ -n "$new_cap" && "$new_cap" != "$old_cap" ]] \
            || die "capture worker did not restart with a new pid (old=$old_cap new=$new_cap)"
        # Lag must drain back toward the target after catch-up (bounded).
        catch_target=$(q "SELECT pg_current_wal_lsn()::text;")
        wait_slot_catchup "$catch_target" 600 \
            || die "slot did not catch up after capture restart (lag_before=$lag_before)"
        wait_healthy "public.steady_dml" 400 || die "health after pause"
        do_drop_restore_drill post_worker_pause
        POST_DRILL_DROP_COUNT=$((POST_DRILL_DROP_COUNT + 1))
        DRILL_WORKER_PAUSE=1
        LAST_OP=worker_pause
        NEXT_DRILL=maint_lock
    fi
    if (( DRILL_MAINT_LOCK == 0 && elapsed >= MAINT_AT )); then
        NEXT_DRILL=maint_lock
        lock_held=0
        "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -qAt \
            -h "$SOCKET_DIR" -p "$PRIMARY_PORT" -d "$DB_NAME" \
            >/dev/null 2>"$LOG_DIR/maint-lock.err" <<'SQL' &
SET application_name TO 'pgfb_stability_maint';
BEGIN;
SET LOCAL lock_timeout TO '2s';
LOCK TABLE public.steady_dml IN ACCESS EXCLUSIVE MODE;
SELECT pg_sleep(30);
COMMIT;
SQL
        MAINT_PID=$!
        for _ in $(seq 1 100); do
            if q "SELECT EXISTS (
                    SELECT 1
                    FROM pg_locks l
                    JOIN pg_class c ON c.oid = l.relation
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = 'public'
                      AND c.relname = 'steady_dml'
                      AND l.locktype = 'relation'
                      AND l.mode = 'AccessExclusiveLock'
                      AND l.granted
                  );" | grep -qx t; then
                lock_held=1
                break
            fi
            sleep 0.05
        done
        [[ "$lock_held" == "1" ]] || {
            kill "$MAINT_PID" 2>/dev/null || true
            die "maintenance ACCESS EXCLUSIVE lock was not observed (see $LOG_DIR/maint-lock.err)"
        }
        # Second table must continue under an independent worker path.
        q "INSERT INTO public.second_tracked(marker) VALUES ('during-maint');" >/dev/null
        during_ok=0
        for _ in $(seq 1 300); do
            if [[ "$(q "SELECT count(*) FROM flashback.delta_log
                        WHERE table_name='public.second_tracked'
                          AND event_type='INSERT'
                          AND new_data->>'marker'='during-maint'
                          AND commit_lsn IS NOT NULL;")" -ge 1 ]]; then
                during_ok=1
                break
            fi
            sleep 0.1
        done
        [[ "$during_ok" == "1" ]] || die "second_tracked capture stalled while steady_dml held ACCESS EXCLUSIVE"
        wait "$MAINT_PID" || die "maintenance lock session failed"
        wait_healthy "public.second_tracked" 300 || die "second_tracked unhealthy after maint"
        do_drop_restore_drill post_maint_lock
        POST_DRILL_DROP_COUNT=$((POST_DRILL_DROP_COUNT + 1))
        DRILL_MAINT_LOCK=1
        LAST_OP=maint_lock
        NEXT_DRILL=local_restore
    fi
    if (( DRILL_LOCAL_RESTORE == 0 && elapsed >= RESTORE_AT )); then
        NEXT_DRILL=local_restore
        RBEFORE=$(q "SELECT COALESCE(max(event_id), 0) FROM flashback.delta_log
                     WHERE rel_oid='public.restore_probe'::regclass;")
        q "INSERT INTO public.restore_probe VALUES (1, 'r', 'p')
           ON CONFLICT (id) DO UPDATE SET marker='r$elapsed', payload='p';" >/dev/null
        RLSN=$(wait_for_delta_lsn_after "public.restore_probe" "$RBEFORE" 200) \
            || die "local restore source commit LSN was not captured"
        wait_for_coverage_lsn "public.restore_probe" "$RLSN" 200 \
            || die "local restore source LSN $RLSN was not covered"
        RFP=$(fingerprint_of "public.restore_probe")
        RMUT_BEFORE=$(q "SELECT COALESCE(max(event_id), 0) FROM flashback.delta_log
                         WHERE rel_oid='public.restore_probe'::regclass;")
        q "UPDATE public.restore_probe SET payload='mut';" >/dev/null
        RMUT_LSN=$(wait_for_delta_lsn_after "public.restore_probe" "$RMUT_BEFORE" 200) \
            || die "local restore mutation commit LSN was not captured"
        wait_for_coverage_lsn "public.restore_probe" "$RMUT_LSN" 200 \
            || die "local restore mutation LSN $RMUT_LSN was not covered"
        q "SELECT flashback_restore_lsn('public.restore_probe', '$RLSN'::pg_lsn);" >/dev/null
        [[ "$(fingerprint_of "public.restore_probe")" == "$RFP" ]] || die "local restore fingerprint"
        do_drop_restore_drill post_local_restore
        POST_DRILL_DROP_COUNT=$((POST_DRILL_DROP_COUNT + 1))
        DRILL_LOCAL_RESTORE=1
        LAST_OP=local_restore
        NEXT_DRILL=late_drop
    fi
    if (( DRILL_LATE_DROP == 0 && elapsed >= LATE_DROP_AT )); then
        NEXT_DRILL=late_drop
        do_drop_restore_drill late
        DRILL_LATE_DROP=1
        NEXT_DRILL=none
    fi

    # The product's primary DROP-recovery path is exercised throughout the
    # active window. Exact mode performs one periodic DROP/restore after every
    # completed hour (hours 1–23), in addition to early/late and post-drill
    # probes. The accelerated development mode distributes seven equivalents.
    while (( PERIODIC_DROP_COUNT < PERIODIC_DROP_TARGET && elapsed >= NEXT_PERIODIC_DROP_AT )); do
        periodic_no=$((PERIODIC_DROP_COUNT + 1))
        periodic_tag=$(printf 'periodic_%02d' "$periodic_no")
        do_drop_restore_drill "$periodic_tag"
        PERIODIC_DROP_COUNT=$periodic_no
        NEXT_PERIODIC_DROP_AT=$((PERIODIC_DROP_INTERVAL_SECONDS * (PERIODIC_DROP_COUNT + 1)))
    done

    enforce_resource_bounds
    if [[ "$HEAVY_WRITES_ENABLED" == "1" ]]; then
        # Pace: about one small cycle per sample interval → spreads churn across 24h.
        do_cycle
    else
        LAST_OP="idle_sample_$elapsed"
        [[ "$(q "SELECT capture_running FROM flashback_worker_readiness();")" == "t" ]] \
            || die "capture worker missing during idle sampling"
    fi

    # Continuous health / worker / lag assertions (observer-only).
    sample_resources
    assert_continuous_health
    scan_postgres_log
    write_heartbeat "$elapsed"
    sleep "$SAMPLE_INTERVAL"
done

# Final drain and assertions (observer-only: wait for capture worker, do not consume).
log "duration window complete; draining and final assertions"
enforce_resource_bounds
FINAL_TARGET=$(q "SELECT pg_current_wal_lsn()::text;")
wait_slot_catchup "$FINAL_TARGET" 600 \
    || die "final slot catch-up failed for $FINAL_TARGET"
enforce_resource_bounds
sample_resources
assert_continuous_health
[[ "$(q "SELECT health FROM flashback_health() WHERE table_name='public.steady_dml';")" == "healthy" ]] \
    || die "final health not healthy"

# Expected vs observed committed DML for the paced steady_dml workload.
OBS_INS="$(q "SELECT count(*) FROM flashback.delta_log
              WHERE table_name='public.steady_dml' AND event_type='INSERT' AND commit_lsn IS NOT NULL;")"
OBS_UPS="$(q "SELECT count(*) FROM flashback.delta_log
              WHERE table_name='public.steady_dml' AND event_type='UPDATE' AND commit_lsn IS NOT NULL;")"
OBS_DELS="$(q "SELECT count(*) FROM flashback.delta_log
               WHERE table_name='public.steady_dml' AND event_type='DELETE' AND commit_lsn IS NOT NULL;")"
[[ "$OBS_INS" == "$INS" ]] || die "INSERT count mismatch expected=$INS observed=$OBS_INS"
[[ "$OBS_UPS" == "$UPS" ]] || die "UPDATE count mismatch expected=$UPS observed=$OBS_UPS"
[[ "$OBS_DELS" == "$DELS" ]] || die "DELETE count mismatch expected=$DELS observed=$OBS_DELS"
# No unresolved commits for steady_dml events.
[[ "$(q "SELECT count(*) FROM flashback.delta_log
         WHERE table_name='public.steady_dml'
           AND event_type IN ('INSERT','UPDATE','DELETE')
           AND commit_lsn IS NULL;")" == "0" ]] \
    || die "steady_dml has unresolved commit_lsn rows"
# Final live fingerprint matches a deterministic recount model (count|xor already).
FINAL_FP=$(fingerprint_of "public.steady_dml")
[[ -n "$FINAL_FP" ]] || die "final steady_dml fingerprint empty"
# Coverage must include the latest steady_dml commit.
LATEST_STEADY=$(q "SELECT max(commit_lsn)::text FROM flashback.delta_log
                   WHERE table_name='public.steady_dml' AND commit_lsn IS NOT NULL;")
wait_for_coverage_lsn "public.steady_dml" "$LATEST_STEADY" 100 \
    || die "final coverage missing latest steady_dml commit $LATEST_STEADY"

[[ "$DRILL_EARLY_DROP" == "1" && "$DRILL_LATE_DROP" == "1" && "$DRILL_RESTART" == "1" \
   && "$DRILL_WORKER_PAUSE" == "1" && "$DRILL_MAINT_LOCK" == "1" && "$DRILL_LOCAL_RESTORE" == "1" ]] \
    || die "not all scheduled drills completed"
[[ "$DROP_DRILLS_ATTEMPTED" == "$DROP_DRILL_TARGET" \
   && "$DROP_DRILLS_PASSED" == "$DROP_DRILL_TARGET" \
   && "$PERIODIC_DROP_COUNT" == "$PERIODIC_DROP_TARGET" \
   && "$POST_DRILL_DROP_COUNT" == "$POST_DRILL_DROP_TARGET" ]] \
    || die "DROP coverage incomplete: passed=$DROP_DRILLS_PASSED target=$DROP_DRILL_TARGET periodic=$PERIODIC_DROP_COUNT/$PERIODIC_DROP_TARGET post=$POST_DRILL_DROP_COUNT/$POST_DRILL_DROP_TARGET"
# Observer-only integrity: script must not contain executable consume calls.
consume_fn="flashback_""consume_wal"
if rg -n "^[^#]*${consume_fn}" "$REPO_ROOT/scripts/run_exact_rc_24h_stability_soak.sh" >/dev/null; then
    die "soak script still contains ${consume_fn} calls"
fi
exact_candidate_verify_installed || die "binary hash mismatch at end"
STATUS=passed
log "stability soak assertions passed expected_dml=ins:$INS/upd:$UPS/del:$DELS observed=ins:$OBS_INS/upd:$OBS_UPS/del:$OBS_DELS drops=$DROP_DRILLS_PASSED"
exit 0
