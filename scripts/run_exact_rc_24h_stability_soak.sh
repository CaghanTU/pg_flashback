#!/usr/bin/env bash
# Dedicated 24-hour exact-candidate bounded stability soak (Gate C).
#
# HARD RULES:
# - qualification_kind is always exact_rc_24h_stability_soak
# - PASS requires >= 86400 active monotonic seconds
# - no env var may emit PASS for a shorter duration
# - installs ONLY from CANDIDATE_DIR (never cargo-builds)
# - workload budget exhaustion stops heavy writes but does NOT end the clock
# - suspension/heartbeat gaps fail closed
# - NOT a chaos suite; destructive repo/slot faults belong elsewhere
#
# Required:
#   CANDIDATE_DIR
#   PGBACKREST
#
# Optional resource bounds (bytes):
#   PG_FLASHBACK_SOAK_MIN_FREE_BYTES   default 2147483648 (2 GiB)
#   PG_FLASHBACK_SOAK_MAX_WORK_BYTES   default 805306368 (~768 MiB)
#   PG_FLASHBACK_SOAK_HEARTBEAT_MAX_GAP_SECONDS  default 180

set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/exact_candidate_identity.sh
source "$REPO_ROOT/scripts/lib/exact_candidate_identity.sh"

CANDIDATE_DIR="${CANDIDATE_DIR:?CANDIDATE_DIR is required}"
PGBACKREST="${PGBACKREST:-/usr/local/bin/pgbackrest}"
KEEP="${PGFB_STABILITY_KEEP:-1}"

# Duration is NOT overridable below 86400 for this qualification_kind.
QUAL_DURATION_SECONDS=86400
MIN_FREE_BYTES="${PG_FLASHBACK_SOAK_MIN_FREE_BYTES:-2147483648}"
MAX_WORK_BYTES="${PG_FLASHBACK_SOAK_MAX_WORK_BYTES:-805306368}"
HEARTBEAT_MAX_GAP="${PG_FLASHBACK_SOAK_HEARTBEAT_MAX_GAP_SECONDS:-180}"
SAMPLE_INTERVAL="${PG_FLASHBACK_SOAK_SAMPLE_INTERVAL_SECONDS:-45}"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
BASE="${PGFB_STABILITY_BASE:-$REPO_ROOT/target/exact-rc-24h-stability}"
RUN_ROOT="$BASE/runs/$RUN_ID"
RESULT_DIR="${PGFB_STABILITY_RESULT_DIR:-$BASE/results}"
RESULT_JSON="$RESULT_DIR/exact-rc-24h-stability-$RUN_ID.json"
HEARTBEAT_FILE="$RUN_ROOT/heartbeat.txt"
SAMPLES_JSONL="$RUN_ROOT/samples.jsonl"
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
DRILL_EARLY_DROP=0
DRILL_LATE_DROP=0
DRILL_RESTART=0
DRILL_WORKER_PAUSE=0
DRILL_MAINT_LOCK=0
DRILL_LOCAL_RESTORE=0
START_MONO_NS=0
LAST_HB_MONO_NS=0
START_UTC=""
PEAK_WORK_BYTES=0
START_FS_FREE=0

log() { printf '[exact-rc-24h-stability] %s %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "$PROGRESS_LOG"; }
die() { log "FAIL: $*"; STATUS=failed; exit 1; }
require_executable() { [[ -x "$1" ]] || die "required executable not found: $1"; }

write_heartbeat() {
    local elapsed=$1
    cat > "$HEARTBEAT_FILE" <<EOF
qualification_kind=exact_rc_24h_stability_soak
run_id=$RUN_ID
elapsed_active_seconds=$elapsed
target_seconds=$QUAL_DURATION_SECONDS
cycles=$CYCLES
inserts=$INS updates=$UPS deletes=$DELS
heavy_writes_enabled=$HEAVY_WRITES_ENABLED
last_operation=${LAST_OP:-none}
health=${LAST_HEALTH:-unknown}
slot_lag_bytes=${LAST_LAG:-0}
free_bytes=${LAST_FREE:-0}
work_bytes=${LAST_WORK:-0}
next_drill=${NEXT_DRILL:-none}
utc_now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
EOF
}

du_bytes() {
    local path=$1
    [[ -e "$path" ]] || { echo 0; return; }
    du -sb "$path" 2>/dev/null | awk '{print $1}'
}

sample_resources() {
    LAST_FREE="$(exact_candidate_free_bytes "$RUN_ROOT")"
    local pgdata_b wal_b flash_b work_b
    pgdata_b="$(du_bytes "$PRIMARY_DIR")"
    wal_b="$(du_bytes "$PRIMARY_DIR/pg_wal")"
    flash_b="$(du_bytes "$PRIMARY_DIR/pg_flashback" 2>/dev/null || echo 0)"
    work_b="$(du_bytes "$RUN_ROOT")"
    LAST_WORK="$work_b"
    if (( work_b > PEAK_WORK_BYTES )); then PEAK_WORK_BYTES=$work_b; fi
    LAST_HEALTH="$(q "SELECT health FROM flashback_health() WHERE table_name='public.steady_dml';" 2>/dev/null || echo unavailable)"
    LAST_LAG="$(q "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn),0)
                   FROM pg_replication_slots
                   WHERE slot_name=flashback_effective_slot_name();" 2>/dev/null || echo 0)"
    local mono now_utc
    mono="$(exact_candidate_monotonic_now_ns)"
    now_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '%s\n' "{\"mono_ns\":$mono,\"utc\":\"$now_utc\",\"free_bytes\":$LAST_FREE,\"pgdata_bytes\":$pgdata_b,\"pg_wal_bytes\":$wal_b,\"flashback_bytes\":$flash_b,\"work_bytes\":$work_b,\"slot_lag_bytes\":$LAST_LAG,\"health\":\"$LAST_HEALTH\",\"cycles\":$CYCLES,\"heavy_writes\":$HEAVY_WRITES_ENABLED}" >> "$SAMPLES_JSONL"
}

enforce_resource_bounds() {
    sample_resources
    if (( LAST_FREE < MIN_FREE_BYTES )); then
        HEAVY_WRITES_ENABLED=0
        die "filesystem free $LAST_FREE below reserve $MIN_FREE_BYTES"
    fi
    if (( LAST_WORK > MAX_WORK_BYTES )); then
        HEAVY_WRITES_ENABLED=0
        log "work bytes $LAST_WORK hit MAX_WORK_BYTES=$MAX_WORK_BYTES; stopping heavy writes (clock continues)"
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
        --arg kind "exact_rc_24h_stability_soak" \
        --arg started_utc "$START_UTC" \
        --arg finished_utc "$end_utc" \
        --argjson elapsed "$elapsed_s" \
        --argjson required "$QUAL_DURATION_SECONDS" \
        --argjson cycles "$CYCLES" \
        --argjson inserts "$INS" --argjson updates "$UPS" --argjson deletes "$DELS" \
        --argjson peak_work "$PEAK_WORK_BYTES" \
        --argjson max_work "$MAX_WORK_BYTES" \
        --argjson min_free "$MIN_FREE_BYTES" \
        --argjson start_free "$START_FS_FREE" \
        --argjson exit_code "$rc" \
        --argjson identity "$(exact_candidate_identity_json 2>/dev/null || echo '{}')" \
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
          peak_work_bytes: $peak_work,
          max_work_bytes: $max_work,
          min_free_bytes: $min_free,
          start_free_bytes: $start_free,
          drills: $drills,
          identity: $identity,
          exit_code: $exit_code,
          claim: "24-hour exact-candidate bounded stability soak plus separate exact-candidate chaos suite on Linux/aarch64 under Lima on an Apple Silicon host."
        }' > "$RESULT_JSON"
    log "result written: $RESULT_JSON status=$STATUS elapsed=${elapsed_s}s"
}

on_interrupt() {
    INTERRUPTED=1
    STATUS=interrupted
    log "interrupted; writing failed/interrupted evidence"
    exit 130
}

cleanup() {
    local rc=$?
    trap - EXIT INT TERM
    set +e
    if [[ "$PRIMARY_STARTED" == "1" ]]; then
        "$PG_BIN/pg_ctl" -D "$PRIMARY_DIR" stop -m fast -w -t 60 >/dev/null 2>&1 || true
    fi
    rm -rf -- "$SOCKET_DIR" 2>/dev/null
    if [[ "$PREFIX_INSTALLED" == "1" ]]; then
        exact_candidate_restore_prefix || true
        PREFIX_INSTALLED=0
    fi
    exact_candidate_verify_end_state || rc=1
    write_result "$rc"
    if [[ "$KEEP" != "1" && "$STATUS" == "passed" ]]; then
        rm -rf -- "$EC_EXTRACT_DIR" "$EC_STASH_DIR"
    fi
    exit "$rc"
}
trap cleanup EXIT
trap on_interrupt INT TERM

mkdir -p "$RUN_ROOT" "$RESULT_DIR" "$RUN_ROOT/log"
: > "$SAMPLES_JSONL"
: > "$PROGRESS_LOG"

require_executable "$PGBACKREST"
require_executable "$(command -v jq)"
require_executable "$(command -v python3)"

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
wait_healthy() {
    local rel=$1 _i h
    for _i in $(seq 1 200); do
        h=$(q "SELECT health FROM flashback_health() WHERE table_name='$rel';")
        [[ "$h" == "healthy" ]] && return 0
        q "SELECT flashback_consume_wal(4096);" >/dev/null || true
        sleep 0.1
    done
    return 1
}

q "CREATE EXTENSION pg_flashback;"
# Noise DB also loads extension for filtered WAL.
qn "CREATE EXTENSION pg_flashback;"
qn "CREATE TABLE public.noise(id bigserial PRIMARY KEY, payload text);"

q "CREATE TABLE public.steady_dml(
     id bigserial PRIMARY KEY, marker text NOT NULL, payload text NOT NULL, wide text);"
q "CREATE TABLE public.second_tracked(
     id bigserial PRIMARY KEY, marker text NOT NULL);"
q "CREATE TABLE public.restore_probe(
     id bigint PRIMARY KEY, marker text NOT NULL, payload text NOT NULL);"
q "SELECT flashback_track('public.steady_dml');" >/dev/null
q "SELECT flashback_track('public.second_tracked');" >/dev/null
q "SELECT flashback_track('public.restore_probe');" >/dev/null
wait_healthy "public.steady_dml" || die "steady_dml not healthy"
wait_healthy "public.second_tracked" || die "second_tracked not healthy"
wait_healthy "public.restore_probe" || die "restore_probe not healthy"

SEED=42
START_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
START_MONO_NS="$(exact_candidate_monotonic_now_ns)"
LAST_HB_MONO_NS="$START_MONO_NS"
LAST_OP="startup"
NEXT_DRILL="early_drop"
log "stability soak started; duration=${QUAL_DURATION_SECONDS}s max_work=$MAX_WORK_BYTES min_free=$MIN_FREE_BYTES"
write_heartbeat 0
enforce_resource_bounds

# Schedule points as fractions of the active window.
EARLY_DROP_AT=$((QUAL_DURATION_SECONDS / 20))          # ~5%
RESTART_AT=$((QUAL_DURATION_SECONDS / 10))             # ~10%
PAUSE_AT=$((QUAL_DURATION_SECONDS * 2 / 10))           # ~20%
MAINT_AT=$((QUAL_DURATION_SECONDS * 3 / 10))           # ~30%
RESTORE_AT=$((QUAL_DURATION_SECONDS * 4 / 10))         # ~40%
LATE_DROP_AT=$((QUAL_DURATION_SECONDS * 85 / 100))     # ~85%

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
    q "DROP TABLE IF EXISTS public.drop_probe;" >/dev/null
    q "CREATE TABLE public.drop_probe(id bigint PRIMARY KEY, marker text NOT NULL, payload text NOT NULL);"
    q "INSERT INTO public.drop_probe VALUES (1,'$tag', repeat('d', 500));" >/dev/null
    q "SELECT flashback_track('public.drop_probe');" >/dev/null
    wait_healthy "public.drop_probe" || die "$tag drop_probe not healthy"
    local lsn fp
    lsn=$(q "SELECT commit_lsn::text FROM flashback.delta_log
             WHERE rel_oid='public.drop_probe'::regclass
             ORDER BY commit_lsn DESC LIMIT 1;")
    for _ in $(seq 1 100); do
        local vt
        vt=$(q "SELECT valid_through_lsn::text FROM flashback.coverage_generations cg
                JOIN flashback.tracked_tables tt USING (tracking_id)
                WHERE tt.table_name='drop_probe' AND cg.state='active'
                ORDER BY cg.generation_no DESC LIMIT 1;")
        [[ -n "$vt" && "$(q "SELECT '$vt'::pg_lsn >= '$lsn'::pg_lsn;")" == "t" ]] && break
        q "SELECT flashback_consume_wal(4096);" >/dev/null || true
        sleep 0.1
    done
    fp=$(fingerprint_of "public.drop_probe")
    q "DROP TABLE public.drop_probe;" >/dev/null
    [[ "$(q "SELECT to_regclass('public.drop_probe') IS NULL;")" == "t" ]] || die "$tag drop failed"
    q "SELECT flashback_restore_lsn('public.drop_probe', '$lsn');" >/dev/null
    [[ "$(fingerprint_of "public.drop_probe")" == "$fp" ]] || die "$tag drop restore fingerprint"
    wait_healthy "public.drop_probe" || die "$tag post-drop coverage"
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
        "$PG_BIN/pg_ctl" -D "$PRIMARY_DIR" restart -w -t 60 -l "$LOG_DIR/primary.log" >/dev/null
        for _ in $(seq 1 60); do q "SELECT 1" >/dev/null 2>&1 && break; sleep 0.5; done
        wait_healthy "public.steady_dml" || die "health after restart"
        DRILL_RESTART=1
        LAST_OP=postgres_restart
        NEXT_DRILL=worker_pause
    fi
    if (( DRILL_WORKER_PAUSE == 0 && elapsed >= PAUSE_AT )); then
        NEXT_DRILL=worker_pause
        # Bounded pause via advisory lock on stream class (does not drop slot).
        "$PG_BIN/psql" -X -qAt -h "$SOCKET_DIR" -p "$PRIMARY_PORT" -d "$DB_NAME" >/dev/null 2>&1 <<'SQL' &
SET application_name='pgfb_stability_pause';
SELECT pg_advisory_lock(358945::integer,
  (SELECT oid::integer FROM pg_database WHERE datname=current_database()));
SELECT pg_sleep(20);
SELECT pg_advisory_unlock(358945::integer,
  (SELECT oid::integer FROM pg_database WHERE datname=current_database()));
SQL
        PAUSE_PID=$!
        sleep 22
        wait "$PAUSE_PID" || true
        wait_healthy "public.steady_dml" || die "health after pause"
        DRILL_WORKER_PAUSE=1
        LAST_OP=worker_pause
        NEXT_DRILL=maint_lock
    fi
    if (( DRILL_MAINT_LOCK == 0 && elapsed >= MAINT_AT )); then
        NEXT_DRILL=maint_lock
        "$PG_BIN/psql" -X -qAt -h "$SOCKET_DIR" -p "$PRIMARY_PORT" -d "$DB_NAME" >/dev/null 2>&1 <<'SQL' &
SET application_name='pgfb_stability_maint';
LOCK TABLE public.steady_dml IN ACCESS EXCLUSIVE MODE;
SELECT pg_sleep(30);
SQL
        MAINT_PID=$!
        # Second table must continue.
        q "INSERT INTO public.second_tracked(marker) VALUES ('during-maint');" >/dev/null
        sleep 32
        wait "$MAINT_PID" || true
        wait_healthy "public.second_tracked" || die "second_tracked stalled during maint"
        DRILL_MAINT_LOCK=1
        LAST_OP=maint_lock
        NEXT_DRILL=local_restore
    fi
    if (( DRILL_LOCAL_RESTORE == 0 && elapsed >= RESTORE_AT )); then
        NEXT_DRILL=local_restore
        q "INSERT INTO public.restore_probe VALUES (1, 'r', 'p')
           ON CONFLICT (id) DO UPDATE SET marker='r$elapsed', payload='p';" >/dev/null
        RLSN=$(q "SELECT commit_lsn::text FROM flashback.delta_log
                  WHERE rel_oid='public.restore_probe'::regclass
                  ORDER BY commit_lsn DESC LIMIT 1;")
        for _ in $(seq 1 80); do
            q "SELECT flashback_consume_wal(4096);" >/dev/null || true
            RVT=$(q "SELECT valid_through_lsn::text FROM flashback.coverage_generations cg
                    JOIN flashback.tracked_tables tt USING (tracking_id)
                    WHERE tt.table_name='restore_probe' AND cg.state='active'
                    ORDER BY cg.generation_no DESC LIMIT 1;")
            [[ -n "$RVT" && "$(q "SELECT '$RVT'::pg_lsn >= '$RLSN'::pg_lsn;")" == "t" ]] && break
            sleep 0.1
        done
        RFP=$(fingerprint_of "public.restore_probe")
        q "UPDATE public.restore_probe SET payload='mut';" >/dev/null
        q "SELECT flashback_restore_lsn('public.restore_probe', '$RLSN');" >/dev/null
        [[ "$(fingerprint_of "public.restore_probe")" == "$RFP" ]] || die "local restore fingerprint"
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

    enforce_resource_bounds
    if [[ "$HEAVY_WRITES_ENABLED" == "1" ]]; then
        # Pace: about one small cycle per sample interval → spreads churn across 24h.
        do_cycle
    else
        LAST_OP="idle_sample_$elapsed"
        # Still consume WAL / prove liveness.
        q "SELECT flashback_consume_wal(1024);" >/dev/null || true
        [[ "$(q "SELECT count(*) FROM pg_stat_activity WHERE backend_type='pg_flashback delta worker';")" -ge 1 ]] \
            || die "capture worker missing during idle sampling"
    fi

    # Continuous health assertions.
    [[ "$LAST_HEALTH" != "slot_lost" ]] || die "unexpected slot_lost in stability soak"
    write_heartbeat "$elapsed"
    sleep "$SAMPLE_INTERVAL"
done

# Final drain and assertions.
log "duration window complete; draining and final assertions"
FINAL_TARGET=$(q "SELECT pg_current_wal_lsn()::text;")
for _ in $(seq 1 600); do
    q "SELECT flashback_consume_wal(8192);" >/dev/null || true
    flush=$(q "SELECT confirmed_flush_lsn::text FROM pg_replication_slots
               WHERE slot_name=flashback_effective_slot_name();")
    [[ -n "$flush" && "$(q "SELECT '$flush'::pg_lsn >= '$FINAL_TARGET'::pg_lsn;")" == "t" ]] && break
    sleep 1
done
[[ "$(q "SELECT health FROM flashback_health() WHERE table_name='public.steady_dml';")" == "healthy" ]] \
    || die "final health not healthy"
[[ "$DRILL_EARLY_DROP" == "1" && "$DRILL_LATE_DROP" == "1" && "$DRILL_RESTART" == "1" \
   && "$DRILL_WORKER_PAUSE" == "1" && "$DRILL_MAINT_LOCK" == "1" && "$DRILL_LOCAL_RESTORE" == "1" ]] \
    || die "not all scheduled drills completed"
exact_candidate_verify_installed || die "binary hash mismatch at end"
STATUS=passed
log "stability soak assertions passed"
exit 0
