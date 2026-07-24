#!/usr/bin/env bash
# Short exact-candidate chaos suite for the supported local_delta path.
# Destructive backup/repository faults are intentionally absent because the
# physical-backup prototype is deferred.
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/exact_candidate_identity.sh
source "$REPO_ROOT/scripts/lib/exact_candidate_identity.sh"

CANDIDATE_DIR="${CANDIDATE_DIR:?CANDIDATE_DIR is required}"
KEEP="${PGFB_CHAOS_KEEP:-0}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
BASE="${PGFB_CHAOS_BASE:-$REPO_ROOT/target/exact-rc-chaos}"
RUN_ROOT="$BASE/runs/$RUN_ID"
RESULT_JSON="${PGFB_CHAOS_RESULT:-$BASE/results/exact-rc-chaos-$RUN_ID.json}"
SOCKET="/tmp/pgfb-chaos-$RUN_ID"
DATA="$RUN_ROOT/data"
PORT=$((34000 + ($$ % 20000)))
PRIMARY_STARTED=0
PREFIX_INSTALLED=0
EC_BOUND=0
RUN_COMPLETE=0
PASSED=0
declare -A FAULT

log() { printf '[local-chaos] %s %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { log "FAIL: $*"; exit 1; }
pass() { PASSED=$((PASSED + 1)); FAULT["$1"]=true; log "PASS[$PASSED]: $1"; }

write_result() {
    local rc=$1 status=failed
    [[ "$rc" == 0 && "$RUN_COMPLETE" == 1 ]] && status=passed
    mkdir -p "$(dirname "$RESULT_JSON")"
    jq -n \
        --arg status "$status" \
        --argjson passed "$PASSED" \
        --argjson identity "$([[ "$EC_BOUND" == 1 ]] && exact_candidate_identity_json || echo '{}')" \
        --argjson maintenance "${FAULT[maintenance_worker_restart]:-false}" \
        --argjson capture "${FAULT[capture_worker_restart]:-false}" \
        --argjson postmaster "${FAULT[postmaster_crash_restart]:-false}" \
        --argjson slot "${FAULT[slot_loss_fail_closed]:-false}" \
        '{
          qualification_kind:"exact_candidate_local_chaos",
          status:$status,
          assertions_passed:$passed,
          identity:$identity,
          faults:{
            maintenance_worker_restart:$maintenance,
            capture_worker_restart:$capture,
            postmaster_crash_restart:$postmaster,
            slot_loss_fail_closed:$slot
          }
        }' >"$RESULT_JSON"
}

cleanup() {
    local rc=$?
    trap - EXIT INT TERM
    set +e
    [[ "$PRIMARY_STARTED" == 1 ]] && "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1
    [[ "$PREFIX_INSTALLED" == 1 ]] && exact_candidate_restore_prefix >/dev/null 2>&1
    write_result "$rc"
    rm -rf "$SOCKET"
    if [[ "$RUN_COMPLETE" == 1 && "$KEEP" != 1 ]]; then rm -rf "$RUN_ROOT"; fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir -p "$RUN_ROOT" "$SOCKET"
chmod 700 "$SOCKET"
exact_candidate_bind_dir "$CANDIDATE_DIR" || die "candidate bind failed"
EC_BOUND=1
exact_candidate_install_into_prefix || die "candidate install failed"
PREFIX_INSTALLED=1

"$PG_BIN/initdb" -D "$DATA" --no-locale --encoding=UTF8 --auth=trust >/dev/null
cat >>"$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_flashback'
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
unix_socket_directories = '$SOCKET'
port = $PORT
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
pg_flashback.worker_interval_ms = 25
pg_flashback.target_databases = 'postgres'
pg_flashback.local_max_snapshot_bytes = 8GB
pg_flashback.local_max_restore_peak_bytes = 16GB
pg_flashback.local_min_filesystem_bytes = 64MB
pg_flashback.local_safety_reserve_bytes = 16MB
pg_flashback.allow_unaudited_restore = on
EOF
"$PG_BIN/pg_ctl" -D "$DATA" -l "$RUN_ROOT/postgresql.log" start -w >/dev/null
PRIMARY_STARTED=1
q() { "$PG_BIN/psql" -X -h "$SOCKET" -p "$PORT" -d postgres -v ON_ERROR_STOP=1 -qAtc "$1"; }
wait_ready() {
    for _ in $(seq 1 400); do
        [[ "$(q "SELECT admission_state FROM flashback_worker_readiness();")" == ready ]] && return 0
        sleep 0.05
    done
    return 1
}
wait_events() {
    local n=$1
    for _ in $(seq 1 400); do
        [[ "$(q "SELECT count(*) FROM flashback.delta_log
                 WHERE table_name='public.chaos_probe' AND event_type='INSERT';")" -ge "$n" ]] && return 0
        sleep 0.05
    done
    return 1
}

q "CREATE EXTENSION pg_flashback;
   CREATE TABLE public.chaos_probe(id int PRIMARY KEY, payload text NOT NULL);"
wait_ready || die "workers not ready"
q "SELECT flashback_track('public.chaos_probe');" >/dev/null
for _ in $(seq 1 400); do
    [[ "$(q "SELECT health FROM flashback_health() WHERE table_name='public.chaos_probe';")" == healthy ]] && break
    sleep 0.05
done
q "INSERT INTO public.chaos_probe VALUES (1,'baseline');"
wait_events 1 || die "baseline event missing"

OLD_MAINT="$(q "SELECT flashback_maintenance_worker_pid();")"
kill -TERM "$OLD_MAINT"
q "INSERT INTO public.chaos_probe VALUES (2,'maintenance-restart');"
wait_events 2 || die "capture stalled during maintenance restart"
for _ in $(seq 1 400); do
    NEW_MAINT="$(q "SELECT COALESCE(flashback_maintenance_worker_pid()::text,'');")"
    [[ -n "$NEW_MAINT" && "$NEW_MAINT" != "$OLD_MAINT" ]] && break
    sleep 0.05
done
[[ -n "${NEW_MAINT:-}" && "$NEW_MAINT" != "$OLD_MAINT" ]] || die "maintenance worker did not restart"
pass maintenance_worker_restart

OLD_CAPTURE="$(q "SELECT flashback_capture_worker_pid();")"
kill -TERM "$OLD_CAPTURE"
q "INSERT INTO public.chaos_probe VALUES (3,'capture-restart');"
for _ in $(seq 1 400); do
    NEW_CAPTURE="$(q "SELECT COALESCE(flashback_capture_worker_pid()::text,'');")"
    [[ -n "$NEW_CAPTURE" && "$NEW_CAPTURE" != "$OLD_CAPTURE" ]] && break
    sleep 0.05
done
[[ -n "${NEW_CAPTURE:-}" && "$NEW_CAPTURE" != "$OLD_CAPTURE" ]] || die "capture worker did not restart"
wait_events 3 || die "retained WAL was not consumed after capture restart"
pass capture_worker_restart

"$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1 || true
PRIMARY_STARTED=0
"$PG_BIN/pg_ctl" -D "$DATA" -l "$RUN_ROOT/postgresql.log" start -w >/dev/null
PRIMARY_STARTED=1
wait_ready || die "workers not ready after postmaster crash"
q "INSERT INTO public.chaos_probe VALUES (4,'postmaster-restart');"
POST_CRASH_HEALTH="$(q "SELECT health FROM flashback_health()
                         WHERE table_name='public.chaos_probe';")"
if [[ "$POST_CRASH_HEALTH" == healthy ]]; then
    wait_events 4 || die "healthy stream failed to capture after postmaster crash"
elif [[ "$POST_CRASH_HEALTH" == slot_lost ]]; then
    POST_CRASH_REASON="$(q "SELECT reason FROM flashback_health()
                            WHERE table_name='public.chaos_probe';")"
    [[ "$POST_CRASH_REASON" == replication_slot_advanced_externally ]] \
        || die "unexpected post-crash slot-loss reason: $POST_CRASH_REASON"
    q "SELECT flashback_reanchor('public.chaos_probe');" >/dev/null
    for _ in $(seq 1 400); do
        [[ "$(q "SELECT health FROM flashback_health()
                 WHERE table_name='public.chaos_probe';")" == healthy ]] && break
        sleep 0.05
    done
    [[ "$(q "SELECT health FROM flashback_health()
             WHERE table_name='public.chaos_probe';")" == healthy ]] \
        || die "post-crash fail-closed stream could not be re-anchored"
    q "INSERT INTO public.chaos_probe VALUES (5,'post-crash-reanchor');"
    wait_events 4 || die "capture failed after post-crash re-anchor"
else
    die "unexpected health after postmaster crash: $POST_CRASH_HEALTH"
fi
pass postmaster_crash_restart

SLOT="$(q "SELECT flashback_effective_slot_name();")"
CAPTURE_PID="$(q "SELECT flashback_capture_worker_pid();")"
kill -STOP "$CAPTURE_PID"
q "SELECT pg_drop_replication_slot('$SLOT');"
kill -CONT "$CAPTURE_PID" >/dev/null 2>&1 || true
for _ in $(seq 1 400); do
    HEALTH="$(q "SELECT health FROM flashback_health() WHERE table_name='public.chaos_probe';" 2>/dev/null || true)"
    [[ "$HEALTH" == slot_lost ]] && break
    sleep 0.05
done
[[ "${HEALTH:-}" == slot_lost ]] || die "slot loss was not projected fail-closed (health=${HEALTH:-missing})"
RC=0
q "SELECT flashback_restore_lsn('public.chaos_probe', pg_current_wal_lsn());" \
    >"$RUN_ROOT/slot-loss-restore.out" 2>&1 || RC=$?
[[ "$RC" != 0 ]] || die "restore beyond lost slot unexpectedly succeeded"
pass slot_loss_fail_closed

exact_candidate_verify_end_state || die "candidate identity changed"
RUN_COMPLETE=1
log "PASS: $PASSED local chaos cases"
