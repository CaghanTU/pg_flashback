#!/usr/bin/env bash
# Exact-WAL restart adversarial regressions: the two named gaps in Step 6's
# audited coverage matrix that no existing script exercises with a genuine
# postmaster restart in the middle of the scenario.
#
#   Case A: a tracked table is DROPed while the capture worker is idled
#           (pg_flashback.enabled=off), so the DROP and its WAL sit unconsumed
#           in the slot ("severe lag") across a full PostgreSQL restart, then
#           capture is re-enabled and the DROP is recovered with an exact
#           fingerprint match -- worker/postmaster restart must never lose or
#           duplicate the boundary.
#   Case B: a recover is intentionally crashed at the before_materialize
#           restore failpoint (RAISEs before touching the destination), then
#           PostgreSQL itself is restarted (not just a same-session retry) to
#           prove the durable operation_journal correctly reconciles/retries
#           after a hard restart instead of leaving a half-applied or
#           permanently stuck operation.
#
# Installs ONLY from CANDIDATE_DIR (scripts/build_candidate_archive.sh
# output). Never cargo-builds. Bind failure (missing/tampered manifest,
# archive digest mismatch, dirty/mismatched source tree, wrong arch/PG major,
# or an installed .so whose hash disagrees with the manifest) is fail-closed:
# the run aborts before any cluster is started. Result JSON carries the full
# candidate identity (source_commit, source_tree, package_sha256,
# extension_binary_sha256, installed_extension_sha256, candidate_dir,
# pg_major, arch) alongside per-case results.
#
# Usage: CANDIDATE_DIR=/path/to/candidate ./scripts/run_exact_wal_restart_recovery_adversarial.sh
# Env:   CANDIDATE_DIR (required), PGFB_RESTART_ADV_PORT, PGFB_RESTART_ADV_KEEP=1

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$ROOT"
export REPO_ROOT
# shellcheck source=scripts/lib/exact_candidate_identity.sh
source "$ROOT/scripts/lib/exact_candidate_identity.sh"

CANDIDATE_DIR="${CANDIDATE_DIR:?CANDIDATE_DIR is required: bind this run to a built candidate archive (scripts/build_candidate_archive.sh)}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
PORT="${PGFB_RESTART_ADV_PORT:-28967}"
WORK_ROOT="${PGFB_RESTART_ADV_WORK_ROOT:-$ROOT/target/exact-wal-restart-adversarial/$RUN_ID}"
DATA="$WORK_ROOT/data"
SOCKET="/tmp/pgfb-restart-adv-$RUN_ID"
RESULT_JSON="${PGFB_RESTART_ADV_RESULT:-$ROOT/target/qualification/exact-wal-restart-adversarial-$RUN_ID.json}"
LOG="$WORK_ROOT/postgresql.log"
DB=postgres
PASSED=0
FAILED=0
EC_BOUND=0
PREFIX_INSTALLED=0
PRIMARY_STARTED=0
declare -a CASE_RESULTS=()

die() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "  PASS: $*"; PASSED=$((PASSED + 1)); CASE_RESULTS+=("{\"name\":$(jq -Rn --arg s "$*" '$s'),\"pass\":true}"); }

cleanup() {
    local rc=$?
    set +e
    if [[ "$PRIMARY_STARTED" == 1 ]]; then
        "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1
    fi
    if [[ "${PGFB_RESTART_ADV_KEEP:-0}" != "1" ]]; then
        rm -rf "$DATA" "$SOCKET"
    else
        echo "PGFB_RESTART_ADV_KEEP=1: leaving $WORK_ROOT" >&2
    fi
    if [[ "$PREFIX_INSTALLED" == 1 ]]; then
        exact_candidate_restore_prefix || true
    fi
    mkdir -p "$(dirname "$RESULT_JSON")"
    local arr
    arr=$(printf '%s\n' "${CASE_RESULTS[@]:-}" | jq -s '.')
    jq -n \
        --arg run_id "$RUN_ID" --argjson passed "$PASSED" --argjson failed "$FAILED" \
        --argjson cases "$arr" --argjson exit_code "$rc" \
        --argjson identity "$([[ "$EC_BOUND" == 1 ]] && exact_candidate_identity_json || echo '{}')" \
        '{run_id:$run_id,passed:$passed,failed:$failed,cases:$cases,exit_code:$exit_code,
          identity:$identity,
          status:(if $failed==0 and $exit_code==0 then "PASS" else "FAIL" end)}' \
        >"$RESULT_JSON"
    echo "exact-WAL restart adversarial: $([[ $FAILED -eq 0 && $rc -eq 0 ]] && echo PASS || echo FAIL) ($RESULT_JSON)"
    [[ $FAILED -eq 0 && $rc -eq 0 ]] || exit 1
    exit 0
}
trap cleanup EXIT

exact_candidate_bind_dir "$CANDIDATE_DIR" || die "candidate bind failed"
EC_BOUND=1
exact_candidate_install_into_prefix || die "candidate install failed"
PREFIX_INSTALLED=1
exact_candidate_verify_installed || die "installed identity mismatch"
SHARE_DIR="$("$PG_BIN/pg_config" --sharedir)"
PSQL="$PG_BIN/psql"

[[ -x "$PSQL" ]] || die "psql not found under candidate-bound $PG_BIN"
[[ -f "$SHARE_DIR/extension/pg_flashback.control" ]] || die "extension not installed in $SHARE_DIR"

mkdir -p "$WORK_ROOT" "$SOCKET"
"$PG_BIN/initdb" -D "$DATA" --locale=C.UTF-8 -A trust >/dev/null
cat >>"$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_flashback'
wal_level = logical
max_replication_slots = 16
max_wal_senders = 16
max_worker_processes = 16
pg_flashback.enabled = on
pg_flashback.capture_mode = wal
pg_flashback.worker_interval_ms = 50
pg_flashback.target_databases = 'postgres'
pg_flashback.local_max_snapshot_bytes = 8GB
pg_flashback.local_max_restore_peak_bytes = 16GB
pg_flashback.local_min_filesystem_bytes = 64MB
pg_flashback.local_safety_reserve_bytes = 16MB
pg_flashback.allow_unaudited_restore = on
EOF
"$PG_BIN/pg_ctl" -D "$DATA" -l "$LOG" -o "-p $PORT -k $SOCKET" start -w >/dev/null
PRIMARY_STARTED=1

q() { "$PSQL" -X -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAtc "$1"; }
qe() { "$PSQL" -X -h "$SOCKET" -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -qAtc "$1" 2>&1; }
PGUSER="$(id -un)"
export PGHOST="$SOCKET" PGPORT="$PORT" PGUSER PGDATABASE="$DB"

restart_pg() {
    "$PG_BIN/pg_ctl" -D "$DATA" restart -w -t 60 -l "$LOG" >/dev/null
    for _ in $(seq 1 60); do
        q "SELECT 1" >/dev/null 2>&1 && return 0
        sleep 0.5
    done
    return 1
}

wait_worker() {
    local timeout_s=60 deadline
    deadline=$(( $(date +%s) + timeout_s ))
    while (( $(date +%s) <= deadline )); do
        q "SELECT count(*)>0 FROM pg_stat_activity
            WHERE backend_type LIKE 'pg_flashback%' AND datname=current_database();" | grep -q t && return 0
        sleep 0.2
    done
    return 1
}

wait_health() {
    local table=$1 expected=${2:-healthy} timeout_s=${3:-60} deadline health
    deadline=$(( $(date +%s) + timeout_s ))
    while (( $(date +%s) <= deadline )); do
        health=$(q "SELECT COALESCE((SELECT h.health FROM flashback_health() h
            WHERE h.table_name = '$table' ORDER BY h.generation_id DESC LIMIT 1),'missing');")
        [[ "$health" == "$expected" ]] && return 0
        sleep 0.1
    done
    return 1
}

wait_drop() {
    local table=$1 timeout_s=${2:-60} deadline n
    deadline=$(( $(date +%s) + timeout_s ))
    while (( $(date +%s) <= deadline )); do
        n=$(q "SELECT count(*) FROM flashback.delta_log
            WHERE event_type='DROP' AND table_name='$table';")
        [[ "$n" != "0" ]] && return 0
        sleep 0.1
    done
    return 1
}

wait_restorable() {
    local table=$1 timeout_s=${2:-60} deadline
    deadline=$(( $(date +%s) + timeout_s ))
    while (( $(date +%s) <= deadline )); do
        [[ "$(q "SELECT flashback_recover_plan('$table')->>'status';")" == "restorable" ]] && return 0
        sleep 0.2
    done
    return 1
}

recover_begin_execute() {
    local table=$1 token op
    token=$(q "SELECT flashback_recover_plan('$table')->>'plan_token';")
    op=$(q "SELECT flashback_recover_begin('$table', '$token')->>'operation_id';")
    q "SELECT flashback_recover_execute('$table', '$token', interval '24 hours', NULL, NULL, NULL, $op);" >/dev/null
    echo "$op"
}

# Freeze the delta worker with SIGSTOP while it is idle between polls (not
# mid-transaction) so DDL/DML capture stays fully live -- unlike
# pg_flashback.enabled=off, which is a global kill-switch that also refuses
# new DDL capture -- and WAL genuinely piles up unconsumed, the actual
# "severe lag" condition this case needs.
stop_idle_worker() {
    local pid=""
    for _ in $(seq 1 100); do
        pid=$(q "SELECT pid FROM pg_stat_activity
            WHERE backend_type='pg_flashback delta worker'
              AND datname=current_database()
              AND wait_event_type='Extension'
            LIMIT 1")
        if [[ -n "$pid" ]]; then
            kill -STOP "$pid"
            sleep 0.05
            if [[ "$(q "SELECT count(*) FROM pg_locks
                         WHERE pid=$pid AND locktype='advisory' AND granted")" == "0" ]]; then
                echo "$pid"
                return 0
            fi
            kill -CONT "$pid" >/dev/null 2>&1 || true
        fi
        sleep 0.05
    done
    return 1
}

q "CREATE EXTENSION pg_flashback;" >/dev/null
q "ALTER SYSTEM SET pg_flashback.target_databases = 'postgres';" >/dev/null
restart_pg || die "initial restart after target_databases"
wait_worker || die "worker did not attach"

# ── Case A: idled-worker lag refuses DROP; retry survives full restart ─────
q "CREATE TABLE public.radv_lagdrop(id int PRIMARY KEY, payload text);"
q "SELECT flashback_track('public.radv_lagdrop');" >/dev/null
wait_health public.radv_lagdrop healthy || die "A: initial health"
q "INSERT INTO public.radv_lagdrop SELECT g, repeat('x', 200) FROM generate_series(1,500) g;"
wait_health public.radv_lagdrop healthy || die "A: health after seed dml"

# Freeze the delta worker (not pg_flashback.enabled=off): DML still commits and
# WAL genuinely piles up. Destructive DDL must now refuse this state because an
# unchanged external TOAST value cannot be reconstructed after DROP unlinks
# the relation files. This fail-closed refusal is the product contract.
WORKER_PID=$(stop_idle_worker) || die "A: could not freeze delta worker"
q "UPDATE public.radv_lagdrop SET payload = 'late-' || id WHERE id <= 50;"
# Fingerprint reflects the state immediately before DROP (i.e. including the
# late UPDATE), since exact recovery must reconstruct the table as of the
# DROP, not as of the earlier seed insert.
FP_BEFORE=$(q "SELECT md5(string_agg(id::text||':'||payload, ',' ORDER BY id)) FROM public.radv_lagdrop;")
set +e
DROP_REFUSAL=$(qe "DROP TABLE public.radv_lagdrop;")
DROP_REFUSAL_RC=$?
set -e
[[ "$DROP_REFUSAL_RC" -ne 0 ]] || die "A: DROP unexpectedly succeeded while capture worker was frozen"
[[ "$DROP_REFUSAL" == *"destructive DDL refused"* ]] \
    || die "A: DROP refusal did not report the fail-closed pre-drain contract: $DROP_REFUSAL"
[[ "$(q "SELECT to_regclass('public.radv_lagdrop') IS NOT NULL;")" == "t" ]] \
    || die "A: refused DROP changed the live table"
pass "A1: DROP is fail-closed while committed relation WAL cannot drain"

# Resume the worker and retry the exact same DROP. The pre-DROP SHARE fence
# lets the independent worker consume the committed UPDATE while preventing
# new writers, then the real DROP upgrades to ACCESS EXCLUSIVE.
kill -CONT "$WORKER_PID" >/dev/null 2>&1 || true
q "DROP TABLE public.radv_lagdrop;"

# The full stop/start proves the admitted pre-DROP history and the DROP marker
# survive a real postmaster restart regardless of whether the fast worker
# consumed the marker just before shutdown.
"$PG_BIN/pg_ctl" -D "$DATA" stop -m fast -w -t 60 >/dev/null
"$PG_BIN/pg_ctl" -D "$DATA" -l "$LOG" -o "-p $PORT -k $SOCKET" start -w >/dev/null
for _ in $(seq 1 60); do q "SELECT 1" >/dev/null 2>&1 && break; sleep 0.5; done

wait_worker || die "A: worker did not reattach after restart"
wait_drop public.radv_lagdrop 120 || die "A: DROP was not observed after restart+catch-up"
wait_restorable public.radv_lagdrop 60 || die "A: DROP not restorable after restart+catch-up"

recover_begin_execute public.radv_lagdrop >/dev/null
wait_health public.radv_lagdrop healthy || die "A: post-recover health"
FP_AFTER=$(q "SELECT md5(string_agg(id::text||':'||payload, ',' ORDER BY id)) FROM public.radv_lagdrop;")
[[ "$FP_AFTER" == "$FP_BEFORE" ]] || die "A: fingerprint mismatch after restart-survived DROP recovery ($FP_AFTER vs $FP_BEFORE)"
pass "A2: retried DROP recovers exactly across a full PostgreSQL restart"

# ── Case B: restore failpoint crash, then a full restart, then reconcile+retry ──
q "CREATE TABLE public.radv_failcrash(id int PRIMARY KEY, v text);"
q "SELECT flashback_track('public.radv_failcrash');" >/dev/null
wait_health public.radv_failcrash healthy || die "B: initial health"
q "INSERT INTO public.radv_failcrash VALUES (1,'a'),(2,'b');"
wait_health public.radv_failcrash healthy || die "B: health after dml"
FP_B_BEFORE=$(q "SELECT md5(string_agg(id::text||':'||v, ',' ORDER BY id)) FROM public.radv_failcrash;")
q "DROP TABLE public.radv_failcrash;"
wait_drop public.radv_failcrash || die "B: drop not captured"

TOKEN=$(q "SELECT flashback_recover_plan('public.radv_failcrash')->>'plan_token';")
OP=$(q "SELECT flashback_recover_begin('public.radv_failcrash', '$TOKEN')->>'operation_id';")
[[ "$(q "SELECT state FROM flashback.operation_current_state WHERE operation_id=$OP;")" == "started" ]] \
    || die "B: begin not durable started"

q "ALTER SYSTEM SET pg_flashback.test_restore_failpoint = 'before_materialize';" >/dev/null
q "SELECT pg_reload_conf();" >/dev/null
set +e
qe "SELECT flashback_recover_execute('public.radv_failcrash', '$TOKEN', interval '24 hours', NULL, NULL, NULL, $OP);" >/dev/null
RC=$?
set -e
[[ $RC -ne 0 ]] || die "B: before_materialize failpoint did not fail as expected"
[[ "$(q "SELECT to_regclass('public.radv_failcrash') IS NULL;")" == "t" ]] \
    || die "B: table mutated by a recover that should have failed before materialize"
[[ "$(q "SELECT state FROM flashback.operation_current_state WHERE operation_id=$OP;")" == "started" ]] \
    || die "B: operation header rolled back with the failed execute transaction"

# Simulate an actual crash: the operation is left at 'started' (never marked
# failed by the crashed session) and PostgreSQL itself restarts -- not just
# the client retrying in the same connection. The stale failpoint GUC value
# is cleared via ALTER SYSTEM before restart so the retry after reconcile can
# succeed instead of hitting the same deliberate failpoint again.
q "ALTER SYSTEM RESET pg_flashback.test_restore_failpoint;" >/dev/null
"$PG_BIN/pg_ctl" -D "$DATA" stop -m fast -w -t 60 >/dev/null
"$PG_BIN/pg_ctl" -D "$DATA" -l "$LOG" -o "-p $PORT -k $SOCKET" start -w >/dev/null
for _ in $(seq 1 60); do q "SELECT 1" >/dev/null 2>&1 && break; sleep 0.5; done
wait_worker || die "B: worker did not reattach after restart"

# Post-restart, the table must still be exactly absent (no half-applied
# shadow/swap survived the crash) and the stale 'started' operation must
# still be present and reconcilable, not silently vanished.
[[ "$(q "SELECT to_regclass('public.radv_failcrash') IS NULL;")" == "t" ]] \
    || die "B: table exists after restart following a before_materialize crash"
[[ "$(q "SELECT state FROM flashback.operation_current_state WHERE operation_id=$OP;")" == "started" ]] \
    || die "B: crashed operation state did not survive the restart as durably 'started'"

# operation_events is append-only; backdating this harness-only probe row to
# simulate elapsed wall-clock time requires the standard superuser bypass,
# never a product API (same pattern as run_dba_acceptance_regressions.sh).
q "SET session_replication_role = replica;
   UPDATE flashback.operation_events SET recorded_at = clock_timestamp() - interval '10 minutes'
    WHERE operation_id=$OP AND event_type='started';
   SET session_replication_role = DEFAULT;" >/dev/null
q "SELECT flashback_reconcile_recover_operations(interval '1 minute');" >/dev/null
[[ "$(q "SELECT state FROM flashback.operation_current_state WHERE operation_id=$OP;")" == "abandoned" ]] \
    || die "B: post-restart crashed operation was not reconciled to abandoned"

wait_restorable public.radv_failcrash 60 || die "B: not restorable after reconcile"
recover_begin_execute public.radv_failcrash >/dev/null
wait_health public.radv_failcrash healthy || die "B: retry health after reconcile"
FP_B_AFTER=$(q "SELECT md5(string_agg(id::text||':'||v, ',' ORDER BY id)) FROM public.radv_failcrash;")
[[ "$FP_B_AFTER" == "$FP_B_BEFORE" ]] || die "B: fingerprint mismatch after post-restart reconcile+retry"
pass "B: restore failpoint crash reconciles and retries exactly across a full PostgreSQL restart"

echo "exact-WAL restart adversarial: all cases passed"
