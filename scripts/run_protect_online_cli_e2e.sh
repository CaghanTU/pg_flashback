#!/usr/bin/env bash
# Step 9 Phase 3: real, isolated E2E for the *installed CLI* driving online
# external_zstd protection end-to-end -- scripts/pg_flashback, not raw SQL
# step calls. Covers: happy path (protect -> "Protection active."), resume
# after an interrupted reservation (flashback_protect_begin committed but
# the CLI process never continued), and protect-abort convergence
# (including replica identity restoration and freeing the name for retry).
#
# Same throwaway-instance recipe as run_protect_online_happy_path_e2e.sh.
#
# Usage:
#   ./scripts/run_protect_online_cli_e2e.sh
#
# Env:
#   PG_CONFIG                 pg_config to build/install against (default:
#                              the pgrx-managed pg17 install)
#   PGFB_CLI_E2E_KEEP=1        keep the work dir even on PASS

set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PG_CONFIG="${PG_CONFIG:-$HOME/.pgrx/17.10/pgrx-install/bin/pg_config}"
PG_BIN="$("$PG_CONFIG" --bindir)"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
WORK="${PGFB_CLI_E2E_WORK:-$ROOT/target/protect-online-cli-e2e/$RUN_ID}"
mkdir -p "$WORK"
ARTIFACT_ROOT="$WORK/external_snapshots"
mkdir -p "$ARTIFACT_ROOT"
chmod 0700 "$ARTIFACT_ROOT"

log() { printf '[protect-online-cli-e2e] %s %s\n' "$(date +%H:%M:%S)" "$*"; }
FAILED=0
die() { FAILED=1; log "FAIL: $*"; exit 1; }

DATA="$WORK/data"
SOCKET="/tmp/pgfb-cli-e2e-$RUN_ID"
mkdir -p "$SOCKET"

cleanup() {
    "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1 || true
    if [[ "$FAILED" == "0" && "${PGFB_CLI_E2E_KEEP:-0}" != "1" ]]; then
        rm -rf "$WORK" "$SOCKET"
    else
        log "evidence retained at $WORK"
    fi
}
trap cleanup EXIT

cd "$ROOT"
log "installing current tip into the pgrx-managed pg17 prefix"
cargo pgrx install --pg-config "$PG_CONFIG" --no-default-features --features pg17 \
    >"$WORK/install.log" 2>&1 \
    || die "pgrx install failed; see $WORK/install.log"

"$PG_BIN/initdb" -D "$DATA" --locale=C.UTF-8 -A trust >"$WORK/initdb.log" 2>&1 \
    || die "initdb failed; see $WORK/initdb.log"

cat >> "$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_flashback'
wal_level = logical
max_replication_slots = 10
max_worker_processes = 16
listen_addresses = ''
unix_socket_directories = '$SOCKET'
log_line_prefix = '%m [%p] %q%a '
pg_flashback.target_databases = 'postgres'
pg_flashback.capture_mode = 'wal'
pg_flashback.snapshot_storage_backend = 'external_zstd'
pg_flashback.external_snapshot_root = '$ARTIFACT_ROOT'
pg_flashback.external_snapshot_min_free_bytes = '1MB'
pg_flashback.external_snapshot_safety_reserve_bytes = '1MB'
pg_flashback.local_boundary_write_stall_ms = 30000
pg_flashback.allow_unaudited_restore = on
pg_flashback.local_max_snapshot_bytes = 8GB
pg_flashback.local_max_restore_peak_bytes = 16GB
pg_flashback.local_min_filesystem_bytes = 64MB
pg_flashback.local_safety_reserve_bytes = 16MB
EOF

log "starting postgres"
"$PG_BIN/pg_ctl" -D "$DATA" -l "$WORK/postgres.log" -w start \
    || die "postgres failed to start; see $WORK/postgres.log"

psql_q() { "$PG_BIN/psql" -h "$SOCKET" -d postgres -X -q -v ON_ERROR_STOP=1 "$@"; }
psql_scalar() { psql_q -tAc "$1"; }

psql_q -c "CREATE EXTENSION pg_flashback;" || die "CREATE EXTENSION failed"
psql_q -c "GRANT flashback_admin TO CURRENT_USER;" || die "could not grant flashback_admin to CURRENT_USER"

log "waiting for admitted capture worker"
running=""
for _ in $(seq 1 30); do
    running="$(psql_scalar "SELECT capture_running FROM flashback_worker_readiness();")"
    [[ "$running" == "t" ]] && break
    sleep 1
done
[[ "$running" == "t" ]] || die "capture worker never became admitted/running"

export PGHOST="$SOCKET"
export PGDATABASE=postgres
export PSQL_BIN="$PG_BIN/psql"
CLI=("$ROOT/scripts/pg_flashback")

# ==================================================================
# Case 1: happy path via the installed CLI.
# ==================================================================
log "case 1: happy path -- pg_flashback protect"
psql_q -c "CREATE TABLE public.cli_e2e_happy (id int PRIMARY KEY, note text);"
psql_q -c "INSERT INTO public.cli_e2e_happy VALUES (1,'a'),(2,'b');"

OUT="$("${CLI[@]}" protect public.cli_e2e_happy --timeout 60 2>&1)" \
    || die "case 1: pg_flashback protect failed: $OUT"
echo "$OUT" | grep -q "Protection active." \
    || die "case 1: CLI did not report 'Protection active.': $OUT"
PROTECTED="$(psql_scalar "SELECT flashback_is_actively_protected('public.cli_e2e_happy');")"
[[ "$PROTECTED" == "t" ]] || die "case 1: table not actively protected after CLI protect"
log "case 1: PASS"

log "case 1b: re-running protect on an already-protected table is a clean no-op"
OUT="$("${CLI[@]}" protect public.cli_e2e_happy 2>&1)" \
    || die "case 1b: re-protect failed: $OUT"
echo "$OUT" | grep -qi "Already protected" \
    || die "case 1b: re-protect did not report already_protected: $OUT"
log "case 1b: PASS"

# ==================================================================
# Case 2: resume after an interrupted reservation (step 1 committed,
# CLI process never continued -- simulated by calling flashback_protect_begin
# directly via psql, exactly what a crashed CLI leaves behind).
# ==================================================================
log "case 2: resume-by-identity after an interrupted reservation"
psql_q -c "CREATE TABLE public.cli_e2e_resume (id int PRIMARY KEY);"
psql_q -c "INSERT INTO public.cli_e2e_resume VALUES (1);"
RESUME_OP_ID="$(psql_scalar "SELECT (flashback_protect_begin('public.cli_e2e_resume'))->>'operation_id';")"
[[ -n "$RESUME_OP_ID" ]] || die "case 2: flashback_protect_begin did not return operation_id"
log "case 2: reservation operation_id=$RESUME_OP_ID left interrupted; now resuming via CLI"

OUT="$("${CLI[@]}" protect public.cli_e2e_resume --timeout 60 2>&1)" \
    || die "case 2: resume via CLI failed: $OUT"
echo "$OUT" | grep -q "Resuming protect operation_id=$RESUME_OP_ID" \
    || die "case 2: CLI did not report resuming the exact interrupted operation_id: $OUT"
echo "$OUT" | grep -q "Protection active." \
    || die "case 2: resumed protect did not reach Protection active.: $OUT"
log "case 2: PASS"

# ==================================================================
# Case 3: protect-abort convergence, including replica identity
# restoration, and that the name is free for a fresh protect afterward.
# ==================================================================
log "case 3: protect-abort on an interrupted reservation"
psql_q -c "CREATE TABLE public.cli_e2e_abort (id int PRIMARY KEY);"
psql_q -c "INSERT INTO public.cli_e2e_abort VALUES (1);"
ORIG_RELIDENT="$(psql_scalar "SELECT relreplident::text FROM pg_class WHERE oid = 'public.cli_e2e_abort'::regclass;")"
ABORT_OP_ID="$(psql_scalar "SELECT (flashback_protect_begin('public.cli_e2e_abort'))->>'operation_id';")"
[[ -n "$ABORT_OP_ID" ]] || die "case 3: flashback_protect_begin did not return operation_id"
psql_q -c "SELECT flashback_protect_prepare_replica_identity($ABORT_OP_ID);" >/dev/null
RELIDENT_AFTER_PREPARE="$(psql_scalar "SELECT relreplident::text FROM pg_class WHERE oid = 'public.cli_e2e_abort'::regclass;")"
[[ "$RELIDENT_AFTER_PREPARE" == "f" ]] || die "case 3: prepare_replica_identity did not set FULL"

OUT="$("${CLI[@]}" protect-abort "$ABORT_OP_ID" --yes 2>&1)" \
    || die "case 3: protect-abort failed: $OUT"
echo "$OUT" | grep -qi "abandoned\|failed" \
    || die "case 3: protect-abort did not report a terminal status: $OUT"

RELIDENT_AFTER_ABORT="$(psql_scalar "SELECT relreplident::text FROM pg_class WHERE oid = 'public.cli_e2e_abort'::regclass;")"
[[ "$RELIDENT_AFTER_ABORT" == "$ORIG_RELIDENT" ]] \
    || die "case 3: replica identity not restored (orig=$ORIG_RELIDENT after=$RELIDENT_AFTER_ABORT)"
PS_AFTER_ABORT="$(psql_scalar "SELECT protection_state FROM flashback.tracked_tables WHERE tracking_id = (SELECT tracking_id FROM flashback.operation_current_state WHERE operation_id = $ABORT_OP_ID);")"
[[ "$PS_AFTER_ABORT" == "abandoned" ]] || die "case 3: protection_state not abandoned after abort: $PS_AFTER_ABORT"

log "case 3b: retry protect-abort on the same operation_id is an idempotent no-op"
OUT="$("${CLI[@]}" protect-abort "$ABORT_OP_ID" --yes 2>&1)" \
    || die "case 3b: repeat protect-abort call failed: $OUT"
echo "$OUT" | grep -qi "already converged\|abandoned\|failed" \
    || die "case 3b: repeat protect-abort did not report idempotent no-op: $OUT"

log "case 3c: the table name is free again for a fresh protect after abandonment"
OUT="$("${CLI[@]}" protect public.cli_e2e_abort --timeout 60 2>&1)" \
    || die "case 3c: fresh protect after abandonment failed: $OUT"
echo "$OUT" | grep -q "Protection active." \
    || die "case 3c: fresh protect after abandonment did not reach Protection active.: $OUT"
log "case 3: PASS"

# ==================================================================
# Case 4: doctor and status surfaces do not crash and never claim
# "protected"/"ok" for the interrupted reservation left by case 3 before
# it was aborted -- re-verify against a fresh interrupted reservation.
# ==================================================================
log "case 4: doctor/status never report healthy/protected before activation"
psql_q -c "CREATE TABLE public.cli_e2e_doctor (id int PRIMARY KEY);"
DOCTOR_OP_ID="$(psql_scalar "SELECT (flashback_protect_begin('public.cli_e2e_doctor'))->>'operation_id';")"
[[ -n "$DOCTOR_OP_ID" ]] || die "case 4: flashback_protect_begin did not return operation_id"

DOCTOR_JSON="$("${CLI[@]}" --json doctor 2>&1)" || die "case 4: doctor --json failed: $DOCTOR_JSON"
printf '%s' "$DOCTOR_JSON" | jq -e '.data[] | select(.check_name == "protect_in_progress" and (.observed | contains("cli_e2e_doctor")))' >/dev/null \
    || die "case 4: doctor did not surface the in-progress protect for cli_e2e_doctor: $DOCTOR_JSON"
printf '%s' "$DOCTOR_JSON" | jq -e '.data[] | select(.check_name == "protect_in_progress" and (.observed | contains("cli_e2e_doctor")) and .status == "ok")' >/dev/null \
    && die "case 4: doctor incorrectly reported ok status for an in-progress (non-active) protect"

STATUS_JSON="$("${CLI[@]}" --json status public.cli_e2e_doctor 2>&1)" || die "case 4: status --json failed: $STATUS_JSON"
printf '%s' "$STATUS_JSON" | jq -e '.tables[0].protect_in_progress.action' >/dev/null \
    || die "case 4: status did not surface protect_in_progress for cli_e2e_doctor: $STATUS_JSON"

psql_q -c "SELECT flashback_protect_abort($DOCTOR_OP_ID);" >/dev/null
log "case 4: PASS"

# ==================================================================
# Case 5: full "abandoned" lifecycle audit -- aborted protect -> cleanup,
# aborted protect -> protect again, historical lifecycle remains
# distinguishable via the operation journal, no non-terminal
# generation/snapshot/artifact leaks, never reported protected/healthy at
# any point.
# ==================================================================
log "case 5: full abandoned-lifecycle audit (cleanup, reprotect, journal, no leaks)"
psql_q -c "CREATE TABLE public.cli_e2e_audit (id int PRIMARY KEY);"
psql_q -c "INSERT INTO public.cli_e2e_audit VALUES (1);"
AUDIT_OP_ID="$(psql_scalar "SELECT (flashback_protect_begin('public.cli_e2e_audit'))->>'operation_id';")"
[[ -n "$AUDIT_OP_ID" ]] || die "case 5: flashback_protect_begin did not return operation_id"
AUDIT_TRACKING_ID="$(psql_scalar "SELECT tracking_id FROM flashback.operation_current_state WHERE operation_id = $AUDIT_OP_ID;")"
[[ -n "$AUDIT_TRACKING_ID" ]] || die "case 5: could not resolve tracking_id for operation_id=$AUDIT_OP_ID"

# Never reported protected/healthy while starting.
HEALTHY_WHILE_STARTING="$(psql_scalar "SELECT flashback_is_actively_protected('public.cli_e2e_audit');")"
[[ "$HEALTHY_WHILE_STARTING" == "f" ]] || die "case 5: table reported actively protected while still starting"
# A 'starting' lifecycle has is_active=true (set at reservation, so
# flashback_consume_wal's tracked_oids computation sees it), so it DOES
# appear in flashback_health() -- but must never show health='healthy'.
HEALTH_WHILE_STARTING="$(psql_scalar "SELECT health FROM flashback_health() WHERE table_name = 'public.cli_e2e_audit';")"
[[ "$HEALTH_WHILE_STARTING" != "healthy" ]] || die "case 5: flashback_health() reported healthy for a still-starting lifecycle"

# Clean abort (no relation-identity change) -> genuinely 'abandoned', not
# 'failed'.
ABORT_JSON="$(psql_q -tAc "SELECT flashback_protect_abort($AUDIT_OP_ID);")"
printf '%s' "$ABORT_JSON" | grep -q '"status": "abandoned"' \
    || die "case 5: expected a clean abort to journal abandoned, got: $ABORT_JSON"

# Never reported protected/healthy while abandoned-awaiting-cleanup either.
HEALTHY_WHILE_ABANDONED="$(psql_scalar "SELECT flashback_is_actively_protected('public.cli_e2e_audit');")"
[[ "$HEALTHY_WHILE_ABANDONED" == "f" ]] || die "case 5: table reported actively protected while abandoned"
HEALTH_ROWS_WHILE_ABANDONED="$(psql_scalar "SELECT count(*) FROM flashback_health() WHERE table_name = 'public.cli_e2e_audit';")"
[[ "$HEALTH_ROWS_WHILE_ABANDONED" == "0" ]] || die "case 5: flashback_health() listed an abandoned lifecycle"

# No non-terminal generation/snapshot leaks: the generation and snapshot
# reserved for this attempt must both be in a terminal (aborted) state, and
# no OTHER row for this tracking_id may be non-terminal.
NON_TERMINAL_GEN="$(psql_scalar "SELECT count(*) FROM flashback.coverage_generations WHERE tracking_id = $AUDIT_TRACKING_ID AND state NOT IN ('aborted','sealed','retired');")"
[[ "$NON_TERMINAL_GEN" == "0" ]] || die "case 5: leaked non-terminal coverage_generations row(s) for an abandoned lifecycle"
NON_TERMINAL_SNAP="$(psql_scalar "SELECT count(*) FROM flashback.snapshots WHERE tracking_id = $AUDIT_TRACKING_ID AND payload_state NOT IN ('aborted','retired');")"
[[ "$NON_TERMINAL_SNAP" == "0" ]] || die "case 5: leaked non-terminal snapshots row(s) for an abandoned lifecycle"

# aborted protect -> cleanup.
CLEANUP_JSON="$("${CLI[@]}" cleanup --tracking-id "$AUDIT_TRACKING_ID" --yes 2>&1)" \
    || die "case 5: cleanup on an abandoned tracking_id failed: $CLEANUP_JSON"
echo "$CLEANUP_JSON" | grep -qi "cleaned" \
    || die "case 5: cleanup did not report cleaned for an abandoned tracking_id: $CLEANUP_JSON"
PS_AFTER_CLEANUP="$(psql_scalar "SELECT protection_state FROM flashback.tracked_tables WHERE tracking_id = $AUDIT_TRACKING_ID;")"
[[ "$PS_AFTER_CLEANUP" == "cleaned" ]] || die "case 5: expected protection_state=cleaned after cleanup, got: $PS_AFTER_CLEANUP"
RESIDUAL_DELTA="$(psql_scalar "SELECT count(*) FROM flashback.delta_log WHERE tracking_id = $AUDIT_TRACKING_ID;")"
[[ "$RESIDUAL_DELTA" == "0" ]] || die "case 5: cleanup left residual delta_log rows for tracking_id=$AUDIT_TRACKING_ID"

# aborted protect -> protect again: a fresh attempt under the same name
# gets a NEW tracking_id (never reuses or resurrects the cleaned one).
OUT="$("${CLI[@]}" protect public.cli_e2e_audit --timeout 60 2>&1)" \
    || die "case 5: fresh protect after cleanup failed: $OUT"
echo "$OUT" | grep -q "Protection active\." \
    || die "case 5: fresh protect after cleanup did not reach Protection active.: $OUT"
NEW_TRACKING_ID="$(psql_scalar "SELECT tracking_id FROM flashback.tracked_tables WHERE table_name = 'cli_e2e_audit' AND is_active;")"
[[ -n "$NEW_TRACKING_ID" && "$NEW_TRACKING_ID" != "$AUDIT_TRACKING_ID" ]] \
    || die "case 5: fresh protect must use a new tracking_id, not reuse/resurrect $AUDIT_TRACKING_ID (got: $NEW_TRACKING_ID)"

# Historical lifecycle remains distinguishable: the OLD tracking_id's
# journal still shows the exact abandoned-then-cleaned sequence, separate
# from the NEW tracking_id's activated sequence -- both coexist and are
# never confused with each other.
OLD_JOURNAL="$(psql_scalar "SELECT string_agg(s.state, ',' ORDER BY s.state_at) FROM flashback.operation_current_state s JOIN flashback.operations o ON o.operation_id = s.operation_id WHERE o.tracking_id = $AUDIT_TRACKING_ID;")"
echo "$OLD_JOURNAL" | grep -q "abandoned" || die "case 5: old tracking_id's journal lost its abandoned event: $OLD_JOURNAL"
echo "$OLD_JOURNAL" | grep -q "cleaned" || die "case 5: old tracking_id's journal lost its cleaned event: $OLD_JOURNAL"
NEW_JOURNAL="$(psql_scalar "SELECT string_agg(s.state, ',' ORDER BY s.state_at) FROM flashback.operation_current_state s JOIN flashback.operations o ON o.operation_id = s.operation_id WHERE o.tracking_id = $NEW_TRACKING_ID AND o.command = 'protect';")"
echo "$NEW_JOURNAL" | grep -q "activated" || die "case 5: new tracking_id's journal did not show activated: $NEW_JOURNAL"
echo "$NEW_JOURNAL" | grep -q "abandoned" && die "case 5: new tracking_id's journal incorrectly carries the old lifecycle's abandoned event"
log "case 5: PASS (cleanup, reprotect, distinguishable history, no leaks, never reported protected/healthy)"

log "ALL CASES PASSED"
