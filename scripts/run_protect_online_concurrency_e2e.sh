#!/usr/bin/env bash
# Step 9 Phase 3: real concurrency E2E for the online external_zstd protect
# lifecycle, driven through the installed CLI. Same throwaway-instance
# recipe as run_protect_online_cli_e2e.sh.
#
# Covers, for real (not simulated):
#   1. Two concurrent `pg_flashback protect` invocations on the SAME table
#      -- must serialize (advisory namespace 358943, lifecycle_bootstrap_
#      core.sql / protect_online.sql), never both reserve a lifecycle for
#      the same name/oid; exactly one must reach "Protection active.", the
#      other must observe it as already-protected (or race harmlessly onto
#      the same operation via resume-by-identity) -- never two distinct
#      tracked_tables rows for the same table.
#   2. `pg_flashback protect` versus `pg_flashback unprotect` racing on the
#      same table while the protect is still mid-flight (protection_state=
#      'starting') -- unprotect must fail closed (this file's fix to
#      flashback_unprotect's 'starting' guard, unprotect_cleanup.sql), never
#      let the two commands interleave into an inconsistent protection_state.
#
# This is a partial concurrency matrix, not the full 9-scenario matrix --
# see the final report for what remains.
#
# Usage:
#   ./scripts/run_protect_online_concurrency_e2e.sh
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PG_CONFIG="${PG_CONFIG:-$HOME/.pgrx/17.10/pgrx-install/bin/pg_config}"
PG_BIN="$("$PG_CONFIG" --bindir)"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
WORK="${PGFB_CONC_E2E_WORK:-$ROOT/target/protect-online-concurrency-e2e/$RUN_ID}"
mkdir -p "$WORK"
ARTIFACT_ROOT="$WORK/external_snapshots"
mkdir -p "$ARTIFACT_ROOT"
chmod 0700 "$ARTIFACT_ROOT"

log() { printf '[protect-online-concurrency-e2e] %s %s\n' "$(date +%H:%M:%S)" "$*"; }
FAILED=0
die() { FAILED=1; log "FAIL: $*"; exit 1; }

DATA="$WORK/data"
SOCKET="/tmp/pgfb-conc-e2e-$RUN_ID"
mkdir -p "$SOCKET"

cleanup() {
    "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1 || true
    if [[ "$FAILED" == "0" && "${PGFB_CONC_E2E_KEEP:-0}" != "1" ]]; then
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
psql_q -c "GRANT flashback_admin TO CURRENT_USER;" || die "could not grant flashback_admin"

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
# Scenario 1: two concurrent `protect` on the same table.
# ==================================================================
log "scenario 1: two concurrent protect invocations on the same table"
psql_q -c "CREATE TABLE public.conc_same_table (id int PRIMARY KEY);"
psql_q -c "INSERT INTO public.conc_same_table VALUES (1);"

OUT_A="$WORK/conc1_a.log"
OUT_B="$WORK/conc1_b.log"
"${CLI[@]}" protect public.conc_same_table --timeout 60 > "$OUT_A" 2>&1 &
PID_A=$!
"${CLI[@]}" protect public.conc_same_table --timeout 60 > "$OUT_B" 2>&1 &
PID_B=$!

RC_A=0; RC_B=0
wait "$PID_A" || RC_A=$?
wait "$PID_B" || RC_B=$?

log "concurrent protect A: exit=$RC_A"; sed 's/^/  A: /' "$OUT_A"
log "concurrent protect B: exit=$RC_B"; sed 's/^/  B: /' "$OUT_B"

# At least one must succeed with "Protection active."; the loser must fail
# closed (non-zero) or observe already-protected -- never silently produce
# a second, independent lifecycle for the same table.
ACTIVE_COUNT_A=0; ACTIVE_COUNT_B=0
grep -q "Protection active\.\|Already protected" "$OUT_A" && ACTIVE_COUNT_A=1
grep -q "Protection active\.\|Already protected" "$OUT_B" && ACTIVE_COUNT_B=1
if [[ "$ACTIVE_COUNT_A" == "0" && "$ACTIVE_COUNT_B" == "0" ]]; then
    die "neither concurrent protect invocation reached Protection active/already protected -- output above"
fi

TRACKING_COUNT="$(psql_scalar "SELECT count(*) FROM flashback.tracked_tables WHERE table_name = 'conc_same_table' AND is_active;")"
[[ "$TRACKING_COUNT" == "1" ]] || die "expected exactly one active tracked_tables row for conc_same_table, found $TRACKING_COUNT"

PROTECTED="$(psql_scalar "SELECT flashback_is_actively_protected('public.conc_same_table');")"
[[ "$PROTECTED" == "t" ]] || die "conc_same_table must end up actively protected after the race resolves"
log "scenario 1: PASS (exactly one active tracked lifecycle, table is protected)"

# ==================================================================
# Scenario 2: protect (mid-flight, protection_state=starting) versus
# unprotect racing on the same table -- unprotect must fail closed
# (this file's fix to flashback_unprotect's 'starting' guard).
# ==================================================================
log "scenario 2: protect vs unprotect race (protect wins the reservation first)"
psql_q -c "CREATE TABLE public.conc_protect_unprotect (id int PRIMARY KEY);"
psql_q -c "INSERT INTO public.conc_protect_unprotect VALUES (1);"
OP_ID="$(psql_scalar "SELECT (flashback_protect_begin('public.conc_protect_unprotect'))->>'operation_id';")"
[[ -n "$OP_ID" ]] || die "flashback_protect_begin did not return operation_id"

UNPROTECT_OUT=""
UNPROTECT_RC=0
UNPROTECT_OUT="$(psql_q -tAc "SELECT flashback_unprotect('public.conc_protect_unprotect');" 2>&1)" || UNPROTECT_RC=$?
log "unprotect while protection_state=starting: rc=$UNPROTECT_RC out=$UNPROTECT_OUT"
if [[ "$UNPROTECT_RC" == "0" ]]; then
    die "flashback_unprotect must fail closed while protection_state=starting, but it succeeded: $UNPROTECT_OUT"
fi
printf '%s' "$UNPROTECT_OUT" | grep -qi "still being protected\|protection_state=starting" \
    || die "flashback_unprotect's failure must be the actionable starting-guard message, got: $UNPROTECT_OUT"

# The protect attempt must still be resumable/completable after the failed
# unprotect race.
OUT_C="$WORK/conc2_resume.log"
"${CLI[@]}" protect public.conc_protect_unprotect --timeout 60 > "$OUT_C" 2>&1 || die "resume after protect-vs-unprotect race failed: $(cat "$OUT_C")"
grep -q "Protection active\." "$OUT_C" || die "resume after protect-vs-unprotect race did not reach Protection active.: $(cat "$OUT_C")"
log "scenario 2: PASS (unprotect correctly refused mid-protect; protect completed cleanly afterward)"

log "ALL SCENARIOS PASSED"
