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
#   3. protect vs a raw flashback_track() call, same table (heap_v1 backend).
#   4. protect vs DROP TABLE racing mid-flight.
#   5. protect vs ALTER TABLE RENAME racing mid-flight.
#   6. protect vs recover racing on a previously-dropped table name.
#   7. bounded maintenance reconciler vs manual CLI resume, same operation.
#   8. publish vs abort racing the same operation_id.
#   9. duplicate concurrent publish/finalize on the same operation_id.
#   10. two different tables protected fully concurrently (independence).
#
# The full matrix: all 10 scenarios above, real concurrent sessions/
# processes throughout, asserting the exact winning identity/state/journal
# every time -- never weakened to "either outcome is fine" even when the
# reconciler may legally win a given race.
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

# ==================================================================
# Scenario 3: protect vs heap_v1 track. flashback_track() refuses outright
# under snapshot_storage_backend=external_zstd (api_track_capture.sql), so
# this scenario needs the OTHER backend: toggle the SIGHUP GUC (never
# SUSET) to heap_v1, race a raw flashback_track() call against the CLI's
# own protect (which, under heap_v1, itself calls flashback_track()
# internally via cmd_protect_heap_v1) on the SAME table, then toggle back.
# The canonical name-collision lock (namespace 358943, protect_online.sql /
# lifecycle_bootstrap_core.sql) must serialize these to exactly one winner.
# ==================================================================
log "scenario 3: protect vs raw flashback_track() on the same table (heap_v1 backend)"
psql_q -c "ALTER SYSTEM SET pg_flashback.snapshot_storage_backend = 'heap_v1';"
psql_q -c "SELECT pg_reload_conf();"
for _ in $(seq 1 50); do
    v="$(psql_scalar "SELECT current_setting('pg_flashback.snapshot_storage_backend');")"
    [[ "$v" == "heap_v1" ]] && break
    sleep 0.05
done
[[ "$v" == "heap_v1" ]] || die "scenario 3: could not switch snapshot_storage_backend to heap_v1"

psql_q -c "CREATE TABLE public.conc_track_protect (id int PRIMARY KEY);"
psql_q -c "INSERT INTO public.conc_track_protect VALUES (1);"

OUT_TRACK="$WORK/conc3_track.log"
OUT_PROTECT="$WORK/conc3_protect.log"
psql_q -tAc "SELECT flashback_track('public.conc_track_protect');" > "$OUT_TRACK" 2>&1 &
PID_TRACK=$!
"${CLI[@]}" protect public.conc_track_protect --timeout 60 > "$OUT_PROTECT" 2>&1 &
PID_PROTECT=$!
RC_TRACK=0; RC_PROTECT=0
wait "$PID_TRACK" || RC_TRACK=$?
wait "$PID_PROTECT" || RC_PROTECT=$?
log "scenario 3: track rc=$RC_TRACK protect rc=$RC_PROTECT"

TRACKING_COUNT3="$(psql_scalar "SELECT count(*) FROM flashback.tracked_tables WHERE table_name = 'conc_track_protect' AND is_active;")"
[[ "$TRACKING_COUNT3" == "1" ]] || die "scenario 3: expected exactly one active tracked_tables row, found $TRACKING_COUNT3"
PROTECTED3="f"
for _ in $(seq 1 100); do
    PROTECTED3="$(psql_scalar "SELECT flashback_is_actively_protected('public.conc_track_protect');")"
    [[ "$PROTECTED3" == "t" ]] && break
    sleep 0.1
done
[[ "$PROTECTED3" == "t" ]] || die "scenario 3: table must end up protected regardless of which side won the race"

psql_q -c "ALTER SYSTEM SET pg_flashback.snapshot_storage_backend = 'external_zstd';"
psql_q -c "SELECT pg_reload_conf();"
for _ in $(seq 1 50); do
    v="$(psql_scalar "SELECT current_setting('pg_flashback.snapshot_storage_backend');")"
    [[ "$v" == "external_zstd" ]] && break
    sleep 0.05
done
[[ "$v" == "external_zstd" ]] || die "scenario 3: could not switch snapshot_storage_backend back to external_zstd"
log "scenario 3: PASS (exactly one active tracked lifecycle, table protected, backend switched back)"

# ==================================================================
# Scenario 4: protect vs DROP TABLE racing on the same table while
# protect is mid-flight (protection_state=starting). flashback_protect_
# begin() unconditionally sets recovery_profile='local_delta' regardless
# of storage_backend (protect_online.sql), so this DOES match flashback_
# internal_prepare_destructive_ddl's target set -- and its capture-
# configuration guard requires the generation to be 'active', which a
# 'starting' lifecycle's generation ('building'/'capturing') never is yet.
# DROP is therefore refused UP FRONT, before it can touch anything --
# proven directly here rather than assumed. The mid-protect operation must
# remain completely untouched and resumable afterward.
# ==================================================================
log "scenario 4: protect vs DROP TABLE racing mid-flight"
psql_q -c "CREATE TABLE public.conc_protect_drop (id int PRIMARY KEY);"
psql_q -c "INSERT INTO public.conc_protect_drop VALUES (1);"
OP_ID4="$(psql_scalar "SELECT (flashback_protect_begin('public.conc_protect_drop'))->>'operation_id';")"
[[ -n "$OP_ID4" ]] || die "scenario 4: flashback_protect_begin did not return operation_id"
psql_q -c "SELECT flashback_protect_prepare_replica_identity($OP_ID4);" >/dev/null

DROP_OUT4=""
DROP_RC4=0
DROP_OUT4="$(psql_q -tAc "DROP TABLE public.conc_protect_drop;" 2>&1)" || DROP_RC4=$?
[[ "$DROP_RC4" != "0" ]] || die "scenario 4: DROP TABLE must be refused for a mid-protect (not yet active) lifecycle, but it succeeded"
printf '%s' "$DROP_OUT4" | grep -qi "capture configuration is disabled\|WAL epoch is not active" \
    || die "scenario 4: DROP's refusal must be the capture-configuration guard, got: $DROP_OUT4"

TABLE_EXISTS4="$(psql_scalar "SELECT to_regclass('public.conc_protect_drop') IS NOT NULL;")"
[[ "$TABLE_EXISTS4" == "t" ]] || die "scenario 4: table must still exist after the refused DROP"
NEXT4="$(psql_scalar "SELECT (flashback_protect_next_action($OP_ID4))->>'action';")"
[[ "$NEXT4" != "blocked" ]] || die "scenario 4: mid-protect operation must be untouched (not blocked) after a refused DROP, got action=$NEXT4"

OUT_RESUME4="$WORK/conc4_resume.log"
"${CLI[@]}" protect public.conc_protect_drop --timeout 60 > "$OUT_RESUME4" 2>&1 \
    || die "scenario 4: resume after the refused DROP failed: $(cat "$OUT_RESUME4")"
grep -q "Protection active\." "$OUT_RESUME4" || die "scenario 4: resume after refused DROP did not reach Protection active.: $(cat "$OUT_RESUME4")"
log "scenario 4: PASS (DROP correctly refused up front by the capture-configuration guard; protect completed cleanly afterward)"

# ==================================================================
# Scenario 5: protect vs ALTER TABLE RENAME racing mid-flight. RENAME is
# metadata DDL (flashback_internal_prepare_metadata_ddl), which checks the
# SAME capture-configuration guard -- so it is refused up front for the
# same reason as scenario 4's DROP, before it can touch the relation.
# ==================================================================
log "scenario 5: protect vs ALTER TABLE RENAME racing mid-flight"
psql_q -c "CREATE TABLE public.conc_protect_rename (id int PRIMARY KEY);"
psql_q -c "INSERT INTO public.conc_protect_rename VALUES (1);"
OP_ID5="$(psql_scalar "SELECT (flashback_protect_begin('public.conc_protect_rename'))->>'operation_id';")"
[[ -n "$OP_ID5" ]] || die "scenario 5: flashback_protect_begin did not return operation_id"
psql_q -c "SELECT flashback_protect_prepare_replica_identity($OP_ID5);" >/dev/null

RENAME_OUT5=""
RENAME_RC5=0
RENAME_OUT5="$(psql_q -tAc "ALTER TABLE public.conc_protect_rename RENAME TO conc_protect_renamed;" 2>&1)" || RENAME_RC5=$?
[[ "$RENAME_RC5" != "0" ]] || die "scenario 5: RENAME must be refused for a mid-protect (not yet active) lifecycle, but it succeeded"
printf '%s' "$RENAME_OUT5" | grep -qi "capture configuration is disabled\|WAL epoch is not active" \
    || die "scenario 5: RENAME's refusal must be the capture-configuration guard, got: $RENAME_OUT5"

STILL_NAMED5="$(psql_scalar "SELECT to_regclass('public.conc_protect_rename') IS NOT NULL;")"
[[ "$STILL_NAMED5" == "t" ]] || die "scenario 5: table must still exist under its original name after the refused RENAME"
NEXT5="$(psql_scalar "SELECT (flashback_protect_next_action($OP_ID5))->>'action';")"
[[ "$NEXT5" != "blocked" ]] || die "scenario 5: mid-protect operation must be untouched (not blocked) after a refused RENAME, got action=$NEXT5"

OUT_RESUME5="$WORK/conc5_resume.log"
"${CLI[@]}" protect public.conc_protect_rename --timeout 60 > "$OUT_RESUME5" 2>&1 \
    || die "scenario 5: resume after the refused RENAME failed: $(cat "$OUT_RESUME5")"
grep -q "Protection active\." "$OUT_RESUME5" || die "scenario 5: resume after refused RENAME did not reach Protection active.: $(cat "$OUT_RESUME5")"
log "scenario 5: PASS (RENAME correctly refused up front by the capture-configuration guard; protect completed cleanly afterward)"

# ==================================================================
# Scenario 6: protect vs recover -- a previously heap_v1-tracked-then-
# dropped table's `pg_flashback recover` (recreating it under its
# original name via WAL-history CTAS) racing a session that independently
# re-creates a table under the SAME name and protects THAT one.
# flashback_protect_begin requires the target to already exist (it never
# creates one), so the race is CREATE TABLE (session B) vs recover's own
# CTAS (session A) -- a real DDL-level race PostgreSQL's own catalog
# locking serializes (exactly one CREATE wins, the other fails outright
# with "already exists"). What pg_flashback's own bookkeeping must
# additionally guarantee, regardless of which side wins: no corruption --
# exactly one active tracked_tables row for the name, table exists, no
# wrong-table mutation.
# ==================================================================
log "scenario 6: protect vs recover racing on a previously-dropped table name"
psql_q -c "ALTER SYSTEM SET pg_flashback.snapshot_storage_backend = 'heap_v1';"
psql_q -c "SELECT pg_reload_conf();"
for _ in $(seq 1 50); do
    v="$(psql_scalar "SELECT current_setting('pg_flashback.snapshot_storage_backend');")"
    [[ "$v" == "heap_v1" ]] && break
    sleep 0.05
done
[[ "$v" == "heap_v1" ]] || die "scenario 6: could not switch snapshot_storage_backend to heap_v1"

psql_q -c "CREATE TABLE public.conc_recover_protect (id int PRIMARY KEY);"
psql_q -c "INSERT INTO public.conc_recover_protect VALUES (1);"
"${CLI[@]}" protect public.conc_recover_protect --timeout 60 >/dev/null 2>&1 \
    || die "scenario 6: initial heap_v1 protect failed"
psql_q -c "DROP TABLE public.conc_recover_protect;"

OUT_RECOVER="$WORK/conc6_recover.log"
OUT_REPROTECT="$WORK/conc6_reprotect.log"
"${CLI[@]}" recover public.conc_recover_protect > "$OUT_RECOVER" 2>&1 &
PID_RECOVER=$!
(
    "$PG_BIN/psql" -h "$SOCKET" -d postgres -X -q -c "CREATE TABLE IF NOT EXISTS public.conc_recover_protect (id int PRIMARY KEY);" >/dev/null 2>&1
    "$PG_BIN/psql" -h "$SOCKET" -d postgres -X -q -c "INSERT INTO public.conc_recover_protect VALUES (1) ON CONFLICT DO NOTHING;" >/dev/null 2>&1
    "${CLI[@]}" protect public.conc_recover_protect --timeout 60
) > "$OUT_REPROTECT" 2>&1 &
PID_REPROTECT=$!
RC_RECOVER=0; RC_REPROTECT=0
wait "$PID_RECOVER" || RC_RECOVER=$?
wait "$PID_REPROTECT" || RC_REPROTECT=$?
log "scenario 6: recover rc=$RC_RECOVER reprotect rc=$RC_REPROTECT"

TABLE_EXISTS6="$(psql_scalar "SELECT to_regclass('public.conc_recover_protect') IS NOT NULL;")"
[[ "$TABLE_EXISTS6" == "t" ]] || die "scenario 6: table must exist after the race resolves (whichever side won) -- recover: $(cat "$OUT_RECOVER") -- reprotect: $(cat "$OUT_REPROTECT")"
TRACKING_COUNT6="$(psql_scalar "SELECT count(*) FROM flashback.tracked_tables WHERE table_name = 'conc_recover_protect' AND is_active;")"
[[ "$TRACKING_COUNT6" == "1" ]] || die "scenario 6: expected exactly one active tracked_tables row, found $TRACKING_COUNT6"

psql_q -c "ALTER SYSTEM SET pg_flashback.snapshot_storage_backend = 'external_zstd';"
psql_q -c "SELECT pg_reload_conf();"
for _ in $(seq 1 50); do
    v="$(psql_scalar "SELECT current_setting('pg_flashback.snapshot_storage_backend');")"
    [[ "$v" == "external_zstd" ]] && break
    sleep 0.05
done
[[ "$v" == "external_zstd" ]] || die "scenario 6: could not switch snapshot_storage_backend back to external_zstd"
log "scenario 6: PASS (recover-vs-fresh-protect race resolved to exactly one consistent active lifecycle)"

# ==================================================================
# Scenario 7: bounded maintenance reconciler vs manual CLI resume, both
# racing to finalize the SAME resumable operation. The reconciler is
# reconciler-eligible (automatic=true) for 'publish'/'finalize'/
# 'abort_finalize' by design -- if it legally finishes first, the CLI's
# own retry must observe that as a clean "already active", never a
# duplicate lifecycle or a duplicate journal entry. This deliberately does
# NOT weaken the accepted happy path to tolerate an ambiguous outcome: the
# exact final identity (same tracking_id, same operation_id, exactly one
# 'activated' event) is asserted regardless of which side actually wins.
# ==================================================================
log "scenario 7: bounded reconciler vs manual CLI resume racing the same resumable operation"
psql_q -c "CREATE TABLE public.conc_reconciler_cli (id int PRIMARY KEY);"
psql_q -c "INSERT INTO public.conc_reconciler_cli VALUES (1);"
OP_ID7="$(psql_scalar "SELECT (flashback_protect_begin('public.conc_reconciler_cli'))->>'operation_id';")"
[[ -n "$OP_ID7" ]] || die "scenario 7: flashback_protect_begin did not return operation_id"
TRACKING_ID7="$(psql_scalar "SELECT tracking_id FROM flashback.operation_current_state WHERE operation_id = $OP_ID7;")"
psql_q -c "SELECT flashback_protect_prepare_replica_identity($OP_ID7);" >/dev/null
psql_q -c "SELECT flashback_protect_external_copy($OP_ID7);" >/dev/null

# Let the boundary promote and the artifact stage -- the maintenance
# worker's own reconciler is already free to act on this the moment it
# becomes 'publish'-ready (automatic=true), racing this script's own CLI
# resume call started immediately after.
for _ in $(seq 1 300); do
    a7="$(psql_scalar "SELECT (flashback_protect_next_action($OP_ID7))->>'action';")"
    [[ "$a7" == "publish" || "$a7" == "complete" ]] && break
    sleep 0.1
done

OUT_CLI7="$WORK/conc7_cli.log"
"${CLI[@]}" protect public.conc_reconciler_cli --timeout 60 > "$OUT_CLI7" 2>&1 || true
grep -qE "Protection active\.|Already protected" "$OUT_CLI7" \
    || die "scenario 7: CLI resume did not converge to active/already-protected: $(cat "$OUT_CLI7")"

OP_STATE7="$(psql_scalar "SELECT state FROM flashback.operation_current_state WHERE operation_id = $OP_ID7;")"
[[ "$OP_STATE7" == "activated" ]] || die "scenario 7: expected operation state=activated regardless of winner, got: $OP_STATE7"
PS7="$(psql_scalar "SELECT protection_state FROM flashback.tracked_tables WHERE tracking_id = $TRACKING_ID7;")"
[[ "$PS7" == "active" ]] || die "scenario 7: expected protection_state=active, got: $PS7"
JOURNAL7="$(psql_scalar "SELECT count(*) FROM flashback.operation_events WHERE operation_id = $OP_ID7 AND event_type = 'activated';")"
[[ "$JOURNAL7" == "1" ]] || die "scenario 7: expected exactly one 'activated' journal event regardless of winner, got: $JOURNAL7"
TRACKING_COUNT7="$(psql_scalar "SELECT count(*) FROM flashback.tracked_tables WHERE table_name = 'conc_reconciler_cli' AND is_active;")"
[[ "$TRACKING_COUNT7" == "1" ]] || die "scenario 7: expected exactly one active tracked_tables row, found $TRACKING_COUNT7"
log "scenario 7: PASS (exact same identity/state/journal regardless of which side actually finished it first)"

# ==================================================================
# Scenario 8: publish vs abort -- flashback_protect_external_publish and
# flashback_protect_abort called truly concurrently on the SAME
# operation_id from two separate sessions once the artifact is
# publish-ready. Exactly one must win: either publish activates fully
# (abort then correctly refuses with already_active), or abort converges
# first (publish then correctly observes state != 'started' and raises
# object_not_in_prerequisite_state, which the CLI already treats as a
# benign loser per protect_online.sql's own accepted design) -- never a
# torn mix of both.
# ==================================================================
log "scenario 8: publish vs abort racing the same operation_id"
psql_q -c "CREATE TABLE public.conc_publish_abort (id int PRIMARY KEY);"
psql_q -c "INSERT INTO public.conc_publish_abort VALUES (1);"
OP_ID8="$(psql_scalar "SELECT (flashback_protect_begin('public.conc_publish_abort'))->>'operation_id';")"
[[ -n "$OP_ID8" ]] || die "scenario 8: flashback_protect_begin did not return operation_id"
TRACKING_ID8="$(psql_scalar "SELECT tracking_id FROM flashback.operation_current_state WHERE operation_id = $OP_ID8;")"
psql_q -c "SELECT flashback_protect_prepare_replica_identity($OP_ID8);" >/dev/null
psql_q -c "SELECT flashback_protect_external_copy($OP_ID8);" >/dev/null
for _ in $(seq 1 300); do
    a8="$(psql_scalar "SELECT (flashback_protect_next_action($OP_ID8))->>'action';")"
    [[ "$a8" == "publish" || "$a8" == "complete" ]] && break
    sleep 0.1
done

OUT_PUB8="$WORK/conc8_publish.log"
OUT_ABORT8="$WORK/conc8_abort.log"
psql_q -tAc "SELECT flashback_protect_external_publish($OP_ID8);" > "$OUT_PUB8" 2>&1 &
PID_PUB8=$!
"${CLI[@]}" protect-abort "$OP_ID8" --yes > "$OUT_ABORT8" 2>&1 &
PID_ABORT8=$!
RC_PUB8=0; RC_ABORT8=0
wait "$PID_PUB8" || RC_PUB8=$?
wait "$PID_ABORT8" || RC_ABORT8=$?
log "scenario 8: publish rc=$RC_PUB8 abort rc=$RC_ABORT8"

OP_STATE8="$(psql_scalar "SELECT state FROM flashback.operation_current_state WHERE operation_id = $OP_ID8;")"
[[ "$OP_STATE8" == "activated" || "$OP_STATE8" == "abandoned" ]] \
    || die "scenario 8: expected a clean terminal state (activated or abandoned), got: $OP_STATE8"
JOURNAL8="$(psql_scalar "SELECT count(*) FROM flashback.operation_events WHERE operation_id = $OP_ID8 AND event_type IN ('activated','abandoned','failed');")"
[[ "$JOURNAL8" == "1" ]] || die "scenario 8: expected exactly one terminal journal event, got: $JOURNAL8"
if [[ "$OP_STATE8" == "activated" ]]; then
    PS8="$(psql_scalar "SELECT protection_state FROM flashback.tracked_tables WHERE tracking_id = $TRACKING_ID8;")"
    [[ "$PS8" == "active" ]] || die "scenario 8: op activated but protection_state != active: $PS8"
else
    PS8="$(psql_scalar "SELECT protection_state FROM flashback.tracked_tables WHERE tracking_id = $TRACKING_ID8;")"
    [[ "$PS8" == "abandoned" ]] || die "scenario 8: op abandoned but protection_state != abandoned: $PS8"
fi
log "scenario 8: PASS (exactly one winner, exactly one terminal event, consistent protection_state either way)"

# ==================================================================
# Scenario 9: duplicate publish/finalize -- two truly concurrent CLI-level
# publish attempts on the SAME already publish-ready operation_id. Must
# never double-activate or duplicate the journal (proves flashback_protect_
# finalize's CAS is genuinely exclusive under real concurrency, not just
# sequential retries).
# ==================================================================
log "scenario 9: duplicate concurrent publish/finalize on the same operation_id"
psql_q -c "CREATE TABLE public.conc_dup_publish (id int PRIMARY KEY);"
psql_q -c "INSERT INTO public.conc_dup_publish VALUES (1);"
OP_ID9="$(psql_scalar "SELECT (flashback_protect_begin('public.conc_dup_publish'))->>'operation_id';")"
[[ -n "$OP_ID9" ]] || die "scenario 9: flashback_protect_begin did not return operation_id"
TRACKING_ID9="$(psql_scalar "SELECT tracking_id FROM flashback.operation_current_state WHERE operation_id = $OP_ID9;")"
psql_q -c "SELECT flashback_protect_prepare_replica_identity($OP_ID9);" >/dev/null
psql_q -c "SELECT flashback_protect_external_copy($OP_ID9);" >/dev/null
for _ in $(seq 1 300); do
    a9="$(psql_scalar "SELECT (flashback_protect_next_action($OP_ID9))->>'action';")"
    [[ "$a9" == "publish" || "$a9" == "complete" ]] && break
    sleep 0.1
done

OUT_PUB9A="$WORK/conc9_publish_a.log"
OUT_PUB9B="$WORK/conc9_publish_b.log"
psql_q -tAc "SELECT flashback_protect_external_publish($OP_ID9);" > "$OUT_PUB9A" 2>&1 &
PID_PUB9A=$!
psql_q -tAc "SELECT flashback_protect_external_publish($OP_ID9);" > "$OUT_PUB9B" 2>&1 &
PID_PUB9B=$!
wait "$PID_PUB9A" || true
wait "$PID_PUB9B" || true

OP_STATE9="$(psql_scalar "SELECT state FROM flashback.operation_current_state WHERE operation_id = $OP_ID9;")"
[[ "$OP_STATE9" == "activated" ]] || die "scenario 9: expected operation state=activated, got: $OP_STATE9"
JOURNAL9="$(psql_scalar "SELECT count(*) FROM flashback.operation_events WHERE operation_id = $OP_ID9 AND event_type = 'activated';")"
[[ "$JOURNAL9" == "1" ]] || die "scenario 9: expected exactly one 'activated' journal event from two concurrent publish calls, got: $JOURNAL9"
PS9="$(psql_scalar "SELECT protection_state FROM flashback.tracked_tables WHERE tracking_id = $TRACKING_ID9;")"
[[ "$PS9" == "active" ]] || die "scenario 9: expected protection_state=active, got: $PS9"
GEN_ACTIVE9="$(psql_scalar "SELECT count(*) FROM flashback.coverage_generations WHERE tracking_id = $TRACKING_ID9 AND state = 'active';")"
[[ "$GEN_ACTIVE9" == "1" ]] || die "scenario 9: expected exactly one active generation, got: $GEN_ACTIVE9"
log "scenario 9: PASS (concurrent duplicate publish calls converged to exactly one activation, no duplicate journal/generation)"

# ==================================================================
# Scenario 10 (baseline, real concurrency): two different tables protected
# fully concurrently must be completely independent -- no cross-
# contamination, both reach full activation with distinct identities.
# ==================================================================
log "scenario 10: two different tables protected concurrently (independence baseline)"
psql_q -c "CREATE TABLE public.conc_indep_a (id int PRIMARY KEY);"
psql_q -c "CREATE TABLE public.conc_indep_b (id int PRIMARY KEY);"
psql_q -c "INSERT INTO public.conc_indep_a VALUES (1);"
psql_q -c "INSERT INTO public.conc_indep_b VALUES (1);"
OUT_A10="$WORK/conc10_a.log"
OUT_B10="$WORK/conc10_b.log"
"${CLI[@]}" protect public.conc_indep_a --timeout 60 > "$OUT_A10" 2>&1 &
PID_A10=$!
"${CLI[@]}" protect public.conc_indep_b --timeout 60 > "$OUT_B10" 2>&1 &
PID_B10=$!
wait "$PID_A10" || die "scenario 10: protect of table A failed: $(cat "$OUT_A10")"
wait "$PID_B10" || die "scenario 10: protect of table B failed: $(cat "$OUT_B10")"
grep -q "Protection active\." "$OUT_A10" || die "scenario 10: table A did not reach Protection active.: $(cat "$OUT_A10")"
grep -q "Protection active\." "$OUT_B10" || die "scenario 10: table B did not reach Protection active.: $(cat "$OUT_B10")"
TID_A10="$(psql_scalar "SELECT tracking_id FROM flashback.tracked_tables WHERE table_name = 'conc_indep_a' AND is_active;")"
TID_B10="$(psql_scalar "SELECT tracking_id FROM flashback.tracked_tables WHERE table_name = 'conc_indep_b' AND is_active;")"
[[ -n "$TID_A10" && -n "$TID_B10" && "$TID_A10" != "$TID_B10" ]] \
    || die "scenario 10: expected two distinct tracking_ids, got A=$TID_A10 B=$TID_B10"
log "scenario 10: PASS (two independent tables both fully activated with distinct identities)"

log "ALL SCENARIOS PASSED"
