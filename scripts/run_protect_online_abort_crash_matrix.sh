#!/usr/bin/env bash
# Step 9 Phase 3 (corrective): real crash injection at all eight documented
# flashback_protect_abort convergence boundaries:
#   1. after snapshot abort
#   2. after generation abort
#   3. before artifact purge
#   4. after artifact purge/receipt
#   5. before replica-identity restoration
#   6. after replica-identity restoration
#   7. after lifecycle deactivation
#   8. after terminal journal append but before commit
#
# Each case: start a fresh instance built WITH the pg_test feature (so the
# cfg(pg_test)-gated flashback_internal_test_trigger_failpoint exists --
# confirmed absent from the production build by
# scripts/check_generated_sql_no_test_surface.sh, which never builds with
# pg_test), reserve a real protect lifecycle with REPLICA IDENTITY FULL
# actually applied, configure the named failpoint via postgresql.conf
# (same mechanism scripts/run_external_zstd_artifact_e2e.sh already uses),
# call flashback_protect_abort -- which proc_exit(86)s at that exact PL/
# pgSQL statement boundary -- wait through PostgreSQL's own crash-restart
# cycle (same wait_postgres_ready idiom), disable the failpoint via
# ALTER SYSTEM + pg_reload_conf(), and retry flashback_protect_abort.
#
# Every case must converge to: exactly one terminal journal event (never
# duplicated by the retry), protection_state=abandoned, is_active=false,
# generation state=aborted, snapshot state=aborted, replica identity
# restored to the original value, exactly one cleanup receipt row, and a
# third protect-abort call remains a clean idempotent no-op.
#
# Usage:
#   ./scripts/run_protect_online_abort_crash_matrix.sh
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PG_CONFIG="${PG_CONFIG:-$HOME/.pgrx/17.10/pgrx-install/bin/pg_config}"
PG_BIN="$("$PG_CONFIG" --bindir)"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
BASE_WORK="${PGFB_ABORT_MATRIX_WORK:-$ROOT/target/protect-abort-crash-matrix/$RUN_ID}"
mkdir -p "$BASE_WORK"

log() { printf '[protect-abort-crash-matrix] %s %s\n' "$(date +%H:%M:%S)" "$*"; }
FAILED=0
die() { FAILED=1; log "FAIL: $*"; exit 1; }

cd "$ROOT"
log "installing a pg_test-feature build (needed only for the cfg-gated failpoint trigger; production builds never include it)"
cargo pgrx install --pg-config "$PG_CONFIG" --no-default-features --features "pg17 pg_test" \
    >"$BASE_WORK/install.log" 2>&1 \
    || die "pgrx install (pg_test feature) failed; see $BASE_WORK/install.log"

FAILPOINTS=(
    protect_abort_after_snapshot_abort
    protect_abort_after_generation_abort
    protect_abort_before_artifact_purge
    protect_abort_after_artifact_purge
    protect_abort_before_identity_restore
    protect_abort_after_identity_restore
    protect_abort_after_lifecycle_deactivation
    protect_abort_after_journal_before_commit
)

run_one_case() {
    local failpoint=$1
    local WORK="$BASE_WORK/$failpoint"
    local DATA="$WORK/data"
    local SOCKET="/tmp/pgfb-abort-crash-$RUN_ID-$failpoint"
    local ARTIFACT_ROOT="$WORK/external_snapshots"
    mkdir -p "$WORK" "$SOCKET" "$ARTIFACT_ROOT"
    chmod 0700 "$ARTIFACT_ROOT"

    "$PG_BIN/initdb" -D "$DATA" --locale=C.UTF-8 -A trust >"$WORK/initdb.log" 2>&1 \
        || die "[$failpoint] initdb failed"

    source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/output_plugin_allowlist.sh"
    opal_configure_postgresql_conf "$PG_BIN" "$DATA"

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
pg_flashback.test_external_zstd_failpoint = '$failpoint'
EOF

    "$PG_BIN/pg_ctl" -D "$DATA" -l "$WORK/postgres.log" -w start \
        || die "[$failpoint] postgres failed to start"

    psql_q() { "$PG_BIN/psql" -h "$SOCKET" -d postgres -X -q -v ON_ERROR_STOP=1 "$@"; }
    psql_scalar() { psql_q -tAc "$1" 2>/dev/null; }

    psql_q -c "CREATE EXTENSION pg_flashback;" >/dev/null 2>&1 || die "[$failpoint] CREATE EXTENSION failed"

    local running=""
    for _ in $(seq 1 30); do
        running="$(psql_scalar "SELECT capture_running FROM flashback_worker_readiness();")"
        [[ "$running" == "t" ]] && break
        sleep 1
    done
    [[ "$running" == "t" ]] || die "[$failpoint] capture worker never became admitted/running"

    psql_q -c "CREATE TABLE public.abort_crash_tbl (id int PRIMARY KEY);" >/dev/null
    psql_q -c "INSERT INTO public.abort_crash_tbl VALUES (1);" >/dev/null
    local orig_relident
    orig_relident="$(psql_scalar "SELECT relreplident::text FROM pg_class WHERE oid = 'public.abort_crash_tbl'::regclass;")"

    local op_id
    op_id="$(psql_scalar "SELECT (flashback_protect_begin('public.abort_crash_tbl'))->>'operation_id';")"
    [[ -n "$op_id" ]] || die "[$failpoint] flashback_protect_begin did not return operation_id"
    psql_q -c "SELECT flashback_protect_prepare_replica_identity($op_id);" >/dev/null
    local relident_after_prepare
    relident_after_prepare="$(psql_scalar "SELECT relreplident::text FROM pg_class WHERE oid = 'public.abort_crash_tbl'::regclass;")"
    [[ "$relident_after_prepare" == "f" ]] || die "[$failpoint] prepare_replica_identity did not set FULL"

    # Fire the abort -- expected to proc_exit(86) at the configured
    # boundary. A clean (zero) exit here would mean the failpoint never
    # fired, which is itself a test failure (proves nothing).
    set +e
    "$PG_BIN/psql" -h "$SOCKET" -d postgres -X -tAc "SELECT flashback_protect_abort($op_id);" \
        > "$WORK/abort_attempt1.log" 2>&1
    local rc1=$?
    set -e
    log "[$failpoint] first abort attempt exit=$rc1 (expected nonzero: the backend crashed)"
    [[ "$rc1" -ne 0 ]] || die "[$failpoint] failpoint never fired -- first abort attempt exited 0 (nothing was tested)"

    # Wait through PostgreSQL's own crash-restart cycle (same idiom
    # scripts/run_external_zstd_artifact_e2e.sh uses): a backend exiting
    # abnormally makes the postmaster reset all shared memory and restart
    # every other backend, so requiring several consecutive successful
    # SELECT 1s (not just one) avoids declaring victory mid-cycle.
    local consecutive=0 ready=0
    for _ in $(seq 1 300); do
        if "$PG_BIN/psql" -h "$SOCKET" -d postgres -X -tAc "SELECT 1" >/dev/null 2>&1; then
            consecutive=$((consecutive + 1))
            if [[ "$consecutive" -ge 10 ]]; then ready=1; break; fi
        else
            consecutive=0
        fi
        sleep 0.1
    done
    [[ "$ready" == "1" ]] || die "[$failpoint] PostgreSQL did not become ready after the injected crash"

    # Disable the failpoint before retrying -- otherwise the retry would
    # crash at the exact same boundary again, forever, which would prove
    # nothing about convergence past that point.
    psql_q -c "ALTER SYSTEM SET pg_flashback.test_external_zstd_failpoint = '';" >/dev/null
    psql_q -c "SELECT pg_reload_conf();" >/dev/null
    sleep 0.2

    # Retry -- must converge cleanly this time.
    local abort2_json
    abort2_json="$(psql_q -tAc "SELECT flashback_protect_abort($op_id);")" \
        || die "[$failpoint] retry abort failed even with the failpoint disabled: see $WORK"
    printf '%s' "$abort2_json" | grep -Eq '"status": ?"(abandoned|failed)"' \
        || die "[$failpoint] retry abort did not report a terminal status: $abort2_json"

    # Exact invariants.
    local ps is_act gen_state snap_state relident receipt_count journal_count
    ps="$(psql_scalar "SELECT protection_state FROM flashback.tracked_tables WHERE tracking_id = (SELECT tracking_id FROM flashback.operation_current_state WHERE operation_id = $op_id);")"
    [[ "$ps" == "abandoned" ]] || die "[$failpoint] expected protection_state=abandoned after convergence, got: $ps"
    is_act="$(psql_scalar "SELECT is_active FROM flashback.tracked_tables WHERE tracking_id = (SELECT tracking_id FROM flashback.operation_current_state WHERE operation_id = $op_id);")"
    [[ "$is_act" == "f" ]] || die "[$failpoint] expected is_active=false after convergence, got: $is_act"
    gen_state="$(psql_scalar "SELECT cg.state FROM flashback.coverage_generations cg JOIN flashback.operations o ON o.generation_id = cg.generation_id WHERE o.operation_id = $op_id;")"
    [[ "$gen_state" == "aborted" ]] || die "[$failpoint] expected generation state=aborted, got: $gen_state"
    snap_state="$(psql_scalar "SELECT s.payload_state FROM flashback.snapshots s JOIN flashback.operations o ON (o.details->>'snapshot_id')::bigint = s.snapshot_id WHERE o.operation_id = $op_id;")"
    [[ "$snap_state" == "aborted" ]] || die "[$failpoint] expected snapshot payload_state=aborted, got: $snap_state"
    relident="$(psql_scalar "SELECT relreplident::text FROM pg_class WHERE oid = 'public.abort_crash_tbl'::regclass;")"
    [[ "$relident" == "$orig_relident" ]] || die "[$failpoint] replica identity not restored (orig=$orig_relident now=$relident)"
    receipt_count="$(psql_scalar "SELECT count(*) FROM flashback.external_artifact_cleanup_receipts WHERE snapshot_id = (SELECT (o.details->>'snapshot_id')::bigint FROM flashback.operations o WHERE o.operation_id = $op_id);")"
    [[ "$receipt_count" == "1" ]] || die "[$failpoint] expected exactly one cleanup receipt, got: $receipt_count"
    journal_count="$(psql_scalar "SELECT count(*) FROM flashback.operation_events WHERE operation_id = $op_id AND event_type IN ('abandoned','failed');")"
    [[ "$journal_count" == "1" ]] || die "[$failpoint] expected exactly one terminal journal event, got: $journal_count"

    # Third call: fully idempotent no-op, no error, no new journal event.
    local abort3_json
    abort3_json="$(psql_q -tAc "SELECT flashback_protect_abort($op_id);")" \
        || die "[$failpoint] third (idempotent) abort call failed"
    printf '%s' "$abort3_json" | grep -q "already converged" \
        || die "[$failpoint] third abort call did not report idempotent no-op: $abort3_json"
    journal_count="$(psql_scalar "SELECT count(*) FROM flashback.operation_events WHERE operation_id = $op_id AND event_type IN ('abandoned','failed');")"
    [[ "$journal_count" == "1" ]] || die "[$failpoint] third abort call duplicated the terminal journal event"

    # Table name is free again for a fresh protect attempt.
    local retry_op_id
    retry_op_id="$(psql_scalar "SELECT (flashback_protect_begin('public.abort_crash_tbl'))->>'operation_id';")"
    [[ -n "$retry_op_id" && "$retry_op_id" != "null" ]] \
        || die "[$failpoint] table name was not free for a fresh protect attempt after convergence"

    "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1 || true
    rm -rf "$SOCKET"
    log "[$failpoint] PASS"
}

for fp in "${FAILPOINTS[@]}"; do
    log "case: $fp"
    run_one_case "$fp"
done

# ==================================================================
# Extra case: a third party (an operator, or any code path outside
# flashback_protect_abort's own control) changes the table's replica
# identity AFTER prepare_replica_identity set it to FULL but BEFORE abort
# runs. This is a different abort-step-7 branch than every case above (all
# of which restore cleanly from an untouched FULL): the third-party value
# must never be overwritten, and the skip reason must be recorded. Proven
# under a crash at the latest, closest-to-done boundary (after journal
# append, before commit) to confirm this skip decision is itself durable
# and does not flip on retry.
#
# (An earlier version of this case tried to simulate "relation replaced"
# via DROP+CREATE under flashback_set_restore_in_progress, but that hits a
# different, deeper guard -- flashback_internal_prepare_destructive_ddl,
# which requires a live WAL-epoch barrier round-trip that
# restore_in_progress alone does not satisfy. Relation-identity-changed
# fail-closed classification is already proven directly by tests/sql/
# integration/protect_reconcile_authority.sql's second fixture, which
# constructs that exact state without going through real DDL at all;
# real DROP/rename-vs-protect races are covered by the concurrency matrix
# instead, where the sequencing is the opposite -- and legitimate -- order
# and the guard is expected and correct to fire.)
# ==================================================================
run_third_party_identity_case() {
    local failpoint="protect_abort_after_journal_before_commit"
    local WORK="$BASE_WORK/third_party_identity_$failpoint"
    local DATA="$WORK/data"
    local SOCKET="/tmp/pgfb-abort-crash-$RUN_ID-hardfail"
    local ARTIFACT_ROOT="$WORK/external_snapshots"
    mkdir -p "$WORK" "$SOCKET" "$ARTIFACT_ROOT"
    chmod 0700 "$ARTIFACT_ROOT"

    "$PG_BIN/initdb" -D "$DATA" --locale=C.UTF-8 -A trust >"$WORK/initdb.log" 2>&1 \
        || die "[third_party_identity] initdb failed"
    source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/output_plugin_allowlist.sh"
    opal_configure_postgresql_conf "$PG_BIN" "$DATA"
    cat >> "$DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_flashback'
wal_level = logical
max_replication_slots = 10
max_worker_processes = 16
listen_addresses = ''
unix_socket_directories = '$SOCKET'
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
pg_flashback.test_external_zstd_failpoint = '$failpoint'
EOF
    "$PG_BIN/pg_ctl" -D "$DATA" -l "$WORK/postgres.log" -w start || die "[third_party_identity] postgres failed to start"
    psql_q() { "$PG_BIN/psql" -h "$SOCKET" -d postgres -X -q -v ON_ERROR_STOP=1 "$@"; }
    psql_scalar() { psql_q -tAc "$1" 2>/dev/null; }
    psql_q -c "CREATE EXTENSION pg_flashback;" >/dev/null 2>&1 || die "[third_party_identity] CREATE EXTENSION failed"
    local running=""
    for _ in $(seq 1 30); do
        running="$(psql_scalar "SELECT capture_running FROM flashback_worker_readiness();")"
        [[ "$running" == "t" ]] && break
        sleep 1
    done
    [[ "$running" == "t" ]] || die "[third_party_identity] capture worker never became admitted/running"

    psql_q -c "CREATE TABLE public.abort_hardfail_tbl (id int PRIMARY KEY);" >/dev/null
    local op_id
    op_id="$(psql_scalar "SELECT (flashback_protect_begin('public.abort_hardfail_tbl'))->>'operation_id';")"
    [[ -n "$op_id" ]] || die "[third_party_identity] flashback_protect_begin did not return operation_id"
    psql_q -c "SELECT flashback_protect_prepare_replica_identity($op_id);" >/dev/null
    local relident_after_prepare
    relident_after_prepare="$(psql_scalar "SELECT relreplident::text FROM pg_class WHERE oid = 'public.abort_hardfail_tbl'::regclass;")"
    [[ "$relident_after_prepare" == "f" ]] || die "[third_party_identity] prepare_replica_identity did not set FULL"

    # Simulate a third party changing replica identity after prepare but
    # before abort. Wrapped in restore_in_progress purely so this test's
    # own ALTER is not itself refused by the capture-configuration guard --
    # what abort must detect is the live catalog value not matching what
    # it expects, regardless of which code path produced it.
    # is_restore_in_progress() resets on transaction end (confirmed by a
    # real "table DDL refused" error here during authoring): a plain
    # multi-statement psql batch runs each statement as its own implicit
    # autocommit transaction, so the flag set by statement 1 was already
    # cleared before statement 2 (the ALTER) ran. All three must share one
    # explicit transaction, exactly like flashback_protect_prepare_
    # replica_identity's own single PL/pgSQL call does.
    psql_q <<'SQL' >/dev/null
BEGIN;
SELECT flashback_set_restore_in_progress(true);
ALTER TABLE public.abort_hardfail_tbl REPLICA IDENTITY NOTHING;
SELECT flashback_set_restore_in_progress(false);
COMMIT;
SQL
    local new_relident
    new_relident="$(psql_scalar "SELECT relreplident::text FROM pg_class WHERE oid = 'public.abort_hardfail_tbl'::regclass;")"
    [[ "$new_relident" == "n" ]] || die "[third_party_identity] fixture setup failed: expected the simulated third-party change to set NOTHING ('n'), got: $new_relident"

    set +e
    "$PG_BIN/psql" -h "$SOCKET" -d postgres -X -tAc "SELECT flashback_protect_abort($op_id);" \
        > "$WORK/abort_attempt1.log" 2>&1
    local rc1=$?
    set -e
    [[ "$rc1" -ne 0 ]] || die "[third_party_identity] failpoint never fired"

    local consecutive=0 ready=0
    for _ in $(seq 1 300); do
        if "$PG_BIN/psql" -h "$SOCKET" -d postgres -X -tAc "SELECT 1" >/dev/null 2>&1; then
            consecutive=$((consecutive + 1))
            if [[ "$consecutive" -ge 10 ]]; then ready=1; break; fi
        else
            consecutive=0
        fi
        sleep 0.1
    done
    [[ "$ready" == "1" ]] || die "[third_party_identity] PostgreSQL did not become ready after the injected crash"

    psql_q -c "ALTER SYSTEM SET pg_flashback.test_external_zstd_failpoint = '';" >/dev/null
    psql_q -c "SELECT pg_reload_conf();" >/dev/null
    sleep 0.2

    local abort2_json
    abort2_json="$(psql_q -tAc "SELECT flashback_protect_abort($op_id);")" \
        || die "[third_party_identity] retry abort failed"
    # A third-party replica-identity change is not itself a relation-
    # identity hard failure (the relation is exactly the one prepared);
    # this converges as an ordinary operator abandonment.
    [[ "$(printf '%s' "$abort2_json" | jq -r '.status')" == "abandoned" ]] \
        || die "[third_party_identity] expected status=abandoned, got: $abort2_json"
    [[ "$(printf '%s' "$abort2_json" | jq -r '.identity_restore_skipped_reason')" == "replica_identity_changed_by_third_party" ]] \
        || die "[third_party_identity] expected identity_restore_skipped_reason=replica_identity_changed_by_third_party, got: $abort2_json"

    # The third party's value must be untouched -- never silently
    # overwritten back to FULL or to the original value, even after a
    # crash-and-retry.
    local relident_after
    relident_after="$(psql_scalar "SELECT relreplident::text FROM pg_class WHERE oid = 'public.abort_hardfail_tbl'::regclass;")"
    [[ "$relident_after" == "$new_relident" ]] \
        || die "[third_party_identity] the third party's replica identity value was overwritten (was=$new_relident now=$relident_after)"

    local journal_count
    journal_count="$(psql_scalar "SELECT count(*) FROM flashback.operation_events WHERE operation_id = $op_id AND event_type IN ('abandoned','failed');")"
    [[ "$journal_count" == "1" ]] || die "[third_party_identity] expected exactly one terminal journal event, got: $journal_count"

    "$PG_BIN/pg_ctl" -D "$DATA" stop -m immediate -w >/dev/null 2>&1 || true
    rm -rf "$SOCKET"
    log "[third_party_identity/$failpoint] PASS (abandoned status, third-party identity value preserved, crash-convergent)"
}

log "case: third-party replica identity change x protect_abort_after_journal_before_commit"
run_third_party_identity_case

if [[ "$FAILED" == "0" && "${PGFB_ABORT_MATRIX_KEEP:-0}" != "1" ]]; then
    rm -rf "$BASE_WORK"
else
    log "evidence retained at $BASE_WORK"
fi

log "ALL 8 ABORT CRASH BOUNDARIES CONVERGED"
