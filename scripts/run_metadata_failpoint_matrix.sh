#!/usr/bin/env bash
# Metadata/restore failpoint matrix.
#
# flashback_restore_lsn()/flashback_verify_restored_relation() have several
# named test-only barriers (pg_flashback.test_restore_failpoint, superuser
# SUSET GUC — see src/storage/worker.rs) at each metadata transition of a
# recover: before the shadow is materialized, after the pre-swap expected
# proof is built, after materialize but before the swap, and after the swap
# but before COMMIT. This matrix fires each one and asserts:
#   - flashback_recover_execute fails (non-zero) instead of silently
#     "succeeding" with corrupted metadata,
#   - the original table is never left half-swapped (DROP state is exact:
#     either still absent, or fully restored — nothing in between),
#   - flashback_recover_mark_failed makes the failure durable, and
#   - a retry with the failpoint cleared succeeds normally.
#
# A fifth case (after_swap_mutate_row) fires post-commit corruption and
# checks that the async independent-proof verification never marks that
# recover operation verified; this half is best-effort/soft since it depends
# on worker scheduling, not a hard pass/fail like the four RAISE barriers.
#
# Requires a live PG* cluster with pg_flashback installed, capture_mode=wal,
# and a role allowed ALTER SYSTEM (superuser) to flip the SUSET GUC across
# separate psql invocations.
set -Eeuo pipefail

PSQL_BIN="${PSQL_BIN:-psql}"
psqlq() { "$PSQL_BIN" -X -v ON_ERROR_STOP=1 -qAt "$@"; }
psqlq_admin() { "$PSQL_BIN" -X -v ON_ERROR_STOP=1 -qAt -d postgres "$@"; }

if [[ "${REQUIRE_LIVE:-0}" != "1" && -z "${PGDATABASE:-}" && -z "${PGHOST:-}" ]]; then
    echo "SKIP: set PG* / REQUIRE_LIVE=1 for live metadata-failpoint matrix" >&2
    exit 0
fi

FAILED=0
pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1"; FAILED=1; }

TABLES=(mfp_a mfp_b mfp_c mfp_d mfp_e)
set_failpoint() {
    psqlq_admin -c "ALTER SYSTEM SET pg_flashback.test_restore_failpoint = '$1';" >/dev/null
    psqlq_admin -c "SELECT pg_reload_conf();" >/dev/null
}
clear_failpoint() {
    psqlq_admin -c "ALTER SYSTEM RESET pg_flashback.test_restore_failpoint;" >/dev/null
    psqlq_admin -c "SELECT pg_reload_conf();" >/dev/null
}
cleanup() {
    clear_failpoint || true
    local t
    for t in "${TABLES[@]}"; do
        psqlq -c "SELECT flashback_unprotect('public.$t');" >/dev/null 2>&1 || true
        psqlq -c "DROP TABLE IF EXISTS public.$t CASCADE;" >/dev/null 2>&1 || true
    done
}
trap cleanup EXIT

wait_healthy() {
    local table=$1 h=""
    for _ in $(seq 1 240); do
        h=$(psqlq -c "SELECT flashback_lifecycle_health('public.$table');")
        [[ "$h" == "healthy" ]] && break
        sleep 0.25
    done
    [[ "$h" == "healthy" ]]
}
wait_drop_restorable() {
    local table=$1 st=""
    for _ in $(seq 1 240); do
        st=$(psqlq -c "SELECT status FROM flashback_disaster_points('public.$table', interval '1 day') WHERE event_type='DROP' ORDER BY disaster_commit_lsn DESC LIMIT 1;")
        [[ "$st" == "restorable" ]] && return 0
        sleep 0.25
    done
    return 1
}

# One (table, RAISE failpoint) case: execute must fail closed and never leave
# the table half-swapped; retry with the barrier cleared must then succeed.
run_raise_case() {
    local table=$1 fp=$2
    psqlq <<SQL >/dev/null
DROP TABLE IF EXISTS public.$table CASCADE;
CREATE TABLE public.$table(id int PRIMARY KEY, v text NOT NULL);
INSERT INTO public.$table VALUES (1,'a'),(2,'b');
SELECT flashback_track('public.$table');
SQL
    wait_healthy "$table" || { fail "$table: not healthy after protect"; return; }
    psqlq -c "DROP TABLE public.$table;"
    wait_drop_restorable "$table" || { fail "$table: DROP never restorable"; return; }

    local token op err rc
    token=$(psqlq -c "SELECT flashback_recover_plan('public.$table', interval '1 day')->>'plan_token';")
    [[ -n "$token" && "$token" != "null" ]] || { fail "$table: no plan_token"; return; }
    op=$(psqlq -c "SELECT flashback_recover_begin('public.$table', '$token', interval '1 day')->>'operation_id';")

    set_failpoint "$fp"
    set +e
    err=$(psqlq -c "SELECT flashback_recover_execute('public.$table', '$token', interval '1 day', NULL, NULL, NULL, $op);" 2>&1)
    rc=$?
    set -e
    clear_failpoint

    if [[ $rc -eq 0 ]]; then
        fail "$table/$fp: execute unexpectedly succeeded"
        return
    fi
    if [[ "$(psqlq -c "SELECT to_regclass('public.$table') IS NULL;")" != "t" ]]; then
        fail "$table/$fp: table is neither absent nor a clean commit (half-swapped?)"
        return
    fi
    psqlq -c "SELECT flashback_recover_mark_failed($op, 'P0001', '$fp', left(replace('$err', '''', ''''''), 200), '{}'::jsonb);" >/dev/null
    if [[ "$(psqlq -c "SELECT state FROM flashback.operation_current_state WHERE operation_id=$op;")" != "failed" ]]; then
        fail "$table/$fp: mark_failed did not make failure durable"
        return
    fi
    pass "$table/$fp fails closed, no half-swap, durable failed"

    # Retry without the failpoint must succeed with a fresh operation.
    local token2 op2 st2
    token2=$(psqlq -c "SELECT flashback_recover_plan('public.$table', interval '1 day')->>'plan_token';")
    op2=$(psqlq -c "SELECT flashback_recover_begin('public.$table', '$token2', interval '1 day')->>'operation_id';")
    psqlq -c "SELECT flashback_recover_execute('public.$table', '$token2', interval '1 day', NULL, NULL, NULL, $op2);" >/dev/null
    for _ in $(seq 1 240); do
        st2=$(psqlq -c "SELECT COALESCE(flashback_operation_state($op2),'missing');")
        [[ "$st2" == "verified" || "$st2" == "failed" ]] && break
        sleep 0.5
    done
    if [[ "$st2" == "verified" ]]; then
        pass "$table/$fp retry after clearing failpoint verified"
    else
        fail "$table/$fp retry did not verify (state=$st2)"
    fi
}

run_raise_case mfp_a before_materialize
run_raise_case mfp_b after_expected_proof_before_swap
run_raise_case mfp_c after_materialize_before_swap
run_raise_case mfp_d after_swap_before_commit

# Row-mutation corruption injected right after the swap: verification
# (flashback_verify_restored_relation / flashback_compare_restore_proofs) runs
# synchronously inside flashback_restore_lsn(), in the SAME transaction as the
# swap — so a caught mismatch aborts that transaction and rolls the swap back
# too (stronger than a merely-post-commit check: the corrupted state is never
# visible at all). That makes this case the same shape as run_raise_case:
# execute fails closed, no half-swap, mark_failed makes it durable, and a
# clean retry (without the failpoint) verifies.
run_mutate_case() {
    local table=mfp_e
    psqlq <<SQL >/dev/null
DROP TABLE IF EXISTS public.$table CASCADE;
CREATE TABLE public.$table(id int PRIMARY KEY, v text NOT NULL);
INSERT INTO public.$table VALUES (1,'a'),(2,'b'),(3,'c');
SELECT flashback_track('public.$table');
SQL
    wait_healthy "$table" || { fail "$table: not healthy after protect"; return; }
    psqlq -c "DROP TABLE public.$table;"
    wait_drop_restorable "$table" || { fail "$table: DROP never restorable"; return; }

    local token op err rc
    token=$(psqlq -c "SELECT flashback_recover_plan('public.$table', interval '1 day')->>'plan_token';")
    op=$(psqlq -c "SELECT flashback_recover_begin('public.$table', '$token', interval '1 day')->>'operation_id';")

    set_failpoint after_swap_mutate_row
    set +e
    err=$(psqlq -c "SELECT flashback_recover_execute('public.$table', '$token', interval '1 day', NULL, NULL, NULL, $op);" 2>&1)
    rc=$?
    set -e
    clear_failpoint

    if [[ $rc -eq 0 ]]; then
        fail "$table/after_swap_mutate_row: execute unexpectedly succeeded despite injected corruption"
        return
    fi
    if [[ "$(psqlq -c "SELECT to_regclass('public.$table') IS NULL;")" != "t" ]]; then
        fail "$table/after_swap_mutate_row: table is neither absent nor a clean commit (half-swapped?)"
        return
    fi
    psqlq -c "SELECT flashback_recover_mark_failed($op, 'P0001', 'after_swap_mutate_row', left(replace('$err', '''', ''''''), 200), '{}'::jsonb);" >/dev/null
    if [[ "$(psqlq -c "SELECT state FROM flashback.operation_current_state WHERE operation_id=$op;")" != "failed" ]]; then
        fail "$table/after_swap_mutate_row: mark_failed did not make failure durable"
        return
    fi
    pass "$table/after_swap_mutate_row: corruption caught atomically, swap rolled back, no half-swap, durable failed"

    local token2 op2 st2
    token2=$(psqlq -c "SELECT flashback_recover_plan('public.$table', interval '1 day')->>'plan_token';")
    op2=$(psqlq -c "SELECT flashback_recover_begin('public.$table', '$token2', interval '1 day')->>'operation_id';")
    psqlq -c "SELECT flashback_recover_execute('public.$table', '$token2', interval '1 day', NULL, NULL, NULL, $op2);" >/dev/null
    for _ in $(seq 1 240); do
        st2=$(psqlq -c "SELECT COALESCE(flashback_operation_state($op2),'missing');")
        [[ "$st2" == "verified" || "$st2" == "failed" ]] && break
        sleep 0.5
    done
    if [[ "$st2" == "verified" ]]; then
        pass "$table/after_swap_mutate_row retry after clearing failpoint verified"
    else
        fail "$table/after_swap_mutate_row retry did not verify (state=$st2)"
    fi
}
run_mutate_case

if [[ $FAILED -ne 0 ]]; then
    echo "FAIL: metadata failpoint matrix"
    exit 1
fi
echo "PASS: metadata failpoint matrix"
