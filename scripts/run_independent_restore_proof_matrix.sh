#!/usr/bin/env bash
# Independent restore proof matrix.
#
# flashback_relation_full_data_fingerprint()/flashback_fingerprint_order_spec()
# (src/restore/proof.rs) are an independent verification layer: the fingerprint
# order comes only from the immutable target schema_def, never from the live
# or shadow relation's own catalog (so a restore that silently reorders,
# drops, or duplicates rows cannot pass just because row counts match).
#
# This matrix protects a shape, fingerprints it, DROPs it, recovers it, then
# fingerprints again and asserts an exact match — across shapes where a
# naive row-count-only check could still hide corruption (no PK / duplicate
# rows, composite PK, NULLs, wide TOASTed text). It also asserts the
# fingerprint function is actually sensitive to a one-row mutation, so a
# trivially-constant "proof" cannot pass this matrix by accident.
#
# Requires a live PG* cluster with pg_flashback installed.
set -Eeuo pipefail

PSQL_BIN="${PSQL_BIN:-psql}"
psqlq() { "$PSQL_BIN" -X -v ON_ERROR_STOP=1 -qAt "$@"; }

if [[ "${REQUIRE_LIVE:-0}" != "1" && -z "${PGDATABASE:-}" && -z "${PGHOST:-}" ]]; then
    echo "SKIP: set PG* / REQUIRE_LIVE=1 for live independent-restore-proof matrix" >&2
    exit 0
fi

FAILED=0
pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1"; FAILED=1; }

TABLES=(irp_nopk irp_composite irp_nulls irp_toast)
cleanup() {
    local t
    for t in "${TABLES[@]}"; do
        psqlq -c "SELECT flashback_unprotect('public.$t');" >/dev/null 2>&1 || true
        psqlq -c "DROP TABLE IF EXISTS public.$t CASCADE;" >/dev/null 2>&1 || true
    done
}
trap cleanup EXIT

wait_healthy() {
    local table=$1
    local h=""
    for _ in $(seq 1 240); do
        h=$(psqlq -c "SELECT flashback_lifecycle_health('public.$table');")
        [[ "$h" == "healthy" ]] && break
        sleep 0.25
    done
    [[ "$h" == "healthy" ]]
}

wait_drop_restorable() {
    local table=$1
    local st=""
    for _ in $(seq 1 240); do
        st=$(psqlq -c "SELECT status FROM flashback_disaster_points('public.$table', interval '1 day') WHERE event_type='DROP' ORDER BY disaster_commit_lsn DESC LIMIT 1;")
        [[ "$st" == "restorable" ]] && return 0
        sleep 0.25
    done
    return 1
}

fingerprint() {
    local table=$1 order_spec_json=$2
    psqlq -c "SELECT flashback_relation_full_data_fingerprint('public.$table'::regclass, flashback_fingerprint_order_spec('$order_spec_json'::jsonb));"
}

# Full drop/recover round-trip for one (table, order_spec) pair, asserting
# the independent fingerprint matches before and after.
run_case() {
    local table=$1 order_spec_json=$2
    local fp_before fp_after plan token op st

    wait_healthy "$table" || { fail "$table not healthy after protect"; return; }
    fp_before=$(fingerprint "$table" "$order_spec_json")
    [[ -n "$fp_before" ]] || { fail "$table: empty fingerprint before DROP"; return; }

    psqlq -c "DROP TABLE public.$table;"
    wait_drop_restorable "$table" || { fail "$table: DROP never became restorable"; return; }

    plan=$(psqlq -c "SELECT flashback_recover_plan('public.$table', interval '1 day');")
    token=$(printf '%s' "$plan" | jq -r '.plan_token // empty')
    [[ -n "$token" && "$token" != "null" ]] || { fail "$table: no plan_token ($plan)"; return; }

    op=$(psqlq -c "SELECT flashback_recover_begin('public.$table', '$token', interval '1 day')->>'operation_id';")
    psqlq -c "SELECT flashback_recover_execute('public.$table', '$token', interval '1 day', NULL, NULL, NULL, $op);" >/dev/null

    for _ in $(seq 1 240); do
        st=$(psqlq -c "SELECT COALESCE(flashback_operation_state($op),'missing');")
        [[ "$st" == "verified" || "$st" == "failed" ]] && break
        sleep 0.5
    done
    [[ "$st" == "verified" ]] || { fail "$table: recover operation_state=$st (expected verified)"; return; }

    fp_after=$(fingerprint "$table" "$order_spec_json")
    if [[ "$fp_after" == "$fp_before" ]]; then
        pass "$table independent fingerprint matches after recover ($fp_after)"
    else
        fail "$table fingerprint mismatch: before=$fp_before after=$fp_after"
    fi
}

# Sensitivity sanity check: mutating one row must change the fingerprint.
# Guards against a trivially-constant "proof" passing this matrix by luck.
assert_sensitive() {
    local table=$1 mutate_sql=$2 order_spec_json=$3
    local fp1 fp2
    fp1=$(fingerprint "$table" "$order_spec_json")
    psqlq -c "$mutate_sql"
    fp2=$(fingerprint "$table" "$order_spec_json")
    if [[ "$fp1" != "$fp2" ]]; then
        pass "$table fingerprint is sensitive to a one-row mutation"
    else
        fail "$table fingerprint did not change after mutating data ($mutate_sql)"
    fi
}

# --- no PK, exact duplicate rows: full_row mode must reflect multiplicity ---
psqlq <<'SQL' >/dev/null
DROP TABLE IF EXISTS public.irp_nopk CASCADE;
CREATE TABLE public.irp_nopk(v int NOT NULL, note text);
INSERT INTO public.irp_nopk(v, note) VALUES (1,'a'), (1,'a'), (2,'b');
SELECT flashback_track('public.irp_nopk');
SQL
assert_sensitive irp_nopk "INSERT INTO public.irp_nopk(v, note) VALUES (1,'a');" '{"primary_key":[]}'
run_case irp_nopk '{"primary_key":[]}'

# --- composite PK ---
psqlq <<'SQL' >/dev/null
DROP TABLE IF EXISTS public.irp_composite CASCADE;
CREATE TABLE public.irp_composite(a int, b int, v text NOT NULL, PRIMARY KEY (a,b));
INSERT INTO public.irp_composite(a,b,v) SELECT g%3, g, 'x'||g FROM generate_series(1,50) g;
SELECT flashback_track('public.irp_composite');
SQL
run_case irp_composite '{"primary_key":["a","b"]}'

# --- NULLs across nullable columns ---
psqlq <<'SQL' >/dev/null
DROP TABLE IF EXISTS public.irp_nulls CASCADE;
CREATE TABLE public.irp_nulls(id int PRIMARY KEY, a int, b text, c timestamptz);
INSERT INTO public.irp_nulls(id,a,b,c)
  SELECT g, CASE WHEN g%2=0 THEN NULL ELSE g END,
            CASE WHEN g%3=0 THEN NULL ELSE 'n'||g END,
            CASE WHEN g%5=0 THEN NULL ELSE clock_timestamp() END
  FROM generate_series(1,50) g;
SELECT flashback_track('public.irp_nulls');
SQL
run_case irp_nulls '{"primary_key":["id"]}'

# --- wide TOASTed text ---
psqlq <<'SQL' >/dev/null
DROP TABLE IF EXISTS public.irp_toast CASCADE;
CREATE TABLE public.irp_toast(id int PRIMARY KEY, blob text NOT NULL);
INSERT INTO public.irp_toast(id, blob)
  SELECT g, repeat('z', 4096) || g::text FROM generate_series(1,20) g;
SELECT flashback_track('public.irp_toast');
SQL
run_case irp_toast '{"primary_key":["id"]}'

if [[ $FAILED -ne 0 ]]; then
    echo "FAIL: independent restore proof matrix"
    exit 1
fi
echo "PASS: independent restore proof matrix"
