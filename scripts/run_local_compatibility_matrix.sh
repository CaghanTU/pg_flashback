#!/usr/bin/env bash
# Machine-checked local_delta compatibility gate matrix.
# Requires a live PG* cluster with pg_flashback installed.
set -Eeuo pipefail

PSQL_BIN="${PSQL_BIN:-psql}"
psqlq() { "$PSQL_BIN" -X -v ON_ERROR_STOP=1 -qAt "$@"; }

if [[ "${REQUIRE_LIVE:-0}" != "1" && -z "${PGDATABASE:-}" && -z "${PGHOST:-}" ]]; then
    echo "SKIP: set PG* / REQUIRE_LIVE=1 for live local-compatibility matrix" >&2
    exit 0
fi

FAILED=0
pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1"; FAILED=1; }

cleanup() {
    local t
    for t in lc_ok lc_gen lc_part lc_excl lc_infk lc_expridx lc_partidx lc_unlogged; do
        psqlq -c "SELECT flashback_unprotect('public.$t');" >/dev/null 2>&1 || true
        psqlq -c "DROP TABLE IF EXISTS public.$t CASCADE;" >/dev/null 2>&1 || true
    done
    psqlq -c "DROP TABLE IF EXISTS public.lc_ref CASCADE;" >/dev/null 2>&1 || true
}
trap cleanup EXIT

assert_supported() {
    local table=$1
    local report supported
    report=$(psqlq -c "SELECT flashback_local_compatibility('public.$table'::regclass);")
    supported=$(printf '%s' "$report" | jq -r '.supported')
    if [[ "$supported" == "true" ]]; then
        pass "$table reports supported=true"
    else
        fail "$table expected supported=true, report=$report"
    fi
}

assert_rejected() {
    local table=$1
    local expected_feature=$2
    local report supported rejected
    report=$(psqlq -c "SELECT flashback_local_compatibility('public.$table'::regclass);")
    supported=$(printf '%s' "$report" | jq -r '.supported')
    rejected=$(printf '%s' "$report" | jq -r '.rejected_features | join(",")')
    if [[ "$supported" != "false" ]]; then
        fail "$table expected supported=false, report=$report"
        return
    fi
    if [[ "$rejected" != *"$expected_feature"* ]]; then
        fail "$table expected rejected_features to contain $expected_feature, got $rejected"
        return
    fi
    pass "$table rejected ($expected_feature)"

    # flashback_track must refuse the same way (fail-closed gate wired in).
    set +e
    local out
    out=$(psqlq -c "SELECT flashback_track('public.$table');" 2>&1)
    local rc=$?
    set -e
    if [[ $rc -eq 0 ]]; then
        fail "$table flashback_track unexpectedly succeeded"
        return
    fi
    # Some reject cases (partition/inheritance/foreign/temp/unlogged/matview)
    # are already caught earlier by flashback_require_supported_local_table's
    # own relkind/persistence checks, with its own wording; others fall
    # through to flashback_require_local_compatibility's message. Either is a
    # correct fail-closed refusal for this matrix's purposes.
    printf '%s' "$out" | grep -Eqi 'not compatible with the local DROP recovery product|not supported by the local DROP recovery product|only permanent LOGGED tables are supported' || {
        fail "$table flashback_track error did not mention a compatibility/support gate: $out"
        return
    }
    pass "$table flashback_track refuse"
}

# Ordinary supported table: PK, unique, check, outgoing FK, btree index,
# identity column, trigger, RLS policy, comment.
psqlq <<'SQL' >/dev/null
DROP TABLE IF EXISTS public.lc_ref CASCADE;
CREATE TABLE public.lc_ref(id int PRIMARY KEY);
DROP TABLE IF EXISTS public.lc_ok CASCADE;
CREATE TABLE public.lc_ok(
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ref_id int REFERENCES public.lc_ref(id),
    n text NOT NULL UNIQUE,
    v int CHECK (v >= 0)
);
CREATE INDEX lc_ok_n_idx ON public.lc_ok USING btree (n);
COMMENT ON TABLE public.lc_ok IS 'compat matrix ok table';
ALTER TABLE public.lc_ok ENABLE ROW LEVEL SECURITY;
CREATE POLICY lc_ok_all ON public.lc_ok USING (true);
SQL
assert_supported lc_ok

# Generated column: rejected by default even though "simple".
psqlq <<'SQL' >/dev/null
DROP TABLE IF EXISTS public.lc_gen CASCADE;
CREATE TABLE public.lc_gen(
    id int PRIMARY KEY,
    a int NOT NULL,
    b int GENERATED ALWAYS AS (a * 2) STORED
);
SQL
assert_rejected lc_gen generated_columns

# Partitioned table: rejected.
psqlq <<'SQL' >/dev/null
DROP TABLE IF EXISTS public.lc_part CASCADE;
CREATE TABLE public.lc_part(id int NOT NULL, d date NOT NULL) PARTITION BY RANGE (d);
CREATE TABLE public.lc_part_p1 PARTITION OF public.lc_part
    FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
SQL
assert_rejected lc_part partitioned_table

# Exclusion constraint: rejected. Skipped if btree_gist isn't installed on
# this cluster (optional contrib extension, not a pg_flashback dependency).
if psqlq -c "SELECT 1 FROM pg_available_extensions WHERE name = 'btree_gist';" | grep -q 1; then
    psqlq <<'SQL' >/dev/null
CREATE EXTENSION IF NOT EXISTS btree_gist;
DROP TABLE IF EXISTS public.lc_excl CASCADE;
CREATE TABLE public.lc_excl(
    id int PRIMARY KEY,
    v int NOT NULL,
    EXCLUDE USING gist (v WITH =)
);
SQL
    assert_rejected lc_excl exclusion_constraints
else
    echo "SKIP: btree_gist not available on this cluster; exclusion-constraint case skipped"
fi

# Incoming foreign key: rejected (the referenced table, not the referencing one).
psqlq <<'SQL' >/dev/null
DROP TABLE IF EXISTS public.lc_infk_child CASCADE;
DROP TABLE IF EXISTS public.lc_infk CASCADE;
CREATE TABLE public.lc_infk(id int PRIMARY KEY);
CREATE TABLE public.lc_infk_child(id int PRIMARY KEY, parent_id int REFERENCES public.lc_infk(id));
SQL
assert_rejected lc_infk incoming_foreign_keys
psqlq -c "DROP TABLE IF EXISTS public.lc_infk_child CASCADE;" >/dev/null 2>&1 || true

# Expression index: rejected.
psqlq <<'SQL' >/dev/null
DROP TABLE IF EXISTS public.lc_expridx CASCADE;
CREATE TABLE public.lc_expridx(id int PRIMARY KEY, n text NOT NULL);
CREATE INDEX lc_expridx_lower_idx ON public.lc_expridx (lower(n));
SQL
assert_rejected lc_expridx expression_indexes

# Partial index: rejected.
psqlq <<'SQL' >/dev/null
DROP TABLE IF EXISTS public.lc_partidx CASCADE;
CREATE TABLE public.lc_partidx(id int PRIMARY KEY, active boolean NOT NULL DEFAULT true);
CREATE INDEX lc_partidx_active_idx ON public.lc_partidx (id) WHERE active;
SQL
assert_rejected lc_partidx partial_indexes

# UNLOGGED table: rejected.
psqlq <<'SQL' >/dev/null
DROP TABLE IF EXISTS public.lc_unlogged CASCADE;
CREATE UNLOGGED TABLE public.lc_unlogged(id int PRIMARY KEY);
SQL
assert_rejected lc_unlogged unlogged_table

if [[ $FAILED -ne 0 ]]; then
    echo "FAIL: local compatibility matrix"
    exit 1
fi
echo "PASS: local compatibility matrix"
