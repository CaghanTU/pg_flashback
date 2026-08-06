#!/usr/bin/env bash
# external_zstd marker-transaction E2E against a live logical-decoding server.
#
# Proves with real, separate database sessions that COMMIT makes both the
# binding and exactly one marker+COMMIT pair visible, while ROLLBACK hides
# both and leaves a clean, retryable reservation.
set -euo pipefail

BINDIR="${1:-}"
if [[ -z "$BINDIR" ]]; then
    for cand in /usr/local/pgsql-17/bin /usr/pgsql-17/bin "$HOME/.pgrx/17."*/pgrx-install/bin /usr/bin; do
        if [[ -x "$cand/psql" ]]; then BINDIR="$cand"; break; fi
    done
fi
[[ -n "$BINDIR" ]] || { echo "FAIL: psql not found; pass PostgreSQL bindir"; exit 1; }

PSQL="$BINDIR/psql"
DB="pgfb_extzstd_marker_$$"
SLOT="pgfb_extzstd_marker_$$"
export PGHOST="${PGHOST:-$HOME/.pgrx}"
export PGPORT="${PGPORT:-28817}"

q() { "$PSQL" -X -v ON_ERROR_STOP=1 -d "$DB" -Atqc "$1"; }
qp() { "$PSQL" -X -v ON_ERROR_STOP=1 -d postgres -Atqc "$1"; }

cleanup() {
    local rc=$?
    q "SELECT pg_drop_replication_slot('$SLOT')
       WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$SLOT')" \
        >/dev/null 2>&1 || true
    qp "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
        WHERE datname='$DB' AND pid <> pg_backend_pid()" >/dev/null 2>&1 || true
    qp "DROP DATABASE IF EXISTS \"$DB\" WITH (FORCE)" >/dev/null 2>&1 || true
    exit "$rc"
}
trap cleanup EXIT

assert_eq() {
    local description=$1 expected=$2 actual=$3
    [[ "$actual" == "$expected" ]] || {
        echo "FAIL: $description (expected=$expected actual=$actual)"
        exit 1
    }
    echo "  ok: $description = $actual"
}

echo "━━━ external_zstd marker transaction E2E ━━━"
qp "DROP DATABASE IF EXISTS \"$DB\" WITH (FORCE)" >/dev/null
qp "CREATE DATABASE \"$DB\"" >/dev/null
q "CREATE EXTENSION pg_flashback" >/dev/null

# Slot creation must be the first write of its own transaction. Keep it in a
# standalone psql call so this proof cannot inherit pg_test's transaction
# limitation.
q "SELECT slot_name FROM pg_create_logical_replication_slot('$SLOT', 'pg_flashback')" >/dev/null

q "DO \$setup\$
DECLARE
    v_db oid := (SELECT oid FROM pg_database WHERE datname=current_database());
    v_stream bigint;
    v_rel oid;
    v_tracking bigint;
    v_snapshot bigint;
    v_generation bigint;
    v_name text;
BEGIN
    v_stream := public.flashback_internal_create_capture_stream(
        p_database_oid => v_db,
        p_initial_state => 'active',
        p_slot_name => '$SLOT',
        p_plugin_name => 'pg_flashback'
    );

    FOREACH v_name IN ARRAY ARRAY['ext_marker_commit', 'ext_marker_rollback'] LOOP
        EXECUTE format('CREATE TABLE public.%I (id int PRIMARY KEY, note text NOT NULL)', v_name);
        EXECUTE format('INSERT INTO public.%I VALUES (1, %L)', v_name, 'before');
        v_rel := format('public.%I', v_name)::regclass::oid;

        INSERT INTO flashback.tracked_tables
            (rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile)
        VALUES (v_rel, 'public', v_name, NULL, 'local_delta')
        RETURNING tracking_id INTO v_tracking;

        v_snapshot := public.flashback_internal_snapshot_create(
            v_tracking, v_rel, 'public', v_name, '0/1000'::pg_lsn, 'initial_track'
        );
        v_generation := public.flashback_internal_create_coverage_generation(
            p_tracking_id => v_tracking,
            p_generation_no => 1,
            p_stream_id => v_stream,
            p_boundary_kind => 'initial_track',
            p_rel_oid_at_boundary => v_rel,
            p_boundary_snapshot_id => v_snapshot,
            p_boundary_xid => txid_current(),
            p_boundary_marker => 'marker-e2e-parent:' || v_name
        );
        PERFORM public.flashback_internal_transition_coverage_generation(
            v_generation, v_tracking, 'building', 'active', 'activate',
            '0/1000'::pg_lsn, clock_timestamp(), '0/1000'::pg_lsn,
            clock_timestamp(), NULL, NULL, '{}'::jsonb
        );
        INSERT INTO flashback.schema_versions (
            rel_oid, tracking_id, generation_id, stream_id, source_xid,
            schema_version, applied_at, applied_lsn, columns, schema_def
        ) VALUES (
            v_rel, v_tracking, v_generation, v_stream, txid_current(),
            1, clock_timestamp(), '0/1000'::pg_lsn,
            COALESCE(public.flashback_collect_schema_def(v_rel)->'columns', '[]'::jsonb),
            public.flashback_collect_schema_def(v_rel)
        );
    END LOOP;
END;
\$setup\$" >/dev/null

COMMIT_OID=$(q "SELECT 'public.ext_marker_commit'::regclass::oid")
ROLLBACK_OID=$(q "SELECT 'public.ext_marker_rollback'::regclass::oid")
TRACKED_OIDS="$COMMIT_OID,$ROLLBACK_OID"

# Discard setup WAL. Only the marker transactions below are evidence.
q "SELECT count(*) FROM pg_logical_slot_get_changes(
       '$SLOT', NULL, NULL, 'tracked_oids', '$TRACKED_OIDS')" >/dev/null

reserve() {
    local table=$1 nonce=$2
    q "WITH ids AS (
           SELECT tt.tracking_id, tt.rel_oid,
                  (SELECT stream_id FROM flashback.capture_streams
                   WHERE state='active' ORDER BY stream_id LIMIT 1) AS stream_id,
                  (SELECT generation_id FROM flashback.coverage_generations cg
                   WHERE cg.tracking_id=tt.tracking_id AND cg.state='active') AS parent_id
           FROM flashback.tracked_tables tt
           WHERE tt.schema_name='public' AND tt.table_name='$table' AND tt.is_active
       )
       SELECT generation_id || '|' || snapshot_id
       FROM ids, LATERAL public.flashback_internal_reserve_online_generation(
           p_tracking_id => ids.tracking_id,
           p_rel_oid => ids.rel_oid,
           p_stream_id => ids.stream_id,
           p_generation_no => 2,
           p_parent_generation_id => ids.parent_id,
           p_storage_backend => 'external_zstd',
           p_operation_nonce => $nonce
       )"
}

marker_call() {
    local table=$1 generation=$2 snapshot=$3
    q "SELECT public.flashback_internal_run_external_marker_transaction(
           tt.tracking_id, tt.rel_oid::bigint, $generation, $snapshot)
       FROM flashback.tracked_tables tt
       WHERE tt.schema_name='public' AND tt.table_name='$table' AND tt.is_active"
}

decode() {
    q "SELECT data FROM pg_logical_slot_get_changes(
           '$SLOT', NULL, NULL, 'tracked_oids', '$TRACKED_OIDS')"
}

echo "── committed marker is visible exactly once ──"
IFS='|' read -r COMMIT_GEN COMMIT_SNAP <<<"$(reserve ext_marker_commit 880001)"
COMMIT_JSON=$(marker_call ext_marker_commit "$COMMIT_GEN" "$COMMIT_SNAP")
COMMIT_XID=$(jq -er '.boundary_xid' <<<"$COMMIT_JSON")
COMMIT_OUTPUT=$(decode)
assert_eq "committed marker count" 1 "$(grep -Fxc "{\"marker\":$COMMIT_XID}" <<<"$COMMIT_OUTPUT" || true)"
assert_eq "committed COMMIT count" 1 "$(grep -Ec "^\{\"commit\":$COMMIT_XID," <<<"$COMMIT_OUTPUT" || true)"
assert_eq "committed decoded row count" 2 "$(grep -c . <<<"$COMMIT_OUTPUT" || true)"
assert_eq "second consume is empty" "" "$(decode)"
assert_eq "committed boundary persisted" "$COMMIT_XID" \
    "$(q "SELECT boundary_xid FROM flashback.coverage_generations WHERE generation_id=$COMMIT_GEN")"

echo "── rolled-back marker is invisible and leaves no binding ──"
IFS='|' read -r ROLLBACK_GEN ROLLBACK_SNAP <<<"$(reserve ext_marker_rollback 880002)"
ROLLBACK_JSON=$(q "BEGIN;
    SELECT public.flashback_internal_run_external_marker_transaction(
        tt.tracking_id, tt.rel_oid::bigint, $ROLLBACK_GEN, $ROLLBACK_SNAP)
    FROM flashback.tracked_tables tt
    WHERE tt.schema_name='public' AND tt.table_name='ext_marker_rollback' AND tt.is_active;
    ROLLBACK")
ROLLBACK_XID=$(jq -er '.boundary_xid' <<<"$ROLLBACK_JSON")
assert_eq "rolled-back decoder output" "" "$(decode)"
assert_eq "rolled-back generation remains unbound" "t" \
    "$(q "SELECT boundary_xid IS NULL AND boundary_marker='online_pending:880002'
          FROM flashback.coverage_generations WHERE generation_id=$ROLLBACK_GEN")"
assert_eq "rolled-back snapshot remains creating/unbound" "t" \
    "$(q "SELECT payload_state='creating' AND snapshot_lsn IS NULL
          FROM flashback.snapshots WHERE snapshot_id=$ROLLBACK_SNAP")"
[[ "$ROLLBACK_XID" =~ ^[0-9]+$ ]] || { echo "FAIL: rollback call did not return its transaction xid"; exit 1; }

echo "── retry after rollback commits exactly once ──"
RETRY_JSON=$(marker_call ext_marker_rollback "$ROLLBACK_GEN" "$ROLLBACK_SNAP")
RETRY_XID=$(jq -er '.boundary_xid' <<<"$RETRY_JSON")
RETRY_OUTPUT=$(decode)
assert_eq "retry marker count" 1 "$(grep -Fxc "{\"marker\":$RETRY_XID}" <<<"$RETRY_OUTPUT" || true)"
assert_eq "retry COMMIT count" 1 "$(grep -Ec "^\{\"commit\":$RETRY_XID," <<<"$RETRY_OUTPUT" || true)"
assert_eq "retry decoded row count" 2 "$(grep -c . <<<"$RETRY_OUTPUT" || true)"
assert_eq "retry consume is idempotently empty" "" "$(decode)"
assert_eq "retry boundary persisted" "$RETRY_XID" \
    "$(q "SELECT boundary_xid FROM flashback.coverage_generations WHERE generation_id=$ROLLBACK_GEN")"

echo "╔══════════════════════════════════════════════╗"
echo "║ EXTERNAL_ZSTD MARKER TRANSACTION E2E: PASS  ║"
echo "╚══════════════════════════════════════════════╝"
