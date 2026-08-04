-- Step 9 / Stage 6: flashback_internal_bind_online_boundary +
-- flashback_internal_materializable_columns. Exercises the marker-
-- transaction authority function directly: the happy path (real
-- boundary_xid/boundary_marker, schema_def/column_contract populated),
-- adversarial DDL committed before the bind call (fail closed, reservation
-- left durable), marker rollback leaving the Stage 5 reservation
-- untouched, and CAS-shaped rejection of a second bind attempt (no raw
-- UPDATE bypass -- this IS the only sanctioned path, and it works exactly
-- once per generation).
DO $tv$
DECLARE
    v_db oid := (SELECT oid FROM pg_database WHERE datname = current_database());
    v_stream bigint;
    v_tracking1 bigint;
    v_tracking2 bigint;
    v_tracking3 bigint;
    v_rel1 oid;
    v_rel2 oid;
    v_rel3 oid;
    v_row RECORD;
    v_bind RECORD;
    v_gen_row flashback.coverage_generations%ROWTYPE;
    v_snap_row flashback.snapshots%ROWTYPE;
    v_raised boolean;
    v_msg text;
    v_txid bigint;
    v_contract jsonb;
BEGIN
    CREATE TABLE IF NOT EXISTS public.it_bind_1 (id int PRIMARY KEY, note text);
    CREATE TABLE IF NOT EXISTS public.it_bind_2 (id int PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS public.it_bind_3 (id int PRIMARY KEY);
    v_rel1 := 'public.it_bind_1'::regclass;
    v_rel2 := 'public.it_bind_2'::regclass;
    v_rel3 := 'public.it_bind_3'::regclass;

    -- One shared active stream throughout (capture_streams_one_active_idx),
    -- matching online_reservation_authority.sql's fixture convention.
    v_stream := public.flashback_internal_create_capture_stream(
        p_database_oid => v_db, p_initial_state => 'active',
        p_slot_name => 'it_bind_slot', p_plugin_name => 'pg_flashback_decoder'
    );

    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile
    ) VALUES (v_rel1, 'public', 'it_bind_1', NULL, 'local_delta')
    RETURNING tracking_id INTO v_tracking1;
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile
    ) VALUES (v_rel2, 'public', 'it_bind_2', NULL, 'local_delta')
    RETURNING tracking_id INTO v_tracking2;
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile
    ) VALUES (v_rel3, 'public', 'it_bind_3', NULL, 'local_delta')
    RETURNING tracking_id INTO v_tracking3;

    -- 1. Happy path.
    SELECT * INTO v_row
    FROM public.flashback_internal_reserve_online_generation(
        p_tracking_id => v_tracking1, p_rel_oid => v_rel1,
        p_stream_id => v_stream, p_generation_no => 1,
        p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
        p_operation_nonce => 900001
    );

    -- Caller (M2, documented precondition) would hold SHARE ROW EXCLUSIVE
    -- here; a real lock is unnecessary to prove this function's own logic
    -- in a single-session test, but is exercised for real by the
    -- production copier once Stage 7 wires it, and by the lock-ordering
    -- regression in external_zstd_handoff.rs (Rust-level, real bgworker).
    LOCK TABLE public.it_bind_1 IN SHARE ROW EXCLUSIVE MODE;

    v_txid := txid_current();
    SELECT * INTO v_bind
    FROM public.flashback_internal_bind_online_boundary(
        p_generation_id => v_row.generation_id,
        p_tracking_id => v_tracking1,
        p_snapshot_id => v_row.snapshot_id,
        p_rel_oid => v_rel1
    );
    IF v_bind.boundary_xid IS DISTINCT FROM v_txid THEN
        RAISE EXCEPTION 'expected boundary_xid=% (txid_current at bind time), got %',
            v_txid, v_bind.boundary_xid;
    END IF;
    IF v_bind.boundary_marker IS DISTINCT FROM
        format('online_external:%s:%s:%s', v_tracking1, 1, v_txid)
    THEN
        RAISE EXCEPTION 'unexpected boundary_marker: %', v_bind.boundary_marker;
    END IF;

    SELECT * INTO v_gen_row FROM flashback.coverage_generations
    WHERE generation_id = v_row.generation_id AND tracking_id = v_tracking1;
    IF v_gen_row.boundary_xid IS DISTINCT FROM v_txid
       OR v_gen_row.boundary_marker IS DISTINCT FROM v_bind.boundary_marker
       OR v_gen_row.state IS DISTINCT FROM 'building'
    THEN
        RAISE EXCEPTION 'generation row not durably bound as expected: xid=%, marker=%, state=%',
            v_gen_row.boundary_xid, v_gen_row.boundary_marker, v_gen_row.state;
    END IF;

    SELECT * INTO v_snap_row FROM flashback.snapshots
    WHERE snapshot_id = v_row.snapshot_id AND tracking_id = v_tracking1;
    IF v_snap_row.schema_def IS NULL OR v_snap_row.schema_def = '{}'::jsonb
       OR v_snap_row.schema_def->>'table' IS DISTINCT FROM 'it_bind_1'
    THEN
        RAISE EXCEPTION 'snapshot schema_def not bound as expected: %', v_snap_row.schema_def;
    END IF;
    v_contract := v_snap_row.external_column_contract;
    IF v_contract IS NULL OR jsonb_array_length(v_contract) IS DISTINCT FROM 2 THEN
        RAISE EXCEPTION 'expected a 2-column materializable contract (id, note), got: %', v_contract;
    END IF;
    IF (v_contract->0->>'name') IS DISTINCT FROM 'id'
       OR (v_contract->1->>'name') IS DISTINCT FROM 'note'
    THEN
        RAISE EXCEPTION 'materializable column contract has unexpected column order/names: %', v_contract;
    END IF;

    -- 2. CAS / no raw UPDATE bypass: a second bind attempt against the now-
    -- already-bound generation must fail closed, not re-stamp a new xid.
    v_raised := false;
    BEGIN
        PERFORM public.flashback_internal_bind_online_boundary(
            p_generation_id => v_row.generation_id, p_tracking_id => v_tracking1,
            p_snapshot_id => v_row.snapshot_id, p_rel_oid => v_rel1
        );
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'a second bind_online_boundary call against an already-bound generation must fail-closed';
    END IF;
    -- And the durable row must be unchanged by the rejected second attempt.
    SELECT * INTO v_gen_row FROM flashback.coverage_generations
    WHERE generation_id = v_row.generation_id AND tracking_id = v_tracking1;
    IF v_gen_row.boundary_xid IS DISTINCT FROM v_txid THEN
        RAISE EXCEPTION 'rejected second bind attempt still mutated boundary_xid: now %, expected %',
            v_gen_row.boundary_xid, v_txid;
    END IF;

    -- 3. Marker rollback with durable reservation: reserve for tracking2,
    -- then attempt a bind with a mismatched rel_oid (simulating a caller
    -- that failed its own identity revalidation) inside a subtransaction.
    -- The bind must fail closed AND the Stage 5 reservation must remain
    -- exactly as it was -- 'building', boundary_xid still NULL -- ready for
    -- the reconciler's tombstone path (plan §1h), never partially mutated.
    SELECT * INTO v_row
    FROM public.flashback_internal_reserve_online_generation(
        p_tracking_id => v_tracking2, p_rel_oid => v_rel2,
        p_stream_id => v_stream, p_generation_no => 1,
        p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
        p_operation_nonce => 900002
    );
    v_raised := false;
    BEGIN
        PERFORM public.flashback_internal_bind_online_boundary(
            p_generation_id => v_row.generation_id, p_tracking_id => v_tracking2,
            p_snapshot_id => v_row.snapshot_id,
            p_rel_oid => v_rel1 -- wrong: this reservation is for v_rel2
        );
    EXCEPTION WHEN invalid_parameter_value THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'bind_online_boundary accepted a rel_oid not matching the reservation';
    END IF;
    SELECT * INTO v_gen_row FROM flashback.coverage_generations
    WHERE generation_id = v_row.generation_id AND tracking_id = v_tracking2;
    IF v_gen_row.state IS DISTINCT FROM 'building' OR v_gen_row.boundary_xid IS NOT NULL THEN
        RAISE EXCEPTION 'reservation was not left durable/untouched after the rejected bind: state=%, xid=%',
            v_gen_row.state, v_gen_row.boundary_xid;
    END IF;
    SELECT * INTO v_snap_row FROM flashback.snapshots
    WHERE snapshot_id = v_row.snapshot_id AND tracking_id = v_tracking2;
    IF v_snap_row.payload_state IS DISTINCT FROM 'creating' OR v_snap_row.schema_def <> '{}'::jsonb THEN
        RAISE EXCEPTION 'reserved snapshot was not left durable/untouched after the rejected bind: state=%, schema_def=%',
            v_snap_row.payload_state, v_snap_row.schema_def;
    END IF;

    -- 4. Adversarial DDL committed between reservation and the (simulated)
    -- lock/bind attempt: the table is dropped entirely before bind_online_
    -- boundary is ever called. flashback_internal_materializable_columns
    -- sees zero pg_attribute rows for the now-nonexistent oid, and this
    -- function's own empty-contract check rejects it -- fail closed, not a
    -- silent empty artifact.
    SELECT * INTO v_row
    FROM public.flashback_internal_reserve_online_generation(
        p_tracking_id => v_tracking3, p_rel_oid => v_rel3,
        p_stream_id => v_stream, p_generation_no => 1,
        p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
        p_operation_nonce => 900003
    );
    PERFORM flashback_set_restore_in_progress(true);
    BEGIN
        DROP TABLE public.it_bind_3;
    EXCEPTION WHEN OTHERS THEN
        PERFORM flashback_set_restore_in_progress(false);
        RAISE;
    END;
    PERFORM flashback_set_restore_in_progress(false);

    v_raised := false;
    BEGIN
        PERFORM public.flashback_internal_bind_online_boundary(
            p_generation_id => v_row.generation_id, p_tracking_id => v_tracking3,
            p_snapshot_id => v_row.snapshot_id, p_rel_oid => v_rel3
        );
    EXCEPTION WHEN invalid_parameter_value THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT ILIKE '%materializable%' THEN
        RAISE EXCEPTION 'bind_online_boundary against a dropped relation must fail-closed on empty column contract, got: %', v_msg;
    END IF;
    SELECT * INTO v_gen_row FROM flashback.coverage_generations
    WHERE generation_id = v_row.generation_id AND tracking_id = v_tracking3;
    IF v_gen_row.state IS DISTINCT FROM 'building' OR v_gen_row.boundary_xid IS NOT NULL THEN
        RAISE EXCEPTION 'reservation was not left durable/untouched after DDL-drift bind rejection: state=%, xid=%',
            v_gen_row.state, v_gen_row.boundary_xid;
    END IF;

    -- 5. flashback_guard_coverage_generation's widening (schema_bootstrap.
    -- sql) that made check 1 above legal must NOT have loosened anything
    -- for heap_v1: its boundary_xid/boundary_marker are always set
    -- atomically at INSERT (OLD.boundary_xid is never NULL for a heap_v1
    -- row), so the widened exception's own preconditions can never be
    -- satisfied and a direct UPDATE attempt must still be rejected exactly
    -- as before. Exercised as a raw UPDATE (this DO block runs with
    -- superuser/table-owner privileges in the test harness, unlike a
    -- delegated role) to probe the trigger itself, independent of
    -- whatever this file's own authority functions do or don't check.
    DECLARE
        v_heap_gen bigint;
        v_heap_snap bigint;
        v_heap_tracking bigint;
    BEGIN
        CREATE TABLE IF NOT EXISTS public.it_bind_heap (id int PRIMARY KEY);
        INSERT INTO flashback.tracked_tables (
            rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile
        ) VALUES ('public.it_bind_heap'::regclass, 'public', 'it_bind_heap', NULL, 'local_delta')
        RETURNING tracking_id INTO v_heap_tracking;
        v_heap_snap := public.flashback_internal_snapshot_create(
            v_heap_tracking, 'public.it_bind_heap'::regclass, 'public', 'it_bind_heap',
            '0/ABCDEF'::pg_lsn, 'initial_track'
        );
        v_heap_gen := public.flashback_internal_create_coverage_generation(
            p_tracking_id => v_heap_tracking,
            p_generation_no => 1,
            p_stream_id => v_stream,
            p_boundary_kind => 'initial_track',
            p_rel_oid_at_boundary => 'public.it_bind_heap'::regclass,
            p_boundary_snapshot_id => v_heap_snap,
            p_boundary_xid => txid_current(),
            p_boundary_marker => 'heap-guard-regression'
        );
        v_raised := false;
        BEGIN
            UPDATE flashback.coverage_generations
               SET boundary_xid = txid_current() + 1
             WHERE generation_id = v_heap_gen;
        EXCEPTION WHEN integrity_constraint_violation THEN
            v_raised := true;
        END;
        IF NOT v_raised THEN
            RAISE EXCEPTION 'the widened coverage_generations_guard trigger let a heap_v1 boundary_xid be mutated';
        END IF;

        PERFORM flashback_set_restore_in_progress(true);
        BEGIN
            DROP TABLE public.it_bind_heap CASCADE;
        EXCEPTION WHEN OTHERS THEN
            PERFORM flashback_set_restore_in_progress(false);
            RAISE;
        END;
        PERFORM flashback_set_restore_in_progress(false);
    END;

    -- 6. RBAC.
    IF has_function_privilege(
        'public', 'public.flashback_internal_bind_online_boundary'::regproc, 'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'RBAC Failed: PUBLIC must not EXECUTE flashback_internal_bind_online_boundary';
    END IF;
    IF has_function_privilege(
        'public', 'public.flashback_internal_materializable_columns'::regproc, 'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'RBAC Failed: PUBLIC must not EXECUTE flashback_internal_materializable_columns';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flashback_admin')
       AND has_function_privilege(
            'flashback_admin', 'public.flashback_internal_bind_online_boundary'::regproc, 'EXECUTE'
       )
    THEN
        RAISE EXCEPTION 'RBAC Failed: flashback_admin must not EXECUTE flashback_internal_bind_online_boundary';
    END IF;
    IF has_function_privilege(
        'pg_monitor', 'public.flashback_internal_bind_online_boundary'::regproc, 'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'RBAC Failed: pg_monitor must not EXECUTE flashback_internal_bind_online_boundary';
    END IF;

    PERFORM flashback_set_restore_in_progress(true);
    BEGIN
        DROP TABLE IF EXISTS public.it_bind_1 CASCADE;
        DROP TABLE IF EXISTS public.it_bind_2 CASCADE;
    EXCEPTION WHEN OTHERS THEN
        PERFORM flashback_set_restore_in_progress(false);
        RAISE;
    END;
    PERFORM flashback_set_restore_in_progress(false);
END;
$tv$;
