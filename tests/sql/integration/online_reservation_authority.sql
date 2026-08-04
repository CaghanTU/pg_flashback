-- Step 9 / Stage 5: flashback_internal_reserve_online_generation +
-- flashback_internal_snapshot_reserve. Exercises the new centralized
-- online-reservation authority directly: the atomic snapshot+generation
-- bind, the reused stream/lifecycle/parent validation, the durable
-- 'building'-row admission check (the exclusivity invariant the entire
-- online-create protocol depends on), operation_nonce uniqueness
-- (binding guardrail: must be enforced by the database, not merely
-- assumed from the caller), and RBAC.
-- capture_streams_one_active_idx (schema_bootstrap.sql) permits at most one
-- 'active' row per database_oid -- pg_flashback uses one shared logical
-- stream per database for every tracked table, not one per table. This
-- fixture therefore uses a single shared active stream throughout (like
-- real production usage / flashback_ensure_active_wal_stream), and builds
-- its broken-stream fixture via initializing -> broken directly (a legal
-- transition per flashback_internal_transition_capture_stream's graph),
-- never passing through 'active' at all, so it never collides with the
-- single already-active stream.
DO $tv$
DECLARE
    v_db oid := (SELECT oid FROM pg_database WHERE datname = current_database());
    v_stream bigint;
    v_broken_stream bigint;
    v_tracking1 bigint;
    v_tracking2 bigint;
    v_tracking3 bigint;
    v_rel1 oid;
    v_rel2 oid;
    v_rel3 oid;
    v_gen1 bigint;
    v_snap1 bigint;
    v_row RECORD;
    v_snap_row flashback.snapshots%ROWTYPE;
    v_gen_row flashback.coverage_generations%ROWTYPE;
    v_raised boolean;
    v_msg text;
    v_other_gen bigint;
    v_other_snap bigint;
BEGIN
    CREATE TABLE IF NOT EXISTS public.it_online_res_1 (id int PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS public.it_online_res_2 (id int PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS public.it_online_res_3 (id int PRIMARY KEY);
    v_rel1 := 'public.it_online_res_1'::regclass;
    v_rel2 := 'public.it_online_res_2'::regclass;
    v_rel3 := 'public.it_online_res_3'::regclass;

    v_stream := public.flashback_internal_create_capture_stream(
        p_database_oid => v_db, p_initial_state => 'active',
        p_slot_name => 'it_online_res_slot', p_plugin_name => 'pg_flashback_decoder'
    );

    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile
    ) VALUES (v_rel1, 'public', 'it_online_res_1', NULL, 'local_delta')
    RETURNING tracking_id INTO v_tracking1;
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile
    ) VALUES (v_rel2, 'public', 'it_online_res_2', NULL, 'local_delta')
    RETURNING tracking_id INTO v_tracking2;
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile
    ) VALUES (v_rel3, 'public', 'it_online_res_3', NULL, 'local_delta')
    RETURNING tracking_id INTO v_tracking3;

    -- 1. Happy path: reserve an online generation for tracking1.
    SELECT * INTO v_row
    FROM public.flashback_internal_reserve_online_generation(
        p_tracking_id => v_tracking1,
        p_rel_oid => v_rel1,
        p_stream_id => v_stream,
        p_generation_no => 1,
        p_parent_generation_id => NULL,
        p_storage_backend => 'external_zstd',
        p_operation_nonce => 424242
    );
    v_gen1 := v_row.generation_id;
    v_snap1 := v_row.snapshot_id;
    IF v_gen1 IS NULL OR v_snap1 IS NULL THEN
        RAISE EXCEPTION 'reserve_online_generation returned NULL generation_id/snapshot_id';
    END IF;

    SELECT * INTO v_snap_row FROM flashback.snapshots
    WHERE snapshot_id = v_snap1 AND tracking_id = v_tracking1;
    IF v_snap_row.payload_state IS DISTINCT FROM 'creating'
       OR v_snap_row.storage_backend IS DISTINCT FROM 'external_zstd'
       OR v_snap_row.snapshot_lsn IS NOT NULL
       OR v_snap_row.rel_oid IS DISTINCT FROM v_rel1
    THEN
        RAISE EXCEPTION 'reserved snapshot row shape wrong: state=%, backend=%, lsn=%, rel=%',
            v_snap_row.payload_state, v_snap_row.storage_backend, v_snap_row.snapshot_lsn, v_snap_row.rel_oid;
    END IF;

    SELECT * INTO v_gen_row FROM flashback.coverage_generations
    WHERE generation_id = v_gen1 AND tracking_id = v_tracking1;
    IF v_gen_row.state IS DISTINCT FROM 'building'
       OR v_gen_row.storage_backend IS DISTINCT FROM 'external_zstd'
       OR v_gen_row.operation_nonce IS DISTINCT FROM 424242
       OR v_gen_row.boundary_marker IS DISTINCT FROM 'online_pending:424242'
       OR v_gen_row.boundary_xid IS NOT NULL
       OR v_gen_row.boundary_snapshot_id IS DISTINCT FROM v_snap1
       OR (v_gen_row.details->>'operation_nonce')::bigint IS DISTINCT FROM 424242
    THEN
        RAISE EXCEPTION 'reserved generation row shape wrong: state=%, backend=%, nonce=%, marker=%, xid=%, snap=%',
            v_gen_row.state, v_gen_row.storage_backend, v_gen_row.operation_nonce,
            v_gen_row.boundary_marker, v_gen_row.boundary_xid, v_gen_row.boundary_snapshot_id;
    END IF;

    -- 2. Durable admission check: a second reservation attempt for the same
    -- tracking_id, while the first is still 'building', must fail -- the
    -- durable row itself is the exclusivity invariant, not any session lock.
    v_raised := false;
    BEGIN
        PERFORM public.flashback_internal_reserve_online_generation(
            p_tracking_id => v_tracking1, p_rel_oid => v_rel1,
            p_stream_id => v_stream, p_generation_no => 2,
            p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
            p_operation_nonce => 424243
        );
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'second online reservation for an already-building tracking_id must fail-closed';
    END IF;

    -- 3. operation_nonce uniqueness is enforced by the database, not merely
    -- assumed: reusing tracking1's nonce for an unrelated tracking_id
    -- (tracking2, otherwise perfectly valid) must fail with unique_violation.
    v_raised := false;
    BEGIN
        PERFORM public.flashback_internal_reserve_online_generation(
            p_tracking_id => v_tracking2, p_rel_oid => v_rel2,
            p_stream_id => v_stream, p_generation_no => 1,
            p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
            p_operation_nonce => 424242
        );
    EXCEPTION WHEN unique_violation THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'colliding operation_nonce across different tracking_ids must fail-closed';
    END IF;

    -- 4. heap_v1 is explicitly rejected by this online-only authority (it
    -- has no use for a no-copy reservation; flashback_internal_create_
    -- coverage_generation remains its only construction path, unmodified).
    v_raised := false;
    BEGIN
        PERFORM public.flashback_internal_reserve_online_generation(
            p_tracking_id => v_tracking2, p_rel_oid => v_rel2,
            p_stream_id => v_stream, p_generation_no => 1,
            p_parent_generation_id => NULL, p_storage_backend => 'heap_v1',
            p_operation_nonce => 555001
        );
    EXCEPTION WHEN invalid_parameter_value THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT ILIKE '%external_zstd%' THEN
        RAISE EXCEPTION 'reserve_online_generation must reject storage_backend=heap_v1, got: %', v_msg;
    END IF;

    -- 5. Reused stream validation: a broken stream must be rejected, exactly
    -- like flashback_internal_create_coverage_generation's own check.
    -- Created as 'initializing', never 'active' -- capture_streams_one_
    -- active_idx permits only one 'active' row per database, already held
    -- by v_stream throughout this test.
    v_broken_stream := public.flashback_internal_create_capture_stream(
        p_database_oid => v_db, p_initial_state => 'initializing',
        p_slot_name => 'it_online_res_broken_slot', p_plugin_name => 'pg_flashback_decoder'
    );
    PERFORM public.flashback_internal_transition_capture_stream(
        v_broken_stream, ARRAY['initializing'], 'broken', 'online_res_test_break', '{}'::jsonb
    );
    v_raised := false;
    BEGIN
        PERFORM public.flashback_internal_reserve_online_generation(
            p_tracking_id => v_tracking2, p_rel_oid => v_rel2,
            p_stream_id => v_broken_stream, p_generation_no => 1,
            p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
            p_operation_nonce => 555002
        );
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'reserve_online_generation accepted a broken stream';
    END IF;

    -- 6. Reused parent-generation validation: a parent_generation_id
    -- belonging to a different tracking_id must be rejected.
    v_raised := false;
    BEGIN
        PERFORM public.flashback_internal_reserve_online_generation(
            p_tracking_id => v_tracking2, p_rel_oid => v_rel2,
            p_stream_id => v_stream, p_generation_no => 1,
            p_parent_generation_id => v_gen1, -- belongs to v_tracking1
            p_storage_backend => 'external_zstd', p_operation_nonce => 555003
        );
    EXCEPTION WHEN invalid_parameter_value THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'reserve_online_generation accepted a parent generation from a different tracking_id';
    END IF;

    -- 7. Successful reservation for an unrelated tracking_id (tracking3)
    -- proves the failures above were specific to their own bad input, not a
    -- side effect that broke the authority for everyone.
    SELECT * INTO v_row
    FROM public.flashback_internal_reserve_online_generation(
        p_tracking_id => v_tracking3, p_rel_oid => v_rel3,
        p_stream_id => v_stream, p_generation_no => 1,
        p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
        p_operation_nonce => 555004
    );
    v_other_gen := v_row.generation_id;
    v_other_snap := v_row.snapshot_id;
    IF v_other_gen IS NULL OR v_other_snap IS NULL THEN
        RAISE EXCEPTION 'reserve_online_generation for an independent tracking_id unexpectedly failed';
    END IF;

    -- 8. RBAC: neither new function is executable by PUBLIC/flashback_admin/
    -- pg_monitor, matching every other SnapshotStore internal primitive.
    IF has_function_privilege(
        'public', 'public.flashback_internal_reserve_online_generation'::regproc, 'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'RBAC Failed: PUBLIC must not EXECUTE flashback_internal_reserve_online_generation';
    END IF;
    IF has_function_privilege(
        'public', 'public.flashback_internal_snapshot_reserve'::regproc, 'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'RBAC Failed: PUBLIC must not EXECUTE flashback_internal_snapshot_reserve';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flashback_admin')
       AND has_function_privilege(
            'flashback_admin', 'public.flashback_internal_reserve_online_generation'::regproc, 'EXECUTE'
       )
    THEN
        RAISE EXCEPTION 'RBAC Failed: flashback_admin must not EXECUTE flashback_internal_reserve_online_generation';
    END IF;
    IF has_function_privilege(
        'pg_monitor', 'public.flashback_internal_reserve_online_generation'::regproc, 'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'RBAC Failed: pg_monitor must not EXECUTE flashback_internal_reserve_online_generation';
    END IF;

    -- Teardown, matching state_authority_transitions.sql's convention: these
    -- fixtures are intentionally left in a non-terminal authority state.
    PERFORM flashback_set_restore_in_progress(true);
    BEGIN
        DROP TABLE IF EXISTS public.it_online_res_1 CASCADE;
        DROP TABLE IF EXISTS public.it_online_res_2 CASCADE;
        DROP TABLE IF EXISTS public.it_online_res_3 CASCADE;
    EXCEPTION WHEN OTHERS THEN
        PERFORM flashback_set_restore_in_progress(false);
        RAISE;
    END;
    PERFORM flashback_set_restore_in_progress(false);
END;
$tv$;
