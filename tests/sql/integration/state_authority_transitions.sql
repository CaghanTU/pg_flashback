-- Centralized state authority: transitions, CAS, progress refuse, journal, constructors, RBAC.
DO $tv$
DECLARE
    v_db oid := (SELECT oid FROM pg_database WHERE datname = current_database());
    v_stream bigint;
    v_stream2 bigint;
    v_tracking bigint;
    v_rel oid;
    v_snap bigint;
    v_gen bigint;
    v_gen2 bigint;
    v_ret bigint;
    v_op bigint;
    v_raised boolean;
    v_msg text;
    v_state text;
    v_ok boolean;
    v_lsn pg_lsn := '0/ABCDEF'::pg_lsn;
    v_epoch bigint;
BEGIN
    CREATE TABLE IF NOT EXISTS public.it_state_auth (id int PRIMARY KEY);
    v_rel := 'public.it_state_auth'::regclass;

    -- Reverse-order lifecycle lock list must sort/distinct without error.
    PERFORM public.flashback_internal_lock_lifecycles(
        ARRAY[300::bigint, 100::bigint, 200::bigint, 100::bigint]
    );

    -- 1. Test Typed Constructors
    v_stream := public.flashback_internal_create_capture_stream(
        p_database_oid => v_db,
        p_initial_state => 'initializing',
        p_slot_name => 'it_auth_slot_1',
        p_plugin_name => 'pg_flashback_decoder'
    );
    IF v_stream IS NULL THEN
        RAISE EXCEPTION 'public.flashback_internal_create_capture_stream returned NULL';
    END IF;

    -- Legal capture stream transition: initializing -> active
    v_ok := public.flashback_internal_transition_capture_stream(
        v_stream, ARRAY['initializing'], 'active', NULL, '{}'::jsonb
    );
    IF NOT COALESCE(v_ok, false) THEN
        RAISE EXCEPTION 'initializing->active should mutate';
    END IF;

    -- Idempotent already-at-target retry
    IF public.flashback_internal_transition_capture_stream(
        v_stream, ARRAY['active'], 'active', NULL, '{}'::jsonb
    ) THEN
        RAISE EXCEPTION 'already-active retry must return false';
    END IF;

    -- Illegal backward capture stream transition: active -> initializing
    v_raised := false;
    BEGIN
        PERFORM public.flashback_internal_transition_capture_stream(
            v_stream, ARRAY['active'], 'initializing', NULL, '{}'::jsonb
        );
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT ILIKE '%illegal%' THEN
        RAISE EXCEPTION 'illegal backward stream transition must fail, got: %', v_msg;
    END IF;

    -- Monotonic stream progress advance
    PERFORM public.flashback_internal_advance_capture_stream_progress(
        v_stream, v_lsn, clock_timestamp(), v_lsn, v_lsn, NULL, NULL
    );

    -- Backward stream progress must fail
    v_raised := false;
    BEGIN
        PERFORM public.flashback_internal_advance_capture_stream_progress(
            v_stream, '0/ABCDE0'::pg_lsn, clock_timestamp(), NULL, NULL, NULL, NULL
        );
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'backward stream progress must raise exception';
    END IF;

    -- Transition active -> broken with reason
    IF NOT public.flashback_internal_transition_capture_stream(
        v_stream, ARRAY['active'], 'broken', 'state_auth_break', '{}'::jsonb
    ) THEN
        RAISE EXCEPTION 'active->broken should mutate';
    END IF;

    -- Conflicting broken reason retry must fail
    v_raised := false;
    BEGIN
        PERFORM public.flashback_internal_transition_capture_stream(
            v_stream, ARRAY['broken'], 'broken', 'different_reason', '{}'::jsonb
        );
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'conflicting broken stream retry must fail';
    END IF;

    -- Broken stream progress must soft-refuse
    IF public.flashback_internal_advance_capture_stream_progress(
        v_stream, '0/FFFFFF'::pg_lsn, clock_timestamp(),
        '0/FFFFFF'::pg_lsn, '0/FFFFFF'::pg_lsn, NULL, NULL
    ) THEN
        RAISE EXCEPTION 'broken stream progress must soft-refuse';
    END IF;

    -- Transition broken -> retired
    IF NOT public.flashback_internal_transition_capture_stream(
        v_stream, ARRAY['broken'], 'retired', 'state_auth_retire', '{}'::jsonb
    ) THEN
        RAISE EXCEPTION 'broken->retired should mutate';
    END IF;

    -- 2. Coverage Generations Authority & Transitions
    v_stream2 := public.flashback_internal_create_capture_stream(
        p_database_oid => v_db,
        p_initial_state => 'active',
        p_slot_name => 'it_auth_slot_2',
        p_plugin_name => 'pg_flashback_decoder'
    );

    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile
    ) VALUES (
        v_rel, 'public', 'it_state_auth', NULL, 'local_delta'
    ) RETURNING tracking_id INTO v_tracking;

    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn,
        schema_def, row_count, captured_at
    ) VALUES (
        v_rel, v_tracking, 'flashback.it_state_auth_snap', v_lsn,
        '{}'::jsonb, 0, clock_timestamp()
    ) RETURNING snapshot_id INTO v_snap;

    -- Use Authority Constructor for Generation
    v_gen := public.flashback_internal_create_coverage_generation(
        p_tracking_id => v_tracking,
        p_generation_no => 1,
        p_stream_id => v_stream2,
        p_boundary_kind => 'initial_track',
        p_rel_oid_at_boundary => v_rel,
        p_boundary_snapshot_id => v_snap,
        p_boundary_xid => txid_current(),
        p_boundary_marker => 'state-auth-build-1'
    );

    -- building -> active
    IF NOT public.flashback_internal_transition_coverage_generation(
        v_gen, v_tracking, 'building', 'active', 'activate',
        v_lsn, clock_timestamp(), v_lsn, clock_timestamp(),
        NULL, NULL, '{}'::jsonb
    ) THEN
        RAISE EXCEPTION 'building->active should mutate';
    END IF;

    -- Conflicting active retry (different boundary_lsn) must fail
    v_raised := false;
    BEGIN
        PERFORM public.flashback_internal_transition_coverage_generation(
            v_gen, v_tracking, 'active', 'active', 'retry',
            '0/999999'::pg_lsn, clock_timestamp(), v_lsn, clock_timestamp(),
            NULL, NULL, '{}'::jsonb
        );
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'conflicting active generation retry must fail-closed';
    END IF;

    -- Direct active -> retired is forbidden
    v_raised := false;
    BEGIN
        PERFORM public.flashback_internal_transition_coverage_generation(
            v_gen, v_tracking, 'active', 'retired', 'illegal',
            NULL, NULL, NULL, NULL, NULL, NULL, '{}'::jsonb
        );
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT ILIKE '%illegal%' THEN
        RAISE EXCEPTION 'active->retired must fail-closed, got: %', v_msg;
    END IF;

    -- active -> sealed
    IF NOT public.flashback_internal_transition_coverage_generation(
        v_gen, v_tracking, 'active', 'sealed', 'seal',
        NULL, NULL, NULL, NULL, '0/ABCDF0'::pg_lsn, clock_timestamp(), '{}'::jsonb
    ) THEN
        RAISE EXCEPTION 'active->sealed should mutate';
    END IF;

    -- sealed -> retired
    IF NOT public.flashback_internal_transition_coverage_generation(
        v_gen, v_tracking, 'sealed', 'retired', 'retire',
        NULL, NULL, NULL, NULL, NULL, NULL, '{}'::jsonb
    ) THEN
        RAISE EXCEPTION 'sealed->retired should mutate';
    END IF;

    -- 3. Generation Payload Retirement Intent Constructor & Transitions
    v_ret := public.flashback_internal_create_retirement_intent(
        p_generation_id => v_gen,
        p_tracking_id => v_tracking,
        p_reason => 'retention_policy',
        p_snapshot_id => v_snap,
        p_snapshot_table => 'flashback.it_state_auth_snap',
        p_snapshot_rel_oid => v_rel,
        p_snapshot_row_count => 0,
        p_snapshot_schema_fingerprint => md5('dummy_schema'),
        p_snapshot_storage_backend => 'heap_v1',
        p_snapshot_locator => jsonb_build_object('schema', 'flashback', 'relation', 'it_state_auth_snap'),
        p_expected_delta_rows => 0,
        p_expected_schema_rows => 0,
        p_first_delta_lsn => v_lsn,
        p_last_delta_lsn => v_lsn
    );

    IF NOT public.flashback_internal_transition_retirement(
        v_ret, 'retiring', 'removed', 0, 0
    ) THEN
        RAISE EXCEPTION 'retiring->removed transition should mutate';
    END IF;

    -- Illegal retirement transition (removed -> retiring)
    v_raised := false;
    BEGIN
        PERFORM public.flashback_internal_transition_retirement(
            v_ret, 'removed', 'retiring', 0, 0
        );
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'illegal backward retirement transition must fail';
    END IF;

    -- 4. Journal Command-Specific Graph Tests
    v_op := public.flashback_operation_begin(
        'recover', 'public.it_state_auth', v_tracking
    );
    -- recover: started -> applied_coverage_pending -> verified
    PERFORM public.flashback_operation_append_event(
        v_op, 'applied_coverage_pending', NULL, NULL, 'pending', jsonb_build_object('rows', 10)
    );
    PERFORM public.flashback_operation_append_event(
        v_op, 'verified', NULL, NULL, 'verified', jsonb_build_object('final', 'ok')
    );

    SELECT state INTO v_state
    FROM flashback.operation_current_state WHERE operation_id = v_op;
    IF v_state IS DISTINCT FROM 'verified' THEN
        RAISE EXCEPTION 'expected verified state, got %', v_state;
    END IF;

    -- Idempotent retry of identical verified terminal event
    PERFORM public.flashback_operation_append_event(
        v_op, 'verified', NULL, NULL, 'verified', jsonb_build_object('final', 'ok')
    );

    -- Conflicting retry of terminal event must fail
    v_raised := false;
    BEGIN
        PERFORM public.flashback_operation_append_event(
            v_op, 'verified', NULL, NULL, 'verified', jsonb_build_object('final', 'different')
        );
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'conflicting terminal retry must fail';
    END IF;

    -- 5. RBAC Enforcement Checks
    IF has_function_privilege(
        'public',
        'public.flashback_internal_transition_capture_stream'::regproc,
        'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'RBAC Check 1 Failed: PUBLIC must not EXECUTE transition_capture_stream';
    END IF;
    IF has_function_privilege(
        'public',
        'public.flashback_internal_create_capture_stream'::regproc,
        'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'RBAC Check 2 Failed: PUBLIC must not EXECUTE create_capture_stream';
    END IF;
    IF has_function_privilege(
        'pg_monitor',
        'public.flashback_internal_advance_capture_stream_progress'::regproc,
        'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'RBAC Check 3 Failed: pg_monitor must not EXECUTE stream progress';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flashback_admin')
       AND has_function_privilege(
            'flashback_admin',
            'public.flashback_internal_lock_database_stream'::regproc,
            'EXECUTE'
       )
    THEN
        RAISE EXCEPTION 'RBAC Check 4 Failed: flashback_admin must not EXECUTE internal locks';
    END IF;

    -- Teardown only: this fixture's generation/stream may be left in a
    -- non-active authority state by the transition tests above, which is the
    -- point of this file -- but that also means this DROP can hit the
    -- now-correctly-firing DDL hook's real capture requirements. Bypass
    -- capture for this cleanup DROP the same way pg_flashback's own internal
    -- DDL does.
    PERFORM flashback_set_restore_in_progress(true);
    BEGIN
        DROP TABLE IF EXISTS public.it_state_auth CASCADE;
    EXCEPTION WHEN OTHERS THEN
        PERFORM flashback_set_restore_in_progress(false);
        RAISE;
    END;
    PERFORM flashback_set_restore_in_progress(false);
END;
$tv$;
