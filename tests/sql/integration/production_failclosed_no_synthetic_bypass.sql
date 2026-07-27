-- Adversarial: production restore/drain/guard must not trust synthetic streams.
-- Stay on capture_mode=wal; never treat trigger mode as a production path.
DO $tv$
DECLARE
    v_stream_id bigint;
    v_tracking_id bigint;
    v_snapshot_id bigint;
    v_ok boolean;
    v_err text;
    v_snap text;
BEGIN
    CREATE TABLE public.it_failclosed_synth (id int PRIMARY KEY);
    PERFORM set_config('pg_flashback.capture_mode', 'wal', true);

    -- 1) Configured physical slot missing + differently-named active stream:
    --    drain must fail closed.
    INSERT INTO flashback.capture_streams (
        database_oid, database_name, epoch_no, capture_mode, timeline_id,
        slot_name, plugin_name, state, valid_through_lsn,
        confirmed_flush_lsn, restart_lsn, activated_at
    ) VALUES (
        (SELECT oid FROM pg_database WHERE datname = current_database()),
        current_database(), 1, 'wal', 1,
        'not_the_configured_slot', 'pg_flashback', 'active', '0/1'::pg_lsn,
        '0/1'::pg_lsn, '0/1'::pg_lsn, clock_timestamp()
    ) RETURNING stream_id INTO v_stream_id;

    BEGIN
        PERFORM flashback_assert_relation_wal_drained(
            ARRAY['public.it_failclosed_synth'::regclass::oid]
        );
        RAISE EXCEPTION 'expected drain to fail when physical slot is missing';
    EXCEPTION WHEN others THEN
        GET STACKED DIAGNOSTICS v_err = MESSAGE_TEXT;
        IF position('unavailable' IN lower(v_err)) = 0 THEN
            RAISE EXCEPTION 'unexpected drain error: %', v_err;
        END IF;
    END;

    -- 2) Active stream named pg_flashback_test_*: under wal, names never relax
    --    the gate. configuration_guard is mode/state based (returns true while
    --    enabled+wal+active), but drain must still fail closed without a
    --    physical slot — no synthetic-name bypass into drained admission.
    UPDATE flashback.capture_streams
       SET slot_name = 'pg_flashback_test_adversarial'
     WHERE stream_id = v_stream_id;

    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table,
        recovery_profile, is_active
    ) VALUES (
        'public.it_failclosed_synth'::regclass,
        'public', 'it_failclosed_synth', NULL,
        'local_delta', true
    ) RETURNING tracking_id INTO v_tracking_id;

    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn,
        schema_def, row_count, captured_at
    ) VALUES (
        'public.it_failclosed_synth'::regclass, v_tracking_id,
        '', '0/100'::pg_lsn, '{}'::jsonb, 0, clock_timestamp()
    ) RETURNING snapshot_id INTO v_snapshot_id;
    v_snap := format('snap_%s_%s', v_tracking_id, v_snapshot_id);
    EXECUTE format(
        'CREATE TABLE flashback.%I AS TABLE public.it_failclosed_synth',
        v_snap
    );
    UPDATE flashback.snapshots
       SET snapshot_table = format('flashback.%I', v_snap)
     WHERE snapshot_id = v_snapshot_id;

    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, stream_id, recovery_profile, state,
        boundary_kind, rel_oid_at_boundary, boundary_snapshot_id,
        boundary_time, boundary_lsn, valid_through_lsn, activated_at
    ) VALUES (
        v_tracking_id, 1, v_stream_id, 'local_delta', 'active',
        'adversarial_fixture', 'public.it_failclosed_synth'::regclass,
        v_snapshot_id, clock_timestamp(), '0/100'::pg_lsn, '0/100'::pg_lsn,
        clock_timestamp()
    );

    v_ok := flashback_capture_configuration_guard('public.it_failclosed_synth'::regclass);
    IF NOT v_ok THEN
        RAISE EXCEPTION 'wal+enabled+active generation should pass configuration_guard (names are irrelevant)';
    END IF;

    BEGIN
        PERFORM flashback_assert_relation_wal_drained(
            ARRAY['public.it_failclosed_synth'::regclass::oid]
        );
        RAISE EXCEPTION 'synthetic test slot name must not bypass drain without a physical slot';
    EXCEPTION WHEN others THEN
        GET STACKED DIAGNOSTICS v_err = MESSAGE_TEXT;
        IF position('unavailable' IN lower(v_err)) = 0 THEN
            RAISE EXCEPTION 'unexpected drain error under synthetic name: %', v_err;
        END IF;
    END;

    -- 3) enabled=off: reconcile must break active stream (no name exemption).
    PERFORM set_config('pg_flashback.enabled', 'off', true);
    IF flashback_reconcile_capture_configuration() NOT IN ('capture_disabled', 'disabled') THEN
        RAISE EXCEPTION 'enabled=off reconcile did not break the adversarial stream';
    END IF;
    IF EXISTS (
        SELECT 1 FROM flashback.capture_streams
        WHERE stream_id = v_stream_id AND state = 'active'
    ) THEN
        RAISE EXCEPTION 'reconcile left test-named stream active under enabled=off';
    END IF;
    PERFORM set_config('pg_flashback.enabled', 'on', true);

    -- 4) Configured slot missing with stale active stream: ensure_active fails
    --    closed and breaks the stale epoch.
    INSERT INTO flashback.capture_streams (
        database_oid, database_name, epoch_no, capture_mode, timeline_id,
        slot_name, plugin_name, state, valid_through_lsn,
        confirmed_flush_lsn, restart_lsn, activated_at
    ) VALUES (
        (SELECT oid FROM pg_database WHERE datname = current_database()),
        current_database(), 2, 'wal', 1,
        'stale_slot_name', 'pg_flashback', 'active', '0/1'::pg_lsn,
        '0/1'::pg_lsn, '0/1'::pg_lsn, clock_timestamp()
    ) RETURNING stream_id INTO v_stream_id;

    IF flashback_ensure_active_wal_stream() IS NOT NULL THEN
        RAISE EXCEPTION 'ensure_active must not return a stream when physical slot is absent';
    END IF;
    IF EXISTS (
        SELECT 1 FROM flashback.capture_streams
        WHERE stream_id = v_stream_id AND state = 'active'
    ) THEN
        RAISE EXCEPTION 'stale active stream must be broken when physical slot is missing';
    END IF;

    -- 5) Internal cores are not executable by PUBLIC / normal roles.
    IF has_function_privilege(
           'public',
           'flashback_apply_decoded_wal_batch(bigint,pg_lsn,pg_lsn)',
           'EXECUTE'
       )
       OR has_function_privilege(
           'public',
           'flashback_bootstrap_local_delta_lifecycle_core(oid,bigint,"char",text)',
           'EXECUTE'
       )
       OR has_function_privilege(
           'public',
           'flashback_stage_local_delta_ddl_event(bigint,text,bigint,pg_lsn,jsonb,boolean,boolean)',
           'EXECUTE'
       )
    THEN
        RAISE EXCEPTION 'internal cores must not be EXECUTE-able by PUBLIC';
    END IF;

    -- Teardown only: this row's stream/generation is a deliberately
    -- adversarial fixture (no real physical slot), which is the whole point
    -- of the test above -- but that also means this DROP would otherwise hit
    -- the same "no real slot" wall via the now-correctly-firing DDL hook.
    -- The assertions this test cares about already ran directly against
    -- flashback_assert_relation_wal_drained/flashback_capture_configuration_guard;
    -- bypass capture for this cleanup DROP the same way pg_flashback's own
    -- internal DDL does.
    PERFORM flashback_set_restore_in_progress(true);
    BEGIN
        DROP TABLE public.it_failclosed_synth;
    EXCEPTION WHEN OTHERS THEN
        PERFORM flashback_set_restore_in_progress(false);
        RAISE;
    END;
    PERFORM flashback_set_restore_in_progress(false);
END;
$tv$;
