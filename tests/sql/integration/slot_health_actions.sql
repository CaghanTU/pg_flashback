-- Slot health / action-required reporting.
-- Health is a read-only projection; durable gaps remain authoritative.
DROP TABLE IF EXISTS public.it_health_slot CASCADE;
DROP TABLE IF EXISTS public.it_health_backup CASCADE;

CREATE TABLE public.it_health_slot (id integer PRIMARY KEY, note text);
INSERT INTO public.it_health_slot VALUES (1, 'seed');
CREATE TABLE public.it_health_backup (id integer PRIMARY KEY, note text);
INSERT INTO public.it_health_backup VALUES (1, 'seed');

DO $setup$
DECLARE
    v_oid oid;
    v_snap text;
BEGIN
    FOREACH v_oid IN ARRAY ARRAY[
        'public.it_health_slot'::regclass,
        'public.it_health_backup'::regclass
    ]
    LOOP
        v_snap := format('base_snapshot_%s', v_oid::oid::text);
        EXECUTE format(
            'CREATE TABLE flashback.%I AS TABLE %s',
            v_snap,
            v_oid::regclass
        );
        PERFORM flashback_own_payload_table(
            format('flashback.%I', v_snap)::regclass
        );
    END LOOP;
END;
$setup$;

DO $test$
DECLARE
    v_tracking_id bigint;
    v_stream_id bigint;
    v_snapshot_id bigint;
    v_generation_id bigint;
    v_health text;
    v_action text;
    v_oid oid;
    v_snap text;
    v_base timestamptz := clock_timestamp();
BEGIN
    -- Scenario A: lost slot remains fail-closed and actionable.
    v_oid := 'public.it_health_slot'::regclass;
    v_snap := format('flashback.base_snapshot_%s', v_oid::text);

    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table,
        recovery_profile, is_active
    ) VALUES (
        v_oid, 'public', 'it_health_slot', v_snap, 'local_delta', true
    ) RETURNING tracking_id INTO v_tracking_id;

    INSERT INTO flashback.capture_streams (
        database_oid, database_name, epoch_no, capture_mode, timeline_id,
        slot_name, plugin_name, state, valid_through_time, valid_through_lsn,
        confirmed_flush_lsn, restart_lsn, activated_at
    ) VALUES (
        (SELECT oid FROM pg_database WHERE datname = current_database()),
        current_database(), 1, 'wal', flashback_current_timeline_id(),
        'pg_flashback_it_health_slot', 'pg_flashback', 'active',
        v_base, '0/1000', '0/1000', '0/1000', clock_timestamp()
    ) RETURNING stream_id INTO v_stream_id;

    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn,
        schema_def, row_count, captured_at
    ) VALUES (
        v_oid, v_tracking_id, v_snap, '0/1000', '{}'::jsonb, 1, v_base
    ) RETURNING snapshot_id INTO v_snapshot_id;

    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, stream_id, recovery_profile, state,
        boundary_kind, rel_oid_at_boundary, boundary_snapshot_id,
        boundary_time, boundary_lsn, valid_through_time, valid_through_lsn,
        activated_at
    ) VALUES (
        v_tracking_id, 1, v_stream_id, 'local_delta', 'active',
        'health_fixture', v_oid, v_snapshot_id,
        v_base, '0/1000', v_base, '0/1000', clock_timestamp()
    );

    PERFORM flashback_mark_capture_stream_broken(
        v_stream_id, 'replication_slot_missing', '{}'::jsonb
    );
    SELECT health, recommended_action
      INTO v_health, v_action
    FROM flashback_health()
    WHERE tracking_id = v_tracking_id;
    IF v_health IS DISTINCT FROM 'slot_lost' THEN
        RAISE EXCEPTION 'lost slot did not project slot_lost health: %', v_health;
    END IF;
    IF v_action IS NULL OR v_action = 'none' THEN
        RAISE EXCEPTION 'lost slot health lacked recommended_action';
    END IF;

    -- Scenario B: durable post-restore gap projects backup_reanchor_required.
    v_oid := 'public.it_health_backup'::regclass;
    v_snap := format('flashback.base_snapshot_%s', v_oid::text);

    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table,
        recovery_profile, is_active
    ) VALUES (
        v_oid, 'public', 'it_health_backup', v_snap, 'backup', true
    ) RETURNING tracking_id INTO v_tracking_id;

    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, recovery_profile, state,
        state_reason, boundary_kind, rel_oid_at_boundary,
        boundary_xid, boundary_marker
    ) VALUES (
        v_tracking_id, 1, 'backup', 'building',
        'post_restore_unanchored', 'post_restore', v_oid,
        42, format('post-restore-backup:%s:42:1', v_tracking_id)
    ) RETURNING generation_id INTO v_generation_id;

    INSERT INTO flashback.coverage_gaps (
        tracking_id, source_generation_id, reason,
        gap_start_lsn, lower_bound_inclusive, details
    ) VALUES (
        v_tracking_id, v_generation_id, 'post_restore_unanchored',
        '0/2000', false,
        jsonb_build_object('required_next_full_backup', true)
    );

    SELECT health, recommended_action
      INTO v_health, v_action
    FROM flashback_health()
    WHERE tracking_id = v_tracking_id;
    IF v_health IS DISTINCT FROM 'backup_reanchor_required' THEN
        RAISE EXCEPTION 'post-restore gap did not project backup_reanchor_required: %',
            v_health;
    END IF;
    IF v_action IS DISTINCT FROM 'take_and_verify_fresh_full_after_marker' THEN
        RAISE EXCEPTION 'unexpected recommended_action: %', v_action;
    END IF;

    BEGIN
        PERFORM pg_notify(
            'pg_flashback_action_required',
            jsonb_build_object(
                'tracking_id', v_tracking_id,
                'action', 'backup_reanchor_required',
                'reason', 'test_rollback'
            )::text
        );
        RAISE EXCEPTION 'rollback notify';
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;
    IF NOT EXISTS (
        SELECT 1 FROM flashback_health()
        WHERE tracking_id = v_tracking_id
          AND health = 'backup_reanchor_required'
    ) THEN
        RAISE EXCEPTION 'health lost durable action after notify rollback';
    END IF;

    PERFORM pg_notify(
        'pg_flashback_action_required',
        jsonb_build_object(
            'tracking_id', v_tracking_id,
            'action', 'backup_reanchor_required',
            'reason', 'test_commit'
        )::text
    );
END;
$test$;
