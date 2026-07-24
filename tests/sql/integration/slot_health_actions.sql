-- Slot health / action-required reporting.
-- Health is a read-only projection; durable gaps remain authoritative.
DROP TABLE IF EXISTS public.it_health_slot CASCADE;

CREATE TABLE public.it_health_slot (id integer PRIMARY KEY, note text);
INSERT INTO public.it_health_slot VALUES (1, 'seed');

DO $setup$
DECLARE
    v_oid oid := 'public.it_health_slot'::regclass;
    v_snap text;
BEGIN
    v_snap := format('base_snapshot_%s', v_oid::oid::text);
    EXECUTE format(
        'CREATE TABLE flashback.%I AS TABLE %s',
        v_snap,
        v_oid::regclass
    );
    PERFORM flashback_own_payload_table(
        format('flashback.%I', v_snap)::regclass
    );
END;
$setup$;

DO $test$
DECLARE
    v_tracking_id bigint;
    v_stream_id bigint;
    v_snapshot_id bigint;
    v_health text;
    v_action text;
    v_oid oid;
    v_snap text;
    v_base timestamptz := clock_timestamp();
BEGIN
    -- A lost slot remains fail-closed and actionable.
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
END;
$test$;
