-- external_zstd eligibility is a three-proof sequence:
-- marker COMMIT -> exact boundary only; artifact publication -> available;
-- explicit activation -> coverage. No earlier phase may imply the next.
DO $test$
DECLARE
    v_db oid := (SELECT oid FROM pg_database WHERE datname = current_database());
    v_stream bigint;
    v_tracking bigint;
    v_rel oid;
    v_reserved record;
    v_bound record;
    v_gen flashback.coverage_generations%ROWTYPE;
    v_snap flashback.snapshots%ROWTYPE;
    v_lsn pg_lsn := '0/A81000'::pg_lsn;
    v_locator jsonb;
    v_schema_hash text;
    v_raised boolean := false;
BEGIN
    CREATE TABLE public.it_external_activation (id integer PRIMARY KEY, note text);
    v_rel := 'public.it_external_activation'::regclass;
    v_stream := public.flashback_internal_create_capture_stream(
        p_database_oid => v_db,
        p_initial_state => 'active',
        p_slot_name => 'it_external_activation_slot',
        p_plugin_name => 'pg_flashback_decoder'
    );
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile
    ) VALUES (v_rel, 'public', 'it_external_activation', NULL, 'local_delta')
    RETURNING tracking_id INTO v_tracking;

    SELECT * INTO v_reserved
    FROM public.flashback_internal_reserve_online_generation(
        p_tracking_id => v_tracking, p_rel_oid => v_rel,
        p_stream_id => v_stream, p_generation_no => 1,
        p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
        p_operation_nonce => 881001
    );
    SELECT * INTO v_bound
    FROM public.flashback_internal_bind_online_boundary(
        v_reserved.generation_id, v_tracking, v_reserved.snapshot_id, v_rel
    );

    PERFORM public.flashback_test_inject_commit(
        v_tracking, v_lsn, clock_timestamp(), v_bound.boundary_xid, '[]'::jsonb
    );
    SELECT * INTO v_gen FROM flashback.coverage_generations
    WHERE generation_id = v_reserved.generation_id;
    SELECT * INTO v_snap FROM flashback.snapshots
    WHERE snapshot_id = v_reserved.snapshot_id;
    IF v_gen.state IS DISTINCT FROM 'building'
       OR v_gen.boundary_lsn IS NOT NULL
       OR v_snap.payload_state IS DISTINCT FROM 'creating'
       OR v_snap.snapshot_lsn IS DISTINCT FROM v_lsn
    THEN
        RAISE EXCEPTION 'marker observation crossed artifact eligibility: gen=%, gen_lsn=%, snapshot=%, snapshot_lsn=%',
            v_gen.state, v_gen.boundary_lsn, v_snap.payload_state, v_snap.snapshot_lsn;
    END IF;

    BEGIN
        PERFORM public.flashback_internal_activate_external_generation(
            v_reserved.generation_id, v_tracking, v_reserved.snapshot_id
        );
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'external generation activated before artifact availability';
    END IF;

    v_locator := jsonb_build_object(
        'system_identifier', (pg_control_system()).system_identifier::text,
        'database_oid', v_db::text,
        'tracking_id', v_tracking::text,
        'snapshot_id', v_reserved.snapshot_id::text,
        'nonce', '881001'
    );
    v_schema_hash := public.flashback_sha256(v_snap.schema_def::text);
    IF NOT public.flashback_internal_publish_external_snapshot(
        v_reserved.snapshot_id, v_tracking, v_reserved.generation_id, 881001,
        v_locator, 0, 'zstd', 1, 1, 1, repeat('a', 64),
        v_snap.external_column_contract, v_schema_hash
    ) THEN
        RAISE EXCEPTION 'first external publication reported idempotent retry';
    END IF;

    SELECT * INTO v_gen FROM flashback.coverage_generations
    WHERE generation_id = v_reserved.generation_id;
    IF v_gen.state IS DISTINCT FROM 'building' THEN
        RAISE EXCEPTION 'artifact publication implicitly activated generation: %', v_gen.state;
    END IF;
    IF NOT public.flashback_internal_activate_external_generation(
        v_reserved.generation_id, v_tracking, v_reserved.snapshot_id
    ) THEN
        RAISE EXCEPTION 'first eligible external activation reported idempotent retry';
    END IF;
    IF public.flashback_internal_activate_external_generation(
        v_reserved.generation_id, v_tracking, v_reserved.snapshot_id
    ) THEN
        RAISE EXCEPTION 'second external activation was not idempotent';
    END IF;
    IF public.flashback_internal_publish_external_snapshot(
        v_reserved.snapshot_id, v_tracking, v_reserved.generation_id, 881001,
        v_locator, 0, 'zstd', 1, 1, 1, repeat('a', 64),
        v_snap.external_column_contract, v_schema_hash
    ) THEN
        RAISE EXCEPTION 'second external publication was not idempotent';
    END IF;

    SELECT * INTO v_gen FROM flashback.coverage_generations
    WHERE generation_id = v_reserved.generation_id;
    IF v_gen.state IS DISTINCT FROM 'active'
       OR v_gen.boundary_lsn IS DISTINCT FROM v_lsn
       OR v_gen.valid_through_lsn IS DISTINCT FROM v_lsn
    THEN
        RAISE EXCEPTION 'external activation mismatch: state=%, boundary=%, valid=%',
            v_gen.state, v_gen.boundary_lsn, v_gen.valid_through_lsn;
    END IF;

    PERFORM public.flashback_set_restore_in_progress(true);
    DROP TABLE public.it_external_activation;
    PERFORM public.flashback_set_restore_in_progress(false);
END;
$test$;
