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
    -- Step 9: this reservation is parentless (p_parent_generation_id =>
    -- NULL above), so observing its boundary COMMIT now durably resolves
    -- boundary_lsn and moves it building -> capturing (wal_promote_core.sql)
    -- -- there is no active predecessor to keep absorbing writes instead,
    -- so it must become a write target itself the moment its boundary is
    -- known. Artifact eligibility is still a separate, later proof: the
    -- generation is 'capturing', not yet 'active'/recoverable, until
    -- publish+activate below.
    IF v_gen.state IS DISTINCT FROM 'capturing'
       OR v_gen.boundary_lsn IS DISTINCT FROM v_lsn
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
    -- Publication proves the artifact, not the generation; this parentless
    -- reservation is 'capturing' (see above), not 'building', by this point.
    IF v_gen.state IS DISTINCT FROM 'capturing' THEN
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

    -- Operator projections must describe the immutable backend actually bound
    -- to this generation.  They must not infer it from the current cluster GUC
    -- (which may select a different backend for the next generation).
    IF NOT EXISTS (
        SELECT 1
        FROM public.flashback_health() h
        WHERE h.tracking_id = v_tracking
          AND h.snapshot_id = v_reserved.snapshot_id
          AND h.snapshot_storage_backend = 'external_zstd'
          AND h.snapshot_payload_state = 'available'
          AND h.snapshot_health_status = 'not_yet_audited'
          AND h.snapshot_compressed_bytes = 1
          AND h.snapshot_uncompressed_bytes = 1
    ) THEN
        RAISE EXCEPTION 'health projection did not expose the active external snapshot identity/status';
    END IF;
    IF public.flashback_status_snapshot('public.it_external_activation')
           #>> '{tables,0,snapshot_storage_backend}'
       IS DISTINCT FROM 'external_zstd'
    THEN
        RAISE EXCEPTION 'status projection did not expose the active external backend';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM public.flashback_doctor()
        WHERE check_name = 'snapshot_storage_backend'
    ) OR NOT EXISTS (
        SELECT 1 FROM public.flashback_doctor()
        WHERE check_name = 'external_snapshot_root'
    ) THEN
        RAISE EXCEPTION 'doctor omitted SnapshotStore configuration checks';
    END IF;

    PERFORM public.flashback_set_restore_in_progress(true);
    DROP TABLE public.it_external_activation;
    PERFORM public.flashback_set_restore_in_progress(false);
END;
$test$;

-- An online successor must inherit every post-boundary fact that the WAL
-- worker promoted while the predecessor was still the only active
-- generation.  This is the exact concurrency window between boundary bind
-- and immutable artifact publication.
DO $test$
DECLARE
    v_boot record;
    v_db oid := (SELECT oid FROM pg_database WHERE datname = current_database());
    v_tracking bigint;
    v_parent bigint;
    v_stream bigint;
    v_rel oid;
    v_reserved record;
    v_bound record;
    v_snap flashback.snapshots%ROWTYPE;
    v_locator jsonb;
    v_parent_schema_version bigint;
BEGIN
    CREATE TABLE public.it_external_handoff (id integer PRIMARY KEY, note text);
    INSERT INTO public.it_external_handoff VALUES (1, 'boundary');
    v_rel := 'public.it_external_handoff'::regclass;
    SELECT stream_id INTO STRICT v_stream
    FROM flashback.capture_streams
    WHERE database_oid = v_db AND state = 'active';
    SELECT * INTO STRICT v_boot
    FROM public.flashback_bootstrap_local_delta_lifecycle_core(
        v_rel, v_stream, 'd'::"char", NULL
    );
    v_tracking := v_boot.out_tracking_id;
    v_parent := v_boot.out_generation_id;
    PERFORM public.flashback_test_inject_commit(
        v_tracking, '0/A82000'::pg_lsn, clock_timestamp(),
        v_boot.out_boundary_xid, '[]'::jsonb
    );

    SELECT * INTO v_reserved
    FROM public.flashback_internal_reserve_online_generation(
        p_tracking_id => v_tracking, p_rel_oid => v_rel,
        p_stream_id => v_stream, p_generation_no => 2,
        p_parent_generation_id => v_parent,
        p_storage_backend => 'external_zstd', p_operation_nonce => 881002
    );
    SELECT * INTO v_bound
    FROM public.flashback_internal_bind_online_boundary(
        v_reserved.generation_id, v_tracking, v_reserved.snapshot_id, v_rel
    );
    PERFORM public.flashback_test_inject_commit(
        v_tracking, '0/A83000'::pg_lsn, clock_timestamp(),
        v_bound.boundary_xid, '[]'::jsonb
    );

    -- Both facts are deliberately consumed before external activation and
    -- therefore initially land on the still-active predecessor.
    PERFORM public.flashback_test_inject_commit(
        v_tracking, '0/A84000'::pg_lsn, clock_timestamp(), 881003,
        jsonb_build_array(jsonb_build_object(
            'op', 'UPDATE',
            'old', '{"id":1,"note":"boundary"}'::jsonb,
            'new', '{"id":1,"note":"after-boundary"}'::jsonb
        ))
    );
    PERFORM public.flashback_test_inject_ddl_commit(
        v_tracking, '0/A85000'::pg_lsn, clock_timestamp(), 881004,
        'ALTER', public.flashback_collect_schema_def(v_rel)
    );
    IF NOT EXISTS (
        SELECT 1 FROM flashback.delta_log
        WHERE tracking_id = v_tracking AND generation_id = v_parent
          AND commit_lsn = '0/A84000'::pg_lsn
    ) THEN
        RAISE EXCEPTION 'fixture did not place post-boundary DML on predecessor';
    END IF;
    SELECT schema_version INTO v_parent_schema_version
    FROM flashback.schema_versions
    WHERE tracking_id = v_tracking AND generation_id = v_parent
      AND commit_lsn = '0/A85000'::pg_lsn;
    IF v_parent_schema_version IS NULL THEN
        RAISE EXCEPTION 'fixture did not place post-boundary DDL on predecessor';
    END IF;

    SELECT * INTO v_snap FROM flashback.snapshots
    WHERE snapshot_id = v_reserved.snapshot_id;
    v_locator := jsonb_build_object(
        'system_identifier', (pg_control_system()).system_identifier::text,
        'database_oid', v_db::text,
        'tracking_id', v_tracking::text,
        'snapshot_id', v_reserved.snapshot_id::text,
        'nonce', '881002'
    );
    PERFORM public.flashback_internal_publish_external_snapshot(
        v_reserved.snapshot_id, v_tracking, v_reserved.generation_id, 881002,
        v_locator, 1, 'zstd', 1, 1, 1, repeat('b', 64),
        v_snap.external_column_contract,
        public.flashback_sha256(v_snap.schema_def::text)
    );
    PERFORM public.flashback_internal_activate_external_generation(
        v_reserved.generation_id, v_tracking, v_reserved.snapshot_id
    );

    IF EXISTS (
        SELECT 1 FROM flashback.delta_log
        WHERE tracking_id = v_tracking AND generation_id = v_parent
          AND commit_lsn > '0/A83000'::pg_lsn
    ) OR NOT EXISTS (
        SELECT 1 FROM flashback.delta_log
        WHERE tracking_id = v_tracking
          AND generation_id = v_reserved.generation_id
          AND commit_lsn = '0/A84000'::pg_lsn
    ) THEN
        RAISE EXCEPTION 'post-boundary DML was not handed to external successor';
    END IF;
    IF EXISTS (
        SELECT 1 FROM flashback.schema_versions
        WHERE tracking_id = v_tracking AND generation_id = v_parent
          AND schema_version = v_parent_schema_version
    ) OR NOT EXISTS (
        SELECT 1 FROM flashback.schema_versions
        WHERE tracking_id = v_tracking
          AND generation_id = v_reserved.generation_id
          AND schema_version = v_parent_schema_version
          AND commit_lsn = '0/A85000'::pg_lsn
    ) THEN
        RAISE EXCEPTION 'post-boundary schema epoch was not handed to external successor';
    END IF;
    IF (SELECT schema_version FROM flashback.tracked_tables
        WHERE tracking_id = v_tracking) IS DISTINCT FROM v_parent_schema_version
    THEN
        RAISE EXCEPTION 'tracked lifecycle schema epoch diverged during handoff';
    END IF;

    PERFORM public.flashback_set_restore_in_progress(true);
    DROP TABLE public.it_external_handoff;
    PERFORM public.flashback_set_restore_in_progress(false);
END;
$test$;
