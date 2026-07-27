-- pg_test-only WAL injection seam. Included only when feature pg_test / test
-- builds load this file. Production package SQL must not contain these names.

-- Test-only stream opener: creates an active capture_streams row without a
-- physical replication slot. Production restore never trusts this path.
CREATE OR REPLACE FUNCTION flashback_internal_open_capture_stream(
    p_slot_name text,
    p_plugin text,
    p_confirmed_flush_lsn pg_lsn,
    p_restart_lsn pg_lsn
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_stream flashback.capture_streams%ROWTYPE;
    v_timeline bigint;
    v_epoch bigint;
    v_stream_id bigint;
BEGIN
    IF p_slot_name IS NULL OR btrim(p_slot_name) = '' THEN
        RAISE EXCEPTION 'flashback_internal_open_capture_stream: slot_name required';
    END IF;
    IF p_plugin IS NULL OR btrim(p_plugin) = '' THEN
        RAISE EXCEPTION 'flashback_internal_open_capture_stream: plugin required';
    END IF;
    IF p_confirmed_flush_lsn IS NULL THEN
        RAISE EXCEPTION 'flashback_internal_open_capture_stream: confirmed_flush_lsn required';
    END IF;

    PERFORM flashback_internal_lock_database_stream(
        (SELECT oid FROM pg_database WHERE datname = current_database())
    );
    SELECT timeline_id::bigint INTO v_timeline FROM pg_control_checkpoint();

    SELECT * INTO v_stream
    FROM flashback.capture_streams
    WHERE database_oid = (SELECT oid FROM pg_database WHERE datname = current_database())
      AND state = 'active'
    FOR UPDATE;

    IF FOUND THEN
        IF v_stream.slot_name IS NOT DISTINCT FROM p_slot_name
           AND v_stream.plugin_name IS NOT DISTINCT FROM p_plugin
           AND v_stream.capture_mode = 'wal'
           AND v_stream.timeline_id IS NOT DISTINCT FROM v_timeline
        THEN
            RETURN v_stream.stream_id;
        END IF;
        RAISE EXCEPTION 'flashback_internal_open_capture_stream: active stream % conflicts with requested slot %',
            v_stream.stream_id, p_slot_name
            USING HINT = 'Break or retire the active capture stream before opening a different slot epoch.';
    END IF;

    SELECT COALESCE(max(epoch_no), 0) + 1 INTO v_epoch
    FROM flashback.capture_streams
    WHERE database_oid = (SELECT oid FROM pg_database WHERE datname = current_database());

    v_stream_id := flashback_internal_create_capture_stream(
        p_database_oid => (SELECT oid FROM pg_database WHERE datname = current_database()),
        p_initial_state => 'active',
        p_epoch_no => v_epoch,
        p_timeline_id => v_timeline,
        p_slot_name => p_slot_name,
        p_plugin_name => p_plugin,
        p_confirmed_flush_lsn => p_confirmed_flush_lsn,
        p_restart_lsn => COALESCE(p_restart_lsn, p_confirmed_flush_lsn),
        -- no_physical_slot marks this stream as backed by no real physical
        -- slot: flashback_internal_prepare_destructive_ddl checks it to skip
        -- a real pg_logical_slot_peek_changes call it could never satisfy
        -- (Postgres refuses to create a logical slot in a transaction that
        -- has already performed writes, which every pg_test script has by
        -- the time it reaches this call -- there is no way to give this
        -- stream a real slot from here). Production streams
        -- (flashback_track's own admission path) never set this.
        p_details => jsonb_build_object(
            'initial_confirmed_flush_lsn', p_confirmed_flush_lsn,
            'no_physical_slot', true
        )
    );

    RETURN v_stream_id;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_test_bootstrap_lifecycle(p_target_table text)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_rel_oid oid;
    v_boot record;
    v_stream_id bigint;
    v_slot_name text;
    v_boundary_lsn pg_lsn;
    v_commit_time_us bigint;
    v_ord bigint := 1;
BEGIN
    v_rel_oid := to_regclass(p_target_table);
    IF v_rel_oid IS NULL THEN
        RAISE EXCEPTION 'flashback_test_bootstrap_lifecycle: table % does not exist', p_target_table;
    END IF;

    -- Ephemeral metadata stream for pg_test. Not a physical slot and not a
    -- production bypass: public restore still requires physical admission
    -- (flashback_ensure_active_wal_stream/flashback_restore_lsn reject this
    -- synthetic capture_streams row regardless of what follows here).
    v_slot_name := format(
        'pg_flashback_test_%s',
        (SELECT oid FROM pg_database WHERE datname = current_database())::text
    );
    v_stream_id := flashback_internal_open_capture_stream(
        v_slot_name,
        'pg_flashback',
        '0/1'::pg_lsn,
        '0/1'::pg_lsn
    );

    SELECT * INTO STRICT v_boot
    FROM flashback_bootstrap_local_delta_lifecycle_core(
        v_rel_oid,
        v_stream_id,
        'd'::"char",
        NULL
    );

    v_boundary_lsn := (
        '0/' || to_hex(4096 + (v_boot.out_tracking_id::integer * 64))
    )::pg_lsn;
    v_commit_time_us := (
        EXTRACT(EPOCH FROM (clock_timestamp() - TIMESTAMPTZ '2000-01-01 00:00:00+00'))
        * 1000000
    )::bigint;

    DROP TABLE IF EXISTS pg_temp._fb_wal_batch;
    CREATE TEMP TABLE _fb_wal_batch (
        change_lsn pg_lsn,
        source_xid bigint,
        data jsonb,
        ord bigint
    ) ON COMMIT DROP;

    INSERT INTO _fb_wal_batch(change_lsn, source_xid, data, ord)
    VALUES (
        v_boundary_lsn,
        v_boot.out_boundary_xid,
        jsonb_build_object(
            'commit', v_boot.out_boundary_xid,
            'lsn', v_boundary_lsn::text,
            'commit_time', v_commit_time_us
        ),
        v_ord
    );

    PERFORM flashback_internal_lock_lifecycle(v_boot.out_tracking_id);
    PERFORM flashback_apply_decoded_wal_batch(v_stream_id, NULL, NULL);

    IF NOT EXISTS (
        SELECT 1 FROM flashback.coverage_generations
        WHERE generation_id = v_boot.out_generation_id
          AND state = 'active'
          AND boundary_lsn = v_boundary_lsn
    ) THEN
        RAISE EXCEPTION 'flashback_test_bootstrap_lifecycle: boundary COMMIT did not activate generation %',
            v_boot.out_generation_id;
    END IF;

    RETURN jsonb_build_object(
        'tracking_id', v_boot.out_tracking_id,
        'generation_id', v_boot.out_generation_id,
        'stream_id', v_boot.out_stream_id,
        'boundary_xid', v_boot.out_boundary_xid,
        'boundary_lsn', v_boundary_lsn,
        'snapshot_id', v_boot.out_snapshot_id,
        'provisional_lsn', v_boot.out_provisional_lsn,
        'rel_oid', v_rel_oid,
        'schema_name', v_boot.out_schema_name,
        'table_name', v_boot.out_table_name
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_test_inject_commit(
    p_tracking_id bigint,
    p_commit_lsn pg_lsn,
    p_commit_time timestamptz,
    p_source_xid bigint,
    p_events jsonb
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_tt record;
    v_gen record;
    v_commit_time_us bigint;
    v_ord bigint := 0;
    ev jsonb;
    v_inserted bigint;
BEGIN
    IF p_tracking_id IS NULL OR p_commit_lsn IS NULL OR p_source_xid IS NULL THEN
        RAISE EXCEPTION 'flashback_test_inject_commit: tracking_id, commit_lsn, and source_xid are required';
    END IF;
    IF p_events IS NULL OR jsonb_typeof(p_events) <> 'array' THEN
        RAISE EXCEPTION 'flashback_test_inject_commit: p_events must be a jsonb array';
    END IF;

    SELECT tt.tracking_id, tt.rel_oid, tt.schema_name, tt.table_name, tt.is_active
      INTO v_tt
    FROM flashback.tracked_tables tt
    WHERE tt.tracking_id = p_tracking_id;
    IF v_tt.tracking_id IS NULL THEN
        RAISE EXCEPTION 'flashback_test_inject_commit: tracking_id % not found', p_tracking_id;
    END IF;

    SELECT cg.*
      INTO v_gen
    FROM flashback.coverage_generations cg
    WHERE cg.tracking_id = p_tracking_id
      AND cg.state = 'active'
    ORDER BY cg.generation_id DESC
    LIMIT 1;

    IF v_gen.generation_id IS NULL THEN
        SELECT cg.*
          INTO v_gen
        FROM flashback.coverage_generations cg
        WHERE cg.tracking_id = p_tracking_id
          AND cg.state = 'building'
          AND cg.boundary_xid = p_source_xid
        ORDER BY cg.generation_id DESC
        LIMIT 1;
        IF v_gen.generation_id IS NULL THEN
            RAISE EXCEPTION 'flashback_test_inject_commit: no active/building generation for tracking_id %',
                p_tracking_id;
        END IF;
        IF COALESCE(jsonb_array_length(p_events), 0) <> 0 THEN
            RAISE EXCEPTION 'flashback_test_inject_commit: building generation % only accepts boundary COMMIT (empty events)',
                v_gen.generation_id;
        END IF;
    ELSIF NOT v_tt.is_active THEN
        RAISE EXCEPTION 'flashback_test_inject_commit: tracking_id % is inactive', p_tracking_id;
    END IF;

    PERFORM flashback_internal_lock_lifecycle(p_tracking_id);

    IF EXISTS (
        SELECT 1
        FROM flashback.capture_commits cc
        WHERE cc.stream_id = v_gen.stream_id
          AND cc.commit_lsn = p_commit_lsn
    ) THEN
        RETURN 0;
    END IF;

    v_commit_time_us := (
        EXTRACT(EPOCH FROM (
            COALESCE(p_commit_time, clock_timestamp())
            - TIMESTAMPTZ '2000-01-01 00:00:00+00'
        )) * 1000000
    )::bigint;

    DROP TABLE IF EXISTS pg_temp._fb_wal_batch;
    CREATE TEMP TABLE _fb_wal_batch (
        change_lsn pg_lsn,
        source_xid bigint,
        data jsonb,
        ord bigint
    ) ON COMMIT DROP;

    FOR ev IN SELECT value FROM jsonb_array_elements(p_events)
    LOOP
        IF COALESCE(ev->>'op', '') NOT IN ('INSERT', 'UPDATE', 'DELETE') THEN
            RAISE EXCEPTION 'flashback_test_inject_commit: unsupported op %', ev->>'op';
        END IF;
        v_ord := v_ord + 1;
        INSERT INTO _fb_wal_batch(change_lsn, source_xid, data, ord)
        VALUES (
            p_commit_lsn,
            p_source_xid,
            jsonb_strip_nulls(jsonb_build_object(
                'op', ev->>'op',
                'schema', v_tt.schema_name,
                'table', v_tt.table_name,
                'oid', v_gen.rel_oid_at_boundary,
                'xid', p_source_xid,
                'old', ev->'old',
                'new', ev->'new'
            )),
            v_ord
        );
    END LOOP;

    v_ord := v_ord + 1;
    INSERT INTO _fb_wal_batch(change_lsn, source_xid, data, ord)
    VALUES (
        p_commit_lsn,
        p_source_xid,
        jsonb_build_object(
            'commit', p_source_xid,
            'lsn', p_commit_lsn::text,
            'commit_time', v_commit_time_us
        ),
        v_ord
    );

    v_inserted := flashback_apply_decoded_wal_batch(v_gen.stream_id, NULL, NULL);
    RETURN v_inserted;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_test_resolve_post_restore_boundary(
    p_tracking_id bigint,
    p_commit_lsn pg_lsn DEFAULT NULL
)
RETURNS pg_lsn
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_building record;
    v_commit_lsn pg_lsn;
    v_commit_time_us bigint;
BEGIN
    SELECT cg.generation_id, cg.stream_id, cg.boundary_xid
      INTO v_building
    FROM flashback.coverage_generations cg
    WHERE cg.tracking_id = p_tracking_id
      AND cg.state = 'building'
    ORDER BY cg.generation_id DESC
    LIMIT 1;

    IF v_building.generation_id IS NULL THEN
        RETURN NULL;
    END IF;

    IF p_commit_lsn IS NULL THEN
        RAISE EXCEPTION 'flashback_test_resolve_post_restore_boundary: commit_lsn is required';
    END IF;
    v_commit_lsn := p_commit_lsn;

    v_commit_time_us := (
        EXTRACT(EPOCH FROM (clock_timestamp() - TIMESTAMPTZ '2000-01-01 00:00:00+00'))
        * 1000000
    )::bigint;

    PERFORM flashback_internal_lock_lifecycle(p_tracking_id);

    DROP TABLE IF EXISTS pg_temp._fb_wal_batch;
    CREATE TEMP TABLE _fb_wal_batch (
        change_lsn pg_lsn,
        source_xid bigint,
        data jsonb,
        ord bigint
    ) ON COMMIT DROP;

    INSERT INTO _fb_wal_batch(change_lsn, source_xid, data, ord)
    VALUES (
        v_commit_lsn,
        v_building.boundary_xid,
        jsonb_build_object(
            'commit', v_building.boundary_xid,
            'lsn', v_commit_lsn::text,
            'commit_time', v_commit_time_us
        ),
        1
    );

    PERFORM flashback_apply_decoded_wal_batch(v_building.stream_id, NULL, NULL);

    IF NOT EXISTS (
        SELECT 1 FROM flashback.coverage_generations
        WHERE generation_id = v_building.generation_id
          AND state = 'active'
    ) THEN
        RAISE EXCEPTION
            'flashback_test_resolve_post_restore_boundary: generation % did not activate',
            v_building.generation_id;
    END IF;

    RETURN v_commit_lsn;
END;
$$;

-- DDL inside DO/SPI is not PROCESS_UTILITY_TOPLEVEL. Stage through the shared
-- product DDL core, then promote with an explicit COMMIT via inject_commit.
CREATE OR REPLACE FUNCTION flashback_test_inject_ddl_commit(
    p_tracking_id bigint,
    p_commit_lsn pg_lsn,
    p_commit_time timestamptz,
    p_source_xid bigint,
    p_event_type text,
    p_ddl_info jsonb DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_event_type text := upper(btrim(p_event_type));
    v_tt record;
BEGIN
    IF v_event_type NOT IN ('DROP', 'TRUNCATE', 'ALTER') THEN
        RAISE EXCEPTION 'flashback_test_inject_ddl_commit: unsupported event_type %',
            p_event_type;
    END IF;

    SELECT tt.tracking_id, tt.schema_name, tt.table_name, tt.rel_oid
      INTO v_tt
    FROM flashback.tracked_tables tt
    WHERE tt.tracking_id = p_tracking_id;
    IF v_tt.tracking_id IS NULL THEN
        RAISE EXCEPTION 'flashback_test_inject_ddl_commit: tracking_id % not found',
            p_tracking_id;
    END IF;

    IF v_event_type = 'DROP'
       AND to_regclass(format('%I.%I', v_tt.schema_name, v_tt.table_name)) IS NOT NULL
    THEN
        PERFORM flashback_capture_drop_dependency_manifest(
            v_tt.schema_name, v_tt.table_name, false
        );
    END IF;

    PERFORM flashback_stage_local_delta_ddl_event(
        p_tracking_id,
        v_event_type,
        p_source_xid,
        p_commit_lsn,
        p_ddl_info,
        false,  -- row snapshot: table may already be gone for DROP
        false   -- commit marker: inject_commit supplies the COMMIT record
    );

    RETURN flashback_test_inject_commit(
        p_tracking_id, p_commit_lsn, p_commit_time, p_source_xid, '[]'::jsonb
    );
END;
$$;

-- Test-only restore wrappers: reuse the shared materialize/swap core without
-- claiming a physical replication slot. Production flashback_restore_lsn keeps
-- ensure_active_wal_stream + assert_relation_wal_drained fail-closed.
CREATE OR REPLACE FUNCTION flashback_test_restore_lsn(
    p_target_table text,
    p_target_lsn pg_lsn
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_stream_id bigint;
    v_locked record;
BEGIN
    PERFORM flashback_internal_lock_database_stream(
        (SELECT oid FROM pg_database WHERE datname = current_database())
    );

    SELECT cs.stream_id
      INTO v_stream_id
    FROM flashback.capture_streams cs
    WHERE cs.database_oid = (SELECT oid FROM pg_database WHERE datname = current_database())
      AND cs.state = 'active'
    ORDER BY cs.stream_id DESC
    LIMIT 1;

    IF v_stream_id IS NULL THEN
        RAISE EXCEPTION 'flashback_test_restore_lsn: no active capture stream from test bootstrap';
    END IF;

    SELECT * INTO STRICT v_locked
    FROM flashback_restore_lsn_lock_phase(p_target_table, p_target_lsn);

    RETURN flashback_internal_restore_lsn_core(
        p_target_table,
        p_target_lsn,
        v_stream_id,
        v_locked.out_disaster_event_id
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_test_restore_lsn(
    p_tables text[],
    p_target_lsn pg_lsn
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_table text;
    v_total bigint := 0;
BEGIN
    IF p_tables IS NULL OR array_length(p_tables, 1) IS NULL THEN
        RAISE EXCEPTION 'flashback_test_restore_lsn: tables array is empty';
    END IF;

    PERFORM set_config('session_replication_role', 'replica', true);
    BEGIN
        FOREACH v_table IN ARRAY p_tables LOOP
            v_total := v_total + flashback_test_restore_lsn(v_table, p_target_lsn);
        END LOOP;
        PERFORM set_config('session_replication_role', 'origin', true);
        RETURN v_total;
    EXCEPTION WHEN OTHERS THEN
        PERFORM set_config('session_replication_role', 'origin', true);
        RAISE;
    END;
END;
$$;

-- pg_test-only wrapper: exercise the production ownership-proven finalize
-- routine (never a forked cleanup algorithm).
CREATE OR REPLACE FUNCTION flashback_test_wal_only_staging_cleanup()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
BEGIN
    PERFORM flashback_internal_finalize_wal_only_upgrade();
END;
$$;

REVOKE ALL ON FUNCTION flashback_internal_open_capture_stream(text, text, pg_lsn, pg_lsn) FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_test_bootstrap_lifecycle(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_test_inject_commit(bigint, pg_lsn, timestamptz, bigint, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_test_resolve_post_restore_boundary(bigint, pg_lsn) FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_test_inject_ddl_commit(bigint, pg_lsn, timestamptz, bigint, text, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_test_restore_lsn(text, pg_lsn) FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_test_restore_lsn(text[], pg_lsn) FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_test_wal_only_staging_cleanup() FROM PUBLIC;
