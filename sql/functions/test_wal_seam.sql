-- pg_test-only WAL injection seam. Included only when feature pg_test / test
-- builds load this file. Production package SQL must not contain these names.

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

    -- Activate the building generation through the real promote core by
    -- injecting the boundary COMMIT record (decoder-shaped JSON).
    -- Per-lifecycle LSN: a shared 0/1000 would collide in capture_commits
    -- across multiple bootstraps on the same synthetic stream.
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

    PERFORM pg_advisory_xact_lock(358944::integer, hashint8(v_boot.out_tracking_id));
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

    -- Post-restore leaves a building successor until its boundary COMMIT is
    -- observed. Allow empty-event activation of that exact boundary_xid even
    -- when the live relation was dropped (tracked row may be inactive).
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

    PERFORM pg_advisory_xact_lock(358944::integer, hashint8(p_tracking_id));

    -- Match consumer idempotency: a commit_lsn already present for this stream
    -- was promoted; do not re-apply events (slot advancement makes this the
    -- physical consumer's contract).
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

    PERFORM pg_advisory_xact_lock(358944::integer, hashint8(p_tracking_id));

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

-- DDL inside DO/SPI is not PROCESS_UTILITY_TOPLEVEL, so the product hook does
-- not record it. This test-only helper stages the same pending_wal_events row
-- the hook would have written, then promotes it through the shared core.
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
    v_tt record;
    v_gen record;
    v_event_type text := upper(btrim(p_event_type));
    v_table_name text;
    v_schema_version bigint;
    v_live_schema text;
    v_live_table text;
BEGIN
    IF v_event_type NOT IN ('DROP', 'TRUNCATE', 'ALTER') THEN
        RAISE EXCEPTION 'flashback_test_inject_ddl_commit: unsupported event_type %',
            p_event_type;
    END IF;

    SELECT tt.tracking_id, tt.rel_oid, tt.schema_name, tt.table_name,
           tt.schema_version
      INTO v_tt
    FROM flashback.tracked_tables tt
    WHERE tt.tracking_id = p_tracking_id;
    IF v_tt.tracking_id IS NULL THEN
        RAISE EXCEPTION 'flashback_test_inject_ddl_commit: tracking_id % not found',
            p_tracking_id;
    END IF;

    SELECT cg.*
      INTO v_gen
    FROM flashback.coverage_generations cg
    WHERE cg.tracking_id = p_tracking_id
      AND cg.state = 'active'
    ORDER BY cg.generation_id DESC
    LIMIT 1;
    IF v_gen.generation_id IS NULL THEN
        RAISE EXCEPTION 'flashback_test_inject_ddl_commit: no active generation for %',
            p_tracking_id;
    END IF;

    -- SET SCHEMA / RENAME keep the OID; refresh tracked names from the catalog.
    SELECT n.nspname, c.relname
      INTO v_live_schema, v_live_table
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = v_tt.rel_oid;

    IF v_live_schema IS NOT NULL THEN
        UPDATE flashback.tracked_tables
           SET schema_name = v_live_schema,
               table_name = v_live_table
         WHERE tracking_id = p_tracking_id
           AND (schema_name, table_name)
               IS DISTINCT FROM (v_live_schema, v_live_table);
        v_tt.schema_name := v_live_schema;
        v_tt.table_name := v_live_table;
    END IF;

    v_table_name := format('%I.%I', v_tt.schema_name, v_tt.table_name);
    v_schema_version := COALESCE(v_tt.schema_version, 1);

    IF v_event_type = 'DROP' AND to_regclass(v_table_name) IS NOT NULL THEN
        PERFORM flashback_capture_drop_dependency_manifest(
            v_tt.schema_name, v_tt.table_name, false
        );
    END IF;

    INSERT INTO flashback.pending_wal_events (
        tracking_id, generation_id, stream_id, source_xid,
        event_type, table_name, rel_oid, event_lsn, schema_version,
        old_data, new_data, ddl_info
    ) VALUES (
        p_tracking_id, v_gen.generation_id, v_gen.stream_id, p_source_xid,
        v_event_type, v_table_name, v_gen.rel_oid_at_boundary, p_commit_lsn,
        v_schema_version,
        NULL, NULL, COALESCE(p_ddl_info, '{}'::jsonb)
    );

    RETURN flashback_test_inject_commit(
        p_tracking_id, p_commit_lsn, p_commit_time, p_source_xid, '[]'::jsonb
    );
END;
$$;

REVOKE ALL ON FUNCTION flashback_test_bootstrap_lifecycle(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_test_inject_commit(bigint, pg_lsn, timestamptz, bigint, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_test_resolve_post_restore_boundary(bigint, pg_lsn) FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_test_inject_ddl_commit(bigint, pg_lsn, timestamptz, bigint, text, jsonb) FROM PUBLIC;
