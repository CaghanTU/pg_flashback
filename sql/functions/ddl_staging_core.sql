-- Internal local_delta DDL staging shared by flashback_capture_ddl_event() and
-- the pg_test DDL injection seam. Not a public API: no EXECUTE grants.

DROP FUNCTION IF EXISTS flashback_stage_local_delta_ddl_event(
    bigint, text, bigint, pg_lsn, jsonb, boolean, boolean
);

CREATE OR REPLACE FUNCTION flashback_stage_local_delta_ddl_event(
    p_tracking_id bigint,
    p_event_type text,
    p_source_xid bigint DEFAULT NULL,
    p_event_lsn pg_lsn DEFAULT NULL,
    p_ddl_info jsonb DEFAULT NULL,
    p_emit_logical_marker boolean DEFAULT true
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    tracked record;
    v_generation_id bigint;
    v_stream_id bigint;
    v_stream_state text;
    v_event_type text := upper(btrim(p_event_type));
    v_actual_schema text;
    v_actual_table text;
    v_ddl_info jsonb;
    v_new_version bigint;
    v_event_time timestamptz := clock_timestamp();
    v_event_lsn pg_lsn;
    v_source_xid bigint;
BEGIN
    IF p_tracking_id IS NULL THEN
        RAISE EXCEPTION 'flashback_stage_local_delta_ddl_event: tracking_id required';
    END IF;
    IF v_event_type NOT IN ('DROP', 'TRUNCATE', 'ALTER', 'RENAME') THEN
        RAISE EXCEPTION 'flashback_stage_local_delta_ddl_event: unsupported event_type %',
            p_event_type;
    END IF;

    SELECT tt.rel_oid, tt.tracking_id, tt.schema_name, tt.table_name,
           tt.schema_version, tt.recovery_profile, tt.is_active
      INTO tracked
    FROM flashback.tracked_tables tt
    WHERE tt.tracking_id = p_tracking_id;
    IF tracked.tracking_id IS NULL THEN
        RAISE EXCEPTION 'flashback_stage_local_delta_ddl_event: tracking_id % not found',
            p_tracking_id;
    END IF;
    IF tracked.recovery_profile IS DISTINCT FROM 'local_delta' THEN
        RAISE EXCEPTION 'flashback_stage_local_delta_ddl_event: tracking_id % is not local_delta',
            p_tracking_id;
    END IF;

    PERFORM flashback_internal_lock_lifecycle(p_tracking_id);

    SELECT cg.generation_id, cg.stream_id, cs.state
      INTO v_generation_id, v_stream_id, v_stream_state
    FROM flashback.coverage_generations cg
    JOIN flashback.capture_streams cs ON cs.stream_id = cg.stream_id
    WHERE cg.tracking_id = p_tracking_id
      AND cg.state = 'active'
    LIMIT 1;

    IF v_generation_id IS NULL THEN
        RAISE EXCEPTION 'flashback_stage_local_delta_ddl_event: tracking lifecycle % has no active WAL generation',
            p_tracking_id;
    END IF;
    IF v_stream_state <> 'active' THEN
        RAISE EXCEPTION 'flashback_stage_local_delta_ddl_event: WAL stream % is %',
            v_stream_id, v_stream_state;
    END IF;

    -- RENAME TABLE / SET SCHEMA: refresh tracked identity from the live catalog.
    SELECT n.nspname, c.relname
      INTO v_actual_schema, v_actual_table
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = tracked.rel_oid;

    IF v_actual_schema IS NOT NULL AND v_actual_table IS NOT NULL
       AND (v_actual_schema, v_actual_table)
           IS DISTINCT FROM (tracked.schema_name, tracked.table_name)
    THEN
        UPDATE flashback.tracked_tables
           SET schema_name = v_actual_schema,
               table_name = v_actual_table
         WHERE tracking_id = p_tracking_id;
        RAISE NOTICE 'pg_flashback: table renamed/moved from %.% to %.% — tracking updated',
            tracked.schema_name, tracked.table_name, v_actual_schema, v_actual_table;
        tracked.schema_name := v_actual_schema;
        tracked.table_name := v_actual_table;
    END IF;

    v_event_lsn := COALESCE(p_event_lsn, pg_current_wal_insert_lsn());
    v_source_xid := COALESCE(p_source_xid, (txid_current() % 4294967296)::bigint);

    IF v_event_type = 'ALTER' THEN
        v_ddl_info := COALESCE(
            p_ddl_info,
            CASE WHEN to_regclass(format('%I.%I', tracked.schema_name, tracked.table_name)) IS NOT NULL
                 THEN flashback_collect_schema_def(tracked.rel_oid)
                 ELSE '{}'::jsonb
            END,
            '{}'::jsonb
        );
        v_new_version := COALESCE(tracked.schema_version, 1) + 1;

        UPDATE flashback.tracked_tables
           SET schema_version = v_new_version
         WHERE tracking_id = p_tracking_id;

        INSERT INTO flashback.schema_versions (
            rel_oid, tracking_id, generation_id, stream_id, source_xid,
            schema_version, applied_at, applied_lsn, committed_at, commit_lsn,
            columns, primary_key, constraints, schema_def, helper_schema_sha256
        ) VALUES (
            tracked.rel_oid, p_tracking_id, v_generation_id, v_stream_id, v_source_xid,
            v_new_version, v_event_time, v_event_lsn, NULL, NULL,
            COALESCE(v_ddl_info -> 'columns', '[]'::jsonb),
            COALESCE(v_ddl_info -> 'primary_key', '[]'::jsonb),
            jsonb_build_object(
                'check_unique_fk', COALESCE(v_ddl_info -> 'constraints', '[]'::jsonb),
                'indexes', COALESCE(v_ddl_info -> 'indexes', '[]'::jsonb),
                'partition_by', v_ddl_info -> 'partition_by',
                'partitions', v_ddl_info -> 'partitions',
                'triggers', COALESCE(v_ddl_info -> 'triggers', '[]'::jsonb),
                'rls_policies', COALESCE(v_ddl_info -> 'rls_policies', '[]'::jsonb),
                'rls_enabled', COALESCE((v_ddl_info -> 'rls_enabled')::boolean, false)
            ),
            v_ddl_info,
            CASE WHEN to_regclass(format('%I.%I', tracked.schema_name, tracked.table_name)) IS NOT NULL
                 THEN flashback_helper_schema_sha256(tracked.rel_oid)
                 ELSE NULL
            END
        );
    ELSE
        v_ddl_info := COALESCE(
            p_ddl_info,
            CASE WHEN to_regclass(format('%I.%I', tracked.schema_name, tracked.table_name)) IS NOT NULL
                 THEN flashback_collect_schema_def(tracked.rel_oid)
                 ELSE '{}'::jsonb
            END,
            '{}'::jsonb
        );
        v_new_version := COALESCE(tracked.schema_version, 1);
    END IF;

    IF v_event_type IN ('DROP', 'TRUNCATE') THEN
        -- Some metadata commands operate on objects related to the table
        -- rather than on the table node itself (CREATE INDEX/TRIGGER/POLICY,
        -- GRANT/COMMENT, ALTER SEQUENCE, ...).  Until every such command has
        -- an exact table-identity mapping in the ProcessUtility hook, never
        -- let a DROP silently recover from a stale schema epoch.  A table DDL
        -- that was captured normally has already advanced tracked.schema_version
        -- and therefore compares equal here; unrecorded drift aborts the same
        -- user transaction before PostgreSQL can unlink the table.
        PERFORM public.flashback_require_current_schema_contract(
            p_tracking_id, v_ddl_info
        );
    END IF;

    -- No inline full-table row snapshot is captured here. Restore never reads
    -- old_data/new_data for a DROP/TRUNCATE/ALTER delta_log row (it truncates
    -- and replays from the boundary snapshot plus per-row DML deltas
    -- instead), so an inline jsonb_agg() of the whole table -- unbounded by
    -- row width and previously capped only by row *count* (100000), not
    -- captured byte size -- was pure OOM/latency risk on the DDL statement's
    -- own transaction with no corresponding recovery benefit.
    INSERT INTO flashback.pending_wal_events (
        tracking_id, generation_id, stream_id, source_xid,
        event_type, table_name, rel_oid, event_lsn, schema_version,
        old_data, new_data, ddl_info
    ) VALUES (
        p_tracking_id, v_generation_id, v_stream_id, v_source_xid,
        v_event_type,
        format('%I.%I', tracked.schema_name, tracked.table_name),
        tracked.rel_oid, v_event_lsn, v_new_version,
        NULL, NULL, v_ddl_info
    );

    IF p_emit_logical_marker THEN
        PERFORM pg_logical_emit_message(
            true,
            'pg_flashback',
            jsonb_build_object(
                'kind', 'commit-marker',
                'source_xid', v_source_xid
            )::text
        );
    END IF;

    RETURN jsonb_build_object(
        'tracking_id', p_tracking_id,
        'generation_id', v_generation_id,
        'stream_id', v_stream_id,
        'schema_version', v_new_version,
        'event_lsn', v_event_lsn,
        'source_xid', v_source_xid,
        'event_type', v_event_type,
        'schema_name', tracked.schema_name,
        'table_name', tracked.table_name,
        'rel_oid', tracked.rel_oid
    );
END;
$$;

REVOKE ALL ON FUNCTION flashback_stage_local_delta_ddl_event(
    bigint, text, bigint, pg_lsn, jsonb, boolean
) FROM PUBLIC;
