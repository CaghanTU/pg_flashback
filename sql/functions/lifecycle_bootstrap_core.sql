-- Internal local_delta lifecycle bootstrap shared by flashback_track() (WAL)
-- and the pg_test injection seam. Not a public API: no EXECUTE grants.

-- Shared schema-contract fingerprint helpers used by the qualified local_delta
-- lifecycle to stamp schema_versions.helper_schema_sha256.
CREATE OR REPLACE FUNCTION flashback_helper_schema_contract(target_rel oid)
RETURNS jsonb
LANGUAGE sql
STABLE
STRICT
SET search_path = pg_catalog
AS $$
    WITH columns AS (
        SELECT a.attnum, a.attname,
               format_type(a.atttypid, a.atttypmod) AS data_type,
               a.attnotnull, a.attidentity, a.attgenerated,
               CASE WHEN a.attcollation = 0 THEN NULL
                    ELSE a.attcollation::regcollation::text END AS collation,
               pg_get_expr(d.adbin, d.adrelid) AS default_expression
        FROM pg_attribute AS a
        LEFT JOIN pg_attrdef AS d
          ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE a.attrelid = target_rel
          AND a.attnum > 0
          AND NOT a.attisdropped
    ), constraints AS (
        SELECT conname, contype, condeferrable, condeferred, convalidated,
               pg_get_constraintdef(oid, true) AS definition
        FROM pg_constraint
        WHERE conrelid = target_rel
    ), indexes AS (
        SELECT c.relname, pg_get_indexdef(i.indexrelid) AS definition
        FROM pg_index AS i
        JOIN pg_class AS c ON c.oid = i.indexrelid
        WHERE i.indrelid = target_rel
    ), triggers AS (
        SELECT tgname, pg_get_triggerdef(oid, true) AS definition
        FROM pg_trigger
        WHERE tgrelid = target_rel AND NOT tgisinternal
    ), policies AS (
        SELECT polname, polcmd, polpermissive, polroles,
               pg_get_expr(polqual, polrelid) AS using_expression,
               pg_get_expr(polwithcheck, polrelid) AS check_expression
        FROM pg_policy
        WHERE polrelid = target_rel
    )
    SELECT jsonb_build_object(
        'table', (
            SELECT jsonb_build_object(
                'schema', n.nspname,
                'name', c.relname,
                'kind', c.relkind,
                'persistence', c.relpersistence,
                'replica_identity', c.relreplident,
                'row_security', c.relrowsecurity,
                'force_row_security', c.relforcerowsecurity,
                'options', c.reloptions,
                'partition_bound', pg_get_expr(c.relpartbound, c.oid)
            )
            FROM pg_class AS c
            JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE c.oid = target_rel
        ),
        'columns', (
            SELECT COALESCE(jsonb_agg(to_jsonb(columns) ORDER BY attnum), '[]'::jsonb)
            FROM columns
        ),
        'constraints', (
            SELECT COALESCE(jsonb_agg(to_jsonb(constraints) ORDER BY conname), '[]'::jsonb)
            FROM constraints
        ),
        'indexes', (
            SELECT COALESCE(jsonb_agg(to_jsonb(indexes) ORDER BY relname), '[]'::jsonb)
            FROM indexes
        ),
        'triggers', (
            SELECT COALESCE(jsonb_agg(to_jsonb(triggers) ORDER BY tgname), '[]'::jsonb)
            FROM triggers
        ),
        'policies', (
            SELECT COALESCE(jsonb_agg(to_jsonb(policies) ORDER BY polname), '[]'::jsonb)
            FROM policies
        )
    );
$$;

CREATE OR REPLACE FUNCTION flashback_helper_schema_sha256(target_rel oid)
RETURNS text
LANGUAGE sql
STABLE
STRICT
SET search_path = pg_catalog, public
AS $$
    SELECT flashback_sha256(flashback_helper_schema_contract(target_rel)::text);
$$;

CREATE OR REPLACE FUNCTION flashback_bootstrap_local_delta_lifecycle_core(
    p_rel_oid oid,
    p_stream_id bigint,
    p_replica_identity_was "char" DEFAULT 'd',
    p_replica_identity_index text DEFAULT NULL
)
RETURNS TABLE(
    out_tracking_id bigint,
    out_generation_id bigint,
    out_stream_id bigint,
    out_boundary_xid bigint,
    out_snapshot_id bigint,
    out_provisional_lsn pg_lsn,
    out_schema_name text,
    out_table_name text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_schema_name text;
    v_table_name text;
    v_tracking_id bigint;
    v_snapshot_name text;
    v_tracked_since timestamptz;
    v_snapshot_id bigint;
    v_generation_id bigint;
    v_boundary_xid bigint;
    v_provisional_lsn pg_lsn;
    v_schema_def jsonb;
    v_row_count bigint;
    stale_snap record;
    old_oid oid;
BEGIN
    IF p_rel_oid IS NULL THEN
        RAISE EXCEPTION 'flashback_bootstrap_local_delta_lifecycle_core: rel_oid required';
    END IF;
    IF p_stream_id IS NULL THEN
        RAISE EXCEPTION 'flashback_bootstrap_local_delta_lifecycle_core: stream_id required';
    END IF;

    SELECT n.nspname, c.relname
      INTO v_schema_name, v_table_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = p_rel_oid;
    IF v_schema_name IS NULL THEN
        RAISE EXCEPTION 'flashback_bootstrap_local_delta_lifecycle_core: relation % missing', p_rel_oid;
    END IF;

    PERFORM flashback_require_supported_local_table(p_rel_oid);
    PERFORM flashback_require_local_compatibility(p_rel_oid);

    PERFORM pg_advisory_xact_lock(
        358943::integer,
        hashtext(format(
            '%s:%s.%s',
            (SELECT oid FROM pg_database WHERE datname = current_database()),
            v_schema_name,
            v_table_name
        ))
    );

    IF EXISTS (
        SELECT 1 FROM flashback.tracked_tables tt
        WHERE tt.is_active
          AND (tt.rel_oid = p_rel_oid
               OR (tt.schema_name = v_schema_name AND tt.table_name = v_table_name))
    ) THEN
        RAISE EXCEPTION 'flashback_bootstrap_local_delta_lifecycle_core: %.% already has a tracking lifecycle',
            v_schema_name, v_table_name;
    END IF;

    v_tracking_id := nextval('flashback.tracking_id_seq');
    PERFORM pg_advisory_xact_lock(358944::integer, hashint8(v_tracking_id));

    PERFORM flashback_admit_local_capacity(p_rel_oid, 'track');
    PERFORM flashback_apply_local_boundary_lock_timeout();
    BEGIN
        EXECUTE format('LOCK TABLE %I.%I IN SHARE ROW EXCLUSIVE MODE', v_schema_name, v_table_name);
    EXCEPTION WHEN lock_not_available THEN
        RAISE EXCEPTION 'pg_flashback: local track lock wait exceeded local_boundary_write_stall_ms'
            USING ERRCODE = 'lock_not_available';
    END;

    IF to_regclass(format('%I.%I', v_schema_name, v_table_name))::oid
           IS DISTINCT FROM p_rel_oid
    THEN
        RAISE EXCEPTION 'pg_flashback: table identity changed while first-track lock was acquired';
    END IF;

    PERFORM flashback_admit_local_capacity(p_rel_oid, 'track');
    EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY FULL', v_schema_name, v_table_name);

    v_snapshot_name := format('base_snapshot_t%s', v_tracking_id::text);

    SELECT rel_oid INTO old_oid
    FROM flashback.tracked_tables
    WHERE schema_name = v_schema_name AND table_name = v_table_name
      AND rel_oid <> p_rel_oid
      AND is_active
    LIMIT 1;

    IF old_oid IS NOT NULL THEN
        FOR stale_snap IN
            SELECT snapshot_table FROM flashback.snapshots WHERE rel_oid = old_oid
        LOOP
            IF stale_snap.snapshot_table IS NOT NULL AND stale_snap.snapshot_table <> '' THEN
                PERFORM public.flashback_drop_payload_table(to_regclass(stale_snap.snapshot_table));
            END IF;
        END LOOP;
        PERFORM public.flashback_drop_payload_table(
            to_regclass(format('flashback.%I', format('base_snapshot_%s', old_oid::text)))
        );
        DELETE FROM flashback.snapshots WHERE rel_oid = old_oid;
        DELETE FROM flashback.delta_log WHERE rel_oid = old_oid;
        DELETE FROM flashback.schema_versions WHERE rel_oid = old_oid;
        DELETE FROM flashback.tracked_tables WHERE rel_oid = old_oid;
    END IF;

    FOR stale_snap IN
        SELECT snapshot_table FROM flashback.snapshots WHERE rel_oid = p_rel_oid
    LOOP
        IF stale_snap.snapshot_table IS NOT NULL AND stale_snap.snapshot_table <> '' THEN
            IF NOT EXISTS (
                SELECT 1 FROM flashback.tracked_tables tt
                WHERE tt.rel_oid = p_rel_oid
                  AND tt.base_snapshot_table = stale_snap.snapshot_table
            ) THEN
                PERFORM public.flashback_drop_payload_table(to_regclass(stale_snap.snapshot_table));
                DELETE FROM flashback.snapshots
                WHERE rel_oid = p_rel_oid
                  AND snapshot_table = stale_snap.snapshot_table;
            END IF;
        END IF;
    END LOOP;

    PERFORM public.flashback_drop_payload_table(
        to_regclass(format('flashback.%I', v_snapshot_name))
    );
    EXECUTE format('CREATE TABLE flashback.%I AS TABLE %I.%I', v_snapshot_name, v_schema_name, v_table_name);
    PERFORM public.flashback_own_payload_table(
        to_regclass(format('flashback.%I', v_snapshot_name))
    );

    INSERT INTO flashback.tracked_tables (
        tracking_id, rel_oid, schema_name, table_name, base_snapshot_table,
        schema_version, recovery_profile, helper_profile,
        coverage_start_lsn, coverage_end_lsn,
        tracked_since, checkpoint_interval, retention_interval, is_active,
        replica_identity_was, replica_identity_index
    ) VALUES (
        v_tracking_id, p_rel_oid, v_schema_name, v_table_name,
        format('flashback.%I', v_snapshot_name),
        1, 'local_delta', NULL, NULL, NULL,
        now(), interval '15 minutes', interval '7 days', true,
        p_replica_identity_was, p_replica_identity_index
    );

    SELECT tracked_since INTO v_tracked_since
    FROM flashback.tracked_tables WHERE tracking_id = v_tracking_id;

    v_boundary_xid := (txid_current() % 4294967296)::bigint;
    v_provisional_lsn := pg_current_wal_insert_lsn();
    v_schema_def := COALESCE(flashback_collect_schema_def(p_rel_oid), '{}'::jsonb);
    EXECUTE format('SELECT count(*) FROM flashback.%I', v_snapshot_name) INTO v_row_count;

    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn,
        schema_def, row_count, captured_at
    ) VALUES (
        p_rel_oid, v_tracking_id, format('flashback.%I', v_snapshot_name),
        v_provisional_lsn, v_schema_def, v_row_count, clock_timestamp()
    ) RETURNING snapshot_id INTO v_snapshot_id;

    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, stream_id, recovery_profile, state,
        boundary_kind, rel_oid_at_boundary, boundary_snapshot_id,
        boundary_xid, boundary_marker, details
    ) VALUES (
        v_tracking_id, 1, p_stream_id, 'local_delta', 'building',
        'initial_track', p_rel_oid, v_snapshot_id,
        v_boundary_xid, format('initial-track:%s:%s', v_tracking_id, v_boundary_xid),
        jsonb_build_object('provisional_snapshot_lsn', v_provisional_lsn)
    ) RETURNING generation_id INTO v_generation_id;

    DELETE FROM flashback.schema_versions WHERE rel_oid = p_rel_oid;

    INSERT INTO flashback.schema_versions (
        rel_oid, tracking_id, generation_id, stream_id, source_xid,
        schema_version, applied_at, applied_lsn, committed_at, commit_lsn,
        columns, primary_key, constraints, helper_schema_sha256
    )
    SELECT
        p_rel_oid, v_tracking_id, v_generation_id, p_stream_id, v_boundary_xid,
        1, COALESCE(v_tracked_since, clock_timestamp()), v_provisional_lsn,
        NULL, NULL,
        COALESCE(schema_def -> 'columns', '[]'::jsonb),
        COALESCE(schema_def -> 'primary_key', '[]'::jsonb),
        jsonb_build_object(
            'check_unique_fk', COALESCE(schema_def -> 'constraints', '[]'::jsonb),
            'indexes', COALESCE(schema_def -> 'indexes', '[]'::jsonb),
            'partition_by', schema_def -> 'partition_by',
            'partitions', schema_def -> 'partitions',
            'triggers', COALESCE(schema_def -> 'triggers', '[]'::jsonb),
            'rls_policies', COALESCE(schema_def -> 'rls_policies', '[]'::jsonb),
            'rls_enabled', COALESCE((schema_def -> 'rls_enabled')::boolean, false)
        ),
        flashback_helper_schema_sha256(p_rel_oid)
    FROM (
        SELECT COALESCE(flashback_collect_schema_def(p_rel_oid), '{}'::jsonb) AS schema_def
    ) s;

    out_tracking_id := v_tracking_id;
    out_generation_id := v_generation_id;
    out_stream_id := p_stream_id;
    out_boundary_xid := v_boundary_xid;
    out_snapshot_id := v_snapshot_id;
    out_provisional_lsn := v_provisional_lsn;
    out_schema_name := v_schema_name;
    out_table_name := v_table_name;
    RETURN NEXT;
END;
$$;

REVOKE ALL ON FUNCTION flashback_bootstrap_local_delta_lifecycle_core(oid, bigint, "char", text) FROM PUBLIC;
