-- =================================================================
-- Backup-backed recovery API.
--
-- PostgreSQL never launches the external helper. These functions create an
-- immutable request, accept a validated helper manifest, verify the imported
-- shadow table and perform the existing transactional shadow swap.
-- =================================================================

CREATE OR REPLACE FUNCTION flashback_resolve_tracked_backup(target_table text)
RETURNS oid
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_live_oid oid;
    v_ident text[];
    v_matches oid[];
BEGIN
    IF target_table IS NULL OR btrim(target_table) = '' THEN
        RAISE EXCEPTION 'backup table reference must not be empty';
    END IF;

    v_live_oid := to_regclass(target_table);
    IF v_live_oid IS NOT NULL AND EXISTS (
        SELECT 1
        FROM flashback.tracked_tables tt
        WHERE tt.rel_oid = v_live_oid
          AND tt.is_active
          AND tt.recovery_profile = 'backup'
    ) THEN
        RETURN v_live_oid;
    END IF;

    BEGIN
        v_ident := parse_ident(target_table, true);
    EXCEPTION WHEN invalid_name OR syntax_error THEN
        RAISE EXCEPTION 'invalid backup table reference: %', target_table;
    END;
    IF cardinality(v_ident) NOT IN (1, 2) THEN
        RAISE EXCEPTION 'backup table reference must be table or schema.table: %', target_table;
    END IF;

    SELECT array_agg(tt.rel_oid ORDER BY tt.tracked_since DESC)
      INTO v_matches
    FROM flashback.tracked_tables tt
    WHERE tt.is_active
      AND tt.recovery_profile = 'backup'
      AND (
          (cardinality(v_ident) = 2
           AND tt.schema_name = v_ident[1]
           AND tt.table_name = v_ident[2])
          OR
          (cardinality(v_ident) = 1 AND tt.table_name = v_ident[1])
      );

    IF cardinality(v_matches) IS NULL THEN
        RETURN NULL;
    END IF;
    IF cardinality(v_matches) > 1 THEN
        RAISE EXCEPTION 'ambiguous backup table reference %, use a schema-qualified name', target_table;
    END IF;
    RETURN v_matches[1];
END;
$$;

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

CREATE OR REPLACE FUNCTION flashback_track_backup(
    target_table text,
    helper_profile text
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_rel_oid oid;
    v_schema_name text;
    v_table_name text;
    v_relkind "char";
    v_existing_profile text;
    v_schema_def jsonb;
    v_schema_hash text;
    v_tracked_since timestamptz := clock_timestamp();
BEGIN
    IF helper_profile IS NULL OR helper_profile !~ '^[A-Za-z0-9_-]+$' THEN
        RAISE EXCEPTION 'flashback_track_backup: invalid helper profile';
    END IF;

    v_rel_oid := to_regclass(target_table);
    IF v_rel_oid IS NULL THEN
        RAISE EXCEPTION 'flashback_track_backup: table % does not exist', target_table;
    END IF;

    SELECT n.nspname, c.relname, c.relkind
      INTO v_schema_name, v_table_name, v_relkind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = v_rel_oid;

    IF v_relkind <> 'r' THEN
        RAISE EXCEPTION 'flashback_track_backup: first release supports ordinary tables only (relkind=%)', v_relkind;
    END IF;
    IF v_schema_name IN ('pg_catalog', 'information_schema', 'flashback', 'flashback_import') THEN
        RAISE EXCEPTION 'flashback_track_backup: schema % is reserved', v_schema_name;
    END IF;

    PERFORM pg_advisory_xact_lock(
        hashtextextended('pg_flashback:backup:' || v_rel_oid::text, 0)
    );
    EXECUTE format('LOCK TABLE %I.%I IN SHARE MODE', v_schema_name, v_table_name);
    IF to_regclass(format('%I.%I', v_schema_name, v_table_name)) IS DISTINCT FROM v_rel_oid THEN
        RAISE EXCEPTION 'flashback_track_backup: table identity changed while tracking';
    END IF;

    SELECT recovery_profile INTO v_existing_profile
    FROM flashback.tracked_tables
    WHERE rel_oid = v_rel_oid AND is_active;
    IF v_existing_profile IS NOT NULL AND v_existing_profile <> 'backup' THEN
        RAISE EXCEPTION 'flashback_track_backup: table is already tracked with profile %; untrack it first', v_existing_profile;
    END IF;

    v_schema_def := COALESCE(flashback_collect_schema_def(v_rel_oid), '{}'::jsonb);
    v_schema_hash := flashback_helper_schema_sha256(v_rel_oid);

    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table,
        schema_version, recovery_profile, helper_profile,
        coverage_start_lsn, coverage_end_lsn,
        tracked_since, checkpoint_interval, retention_interval, is_active
    ) VALUES (
        v_rel_oid, v_schema_name, v_table_name, NULL,
        1, 'backup', helper_profile,
        NULL, NULL,
        v_tracked_since, interval '15 minutes', interval '7 days', true
    )
    ON CONFLICT (rel_oid) DO UPDATE SET
        schema_name = EXCLUDED.schema_name,
        table_name = EXCLUDED.table_name,
        base_snapshot_table = NULL,
        schema_version = 1,
        recovery_profile = 'backup',
        helper_profile = EXCLUDED.helper_profile,
        coverage_start_lsn = NULL,
        coverage_end_lsn = NULL,
        tracked_since = EXCLUDED.tracked_since,
        is_active = true;

    DELETE FROM flashback.schema_versions WHERE rel_oid = v_rel_oid;
    DELETE FROM flashback.delta_log WHERE rel_oid = v_rel_oid;
    DELETE FROM flashback.staging_events WHERE rel_oid = v_rel_oid;

    INSERT INTO flashback.schema_versions (
        rel_oid, schema_version, applied_at, applied_lsn,
        columns, primary_key, constraints, helper_schema_sha256
    ) VALUES (
        v_rel_oid, 1, v_tracked_since, pg_current_wal_insert_lsn(),
        COALESCE(v_schema_def -> 'columns', '[]'::jsonb),
        COALESCE(v_schema_def -> 'primary_key', '[]'::jsonb),
        jsonb_build_object(
            'check_unique_fk', COALESCE(v_schema_def -> 'constraints', '[]'::jsonb),
            'indexes', COALESCE(v_schema_def -> 'indexes', '[]'::jsonb),
            'partition_by', v_schema_def -> 'partition_by',
            'partitions', v_schema_def -> 'partitions',
            'triggers', COALESCE(v_schema_def -> 'triggers', '[]'::jsonb),
            'rls_policies', COALESCE(v_schema_def -> 'rls_policies', '[]'::jsonb),
            'rls_enabled', COALESCE((v_schema_def -> 'rls_enabled')::boolean, false)
        ),
        v_schema_hash
    );

    RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_set_backup_coverage(
    target_table text,
    first_lsn pg_lsn,
    latest_lsn pg_lsn
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_rel_oid oid;
BEGIN
    IF first_lsn IS NULL OR latest_lsn IS NULL OR first_lsn > latest_lsn THEN
        RAISE EXCEPTION 'flashback_set_backup_coverage: invalid LSN range % .. %', first_lsn, latest_lsn;
    END IF;

    v_rel_oid := flashback_resolve_tracked_backup(target_table);

    IF v_rel_oid IS NULL THEN
        RAISE EXCEPTION 'flashback_set_backup_coverage: backup profile is not active for %', target_table;
    END IF;

    UPDATE flashback.tracked_tables
       SET coverage_start_lsn = first_lsn,
           coverage_end_lsn = latest_lsn
     WHERE rel_oid = v_rel_oid;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_backup_disaster_points(
    target_table text,
    lookback interval DEFAULT interval '24 hours'
)
RETURNS TABLE (
    event_time timestamptz,
    event_type text,
    target_lsn pg_lsn,
    source_xid bigint,
    schema_version bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
STABLE
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_rel_oid oid;
BEGIN
    IF lookback IS NULL OR lookback <= interval '0' THEN
        RAISE EXCEPTION 'flashback_backup_disaster_points: lookback must be positive';
    END IF;
    v_rel_oid := flashback_resolve_tracked_backup(target_table);
    IF v_rel_oid IS NULL THEN
        RAISE EXCEPTION 'flashback_backup_disaster_points: backup profile is not active for %',
            target_table;
    END IF;

    RETURN QUERY
    SELECT d.event_time, d.event_type, d.lsn, d.source_xid, d.schema_version
    FROM flashback.delta_log d
    WHERE d.rel_oid = v_rel_oid
      AND d.event_type IN ('DROP', 'TRUNCATE', 'ALTER')
      AND d.lsn IS NOT NULL
      AND d.event_time >= statement_timestamp() - lookback
    ORDER BY d.event_time DESC, d.event_id DESC;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_prepare_backup_restore(
    target_table text,
    target_lsn pg_lsn
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_tracked record;
    v_schema record;
    v_request_id text;
    v_request jsonb;
BEGIN
    SELECT tt.* INTO v_tracked
    FROM flashback.tracked_tables tt
    WHERE tt.rel_oid = flashback_resolve_tracked_backup(target_table);

    IF v_tracked.rel_oid IS NULL THEN
        RAISE EXCEPTION 'flashback_prepare_backup_restore: backup profile is not active for %', target_table;
    END IF;
    IF v_tracked.coverage_start_lsn IS NULL OR v_tracked.coverage_end_lsn IS NULL THEN
        RAISE EXCEPTION 'flashback_prepare_backup_restore: verified backup/WAL coverage is unknown';
    END IF;
    IF target_lsn < v_tracked.coverage_start_lsn OR target_lsn > v_tracked.coverage_end_lsn THEN
        RAISE EXCEPTION 'flashback_prepare_backup_restore: target % is outside verified coverage % .. %',
            target_lsn, v_tracked.coverage_start_lsn, v_tracked.coverage_end_lsn;
    END IF;

    SELECT sv.schema_version, sv.helper_schema_sha256
      INTO v_schema
    FROM flashback.schema_versions sv
    WHERE sv.rel_oid = v_tracked.rel_oid
      AND sv.applied_lsn <= target_lsn
    ORDER BY sv.applied_lsn DESC, sv.schema_version DESC
    LIMIT 1;

    IF v_schema.schema_version IS NULL OR v_schema.helper_schema_sha256 IS NULL THEN
        RAISE EXCEPTION 'flashback_prepare_backup_restore: target schema fingerprint is unavailable';
    END IF;

    v_request_id := format(
        'fb-%s-%s',
        to_char(clock_timestamp(), 'YYYYMMDDHH24MISSUS'),
        nextval('flashback.backup_restore_request_seq')
    );
    v_request := jsonb_build_object(
        'request_id', v_request_id,
        'database', current_database(),
        'table', jsonb_build_object(
            'schema', v_tracked.schema_name,
            'name', v_tracked.table_name,
            'rel_oid', v_tracked.rel_oid::bigint
        ),
        'target', jsonb_build_object(
            'kind', 'lsn',
            'value', target_lsn::text,
            'observed_at_unix_seconds', extract(epoch FROM clock_timestamp())::bigint,
            'inclusive', true
        ),
        'expected_schema_version', v_schema.schema_version,
        'expected_schema_sha256', v_schema.helper_schema_sha256,
        'expected_fingerprint', NULL
    );

    INSERT INTO flashback.backup_restore_requests (
        request_id, rel_oid, schema_name, table_name, target_lsn,
        expected_schema_version, expected_schema_sha256,
        helper_profile, request_json, requested_by
    ) VALUES (
        v_request_id, v_tracked.rel_oid, v_tracked.schema_name, v_tracked.table_name,
        target_lsn, v_schema.schema_version, v_schema.helper_schema_sha256,
        v_tracked.helper_profile, v_request, session_user
    );

    RETURN v_request;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_claim_backup_restore(p_request_id text)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_request jsonb;
BEGIN
    UPDATE flashback.backup_restore_requests
       SET status = 'running', updated_at = clock_timestamp()
     WHERE request_id = p_request_id AND status = 'pending'
    RETURNING request_json INTO v_request;

    IF v_request IS NULL THEN
        RAISE EXCEPTION 'flashback_claim_backup_restore: request % is not pending', p_request_id;
    END IF;
    RETURN v_request;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_accept_backup_restore(
    p_request_id text,
    p_result jsonb
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_request flashback.backup_restore_requests%ROWTYPE;
    v_artifact_schema text;
    v_artifact_table text;
    v_artifact_sha text;
    v_artifact_oid oid;
BEGIN
    SELECT * INTO v_request
    FROM flashback.backup_restore_requests
    WHERE request_id = p_request_id
    FOR UPDATE;

    IF v_request.request_id IS NULL OR v_request.status <> 'running' THEN
        RAISE EXCEPTION 'flashback_accept_backup_restore: request % is not running', p_request_id;
    END IF;
    IF p_result ->> 'status' IS DISTINCT FROM 'completed'
       OR COALESCE((p_result ->> 'cleanup_complete')::boolean, false) IS NOT TRUE
       OR COALESCE((p_result ->> 'result_format_version')::integer, 0) < 3
       OR p_result ->> 'profile' IS DISTINCT FROM v_request.helper_profile
       OR p_result -> 'request' IS DISTINCT FROM v_request.request_json
       OR p_result ->> 'recovered_schema_sha256' IS DISTINCT FROM v_request.expected_schema_sha256
       OR COALESCE(p_result ->> 'recovered_owner', '') = ''
       OR jsonb_typeof(p_result -> 'recovered_acl') IS DISTINCT FROM 'array'
    THEN
        RAISE EXCEPTION 'flashback_accept_backup_restore: helper result does not match the immutable request';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements(p_result -> 'recovered_acl') AS entry
        WHERE COALESCE(entry ->> 'grantee', '') = ''
           OR COALESCE(entry ->> 'privilege', '') NOT IN (
               'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE',
               'REFERENCES', 'TRIGGER', 'MAINTAIN'
           )
           OR jsonb_typeof(entry -> 'is_grantable') IS DISTINCT FROM 'boolean'
    ) THEN
        RAISE EXCEPTION 'flashback_accept_backup_restore: invalid recovered ACL manifest';
    END IF;

    v_artifact_schema := p_result ->> 'artifact_schema';
    v_artifact_table := p_result ->> 'artifact_table';
    v_artifact_sha := p_result ->> 'artifact_sha256';
    IF v_artifact_schema IS DISTINCT FROM 'flashback_import'
       OR COALESCE(v_artifact_table, '') !~ '^r_[0-9a-f]{16}$'
       OR v_artifact_table IS DISTINCT FROM 'r_' || left(flashback_sha256(p_request_id), 16)
       OR COALESCE(v_artifact_sha, '') !~ '^[0-9a-f]{64}$'
       OR COALESCE(p_result ->> 'artifact_schema_sha256', '') !~ '^[0-9a-f]{64}$'
       OR COALESCE(p_result ->> 'recovered_fingerprint', '') = ''
    THEN
        RAISE EXCEPTION 'flashback_accept_backup_restore: invalid artifact manifest';
    END IF;

    v_artifact_oid := to_regclass(format('%I.%I', v_artifact_schema, v_artifact_table));
    IF v_artifact_oid IS NULL THEN
        RAISE EXCEPTION
            'flashback_accept_backup_restore: imported artifact table %.% does not exist',
            v_artifact_schema, v_artifact_table;
    END IF;
    -- An accepted artifact is extension payload, not an application table.
    -- Membership prevents a logical dump taken while the request is waiting
    -- for finalize from exporting a meaningless orphan relation.
    PERFORM public.flashback_own_payload_table(v_artifact_oid::regclass);

    UPDATE flashback.backup_restore_requests
       SET status = 'artifact_ready',
           result_json = p_result,
           artifact_schema = v_artifact_schema,
           artifact_table = v_artifact_table,
           artifact_sha256 = v_artifact_sha,
           updated_at = clock_timestamp()
     WHERE request_id = p_request_id;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_finalize_backup_restore(p_request_id text)
RETURNS oid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_request flashback.backup_restore_requests%ROWTYPE;
    v_shadow_oid oid;
    v_current_oid oid;
    v_new_oid oid;
    v_schema_hash text;
    v_fingerprint text;
    v_ddl_info jsonb;
    v_owner_name text;
    v_acl_rec record;
    v_existing_acl_rec record;
BEGIN
    SELECT * INTO v_request
    FROM flashback.backup_restore_requests
    WHERE request_id = p_request_id
    FOR UPDATE;

    IF v_request.request_id IS NULL OR v_request.status <> 'artifact_ready' THEN
        RAISE EXCEPTION 'flashback_finalize_backup_restore: request % is not artifact_ready', p_request_id;
    END IF;

    v_owner_name := v_request.result_json ->> 'recovered_owner';
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = v_owner_name) THEN
        RAISE EXCEPTION 'flashback_finalize_backup_restore: recovered owner role % does not exist',
            v_owner_name;
    END IF;
    FOR v_acl_rec IN
        SELECT entry ->> 'grantee' AS grantee,
               entry ->> 'privilege' AS privilege,
               (entry ->> 'is_grantable')::boolean AS is_grantable
        FROM jsonb_array_elements(v_request.result_json -> 'recovered_acl') AS entry
    LOOP
        IF v_acl_rec.grantee <> 'PUBLIC'
           AND NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = v_acl_rec.grantee)
        THEN
            RAISE EXCEPTION 'flashback_finalize_backup_restore: recovered grantee role % does not exist',
                v_acl_rec.grantee;
        END IF;
        IF v_acl_rec.privilege NOT IN (
            'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE',
            'REFERENCES', 'TRIGGER', 'MAINTAIN'
        ) THEN
            RAISE EXCEPTION 'flashback_finalize_backup_restore: invalid recovered privilege %',
                v_acl_rec.privilege;
        END IF;
    END LOOP;

    PERFORM pg_advisory_xact_lock(
        hashtextextended('pg_flashback:backup:' || v_request.rel_oid::text, 0)
    );

    v_current_oid := to_regclass(format('%I.%I', v_request.schema_name, v_request.table_name));
    IF v_current_oid IS NOT NULL AND v_current_oid <> v_request.rel_oid THEN
        RAISE EXCEPTION 'flashback_finalize_backup_restore: live table identity changed (expected %, actual %)',
            v_request.rel_oid, v_current_oid;
    END IF;
    IF v_current_oid IS NOT NULL THEN
        EXECUTE format(
            'LOCK TABLE %I.%I IN ACCESS EXCLUSIVE MODE',
            v_request.schema_name, v_request.table_name
        );
        v_current_oid := to_regclass(format('%I.%I', v_request.schema_name, v_request.table_name));
        IF v_current_oid <> v_request.rel_oid THEN
            RAISE EXCEPTION 'flashback_finalize_backup_restore: live table identity changed while locking';
        END IF;
    END IF;

    v_shadow_oid := to_regclass(format('%I.%I', v_request.artifact_schema, v_request.artifact_table));
    IF v_shadow_oid IS NULL THEN
        RAISE EXCEPTION 'flashback_finalize_backup_restore: imported artifact table %.% does not exist',
            v_request.artifact_schema, v_request.artifact_table;
    END IF;
    IF (SELECT relkind FROM pg_class WHERE oid = v_shadow_oid) <> 'r' THEN
        RAISE EXCEPTION 'flashback_finalize_backup_restore: imported artifact is not an ordinary table';
    END IF;
    EXECUTE format(
        'LOCK TABLE %I.%I IN ACCESS EXCLUSIVE MODE',
        v_request.artifact_schema, v_request.artifact_table
    );

    v_schema_hash := flashback_helper_schema_sha256(v_shadow_oid);
    IF v_schema_hash IS DISTINCT FROM v_request.result_json ->> 'artifact_schema_sha256' THEN
        RAISE EXCEPTION 'flashback_finalize_backup_restore: imported schema fingerprint mismatch';
    END IF;

    EXECUTE format(
        'SELECT count(*)::text || ''|'' || COALESCE(bit_xor(hashtextextended(row_to_json(t)::text, 0)), 0)::text FROM %I.%I AS t',
        v_request.artifact_schema, v_request.artifact_table
    ) INTO v_fingerprint;
    IF v_fingerprint IS DISTINCT FROM v_request.result_json ->> 'recovered_fingerprint' THEN
        RAISE EXCEPTION 'flashback_finalize_backup_restore: imported row fingerprint mismatch';
    END IF;

    SELECT jsonb_build_object(
        'columns', sv.columns,
        'primary_key', sv.primary_key,
        'constraints', COALESCE(sv.constraints -> 'check_unique_fk', '[]'::jsonb),
        'indexes', COALESCE(sv.constraints -> 'indexes', '[]'::jsonb),
        'partition_by', sv.constraints -> 'partition_by',
        'partitions', COALESCE(sv.constraints -> 'partitions', '[]'::jsonb),
        'triggers', COALESCE(sv.constraints -> 'triggers', '[]'::jsonb),
        'rls_policies', COALESCE(sv.constraints -> 'rls_policies', '[]'::jsonb),
        'rls_enabled', COALESCE((sv.constraints -> 'rls_enabled')::boolean, false)
    ) INTO v_ddl_info
    FROM flashback.schema_versions sv
    WHERE sv.rel_oid = v_request.rel_oid
      AND sv.schema_version = v_request.expected_schema_version;

    IF v_ddl_info IS NULL THEN
        RAISE EXCEPTION 'flashback_finalize_backup_restore: target schema metadata is missing';
    END IF;

    BEGIN
        -- The validated artifact is about to become the application's live
        -- table. Release extension membership in the same transaction before
        -- the rename; rollback restores membership if finalization fails.
        PERFORM public.flashback_release_payload_table(v_shadow_oid::regclass);
        PERFORM flashback_set_restore_in_progress(true);
        v_new_oid := flashback_finalize_shadow_swap(
            v_request.schema_name,
            v_request.table_name,
            v_request.artifact_schema,
            v_request.artifact_table,
            v_ddl_info
        );
        PERFORM flashback_set_restore_in_progress(false);
    EXCEPTION WHEN OTHERS THEN
        PERFORM flashback_set_restore_in_progress(false);
        RAISE;
    END;

    -- pg_restore intentionally imports without ownership or ACL changes. Apply
    -- the target-time security metadata only after the structural swap, in
    -- the same transaction, so any missing role or invalid GRANT rolls the
    -- entire restore back.
    EXECUTE format(
        'ALTER TABLE %I.%I OWNER TO %I',
        v_request.schema_name, v_request.table_name, v_owner_name
    );
    FOR v_existing_acl_rec IN
        SELECT DISTINCT
               CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                    ELSE pg_get_userbyid(acl.grantee) END AS grantee
        FROM pg_class c
        CROSS JOIN LATERAL aclexplode(c.relacl) AS acl
        WHERE c.oid = v_new_oid
    LOOP
        IF v_existing_acl_rec.grantee = 'PUBLIC' THEN
            EXECUTE format(
                'REVOKE ALL PRIVILEGES ON TABLE %I.%I FROM PUBLIC',
                v_request.schema_name, v_request.table_name
            );
        ELSIF v_existing_acl_rec.grantee <> v_owner_name THEN
            EXECUTE format(
                'REVOKE ALL PRIVILEGES ON TABLE %I.%I FROM %I',
                v_request.schema_name, v_request.table_name,
                v_existing_acl_rec.grantee
            );
        END IF;
    END LOOP;
    FOR v_acl_rec IN
        SELECT entry ->> 'grantee' AS grantee,
               entry ->> 'privilege' AS privilege,
               (entry ->> 'is_grantable')::boolean AS is_grantable
        FROM jsonb_array_elements(v_request.result_json -> 'recovered_acl') AS entry
    LOOP
        IF v_acl_rec.grantee = v_owner_name THEN
            CONTINUE;
        END IF;
        IF v_acl_rec.grantee = 'PUBLIC' THEN
            EXECUTE format(
                'GRANT %s ON TABLE %I.%I TO PUBLIC%s',
                v_acl_rec.privilege,
                v_request.schema_name, v_request.table_name,
                CASE WHEN v_acl_rec.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END
            );
        ELSE
            EXECUTE format(
                'GRANT %s ON TABLE %I.%I TO %I%s',
                v_acl_rec.privilege,
                v_request.schema_name, v_request.table_name,
                v_acl_rec.grantee,
                CASE WHEN v_acl_rec.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END
            );
        END IF;
    END LOOP;

    UPDATE flashback.tracked_tables
       SET rel_oid = v_new_oid,
           schema_name = v_request.schema_name,
           table_name = v_request.table_name
     WHERE rel_oid = v_request.rel_oid;
    UPDATE flashback.schema_versions SET rel_oid = v_new_oid WHERE rel_oid = v_request.rel_oid;
    UPDATE flashback.delta_log SET rel_oid = v_new_oid WHERE rel_oid = v_request.rel_oid;
    UPDATE flashback.staging_events SET rel_oid = v_new_oid WHERE rel_oid = v_request.rel_oid;

    UPDATE flashback.backup_restore_requests
       SET status = 'completed',
           completed_at = clock_timestamp(),
           updated_at = clock_timestamp()
     WHERE request_id = p_request_id;

    RETURN v_new_oid;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_fail_backup_restore(
    p_request_id text,
    p_error_message text,
    p_cancelled boolean DEFAULT false
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback
AS $$
BEGIN
    UPDATE flashback.backup_restore_requests
       SET status = CASE WHEN p_cancelled THEN 'cancelled' ELSE 'failed' END,
           error_message = left(p_error_message, 4000),
           updated_at = clock_timestamp(),
           completed_at = clock_timestamp()
     WHERE request_id = p_request_id
       AND status IN ('pending', 'running');
    IF NOT FOUND THEN
        RAISE EXCEPTION 'flashback_fail_backup_restore: request % cannot transition to failed', p_request_id;
    END IF;
END;
$$;
