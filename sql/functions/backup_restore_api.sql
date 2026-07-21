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
    v_tracking_id bigint;
    v_bound_tracking_id bigint;
    v_generation_id bigint;
    v_boundary_xid bigint;
    v_provisional_lsn pg_lsn;
    v_schema_def jsonb;
    v_schema_hash text;
    v_tracked_since timestamptz := clock_timestamp();
    v_stream_id bigint;
BEGIN
    IF helper_profile IS NULL OR helper_profile !~ '^[A-Za-z0-9_-]+$' THEN
        RAISE EXCEPTION 'flashback_track_backup: invalid helper profile';
    END IF;
    IF txid_current_if_assigned() IS NOT NULL THEN
        RAISE EXCEPTION 'pg_flashback: flashback_track_backup() must run before any write in a dedicated transaction'
            USING HINT = 'Commit or roll back the current transaction, then call flashback_track_backup() alone.';
    END IF;
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'pg_flashback: flashback_track_backup() requires READ COMMITTED isolation';
    END IF;
    IF current_setting('wal_level') <> 'logical' THEN
        RAISE EXCEPTION 'pg_flashback: flashback_track_backup() requires wal_level=logical so the tracking marker COMMIT LSN can be resolved';
    END IF;

    -- Marker resolution and frontier advancement require the admitted capture
    -- worker. Refuse before creating a lifecycle when admission is false.
    PERFORM flashback_require_admitted_capture_worker('flashback_track_backup()');

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
    IF EXISTS (SELECT 1 FROM pg_class WHERE oid = v_rel_oid AND relpersistence <> 'p') THEN
        RAISE EXCEPTION 'flashback_track_backup: first release supports permanent LOGGED tables only';
    END IF;

    -- Create the marker-resolution slot before taking the stream lock so the
    -- background worker cannot deadlock against slot creation.
    IF NOT EXISTS (
        SELECT 1 FROM pg_replication_slots
        WHERE slot_name = flashback_effective_slot_name()
          AND database = current_database()
    ) THEN
        IF EXISTS (
            SELECT 1 FROM pg_replication_slots
            WHERE slot_name = flashback_effective_slot_name()
        ) THEN
            RAISE EXCEPTION 'flashback_track_backup: replication slot % already exists but belongs to another database',
                flashback_effective_slot_name();
        END IF;
        BEGIN
            PERFORM pg_create_logical_replication_slot(
                flashback_effective_slot_name(),
                'pg_flashback'
            );
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'flashback_track_backup: could not create logical slot %: %',
                flashback_effective_slot_name(), SQLERRM
                USING HINT = 'Call flashback_track_backup() as the first write in a dedicated READ COMMITTED transaction.';
        END;
    END IF;

    PERFORM pg_advisory_xact_lock(
        358945::integer,
        (SELECT oid::integer FROM pg_database WHERE datname = current_database())
    );

    -- Resolve the marker COMMIT through the database WAL stream without binding
    -- the backup generation to that stream (backup gens keep stream_id NULL).
    v_stream_id := flashback_ensure_active_wal_stream();
    IF v_stream_id IS NULL THEN
        RAISE EXCEPTION 'flashback_track_backup: WAL stream could not be activated for marker resolution';
    END IF;

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
          AND (tt.rel_oid = v_rel_oid
               OR (tt.schema_name = v_schema_name AND tt.table_name = v_table_name))
    ) THEN
        RAISE EXCEPTION 'flashback_track_backup: %.% already has a tracking lifecycle; untrack it before creating a new generation',
            v_schema_name, v_table_name;
    END IF;

    v_tracking_id := nextval('flashback.tracking_id_seq');
    PERFORM pg_advisory_xact_lock(358944::integer, hashint8(v_tracking_id));

    EXECUTE format('LOCK TABLE %I.%I IN SHARE ROW EXCLUSIVE MODE', v_schema_name, v_table_name);
    IF to_regclass(format('%I.%I', v_schema_name, v_table_name)) IS DISTINCT FROM v_rel_oid THEN
        RAISE EXCEPTION 'flashback_track_backup: table identity changed while tracking';
    END IF;

    v_schema_def := COALESCE(flashback_collect_schema_def(v_rel_oid), '{}'::jsonb);
    v_schema_hash := flashback_helper_schema_sha256(v_rel_oid);
    v_boundary_xid := (txid_current() % 4294967296)::bigint;
    v_provisional_lsn := pg_current_wal_insert_lsn();

    INSERT INTO flashback.tracked_tables (
        tracking_id, rel_oid, schema_name, table_name, base_snapshot_table,
        schema_version, recovery_profile, helper_profile,
        coverage_start_lsn, coverage_end_lsn,
        tracked_since, checkpoint_interval, retention_interval, is_active
    ) VALUES (
        v_tracking_id, v_rel_oid, v_schema_name, v_table_name, NULL,
        1, 'backup', helper_profile,
        NULL, NULL,
        v_tracked_since, interval '15 minutes', interval '7 days', true
    );

    SELECT tracking_id INTO v_bound_tracking_id
    FROM flashback.tracked_tables
    WHERE rel_oid = v_rel_oid AND is_active;
    IF v_bound_tracking_id IS DISTINCT FROM v_tracking_id THEN
        RAISE EXCEPTION 'pg_flashback: concurrent first-track bound table % to lifecycle %, expected %',
            target_table, v_bound_tracking_id, v_tracking_id;
    END IF;

    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, stream_id, recovery_profile, state,
        boundary_kind, rel_oid_at_boundary, boundary_snapshot_id, backup_anchor_id,
        boundary_xid, boundary_marker, details
    ) VALUES (
        v_tracking_id, 1, NULL, 'backup', 'building',
        'initial_track', v_rel_oid, NULL, NULL,
        v_boundary_xid,
        format('initial-backup-track:%s:%s', v_tracking_id, v_boundary_xid),
        jsonb_build_object(
            'provisional_insert_lsn', v_provisional_lsn,
            'helper_profile', helper_profile,
            'marker_stream_id', v_stream_id
        )
    ) RETURNING generation_id INTO v_generation_id;

    DELETE FROM flashback.schema_versions WHERE rel_oid = v_rel_oid;
    DELETE FROM flashback.delta_log WHERE rel_oid = v_rel_oid;
    DELETE FROM flashback.staging_events WHERE rel_oid = v_rel_oid;

    INSERT INTO flashback.schema_versions (
        rel_oid, tracking_id, generation_id, stream_id, source_xid,
        schema_version, applied_at, applied_lsn, committed_at, commit_lsn,
        columns, primary_key, constraints, helper_schema_sha256
    ) VALUES (
        v_rel_oid, v_tracking_id, v_generation_id, NULL, v_boundary_xid,
        1, v_tracked_since, v_provisional_lsn, NULL, NULL,
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

    PERFORM pg_logical_emit_message(
        true,
        'pg_flashback',
        jsonb_build_object(
            'op', 'BOUNDARY',
            'kind', 'initial_backup_track',
            'tracking_id', v_tracking_id,
            'generation_id', v_generation_id
        )::text
    );

    RETURN true;
END;
$$;

-- Legacy controller assertion. Fail closed: recoverability requires a
-- one-time verified FULL backup proof consumed after the tracking marker.
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
BEGIN
    RAISE EXCEPTION 'flashback_set_backup_coverage: legacy coverage assertion is rejected'
        USING ERRCODE = 'feature_not_supported',
              HINT = 'Install a verified FULL backup proof via flashback_install_verified_backup_proof() then consume it with flashback_consume_verified_backup_proof().';
END;
$$;

-- Public raw activation is intentionally closed. Caller-supplied LSNs and
-- manifest digests are not recoverability evidence.
CREATE OR REPLACE FUNCTION flashback_activate_backup_anchor(
    p_target_table text,
    p_repository_key text,
    p_stanza text,
    p_backup_label text,
    p_database_system_identifier numeric,
    p_timeline_id bigint,
    p_manifest_reference text,
    p_manifest_sha256 text,
    p_backup_start_lsn pg_lsn,
    p_backup_stop_lsn pg_lsn,
    p_verified_at timestamptz DEFAULT clock_timestamp()
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
BEGIN
    RAISE EXCEPTION 'flashback_activate_backup_anchor: raw caller-supplied activation is rejected'
        USING ERRCODE = 'feature_not_supported',
              HINT = 'Only flashback_consume_verified_backup_proof() may activate coverage from a one-time recovery-agent proof.';
END;
$$;

-- Public raw frontier advance is intentionally closed.
CREATE OR REPLACE FUNCTION flashback_advance_backup_frontier(
    p_target_table text,
    p_valid_through_lsn pg_lsn,
    p_timeline_id bigint DEFAULT NULL
)
RETURNS pg_lsn
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
BEGIN
    RAISE EXCEPTION 'flashback_advance_backup_frontier: raw caller-supplied frontier advance is rejected'
        USING ERRCODE = 'feature_not_supported',
              HINT = 'Only flashback_consume_verified_wal_frontier_proof() may advance coverage from a one-time archive verification proof.';
END;
$$;

CREATE OR REPLACE FUNCTION flashback_caller_may_install_backup_proof()
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
    SELECT COALESCE(
        (SELECT rolsuper FROM pg_roles WHERE rolname = session_user),
        false
    )
    OR pg_has_role(session_user, 'flashback_recovery_agent', 'member');
$$;

CREATE OR REPLACE FUNCTION flashback_backup_anchor_verification_context(
    p_tracking_id bigint
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_context jsonb;
BEGIN
    IF NOT flashback_caller_may_install_backup_proof() THEN
        RAISE EXCEPTION 'backup anchor verification context requires flashback_recovery_agent'
            USING ERRCODE = 'insufficient_privilege';
    END IF;

    SELECT jsonb_build_object(
        'tracking_id', tt.tracking_id,
        'helper_profile', tt.helper_profile,
        'generation_id', cg.generation_id,
        'boundary_kind', cg.boundary_kind,
        'marker_commit_lsn', COALESCE(
            cg.details->>'tracking_marker_lsn',
            cg.boundary_lsn::text
        ),
        'predecessor_generation_id', NULLIF(cg.details->>'predecessor_generation_id', '')::bigint,
        'predecessor_backup_label', pred_ba.backup_label,
        'predecessor_backup_stop_lsn', pred.boundary_lsn::text,
        'predecessor_valid_through_lsn', pred.valid_through_lsn::text,
        'database_system_identifier', (SELECT system_identifier::text FROM pg_control_system()),
        'timeline_id', (SELECT timeline_id FROM pg_control_checkpoint()),
        'wal_segment_size_bytes', pg_size_bytes(current_setting('wal_segment_size'))
    )
      INTO v_context
    FROM flashback.tracked_tables tt
    JOIN flashback.coverage_generations cg USING (tracking_id)
    LEFT JOIN flashback.coverage_generations pred
      ON pred.generation_id = NULLIF(cg.details->>'predecessor_generation_id', '')::bigint
    LEFT JOIN flashback.backup_anchors pred_ba
      ON pred_ba.backup_anchor_id = pred.backup_anchor_id
     AND pred_ba.tracking_id = pred.tracking_id
    WHERE tt.tracking_id = p_tracking_id
      AND tt.is_active
      AND tt.recovery_profile = 'backup'
      AND cg.recovery_profile = 'backup'
      AND cg.state = 'building'
    ORDER BY cg.generation_no DESC
    LIMIT 1;

    IF v_context IS NULL
       OR NULLIF(v_context->>'marker_commit_lsn', '') IS NULL
    THEN
        RAISE EXCEPTION 'no resolved building backup generation for tracking_id %',
            p_tracking_id;
    END IF;
    RETURN v_context;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_backup_frontier_verification_context(
    p_tracking_id bigint
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_context jsonb;
BEGIN
    IF NOT flashback_caller_may_install_backup_proof() THEN
        RAISE EXCEPTION 'backup frontier verification context requires flashback_recovery_agent'
            USING ERRCODE = 'insufficient_privilege';
    END IF;

    SELECT jsonb_build_object(
        'tracking_id', tt.tracking_id,
        'helper_profile', tt.helper_profile,
        'generation_id', cg.generation_id,
        'backup_anchor_id', ba.backup_anchor_id,
        'repository_key', ba.repository_key,
        'stanza', ba.stanza,
        'backup_label', ba.backup_label,
        'backup_stop_lsn', ba.backup_stop_lsn,
        'anchor_timeline_id', ba.timeline_id,
        'live_timeline_id', (SELECT timeline_id FROM pg_control_checkpoint()),
        'database_system_identifier', (SELECT system_identifier::text FROM pg_control_system()),
        'wal_segment_size_bytes', pg_size_bytes(current_setting('wal_segment_size'))
    )
      INTO v_context
    FROM flashback.tracked_tables tt
    JOIN flashback.coverage_generations cg USING (tracking_id)
    JOIN flashback.backup_anchors ba
      ON ba.backup_anchor_id = cg.backup_anchor_id
     AND ba.tracking_id = cg.tracking_id
    WHERE tt.tracking_id = p_tracking_id
      AND tt.is_active
      AND tt.recovery_profile = 'backup'
      AND cg.recovery_profile = 'backup'
      AND cg.state = 'active';

    IF v_context IS NULL THEN
        RAISE EXCEPTION 'no active anchored backup generation for tracking_id %',
            p_tracking_id;
    END IF;
    RETURN v_context;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_active_backup_labels()
RETURNS text[]
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
BEGIN
    IF NOT flashback_caller_may_install_backup_proof() THEN
        RAISE EXCEPTION 'active backup labels require flashback_recovery_agent'
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    RETURN ARRAY(
        SELECT DISTINCT ba.backup_label
        FROM flashback.coverage_generations cg
        JOIN flashback.backup_anchors ba
          ON ba.backup_anchor_id = cg.backup_anchor_id
         AND ba.tracking_id = cg.tracking_id
        JOIN flashback.tracked_tables tt
          ON tt.tracking_id = cg.tracking_id
        WHERE tt.is_active
          AND cg.state IN ('active', 'sealed')
        ORDER BY ba.backup_label
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_begin_backup_expire(
    p_helper_profile text,
    p_repository_key text,
    p_stanza text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_labels text[];
    v_lease flashback.backup_expire_leases%ROWTYPE;
BEGIN
    IF NOT flashback_caller_may_install_backup_proof() THEN
        RAISE EXCEPTION 'backup expiration lease requires flashback_recovery_agent'
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    IF p_helper_profile IS NULL OR btrim(p_helper_profile) = ''
       OR p_repository_key IS NULL OR btrim(p_repository_key) = ''
       OR p_stanza IS NULL OR btrim(p_stanza) = ''
    THEN
        RAISE EXCEPTION 'backup expiration lease identity is incomplete';
    END IF;

    -- Serialize atomically with the shared lock in the generation guard.
    PERFORM pg_advisory_xact_lock(358946::integer, 0);
    SELECT ARRAY(
        SELECT DISTINCT ba.backup_label
        FROM flashback.coverage_generations cg
        JOIN flashback.backup_anchors ba
          ON ba.backup_anchor_id = cg.backup_anchor_id
         AND ba.tracking_id = cg.tracking_id
        JOIN flashback.tracked_tables tt
          ON tt.tracking_id = cg.tracking_id
        WHERE tt.is_active
          AND cg.state IN ('active', 'sealed')
        ORDER BY ba.backup_label
    ) INTO v_labels;
    IF cardinality(v_labels) > 0 THEN
        RETURN jsonb_build_object('status', 'protected', 'labels', to_jsonb(v_labels));
    END IF;

    SELECT * INTO v_lease
    FROM flashback.backup_expire_leases
    WHERE state = 'active'
    FOR UPDATE;
    IF v_lease.lease_id IS NOT NULL THEN
        IF v_lease.helper_profile IS DISTINCT FROM p_helper_profile
           OR v_lease.repository_key IS DISTINCT FROM p_repository_key
           OR v_lease.stanza IS DISTINCT FROM p_stanza
        THEN
            RETURN jsonb_build_object(
                'status', 'busy',
                'lease_id', v_lease.lease_id,
                'helper_profile', v_lease.helper_profile,
                'repository_key', v_lease.repository_key,
                'stanza', v_lease.stanza
            );
        END IF;
        RETURN jsonb_build_object('status', 'resumed', 'lease_id', v_lease.lease_id);
    END IF;

    INSERT INTO flashback.backup_expire_leases (
        helper_profile, repository_key, stanza
    ) VALUES (
        p_helper_profile, p_repository_key, p_stanza
    ) RETURNING * INTO v_lease;
    RETURN jsonb_build_object('status', 'started', 'lease_id', v_lease.lease_id);
END;
$$;

CREATE OR REPLACE FUNCTION flashback_complete_backup_expire(
    p_lease_id bigint,
    p_helper_profile text,
    p_repository_key text,
    p_stanza text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_lease flashback.backup_expire_leases%ROWTYPE;
BEGIN
    IF NOT flashback_caller_may_install_backup_proof() THEN
        RAISE EXCEPTION 'backup expiration lease completion requires flashback_recovery_agent'
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    PERFORM pg_advisory_xact_lock(358946::integer, 0);
    SELECT * INTO v_lease
    FROM flashback.backup_expire_leases
    WHERE lease_id = p_lease_id
    FOR UPDATE;
    IF v_lease.lease_id IS NULL
       OR v_lease.helper_profile IS DISTINCT FROM p_helper_profile
       OR v_lease.repository_key IS DISTINCT FROM p_repository_key
       OR v_lease.stanza IS DISTINCT FROM p_stanza
    THEN
        RAISE EXCEPTION 'backup expiration lease identity mismatch';
    END IF;
    IF v_lease.state = 'completed' THEN
        RETURN jsonb_build_object('status', 'completed', 'lease_id', p_lease_id);
    END IF;

    UPDATE flashback.backup_expire_leases
       SET state = 'completed',
           completed_at = clock_timestamp(),
           completed_by = session_user
     WHERE lease_id = p_lease_id
       AND state = 'active';
    RETURN jsonb_build_object('status', 'completed', 'lease_id', p_lease_id);
END;
$$;

-- Create (or resume) a building successor for advancing to a newer FULL.
-- Does not start a backup and does not activate coverage; verify-anchor does.
CREATE OR REPLACE FUNCTION flashback_begin_backup_anchor_advancement(
    p_tracking_id bigint
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_tracked record;
    v_active record;
    v_building record;
    v_generation_id bigint;
    v_generation_no integer;
    v_marker_lsn pg_lsn;
    v_rel_oid oid;
BEGIN
    IF NOT flashback_caller_may_install_backup_proof() THEN
        RAISE EXCEPTION 'backup anchor advancement requires flashback_recovery_agent'
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    IF p_tracking_id IS NULL THEN
        RAISE EXCEPTION 'flashback_begin_backup_anchor_advancement: tracking_id is required';
    END IF;

    PERFORM pg_advisory_xact_lock(358944::integer, hashint8(p_tracking_id));
    -- Serialize with expire leases.
    PERFORM pg_advisory_xact_lock_shared(358946::integer, 0);

    SELECT tt.* INTO v_tracked
    FROM flashback.tracked_tables tt
    WHERE tt.tracking_id = p_tracking_id
      AND tt.is_active
      AND tt.recovery_profile = 'backup'
    FOR UPDATE;
    IF v_tracked.tracking_id IS NULL THEN
        RAISE EXCEPTION 'flashback_begin_backup_anchor_advancement: no active backup lifecycle %',
            p_tracking_id;
    END IF;

    SELECT cg.* INTO v_building
    FROM flashback.coverage_generations cg
    WHERE cg.tracking_id = p_tracking_id
      AND cg.recovery_profile = 'backup'
      AND cg.state = 'building'
    ORDER BY cg.generation_no DESC
    LIMIT 1
    FOR UPDATE;
    IF v_building.generation_id IS NOT NULL THEN
        IF v_building.boundary_kind = 'full_reanchor' THEN
            RETURN jsonb_build_object(
                'status', 'resumed',
                'tracking_id', p_tracking_id,
                'generation_id', v_building.generation_id,
                'predecessor_generation_id',
                    NULLIF(v_building.details->>'predecessor_generation_id', '')::bigint
            );
        END IF;
        RETURN jsonb_build_object(
            'status', 'blocked',
            'reason', format(
                'building generation %s boundary_kind=%s blocks advancement',
                v_building.generation_id, v_building.boundary_kind
            ),
            'tracking_id', p_tracking_id,
            'generation_id', v_building.generation_id
        );
    END IF;

    SELECT cg.* INTO v_active
    FROM flashback.coverage_generations cg
    WHERE cg.tracking_id = p_tracking_id
      AND cg.recovery_profile = 'backup'
      AND cg.state = 'active'
    FOR UPDATE;
    IF v_active.generation_id IS NULL
       OR v_active.backup_anchor_id IS NULL
       OR v_active.boundary_lsn IS NULL
       OR v_active.valid_through_lsn IS NULL
    THEN
        RETURN jsonb_build_object(
            'status', 'blocked',
            'reason', 'no verified active backup generation to advance from',
            'tracking_id', p_tracking_id
        );
    END IF;
    IF COALESCE(v_active.state_reason, '') IN (
        'timeline_mismatch_frontier_frozen',
        'repository_verification_failed',
        'anchor_missing'
    ) THEN
        RETURN jsonb_build_object(
            'status', 'blocked',
            'reason', format('active generation is frozen (%s)', v_active.state_reason),
            'tracking_id', p_tracking_id,
            'generation_id', v_active.generation_id
        );
    END IF;

    v_marker_lsn := COALESCE(
        NULLIF(v_active.details->>'tracking_marker_lsn', '')::pg_lsn,
        v_tracked.coverage_start_lsn,
        v_active.boundary_lsn
    );
    v_rel_oid := COALESCE(v_tracked.rel_oid, v_active.rel_oid_at_boundary);
    v_generation_no := v_active.generation_no + 1;

    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, stream_id, recovery_profile, state,
        boundary_kind, rel_oid_at_boundary, boundary_snapshot_id, backup_anchor_id,
        boundary_xid, boundary_marker, details
    ) VALUES (
        p_tracking_id, v_generation_no, NULL, 'backup', 'building',
        'full_reanchor', v_rel_oid, NULL, NULL,
        (txid_current() % 4294967296)::bigint,
        format('full-reanchor:%s:%s', p_tracking_id, v_active.generation_id),
        jsonb_build_object(
            'tracking_marker_lsn', v_marker_lsn,
            'predecessor_generation_id', v_active.generation_id,
            'predecessor_backup_anchor_id', v_active.backup_anchor_id,
            'predecessor_boundary_lsn', v_active.boundary_lsn,
            'predecessor_valid_through_lsn', v_active.valid_through_lsn,
            'helper_profile', v_tracked.helper_profile
        )
    ) RETURNING generation_id INTO v_generation_id;

    RETURN jsonb_build_object(
        'status', 'started',
        'tracking_id', p_tracking_id,
        'generation_id', v_generation_id,
        'predecessor_generation_id', v_active.generation_id,
        'predecessor_boundary_lsn', v_active.boundary_lsn,
        'predecessor_valid_through_lsn', v_active.valid_through_lsn
    );
END;
$$;

-- Retire a sealed predecessor only after its exclusive target range is outside
-- the configured retention window. Does not call pgBackRest.
CREATE OR REPLACE FUNCTION flashback_retire_sealed_backup_generation(
    p_generation_id bigint
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_gen record;
    v_tracked record;
    v_cutoff timestamptz;
BEGIN
    IF NOT flashback_caller_may_install_backup_proof() THEN
        RAISE EXCEPTION 'backup generation retirement requires flashback_recovery_agent'
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    IF p_generation_id IS NULL THEN
        RAISE EXCEPTION 'flashback_retire_sealed_backup_generation: generation_id is required';
    END IF;

    SELECT cg.* INTO v_gen
    FROM flashback.coverage_generations cg
    WHERE cg.generation_id = p_generation_id
      AND cg.recovery_profile = 'backup'
    FOR UPDATE;
    IF v_gen.generation_id IS NULL THEN
        RAISE EXCEPTION 'flashback_retire_sealed_backup_generation: generation % not found',
            p_generation_id;
    END IF;

    PERFORM pg_advisory_xact_lock(358944::integer, hashint8(v_gen.tracking_id));
    PERFORM pg_advisory_xact_lock_shared(358946::integer, 0);

    SELECT tt.* INTO v_tracked
    FROM flashback.tracked_tables tt
    WHERE tt.tracking_id = v_gen.tracking_id;

    IF v_gen.state = 'retired' THEN
        RETURN jsonb_build_object(
            'status', 'already_retired',
            'generation_id', p_generation_id,
            'tracking_id', v_gen.tracking_id
        );
    END IF;
    IF v_gen.state IS DISTINCT FROM 'sealed' THEN
        RETURN jsonb_build_object(
            'status', 'blocked',
            'reason', format('generation state is %s, not sealed', v_gen.state),
            'generation_id', p_generation_id
        );
    END IF;
    IF v_gen.superseded_before_lsn IS NULL OR v_gen.sealed_at IS NULL THEN
        RETURN jsonb_build_object(
            'status', 'blocked',
            'reason', 'sealed generation lacks superseded_before/sealed_at',
            'generation_id', p_generation_id
        );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM flashback.coverage_generations succ
        WHERE succ.tracking_id = v_gen.tracking_id
          AND succ.recovery_profile = 'backup'
          AND succ.state IN ('active', 'sealed')
          AND succ.generation_no > v_gen.generation_no
          AND succ.backup_anchor_id IS NOT NULL
    ) THEN
        RETURN jsonb_build_object(
            'status', 'blocked',
            'reason', 'no verified successor generation remains',
            'generation_id', p_generation_id
        );
    END IF;
    IF EXISTS (
        SELECT 1 FROM flashback.backup_restore_requests r
        WHERE r.generation_id = p_generation_id
          AND r.status IN ('pending', 'running')
    ) THEN
        RETURN jsonb_build_object(
            'status', 'blocked',
            'reason', 'restore requests still pin this generation',
            'generation_id', p_generation_id,
            'tracking_id', v_gen.tracking_id
        );
    END IF;

    v_cutoff := clock_timestamp() - COALESCE(v_tracked.retention_interval, interval '7 days');
    IF v_gen.sealed_at > v_cutoff THEN
        RETURN jsonb_build_object(
            'status', 'blocked',
            'reason', 'retention window still includes this predecessor exclusive range',
            'generation_id', p_generation_id,
            'sealed_at', v_gen.sealed_at,
            'retention_cutoff', v_cutoff
        );
    END IF;

    -- state_reason/details stay immutable once sealed; only lifecycle columns
    -- may change under the coverage generation guard.
    UPDATE flashback.coverage_generations
       SET state = 'retired',
           retired_at = clock_timestamp()
     WHERE generation_id = p_generation_id
       AND state = 'sealed';

    RETURN jsonb_build_object(
        'status', 'retired',
        'generation_id', p_generation_id,
        'tracking_id', v_gen.tracking_id,
        'backup_anchor_id', v_gen.backup_anchor_id,
        'superseded_before_lsn', v_gen.superseded_before_lsn
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_active_backup_anchor_contexts()
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
BEGIN
    IF NOT flashback_caller_may_install_backup_proof() THEN
        RAISE EXCEPTION 'active backup anchor contexts require flashback_recovery_agent'
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    RETURN COALESCE((
        SELECT jsonb_agg(jsonb_build_object(
            'tracking_id', cg.tracking_id,
            'generation_id', cg.generation_id,
            'generation_state', cg.state,
            'helper_profile', tt.helper_profile,
            'repository_key', ba.repository_key,
            'stanza', ba.stanza,
            'backup_label', ba.backup_label,
            'database_system_identifier', ba.database_system_identifier::text,
            'timeline_id', ba.timeline_id,
            'manifest_reference', ba.manifest_reference,
            'manifest_sha256', ba.manifest_sha256
        ) ORDER BY cg.tracking_id, cg.generation_no)
        FROM flashback.coverage_generations cg
        JOIN flashback.backup_anchors ba
          ON ba.backup_anchor_id = cg.backup_anchor_id
         AND ba.tracking_id = cg.tracking_id
        JOIN flashback.tracked_tables tt
          ON tt.tracking_id = cg.tracking_id
        WHERE tt.is_active
          AND tt.recovery_profile = 'backup'
          AND cg.recovery_profile = 'backup'
          AND cg.state IN ('active', 'sealed')
    ), '[]'::jsonb);
END;
$$;

CREATE OR REPLACE FUNCTION flashback_freeze_missing_backup_anchor(
    p_tracking_id bigint,
    p_generation_id bigint,
    p_details jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_generation record;
    v_successor record;
    v_gap_id bigint;
BEGIN
    IF NOT flashback_caller_may_install_backup_proof() THEN
        RAISE EXCEPTION 'missing backup anchor freeze requires flashback_recovery_agent'
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    PERFORM pg_advisory_xact_lock(358944::integer, hashint8(p_tracking_id));
    SELECT cg.* INTO v_generation
    FROM flashback.coverage_generations cg
    WHERE cg.tracking_id = p_tracking_id
      AND cg.generation_id = p_generation_id
      AND cg.recovery_profile = 'backup'
      AND cg.state IN ('active', 'sealed')
    FOR UPDATE;
    IF v_generation.generation_id IS NULL THEN
        RAISE EXCEPTION 'active/sealed backup generation % is not bound to tracking_id %',
            p_generation_id, p_tracking_id;
    END IF;

    SELECT cg.generation_id, cg.boundary_lsn, cg.boundary_time
      INTO v_successor
    FROM flashback.coverage_generations cg
    WHERE cg.tracking_id = p_tracking_id
      AND cg.generation_no > v_generation.generation_no
      AND cg.state IN ('active', 'sealed')
      AND cg.boundary_lsn IS NOT NULL
    ORDER BY cg.generation_no
    LIMIT 1;

    UPDATE flashback.coverage_generations
       SET state_reason = 'anchor_missing'
     WHERE generation_id = p_generation_id;

    INSERT INTO flashback.coverage_gaps (
        tracking_id, source_generation_id, reason,
        gap_start_lsn, gap_start_time, lower_bound_inclusive,
        gap_end_lsn, gap_end_time, reanchored_by_generation_id,
        reanchored_at, details
    )
    SELECT
        p_tracking_id, p_generation_id, 'anchor_missing',
        v_generation.boundary_lsn, v_generation.boundary_time, true,
        v_successor.boundary_lsn, v_successor.boundary_time,
        v_successor.generation_id,
        CASE WHEN v_successor.generation_id IS NOT NULL THEN clock_timestamp() END,
        COALESCE(p_details, '{}'::jsonb)
    WHERE NOT EXISTS (
        SELECT 1 FROM flashback.coverage_gaps g
        WHERE g.tracking_id = p_tracking_id
          AND g.source_generation_id = p_generation_id
          AND g.reason = 'anchor_missing'
    )
    RETURNING gap_id INTO v_gap_id;

    RETURN jsonb_build_object(
        'status', 'frozen',
        'tracking_id', p_tracking_id,
        'generation_id', p_generation_id,
        'reason', 'anchor_missing',
        'gap_id', v_gap_id
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_freeze_backup_generation(
    p_tracking_id bigint,
    p_reason text,
    p_details jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_generation record;
    v_gap_id bigint;
BEGIN
    IF NOT flashback_caller_may_install_backup_proof() THEN
        RAISE EXCEPTION 'backup generation freeze requires flashback_recovery_agent'
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    IF p_reason NOT IN ('repository_verification_failed', 'anchor_missing', 'timeline_mismatch') THEN
        RAISE EXCEPTION 'unsupported backup generation freeze reason %', p_reason;
    END IF;

    PERFORM pg_advisory_xact_lock(358944::integer, hashint8(p_tracking_id));
    SELECT cg.* INTO v_generation
    FROM flashback.coverage_generations cg
    WHERE cg.tracking_id = p_tracking_id
      AND cg.recovery_profile = 'backup'
      AND cg.state = 'active'
    FOR UPDATE;
    IF v_generation.generation_id IS NULL THEN
        RAISE EXCEPTION 'no active backup generation for tracking_id %', p_tracking_id;
    END IF;

    UPDATE flashback.coverage_generations
       SET state_reason = CASE p_reason
             WHEN 'timeline_mismatch' THEN 'timeline_mismatch_frontier_frozen'
             ELSE p_reason
           END
     WHERE generation_id = v_generation.generation_id;

    INSERT INTO flashback.coverage_gaps (
        tracking_id, source_generation_id, reason,
        gap_start_lsn, gap_start_time, lower_bound_inclusive, details
    )
    SELECT
        p_tracking_id, v_generation.generation_id, p_reason,
        v_generation.valid_through_lsn, v_generation.valid_through_time,
        false, COALESCE(p_details, '{}'::jsonb)
    WHERE NOT EXISTS (
        SELECT 1 FROM flashback.coverage_gaps g
        WHERE g.tracking_id = p_tracking_id
          AND g.source_generation_id = v_generation.generation_id
          AND g.reanchored_by_generation_id IS NULL
          AND g.reason = p_reason
    )
    RETURNING gap_id INTO v_gap_id;

    RETURN jsonb_build_object(
        'status', 'frozen',
        'tracking_id', p_tracking_id,
        'generation_id', v_generation.generation_id,
        'reason', p_reason,
        'gap_id', v_gap_id
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_backup_proof_result(
    p_verification_request_id text,
    p_tracking_id bigint
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_proof record;
BEGIN
    IF NOT flashback_caller_may_install_backup_proof() THEN
        RAISE EXCEPTION 'backup proof result requires flashback_recovery_agent'
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    SELECT * INTO v_proof
    FROM flashback.verified_backup_proofs
    WHERE verification_request_id = $1;
    IF v_proof.proof_id IS NULL THEN
        RETURN NULL;
    END IF;
    IF v_proof.tracking_id IS DISTINCT FROM p_tracking_id THEN
        RAISE EXCEPTION 'verification request % belongs to another tracking lifecycle',
            p_verification_request_id;
    END IF;
    RETURN jsonb_build_object(
        'proof_id', v_proof.proof_id,
        'tracking_id', v_proof.tracking_id,
        'generation_id', v_proof.consumed_generation_id,
        'helper_profile', v_proof.helper_profile,
        'repository_key', v_proof.repository_key,
        'stanza', v_proof.stanza,
        'backup_label', v_proof.backup_label,
        'timeline_id', v_proof.timeline_id,
        'manifest_reference', v_proof.manifest_reference,
        'manifest_sha256', v_proof.manifest_sha256,
        'verified_lsn', v_proof.backup_stop_lsn,
        'consumed', v_proof.consumed_at IS NOT NULL
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_frontier_proof_result(
    p_verification_request_id text,
    p_tracking_id bigint
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_result jsonb;
BEGIN
    IF NOT flashback_caller_may_install_backup_proof() THEN
        RAISE EXCEPTION 'frontier proof result requires flashback_recovery_agent'
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    SELECT jsonb_build_object(
        'proof_id', p.proof_id,
        'tracking_id', p.tracking_id,
        'generation_id', p.generation_id,
        'helper_profile', p.helper_profile,
        'repository_key', p.repository_key,
        'stanza', p.stanza,
        'backup_label', ba.backup_label,
        'timeline_id', p.timeline_id,
        'archive_proof_sha256', p.archive_proof_sha256,
        'verified_lsn', p.valid_through_lsn,
        'status', p.details->>'consume_status',
        'consumed', p.consumed_at IS NOT NULL
    )
      INTO v_result
    FROM flashback.verified_wal_frontier_proofs p
    LEFT JOIN flashback.coverage_generations cg
      ON cg.generation_id = p.generation_id
     AND cg.tracking_id = p.tracking_id
    LEFT JOIN flashback.backup_anchors ba
      ON ba.backup_anchor_id = cg.backup_anchor_id
     AND ba.tracking_id = cg.tracking_id
    WHERE p.verification_request_id = $1;
    IF v_result IS NULL THEN
        RETURN NULL;
    END IF;
    IF (v_result->>'tracking_id')::bigint IS DISTINCT FROM p_tracking_id THEN
        RAISE EXCEPTION 'verification request % belongs to another tracking lifecycle',
            p_verification_request_id;
    END IF;
    RETURN v_result;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_backup_proof_attestation_payload(
    p_verification_request_id text,
    p_tracking_id bigint,
    p_helper_profile text,
    p_repository_key text,
    p_stanza text,
    p_backup_label text,
    p_database_system_identifier numeric,
    p_timeline_id bigint,
    p_manifest_reference text,
    p_manifest_sha256 text,
    p_backup_start_lsn pg_lsn,
    p_backup_stop_lsn pg_lsn,
    p_activation_mode text DEFAULT 'fresh_full_after_marker',
    p_wal_verified_through_lsn pg_lsn DEFAULT NULL
)
RETURNS text
LANGUAGE sql
IMMUTABLE
SET search_path = pg_catalog
AS $$
    SELECT jsonb_build_object(
        'kind', 'backup-anchor-v2',
        'verification_request_id', p_verification_request_id,
        'tracking_id', p_tracking_id,
        'helper_profile', p_helper_profile,
        'repository_key', p_repository_key,
        'stanza', p_stanza,
        'backup_label', p_backup_label,
        'database_system_identifier', p_database_system_identifier,
        'timeline_id', p_timeline_id,
        'manifest_reference', p_manifest_reference,
        'manifest_sha256', p_manifest_sha256,
        'backup_start_lsn', p_backup_start_lsn::text,
        'backup_stop_lsn', p_backup_stop_lsn::text,
        'activation_mode', COALESCE(NULLIF(btrim(p_activation_mode), ''), 'fresh_full_after_marker'),
        'wal_verified_through_lsn', p_wal_verified_through_lsn::text
    )::text;
$$;

CREATE OR REPLACE FUNCTION flashback_install_verified_backup_proof(
    p_verification_request_id text,
    p_tracking_id bigint,
    p_helper_profile text,
    p_repository_key text,
    p_stanza text,
    p_backup_label text,
    p_database_system_identifier numeric,
    p_timeline_id bigint,
    p_manifest_reference text,
    p_manifest_sha256 text,
    p_backup_start_lsn pg_lsn,
    p_backup_stop_lsn pg_lsn,
    p_verified_at timestamptz DEFAULT clock_timestamp(),
    p_details jsonb DEFAULT '{}'::jsonb,
    p_attestation_hmac text DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_tracked record;
    v_proof_id bigint;
    v_is_superuser boolean;
    v_payload text;
BEGIN
    IF NOT flashback_caller_may_install_backup_proof() THEN
        RAISE EXCEPTION 'flashback_install_verified_backup_proof: only flashback_recovery_agent or a superuser may install proofs'
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    IF p_verification_request_id IS NULL OR btrim(p_verification_request_id) = ''
       OR p_tracking_id IS NULL
       OR p_helper_profile IS NULL OR btrim(p_helper_profile) = ''
       OR p_repository_key IS NULL OR btrim(p_repository_key) = ''
       OR p_stanza IS NULL OR btrim(p_stanza) = ''
       OR p_backup_label IS NULL OR btrim(p_backup_label) = ''
       OR p_manifest_reference IS NULL OR btrim(p_manifest_reference) = ''
       OR p_manifest_sha256 IS NULL OR p_manifest_sha256 !~ '^[0-9a-f]{64}$'
       OR p_backup_start_lsn IS NULL OR p_backup_stop_lsn IS NULL
       OR p_backup_stop_lsn < p_backup_start_lsn
       OR p_database_system_identifier IS NULL
       OR p_timeline_id IS NULL OR p_timeline_id <= 0
    THEN
        RAISE EXCEPTION 'flashback_install_verified_backup_proof: incomplete or invalid proof identity';
    END IF;

    SELECT tt.* INTO v_tracked
    FROM flashback.tracked_tables tt
    WHERE tt.tracking_id = p_tracking_id
      AND tt.is_active
      AND tt.recovery_profile = 'backup';
    IF v_tracked.tracking_id IS NULL THEN
        RAISE EXCEPTION 'flashback_install_verified_backup_proof: no active backup lifecycle for tracking_id %',
            p_tracking_id;
    END IF;
    IF v_tracked.helper_profile IS DISTINCT FROM p_helper_profile THEN
        RAISE EXCEPTION 'flashback_install_verified_backup_proof: helper profile % does not match tracked profile %',
            p_helper_profile, v_tracked.helper_profile;
    END IF;

    SELECT COALESCE(rolsuper, false) INTO v_is_superuser
    FROM pg_roles WHERE rolname = session_user;
    IF NOT COALESCE(v_is_superuser, false) THEN
        v_payload := flashback_backup_proof_attestation_payload(
            p_verification_request_id, p_tracking_id, p_helper_profile,
            p_repository_key, p_stanza, p_backup_label,
            p_database_system_identifier, p_timeline_id,
            p_manifest_reference, p_manifest_sha256,
            p_backup_start_lsn, p_backup_stop_lsn,
            COALESCE(NULLIF(btrim(COALESCE(p_details, '{}'::jsonb) ->> 'activation_mode'), ''),
                     'fresh_full_after_marker'),
            NULLIF(btrim(COALESCE(p_details, '{}'::jsonb) ->> 'wal_verified_through_lsn'), '')::pg_lsn
        );
        IF p_attestation_hmac IS NULL
           OR NOT flashback_verify_proof_hmac(v_payload, p_attestation_hmac)
        THEN
            RAISE EXCEPTION 'flashback_install_verified_backup_proof: valid helper HMAC attestation is required'
                USING ERRCODE = 'insufficient_privilege',
                      HINT = 'Run verify-anchor through the configured recovery helper; raw recovery-agent proof installation is forbidden.';
        END IF;
    END IF;

    INSERT INTO flashback.verified_backup_proofs (
        verification_request_id, tracking_id, helper_profile,
        repository_key, stanza, backup_label, backup_type,
        database_system_identifier, timeline_id,
        manifest_reference, manifest_sha256,
        backup_start_lsn, backup_stop_lsn,
        verified_at, installed_by, details
    ) VALUES (
        p_verification_request_id, p_tracking_id, p_helper_profile,
        p_repository_key, p_stanza, p_backup_label, 'full',
        p_database_system_identifier, p_timeline_id,
        p_manifest_reference, p_manifest_sha256,
        p_backup_start_lsn, p_backup_stop_lsn,
        COALESCE(p_verified_at, clock_timestamp()), session_user,
        COALESCE(p_details, '{}'::jsonb)
    )
    RETURNING proof_id INTO v_proof_id;

    RETURN v_proof_id;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_consume_verified_backup_proof(p_proof_id bigint)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_proof record;
    v_tracked record;
    v_pending record;
    v_predecessor record;
    v_anchor_id bigint;
    v_generation_id bigint;
    v_sysid numeric;
    v_timeline bigint;
    v_marker_lsn pg_lsn;
    v_activation_mode text;
    v_wal_through pg_lsn;
    v_valid_through pg_lsn;
    v_coverage_lower pg_lsn;
BEGIN
    IF p_proof_id IS NULL THEN
        RAISE EXCEPTION 'flashback_consume_verified_backup_proof: proof_id is required';
    END IF;

    SELECT * INTO v_proof
    FROM flashback.verified_backup_proofs
    WHERE proof_id = p_proof_id
    FOR UPDATE;
    IF v_proof.proof_id IS NULL THEN
        RAISE EXCEPTION 'flashback_consume_verified_backup_proof: proof % does not exist', p_proof_id;
    END IF;
    IF v_proof.consumed_at IS NOT NULL THEN
        RAISE EXCEPTION 'flashback_consume_verified_backup_proof: proof % was already consumed',
            p_proof_id;
    END IF;

    SELECT tt.* INTO v_tracked
    FROM flashback.tracked_tables tt
    WHERE tt.tracking_id = v_proof.tracking_id
      AND tt.is_active
      AND tt.recovery_profile = 'backup';
    IF v_tracked.tracking_id IS NULL THEN
        RAISE EXCEPTION 'flashback_consume_verified_backup_proof: proof % is not bound to an active backup lifecycle',
            p_proof_id;
    END IF;
    IF v_tracked.helper_profile IS DISTINCT FROM v_proof.helper_profile THEN
        RAISE EXCEPTION 'flashback_consume_verified_backup_proof: proof profile mismatch';
    END IF;

    PERFORM pg_advisory_xact_lock(358944::integer, hashint8(v_tracked.tracking_id));

    SELECT system_identifier INTO v_sysid FROM pg_control_system();
    SELECT timeline_id INTO v_timeline FROM pg_control_checkpoint();
    IF v_proof.database_system_identifier IS DISTINCT FROM v_sysid THEN
        RAISE EXCEPTION 'flashback_consume_verified_backup_proof: proof system identifier % does not match live cluster %',
            v_proof.database_system_identifier, v_sysid;
    END IF;
    IF v_proof.timeline_id IS DISTINCT FROM v_timeline THEN
        RAISE EXCEPTION 'flashback_consume_verified_backup_proof: proof timeline % does not match live timeline %',
            v_proof.timeline_id, v_timeline;
    END IF;

    SELECT cg.* INTO v_pending
    FROM flashback.coverage_generations cg
    WHERE cg.tracking_id = v_tracked.tracking_id
      AND cg.recovery_profile = 'backup'
      AND cg.state = 'building'
    ORDER BY cg.generation_no DESC
    LIMIT 1
    FOR UPDATE;
    IF v_pending.generation_id IS NULL THEN
        RAISE EXCEPTION 'flashback_consume_verified_backup_proof: no building backup generation for tracking_id %',
            v_tracked.tracking_id;
    END IF;

    v_marker_lsn := COALESCE(
        (v_pending.details ->> 'tracking_marker_lsn')::pg_lsn,
        v_pending.boundary_lsn
    );
    IF v_marker_lsn IS NULL THEN
        RAISE EXCEPTION 'flashback_consume_verified_backup_proof: tracking marker COMMIT LSN is not resolved yet'
            USING HINT = 'Wait for the worker to consume the BOUNDARY commit, then retry.';
    END IF;

    v_activation_mode := COALESCE(
        NULLIF(btrim(COALESCE(v_proof.details, '{}'::jsonb) ->> 'activation_mode'), ''),
        'fresh_full_after_marker'
    );
    -- Post-swap and FULL re-anchor successors require a fresh FULL after the
    -- resolved marker; a pre-marker retained FULL must not re-enter coverage.
    IF v_pending.boundary_kind IN ('post_restore', 'full_reanchor')
       AND v_activation_mode = 'retained_full_plus_wal'
    THEN
        RAISE EXCEPTION 'flashback_consume_verified_backup_proof: boundary_kind % forbids retained_full_plus_wal; take and verify a fresh FULL after the marker',
            v_pending.boundary_kind;
    END IF;
    IF v_activation_mode = 'retained_full_plus_wal' THEN
        -- Retained FULL completed at/before the marker; continuous WAL must
        -- already be attested through the marker in the helper proof.
        IF v_proof.backup_stop_lsn > v_marker_lsn THEN
            RAISE EXCEPTION 'flashback_consume_verified_backup_proof: retained FULL stop % is after marker %; use fresh_full_after_marker or a completed pre-marker FULL',
                v_proof.backup_stop_lsn, v_marker_lsn;
        END IF;
        v_wal_through := NULLIF(
            btrim(COALESCE(v_proof.details, '{}'::jsonb) ->> 'wal_verified_through_lsn'),
            ''
        )::pg_lsn;
        IF v_wal_through IS NULL OR v_wal_through < v_marker_lsn THEN
            RAISE EXCEPTION 'flashback_consume_verified_backup_proof: retained FULL requires wal_verified_through_lsn >= marker %',
                v_marker_lsn;
        END IF;
        v_valid_through := v_wal_through;
    ELSIF v_activation_mode = 'fresh_full_after_marker' THEN
        IF v_proof.backup_start_lsn <= v_marker_lsn THEN
            RAISE EXCEPTION 'flashback_consume_verified_backup_proof: FULL backup start % must be strictly after marker COMMIT %',
                v_proof.backup_start_lsn, v_marker_lsn;
        END IF;
        v_valid_through := v_proof.backup_stop_lsn;
    ELSE
        RAISE EXCEPTION 'flashback_consume_verified_backup_proof: unsupported activation_mode %',
            v_activation_mode;
    END IF;

    SELECT ba.backup_anchor_id, cg.generation_id
      INTO v_anchor_id, v_generation_id
    FROM flashback.backup_anchors ba
    JOIN flashback.coverage_generations cg
      ON cg.backup_anchor_id = ba.backup_anchor_id
     AND cg.tracking_id = ba.tracking_id
    WHERE ba.tracking_id = v_tracked.tracking_id
      AND ba.backup_label = v_proof.backup_label
      AND ba.manifest_sha256 = v_proof.manifest_sha256
      AND ba.backup_start_lsn = v_proof.backup_start_lsn
      AND ba.backup_stop_lsn = v_proof.backup_stop_lsn
      AND cg.state = 'active'
    LIMIT 1;
    IF v_generation_id IS NOT NULL THEN
        UPDATE flashback.verified_backup_proofs
           SET consumed_at = clock_timestamp(),
               consumed_generation_id = v_generation_id
         WHERE proof_id = p_proof_id
           AND consumed_at IS NULL;
        RETURN v_generation_id;
    END IF;

    SELECT cg.* INTO v_predecessor
    FROM flashback.coverage_generations cg
    WHERE cg.tracking_id = v_tracked.tracking_id
      AND cg.recovery_profile = 'backup'
      AND cg.state = 'active'
    FOR UPDATE;

    IF v_predecessor.generation_id IS NOT NULL THEN
        IF v_pending.boundary_kind IS DISTINCT FROM 'full_reanchor' THEN
            RAISE EXCEPTION 'flashback_consume_verified_backup_proof: an active backup generation already exists';
        END IF;
        IF v_activation_mode IS DISTINCT FROM 'fresh_full_after_marker' THEN
            RAISE EXCEPTION 'flashback_consume_verified_backup_proof: full_reanchor requires fresh_full_after_marker';
        END IF;
        IF v_predecessor.boundary_lsn IS NULL
           OR v_predecessor.valid_through_lsn IS NULL
           OR v_predecessor.backup_anchor_id IS NULL
        THEN
            RAISE EXCEPTION 'flashback_consume_verified_backup_proof: predecessor is not a verified active anchor';
        END IF;
        -- No coverage gap: successor physical boundary must already be covered
        -- by the predecessor's proven WAL frontier.
        IF v_proof.backup_stop_lsn <= v_predecessor.boundary_lsn THEN
            RAISE EXCEPTION 'flashback_consume_verified_backup_proof: successor FULL stop % is not after predecessor boundary %',
                v_proof.backup_stop_lsn, v_predecessor.boundary_lsn;
        END IF;
        IF v_proof.backup_stop_lsn > v_predecessor.valid_through_lsn THEN
            RAISE EXCEPTION 'flashback_consume_verified_backup_proof: successor FULL stop % is beyond predecessor valid_through %; advance the frontier or choose an earlier FULL',
                v_proof.backup_stop_lsn, v_predecessor.valid_through_lsn;
        END IF;
        IF NULLIF(v_pending.details->>'predecessor_generation_id', '')::bigint
           IS DISTINCT FROM v_predecessor.generation_id
        THEN
            RAISE EXCEPTION 'flashback_consume_verified_backup_proof: building successor is not bound to the active predecessor';
        END IF;
        -- Inherit the already-proven WAL frontier so targets at/after the
        -- successor stop remain covered without a gap.
        v_valid_through := v_predecessor.valid_through_lsn;

        -- Inherit the proven frontier for the successor first, then seal the
        -- predecessor. Clamp the sealed watermark to superseded_before so the
        -- applicability CHECK remains true (half-open exclusive upper bound).
        -- Do not mutate immutable qualified details.
        UPDATE flashback.coverage_generations
           SET state = 'sealed',
               sealed_at = clock_timestamp(),
               superseded_before_lsn = v_proof.backup_stop_lsn,
               superseded_before_time = v_proof.verified_at,
               valid_through_lsn = LEAST(
                   v_predecessor.valid_through_lsn,
                   v_proof.backup_stop_lsn
               ),
               state_reason = 'superseded_by_full_reanchor'
         WHERE generation_id = v_predecessor.generation_id
           AND state = 'active';
    END IF;

    IF EXISTS (
        SELECT 1 FROM flashback.backup_anchors ba
        WHERE ba.tracking_id = v_tracked.tracking_id
          AND ba.backup_label = v_proof.backup_label
          AND ba.manifest_sha256 IS DISTINCT FROM v_proof.manifest_sha256
    ) THEN
        RAISE EXCEPTION 'flashback_consume_verified_backup_proof: backup label % already bound to a different manifest digest',
            v_proof.backup_label;
    END IF;

    -- Physical generation boundary is always the FULL stop LSN (FK to
    -- backup_anchors). Retained mode still requires stop <= marker and proves
    -- WAL through the marker; coverage_start on tracked_tables reflects the
    -- advertised tracking lower bound (marker).
    IF v_pending.boundary_lsn IS NOT NULL
       AND v_pending.boundary_lsn IS DISTINCT FROM v_proof.backup_stop_lsn
    THEN
        RAISE EXCEPTION 'flashback_consume_verified_backup_proof: building generation already has immutable boundary %',
            v_pending.boundary_lsn;
    END IF;

    v_coverage_lower := CASE
        WHEN v_activation_mode = 'retained_full_plus_wal' THEN v_marker_lsn
        ELSE v_proof.backup_stop_lsn
    END;

    INSERT INTO flashback.backup_anchors (
        tracking_id, helper_profile, repository_key, stanza, backup_label,
        backup_type, database_system_identifier, timeline_id,
        manifest_reference, manifest_sha256,
        tracking_marker_lsn, backup_start_lsn, backup_stop_lsn,
        verified_at, verified_by, details
    ) VALUES (
        v_tracked.tracking_id, v_proof.helper_profile,
        v_proof.repository_key, v_proof.stanza, v_proof.backup_label,
        'full', v_proof.database_system_identifier, v_proof.timeline_id,
        v_proof.manifest_reference, v_proof.manifest_sha256,
        v_marker_lsn, v_proof.backup_start_lsn, v_proof.backup_stop_lsn,
        v_proof.verified_at, session_user,
        jsonb_build_object(
            'activation_kind', COALESCE(v_pending.boundary_kind, 'initial_track'),
            'activation_mode', v_activation_mode,
            'source_generation_id', v_pending.generation_id,
            'verification_request_id', v_proof.verification_request_id,
            'proof_id', v_proof.proof_id,
            'wal_verified_through_lsn', v_valid_through,
            'required_dependencies', COALESCE(v_proof.details -> 'required_dependencies', '[]'::jsonb)
        )
    ) RETURNING backup_anchor_id INTO v_anchor_id;

    UPDATE flashback.coverage_generations
       SET backup_anchor_id = v_anchor_id,
           boundary_lsn = v_proof.backup_stop_lsn,
           boundary_time = v_proof.verified_at,
           valid_through_lsn = v_valid_through,
           valid_through_time = v_proof.verified_at,
           state = 'active',
           activated_at = clock_timestamp(),
           state_reason = CASE
               WHEN v_activation_mode = 'retained_full_plus_wal'
                   THEN 'verified_retained_full_plus_wal'
               ELSE 'verified_full_backup'
           END,
           details = COALESCE(details, '{}'::jsonb)
               || jsonb_build_object(
                   'tracking_marker_lsn', v_marker_lsn,
                   'coverage_lower_lsn', v_coverage_lower,
                   'verification_request_id', v_proof.verification_request_id,
                   'activation_mode', v_activation_mode,
                   'backup_stop_lsn', v_proof.backup_stop_lsn,
                   'wal_verified_through_lsn', v_valid_through,
                   'required_dependencies', COALESCE(
                       v_proof.details -> 'required_dependencies', '[]'::jsonb
                   ),
                   'dependency_pin_id', v_proof.details ->> 'dependency_pin_id',
                   'superseded_predecessor_generation_id',
                       CASE
                           WHEN v_predecessor.generation_id IS NOT NULL
                           THEN to_jsonb(v_predecessor.generation_id)
                           ELSE 'null'::jsonb
                       END
               )
     WHERE generation_id = v_pending.generation_id
       AND state = 'building'
    RETURNING generation_id INTO v_generation_id;

    IF v_generation_id IS NULL THEN
        RAISE EXCEPTION 'flashback_consume_verified_backup_proof: generation % could not be activated',
            v_pending.generation_id;
    END IF;

    UPDATE flashback.tracked_tables
       SET coverage_start_lsn = CASE
               WHEN v_pending.boundary_kind = 'full_reanchor'
                   THEN COALESCE(coverage_start_lsn, v_coverage_lower)
               ELSE v_coverage_lower
           END,
           coverage_end_lsn = GREATEST(
               COALESCE(coverage_end_lsn, v_valid_through),
               v_valid_through
           )
     WHERE tracking_id = v_tracked.tracking_id;

    UPDATE flashback.coverage_gaps
       SET gap_end_lsn = CASE
               WHEN v_activation_mode = 'retained_full_plus_wal' THEN v_marker_lsn
               ELSE v_proof.backup_stop_lsn
           END,
           gap_end_time = v_proof.verified_at,
           reanchored_by_generation_id = v_generation_id,
           reanchored_at = clock_timestamp()
     WHERE tracking_id = v_tracked.tracking_id
       AND reanchored_by_generation_id IS NULL
       AND gap_start_lsn < CASE
               WHEN v_activation_mode = 'retained_full_plus_wal' THEN v_marker_lsn
               ELSE v_proof.backup_stop_lsn
           END;

    UPDATE flashback.verified_backup_proofs
       SET consumed_at = clock_timestamp(),
           consumed_generation_id = v_generation_id
     WHERE proof_id = p_proof_id
       AND consumed_at IS NULL;

    RETURN v_generation_id;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_wal_frontier_attestation_payload(
    p_verification_request_id text,
    p_tracking_id bigint,
    p_generation_id bigint,
    p_helper_profile text,
    p_repository_key text,
    p_stanza text,
    p_timeline_id bigint,
    p_valid_through_lsn pg_lsn,
    p_archive_proof_sha256 text
)
RETURNS text
LANGUAGE sql
IMMUTABLE
STRICT
SET search_path = pg_catalog
AS $$
    SELECT jsonb_build_object(
        'kind', 'wal-frontier-v1',
        'verification_request_id', p_verification_request_id,
        'tracking_id', p_tracking_id,
        'generation_id', p_generation_id,
        'helper_profile', p_helper_profile,
        'repository_key', p_repository_key,
        'stanza', p_stanza,
        'timeline_id', p_timeline_id,
        'valid_through_lsn', p_valid_through_lsn::text,
        'archive_proof_sha256', p_archive_proof_sha256
    )::text;
$$;

CREATE OR REPLACE FUNCTION flashback_install_verified_wal_frontier_proof(
    p_verification_request_id text,
    p_tracking_id bigint,
    p_generation_id bigint,
    p_helper_profile text,
    p_repository_key text,
    p_stanza text,
    p_timeline_id bigint,
    p_valid_through_lsn pg_lsn,
    p_archive_proof_sha256 text,
    p_verified_at timestamptz DEFAULT clock_timestamp(),
    p_details jsonb DEFAULT '{}'::jsonb,
    p_attestation_hmac text DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_tracked record;
    v_gen record;
    v_proof_id bigint;
    v_is_superuser boolean;
    v_payload text;
BEGIN
    IF NOT flashback_caller_may_install_backup_proof() THEN
        RAISE EXCEPTION 'flashback_install_verified_wal_frontier_proof: only flashback_recovery_agent or a superuser may install proofs'
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    IF p_verification_request_id IS NULL OR btrim(p_verification_request_id) = ''
       OR p_tracking_id IS NULL OR p_generation_id IS NULL
       OR p_helper_profile IS NULL OR btrim(p_helper_profile) = ''
       OR p_repository_key IS NULL OR btrim(p_repository_key) = ''
       OR p_stanza IS NULL OR btrim(p_stanza) = ''
       OR p_timeline_id IS NULL OR p_timeline_id <= 0
       OR p_valid_through_lsn IS NULL
       OR p_archive_proof_sha256 IS NULL OR p_archive_proof_sha256 !~ '^[0-9a-f]{64}$'
    THEN
        RAISE EXCEPTION 'flashback_install_verified_wal_frontier_proof: incomplete or invalid proof identity';
    END IF;

    SELECT tt.* INTO v_tracked
    FROM flashback.tracked_tables tt
    WHERE tt.tracking_id = p_tracking_id
      AND tt.is_active
      AND tt.recovery_profile = 'backup';
    IF v_tracked.tracking_id IS NULL THEN
        RAISE EXCEPTION 'flashback_install_verified_wal_frontier_proof: no active backup lifecycle for tracking_id %',
            p_tracking_id;
    END IF;
    IF v_tracked.helper_profile IS DISTINCT FROM p_helper_profile THEN
        RAISE EXCEPTION 'flashback_install_verified_wal_frontier_proof: helper profile mismatch';
    END IF;

    SELECT cg.* INTO v_gen
    FROM flashback.coverage_generations cg
    WHERE cg.generation_id = p_generation_id
      AND cg.tracking_id = p_tracking_id
      AND cg.recovery_profile = 'backup';
    IF v_gen.generation_id IS NULL THEN
        RAISE EXCEPTION 'flashback_install_verified_wal_frontier_proof: generation % is not bound to tracking_id %',
            p_generation_id, p_tracking_id;
    END IF;

    SELECT COALESCE(rolsuper, false) INTO v_is_superuser
    FROM pg_roles WHERE rolname = session_user;
    IF NOT COALESCE(v_is_superuser, false) THEN
        v_payload := flashback_wal_frontier_attestation_payload(
            p_verification_request_id, p_tracking_id, p_generation_id,
            p_helper_profile, p_repository_key, p_stanza, p_timeline_id,
            p_valid_through_lsn, p_archive_proof_sha256
        );
        IF p_attestation_hmac IS NULL
           OR NOT flashback_verify_proof_hmac(v_payload, p_attestation_hmac)
        THEN
            RAISE EXCEPTION 'flashback_install_verified_wal_frontier_proof: valid helper HMAC attestation is required'
                USING ERRCODE = 'insufficient_privilege',
                      HINT = 'Run verify-frontier through the configured recovery helper; raw recovery-agent proof installation is forbidden.';
        END IF;
    END IF;

    INSERT INTO flashback.verified_wal_frontier_proofs (
        verification_request_id, tracking_id, generation_id,
        helper_profile, repository_key, stanza, timeline_id,
        valid_through_lsn, archive_proof_sha256,
        verified_at, installed_by, details
    ) VALUES (
        p_verification_request_id, p_tracking_id, p_generation_id,
        p_helper_profile, p_repository_key, p_stanza, p_timeline_id,
        p_valid_through_lsn, p_archive_proof_sha256,
        COALESCE(p_verified_at, clock_timestamp()), session_user,
        COALESCE(p_details, '{}'::jsonb)
    )
    RETURNING proof_id INTO v_proof_id;

    RETURN v_proof_id;
END;
$$;

-- Structured frontier consume: timeline mismatch freezes coverage and returns
-- a durable status without raising, so the controller can COMMIT the freeze.
CREATE OR REPLACE FUNCTION flashback_consume_verified_wal_frontier_proof(p_proof_id bigint)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_proof record;
    v_tracked record;
    v_active record;
    v_anchor record;
    v_timeline bigint;
    v_gap_id bigint;
BEGIN
    IF p_proof_id IS NULL THEN
        RAISE EXCEPTION 'flashback_consume_verified_wal_frontier_proof: proof_id is required';
    END IF;

    SELECT * INTO v_proof
    FROM flashback.verified_wal_frontier_proofs
    WHERE proof_id = p_proof_id
    FOR UPDATE;
    IF v_proof.proof_id IS NULL THEN
        RAISE EXCEPTION 'flashback_consume_verified_wal_frontier_proof: proof % does not exist', p_proof_id;
    END IF;
    IF v_proof.consumed_at IS NOT NULL THEN
        RAISE EXCEPTION 'flashback_consume_verified_wal_frontier_proof: proof % was already consumed',
            p_proof_id;
    END IF;

    SELECT tt.* INTO v_tracked
    FROM flashback.tracked_tables tt
    WHERE tt.tracking_id = v_proof.tracking_id
      AND tt.is_active
      AND tt.recovery_profile = 'backup';
    IF v_tracked.tracking_id IS NULL THEN
        RAISE EXCEPTION 'flashback_consume_verified_wal_frontier_proof: proof is not bound to an active backup lifecycle';
    END IF;
    IF v_tracked.helper_profile IS DISTINCT FROM v_proof.helper_profile THEN
        RAISE EXCEPTION 'flashback_consume_verified_wal_frontier_proof: helper profile mismatch';
    END IF;

    PERFORM pg_advisory_xact_lock(358944::integer, hashint8(v_tracked.tracking_id));

    SELECT cg.* INTO v_active
    FROM flashback.coverage_generations cg
    WHERE cg.generation_id = v_proof.generation_id
      AND cg.tracking_id = v_proof.tracking_id
      AND cg.recovery_profile = 'backup'
    FOR UPDATE;
    IF v_active.generation_id IS NULL OR v_active.state <> 'active' THEN
        RAISE EXCEPTION 'flashback_consume_verified_wal_frontier_proof: generation % is not an active backup generation',
            v_proof.generation_id;
    END IF;
    IF COALESCE(v_active.state_reason, '') = 'timeline_mismatch_frontier_frozen' THEN
        RAISE EXCEPTION 'flashback_consume_verified_wal_frontier_proof: frontier is frozen after timeline mismatch';
    END IF;

    SELECT ba.* INTO v_anchor
    FROM flashback.backup_anchors ba
    WHERE ba.backup_anchor_id = v_active.backup_anchor_id
      AND ba.tracking_id = v_active.tracking_id;
    IF v_anchor.backup_anchor_id IS NULL THEN
        RAISE EXCEPTION 'flashback_consume_verified_wal_frontier_proof: active generation has no backup anchor';
    END IF;
    IF v_anchor.helper_profile IS DISTINCT FROM v_proof.helper_profile
       OR v_anchor.repository_key IS DISTINCT FROM v_proof.repository_key
       OR v_anchor.stanza IS DISTINCT FROM v_proof.stanza
    THEN
        RAISE EXCEPTION 'flashback_consume_verified_wal_frontier_proof: repository/profile identity mismatch';
    END IF;

    SELECT timeline_id INTO v_timeline FROM pg_control_checkpoint();
    IF v_proof.timeline_id IS DISTINCT FROM v_timeline
       OR v_proof.timeline_id IS DISTINCT FROM v_anchor.timeline_id
    THEN
        UPDATE flashback.coverage_generations
           SET state_reason = 'timeline_mismatch_frontier_frozen'
         WHERE generation_id = v_active.generation_id
           AND state = 'active';

        INSERT INTO flashback.coverage_gaps (
            tracking_id, source_generation_id, reason,
            gap_start_lsn, gap_start_time, lower_bound_inclusive, details
        )
        SELECT
            v_tracked.tracking_id, v_active.generation_id, 'timeline_mismatch',
            v_active.valid_through_lsn, v_active.valid_through_time, false,
            jsonb_build_object(
                'expected_timeline', v_anchor.timeline_id,
                'observed_timeline', v_proof.timeline_id,
                'live_timeline', v_timeline,
                'verification_request_id', v_proof.verification_request_id,
                'proof_id', v_proof.proof_id,
                'frontier_at_freeze', v_active.valid_through_lsn
            )
        WHERE NOT EXISTS (
            SELECT 1 FROM flashback.coverage_gaps g
            WHERE g.tracking_id = v_tracked.tracking_id
              AND g.source_generation_id = v_active.generation_id
              AND g.reanchored_by_generation_id IS NULL
              AND g.reason = 'timeline_mismatch'
        )
        RETURNING gap_id INTO v_gap_id;

        UPDATE flashback.verified_wal_frontier_proofs
           SET consumed_at = clock_timestamp(),
               details = COALESCE(details, '{}'::jsonb) || jsonb_build_object(
                   'consume_status', 'timeline_mismatch',
                   'gap_id', v_gap_id
               )
         WHERE proof_id = p_proof_id
           AND consumed_at IS NULL;

        RETURN jsonb_build_object(
            'status', 'timeline_mismatch',
            'tracking_id', v_tracked.tracking_id,
            'generation_id', v_active.generation_id,
            'valid_through_lsn', v_active.valid_through_lsn,
            'gap_id', v_gap_id,
            'expected_timeline', v_anchor.timeline_id,
            'observed_timeline', v_proof.timeline_id,
            'live_timeline', v_timeline
        );
    END IF;

    IF v_proof.valid_through_lsn < v_active.valid_through_lsn THEN
        RAISE EXCEPTION 'flashback_consume_verified_wal_frontier_proof: frontier % is before current valid_through %',
            v_proof.valid_through_lsn, v_active.valid_through_lsn;
    END IF;

    IF v_proof.valid_through_lsn > v_active.valid_through_lsn THEN
        UPDATE flashback.coverage_generations
           SET valid_through_lsn = v_proof.valid_through_lsn,
               valid_through_time = clock_timestamp(),
               state_reason = 'physical_wal_frontier_advanced'
         WHERE generation_id = v_active.generation_id
           AND state = 'active';

        UPDATE flashback.tracked_tables
           SET coverage_end_lsn = v_proof.valid_through_lsn
         WHERE tracking_id = v_tracked.tracking_id;
    END IF;

    UPDATE flashback.verified_wal_frontier_proofs
       SET consumed_at = clock_timestamp(),
           details = COALESCE(details, '{}'::jsonb) || jsonb_build_object(
               'consume_status', 'ok'
           )
     WHERE proof_id = p_proof_id
       AND consumed_at IS NULL;

    RETURN jsonb_build_object(
        'status', 'ok',
        'tracking_id', v_tracked.tracking_id,
        'generation_id', v_active.generation_id,
        'valid_through_lsn', GREATEST(v_active.valid_through_lsn, v_proof.valid_through_lsn)
    );
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
    v_generation record;
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

    PERFORM pg_advisory_xact_lock(358944::integer, hashint8(v_tracked.tracking_id));

    SELECT cg.* INTO v_generation
    FROM flashback.coverage_generations cg
    WHERE cg.tracking_id = v_tracked.tracking_id
      AND cg.recovery_profile = 'backup'
      AND cg.state IN ('active', 'sealed')
      AND cg.backup_anchor_id IS NOT NULL
      AND cg.boundary_lsn IS NOT NULL
      AND cg.valid_through_lsn IS NOT NULL
      -- Advertised coverage lower bound: retained gens use the tracking marker
      -- (coverage_lower_lsn), not the earlier physical FULL stop.
      AND target_lsn >= COALESCE(
              NULLIF(cg.details->>'coverage_lower_lsn', '')::pg_lsn,
              CASE
                  WHEN COALESCE(cg.details->>'activation_mode', '') = 'retained_full_plus_wal'
                  THEN NULLIF(cg.details->>'tracking_marker_lsn', '')::pg_lsn
                  ELSE NULL
              END,
              cg.boundary_lsn
          )
      AND target_lsn <= cg.valid_through_lsn
      AND (cg.superseded_before_lsn IS NULL OR target_lsn < cg.superseded_before_lsn)
    ORDER BY cg.generation_no DESC
    LIMIT 1
    FOR UPDATE;

    IF v_generation.generation_id IS NULL THEN
        RAISE EXCEPTION 'flashback_prepare_backup_restore: no admissible backup generation covers target %',
            target_lsn
            USING HINT = 'Targets require an active/sealed generation with a verified FULL backup anchor; unanchored or gap intervals are rejected.';
    END IF;

    IF COALESCE(v_generation.state_reason, '') IN (
        'timeline_mismatch_frontier_frozen',
        'repository_verification_failed',
        'anchor_missing'
    ) THEN
        RAISE EXCEPTION 'flashback_prepare_backup_restore: backup generation is frozen (%)',
            v_generation.state_reason
            USING HINT = 'Consume a new verified FULL backup proof before preparing restore.';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM flashback.coverage_gaps g
        WHERE g.tracking_id = v_tracked.tracking_id
          AND target_lsn > g.gap_start_lsn
          AND (g.gap_end_lsn IS NULL OR target_lsn < g.gap_end_lsn)
    ) THEN
        RAISE EXCEPTION 'flashback_prepare_backup_restore: target % falls inside a coverage gap',
            target_lsn;
    END IF;

    SELECT sv.schema_version, sv.helper_schema_sha256
      INTO v_schema
    FROM flashback.schema_versions sv
    WHERE sv.rel_oid = v_tracked.rel_oid
      AND (sv.applied_lsn IS NULL OR sv.applied_lsn <= target_lsn)
    ORDER BY sv.applied_lsn DESC NULLS LAST, sv.schema_version DESC
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
        'tracking_id', v_tracked.tracking_id,
        'generation_id', v_generation.generation_id,
        'backup_anchor_id', v_generation.backup_anchor_id
    );

    INSERT INTO flashback.backup_restore_requests (
        request_id, rel_oid, tracking_id, generation_id,
        schema_name, table_name, target_lsn,
        expected_schema_version, expected_schema_sha256,
        helper_profile, request_json, requested_by
    ) VALUES (
        v_request_id, v_tracked.rel_oid, v_tracked.tracking_id, v_generation.generation_id,
        v_tracked.schema_name, v_tracked.table_name,
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
    v_tracking_id bigint;
    v_parent_generation_id bigint;
    v_generation_no bigint;
    v_boundary_xid bigint;
    v_provisional_lsn pg_lsn;
    v_successor_id bigint;
BEGIN
    SELECT * INTO v_request
    FROM flashback.backup_restore_requests
    WHERE request_id = p_request_id
    FOR UPDATE;

    IF v_request.request_id IS NULL OR v_request.status <> 'artifact_ready' THEN
        RAISE EXCEPTION 'flashback_finalize_backup_restore: request % is not artifact_ready', p_request_id;
    END IF;
    IF v_request.tracking_id IS NULL OR v_request.generation_id IS NULL THEN
        RAISE EXCEPTION 'flashback_finalize_backup_restore: request % is not pinned to an admitted generation',
            p_request_id;
    END IF;

    v_tracking_id := v_request.tracking_id;
    PERFORM pg_advisory_xact_lock(358944::integer, hashint8(v_tracking_id));

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
           table_name = v_request.table_name,
           coverage_start_lsn = NULL,
           coverage_end_lsn = NULL
     WHERE tracking_id = v_tracking_id;
    UPDATE flashback.schema_versions SET rel_oid = v_new_oid WHERE rel_oid = v_request.rel_oid;
    UPDATE flashback.delta_log SET rel_oid = v_new_oid WHERE rel_oid = v_request.rel_oid;
    UPDATE flashback.staging_events SET rel_oid = v_new_oid WHERE rel_oid = v_request.rel_oid;

    SELECT generation_id INTO v_parent_generation_id
    FROM flashback.coverage_generations
    WHERE tracking_id = v_tracking_id
      AND recovery_profile = 'backup'
      AND state = 'active'
    FOR UPDATE;
    IF v_parent_generation_id IS NULL THEN
        v_parent_generation_id := v_request.generation_id;
    END IF;

    SELECT COALESCE(MAX(generation_no), 0) + 1
      INTO v_generation_no
    FROM flashback.coverage_generations
    WHERE tracking_id = v_tracking_id;

    v_boundary_xid := (txid_current() % 4294967296)::bigint;
    v_provisional_lsn := pg_current_wal_insert_lsn();

    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, parent_generation_id, stream_id,
        recovery_profile, state, boundary_kind, rel_oid_at_boundary,
        boundary_snapshot_id, backup_anchor_id,
        boundary_xid, boundary_marker, restored_target_lsn, details
    ) VALUES (
        v_tracking_id, v_generation_no, v_parent_generation_id, NULL,
        'backup', 'building', 'post_restore', v_new_oid,
        NULL, NULL,
        v_boundary_xid,
        format('post-restore-backup:%s:%s:%s', v_tracking_id, v_boundary_xid, v_generation_no),
        v_request.target_lsn,
        jsonb_build_object(
            'source_generation_id', v_parent_generation_id,
            'request_id', p_request_id,
            'provisional_insert_lsn', v_provisional_lsn
        )
    ) RETURNING generation_id INTO v_successor_id;

    PERFORM pg_logical_emit_message(
        true,
        'pg_flashback',
        jsonb_build_object(
            'op', 'BOUNDARY',
            'kind', 'post_restore',
            'tracking_id', v_tracking_id,
            'generation_id', v_successor_id,
            'parent_generation_id', v_parent_generation_id
        )::text
    );

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
