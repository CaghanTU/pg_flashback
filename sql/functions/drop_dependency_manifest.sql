-- =================================================================
-- Pre-DROP dependency manifests (ProcessUtility, same DROP transaction)
-- Captured while target OIDs exist; rolls back with the DROP TX.
-- Restore preflight uses this LOGGED metadata — never post-DROP regclass.
-- =================================================================

DO $$
BEGIN
    IF to_regclass('flashback.drop_dependency_manifests') IS NULL THEN
        EXECUTE $ddl$
            CREATE TABLE flashback.drop_dependency_manifests (
                manifest_id      BIGSERIAL PRIMARY KEY,
                tracking_id      BIGINT NOT NULL,
                rel_oid          OID NOT NULL,
                schema_name      TEXT NOT NULL,
                table_name       TEXT NOT NULL,
                captured_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                cascade_requested BOOLEAN NOT NULL DEFAULT false,
                manifest         JSONB NOT NULL,
                has_unsupported  BOOLEAN NOT NULL DEFAULT false,
                source_xid       BIGINT,
                disaster_commit_lsn PG_LSN,
                disaster_event_id BIGINT
            )
        $ddl$;
        EXECUTE 'CREATE INDEX drop_dependency_manifests_tracking_idx
                 ON flashback.drop_dependency_manifests (tracking_id, captured_at DESC)';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flashback' AND table_name = 'drop_dependency_manifests'
          AND column_name = 'source_xid'
    ) THEN
        ALTER TABLE flashback.drop_dependency_manifests ADD COLUMN source_xid BIGINT;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flashback' AND table_name = 'drop_dependency_manifests'
          AND column_name = 'disaster_commit_lsn'
    ) THEN
        ALTER TABLE flashback.drop_dependency_manifests ADD COLUMN disaster_commit_lsn PG_LSN;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flashback' AND table_name = 'drop_dependency_manifests'
          AND column_name = 'disaster_event_id'
    ) THEN
        ALTER TABLE flashback.drop_dependency_manifests ADD COLUMN disaster_event_id BIGINT;
    END IF;
END
$$;

-- Build a conservative dependency report for a live relation.
CREATE OR REPLACE FUNCTION flashback_build_dependency_manifest(p_rel regclass)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_oid oid := p_rel;
    v_out jsonb;
    v_unsupported boolean := false;
    v_incoming_fk jsonb;
    v_outgoing_fk jsonb;
    v_views jsonb;
    v_matviews jsonb;
    v_triggers jsonb;
    v_sequences jsonb;
    v_inherits_children jsonb;
    v_inherits_parents jsonb;
BEGIN
    IF v_oid IS NULL THEN
        RAISE EXCEPTION 'flashback_build_dependency_manifest: relation does not exist';
    END IF;

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'conname', c.conname,
               'contype', c.contype,
               'referencing', format('%I.%I', n.nspname, rel.relname),
               'class', 'incoming_fk'
           ) ORDER BY c.conname), '[]'::jsonb)
      INTO v_incoming_fk
    FROM pg_constraint c
    JOIN pg_class rel ON rel.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = rel.relnamespace
    WHERE c.confrelid = v_oid AND c.contype = 'f';

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'conname', c.conname,
               'contype', c.contype,
               'referenced', format('%I.%I', n.nspname, rel.relname),
               'class', 'outgoing_fk'
           ) ORDER BY c.conname), '[]'::jsonb)
      INTO v_outgoing_fk
    FROM pg_constraint c
    JOIN pg_class rel ON rel.oid = c.confrelid
    JOIN pg_namespace n ON n.oid = rel.relnamespace
    WHERE c.conrelid = v_oid AND c.contype = 'f';

    SELECT COALESCE(jsonb_agg(obj ORDER BY obj->>'name'), '[]'::jsonb)
      INTO v_views
    FROM (
        SELECT DISTINCT ON (c.oid) jsonb_build_object(
               'name', format('%I.%I', n.nspname, c.relname),
               'class', 'view',
               'reconstruct', false,
               'reason', 'view_reconstruction_not_qualified'
           ) AS obj
        FROM pg_depend d
        JOIN pg_rewrite r ON r.oid = d.objid AND d.classid = 'pg_rewrite'::regclass
        JOIN pg_class c ON c.oid = r.ev_class AND c.relkind = 'v'
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE d.refclassid = 'pg_class'::regclass
          AND d.refobjid = v_oid
          AND d.deptype = 'n'
        ORDER BY c.oid, n.nspname, c.relname
    ) uniq_views;

    SELECT COALESCE(jsonb_agg(obj ORDER BY obj->>'name'), '[]'::jsonb)
      INTO v_matviews
    FROM (
        SELECT DISTINCT ON (c.oid) jsonb_build_object(
               'name', format('%I.%I', n.nspname, c.relname),
               'class', 'matview',
               'reconstruct', false,
               'reason', 'matview_reconstruction_not_qualified'
           ) AS obj
        FROM pg_depend d
        JOIN pg_rewrite r ON r.oid = d.objid AND d.classid = 'pg_rewrite'::regclass
        JOIN pg_class c ON c.oid = r.ev_class AND c.relkind = 'm'
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE d.refclassid = 'pg_class'::regclass
          AND d.refobjid = v_oid
          AND d.deptype = 'n'
        ORDER BY c.oid, n.nspname, c.relname
    ) uniq_matviews;

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'name', t.tgname,
               'class', 'trigger',
               'reconstruct', true
           ) ORDER BY t.tgname), '[]'::jsonb)
      INTO v_triggers
    FROM pg_trigger t
    WHERE t.tgrelid = v_oid AND NOT t.tgisinternal;

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'name', format('%I.%I', n.nspname, c.relname),
               'class', 'sequence',
               'reconstruct', true
           ) ORDER BY n.nspname, c.relname), '[]'::jsonb)
      INTO v_sequences
    FROM pg_depend d
    JOIN pg_class c ON c.oid = d.objid AND c.relkind = 'S'
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE d.refclassid = 'pg_class'::regclass
      AND d.refobjid = v_oid
      AND d.deptype IN ('a', 'i');

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'name', format('%I.%I', n.nspname, c.relname),
               'class', 'inheritance_child',
               'reconstruct', false,
               'reason', 'inheritance_not_supported'
           ) ORDER BY n.nspname, c.relname), '[]'::jsonb)
      INTO v_inherits_children
    FROM pg_inherits i
    JOIN pg_class c ON c.oid = i.inhrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE i.inhparent = v_oid;

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'name', format('%I.%I', n.nspname, c.relname),
               'class', 'inheritance_parent',
               'reconstruct', false,
               'reason', 'inheritance_not_supported'
           ) ORDER BY n.nspname, c.relname), '[]'::jsonb)
      INTO v_inherits_parents
    FROM pg_inherits i
    JOIN pg_class c ON c.oid = i.inhparent
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE i.inhrelid = v_oid;

    IF jsonb_array_length(v_views) > 0
       OR jsonb_array_length(v_matviews) > 0
       OR jsonb_array_length(v_inherits_children) > 0
       OR jsonb_array_length(v_inherits_parents) > 0 THEN
        v_unsupported := true;
    END IF;

    -- Incoming FKs are reported; reconstruction is not yet qualification-proven.
    IF jsonb_array_length(v_incoming_fk) > 0 THEN
        v_unsupported := true;
        v_incoming_fk := (
            SELECT COALESCE(jsonb_agg(elem || jsonb_build_object(
                'reconstruct', false,
                'reason', 'incoming_fk_reconstruction_not_qualified'
            )), '[]'::jsonb)
            FROM jsonb_array_elements(v_incoming_fk) elem
        );
    END IF;

    v_out := jsonb_build_object(
        'schema_version', 1,
        'rel', v_oid::text,
        'incoming_fk', v_incoming_fk,
        'outgoing_fk', v_outgoing_fk,
        'views', v_views,
        'matviews', v_matviews,
        'triggers', v_triggers,
        'sequences', v_sequences,
        'inheritance_children', v_inherits_children,
        'inheritance_parents', v_inherits_parents,
        'has_unsupported', v_unsupported,
        'policy', 'report_all_reconstruct_only_qualified'
    );
    RETURN v_out;
END;
$$;

-- Exact-OID production entry point used by the ProcessUtility hook. The OID
-- is resolved under the caller's search_path before switching to the
-- extension owner, so a SECURITY DEFINER lookup can never drift to a
-- same-named relation in another schema.
CREATE OR REPLACE FUNCTION flashback_capture_drop_dependency_manifest(
    input_rel_oid oid,
    cascade_requested boolean DEFAULT false
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    tracked record;
    v_manifest jsonb;
BEGIN
    IF input_rel_oid IS NULL OR input_rel_oid = 0 THEN
        RETURN;
    END IF;

    SELECT tt.tracking_id, tt.rel_oid, tt.schema_name, tt.table_name, tt.recovery_profile
      INTO tracked
    FROM flashback.tracked_tables tt
    WHERE tt.rel_oid = input_rel_oid
      AND tt.recovery_profile = 'local_delta'
      AND tt.is_active
    ORDER BY tt.tracked_since DESC
    LIMIT 1;

    IF tracked.tracking_id IS NULL THEN
        RETURN;
    END IF;

    -- The caller holds ACCESS EXCLUSIVE for destructive DDL. Recheck that the
    -- immutable OID still names the tracked relation before reading its
    -- dependency graph.
    IF NOT EXISTS (
        SELECT 1
        FROM pg_class c
        WHERE c.oid = input_rel_oid
    ) THEN
        RAISE EXCEPTION
            'pg_flashback: DROP dependency manifest target OID % disappeared',
            input_rel_oid
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    v_manifest := flashback_build_dependency_manifest(input_rel_oid::regclass);

    INSERT INTO flashback.drop_dependency_manifests (
        tracking_id, rel_oid, schema_name, table_name,
        cascade_requested, manifest, has_unsupported, source_xid
    ) VALUES (
        tracked.tracking_id, tracked.rel_oid, tracked.schema_name, tracked.table_name,
        COALESCE(cascade_requested, false), v_manifest,
        COALESCE((v_manifest->>'has_unsupported')::boolean, false),
        (txid_current() % 4294967296)::bigint
    );
END;
$$;

-- Compatibility/test wrapper for explicit names. Production hooks never use
-- this overload. Unqualified input is rejected because SECURITY DEFINER must
-- never reinterpret a caller-relative name under its fixed search_path.
CREATE OR REPLACE FUNCTION flashback_capture_drop_dependency_manifest(
    input_schema text,
    input_table text,
    cascade_requested boolean DEFAULT false
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_rel oid;
BEGIN
    IF input_table IS NULL OR input_table = '' THEN
        RETURN;
    END IF;
    IF input_schema IS NULL OR input_schema = '' THEN
        RAISE EXCEPTION
            'pg_flashback: dependency manifest capture requires an exact schema or OID'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    v_rel := to_regclass(format('%I.%I', input_schema, input_table));
    IF v_rel IS NULL THEN
        RETURN;
    END IF;
    PERFORM public.flashback_capture_drop_dependency_manifest(
        v_rel, cascade_requested
    );
END;
$$;

-- Bind committed DROP events to pre-DROP manifests captured in the same TX.
-- Called after delta_log insert so plan/execute can select by event identity.
CREATE OR REPLACE FUNCTION flashback_bind_drop_dependency_manifests()
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_n integer := 0;
BEGIN
    UPDATE flashback.drop_dependency_manifests m
       SET disaster_event_id = dl.event_id,
           disaster_commit_lsn = dl.commit_lsn
      FROM flashback.delta_log dl
     WHERE dl.event_type = 'DROP'
       AND dl.tracking_id = m.tracking_id
       AND dl.source_xid IS NOT DISTINCT FROM m.source_xid
       AND m.disaster_event_id IS NULL
       AND dl.commit_lsn IS NOT NULL;

    GET DIAGNOSTICS v_n = ROW_COUNT;
    RETURN v_n;
END;
$$;

-- Restore preflight: refuse swap unless an exact event-bound dependency
-- manifest is present and reports only supported dependencies.
-- Matching is by disaster_event_id ONLY — never "latest for tracking_id",
-- never a commit_lsn/xid transitional admit arm, and never a missing
-- binding treated as "no dependents". flashback_bind_drop_dependency_manifests()
-- remains the sole path that resolves disaster_event_id (via xid/lsn join
-- against delta_log) onto a captured manifest row.
-- Drop the retired 4-arg (tracking_id, event_id, commit_lsn, xid) overload
-- from earlier installs; CREATE OR REPLACE cannot narrow a signature.
DROP FUNCTION IF EXISTS flashback_require_supported_drop_manifest(bigint, bigint, pg_lsn, bigint);

CREATE OR REPLACE FUNCTION flashback_require_supported_drop_manifest(
    p_tracking_id bigint,
    p_disaster_event_id bigint DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_row record;
    v_any_manifest boolean;
BEGIN
    IF p_disaster_event_id IS NULL THEN
        RAISE EXCEPTION
            'pg_flashback: restore refused: exact DROP identity required for dependency manifest check (tracking_id %)',
            p_tracking_id
            USING ERRCODE = 'invalid_parameter_value',
                  HINT = 'Pass the exact disaster_event_id selected by flashback_recover_plan/flashback_disaster_points.';
    END IF;

    SELECT *
      INTO v_row
    FROM flashback.drop_dependency_manifests m
    WHERE m.tracking_id = p_tracking_id
      AND m.disaster_event_id = p_disaster_event_id
    ORDER BY m.captured_at DESC, m.manifest_id DESC
    LIMIT 1;

    IF v_row.manifest_id IS NULL THEN
        SELECT EXISTS (
            SELECT 1
            FROM flashback.drop_dependency_manifests m
            WHERE m.tracking_id = p_tracking_id
        ) INTO v_any_manifest;

        IF v_any_manifest THEN
            RAISE EXCEPTION
                'pg_flashback: restore refused: no exact pre-DROP dependency manifest bound to the selected DROP (tracking_id %, disaster_event_id %)',
                p_tracking_id, p_disaster_event_id
                USING ERRCODE = 'feature_not_supported',
                      HINT = 'Wait for worker bind (flashback_bind_drop_dependency_manifests) or replan; never recovers from another DROP generation''s manifest.';
        END IF;

        RAISE EXCEPTION
            'pg_flashback: restore refused: pre-DROP dependency manifest is missing for tracking_id %',
            p_tracking_id
            USING ERRCODE = 'feature_not_supported',
                  HINT = 'ProcessUtility must capture a dependency manifest in the DROP transaction; recovery refuses without it.';
    END IF;

    IF v_row.has_unsupported OR COALESCE((v_row.manifest->>'has_unsupported')::boolean, false) THEN
        RAISE EXCEPTION
            'pg_flashback: restore refused: pre-DROP dependency manifest for tracking_id % contains unsupported or unqualified dependencies',
            p_tracking_id
            USING ERRCODE = 'feature_not_supported',
                  HINT = 'Inspect flashback.drop_dependency_manifests.manifest. Only qualification-proven dependency classes are reconstructed; unknown kinds fail closed before swap.',
                  DETAIL = v_row.manifest::text;
    END IF;
END;
$$;
