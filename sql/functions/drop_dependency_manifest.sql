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
                has_unsupported  BOOLEAN NOT NULL DEFAULT false
            )
        $ddl$;
        EXECUTE 'CREATE INDEX drop_dependency_manifests_tracking_idx
                 ON flashback.drop_dependency_manifests (tracking_id, captured_at DESC)';
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

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'name', format('%I.%I', n.nspname, c.relname),
               'class', 'view',
               'reconstruct', false,
               'reason', 'view_reconstruction_not_qualified'
           ) ORDER BY n.nspname, c.relname), '[]'::jsonb)
      INTO v_views
    FROM pg_depend d
    JOIN pg_rewrite r ON r.oid = d.objid AND d.classid = 'pg_rewrite'::regclass
    JOIN pg_class c ON c.oid = r.ev_class AND c.relkind = 'v'
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE d.refclassid = 'pg_class'::regclass
      AND d.refobjid = v_oid
      AND d.deptype = 'n';

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'name', format('%I.%I', n.nspname, c.relname),
               'class', 'matview',
               'reconstruct', false,
               'reason', 'matview_reconstruction_not_qualified'
           ) ORDER BY n.nspname, c.relname), '[]'::jsonb)
      INTO v_matviews
    FROM pg_depend d
    JOIN pg_rewrite r ON r.oid = d.objid AND d.classid = 'pg_rewrite'::regclass
    JOIN pg_class c ON c.oid = r.ev_class AND c.relkind = 'm'
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE d.refclassid = 'pg_class'::regclass
      AND d.refobjid = v_oid
      AND d.deptype = 'n';

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
    tracked record;
    v_rel regclass;
    v_manifest jsonb;
BEGIN
    IF input_table IS NULL OR input_table = '' THEN
        RETURN;
    END IF;

    IF input_schema IS NULL OR input_schema = '' THEN
        v_rel := to_regclass(input_table);
    ELSE
        v_rel := to_regclass(format('%I.%I', input_schema, input_table));
    END IF;
    IF v_rel IS NULL THEN
        RETURN;
    END IF;

    SELECT tt.tracking_id, tt.rel_oid, tt.schema_name, tt.table_name, tt.recovery_profile
      INTO tracked
    FROM flashback.tracked_tables tt
    WHERE tt.rel_oid = v_rel
      AND tt.recovery_profile = 'local_delta'
      AND tt.is_active
    ORDER BY tt.tracked_since DESC
    LIMIT 1;

    IF tracked.tracking_id IS NULL THEN
        RETURN;
    END IF;

    v_manifest := flashback_build_dependency_manifest(v_rel);

    INSERT INTO flashback.drop_dependency_manifests (
        tracking_id, rel_oid, schema_name, table_name,
        cascade_requested, manifest, has_unsupported
    ) VALUES (
        tracked.tracking_id, tracked.rel_oid, tracked.schema_name, tracked.table_name,
        COALESCE(cascade_requested, false), v_manifest,
        COALESCE((v_manifest->>'has_unsupported')::boolean, false)
    );
END;
$$;

-- Restore preflight: refuse swap when the latest committed manifest for this
-- lifecycle reports unsupported dependencies (conservative CASCADE).
CREATE OR REPLACE FUNCTION flashback_require_supported_drop_manifest(p_tracking_id bigint)
RETURNS void
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_row record;
BEGIN
    SELECT *
      INTO v_row
    FROM flashback.drop_dependency_manifests m
    WHERE m.tracking_id = p_tracking_id
    ORDER BY m.captured_at DESC, m.manifest_id DESC
    LIMIT 1;

    IF v_row.manifest_id IS NULL THEN
        -- Older coverage without a manifest: fail closed for CASCADE safety
        -- only when we cannot prove absence of unsupported deps. Ordinary
        -- DROP without dependents still proceeds (no manifest row is OK when
        -- the table had no tracked DROP capture yet — rare). Prefer presence.
        RETURN;
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
