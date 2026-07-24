-- =================================================================
-- WAL-only upgrade finalize: ownership-proven cleanup of legacy
-- trigger-capture objects. Idempotent; never CASCADE; never wildcard
-- trigger names; never touch non-extension-owned collisions.
-- =================================================================

CREATE OR REPLACE FUNCTION flashback_internal_is_pg_flashback_member(
    p_classid oid,
    p_objid oid
)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM pg_depend d
        JOIN pg_extension e
          ON e.oid = d.refobjid
         AND d.refclassid = 'pg_extension'::regclass
        WHERE d.classid = p_classid
          AND d.objid = p_objid
          AND d.deptype = 'e'
          AND e.extname = 'pg_flashback'
    );
$$;

CREATE OR REPLACE FUNCTION flashback_internal_finalize_wal_only_upgrade()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_ext_oid oid;
    v_staging oid;
    v_staging_kind "char";
    v_count bigint;
    v_tg record;
    v_fn_sig text;
    v_fn_name text;
    v_fn_args text;
    v_fn_oid oid;
    v_fn_nsp text;
    v_dep_sch text;
    v_dep_tbl text;
    v_dep_tg text;
    -- Exact historical trigger names from flashback_attach_capture_trigger
    -- plus flashback_capture_row (detach always attempted to remove it).
    v_trigger_names text[] := ARRAY[
        'flashback_capture_row',
        'flashback_capture_ins',
        'flashback_capture_upd',
        'flashback_capture_del'
    ];
    -- Exact historical capture-function signatures (identity args).
    v_fn_sigs text[] := ARRAY[
        'flashback_flush_staging(integer)',
        'flashback_attach_capture_trigger(text,text)',
        'flashback_detach_capture_trigger(text,text)',
        'flashback_capture_insert_trigger()',
        'flashback_capture_insert_row_trigger()',
        'flashback_capture_update_trigger()',
        'flashback_capture_delete_trigger()',
        'flashback_capture_delete_row_trigger()'
    ];
    v_capture_fn_oids oid[] := ARRAY[]::oid[];
BEGIN
    SELECT e.oid INTO v_ext_oid
    FROM pg_extension e
    WHERE e.extname = 'pg_flashback';
    IF v_ext_oid IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: internal WAL-only finalize requires extension pg_flashback'
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    -- ------------------------------------------------------------------
    -- staging_events: probe only (drop later). Name alone is not ownership.
    -- ------------------------------------------------------------------
    v_staging := to_regclass('flashback.staging_events');
    IF v_staging IS NOT NULL THEN
        SELECT c.relkind INTO v_staging_kind
        FROM pg_class c
        WHERE c.oid = v_staging;

        IF NOT public.flashback_internal_is_pg_flashback_member(
            'pg_class'::regclass::oid, v_staging
        ) THEN
            RAISE EXCEPTION
                'pg_flashback: flashback.staging_events exists but is not a pg_flashback extension member; refusing WAL-only cleanup'
                USING ERRCODE = 'duplicate_table',
                      HINT =
                          'Rename or move the colliding relation out of schema flashback, then reload/upgrade. pg_flashback will not drop a non-extension table.';
        END IF;

        IF v_staging_kind IS DISTINCT FROM 'r' THEN
            RAISE EXCEPTION
                'pg_flashback: extension-owned flashback.staging_events has unexpected relkind %; refusing cleanup',
                v_staging_kind
                USING ERRCODE = 'wrong_object_type',
                      HINT = 'Inspect flashback.staging_events; expected an ordinary table leftover from trigger capture.';
        END IF;

        EXECUTE 'SELECT count(*) FROM flashback.staging_events' INTO v_count;
        IF v_count > 0 THEN
            RAISE EXCEPTION
                'pg_flashback: flashback.staging_events still contains % row(s); WAL-only upgrade cannot discard them',
                v_count
                USING ERRCODE = 'feature_not_supported',
                      HINT =
                          'Flush with the previous pg_flashback binary (flashback_flush_staging), or unprotect/re-anchor after draining capture, then reload/upgrade. Do not DELETE staging rows manually.';
        END IF;
    END IF;

    -- Resolve allowlisted functions in ANY schema: extension members are
    -- queued for drop; non-members with the same identity are collisions.
    FOREACH v_fn_sig IN ARRAY v_fn_sigs LOOP
        v_fn_name := split_part(v_fn_sig, '(', 1);
        v_fn_args := substring(v_fn_sig FROM '\((.*)\)\s*$');

        FOR v_fn_oid, v_fn_nsp IN
            SELECT p.oid, n.nspname
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE p.proname = v_fn_name
              AND pg_get_function_identity_arguments(p.oid) = v_fn_args
            ORDER BY n.nspname, p.oid
        LOOP
            IF public.flashback_internal_is_pg_flashback_member(
                'pg_proc'::regclass::oid, v_fn_oid
            ) THEN
                IF NOT (v_fn_oid = ANY (v_capture_fn_oids)) THEN
                    v_capture_fn_oids := array_append(v_capture_fn_oids, v_fn_oid);
                END IF;
            ELSE
                RAISE EXCEPTION
                    'pg_flashback: % exists as %.% but is not a pg_flashback extension member; refusing WAL-only cleanup',
                    v_fn_sig, v_fn_nsp, v_fn_name
                    USING ERRCODE = 'duplicate_function',
                          HINT =
                              'Rename or drop the colliding user function, then reload/upgrade. pg_flashback will not DROP a non-extension function.';
            END IF;
        END LOOP;
    END LOOP;

    -- ------------------------------------------------------------------
    -- Exact-name trigger cleanup (never LIKE / wildcard).
    -- ------------------------------------------------------------------
    FOR v_tg IN
        SELECT t.oid AS tg_oid,
               t.tgname,
               t.tgfoid,
               n.nspname AS sch,
               c.relname AS tbl
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE NOT t.tgisinternal
          AND t.tgname = ANY (v_trigger_names)
        ORDER BY n.nspname, c.relname, t.tgname
    LOOP
        IF v_tg.tgfoid = ANY (v_capture_fn_oids)
           AND public.flashback_internal_is_pg_flashback_member(
               'pg_proc'::regclass::oid, v_tg.tgfoid
           )
        THEN
            EXECUTE format(
                'DROP TRIGGER %I ON %I.%I',
                v_tg.tgname, v_tg.sch, v_tg.tbl
            );
        ELSE
            RAISE EXCEPTION
                'pg_flashback: trigger %.%.% matches a legacy capture name but calls non-extension function oid %; refusing cleanup',
                v_tg.sch, v_tg.tbl, v_tg.tgname, v_tg.tgfoid
                USING ERRCODE = 'dependent_objects_still_exist',
                      HINT =
                          'Rename the colliding user trigger (or rebind it), then reload/upgrade. Ordinary user triggers are never removed by WAL-only finalize.';
        END IF;
    END LOOP;

    -- ------------------------------------------------------------------
    -- Extension-owned legacy function DROP (RESTRICT / no CASCADE).
    -- ------------------------------------------------------------------
    FOREACH v_fn_oid IN ARRAY v_capture_fn_oids LOOP
        IF EXISTS (
            SELECT 1
            FROM pg_depend d
            WHERE d.refclassid = 'pg_proc'::regclass::oid
              AND d.refobjid = v_fn_oid
              AND d.deptype = 'n'
        ) THEN
            SELECT n.nspname, c.relname, t.tgname
              INTO v_dep_sch, v_dep_tbl, v_dep_tg
            FROM pg_depend d
            JOIN pg_trigger t ON t.oid = d.objid AND d.classid = 'pg_trigger'::regclass
            JOIN pg_class c ON c.oid = t.tgrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE d.refclassid = 'pg_proc'::regclass::oid
              AND d.refobjid = v_fn_oid
              AND d.deptype = 'n'
            LIMIT 1;

            RAISE EXCEPTION
                'pg_flashback: cannot drop legacy capture function % because dependent object still exists (e.g. trigger %.%.%)',
                v_fn_oid::regprocedure,
                COALESCE(v_dep_sch, '?'),
                COALESCE(v_dep_tbl, '?'),
                COALESCE(v_dep_tg, '?')
                USING ERRCODE = 'dependent_objects_still_exist',
                      HINT =
                          'Remove or rename the unexpected dependency, then reload/upgrade. WAL-only finalize never uses CASCADE.';
        END IF;

        BEGIN
            EXECUTE format(
                'ALTER EXTENSION pg_flashback DROP FUNCTION %s',
                v_fn_oid::regprocedure
            );
        EXCEPTION WHEN OTHERS THEN
            NULL;
        END;

        BEGIN
            EXECUTE format('DROP FUNCTION %s', v_fn_oid::regprocedure);
        EXCEPTION WHEN dependent_objects_still_exist THEN
            RAISE EXCEPTION
                'pg_flashback: cannot DROP legacy capture function % due to unexpected dependencies',
                v_fn_oid::regprocedure
                USING ERRCODE = 'dependent_objects_still_exist',
                      HINT =
                          'Inspect pg_depend for this function, remove unexpected dependents, then reload/upgrade. CASCADE is forbidden.';
        END;
    END LOOP;

    -- ------------------------------------------------------------------
    -- Empty extension-owned staging_events
    -- ------------------------------------------------------------------
    v_staging := to_regclass('flashback.staging_events');
    IF v_staging IS NOT NULL THEN
        IF NOT public.flashback_internal_is_pg_flashback_member(
            'pg_class'::regclass::oid, v_staging
        ) THEN
            RAISE EXCEPTION
                'pg_flashback: flashback.staging_events lost extension membership during finalize; refusing DROP'
                USING ERRCODE = 'object_not_in_prerequisite_state';
        END IF;

        BEGIN
            EXECUTE 'ALTER EXTENSION pg_flashback DROP TABLE flashback.staging_events';
        EXCEPTION WHEN OTHERS THEN
            NULL;
        END;

        BEGIN
            EXECUTE 'DROP TABLE flashback.staging_events';
        EXCEPTION WHEN dependent_objects_still_exist THEN
            RAISE EXCEPTION
                'pg_flashback: cannot DROP flashback.staging_events due to unexpected dependencies'
                USING ERRCODE = 'dependent_objects_still_exist',
                      HINT =
                          'Inspect dependents of flashback.staging_events, then reload/upgrade. CASCADE is forbidden.';
        END;

        RAISE NOTICE 'pg_flashback: dropped empty legacy staging_events (WAL-only capture)';
    END IF;
END;
$$;

REVOKE ALL ON FUNCTION flashback_internal_is_pg_flashback_member(oid, oid) FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_internal_finalize_wal_only_upgrade() FROM PUBLIC;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flashback_admin') THEN
        EXECUTE 'REVOKE ALL ON FUNCTION flashback_internal_is_pg_flashback_member(oid, oid) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION flashback_internal_finalize_wal_only_upgrade() FROM flashback_admin';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pg_monitor') THEN
        EXECUTE 'REVOKE ALL ON FUNCTION flashback_internal_is_pg_flashback_member(oid, oid) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION flashback_internal_finalize_wal_only_upgrade() FROM pg_monitor';
    END IF;
END
$$;

-- Run once at extension install/upgrade load. Idempotent no-op when clean.
SELECT flashback_internal_finalize_wal_only_upgrade();
