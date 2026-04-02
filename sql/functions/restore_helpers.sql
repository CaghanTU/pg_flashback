-- =================================================================
-- Restore helper functions: predicate builder, insert builder,
-- table recreation (with shadow-table support), and atomic swap.
-- =================================================================

CREATE OR REPLACE FUNCTION flashback_build_predicate(target_rel oid, payload jsonb)
RETURNS text
LANGUAGE sql
AS $$
    SELECT string_agg(
        CASE
            WHEN kv.value = 'null'::jsonb THEN format('%I IS NULL', a.attname)
            -- For array columns: JSON [1,2,3] → PG '{1,2,3}'
            WHEN a.attndims > 0 OR t.typlen = -1 AND t.typelem <> 0 THEN
                format('%I IS NOT DISTINCT FROM %L::%s', a.attname,
                    translate(kv.value::text, '[]', '{}'),
                    pg_catalog.format_type(a.atttypid, a.atttypmod))
            -- For jsonb/json columns: preserve JSON representation
            WHEN t.typname IN ('jsonb', 'json') THEN
                format('%I IS NOT DISTINCT FROM %L::%s', a.attname,
                    kv.value::text,
                    pg_catalog.format_type(a.atttypid, a.atttypmod))
            ELSE format(
                '%I IS NOT DISTINCT FROM %L::%s',
                a.attname,
                kv.value #>> '{}',
                pg_catalog.format_type(a.atttypid, a.atttypmod)
            )
        END,
        ' AND '
        ORDER BY a.attnum
    )
    FROM pg_attribute a
    JOIN pg_type t ON t.oid = a.atttypid
    JOIN LATERAL jsonb_each(payload) kv(key, value)
      ON kv.key = a.attname
    WHERE a.attrelid = target_rel
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND a.attgenerated = '';
$$;

CREATE OR REPLACE FUNCTION flashback_build_insert_parts(
    target_rel oid,
    payload jsonb,
    OUT col_list text,
    OUT val_list text
)
LANGUAGE sql
AS $$
    SELECT
        string_agg(format('%I', a.attname), ', ' ORDER BY a.attnum),
        string_agg(
            CASE
                WHEN kv.value = 'null'::jsonb THEN 'NULL'
                -- For array columns: JSON [1,2,3] → PG '{1,2,3}'
                WHEN a.attndims > 0 OR t.typlen = -1 AND t.typelem <> 0 THEN
                    format('%L::%s',
                        translate(kv.value::text, '[]', '{}'),
                        pg_catalog.format_type(a.atttypid, a.atttypmod))
                -- For jsonb/json columns: preserve JSON representation
                WHEN t.typname IN ('jsonb', 'json') THEN
                    format('%L::%s',
                        kv.value::text,
                        pg_catalog.format_type(a.atttypid, a.atttypmod))
                ELSE format(
                    '%L::%s',
                    kv.value #>> '{}',
                    pg_catalog.format_type(a.atttypid, a.atttypmod)
                )
            END,
            ', '
            ORDER BY a.attnum
        )
    FROM pg_attribute a
    JOIN pg_type t ON t.oid = a.atttypid
    JOIN LATERAL jsonb_each(payload) kv(key, value)
      ON kv.key = a.attname
    WHERE a.attrelid = target_rel
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND a.attgenerated = '';
$$;

-- ----------------------------------------------------------------
-- flashback_recreate_table_from_ddl
-- ----------------------------------------------------------------
-- When p_shadow_schema IS NULL  → legacy in-place: DROP + CREATE original
-- When p_shadow_schema IS NOT NULL → create shadow table in that schema,
--   skip FK / triggers / RLS / ACL (deferred to swap phase).
-- ----------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_recreate_table_from_ddl(
    ddl_info        jsonb,
    p_shadow_schema text DEFAULT NULL,
    p_shadow_table  text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_schema text;
    v_table  text;
    v_tgt_schema text;
    v_tgt_table  text;
    v_is_shadow  boolean;
    col_defs text;
    pk_cols  text;
    v_skipped_defaults jsonb;
    v_con  record;
    v_idx  record;
    v_part record;
    v_trig record;
    v_pol  record;
    v_saved_acl aclitem[];
    v_owner     regrole;
BEGIN
    IF ddl_info IS NULL THEN
        RAISE EXCEPTION 'flashback_recreate_table_from_ddl: ddl_info is null';
    END IF;

    v_schema := ddl_info->>'schema';
    v_table  := ddl_info->>'table';

    IF v_schema IS NULL OR v_table IS NULL THEN
        RAISE EXCEPTION 'flashback_recreate_table_from_ddl: invalid ddl_info %', ddl_info;
    END IF;

    v_is_shadow := (p_shadow_schema IS NOT NULL);
    IF v_is_shadow THEN
        v_tgt_schema := p_shadow_schema;
        v_tgt_table  := p_shadow_table;
    ELSE
        v_tgt_schema := v_schema;
        v_tgt_table  := v_table;
    END IF;

    -- Build column definitions (skip nextval defaults — restored later)
    SELECT string_agg(
        format(
            '%I %s%s%s',
            col->>'name',
            col->>'type',
            CASE
                WHEN COALESCE((col->>'not_null')::boolean, false) THEN ' NOT NULL'
                ELSE ''
            END,
            CASE
                WHEN col ? 'default_expr'
                     AND col->>'default_expr' IS NOT NULL
                     AND col->>'default_expr' <> ''
                     AND col->>'default_expr' NOT ILIKE 'nextval(%'
                THEN format(' DEFAULT %s', col->>'default_expr')
                ELSE ''
            END
        ),
        ', '
        ORDER BY ord
    )
      INTO col_defs
    FROM jsonb_array_elements(COALESCE(ddl_info->'columns', '[]'::jsonb)) WITH ORDINALITY AS t(col, ord);

    IF col_defs IS NULL OR col_defs = '' THEN
        RAISE EXCEPTION 'flashback_recreate_table_from_ddl: no columns in ddl_info %', ddl_info;
    END IF;

    -- Collect nextval defaults to defer
    SELECT COALESCE(
        jsonb_agg(
            jsonb_build_object('col', col->>'name', 'default_expr', col->>'default_expr')
            ORDER BY ord
        ) FILTER (
            WHERE col ? 'default_expr'
                  AND col->>'default_expr' IS NOT NULL
                  AND col->>'default_expr' <> ''
                  AND col->>'default_expr' ILIKE 'nextval(%'
        ),
        '[]'::jsonb
    )
      INTO v_skipped_defaults
    FROM jsonb_array_elements(COALESCE(ddl_info->'columns', '[]'::jsonb)) WITH ORDINALITY AS t(col, ord);

    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = v_tgt_schema) THEN
        EXECUTE format('CREATE SCHEMA %I', v_tgt_schema);
    END IF;

    IF NOT v_is_shadow THEN
        -- In-place mode: save ACL/owner then DROP original
        SELECT c.relacl, c.relowner::regrole
          INTO v_saved_acl, v_owner
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = v_schema AND c.relname = v_table;

        EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', v_schema, v_table);
    ELSE
        -- Shadow mode: drop leftover shadow from a previous failed restore
        EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', v_tgt_schema, v_tgt_table);
    END IF;

    -- CREATE TABLE (partitioned or regular)
    IF ddl_info->>'partition_by' IS NOT NULL THEN
        EXECUTE format('CREATE TABLE %I.%I (%s) PARTITION BY %s',
            v_tgt_schema, v_tgt_table, col_defs, ddl_info->>'partition_by');

        FOR v_part IN
            SELECT part->>'schema' AS sch,
                   part->>'name'   AS name,
                   part->>'bound'  AS bound
            FROM jsonb_array_elements(COALESCE(ddl_info->'partitions', '[]'::jsonb)) AS part
        LOOP
            IF v_is_shadow THEN
                EXECUTE format('CREATE TABLE %I.%I PARTITION OF %I.%I %s',
                    v_tgt_schema, v_tgt_table || '_' || v_part.name,
                    v_tgt_schema, v_tgt_table, v_part.bound);
            ELSE
                EXECUTE format('CREATE TABLE %I.%I PARTITION OF %I.%I %s',
                    v_part.sch, v_part.name, v_tgt_schema, v_tgt_table, v_part.bound);
            END IF;
        END LOOP;
    ELSE
        EXECUTE format('CREATE TABLE %I.%I (%s)', v_tgt_schema, v_tgt_table, col_defs);
    END IF;

    -- Primary key
    SELECT string_agg(format('%I', key_col), ', ')
      INTO pk_cols
    FROM jsonb_array_elements_text(COALESCE(ddl_info->'primary_key', '[]'::jsonb)) AS key_col;

    IF pk_cols IS NOT NULL AND pk_cols <> '' THEN
        EXECUTE format(
            'ALTER TABLE %I.%I ADD PRIMARY KEY (%s)',
            v_tgt_schema, v_tgt_table, pk_cols
        );
    END IF;

    -- CHECK / UNIQUE constraints (always); FK only in non-shadow mode
    FOR v_con IN
        SELECT con->>'name' AS name, con->>'def' AS def
        FROM jsonb_array_elements(COALESCE(ddl_info->'constraints', '[]'::jsonb)) AS con
    LOOP
        IF v_is_shadow AND v_con.def ILIKE 'FOREIGN KEY%' THEN
            CONTINUE;
        END IF;
        BEGIN
            EXECUTE format(
                'ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
                v_tgt_schema, v_tgt_table, v_con.name, v_con.def
            );
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'flashback: constraint % skipped: %', v_con.name, SQLERRM;
        END;
    END LOOP;

    -- Indexes
    FOR v_idx IN
        SELECT idx->>'def' AS def
        FROM jsonb_array_elements(COALESCE(ddl_info->'indexes', '[]'::jsonb)) AS idx
    LOOP
        IF v_is_shadow THEN
            DECLARE
                v_idx_def text;
            BEGIN
                v_idx_def := replace(v_idx.def,
                    format('%I.%I', v_schema, v_table),
                    format('%I.%I', v_tgt_schema, v_tgt_table));
                -- Also replace unqualified references
                v_idx_def := replace(v_idx_def,
                    format(' ON %I ', v_table),
                    format(' ON %I.%I ', v_tgt_schema, v_tgt_table));
                EXECUTE v_idx_def;
            EXCEPTION WHEN OTHERS THEN
                RAISE WARNING 'flashback: shadow index skipped: %', SQLERRM;
            END;
        ELSE
            EXECUTE v_idx.def;
        END IF;
    END LOOP;

    -- Non-shadow mode: restore triggers, RLS, ACL
    IF NOT v_is_shadow THEN
        FOR v_trig IN
            SELECT trig->>'def' AS def
            FROM jsonb_array_elements(COALESCE(ddl_info->'triggers', '[]'::jsonb)) AS trig
        LOOP
            EXECUTE v_trig.def;
        END LOOP;

        IF COALESCE((ddl_info->>'rls_enabled')::boolean, false) THEN
            EXECUTE format('ALTER TABLE %I.%I ENABLE ROW LEVEL SECURITY', v_tgt_schema, v_tgt_table);
        END IF;

        FOR v_pol IN
            SELECT pol->>'name' AS name,
                   pol->>'cmd' AS cmd,
                   COALESCE((pol->>'permissive')::boolean, true) AS permissive,
                   pol->>'qual' AS qual,
                   pol->>'with_check' AS with_check,
                   COALESCE((
                       SELECT string_agg(r::text, ', ')
                       FROM jsonb_array_elements_text(COALESCE(pol->'roles', '[]'::jsonb)) r
                   ), 'PUBLIC') AS roles
            FROM jsonb_array_elements(COALESCE(ddl_info->'rls_policies', '[]'::jsonb)) AS pol
        LOOP
            EXECUTE format(
                'CREATE POLICY %I ON %I.%I AS %s FOR %s TO %s%s%s',
                v_pol.name,
                v_tgt_schema,
                v_tgt_table,
                CASE WHEN v_pol.permissive THEN 'PERMISSIVE' ELSE 'RESTRICTIVE' END,
                v_pol.cmd,
                v_pol.roles,
                CASE WHEN v_pol.qual IS NOT NULL AND v_pol.qual <> '' THEN format(' USING (%s)', v_pol.qual) ELSE '' END,
                CASE WHEN v_pol.with_check IS NOT NULL AND v_pol.with_check <> '' THEN format(' WITH CHECK (%s)', v_pol.with_check) ELSE '' END
            );
        END LOOP;

        IF v_owner IS NOT NULL THEN
            EXECUTE format('ALTER TABLE %I.%I OWNER TO %s', v_tgt_schema, v_tgt_table, v_owner);
        END IF;
        IF v_saved_acl IS NOT NULL THEN
            DECLARE
                v_acl_rec record;
            BEGIN
                FOR v_acl_rec IN
                    SELECT (aclexplode(v_saved_acl)).*
                LOOP
                    IF v_acl_rec.grantee = 0 THEN
                        EXECUTE format('GRANT %s ON %I.%I TO PUBLIC',
                            v_acl_rec.privilege_type, v_tgt_schema, v_tgt_table);
                    ELSE
                        EXECUTE format('GRANT %s ON %I.%I TO %s',
                            v_acl_rec.privilege_type, v_tgt_schema, v_tgt_table,
                            quote_ident((SELECT rolname FROM pg_roles WHERE oid = v_acl_rec.grantee)));
                    END IF;
                END LOOP;
            END;
        END IF;
    END IF;

    RETURN v_skipped_defaults;
END;
$$;

-- ----------------------------------------------------------------
-- flashback_finalize_shadow_swap
-- ----------------------------------------------------------------
-- Performs the atomic swap:  brief AccessExclusiveLock on original,
-- DROP old → RENAME shadow → restore FK / triggers / RLS / ACL.
-- Returns the new OID of the restored table.
-- ----------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_finalize_shadow_swap(
    p_orig_schema   text,
    p_orig_table    text,
    p_shadow_schema text,
    p_shadow_table  text,
    ddl_info        jsonb
)
RETURNS oid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_saved_acl aclitem[];
    v_owner     regrole;
    v_new_oid   oid;
    v_part      record;
    v_trig      record;
    v_pol       record;
    v_con       record;
BEGIN
    -- Save ACL and owner from original table (before DROP)
    SELECT c.relacl, c.relowner::regrole
      INTO v_saved_acl, v_owner
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = p_orig_schema AND c.relname = p_orig_table;

    -- ── Brief exclusive lock window ────────────────────────────
    EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', p_orig_schema, p_orig_table);

    IF p_shadow_schema <> p_orig_schema THEN
        EXECUTE format('ALTER TABLE %I.%I SET SCHEMA %I',
            p_shadow_schema, p_shadow_table, p_orig_schema);
    END IF;
    EXECUTE format('ALTER TABLE %I.%I RENAME TO %I',
        p_orig_schema, p_shadow_table, p_orig_table);

    -- Rename partitions back to original names
    IF jsonb_typeof(ddl_info->'partitions') = 'array' AND jsonb_array_length(ddl_info->'partitions') > 0 THEN
        FOR v_part IN
            SELECT part->>'schema' AS sch,
                   part->>'name'   AS name
            FROM jsonb_array_elements(COALESCE(ddl_info->'partitions', '[]'::jsonb)) AS part
        LOOP
            DECLARE
                v_shadow_part text;
            BEGIN
                v_shadow_part := p_shadow_table || '_' || v_part.name;
                -- Child partitions stay in shadow schema; move + rename
                IF p_shadow_schema <> v_part.sch THEN
                    EXECUTE format('ALTER TABLE %I.%I SET SCHEMA %I',
                        p_shadow_schema, v_shadow_part, v_part.sch);
                END IF;
                EXECUTE format('ALTER TABLE %I.%I RENAME TO %I',
                    v_part.sch, v_shadow_part, v_part.name);
            EXCEPTION WHEN OTHERS THEN
                RAISE WARNING 'flashback: partition rename % failed: %', v_part.name, SQLERRM;
            END;
        END LOOP;
    END IF;
    -- ── End of brief lock window ───────────────────────────────

    v_new_oid := to_regclass(format('%I.%I', p_orig_schema, p_orig_table))::oid;

    -- Add FK constraints (deferred from shadow creation)
    FOR v_con IN
        SELECT con->>'name' AS name, con->>'def' AS def
        FROM jsonb_array_elements(COALESCE(ddl_info->'constraints', '[]'::jsonb)) AS con
        WHERE (con->>'def') ILIKE 'FOREIGN KEY%'
    LOOP
        BEGIN
            EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
                p_orig_schema, p_orig_table, v_con.name, v_con.def);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'flashback: FK constraint % deferred: %', v_con.name, SQLERRM;
        END;
    END LOOP;

    -- Restore user triggers
    FOR v_trig IN
        SELECT trig->>'def' AS def
        FROM jsonb_array_elements(COALESCE(ddl_info->'triggers', '[]'::jsonb)) AS trig
    LOOP
        BEGIN
            EXECUTE v_trig.def;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'flashback: trigger restore failed: %', SQLERRM;
        END;
    END LOOP;

    -- Restore RLS
    IF COALESCE((ddl_info->>'rls_enabled')::boolean, false) THEN
        EXECUTE format('ALTER TABLE %I.%I ENABLE ROW LEVEL SECURITY',
            p_orig_schema, p_orig_table);
    END IF;

    FOR v_pol IN
        SELECT pol->>'name' AS name,
               pol->>'cmd' AS cmd,
               COALESCE((pol->>'permissive')::boolean, true) AS permissive,
               pol->>'qual' AS qual,
               pol->>'with_check' AS with_check,
               COALESCE((
                   SELECT string_agg(r::text, ', ')
                   FROM jsonb_array_elements_text(COALESCE(pol->'roles', '[]'::jsonb)) r
               ), 'PUBLIC') AS roles
        FROM jsonb_array_elements(COALESCE(ddl_info->'rls_policies', '[]'::jsonb)) AS pol
    LOOP
        BEGIN
            EXECUTE format(
                'CREATE POLICY %I ON %I.%I AS %s FOR %s TO %s%s%s',
                v_pol.name,
                p_orig_schema, p_orig_table,
                CASE WHEN v_pol.permissive THEN 'PERMISSIVE' ELSE 'RESTRICTIVE' END,
                v_pol.cmd, v_pol.roles,
                CASE WHEN v_pol.qual IS NOT NULL AND v_pol.qual <> ''
                     THEN format(' USING (%s)', v_pol.qual) ELSE '' END,
                CASE WHEN v_pol.with_check IS NOT NULL AND v_pol.with_check <> ''
                     THEN format(' WITH CHECK (%s)', v_pol.with_check) ELSE '' END
            );
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'flashback: RLS policy % failed: %', v_pol.name, SQLERRM;
        END;
    END LOOP;

    -- Restore owner and ACL
    IF v_owner IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %I.%I OWNER TO %s',
            p_orig_schema, p_orig_table, v_owner);
    END IF;
    IF v_saved_acl IS NOT NULL THEN
        DECLARE
            v_acl_rec record;
        BEGIN
            FOR v_acl_rec IN
                SELECT (aclexplode(v_saved_acl)).*
            LOOP
                IF v_acl_rec.grantee = 0 THEN
                    EXECUTE format('GRANT %s ON %I.%I TO PUBLIC',
                        v_acl_rec.privilege_type, p_orig_schema, p_orig_table);
                ELSE
                    EXECUTE format('GRANT %s ON %I.%I TO %s',
                        v_acl_rec.privilege_type, p_orig_schema, p_orig_table,
                        quote_ident((SELECT rolname FROM pg_roles WHERE oid = v_acl_rec.grantee)));
                END IF;
            END LOOP;
        END;
    END IF;

    RETURN v_new_oid;
END;
$$;
