use pgrx::prelude::*;

extension_sql!(
    r#"
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
          AND NOT a.attisdropped;
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
          AND NOT a.attisdropped;
    $$;

    CREATE OR REPLACE FUNCTION flashback_restore_rows_from_snapshot(
        target_rel oid,
        target_schema text,
        target_table text,
        snapshot jsonb
    )
    RETURNS void
    LANGUAGE plpgsql
    AS $$
    DECLARE
        row_item record;
        cols text;
        vals text;
    BEGIN
        FOR row_item IN
            SELECT value AS row_data
            FROM jsonb_array_elements(COALESCE(snapshot, '[]'::jsonb))
        LOOP
            SELECT col_list, val_list
              INTO cols, vals
            FROM flashback_build_insert_parts(target_rel, row_item.row_data);

            IF cols IS NOT NULL AND cols <> '' THEN
                EXECUTE format(
                    'INSERT INTO %I.%I (%s) VALUES (%s)',
                    target_schema,
                    target_table,
                    cols,
                    vals
                );
            END IF;
        END LOOP;
    END;
    $$;

    CREATE OR REPLACE FUNCTION flashback_recreate_table_from_ddl(ddl_info jsonb)
    RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
    DECLARE
        v_schema text;
        v_table text;
        col_defs text;
        pk_cols text;
        v_skipped_defaults jsonb;
        v_con record;
        v_idx record;
        v_part record;
    BEGIN
        IF ddl_info IS NULL THEN
            RAISE EXCEPTION 'flashback_recreate_table_from_ddl: ddl_info is null';
        END IF;

        v_schema := ddl_info->>'schema';
        v_table := ddl_info->>'table';

        IF v_schema IS NULL OR v_table IS NULL THEN
            RAISE EXCEPTION 'flashback_recreate_table_from_ddl: invalid ddl_info %', ddl_info;
        END IF;

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

        -- Collect nextval defaults that were skipped during CREATE TABLE so they
        -- can be restored via ALTER TABLE after data is loaded.
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

        EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', v_schema);
        EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', v_schema, v_table);

        IF ddl_info->>'partition_by' IS NOT NULL THEN
            EXECUTE format('CREATE TABLE %I.%I (%s) PARTITION BY %s',
                v_schema, v_table, col_defs, ddl_info->>'partition_by');

            FOR v_part IN
                SELECT part->>'schema' AS sch,
                       part->>'name'   AS name,
                       part->>'bound'  AS bound
                FROM jsonb_array_elements(COALESCE(ddl_info->'partitions', '[]'::jsonb)) AS part
            LOOP
                EXECUTE format('CREATE TABLE %I.%I PARTITION OF %I.%I %s',
                    v_part.sch, v_part.name, v_schema, v_table, v_part.bound);
            END LOOP;
        ELSE
            EXECUTE format('CREATE TABLE %I.%I (%s)', v_schema, v_table, col_defs);
        END IF;

        SELECT string_agg(format('%I', key_col), ', ')
          INTO pk_cols
        FROM jsonb_array_elements_text(COALESCE(ddl_info->'primary_key', '[]'::jsonb)) AS key_col;

        IF pk_cols IS NOT NULL AND pk_cols <> '' THEN
            EXECUTE format(
                'ALTER TABLE %I.%I ADD PRIMARY KEY (%s)',
                v_schema,
                v_table,
                pk_cols
            );
        END IF;

        -- Restore CHECK, UNIQUE, FK constraints
        FOR v_con IN
            SELECT con->>'name' AS name, con->>'def' AS def
            FROM jsonb_array_elements(COALESCE(ddl_info->'constraints', '[]'::jsonb)) AS con
        LOOP
            EXECUTE format(
                'ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
                v_schema, v_table, v_con.name, v_con.def
            );
        END LOOP;

        -- Restore custom indexes (non-PK, non-constraint)
        FOR v_idx IN
            SELECT idx->>'def' AS def
            FROM jsonb_array_elements(COALESCE(ddl_info->'indexes', '[]'::jsonb)) AS idx
        LOOP
            EXECUTE v_idx.def;
        END LOOP;

        RETURN v_skipped_defaults;
    END;
    $$;
    "#,
    name = "flashback_restore_replay_helpers",
    requires = ["flashback_storage_schema_bootstrap"],
);
