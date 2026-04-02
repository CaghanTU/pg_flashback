-- =================================================================
-- Restore helper functions: predicate builder, insert builder,
-- update-set builder, jsonb merge aggregate, table recreation
-- (with shadow-table support), and atomic swap.
-- =================================================================

-- ----------------------------------------------------------------
-- flashback_jsonb_concat: NULL-safe jsonb merge (right side wins)
-- ----------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_jsonb_concat(a jsonb, b jsonb)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE WHEN b IS NULL THEN a WHEN a IS NULL THEN b ELSE a || b END;
$$;

-- Custom aggregate: overlay jsonb objects in order (later keys win)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public' AND p.proname = 'flashback_jsonb_merge_agg'
          AND p.prokind = 'a'
    ) THEN
        CREATE AGGREGATE flashback_jsonb_merge_agg(jsonb) (
            SFUNC = flashback_jsonb_concat,
            STYPE = jsonb,
            INITCOND = '{}'
        );
    END IF;
END
$$;

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
-- flashback_build_update_set: builds SET clause for UPDATE replay
-- Generates "col1 = val1, col2 = val2, ..." from new_data jsonb.
-- Excludes PK columns from the SET clause (they don't change).
-- ----------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_build_update_set(
    target_rel oid,
    new_data   jsonb,
    OUT set_clause text,
    OUT pk_predicate text
)
LANGUAGE sql
AS $$
    WITH pk_cols AS (
        SELECT att.attname
        FROM pg_index i
        JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
        JOIN pg_attribute att ON att.attrelid = i.indrelid AND att.attnum = k.attnum
        WHERE i.indrelid = target_rel
          AND i.indisprimary
    )
    SELECT
        -- SET clause: non-PK columns from new_data
        (SELECT string_agg(
            format(
                '%I = %s',
                a.attname,
                CASE
                    WHEN kv.value = 'null'::jsonb THEN 'NULL'
                    WHEN a.attndims > 0 OR t.typlen = -1 AND t.typelem <> 0 THEN
                        format('%L::%s',
                            translate(kv.value::text, '[]', '{}'),
                            pg_catalog.format_type(a.atttypid, a.atttypmod))
                    WHEN t.typname IN ('jsonb', 'json') THEN
                        format('%L::%s',
                            kv.value::text,
                            pg_catalog.format_type(a.atttypid, a.atttypmod))
                    ELSE format(
                        '%L::%s',
                        kv.value #>> '{}',
                        pg_catalog.format_type(a.atttypid, a.atttypmod)
                    )
                END
            ),
            ', '
            ORDER BY a.attnum
        )
        FROM pg_attribute a
        JOIN pg_type t ON t.oid = a.atttypid
        JOIN LATERAL jsonb_each(new_data) kv(key, value)
          ON kv.key = a.attname
        WHERE a.attrelid = target_rel
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND a.attgenerated = ''
          AND a.attname NOT IN (SELECT attname FROM pk_cols)
        ),
        -- PK predicate: WHERE pk_col1 = val1 AND pk_col2 = val2
        (SELECT string_agg(
            CASE
                WHEN kv.value = 'null'::jsonb THEN format('%I IS NULL', a.attname)
                ELSE format(
                    '%I = %L::%s',
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
        JOIN LATERAL jsonb_each(new_data) kv(key, value)
          ON kv.key = a.attname
        WHERE a.attrelid = target_rel
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND a.attname IN (SELECT attname FROM pk_cols)
        );
$$;

-- ----------------------------------------------------------------
-- flashback_replay_batch_pk: batch replay for tables WITH primary key
-- ----------------------------------------------------------------
-- Uses net-effect computation: for each PK, determines the final
-- desired row state and applies it in bulk.
-- 1. TRUNCATE/DROP barrier optimization (skip events before last barrier)
-- 2. Net-effect per PK via generation tracking + jsonb merge
-- 3. Bulk DELETE / UPSERT / UPDATE in three passes
-- Returns the total number of delta events consumed (not rows affected).
-- ----------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_replay_batch_pk(
    p_shadow_schema text,
    p_shadow_table  text,
    p_shadow_oid    oid,
    p_rel_oid       oid,
    p_start_at      timestamptz,
    p_target_time   timestamptz,
    p_label         text DEFAULT 'batch_replay'
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_pk_cols       text[];
    v_pk_col_list   text;
    v_all_cols      text[];
    v_col_list      text;
    v_select_cols   text;
    v_total_events  bigint;
    v_pk_extract    text;
    v_pk_join       text;
    v_conflict_set  text;
    v_cond_set      text;
    v_last_barrier  bigint;
    v_barrier_type  text;
    v_start_id      bigint := 0;
    v_deleted       bigint := 0;
    v_upserted      bigint := 0;
    v_updated       bigint := 0;
    v_net_count     bigint;
BEGIN
    -- ── Resolve PK columns ──────────────────────────────────────
    SELECT array_agg(att.attname ORDER BY k.ord)
      INTO v_pk_cols
    FROM pg_index i
    JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
    JOIN pg_attribute att ON att.attrelid = i.indrelid AND att.attnum = k.attnum
    WHERE i.indrelid = p_shadow_oid AND i.indisprimary;

    IF v_pk_cols IS NULL OR array_length(v_pk_cols, 1) IS NULL THEN
        RAISE EXCEPTION 'flashback_replay_batch_pk: table %.% has no primary key',
            p_shadow_schema, p_shadow_table;
    END IF;

    v_pk_col_list := (SELECT string_agg(format('%I', c), ', ') FROM unnest(v_pk_cols) c);

    -- ── Resolve all insertable columns ──────────────────────────
    SELECT array_agg(a.attname ORDER BY a.attnum)
      INTO v_all_cols
    FROM pg_attribute a
    WHERE a.attrelid = p_shadow_oid AND a.attnum > 0
      AND NOT a.attisdropped AND a.attgenerated = '';

    v_col_list   := (SELECT string_agg(format('%I', c), ', ') FROM unnest(v_all_cols) c);
    v_select_cols := (SELECT string_agg(format('r.%I', c), ', ') FROM unnest(v_all_cols) c);

    -- ── PK join expression: t.pk = (ne.pk_key->>'pk')::type ────
    v_pk_join := (SELECT string_agg(
        format('t.%I = (ne.pk_key->>%L)::%s',
            a.attname, a.attname,
            pg_catalog.format_type(a.atttypid, a.atttypmod)),
        ' AND ' ORDER BY a.attnum)
    FROM pg_attribute a
    WHERE a.attrelid = p_shadow_oid AND a.attname = ANY(v_pk_cols)
      AND a.attnum > 0 AND NOT a.attisdropped);

    -- ── TRUNCATE / DROP barrier optimisation ────────────────────
    SELECT max(d.event_id) INTO v_last_barrier
    FROM flashback.delta_log d
    WHERE d.rel_oid = p_rel_oid
      AND d.committed_at IS NOT NULL
      AND d.committed_at > p_start_at    -- partition pruning lower bound
      AND d.event_time <= p_target_time  -- accurate PITR: tx commit time
      AND d.event_type IN ('TRUNCATE', 'DROP');

    IF v_last_barrier IS NOT NULL THEN
        SELECT d.event_type INTO v_barrier_type
        FROM flashback.delta_log d WHERE d.event_id = v_last_barrier;

        EXECUTE format('TRUNCATE TABLE %I.%I', p_shadow_schema, p_shadow_table);
        v_start_id := v_last_barrier;

        RAISE NOTICE 'flashback_replay_batch_pk [%]: barrier % at event_id=%, shadow truncated',
            p_label, v_barrier_type, v_last_barrier;
    END IF;

    -- ── Count remaining DML events ──────────────────────────────
    SELECT count(*) INTO v_total_events
    FROM flashback.delta_log d
    WHERE d.rel_oid = p_rel_oid
      AND d.committed_at IS NOT NULL
      AND d.committed_at > p_start_at    -- partition pruning lower bound
      AND d.event_time <= p_target_time  -- accurate PITR: tx commit time
      AND d.event_id > v_start_id
      AND d.event_type IN ('INSERT', 'UPDATE', 'DELETE');

    IF v_total_events = 0 THEN
        RAISE NOTICE 'flashback_replay_batch_pk [%]: 0 DML events — nothing to replay', p_label;
        RETURN 0;
    END IF;

    -- ── PK extraction SQL (picks key from new_data or old_data) ─
    v_pk_extract := format(
        'CASE WHEN d.event_type = ''DELETE''
              THEN (SELECT jsonb_object_agg(k, d.old_data->k) FROM unnest(%L::text[]) k WHERE d.old_data ? k)
              ELSE (SELECT jsonb_object_agg(k, d.new_data->k) FROM unnest(%L::text[]) k WHERE d.new_data ? k)
         END',
        v_pk_cols, v_pk_cols);

    -- ── Materialise events with extracted PK ────────────────────
    EXECUTE format(
        'CREATE TEMP TABLE _fb_batch_events ON COMMIT DROP AS
         SELECT d.event_id, d.event_type, d.old_data, d.new_data,
                %s AS pk_key
         FROM flashback.delta_log d
         WHERE d.rel_oid = $1
           AND d.committed_at IS NOT NULL
           AND d.committed_at > $2    -- partition pruning lower bound
           AND d.event_time <= $3     -- accurate PITR: tx commit time
           AND d.event_id > $4
           AND d.event_type IN (''INSERT'', ''UPDATE'', ''DELETE'')
         ORDER BY d.event_id',
        v_pk_extract
    ) USING p_rel_oid, p_start_at, p_target_time, v_start_id;

    -- ── Compute net-effect per PK ───────────────────────────────
    -- Generation = running count of INSERTs per PK.
    -- Within the latest generation the merged new_data is the final row.
    EXECUTE '
        CREATE TEMP TABLE _fb_net_effect ON COMMIT DROP AS
        WITH events_gen AS (
            SELECT *,
                SUM(CASE WHEN event_type = ''INSERT'' THEN 1 ELSE 0 END)
                    OVER (PARTITION BY pk_key ORDER BY event_id) AS gen
            FROM _fb_batch_events
        ),
        last_gen AS (
            SELECT pk_key, MAX(gen) AS max_gen
            FROM events_gen GROUP BY pk_key
        ),
        final_phase AS (
            SELECT e.pk_key, e.event_id, e.event_type, e.new_data
            FROM events_gen e
            JOIN last_gen lg ON lg.pk_key = e.pk_key AND e.gen = lg.max_gen
        )
        SELECT
            pk_key,
            CASE
                WHEN (array_agg(event_type ORDER BY event_id DESC))[1] = ''DELETE'' THEN ''DELETE''
                WHEN (array_agg(event_type ORDER BY event_id))[1] = ''INSERT'' THEN ''UPSERT''
                ELSE ''UPDATE''
            END AS action,
            flashback_jsonb_merge_agg(new_data ORDER BY event_id)
                FILTER (WHERE new_data IS NOT NULL) AS merged_data
        FROM final_phase
        GROUP BY pk_key';

    SELECT count(*) INTO v_net_count FROM _fb_net_effect;
    RAISE NOTICE 'flashback_replay_batch_pk [%]: % events → % unique PKs',
        p_label, v_total_events, v_net_count;

    -- ── Phase 1: Bulk DELETE ────────────────────────────────────
    EXECUTE format(
        'DELETE FROM %I.%I t USING _fb_net_effect ne
         WHERE ne.action = ''DELETE'' AND %s',
        p_shadow_schema, p_shadow_table, v_pk_join);
    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    -- ── Phase 2: Bulk UPSERT (rows whose chain contains INSERT) ─
    v_conflict_set := (SELECT string_agg(
        format('%I = EXCLUDED.%I', a.attname, a.attname),
        ', ' ORDER BY a.attnum)
    FROM pg_attribute a
    WHERE a.attrelid = p_shadow_oid AND a.attnum > 0
      AND NOT a.attisdropped AND a.attgenerated = ''
      AND a.attname <> ALL(v_pk_cols));

    IF v_conflict_set IS NOT NULL AND v_conflict_set <> '' THEN
        EXECUTE format(
            'INSERT INTO %I.%I (%s)
             SELECT %s
             FROM (SELECT (jsonb_populate_record(NULL::%I.%I, ne.merged_data)).* FROM _fb_net_effect ne WHERE ne.action = ''UPSERT'') r
             ON CONFLICT (%s) DO UPDATE SET %s',
            p_shadow_schema, p_shadow_table, v_col_list,
            v_select_cols,
            p_shadow_schema, p_shadow_table,
            v_pk_col_list, v_conflict_set);
    ELSE
        -- PK-only table
        EXECUTE format(
            'INSERT INTO %I.%I (%s)
             SELECT %s
             FROM (SELECT (jsonb_populate_record(NULL::%I.%I, ne.merged_data)).* FROM _fb_net_effect ne WHERE ne.action = ''UPSERT'') r
             ON CONFLICT (%s) DO NOTHING',
            p_shadow_schema, p_shadow_table, v_col_list,
            v_select_cols,
            p_shadow_schema, p_shadow_table,
            v_pk_col_list);
    END IF;
    GET DIAGNOSTICS v_upserted = ROW_COUNT;

    -- ── Phase 3: Bulk UPDATE (rows with only UPDATE events) ─────
    -- These rows exist in the snapshot; merged_data may be partial (diff).
    -- Use CASE WHEN ? to set only columns present in the diff.
    v_cond_set := (SELECT string_agg(
        format(
            '%I = CASE WHEN ne.merged_data ? %L '
            'THEN (jsonb_populate_record(NULL::%I.%I, ne.merged_data)).%I '
            'ELSE t.%I END',
            a.attname, a.attname,
            p_shadow_schema, p_shadow_table, a.attname, a.attname),
        ', ' ORDER BY a.attnum)
    FROM pg_attribute a
    WHERE a.attrelid = p_shadow_oid AND a.attnum > 0
      AND NOT a.attisdropped AND a.attgenerated = ''
      AND a.attname <> ALL(v_pk_cols));

    IF v_cond_set IS NOT NULL AND v_cond_set <> '' THEN
        EXECUTE format(
            'UPDATE %I.%I t SET %s
             FROM _fb_net_effect ne
             WHERE ne.action = ''UPDATE'' AND %s',
            p_shadow_schema, p_shadow_table, v_cond_set, v_pk_join);
        GET DIAGNOSTICS v_updated = ROW_COUNT;
    END IF;

    DROP TABLE IF EXISTS _fb_batch_events;
    DROP TABLE IF EXISTS _fb_net_effect;

    RAISE NOTICE 'flashback_replay_batch_pk [%]: complete — % deleted, % upserted, % updated',
        p_label, v_deleted, v_upserted, v_updated;

    RETURN v_total_events;
END;
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

    -- Primary key: defer in shadow mode for faster bulk load
    SELECT string_agg(format('%I', key_col), ', ')
      INTO pk_cols
    FROM jsonb_array_elements_text(COALESCE(ddl_info->'primary_key', '[]'::jsonb)) AS key_col;

    IF NOT v_is_shadow THEN
        -- Non-shadow: create PK immediately
        IF pk_cols IS NOT NULL AND pk_cols <> '' THEN
            EXECUTE format(
                'ALTER TABLE %I.%I ADD PRIMARY KEY (%s)',
                v_tgt_schema, v_tgt_table, pk_cols
            );
        END IF;
    END IF;
    -- Shadow mode: PK is deferred — caller adds it after snapshot load

    -- CHECK / UNIQUE constraints (always); FK only in non-shadow mode
    FOR v_con IN
        SELECT con->>'name' AS name, con->>'def' AS def
        FROM jsonb_array_elements(COALESCE(ddl_info->'constraints', '[]'::jsonb)) AS con
    LOOP
        IF v_is_shadow AND v_con.def ILIKE 'FOREIGN KEY%' THEN
            CONTINUE;
        END IF;
        -- Defer UNIQUE constraints in shadow mode (index-backed, slow during load)
        IF v_is_shadow AND v_con.def ILIKE 'UNIQUE%' THEN
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

    -- Indexes: defer all in shadow mode for faster bulk load
    IF NOT v_is_shadow THEN
        FOR v_idx IN
            SELECT idx->>'def' AS def
            FROM jsonb_array_elements(COALESCE(ddl_info->'indexes', '[]'::jsonb)) AS idx
        LOOP
            EXECUTE v_idx.def;
        END LOOP;
    END IF;
    -- Shadow mode: indexes are deferred — caller adds them after delta replay

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
-- flashback_apply_deferred_pk: add PK to shadow after snapshot load
-- ----------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_apply_deferred_pk(
    p_shadow_schema text,
    p_shadow_table  text,
    ddl_info        jsonb
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    pk_cols text;
BEGIN
    SELECT string_agg(format('%I', key_col), ', ')
      INTO pk_cols
    FROM jsonb_array_elements_text(COALESCE(ddl_info->'primary_key', '[]'::jsonb)) AS key_col;

    IF pk_cols IS NOT NULL AND pk_cols <> '' THEN
        -- Higher maintenance_work_mem for faster index build on large tables
        PERFORM set_config('maintenance_work_mem',
            COALESCE(NULLIF(current_setting('pg_flashback.index_build_work_mem', true), ''), '512MB'),
            true);
        EXECUTE format(
            'ALTER TABLE %I.%I ADD PRIMARY KEY (%s)',
            p_shadow_schema, p_shadow_table, pk_cols
        );
    END IF;

    -- Also apply deferred UNIQUE constraints
    DECLARE
        v_con record;
    BEGIN
        FOR v_con IN
            SELECT con->>'name' AS name, con->>'def' AS def
            FROM jsonb_array_elements(COALESCE(ddl_info->'constraints', '[]'::jsonb)) AS con
            WHERE con->>'def' ILIKE 'UNIQUE%'
        LOOP
            BEGIN
                EXECUTE format(
                    'ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
                    p_shadow_schema, p_shadow_table, v_con.name, v_con.def
                );
            EXCEPTION WHEN OTHERS THEN
                RAISE WARNING 'flashback: deferred constraint % skipped: %', v_con.name, SQLERRM;
            END;
        END LOOP;
    END;
END;
$$;

-- ----------------------------------------------------------------
-- flashback_apply_deferred_indexes: add indexes after delta replay
-- ----------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_apply_deferred_indexes(
    p_orig_schema   text,
    p_orig_table    text,
    p_shadow_schema text,
    p_shadow_table  text,
    ddl_info        jsonb
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_idx record;
    v_idx_def text;
BEGIN
    -- Higher maintenance_work_mem for faster index build on large tables
    PERFORM set_config('maintenance_work_mem',
        COALESCE(NULLIF(current_setting('pg_flashback.index_build_work_mem', true), ''), '512MB'),
        true);

    FOR v_idx IN
        SELECT idx->>'def' AS def
        FROM jsonb_array_elements(COALESCE(ddl_info->'indexes', '[]'::jsonb)) AS idx
    LOOP
        BEGIN
            v_idx_def := replace(v_idx.def,
                format('%I.%I', p_orig_schema, p_orig_table),
                format('%I.%I', p_shadow_schema, p_shadow_table));
            v_idx_def := replace(v_idx_def,
                format(' ON %I ', p_orig_table),
                format(' ON %I.%I ', p_shadow_schema, p_shadow_table));
            EXECUTE v_idx_def;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'flashback: deferred index skipped: %', SQLERRM;
        END;
    END LOOP;
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

    -- Rename shadow-prefixed PK constraint back to original name
    DECLARE
        v_shadow_con record;
    BEGIN
        FOR v_shadow_con IN
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = to_regclass(format('%I.%I', p_orig_schema, p_orig_table))
              AND contype IN ('p', 'u')
              AND conname LIKE p_shadow_table || '%'
        LOOP
            DECLARE
                v_new_conname text;
            BEGIN
                v_new_conname := replace(v_shadow_con.conname, p_shadow_table, p_orig_table);
                EXECUTE format('ALTER TABLE %I.%I RENAME CONSTRAINT %I TO %I',
                    p_orig_schema, p_orig_table, v_shadow_con.conname, v_new_conname);
            EXCEPTION WHEN OTHERS THEN
                RAISE WARNING 'flashback: constraint rename % → % failed: %',
                    v_shadow_con.conname, v_new_conname, SQLERRM;
            END;
        END LOOP;
    END;

    -- Rename shadow-prefixed indexes back to original names
    DECLARE
        v_shadow_idx record;
        v_new_idxname text;
    BEGIN
        FOR v_shadow_idx IN
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = p_orig_schema
              AND tablename = p_orig_table
              AND indexname LIKE p_shadow_table || '%'
        LOOP
            BEGIN
                v_new_idxname := replace(v_shadow_idx.indexname, p_shadow_table, p_orig_table);
                EXECUTE format('ALTER INDEX %I.%I RENAME TO %I',
                    p_orig_schema, v_shadow_idx.indexname, v_new_idxname);
            EXCEPTION WHEN OTHERS THEN
                RAISE WARNING 'flashback: index rename % → % failed: %',
                    v_shadow_idx.indexname, v_new_idxname, SQLERRM;
            END;
        END LOOP;
    END;

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
