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

CREATE OR REPLACE FUNCTION flashback_build_predicate(col_meta jsonb, payload jsonb)
RETURNS text
LANGUAGE sql
AS $$
    SELECT string_agg(
        CASE
            WHEN kv.value = 'null'::jsonb THEN format('%I IS NULL', kv.key)
            WHEN (col_meta->kv.key->>'is_array')::boolean THEN
                format('%I IS NOT DISTINCT FROM %L::%s', kv.key,
                    CASE WHEN jsonb_typeof(kv.value) = 'string'
                         THEN kv.value #>> '{}'
                         ELSE translate(kv.value::text, '[]', '{}') END,
                    col_meta->kv.key->>'type')
            WHEN (col_meta->kv.key->>'is_json')::boolean THEN
                format('%I IS NOT DISTINCT FROM %L::%s', kv.key,
                    CASE WHEN jsonb_typeof(kv.value) = 'string'
                         THEN kv.value #>> '{}'
                         ELSE kv.value::text END,
                    col_meta->kv.key->>'type')
            ELSE format(
                '%I IS NOT DISTINCT FROM %L::%s',
                kv.key,
                kv.value #>> '{}',
                col_meta->kv.key->>'type'
            )
        END,
        ' AND '
        ORDER BY (col_meta->kv.key->>'attnum')::int
    )
    FROM jsonb_each(payload) kv(key, value)
    WHERE col_meta ? kv.key;
$$;

CREATE OR REPLACE FUNCTION flashback_build_insert_parts(
    col_meta jsonb,
    payload jsonb,
    OUT col_list text,
    OUT val_list text
)
LANGUAGE sql
AS $$
    SELECT
        string_agg(format('%I', kv.key), ', ' ORDER BY (col_meta->kv.key->>'attnum')::int),
        string_agg(
            CASE
                WHEN kv.value = 'null'::jsonb THEN 'NULL'
                WHEN (col_meta->kv.key->>'is_array')::boolean THEN
                    format('%L::%s',
                        CASE WHEN jsonb_typeof(kv.value) = 'string'
                             THEN kv.value #>> '{}'
                             ELSE translate(kv.value::text, '[]', '{}') END,
                        col_meta->kv.key->>'type')
                WHEN (col_meta->kv.key->>'is_json')::boolean THEN
                    format('%L::%s',
                        CASE WHEN jsonb_typeof(kv.value) = 'string'
                             THEN kv.value #>> '{}'
                             ELSE kv.value::text END,
                        col_meta->kv.key->>'type')
                ELSE format(
                    '%L::%s',
                    kv.value #>> '{}',
                    col_meta->kv.key->>'type'
                )
            END,
            ', '
            ORDER BY (col_meta->kv.key->>'attnum')::int
        )
    FROM jsonb_each(payload) kv(key, value)
    WHERE col_meta ? kv.key;
$$;

-- ----------------------------------------------------------------
-- flashback_build_update_set: builds SET clause for UPDATE replay
-- Generates "col1 = val1, col2 = val2, ..." from new_data jsonb.
-- Excludes PK columns from the SET clause (they don't change).
-- ----------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_build_update_set(
    col_meta jsonb,
    new_data jsonb,
    pk_cols  text[],
    OUT set_clause text,
    OUT pk_predicate text
)
LANGUAGE sql
AS $$
    SELECT
        (SELECT string_agg(
            format(
                '%I = %s',
                kv.key,
                CASE
                    WHEN kv.value = 'null'::jsonb THEN 'NULL'
                    WHEN (col_meta->kv.key->>'is_array')::boolean THEN
                        format('%L::%s',
                            CASE WHEN jsonb_typeof(kv.value) = 'string'
                                 THEN kv.value #>> '{}'
                                 ELSE translate(kv.value::text, '[]', '{}') END,
                            col_meta->kv.key->>'type')
                    WHEN (col_meta->kv.key->>'is_json')::boolean THEN
                        format('%L::%s',
                            CASE WHEN jsonb_typeof(kv.value) = 'string'
                                 THEN kv.value #>> '{}'
                                 ELSE kv.value::text END,
                            col_meta->kv.key->>'type')
                    ELSE format(
                        '%L::%s',
                        kv.value #>> '{}',
                        col_meta->kv.key->>'type'
                    )
                END
            ),
            ', '
            ORDER BY (col_meta->kv.key->>'attnum')::int
        )
        FROM jsonb_each(new_data) kv(key, value)
        WHERE col_meta ? kv.key
          AND NOT (kv.key = ANY(pk_cols))
        ),
        (SELECT string_agg(
            CASE
                WHEN kv.value = 'null'::jsonb THEN format('%I IS NULL', kv.key)
                ELSE format(
                    '%I = %L::%s',
                    kv.key,
                    kv.value #>> '{}',
                    col_meta->kv.key->>'type'
                )
            END,
            ' AND '
            ORDER BY (col_meta->kv.key->>'attnum')::int
        )
        FROM jsonb_each(new_data) kv(key, value)
        WHERE col_meta ? kv.key
          AND kv.key = ANY(pk_cols)
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
    v_temp_probe text;
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

    -- pg_temp is a session alias, not a schema PostgreSQL permits us to
    -- CREATE.  Ensure the backend has a temporary namespace and resolve the
    -- alias to its physical pg_temp_N name before the generic schema check.
    IF v_tgt_schema = 'pg_temp' THEN
        IF pg_my_temp_schema() = 0 THEN
            v_temp_probe := format('__fb_temp_namespace_%s_%s',
                                   pg_backend_pid(),
                                   floor(random() * 1000000000)::bigint);
            EXECUTE format('CREATE TEMP TABLE %I () ON COMMIT DROP', v_temp_probe);
            EXECUTE format('DROP TABLE pg_temp.%I', v_temp_probe);
        END IF;

        SELECT nspname INTO STRICT v_tgt_schema
        FROM pg_namespace
        WHERE oid = pg_my_temp_schema();
    END IF;

    -- Build column definitions (skip nextval defaults — restored later)
    SELECT string_agg(
        format(
            '%I %s%s%s%s',
            col->>'name',
            col->>'type',
            CASE
                WHEN NULLIF(col->>'collation', '') IS NOT NULL
                THEN format(' COLLATE %s', col->>'collation')
                ELSE ''
            END,
            CASE
                WHEN COALESCE((col->>'not_null')::boolean, false) THEN ' NOT NULL'
                ELSE ''
            END,
            CASE
                -- Identity is catalog state, not a normal nextval default.
                -- Recreate its mode/options directly; restore_lsn resets the
                -- new sequence to the recovered edge after the shadow swap.
                WHEN col->>'identity' IN ('a', 'd') THEN format(
                    ' GENERATED %s AS IDENTITY%s',
                    CASE col->>'identity'
                        WHEN 'a' THEN 'ALWAYS'
                        ELSE 'BY DEFAULT'
                    END,
                    CASE
                        WHEN col->'identity_options' IS NOT NULL
                             AND col->'identity_options' <> 'null'::jsonb
                        THEN format(
                            ' (START WITH %s INCREMENT BY %s MINVALUE %s MAXVALUE %s CACHE %s %s)',
                            col#>>'{identity_options,start}',
                            col#>>'{identity_options,increment}',
                            col#>>'{identity_options,min}',
                            col#>>'{identity_options,max}',
                            col#>>'{identity_options,cache}',
                            CASE WHEN COALESCE((col#>>'{identity_options,cycle}')::boolean, false)
                                 THEN 'CYCLE' ELSE 'NO CYCLE' END
                        )
                        ELSE ''
                    END
                )
                -- GENERATED ALWAYS AS (expr) STORED
                WHEN col->>'generated' = 's'
                     AND col->>'default_expr' IS NOT NULL
                THEN format(' GENERATED ALWAYS AS (%s) STORED', col->>'default_expr')
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
            jsonb_build_object(
                'col', col->>'name',
                'default_expr', col->>'default_expr',
                'sequence_options', col->'identity_options'
            )
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
            IF ddl_info->'primary_key_constraint' IS NOT NULL
               AND ddl_info->'primary_key_constraint' <> 'null'::jsonb
            THEN
                EXECUTE format(
                    'ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
                    v_tgt_schema, v_tgt_table,
                    ddl_info#>>'{primary_key_constraint,name}',
                    ddl_info#>>'{primary_key_constraint,def}'
                );
            ELSE
                -- Backward compatibility for stored schema_def payloads
                -- created before the PK constraint identity was captured.
                EXECUTE format(
                    'ALTER TABLE %I.%I ADD PRIMARY KEY (%s)',
                    v_tgt_schema, v_tgt_table, pk_cols
                );
            END IF;
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
            RAISE EXCEPTION 'flashback: constraint % failed: %', v_con.name, SQLERRM;
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
            SELECT trig->>'name' AS name,
                   trig->>'def' AS def,
                   COALESCE(trig->>'enabled', 'O') AS enabled
            FROM jsonb_array_elements(COALESCE(ddl_info->'triggers', '[]'::jsonb)) AS trig
        LOOP
            EXECUTE v_trig.def;
            IF v_trig.enabled = 'D' THEN
                EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER %I',
                    v_tgt_schema, v_tgt_table, v_trig.name);
            ELSIF v_trig.enabled = 'R' THEN
                EXECUTE format('ALTER TABLE %I.%I ENABLE REPLICA TRIGGER %I',
                    v_tgt_schema, v_tgt_table, v_trig.name);
            ELSIF v_trig.enabled = 'A' THEN
                EXECUTE format('ALTER TABLE %I.%I ENABLE ALWAYS TRIGGER %I',
                    v_tgt_schema, v_tgt_table, v_trig.name);
            ELSIF v_trig.enabled <> 'O' THEN
                RAISE EXCEPTION 'flashback: unknown trigger enabled state % for %',
                    v_trig.enabled, v_trig.name;
            END IF;
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
                       SELECT string_agg(format('%I', r), ', ' ORDER BY r)
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
        IF ddl_info->'primary_key_constraint' IS NOT NULL
           AND ddl_info->'primary_key_constraint' <> 'null'::jsonb
        THEN
            EXECUTE format(
                'ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
                p_shadow_schema, p_shadow_table,
                ddl_info#>>'{primary_key_constraint,name}',
                ddl_info#>>'{primary_key_constraint,def}'
            );
        ELSE
            EXECUTE format(
                'ALTER TABLE %I.%I ADD PRIMARY KEY (%s)',
                p_shadow_schema, p_shadow_table, pk_cols
            );
        END IF;
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
                RAISE EXCEPTION 'flashback: deferred constraint % failed: %', v_con.name, SQLERRM;
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
            RAISE EXCEPTION 'flashback: deferred index failed: %', SQLERRM;
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
    v_orig_oid  oid;
    v_incoming_fks  jsonb := '[]'::jsonb;
    v_dependent_views jsonb := '[]'::jsonb;
    v_inherit_children jsonb := '[]'::jsonb;  -- classical INHERITS children (not partitions)
    v_dep  record;
    v_ifk  record;
    v_heir record;
    v_partition_parent text;   -- parent table qualified name if target is a leaf partition
    v_partition_bound  text;   -- partition bound expression (FOR VALUES ...)
BEGIN
    -- Save ACL and owner from original table (before DROP)
    SELECT c.relacl, c.relowner::regrole, c.oid
      INTO v_saved_acl, v_owner, v_orig_oid
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = p_orig_schema AND c.relname = p_orig_table;

    -- ── Capture partition parent info BEFORE the DROP ──────────────
    -- If the target table is a leaf partition we must re-ATTACH it after the
    -- shadow rename, otherwise the parent partitioned table loses the partition.
    IF v_orig_oid IS NOT NULL THEN
        SELECT format('%I.%I', pn.nspname, pc.relname),
               pg_get_expr(child.relpartbound, child.oid)
          INTO v_partition_parent, v_partition_bound
        FROM pg_inherits i
        JOIN pg_class  child  ON child.oid  = i.inhrelid
        JOIN pg_class  pc     ON pc.oid     = i.inhparent
        JOIN pg_namespace pn  ON pn.oid     = pc.relnamespace
        WHERE i.inhrelid = v_orig_oid
          AND pc.relkind = 'p'   -- parent must be a partitioned table
        LIMIT 1;
    END IF;

    -- ── Collect dependent objects BEFORE the CASCADE drop ─────────
    -- Incoming FKs: other tables' constraints that reference this table.
    -- DROP TABLE CASCADE would silently remove them; we recreate them after rename.
    IF v_orig_oid IS NOT NULL THEN

        -- Classical INHERITS children: tables that INHERIT from this table
        -- (NOT partition-based — those have a partitioned parent with relkind='p').
        -- DROP TABLE CASCADE would destroy them; we re-attach via ALTER TABLE INHERIT.
        SELECT COALESCE(jsonb_agg(
            jsonb_build_object(
                'schema', cn.nspname,
                'table',  cc.relname
            ) ORDER BY cn.nspname, cc.relname
        ), '[]'::jsonb)
          INTO v_inherit_children
        FROM pg_inherits i
        JOIN pg_class  cc ON cc.oid  = i.inhrelid
        JOIN pg_namespace cn ON cn.oid = cc.relnamespace
        WHERE i.inhparent = v_orig_oid
          AND NOT EXISTS (
              -- Exclude partition children (their parent has relkind='p')
              SELECT 1 FROM pg_class pp WHERE pp.oid = v_orig_oid AND pp.relkind = 'p'
          );

        SELECT COALESCE(jsonb_agg(
            jsonb_build_object(
                'conname', con.conname,
                'schema',  n2.nspname,
                'table',   c2.relname,
                'def',     pg_get_constraintdef(con.oid)
            ) ORDER BY con.conname
        ), '[]'::jsonb)
          INTO v_incoming_fks
        FROM pg_constraint con
        JOIN pg_class c2 ON c2.oid = con.conrelid
        JOIN pg_namespace n2 ON n2.oid = c2.relnamespace
        WHERE con.confrelid = v_orig_oid
          AND con.conrelid  <> v_orig_oid   -- exclude self-referential FKs (handled via ddl_info)
          AND con.contype = 'f';

        -- Dependent views and materialized views (deptype='n' = normal dependency).
        -- Capture enough metadata to faithfully recreate them after the swap:
        --   def        – the view body (pg_get_viewdef)
        --   owner      – role name so OWNER TO can be reapplied
        --   options    – reloptions (security_barrier, check_option, etc.)
        --   acl        – serialised aclitem[] for GRANT replay
        --   indexes    – matview index definitions (pg_get_indexdef) so we can
        --                rebuild them; ignored for plain views
        --   populate   – true when the matview is currently populated
        SELECT COALESCE(jsonb_agg(
            jsonb_build_object(
                'schema',   n3.nspname,
                'name',     c3.relname,
                'kind',     c3.relkind,
                'def',      pg_get_viewdef(c3.oid),
                'owner',    (SELECT rolname FROM pg_roles WHERE oid = c3.relowner),
                'options',  COALESCE(to_jsonb(c3.reloptions), 'null'::jsonb),
                'acl',      COALESCE((
                    SELECT jsonb_agg(jsonb_build_object(
                        'grantee',      CASE WHEN ae.grantee = 0 THEN 'PUBLIC'
                                             ELSE (SELECT rolname FROM pg_roles WHERE oid = ae.grantee)
                                        END,
                        'privilege',    ae.privilege_type,
                        'is_grantable', ae.is_grantable
                    ))
                    FROM aclexplode(c3.relacl) AS ae(grantor, grantee, privilege_type, is_grantable)
                ), '[]'::jsonb),
                'indexes',  COALESCE((
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'def',   pg_get_indexdef(i.indexrelid),
                            'valid', i.indisvalid
                        )
                    )
                    FROM pg_index i
                    WHERE i.indrelid = c3.oid
                ), '[]'::jsonb),
                'populate', CASE WHEN c3.relkind = 'm'
                                 THEN (c3.relispopulated)
                                 ELSE NULL END
            ) ORDER BY n3.nspname, c3.relname
        ), '[]'::jsonb)
          INTO v_dependent_views
        FROM (
            -- Views/matviews depend on tables via their rewrite rules, not directly.
            -- Use a recursive CTE to capture chained views (view2→view1→table).
            -- Seed: views/matviews whose rewrite rule directly references v_orig_oid.
            -- Recurse: views whose rule references any already-found view.
            WITH RECURSIVE dep_views(view_oid) AS (
                -- Base: direct dependents of the original table
                SELECT DISTINCT rw.ev_class AS view_oid
                FROM pg_depend d
                JOIN pg_rewrite rw ON rw.oid = d.objid
                                   AND d.classid = 'pg_rewrite'::regclass
                JOIN pg_class cv ON cv.oid = rw.ev_class
                WHERE d.refobjid   = v_orig_oid
                  AND d.refclassid = 'pg_class'::regclass
                  AND cv.relkind   IN ('v', 'm')
                  AND d.deptype    = 'n'
                  AND rw.ev_class <> v_orig_oid

                UNION

                -- Recursive: views that depend on any already-found view
                SELECT DISTINCT rw2.ev_class
                FROM dep_views dv
                JOIN pg_depend d2 ON d2.refobjid   = dv.view_oid
                                  AND d2.refclassid = 'pg_class'::regclass
                                  AND d2.deptype    = 'n'
                JOIN pg_rewrite rw2 ON rw2.oid = d2.objid
                                    AND d2.classid = 'pg_rewrite'::regclass
                JOIN pg_class cv2 ON cv2.oid = rw2.ev_class
                WHERE cv2.relkind IN ('v', 'm')
                  AND rw2.ev_class <> dv.view_oid
            )
            SELECT DISTINCT view_oid
            FROM dep_views
        ) subq
        JOIN pg_class c3 ON c3.oid = subq.view_oid
        JOIN pg_namespace n3 ON n3.oid = c3.relnamespace;
    END IF;

    -- ── Detach classical INHERITS children BEFORE DROP ───────────
    -- DROP TABLE CASCADE would destroy them; detach first so DROP doesn't cascade.
    FOR v_heir IN
        SELECT r->>'schema' AS cschema, r->>'table' AS ctable
        FROM jsonb_array_elements(v_inherit_children) AS t(r)
    LOOP
        BEGIN
            EXECUTE format('ALTER TABLE %I.%I NO INHERIT %I.%I',
                v_heir.cschema, v_heir.ctable,
                p_orig_schema, p_orig_table);
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'flashback: could not detach INHERITS child %.% from parent %.%: %',
                v_heir.cschema, v_heir.ctable, p_orig_schema, p_orig_table, SQLERRM;
        END;
    END LOOP;

    -- ── Brief exclusive lock window ────────────────────────────
    EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', p_orig_schema, p_orig_table);

    IF p_shadow_schema <> p_orig_schema THEN
        EXECUTE format('ALTER TABLE %I.%I SET SCHEMA %I',
            p_shadow_schema, p_shadow_table, p_orig_schema);
    END IF;
    EXECUTE format('ALTER TABLE %I.%I RENAME TO %I',
        p_orig_schema, p_shadow_table, p_orig_table);

    -- ── Re-attach classical INHERITS children ────────────────────
    -- DROP TABLE CASCADE removed the INHERITS relationship.
    -- Re-establish it with the renamed table as the new parent.
    FOR v_heir IN
        SELECT r->>'schema' AS cschema, r->>'table' AS ctable
        FROM jsonb_array_elements(v_inherit_children) AS t(r)
    LOOP
        BEGIN
            EXECUTE format('ALTER TABLE %I.%I INHERIT %I.%I',
                v_heir.cschema, v_heir.ctable,
                p_orig_schema, p_orig_table);
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'flashback: could not re-attach INHERITS child %.% to parent %.%: %',
                v_heir.cschema, v_heir.ctable, p_orig_schema, p_orig_table, SQLERRM;
        END;
    END LOOP;

    -- ── Re-attach as partition if original was a leaf partition ───
    -- DROP TABLE CASCADE removes the partition from its parent's tree.
    -- After rename we must ATTACH it back with the original bound expression.
    -- The shadow table was a plain table (not a partition), so any transition
    -- table triggers installed on it are compatible — we use ROW triggers.
    IF v_partition_parent IS NOT NULL AND v_partition_bound IS NOT NULL THEN
        BEGIN
            EXECUTE format('ALTER TABLE %s ATTACH PARTITION %I.%I %s',
                v_partition_parent,
                p_orig_schema, p_orig_table,
                v_partition_bound);
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'flashback: could not re-attach % to partition tree (%): %',
                p_orig_table, v_partition_parent, SQLERRM;
        END;
    END IF;

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
                RAISE EXCEPTION 'flashback: constraint rename % → % failed: %',
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
                RAISE EXCEPTION 'flashback: index rename % → % failed: %',
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
                RAISE EXCEPTION 'flashback: partition rename % failed: %', v_part.name, SQLERRM;
            END;
        END LOOP;
    END IF;
    -- ── End of brief lock window ───────────────────────────────

    v_new_oid := to_regclass(format('%I.%I', p_orig_schema, p_orig_table))::oid;

    -- ── Attempt to recreate CASCADE-dropped dependent views ────────
    -- Views and matviews were removed by DROP TABLE ... CASCADE above.
    -- We attempt to recreate them in dependency order (sorted by schema+name,
    -- a reasonable proxy for shallow nesting). After recreation we replay:
    --   • OWNER TO            – from captured relowner
    --   • view options        – security_barrier, check_option etc. via reloptions
    --   • GRANT statements    – from captured aclitem[]
    -- For materialized views we also:
    --   • Recreate indexes    – from pg_get_indexdef snapshots
    --   • REFRESH             – only when the matview was populated before the swap
    -- If any step fails the error is logged and we continue with remaining views.
    IF jsonb_array_length(v_dependent_views) > 0 THEN
        FOR v_dep IN
            SELECT d->>'schema'  AS sch,
                   d->>'name'    AS nm,
                   d->>'kind'    AS knd,
                   d->>'def'     AS def,
                   d->>'owner'   AS owner,
                   d->'options'  AS options,
                   d->'acl'      AS acl,
                   d->'indexes'  AS indexes,
                   COALESCE((d->>'populate')::boolean, false) AS populate
            FROM jsonb_array_elements(v_dependent_views) d
            ORDER BY d->>'schema', d->>'name'
        LOOP
            BEGIN
                -- 1. Create the view / matview body
                IF v_dep.knd = 'm' THEN
                    EXECUTE format('CREATE MATERIALIZED VIEW %I.%I AS %s WITH NO DATA',
                        v_dep.sch, v_dep.nm,
                        rtrim(v_dep.def, ' ;'));
                ELSE
                    EXECUTE format('CREATE VIEW %I.%I AS %s',
                        v_dep.sch, v_dep.nm, v_dep.def);
                END IF;

                -- 2. Restore owner
                IF v_dep.owner IS NOT NULL THEN
                    BEGIN
                        EXECUTE format('ALTER %s %I.%I OWNER TO %I',
                            CASE WHEN v_dep.knd = 'm' THEN 'MATERIALIZED VIEW' ELSE 'VIEW' END,
                            v_dep.sch, v_dep.nm, v_dep.owner);
                    EXCEPTION WHEN OTHERS THEN
                        RAISE EXCEPTION 'flashback: could not restore owner % for %.%: %',
                            v_dep.owner, v_dep.sch, v_dep.nm, SQLERRM;
                    END;
                END IF;

                -- 3. Restore reloptions (security_barrier, check_option, etc.)
                IF v_dep.knd = 'v' AND v_dep.options IS NOT NULL AND v_dep.options <> 'null'::jsonb THEN
                    DECLARE
                        v_opt text;
                    BEGIN
                        FOR v_opt IN
                            SELECT jsonb_array_elements_text(v_dep.options)
                        LOOP
                            BEGIN
                                EXECUTE format('ALTER VIEW %I.%I SET (%s)',
                                    v_dep.sch, v_dep.nm, v_opt);
                            EXCEPTION WHEN OTHERS THEN
                                RAISE EXCEPTION 'flashback: view option % on %.% failed: %',
                                    v_opt, v_dep.sch, v_dep.nm, SQLERRM;
                            END;
                        END LOOP;
                    END;
                END IF;

                -- 4. Restore grants from aclexplode() snapshot
                IF v_dep.acl IS NOT NULL AND jsonb_array_length(v_dep.acl) > 0 THEN
                    DECLARE
                        v_acl_rec record;
                    BEGIN
                        FOR v_acl_rec IN
                            SELECT a->>'grantee'             AS grantee,
                                   a->>'privilege'           AS privilege,
                                   (a->>'is_grantable')::boolean AS is_grantable
                            FROM jsonb_array_elements(v_dep.acl) a
                        LOOP
                            BEGIN
                                IF v_acl_rec.grantee = 'PUBLIC' THEN
                                    EXECUTE format('GRANT %s ON %I.%I TO PUBLIC%s',
                                        v_acl_rec.privilege, v_dep.sch, v_dep.nm,
                                        CASE WHEN v_acl_rec.is_grantable
                                             THEN ' WITH GRANT OPTION' ELSE '' END);
                                ELSE
                                    EXECUTE format('GRANT %s ON %I.%I TO %I%s',
                                        v_acl_rec.privilege, v_dep.sch, v_dep.nm,
                                        v_acl_rec.grantee,
                                        CASE WHEN v_acl_rec.is_grantable
                                             THEN ' WITH GRANT OPTION' ELSE '' END);
                                END IF;
                            EXCEPTION WHEN OTHERS THEN
                                RAISE EXCEPTION 'flashback: GRANT % on %.% to % failed: %',
                                    v_acl_rec.privilege, v_dep.sch, v_dep.nm,
                                    v_acl_rec.grantee, SQLERRM;
                            END;
                        END LOOP;
                    END;
                END IF;

                -- 5. Recreate matview indexes
                IF v_dep.knd = 'm' AND v_dep.indexes IS NOT NULL
                   AND jsonb_array_length(v_dep.indexes) > 0 THEN
                    DECLARE
                        v_idx_rec record;
                        v_idx_def text;
                    BEGIN
                        FOR v_idx_rec IN
                            SELECT idx->>'def' AS def
                            FROM jsonb_array_elements(v_dep.indexes) idx
                            WHERE (idx->>'valid')::boolean
                        LOOP
                            BEGIN
                                EXECUTE v_idx_rec.def;
                            EXCEPTION WHEN OTHERS THEN
                                RAISE EXCEPTION 'flashback: matview index recreate for %.% failed: %',
                                    v_dep.sch, v_dep.nm, SQLERRM;
                            END;
                        END LOOP;
                    END;
                END IF;

                -- 6. Populate matview if it was populated before the cascade drop
                IF v_dep.knd = 'm' AND v_dep.populate THEN
                    BEGIN
                        EXECUTE format('REFRESH MATERIALIZED VIEW %I.%I',
                            v_dep.sch, v_dep.nm);
                    EXCEPTION WHEN OTHERS THEN
                        RAISE EXCEPTION 'flashback: REFRESH MATERIALIZED VIEW %.% failed: %',
                            v_dep.sch, v_dep.nm, SQLERRM;
                    END;
                END IF;

            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'flashback: could not recreate % %.% after restore (%): recreate manually. Definition: %',
                    CASE WHEN v_dep.knd = 'm' THEN 'materialized view' ELSE 'view' END,
                    v_dep.sch, v_dep.nm, SQLERRM, v_dep.def;
            END;
        END LOOP;
    END IF;

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
            RAISE EXCEPTION 'flashback: FK constraint % deferred: %', v_con.name, SQLERRM;
        END;
    END LOOP;

    -- Restore user triggers
    FOR v_trig IN
        SELECT trig->>'name' AS name,
               trig->>'def' AS def,
               COALESCE(trig->>'enabled', 'O') AS enabled
        FROM jsonb_array_elements(COALESCE(ddl_info->'triggers', '[]'::jsonb)) AS trig
    LOOP
        BEGIN
            EXECUTE v_trig.def;
            IF v_trig.enabled = 'D' THEN
                EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER %I',
                    p_orig_schema, p_orig_table, v_trig.name);
            ELSIF v_trig.enabled = 'R' THEN
                EXECUTE format('ALTER TABLE %I.%I ENABLE REPLICA TRIGGER %I',
                    p_orig_schema, p_orig_table, v_trig.name);
            ELSIF v_trig.enabled = 'A' THEN
                EXECUTE format('ALTER TABLE %I.%I ENABLE ALWAYS TRIGGER %I',
                    p_orig_schema, p_orig_table, v_trig.name);
            ELSIF v_trig.enabled <> 'O' THEN
                RAISE EXCEPTION 'flashback: unknown trigger enabled state % for %',
                    v_trig.enabled, v_trig.name;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'flashback: trigger restore failed: %', SQLERRM;
        END;
    END LOOP;

    -- Restore RLS
    IF COALESCE((ddl_info->>'rls_enabled')::boolean, false) THEN
        EXECUTE format('ALTER TABLE %I.%I ENABLE ROW LEVEL SECURITY',
            p_orig_schema, p_orig_table);
    END IF;
    IF COALESCE((ddl_info->>'force_rls')::boolean, false) THEN
        EXECUTE format('ALTER TABLE %I.%I FORCE ROW LEVEL SECURITY',
            p_orig_schema, p_orig_table);
    END IF;

    FOR v_pol IN
        SELECT pol->>'name' AS name,
               pol->>'cmd' AS cmd,
               COALESCE((pol->>'permissive')::boolean, true) AS permissive,
               pol->>'qual' AS qual,
               pol->>'with_check' AS with_check,
               COALESCE((
                   SELECT string_agg(format('%I', r), ', ' ORDER BY r)
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
            RAISE EXCEPTION 'flashback: RLS policy % failed: %', v_pol.name, SQLERRM;
        END;
    END LOOP;

    -- Restore owner and ACL. When the live relation was already DROP'd before
    -- restore, catalog rows are gone — use ownership captured in schema_def.
    IF v_owner IS NULL AND COALESCE(ddl_info->>'owner', '') <> '' THEN
        BEGIN
            EXECUTE format('ALTER TABLE %I.%I OWNER TO %I',
                p_orig_schema, p_orig_table, ddl_info->>'owner');
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'flashback: could not restore owner % for %.% from schema_def: %',
                ddl_info->>'owner', p_orig_schema, p_orig_table, SQLERRM;
        END;
    ELSIF v_owner IS NOT NULL THEN
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
    ELSIF jsonb_typeof(ddl_info->'acl') = 'array'
          AND jsonb_array_length(ddl_info->'acl') > 0
    THEN
        DECLARE
            v_acl_json record;
        BEGIN
            FOR v_acl_json IN
                SELECT a->>'grantee' AS grantee,
                       a->>'privilege' AS privilege,
                       COALESCE((a->>'is_grantable')::boolean, false) AS is_grantable
                FROM jsonb_array_elements(ddl_info->'acl') a
            LOOP
                BEGIN
                    IF v_acl_json.grantee = 'PUBLIC' THEN
                        EXECUTE format('GRANT %s ON %I.%I TO PUBLIC%s',
                            v_acl_json.privilege, p_orig_schema, p_orig_table,
                            CASE WHEN v_acl_json.is_grantable
                                 THEN ' WITH GRANT OPTION' ELSE '' END);
                    ELSE
                        EXECUTE format('GRANT %s ON %I.%I TO %I%s',
                            v_acl_json.privilege, p_orig_schema, p_orig_table,
                            v_acl_json.grantee,
                            CASE WHEN v_acl_json.is_grantable
                                 THEN ' WITH GRANT OPTION' ELSE '' END);
                    END IF;
                EXCEPTION WHEN OTHERS THEN
                    RAISE EXCEPTION 'flashback: GRANT % on %.% to % from schema_def failed: %',
                        v_acl_json.privilege, p_orig_schema, p_orig_table,
                        v_acl_json.grantee, SQLERRM;
                END;
            END LOOP;
        END;
    END IF;

    -- Restore table/column comments captured in schema_def. DROP removes
    -- pg_description rows with the relation, so this is the only source once
    -- the live relation is gone.
    IF jsonb_typeof(ddl_info->'comments') = 'array'
       AND jsonb_array_length(ddl_info->'comments') > 0
    THEN
        DECLARE
            v_comment_json record;
        BEGIN
            FOR v_comment_json IN
                SELECT c->>'target' AS target,
                       c->>'column' AS col,
                       c->>'text' AS txt
                FROM jsonb_array_elements(ddl_info->'comments') c
            LOOP
                BEGIN
                    IF v_comment_json.target = 'column' THEN
                        EXECUTE format('COMMENT ON COLUMN %I.%I.%I IS %L',
                            p_orig_schema, p_orig_table, v_comment_json.col, v_comment_json.txt);
                    ELSE
                        EXECUTE format('COMMENT ON TABLE %I.%I IS %L',
                            p_orig_schema, p_orig_table, v_comment_json.txt);
                    END IF;
                EXCEPTION WHEN OTHERS THEN
                    RAISE EXCEPTION 'flashback: COMMENT on %.% (target %, column %) from schema_def failed: %',
                        p_orig_schema, p_orig_table, v_comment_json.target, v_comment_json.col, SQLERRM;
                END;
            END LOOP;
        END;
    END IF;

    -- ── Incoming FK constraints from other tables ──────────────────
    -- Staging contract: peer-table ALTER during restore is not proven for
    -- lock/RBAC/concurrency. Fail closed rather than mutate other relations.
    IF jsonb_array_length(COALESCE(v_incoming_fks, '[]'::jsonb)) > 0 THEN
        RAISE EXCEPTION
            'pg_flashback: incoming foreign keys are not supported in local_delta staging restore (% refs)',
            jsonb_array_length(v_incoming_fks)
            USING ERRCODE = 'feature_not_supported',
                  HINT = 'Drop or defer incoming FKs before protect, or unprotect peers first.';
    END IF;

    RETURN v_new_oid;
END;
$$;
