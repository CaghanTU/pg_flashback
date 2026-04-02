-- =================================================================
-- Public API: schema def collection, capture triggers, track/untrack,
-- checkpoint, retention, history, DDL capture.
-- =================================================================

CREATE OR REPLACE FUNCTION flashback_collect_schema_def(input_rel_oid oid)
RETURNS jsonb
LANGUAGE sql
AS $$
    SELECT jsonb_build_object(
        'schema', n.nspname,
        'table', c.relname,
        'columns', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', a.attname,
                    'attnum', a.attnum,
                    'type_oid', a.atttypid,
                    'typmod', a.atttypmod,
                    'type', pg_catalog.format_type(a.atttypid, a.atttypmod),
                    'not_null', a.attnotnull,
                    'default_expr', pg_get_expr(d.adbin, d.adrelid)
                )
                ORDER BY a.attnum
            )
            FROM pg_attribute a
            LEFT JOIN pg_attrdef d
                ON d.adrelid = a.attrelid
               AND d.adnum = a.attnum
            WHERE a.attrelid = c.oid
              AND a.attnum > 0
              AND NOT a.attisdropped
        ), '[]'::jsonb),
        'primary_key', COALESCE((
            SELECT jsonb_agg(att.attname ORDER BY k.ord)
            FROM pg_index i
            JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
            JOIN pg_attribute att ON att.attrelid = i.indrelid AND att.attnum = k.attnum
            WHERE i.indrelid = c.oid
              AND i.indisprimary
        ), '[]'::jsonb),
        'constraints', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', con.conname,
                    'type', con.contype,
                    'def', pg_get_constraintdef(con.oid)
                )
                ORDER BY con.conname
            )
            FROM pg_constraint con
            WHERE con.conrelid = c.oid
              AND con.contype IN ('c', 'u', 'f')
        ), '[]'::jsonb),
        'indexes', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', ic.relname,
                    'def', pg_get_indexdef(i.indexrelid)
                )
                ORDER BY ic.relname
            )
            FROM pg_index i
            JOIN pg_class ic ON ic.oid = i.indexrelid
            WHERE i.indrelid = c.oid
              AND NOT i.indisprimary
              AND NOT EXISTS (
                  SELECT 1 FROM pg_constraint con
                  WHERE con.conindid = i.indexrelid
              )
        ), '[]'::jsonb),
        'partition_by', CASE
            WHEN c.relkind = 'p' THEN pg_get_partkeydef(c.oid)
            ELSE NULL
        END,
        'partitions', CASE
            WHEN c.relkind = 'p' THEN COALESCE((
                SELECT jsonb_agg(
                    jsonb_build_object(
                        'name', child.relname,
                        'schema', cn.nspname,
                        'bound', pg_get_expr(child.relpartbound, child.oid)
                    )
                    ORDER BY child.relname
                )
                FROM pg_inherits inh
                JOIN pg_class child ON child.oid = inh.inhrelid
                JOIN pg_namespace cn ON cn.oid = child.relnamespace
                WHERE inh.inhparent = c.oid
            ), '[]'::jsonb)
            ELSE NULL
        END,
        'triggers', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', tg.tgname,
                    'def', pg_get_triggerdef(tg.oid)
                )
                ORDER BY tg.tgname
            )
            FROM pg_trigger tg
            WHERE tg.tgrelid = c.oid
              AND NOT tg.tgisinternal
              AND tg.tgname NOT LIKE 'flashback_capture_%'
        ), '[]'::jsonb),
        'rls_policies', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', pol.polname,
                    'cmd', CASE pol.polcmd
                        WHEN 'r' THEN 'SELECT'
                        WHEN 'a' THEN 'INSERT'
                        WHEN 'w' THEN 'UPDATE'
                        WHEN 'd' THEN 'DELETE'
                        ELSE 'ALL'
                    END,
                    'permissive', (pol.polpermissive),
                    'roles', COALESCE((
                        SELECT jsonb_agg(rolname)
                        FROM pg_roles r2
                        WHERE r2.oid = ANY(pol.polroles)
                    ), '[]'::jsonb),
                    'qual', pg_get_expr(pol.polqual, pol.polrelid),
                    'with_check', pg_get_expr(pol.polwithcheck, pol.polrelid)
                )
                ORDER BY pol.polname
            )
            FROM pg_policy pol
            WHERE pol.polrelid = c.oid
        ), '[]'::jsonb),
        'rls_enabled', c.relrowsecurity
    )
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = input_rel_oid;
$$;

-- Statement-level trigger for INSERT (regular / non-partitioned tables only)
-- Uses REFERENCING NEW TABLE transition table for efficiency.
-- NOT compatible with partitioned tables — use flashback_capture_insert_row_trigger instead.
CREATE OR REPLACE FUNCTION flashback_capture_insert_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_table_name text;
    v_max_size   integer;
    v_skipped    bigint;
BEGIN
    IF COALESCE(current_setting('pg_flashback.enabled', true), 'on') = 'off' THEN
        RETURN NULL;
    END IF;
    IF flashback_is_restore_in_progress(TG_RELID) THEN
        RETURN NULL;
    END IF;

    IF TG_NARGS > 0 THEN
        v_table_name := TG_ARGV[0];
    ELSE
        v_table_name := format('%I.%I', TG_TABLE_SCHEMA, TG_TABLE_NAME);
    END IF;
    v_max_size := COALESCE(pg_size_bytes(current_setting('pg_flashback.max_row_size', true)), 65536);

    INSERT INTO flashback.staging_events
           (event_time, rel_oid, source_xid, event_type, table_name, old_data, new_data)
    SELECT  clock_timestamp(), COALESCE(to_regclass(v_table_name), TG_RELID), txid_current()::bigint,
            'INSERT', v_table_name, NULL, to_jsonb(r.*)
    FROM    _fb_new r
    WHERE   pg_column_size(r.*) <= v_max_size;

    SELECT count(*) INTO v_skipped FROM _fb_new r WHERE pg_column_size(r.*) > v_max_size;
    IF v_skipped > 0 THEN
        RAISE WARNING 'pg_flashback: % rows skipped (exceed max_row_size %) for %', v_skipped, v_max_size, v_table_name;
    END IF;

    RETURN NULL;
END;
$$;

-- Per-row trigger for INSERT (partitioned tables)
-- PostgreSQL does not support REFERENCING NEW TABLE (transition tables) on
-- partitioned tables. This per-row variant is used automatically when
-- flashback_attach_capture_trigger detects a partitioned parent.
CREATE OR REPLACE FUNCTION flashback_capture_insert_row_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_table_name text;
    v_max_size   integer;
    v_rel_oid    oid;
BEGIN
    IF COALESCE(current_setting('pg_flashback.enabled', true), 'on') = 'off' THEN
        RETURN NULL;
    END IF;
    IF flashback_is_restore_in_progress(TG_RELID) THEN
        RETURN NULL;
    END IF;

    IF TG_NARGS > 0 THEN
        v_table_name := TG_ARGV[0];
    ELSE
        v_table_name := format('%I.%I', TG_TABLE_SCHEMA, TG_TABLE_NAME);
    END IF;
    v_max_size := COALESCE(pg_size_bytes(current_setting('pg_flashback.max_row_size', true)), 65536);

    IF pg_column_size(NEW.*) > v_max_size THEN
        RAISE WARNING 'pg_flashback: row too large (% bytes), skipping INSERT capture for %',
            pg_column_size(NEW.*), v_table_name;
        RETURN NULL;
    END IF;

    -- Resolve parent OID (partitioned parent, not the individual partition)
    v_rel_oid := COALESCE(to_regclass(v_table_name), TG_RELID);

    INSERT INTO flashback.staging_events
           (event_time, rel_oid, source_xid, event_type, table_name, old_data, new_data)
    VALUES (clock_timestamp(), v_rel_oid, txid_current()::bigint,
            'INSERT', v_table_name, NULL, to_jsonb(NEW));

    RETURN NULL;
END;
$$;

-- Per-row trigger for DELETE (partitioned tables)
-- PostgreSQL does not support REFERENCING OLD TABLE (transition tables) on
-- partitioned tables. This per-row variant is used automatically when
-- flashback_attach_capture_trigger detects a partitioned parent.
CREATE OR REPLACE FUNCTION flashback_capture_delete_row_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_table_name text;
    v_max_size   integer;
    v_rel_oid    oid;
BEGIN
    IF COALESCE(current_setting('pg_flashback.enabled', true), 'on') = 'off' THEN
        RETURN NULL;
    END IF;
    IF flashback_is_restore_in_progress(TG_RELID) THEN
        RETURN NULL;
    END IF;

    IF TG_NARGS > 0 THEN
        v_table_name := TG_ARGV[0];
    ELSE
        v_table_name := format('%I.%I', TG_TABLE_SCHEMA, TG_TABLE_NAME);
    END IF;
    v_max_size := COALESCE(pg_size_bytes(current_setting('pg_flashback.max_row_size', true)), 65536);

    IF pg_column_size(OLD.*) > v_max_size THEN
        RAISE WARNING 'pg_flashback: row too large (% bytes), skipping DELETE capture for %',
            pg_column_size(OLD.*), v_table_name;
        RETURN NULL;
    END IF;

    -- Resolve parent OID (partitioned parent, not the individual partition)
    v_rel_oid := COALESCE(to_regclass(v_table_name), TG_RELID);

    INSERT INTO flashback.staging_events
           (event_time, rel_oid, source_xid, event_type, table_name, old_data, new_data)
    VALUES (clock_timestamp(), v_rel_oid, txid_current()::bigint,
            'DELETE', v_table_name, to_jsonb(OLD), NULL);

    RETURN NULL;
END;
$$;

-- Per-row trigger for UPDATE (diff-only capture)
-- For tables WITH a primary key: stores only PK columns + changed columns.
-- For tables WITHOUT a primary key: stores full OLD and NEW rows (fallback).
-- Skips capture entirely if no columns actually changed.
CREATE OR REPLACE FUNCTION flashback_capture_update_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_table_name text;
    v_max_size integer;
    v_old_json jsonb;
    v_new_json jsonb;
    v_pk_cols text[];
    v_old_diff jsonb;
    v_new_diff jsonb;
BEGIN
    IF COALESCE(current_setting('pg_flashback.enabled', true), 'on') = 'off' THEN
        RETURN NULL;
    END IF;
    IF flashback_is_restore_in_progress(TG_RELID) THEN
        RETURN NULL;
    END IF;

    IF TG_NARGS > 0 THEN
        v_table_name := TG_ARGV[0];
    ELSE
        v_table_name := format('%I.%I', TG_TABLE_SCHEMA, TG_TABLE_NAME);
    END IF;

    v_max_size := COALESCE(pg_size_bytes(current_setting('pg_flashback.max_row_size', true)), 65536);

    v_old_json := to_jsonb(OLD);
    v_new_json := to_jsonb(NEW);

    -- Skip capture if no columns actually changed (no-op UPDATE)
    IF v_old_json = v_new_json THEN
        RETURN NULL;
    END IF;

    -- Get primary key columns for this table
    SELECT array_agg(a.attname ORDER BY k.ord)
      INTO v_pk_cols
    FROM pg_index i
    JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
    WHERE i.indrelid = TG_RELID AND i.indisprimary;

    IF v_pk_cols IS NOT NULL AND array_length(v_pk_cols, 1) > 0 THEN
        -- Diff-only: PK columns + changed columns only
        SELECT jsonb_object_agg(kv.key, kv.value)
          INTO v_old_diff
        FROM jsonb_each(v_old_json) kv
        WHERE kv.key = ANY(v_pk_cols)
           OR v_old_json->kv.key IS DISTINCT FROM v_new_json->kv.key;

        SELECT jsonb_object_agg(kv.key, kv.value)
          INTO v_new_diff
        FROM jsonb_each(v_new_json) kv
        WHERE kv.key = ANY(v_pk_cols)
           OR v_old_json->kv.key IS DISTINCT FROM v_new_json->kv.key;
    ELSE
        -- No PK: store full rows for reliable matching during restore
        v_old_diff := v_old_json;
        v_new_diff := v_new_json;
    END IF;

    IF pg_column_size(v_old_diff) > v_max_size OR pg_column_size(v_new_diff) > v_max_size THEN
        RAISE WARNING 'pg_flashback: row too large, skipping capture for %', v_table_name;
        RETURN NULL;
    END IF;

    INSERT INTO flashback.staging_events
           (event_time, rel_oid, source_xid, event_type, table_name, old_data, new_data)
    VALUES (clock_timestamp(), COALESCE(to_regclass(v_table_name), TG_RELID), txid_current()::bigint, 'UPDATE',
            v_table_name, v_old_diff, v_new_diff);
    RETURN NULL;
END;
$$;

-- Statement-level trigger for DELETE
CREATE OR REPLACE FUNCTION flashback_capture_delete_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_table_name text;
    v_max_size   integer;
    v_skipped    bigint;
BEGIN
    IF COALESCE(current_setting('pg_flashback.enabled', true), 'on') = 'off' THEN
        RETURN NULL;
    END IF;
    IF flashback_is_restore_in_progress(TG_RELID) THEN
        RETURN NULL;
    END IF;

    IF TG_NARGS > 0 THEN
        v_table_name := TG_ARGV[0];
    ELSE
        v_table_name := format('%I.%I', TG_TABLE_SCHEMA, TG_TABLE_NAME);
    END IF;
    v_max_size := COALESCE(pg_size_bytes(current_setting('pg_flashback.max_row_size', true)), 65536);

    INSERT INTO flashback.staging_events
           (event_time, rel_oid, source_xid, event_type, table_name, old_data, new_data)
    SELECT  clock_timestamp(), COALESCE(to_regclass(v_table_name), TG_RELID), txid_current()::bigint,
            'DELETE', v_table_name, to_jsonb(r.*), NULL
    FROM    _fb_old r
    WHERE   pg_column_size(r.*) <= v_max_size;

    SELECT count(*) INTO v_skipped FROM _fb_old r WHERE pg_column_size(r.*) > v_max_size;
    IF v_skipped > 0 THEN
        RAISE WARNING 'pg_flashback: % rows skipped (exceed max_row_size %) for %', v_skipped, v_max_size, v_table_name;
    END IF;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_attach_capture_trigger(input_schema text, input_table text)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_relkind char;
    v_qualified text := format('%I.%I', input_schema, input_table);
BEGIN
    EXECUTE format('DROP TRIGGER IF EXISTS flashback_capture_row ON %I.%I', input_schema, input_table);
    EXECUTE format('DROP TRIGGER IF EXISTS flashback_capture_ins ON %I.%I', input_schema, input_table);
    EXECUTE format('DROP TRIGGER IF EXISTS flashback_capture_upd ON %I.%I', input_schema, input_table);
    EXECUTE format('DROP TRIGGER IF EXISTS flashback_capture_del ON %I.%I', input_schema, input_table);

    -- Detect partitioned table: relkind = 'p'
    SELECT c.relkind INTO v_relkind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = input_schema AND c.relname = input_table;

    IF v_relkind = 'p' THEN
        -- Partitioned table: PostgreSQL does NOT support REFERENCING NEW/OLD TABLE
        -- (transition tables) on partitioned tables. Use per-row triggers instead.
        -- PostgreSQL automatically propagates FOR EACH ROW triggers to all current
        -- and future partitions.
        EXECUTE format(
            'CREATE TRIGGER flashback_capture_ins AFTER INSERT ON %I.%I FOR EACH ROW EXECUTE FUNCTION flashback_capture_insert_row_trigger(%L)',
            input_schema, input_table, v_qualified
        );
        EXECUTE format(
            'CREATE TRIGGER flashback_capture_upd AFTER UPDATE ON %I.%I FOR EACH ROW EXECUTE FUNCTION flashback_capture_update_trigger(%L)',
            input_schema, input_table, v_qualified
        );
        EXECUTE format(
            'CREATE TRIGGER flashback_capture_del AFTER DELETE ON %I.%I FOR EACH ROW EXECUTE FUNCTION flashback_capture_delete_row_trigger(%L)',
            input_schema, input_table, v_qualified
        );
    ELSE
        -- Regular (non-partitioned) table: use statement-level triggers with
        -- transition tables for efficient bulk-insert / bulk-delete capture.
        EXECUTE format(
            'CREATE TRIGGER flashback_capture_ins AFTER INSERT ON %I.%I REFERENCING NEW TABLE AS _fb_new FOR EACH STATEMENT EXECUTE FUNCTION flashback_capture_insert_trigger(%L)',
            input_schema, input_table, v_qualified
        );
        EXECUTE format(
            'CREATE TRIGGER flashback_capture_upd AFTER UPDATE ON %I.%I FOR EACH ROW EXECUTE FUNCTION flashback_capture_update_trigger(%L)',
            input_schema, input_table, v_qualified
        );
        EXECUTE format(
            'CREATE TRIGGER flashback_capture_del AFTER DELETE ON %I.%I REFERENCING OLD TABLE AS _fb_old FOR EACH STATEMENT EXECUTE FUNCTION flashback_capture_delete_trigger(%L)',
            input_schema, input_table, v_qualified
        );
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_detach_capture_trigger(input_schema text, input_table text)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, flashback, public
AS $$
BEGIN
    EXECUTE format('DROP TRIGGER IF EXISTS flashback_capture_row ON %I.%I', input_schema, input_table);
    EXECUTE format('DROP TRIGGER IF EXISTS flashback_capture_ins ON %I.%I', input_schema, input_table);
    EXECUTE format('DROP TRIGGER IF EXISTS flashback_capture_upd ON %I.%I', input_schema, input_table);
    EXECUTE format('DROP TRIGGER IF EXISTS flashback_capture_del ON %I.%I', input_schema, input_table);
END;
$$;

CREATE OR REPLACE FUNCTION flashback_track(target_table text)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_rel_oid oid;
    v_schema_name text;
    v_table_name text;
    v_snapshot_name text;
    v_tracked_since timestamptz;
BEGIN
    SELECT c.oid, n.nspname, c.relname
      INTO v_rel_oid, v_schema_name, v_table_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = to_regclass(target_table);

    IF v_rel_oid IS NULL THEN
        RAISE EXCEPTION 'flashback_track: table % does not exist', target_table;
    END IF;

    PERFORM flashback_attach_capture_trigger(v_schema_name, v_table_name);

    v_snapshot_name := format('base_snapshot_%s', v_rel_oid::text);

    EXECUTE format('DROP TABLE IF EXISTS flashback.%I', v_snapshot_name);
    EXECUTE format('CREATE TABLE flashback.%I AS TABLE %I.%I', v_snapshot_name, v_schema_name, v_table_name);

    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table,
        schema_version, tracked_since, checkpoint_interval, retention_interval, is_active
    )
    VALUES (
        v_rel_oid, v_schema_name, v_table_name,
        format('flashback.%I', v_snapshot_name),
        1, now(), interval '15 minutes', interval '7 days', true
    )
    ON CONFLICT (rel_oid)
    DO UPDATE SET
        schema_name = EXCLUDED.schema_name,
        table_name = EXCLUDED.table_name,
        base_snapshot_table = EXCLUDED.base_snapshot_table,
        schema_version = 1,
        tracked_since = now(),
        is_active = true;

    SELECT tracked_since INTO v_tracked_since
    FROM flashback.tracked_tables WHERE rel_oid = v_rel_oid;

    DELETE FROM flashback.schema_versions WHERE rel_oid = v_rel_oid;

    INSERT INTO flashback.schema_versions (
        rel_oid, schema_version, applied_at, applied_lsn, columns, primary_key, constraints
    )
    SELECT
        v_rel_oid, 1,
        COALESCE(v_tracked_since, clock_timestamp()),
        pg_current_wal_lsn(),
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
        )
    FROM (
        SELECT COALESCE(flashback_collect_schema_def(v_rel_oid), '{}'::jsonb) AS schema_def
    ) s;

    RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_checkpoint(target_table text)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_rel_oid oid;
    v_schema_name text;
    v_table_name text;
    v_snapshot_id bigint;
    v_snapshot_table_name text;
    v_row_count bigint;
BEGIN
    SELECT tt.rel_oid, tt.schema_name, tt.table_name
      INTO v_rel_oid, v_schema_name, v_table_name
    FROM flashback.tracked_tables tt
    WHERE tt.is_active
      AND (
          tt.rel_oid = to_regclass(target_table)::oid
          OR format('%I.%I', tt.schema_name, tt.table_name) = target_table
          OR (position('.' IN target_table) = 0 AND tt.table_name = target_table)
      )
    ORDER BY
        (tt.rel_oid = to_regclass(target_table)::oid) DESC,
        (format('%I.%I', tt.schema_name, tt.table_name) = target_table) DESC,
        tt.tracked_since DESC
    LIMIT 1;

    IF v_rel_oid IS NULL THEN
        RAISE EXCEPTION 'flashback_checkpoint: table % is not tracked', target_table;
    END IF;

    INSERT INTO flashback.snapshots (
        rel_oid, snapshot_table, snapshot_lsn, schema_def, row_count, captured_at
    )
    VALUES (
        v_rel_oid, '', pg_current_wal_lsn(),
        COALESCE(flashback_collect_schema_def(v_rel_oid), '{}'::jsonb),
        0, clock_timestamp()
    )
    RETURNING snapshot_id INTO v_snapshot_id;

    v_snapshot_table_name := format('snap_%s_%s', v_rel_oid::text, v_snapshot_id::text);

    EXECUTE format('DROP TABLE IF EXISTS flashback.%I', v_snapshot_table_name);
    EXECUTE format(
        'CREATE TABLE flashback.%I AS TABLE %I.%I',
        v_snapshot_table_name, v_schema_name, v_table_name
    );
    EXECUTE format('SELECT count(*) FROM flashback.%I', v_snapshot_table_name)
      INTO v_row_count;

    UPDATE flashback.snapshots
    SET snapshot_table = format('flashback.%I', v_snapshot_table_name),
        snapshot_lsn = pg_current_wal_lsn(),
        schema_def = COALESCE(flashback_collect_schema_def(v_rel_oid), '{}'::jsonb),
        row_count = v_row_count,
        captured_at = clock_timestamp()
    WHERE snapshot_id = v_snapshot_id;

    RETURN v_snapshot_id;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_take_due_checkpoints()
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    rec record;
    v_last_snapshot_at timestamptz;
    v_taken integer := 0;
BEGIN
    FOR rec IN
        SELECT tt.rel_oid, tt.schema_name, tt.table_name, tt.checkpoint_interval
        FROM flashback.tracked_tables tt
        WHERE tt.is_active
    LOOP
        SELECT max(s.captured_at) INTO v_last_snapshot_at
        FROM flashback.snapshots s WHERE s.rel_oid = rec.rel_oid;

        IF v_last_snapshot_at IS NULL
           OR v_last_snapshot_at + rec.checkpoint_interval <= clock_timestamp()
        THEN
            PERFORM flashback_checkpoint(format('%I.%I', rec.schema_name, rec.table_name));
            v_taken := v_taken + 1;
        END IF;
    END LOOP;

    RETURN v_taken;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_apply_retention()
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    rec record;
    snap_rec record;
    v_deleted integer := 0;
    v_rows integer := 0;
    v_part record;
    v_min_cutoff timestamptz;
    v_bound_text text;
    v_bound_upper timestamptz;
BEGIN
    FOR rec IN
        SELECT rel_oid, retention_interval
        FROM flashback.tracked_tables WHERE is_active
    LOOP
        DELETE FROM flashback.delta_log d
        WHERE d.rel_oid = rec.rel_oid
          AND d.event_time < clock_timestamp() - rec.retention_interval;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_deleted := v_deleted + v_rows;

        FOR snap_rec IN
            SELECT snapshot_id, snapshot_table
            FROM flashback.snapshots s
            WHERE s.rel_oid = rec.rel_oid
              AND s.captured_at < clock_timestamp() - rec.retention_interval
        LOOP
            IF snap_rec.snapshot_table IS NOT NULL AND snap_rec.snapshot_table <> '' THEN
                IF snap_rec.snapshot_table ~ '^flashback\\.\"?[a-zA-Z0-9_]+\"?$' THEN
                    EXECUTE format('DROP TABLE IF EXISTS %s', snap_rec.snapshot_table);
                END IF;
            END IF;
            DELETE FROM flashback.snapshots WHERE snapshot_id = snap_rec.snapshot_id;
        END LOOP;
    END LOOP;

    SELECT min(clock_timestamp() - retention_interval)
      INTO v_min_cutoff
    FROM flashback.tracked_tables WHERE is_active;

    IF v_min_cutoff IS NOT NULL AND to_regclass('flashback.delta_log') IS NOT NULL THEN
        FOR v_part IN
            SELECT c.oid, format('%I.%I', n.nspname, c.relname) AS part_name
            FROM pg_inherits i
            JOIN pg_class p ON p.oid = i.inhparent
            JOIN pg_class c ON c.oid = i.inhrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE p.oid = 'flashback.delta_log'::regclass
        LOOP
            SELECT pg_get_expr(c.relpartbound, c.oid) INTO v_bound_text
            FROM pg_class c WHERE c.oid = v_part.oid;

            IF v_bound_text IS NOT NULL THEN
                BEGIN
                    v_bound_upper := substring(v_bound_text from '''([^'']+)''')::timestamptz;
                    IF v_bound_upper < v_min_cutoff THEN
                        EXECUTE format('DROP TABLE IF EXISTS %s', v_part.part_name);
                    END IF;
                EXCEPTION WHEN OTHERS THEN
                    NULL;
                END;
            END IF;
        END LOOP;
    END IF;

    RETURN v_deleted;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_retention_status()
RETURNS TABLE(
    table_name        text,
    retention_interval interval,
    oldest_delta      timestamptz,
    newest_delta      timestamptz,
    delta_count       bigint,
    restorable_window interval,
    retention_warning boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    rec record;
BEGIN
    FOR rec IN
        SELECT tt.rel_oid,
               format('%I.%I', tt.schema_name, tt.table_name) AS tbl,
               tt.retention_interval AS ri
        FROM flashback.tracked_tables tt WHERE tt.is_active
    LOOP
        RETURN QUERY
        SELECT rec.tbl, rec.ri,
               min(d.event_time), max(d.event_time),
               count(*)::bigint,
               (clock_timestamp() - COALESCE(min(d.event_time), clock_timestamp()))::interval,
               COALESCE((clock_timestamp() - min(d.event_time)) > (rec.ri * 0.9), false)
        FROM flashback.delta_log d WHERE d.rel_oid = rec.rel_oid;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_history(target_table text, lookback interval)
RETURNS TABLE(
    event_time timestamptz,
    event_type text,
    row_identity jsonb,
    old_data jsonb,
    new_data jsonb
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_rel_oid oid;
    v_pk_cols text[];
    rec record;
BEGIN
    SELECT tt.rel_oid INTO v_rel_oid
    FROM flashback.tracked_tables tt
    WHERE tt.is_active
      AND (
          tt.rel_oid = to_regclass(target_table)::oid
          OR format('%I.%I', tt.schema_name, tt.table_name) = target_table
          OR (position('.' IN target_table) = 0 AND tt.table_name = target_table)
      )
    ORDER BY
        (tt.rel_oid = to_regclass(target_table)::oid) DESC,
        tt.tracked_since DESC
    LIMIT 1;

    IF v_rel_oid IS NULL THEN
        SELECT d.rel_oid INTO v_rel_oid
        FROM flashback.delta_log d
        WHERE d.committed_at IS NOT NULL
          AND (d.table_name = target_table OR d.table_name = format('public.%s', target_table))
        ORDER BY d.event_id DESC LIMIT 1;
    END IF;

    IF v_rel_oid IS NULL THEN RETURN; END IF;

    SELECT ARRAY(
        SELECT jsonb_array_elements_text(
            COALESCE(flashback_collect_schema_def(v_rel_oid)->'primary_key', '[]'::jsonb)
        )
    ) INTO v_pk_cols;

    FOR rec IN
        SELECT d.event_time, d.event_type, d.old_data, d.new_data
        FROM flashback.delta_log d
        WHERE d.rel_oid = v_rel_oid
          AND d.committed_at IS NOT NULL
          AND d.event_time >= clock_timestamp() - lookback
        ORDER BY d.event_time DESC
    LOOP
        event_time := rec.event_time;
        event_type := rec.event_type;
        old_data := rec.old_data;
        new_data := rec.new_data;

        IF array_length(v_pk_cols, 1) IS NULL THEN
            row_identity := COALESCE(rec.new_data, rec.old_data);
        ELSE
            SELECT COALESCE(jsonb_object_agg(pk, COALESCE(rec.new_data -> pk, rec.old_data -> pk)), '{}'::jsonb)
              INTO row_identity
            FROM unnest(v_pk_cols) AS pk;
        END IF;

        RETURN NEXT;
    END LOOP;

    RETURN;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_untrack(target_table text)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_rel_oid oid;
    v_schema_name text;
    v_table_name text;
    v_base_snapshot text;
    snap_rec record;
BEGIN
    SELECT tt.rel_oid, tt.schema_name, tt.table_name, tt.base_snapshot_table
      INTO v_rel_oid, v_schema_name, v_table_name, v_base_snapshot
    FROM flashback.tracked_tables tt
    WHERE tt.is_active
      AND (
          tt.rel_oid = to_regclass(target_table)::oid
          OR format('%I.%I', tt.schema_name, tt.table_name) = target_table
          OR (position('.' IN target_table) = 0 AND tt.table_name = target_table)
      )
    ORDER BY
        (tt.rel_oid = to_regclass(target_table)::oid) DESC,
        (format('%I.%I', tt.schema_name, tt.table_name) = target_table) DESC,
        tt.tracked_since DESC
    LIMIT 1;

    IF v_rel_oid IS NULL THEN RETURN false; END IF;

    IF to_regclass(format('%I.%I', v_schema_name, v_table_name)) IS NOT NULL THEN
        PERFORM flashback_detach_capture_trigger(v_schema_name, v_table_name);
    END IF;

    IF v_base_snapshot IS NOT NULL AND v_base_snapshot <> '' THEN
        IF v_base_snapshot !~ '^flashback\."?[a-zA-Z0-9_]+"?$' THEN
            RAISE EXCEPTION 'flashback_untrack: invalid snapshot ref: %', v_base_snapshot;
        END IF;
        EXECUTE format('DROP TABLE IF EXISTS %s', v_base_snapshot);
    END IF;

    FOR snap_rec IN
        SELECT snapshot_id, snapshot_table
        FROM flashback.snapshots WHERE rel_oid = v_rel_oid
    LOOP
        IF snap_rec.snapshot_table IS NOT NULL AND snap_rec.snapshot_table <> '' THEN
            IF snap_rec.snapshot_table !~ '^flashback\."?[a-zA-Z0-9_]+"?$' THEN
                RAISE WARNING 'flashback_untrack: skipping invalid snapshot ref: %', snap_rec.snapshot_table;
                CONTINUE;
            END IF;
            EXECUTE format('DROP TABLE IF EXISTS %s', snap_rec.snapshot_table);
        END IF;
        DELETE FROM flashback.snapshots WHERE snapshot_id = snap_rec.snapshot_id;
    END LOOP;

    DELETE FROM flashback.delta_log WHERE rel_oid = v_rel_oid;
    DELETE FROM flashback.staging_events WHERE rel_oid = v_rel_oid;
    DELETE FROM flashback.schema_versions WHERE rel_oid = v_rel_oid;
    DELETE FROM flashback.tracked_tables WHERE rel_oid = v_rel_oid;

    RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_capture_ddl_event(
    event_type text,
    input_schema text,
    input_table text
)
RETURNS void
LANGUAGE plpgsql
SET search_path = public, flashback, pg_catalog
AS $$
DECLARE
    tracked record;
    ddl_info jsonb;
    row_snapshot jsonb;
    new_version bigint;
    ddl_event_time timestamptz;
    ddl_event_lsn pg_lsn;
BEGIN
    IF input_table IS NULL OR input_table = '' THEN RETURN; END IF;

    IF input_schema IS NULL OR input_schema = '' THEN
        SELECT tt.rel_oid, tt.schema_name, tt.table_name, tt.schema_version
          INTO tracked
        FROM flashback.tracked_tables tt
        WHERE tt.table_name = input_table AND tt.is_active
        ORDER BY tt.tracked_since DESC LIMIT 1;
    ELSE
        SELECT tt.rel_oid, tt.schema_name, tt.table_name, tt.schema_version
          INTO tracked
        FROM flashback.tracked_tables tt
        WHERE tt.schema_name = input_schema AND tt.table_name = input_table AND tt.is_active
        LIMIT 1;
    END IF;

    IF tracked.rel_oid IS NULL THEN RETURN; END IF;

    ddl_event_time := clock_timestamp();
    ddl_event_lsn := pg_current_wal_lsn();

    IF upper(event_type) = 'ALTER' THEN
        ddl_info := COALESCE(flashback_collect_schema_def(tracked.rel_oid), '{}'::jsonb);
        new_version := COALESCE(tracked.schema_version, 1) + 1;

        UPDATE flashback.tracked_tables
        SET schema_version = new_version
        WHERE rel_oid = tracked.rel_oid;

        INSERT INTO flashback.schema_versions (
            rel_oid, schema_version, applied_at, applied_lsn,
            columns, primary_key, constraints
        )
        SELECT
            tracked.rel_oid, new_version, ddl_event_time, ddl_event_lsn,
            COALESCE(ddl_info -> 'columns', '[]'::jsonb),
            COALESCE(ddl_info -> 'primary_key', '[]'::jsonb),
            jsonb_build_object(
                'check_unique_fk', COALESCE(ddl_info -> 'constraints', '[]'::jsonb),
                'indexes', COALESCE(ddl_info -> 'indexes', '[]'::jsonb),
                'partition_by', ddl_info -> 'partition_by',
                'partitions', ddl_info -> 'partitions',
                'triggers', COALESCE(ddl_info -> 'triggers', '[]'::jsonb),
                'rls_policies', COALESCE(ddl_info -> 'rls_policies', '[]'::jsonb),
                'rls_enabled', COALESCE((ddl_info -> 'rls_enabled')::boolean, false)
            );
    ELSE
        ddl_info := COALESCE(flashback_collect_schema_def(tracked.rel_oid), '{}'::jsonb);
        new_version := COALESCE(tracked.schema_version, 1);
    END IF;

    DECLARE
        v_row_count bigint;
    BEGIN
        EXECUTE format('SELECT count(*) FROM %I.%I', tracked.schema_name, tracked.table_name)
          INTO v_row_count;
        IF v_row_count > 100000 THEN
            RAISE WARNING 'pg_flashback: table %.% has % rows — skipping inline DDL snapshot (checkpoint data preserved)',
                tracked.schema_name, tracked.table_name, v_row_count;
            row_snapshot := NULL;
        ELSE
            EXECUTE format(
                'SELECT COALESCE(jsonb_agg(to_jsonb(t)), ''[]''::jsonb) FROM %I.%I t',
                tracked.schema_name, tracked.table_name
            ) INTO row_snapshot;
        END IF;
    END;

    INSERT INTO flashback.delta_log (
        event_time, event_type, table_name, rel_oid, source_xid,
        committed_at, schema_version, old_data, new_data, ddl_info
    )
    VALUES (
        ddl_event_time, upper(event_type),
        format('%I.%I', tracked.schema_name, tracked.table_name),
        tracked.rel_oid, txid_current()::bigint, clock_timestamp(),
        new_version, row_snapshot, NULL, ddl_info
    );
END;
$$;
