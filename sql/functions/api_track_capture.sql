-- =================================================================
-- Public API: schema def collection, capture triggers, track/untrack,
-- checkpoint, retention, history, DDL capture.
-- =================================================================

-- Returns the effective capture mode: 'wal' or 'trigger'.
-- 'auto' resolves based on wal_level.
CREATE OR REPLACE FUNCTION flashback_effective_capture_mode()
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    v_mode text;
    v_wal_level text;
BEGIN
    v_mode := COALESCE(current_setting('pg_flashback.capture_mode', true), 'auto');
    IF v_mode = 'wal' THEN RETURN 'wal'; END IF;
    IF v_mode = 'trigger' THEN RETURN 'trigger'; END IF;
    -- auto: detect wal_level
    v_wal_level := current_setting('wal_level');
    IF v_wal_level = 'logical' THEN RETURN 'wal'; END IF;
    RETURN 'trigger';
END;
$$;

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
                    'default_expr', pg_get_expr(d.adbin, d.adrelid),
                    'generated', a.attgenerated
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

    -- Detect partitioned table (parent 'p') OR leaf partition ('r' with partition parent).
    -- Transition tables (REFERENCING NEW/OLD TABLE) are not supported on either.
    SELECT c.relkind INTO v_relkind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = input_schema AND c.relname = input_table;

    -- Treat a leaf partition the same as a partitioned parent: use FOR EACH ROW.
    IF v_relkind = 'r' THEN
        SELECT relkind INTO v_relkind
        FROM pg_class
        WHERE oid = (
            SELECT i.inhparent FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = input_schema AND c.relname = input_table
            LIMIT 1
        );
        -- if parent is 'p' (partitioned), use 'p' path; otherwise revert to 'r'
        IF v_relkind IS DISTINCT FROM 'p' THEN
            v_relkind := 'r';
        END IF;
    END IF;

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
    v_replica_identity_was "char" := 'd';
    v_replica_identity_index text := NULL;
BEGIN
    SELECT c.oid, n.nspname, c.relname
      INTO v_rel_oid, v_schema_name, v_table_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = to_regclass(target_table);

    IF v_rel_oid IS NULL THEN
        RAISE EXCEPTION 'flashback_track: table % does not exist', target_table;
    END IF;

    -- In WAL mode, ensure replication slot exists.
    -- pg_create_logical_replication_slot requires a write-free transaction,
    -- so we wrap it in a sub-block with EXCEPTION handler. If it fails
    -- (e.g. inside a test harness or an already-dirty transaction), the
    -- background worker will pick it up on its next cycle.
    IF flashback_effective_capture_mode() = 'wal' THEN
        -- Capture the current replica identity BEFORE we change it so that
        -- flashback_untrack() can restore the table to its original setting.
        SELECT c.relreplident INTO v_replica_identity_was
        FROM pg_class c WHERE c.oid = v_rel_oid;

        -- If the table uses REPLICA IDENTITY USING INDEX, remember which index
        -- so untrack can restore it exactly.
        IF v_replica_identity_was = 'i' THEN
            SELECT ic.relname INTO v_replica_identity_index
            FROM pg_index i
            JOIN pg_class ic ON ic.oid = i.indexrelid
            WHERE i.indrelid = v_rel_oid
              AND i.indisreplident;
        END IF;

        IF NOT EXISTS (
            SELECT 1 FROM pg_replication_slots
            WHERE slot_name = COALESCE(NULLIF(current_setting('pg_flashback.slot_name', true), ''), 'pg_flashback_slot')
        ) THEN
            BEGIN
                PERFORM pg_create_logical_replication_slot(
                    COALESCE(NULLIF(current_setting('pg_flashback.slot_name', true), ''), 'pg_flashback_slot'),
                    'pg_flashback'
                );
                RAISE NOTICE 'pg_flashback: created logical replication slot %',
                    COALESCE(NULLIF(current_setting('pg_flashback.slot_name', true), ''), 'pg_flashback_slot');
            EXCEPTION WHEN OTHERS THEN
                RAISE WARNING 'pg_flashback: could not create replication slot in this transaction (%), background worker will retry', SQLERRM;
            END;
        END IF;
        -- WAL mode: enable REPLICA IDENTITY FULL so old_data is available in UPDATE events
        EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY FULL', v_schema_name, v_table_name);
    ELSE
        PERFORM flashback_attach_capture_trigger(v_schema_name, v_table_name);

        -- Warn if track_commit_timestamp is off. In trigger mode, event_time is the
        -- trigger's clock_timestamp() at statement execution, NOT the transaction
        -- commit time. A long-running transaction can therefore appear in the PITR
        -- window before it actually committed. Enable track_commit_timestamp = on
        -- in postgresql.conf for commit-time-correct PITR in trigger mode.
        IF NOT EXISTS (
            SELECT 1 FROM pg_settings
            WHERE name = 'track_commit_timestamp' AND setting = 'on'
        ) THEN
            RAISE NOTICE 'pg_flashback (%): track_commit_timestamp is off. In trigger mode, event_time is statement-level clock_timestamp(), not transaction commit time. Long-running transactions may appear in the PITR window before they committed. Set track_commit_timestamp = on for commit-time-correct PITR.',
                target_table;
        END IF;
    END IF;

    v_snapshot_name := format('base_snapshot_%s', v_rel_oid::text);

    -- Clean up any stale checkpoint snapshots for this OID (handles OID recycling)
    DECLARE
        stale_snap record;
        old_oid    oid;
    BEGIN
        -- Handle DROP+recreate without flashback_untrack: table has same name but new OID.
        -- Remove the old tracked_tables row (and its data) so the INSERT below succeeds.
        SELECT rel_oid INTO old_oid
        FROM flashback.tracked_tables
        WHERE schema_name = v_schema_name AND table_name = v_table_name
          AND rel_oid <> v_rel_oid
        LIMIT 1;

        IF old_oid IS NOT NULL THEN
            -- Drop checkpoint snapshot tables for the old OID
            FOR stale_snap IN
                SELECT snapshot_table FROM flashback.snapshots WHERE rel_oid = old_oid
            LOOP
                IF stale_snap.snapshot_table IS NOT NULL AND stale_snap.snapshot_table <> '' THEN
                    EXECUTE format('DROP TABLE IF EXISTS %s', stale_snap.snapshot_table);
                END IF;
            END LOOP;
            DECLARE old_snap_name text := format('base_snapshot_%s', old_oid::text);
            BEGIN
                EXECUTE format('DROP TABLE IF EXISTS flashback.%I', old_snap_name);
            END;
            DELETE FROM flashback.snapshots         WHERE rel_oid = old_oid;
            DELETE FROM flashback.delta_log         WHERE rel_oid = old_oid;
            DELETE FROM flashback.staging_events    WHERE rel_oid = old_oid;
            DELETE FROM flashback.schema_versions   WHERE rel_oid = old_oid;
            DELETE FROM flashback.tracked_tables    WHERE rel_oid = old_oid;
            RAISE NOTICE 'pg_flashback (%): stale tracking entry for old OID % removed (table was dropped+recreated without flashback_untrack)',
                target_table, old_oid;
        END IF;

        FOR stale_snap IN
            SELECT snapshot_table FROM flashback.snapshots WHERE rel_oid = v_rel_oid
        LOOP
            IF stale_snap.snapshot_table IS NOT NULL AND stale_snap.snapshot_table <> '' THEN
                EXECUTE format('DROP TABLE IF EXISTS %s', stale_snap.snapshot_table);
            END IF;
        END LOOP;
        DELETE FROM flashback.snapshots WHERE rel_oid = v_rel_oid;
        DELETE FROM flashback.delta_log WHERE rel_oid = v_rel_oid;
        DELETE FROM flashback.staging_events WHERE rel_oid = v_rel_oid;
    END;

    EXECUTE format('DROP TABLE IF EXISTS flashback.%I', v_snapshot_name);
    EXECUTE format('CREATE TABLE flashback.%I AS TABLE %I.%I', v_snapshot_name, v_schema_name, v_table_name);

    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table,
        schema_version, tracked_since, checkpoint_interval, retention_interval, is_active,
        replica_identity_was, replica_identity_index
    )
    VALUES (
        v_rel_oid, v_schema_name, v_table_name,
        format('flashback.%I', v_snapshot_name),
        1, now(), interval '15 minutes', interval '7 days', true,
        v_replica_identity_was, v_replica_identity_index
    )
    ON CONFLICT (rel_oid)
    DO UPDATE SET
        schema_name = EXCLUDED.schema_name,
        table_name = EXCLUDED.table_name,
        base_snapshot_table = EXCLUDED.base_snapshot_table,
        schema_version = 1,
        tracked_since = now(),
        is_active = true,
        replica_identity_was = EXCLUDED.replica_identity_was,
        replica_identity_index = EXCLUDED.replica_identity_index;

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
        -- Guard: relation may have been dropped without flashback_untrack().
        -- Auto-deactivate stale entries to prevent worker crash loops.
        IF NOT EXISTS (SELECT 1 FROM pg_class WHERE oid = rec.rel_oid) THEN
            UPDATE flashback.tracked_tables
               SET is_active = false
             WHERE rel_oid = rec.rel_oid;
            RAISE WARNING 'pg_flashback: relation with OID % (%.%) no longer exists. Deactivating tracking entry. Run flashback_untrack() to clean up.',
                rec.rel_oid, rec.schema_name, rec.table_name;
            CONTINUE;
        END IF;

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

-- Manually flush staging_events -> delta_log.
-- Normally done by the background worker. Call this if the worker is not
-- running (e.g. in testing environments or after worker downtime) to make
-- trigger-captured events visible to flashback_restore/flashback_query.
-- Returns the total number of events promoted.
CREATE OR REPLACE FUNCTION flashback_flush_staging(batch_size integer DEFAULT 1000)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_total   integer := 0;
    v_moved   integer;
BEGIN
    LOOP
        WITH moved AS (
            DELETE FROM flashback.staging_events
            WHERE staging_id IN (
                SELECT staging_id
                FROM flashback.staging_events
                ORDER BY staging_id
                LIMIT batch_size
            )
            RETURNING *
        )
        INSERT INTO flashback.delta_log (
            event_time, event_type, table_name, rel_oid, source_xid,
            committed_at, schema_version, old_data, new_data
        )
        SELECT
            COALESCE(
                CASE WHEN EXISTS (
                    SELECT 1 FROM pg_settings
                    WHERE name = 'track_commit_timestamp' AND setting = 'on'
                ) THEN pg_xact_commit_timestamp(m.source_xid::text::xid) END,
                m.event_time
            ),
            m.event_type, m.table_name, m.rel_oid, m.source_xid,
            COALESCE(
                CASE WHEN EXISTS (
                    SELECT 1 FROM pg_settings
                    WHERE name = 'track_commit_timestamp' AND setting = 'on'
                ) THEN pg_xact_commit_timestamp(m.source_xid::text::xid) END,
                clock_timestamp()
            ),
            COALESCE((
                SELECT sv.schema_version
                FROM flashback.schema_versions sv
                WHERE sv.rel_oid = m.rel_oid
                  AND sv.applied_at <= m.event_time
                ORDER BY sv.schema_version DESC
                LIMIT 1
            ), 1),
            m.old_data, m.new_data
        FROM moved m
        WHERE EXISTS (
            SELECT 1 FROM flashback.tracked_tables tt
            WHERE tt.rel_oid = m.rel_oid
              AND tt.is_active
              AND m.event_time >= tt.tracked_since
        );

        GET DIAGNOSTICS v_moved = ROW_COUNT;
        v_total := v_total + v_moved;
        EXIT WHEN v_moved < batch_size;
    END LOOP;

    RETURN v_total;
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
          AND d.committed_at < clock_timestamp() - rec.retention_interval;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_deleted := v_deleted + v_rows;

        -- Record the retention cutoff so flashback_restore can detect expired windows.
        -- Only advance the cutoff — never go backward.
        IF v_rows > 0 THEN
            UPDATE flashback.tracked_tables
               SET retention_cutoff = GREATEST(
                   retention_cutoff,
                   clock_timestamp() - rec.retention_interval
               )
             WHERE rel_oid = rec.rel_oid;
        END IF;

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

    -- Only detach triggers in trigger mode (WAL mode has no triggers to detach)
    IF flashback_effective_capture_mode() = 'trigger' THEN
        IF to_regclass(format('%I.%I', v_schema_name, v_table_name)) IS NOT NULL THEN
            PERFORM flashback_detach_capture_trigger(v_schema_name, v_table_name);
        END IF;
    ELSE
        -- WAL mode: restore the table's original REPLICA IDENTITY.
        -- flashback_track() forced it to FULL; leaving it there permanently
        -- causes write amplification and changes logical decoding behaviour
        -- for the application after the table is untracked.
        IF to_regclass(format('%I.%I', v_schema_name, v_table_name)) IS NOT NULL THEN
            DECLARE
                v_original_ri    "char";
                v_original_ri_idx text;
                v_ri_clause      text;
            BEGIN
                SELECT tt.replica_identity_was, tt.replica_identity_index
                  INTO v_original_ri, v_original_ri_idx
                FROM flashback.tracked_tables tt WHERE tt.rel_oid = v_rel_oid;

                v_ri_clause := CASE COALESCE(v_original_ri, 'd')
                    WHEN 'f' THEN 'FULL'
                    WHEN 'n' THEN 'NOTHING'
                    WHEN 'i' THEN
                        CASE WHEN v_original_ri_idx IS NOT NULL
                             THEN 'USING INDEX ' || quote_ident(v_original_ri_idx)
                             ELSE 'DEFAULT'   -- index name unknown; fall back
                        END
                    ELSE 'DEFAULT'
                END;
                EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY %s',
                    v_schema_name, v_table_name, v_ri_clause);
            END;
        END IF;
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
    v_actual_schema text;
    v_actual_table  text;
BEGIN
    IF input_table IS NULL OR input_table = '' THEN RETURN; END IF;

    -- After RENAME TABLE the hook fires with the NEW name.
    -- tracked_tables still has the OLD name but same OID.
    -- Try new name first; fall back to OID-based lookup.
    IF input_schema IS NULL OR input_schema = '' THEN
        SELECT tt.rel_oid, tt.schema_name, tt.table_name, tt.schema_version
          INTO tracked
        FROM flashback.tracked_tables tt
        WHERE tt.table_name = input_table
        ORDER BY tt.is_active DESC, tt.tracked_since DESC LIMIT 1;
    ELSE
        SELECT tt.rel_oid, tt.schema_name, tt.table_name, tt.schema_version
          INTO tracked
        FROM flashback.tracked_tables tt
        WHERE tt.schema_name = input_schema AND tt.table_name = input_table
        LIMIT 1;
    END IF;

    -- If not found by name, try by OID (handles RENAME TABLE: new name passed,
    -- tracked_tables still has old name, but OID is stable).
    IF tracked.rel_oid IS NULL THEN
        DECLARE v_oid oid;
        BEGIN
            IF input_schema IS NOT NULL AND input_schema <> '' THEN
                v_oid := to_regclass(format('%I.%I', input_schema, input_table));
            ELSE
                v_oid := to_regclass(input_table);
            END IF;
            IF v_oid IS NOT NULL THEN
                SELECT tt.rel_oid, tt.schema_name, tt.table_name, tt.schema_version
                  INTO tracked
                FROM flashback.tracked_tables tt
                WHERE tt.rel_oid = v_oid
                ORDER BY tt.is_active DESC LIMIT 1;
            END IF;
        END;
    END IF;

    IF tracked.rel_oid IS NULL THEN RETURN; END IF;

    -- Resolve current (post-DDL) actual name from catalog
    SELECT n.nspname, c.relname
      INTO v_actual_schema, v_actual_table
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = tracked.rel_oid;

    -- RENAME TABLE / SET SCHEMA: update tracked_tables with the new name
    IF v_actual_schema IS NOT NULL AND v_actual_table IS NOT NULL
       AND (v_actual_schema <> tracked.schema_name OR v_actual_table <> tracked.table_name)
    THEN
        UPDATE flashback.tracked_tables
           SET schema_name = v_actual_schema,
               table_name  = v_actual_table
         WHERE rel_oid = tracked.rel_oid;

        RAISE NOTICE 'pg_flashback: table renamed/moved from %.% to %.% — tracking updated',
            tracked.schema_name, tracked.table_name, v_actual_schema, v_actual_table;

        -- Recreate triggers with updated table-name argument
        PERFORM flashback_detach_capture_trigger(v_actual_schema, v_actual_table);
        PERFORM flashback_attach_capture_trigger(v_actual_schema, v_actual_table);

        -- Use new name for the rest of this function
        tracked.schema_name := v_actual_schema;
        tracked.table_name  := v_actual_table;
    END IF;

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

    -- In WAL mode: emit DDL event as a WAL message (same pipeline as DML).
    -- In trigger mode: direct delta_log INSERT (legacy path).
    IF flashback_effective_capture_mode() = 'wal' THEN
        PERFORM pg_logical_emit_message(
            true,   -- transactional: tied to current transaction
            'pg_flashback',
            jsonb_build_object(
                'op', upper(event_type),
                'schema', tracked.schema_name,
                'table', tracked.table_name,
                'oid', tracked.rel_oid,
                'xid', txid_current(),
                'old', row_snapshot,
                'ddl_info', ddl_info,
                'schema_version', new_version
            )::text
        );
    ELSE
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
    END IF;
END;
$$;

-- ----------------------------------------------------------------
-- flashback_ensure_delta_partition
-- ----------------------------------------------------------------
-- Called by the background worker each cycle (via run_ensure_partitions).
-- Creates monthly range partitions for delta_log if it is a partitioned table.
-- Idempotent — safe to call repeatedly.
-- Creates the current month's partition and next month's partition
-- (pre-created 7 days before month-end to prevent data loss at rollover).
-- ----------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_ensure_delta_partition(for_date date DEFAULT CURRENT_DATE)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_is_partitioned boolean;
    v_month_start    timestamptz;
    v_month_end      timestamptz;
    v_next_start     timestamptz;
    v_next_end       timestamptz;
    v_part_name      text;
    v_next_part_name text;
BEGIN
    -- Only act if delta_log is a partitioned table
    SELECT c.relkind = 'p'
      INTO v_is_partitioned
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'flashback' AND c.relname = 'delta_log';

    IF NOT FOUND OR NOT v_is_partitioned THEN
        RETURN;
    END IF;

    -- Current month boundaries
    v_month_start := date_trunc('month', for_date::timestamptz);
    v_month_end   := date_trunc('month', for_date::timestamptz) + interval '1 month';
    v_part_name   := 'delta_log_' || to_char(for_date, 'YYYY_MM');

    PERFORM flashback__create_range_partition(v_part_name, v_month_start, v_month_end);

    -- Pre-create next month's partition when within the last 7 days of the month
    IF for_date >= (v_month_end::date - 7) THEN
        v_next_start     := v_month_end;
        v_next_end       := v_month_end + interval '1 month';
        v_next_part_name := 'delta_log_' || to_char(v_next_start, 'YYYY_MM');
        PERFORM flashback__create_range_partition(v_next_part_name, v_next_start, v_next_end);
    END IF;
END;
$$;

-- ----------------------------------------------------------------
-- flashback__create_range_partition (internal helper)
-- ----------------------------------------------------------------
-- Creates a monthly delta_log partition.
-- If the default partition has rows in this range, migrates them first.
-- ----------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback__create_range_partition(
    p_part_name  text,
    p_range_from timestamptz,
    p_range_to   timestamptz
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_default_oid oid;
    v_tmp_table   text;
BEGIN
    -- Already exists: nothing to do
    IF to_regclass(format('flashback.%I', p_part_name)) IS NOT NULL THEN
        RETURN;
    END IF;

    -- Check if default partition has rows in this range (would block CREATE PARTITION)
    v_default_oid := to_regclass('flashback.delta_log_default');
    IF v_default_oid IS NOT NULL THEN
        v_tmp_table := '_fb_part_mig_' || p_part_name;

        -- Move rows out of default partition into a temp table
        EXECUTE format(
            'CREATE TEMP TABLE %I ON COMMIT PRESERVE ROWS AS
             WITH migrated AS (
                 DELETE FROM flashback.delta_log_default
                 WHERE committed_at >= %L AND committed_at < %L
                 RETURNING *
             )
             SELECT * FROM migrated',
            v_tmp_table, p_range_from, p_range_to
        );
    END IF;

    -- Now create the named partition (default partition is clear)
    EXECUTE format(
        'CREATE TABLE flashback.%I PARTITION OF flashback.delta_log
         FOR VALUES FROM (%L) TO (%L)',
        p_part_name, p_range_from, p_range_to
    );

    -- Re-insert migrated rows into the new named partition
    IF v_default_oid IS NOT NULL THEN
        EXECUTE format(
            'INSERT INTO flashback.%I SELECT * FROM %I',
            p_part_name, v_tmp_table
        );
        EXECUTE format('DROP TABLE IF EXISTS %I', v_tmp_table);
    END IF;
EXCEPTION WHEN OTHERS THEN
    -- Clean up temp table if it was created
    IF v_tmp_table IS NOT NULL THEN
        EXECUTE format('DROP TABLE IF EXISTS %I', v_tmp_table);
    END IF;
    RAISE WARNING 'flashback: could not create partition %: %', p_part_name, SQLERRM;
END;
$$;
