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

-- Effective replication slot name for THIS database.
-- Logical replication slots are database-specific and slot names are
-- cluster-wide unique, so the default derives from the database name.
-- pg_flashback.slot_name overrides it (single-database installs only).
-- Slot names may contain only lower-case letters, digits and underscores.
CREATE OR REPLACE FUNCTION flashback_effective_slot_name()
RETURNS text
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE(
        NULLIF(current_setting('pg_flashback.slot_name', true), ''),
        left('pg_flashback_' ||
             lower(regexp_replace(current_database(), '[^a-zA-Z0-9_]', '_', 'g')),
             63)
    );
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
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_table_name text;
    v_max_size   integer;
    v_skipped    bigint;
BEGIN
    IF flashback_is_restore_in_progress(TG_RELID) THEN
        RETURN NULL;
    END IF;
    IF NOT flashback_capture_configuration_guard(TG_RELID) THEN
        RETURN NULL;
    END IF;
    IF COALESCE(current_setting('pg_flashback.enabled', true), 'on') = 'off' THEN
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
    SELECT  clock_timestamp(), COALESCE(to_regclass(v_table_name), TG_RELID),
            (txid_current() % 4294967296)::bigint,
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
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_table_name text;
    v_max_size   integer;
    v_rel_oid    oid;
BEGIN
    IF flashback_is_restore_in_progress(TG_RELID) THEN
        RETURN NULL;
    END IF;
    IF NOT flashback_capture_configuration_guard(TG_RELID) THEN
        RETURN NULL;
    END IF;
    IF COALESCE(current_setting('pg_flashback.enabled', true), 'on') = 'off' THEN
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
    VALUES (clock_timestamp(), v_rel_oid,
            (txid_current() % 4294967296)::bigint,
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
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_table_name text;
    v_max_size   integer;
    v_rel_oid    oid;
BEGIN
    IF flashback_is_restore_in_progress(TG_RELID) THEN
        RETURN NULL;
    END IF;
    IF NOT flashback_capture_configuration_guard(TG_RELID) THEN
        RETURN NULL;
    END IF;
    IF COALESCE(current_setting('pg_flashback.enabled', true), 'on') = 'off' THEN
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
    VALUES (clock_timestamp(), v_rel_oid,
            (txid_current() % 4294967296)::bigint,
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
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
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
    IF flashback_is_restore_in_progress(TG_RELID) THEN
        RETURN NULL;
    END IF;
    IF NOT flashback_capture_configuration_guard(TG_RELID) THEN
        RETURN NULL;
    END IF;
    IF COALESCE(current_setting('pg_flashback.enabled', true), 'on') = 'off' THEN
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
    VALUES (clock_timestamp(), COALESCE(to_regclass(v_table_name), TG_RELID),
            (txid_current() % 4294967296)::bigint, 'UPDATE',
            v_table_name, v_old_diff, v_new_diff);
    RETURN NULL;
END;
$$;

-- Statement-level trigger for DELETE
CREATE OR REPLACE FUNCTION flashback_capture_delete_trigger()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_table_name text;
    v_max_size   integer;
    v_skipped    bigint;
BEGIN
    IF flashback_is_restore_in_progress(TG_RELID) THEN
        RETURN NULL;
    END IF;
    IF NOT flashback_capture_configuration_guard(TG_RELID) THEN
        RETURN NULL;
    END IF;
    IF COALESCE(current_setting('pg_flashback.enabled', true), 'on') = 'off' THEN
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
    SELECT  clock_timestamp(), COALESCE(to_regclass(v_table_name), TG_RELID),
            (txid_current() % 4294967296)::bigint,
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
    v_requested_mode text;
    v_stream_id bigint;
    v_tracking_id bigint;
    v_bound_tracking_id bigint;
    v_snapshot_id bigint;
    v_generation_id bigint;
    v_boundary_xid bigint;
    v_provisional_lsn pg_lsn;
    v_schema_def jsonb;
    v_row_count bigint;
BEGIN
    v_requested_mode := COALESCE(current_setting('pg_flashback.capture_mode', true), 'auto');

    -- The release-qualified local profile is WAL-only. Explicit trigger mode
    -- remains available solely as the named legacy/experimental path used by
    -- the legacy timestamp test matrix; auto never silently downgrades.
    IF flashback_effective_capture_mode() <> 'wal' THEN
        IF v_requested_mode = 'trigger' THEN
            RAISE WARNING 'pg_flashback: explicit trigger capture is legacy/experimental and creates no correctness-qualified coverage generation';
        ELSE
            RAISE EXCEPTION 'pg_flashback: local_delta requires WAL capture; auto mode will not fall back to trigger capture'
                USING HINT = 'Set wal_level=logical in postgresql.conf, restart PostgreSQL, and use pg_flashback.capture_mode=wal or auto.';
        END IF;
    ELSE
        IF txid_current_if_assigned() IS NOT NULL THEN
            RAISE EXCEPTION 'pg_flashback: flashback_track() must run before any write in a dedicated transaction'
                USING HINT = 'COMMIT or ROLLBACK, then call flashback_track() as the first write in a new READ COMMITTED transaction.';
        END IF;
        IF current_setting('transaction_isolation') <> 'read committed' THEN
            RAISE EXCEPTION 'pg_flashback: flashback_track() requires READ COMMITTED isolation for a fresh post-lock snapshot';
        END IF;
    END IF;

    SELECT c.oid, n.nspname, c.relname
      INTO v_rel_oid, v_schema_name, v_table_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = to_regclass(target_table);

    IF v_rel_oid IS NULL THEN
        RAISE EXCEPTION 'flashback_track: table % does not exist', target_table;
    END IF;

    IF EXISTS (
        SELECT 1 FROM flashback.tracked_tables
        WHERE rel_oid = v_rel_oid
          AND is_active
          AND recovery_profile = 'backup'
    ) THEN
        RAISE EXCEPTION 'flashback_track: table is already tracked with the backup profile; untrack it first';
    END IF;

    -- The background worker only serves the databases listed in
    -- pg_flashback.target_databases / target_database. If this database is
    -- not covered, captured events are never flushed (trigger mode) nor
    -- consumed from the slot (WAL mode) — capture silently does nothing.
    DECLARE
        v_db_list text;
    BEGIN
        v_db_list := COALESCE(
            NULLIF(current_setting('pg_flashback.target_databases', true), ''),
            NULLIF(current_setting('pg_flashback.target_database', true), ''),
            'postgres'
        );
        IF NOT EXISTS (
            SELECT 1 FROM unnest(string_to_array(v_db_list, ',')) AS d
            WHERE trim(d) = current_database()
        ) THEN
            IF flashback_effective_capture_mode() = 'wal' THEN
                RAISE EXCEPTION 'pg_flashback: database % is not covered by a background worker (pg_flashback.target_databases = %)',
                    current_database(), v_db_list
                    USING HINT = 'Add this database to pg_flashback.target_databases and restart PostgreSQL before tracking.';
            ELSE
                RAISE WARNING 'pg_flashback: database % is NOT covered by any background worker (pg_flashback.target_databases = %). Captured events will not be processed until this database is added and PostgreSQL is restarted.',
                    current_database(), v_db_list;
            END IF;
        END IF;
    END;

    -- In WAL mode, ensure the replication slot exists. The slot is created
    -- HERE and only here — the background worker merely checks for it — so
    -- a creation failure must abort tracking (fail-closed): returning
    -- success without a slot would mean silently capturing nothing.
    -- pg_create_logical_replication_slot requires a transaction that has
    -- not performed writes yet.
    IF flashback_effective_capture_mode() = 'wal' THEN
        -- Lock order for every qualified lifecycle operation is database
        -- stream -> canonical pre-identity key -> stable tracking ID ->
        -- relation.  Take the database key before slot creation so two first
        -- trackers cannot race while creating the same per-database slot.
        PERFORM pg_advisory_xact_lock(
            358945::integer,
            (SELECT oid::integer
             FROM pg_database
             WHERE datname = current_database())
        );

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

        -- Logical slots are database-specific: a slot with our name that
        -- belongs to ANOTHER database cannot decode this database's changes,
        -- so the existence check must be scoped to current_database().
        IF NOT EXISTS (
            SELECT 1 FROM pg_replication_slots
            WHERE slot_name = flashback_effective_slot_name()
              AND database = current_database()
        ) THEN
            IF EXISTS (
                SELECT 1 FROM pg_replication_slots
                WHERE slot_name = flashback_effective_slot_name()
            ) THEN
                RAISE EXCEPTION 'pg_flashback: replication slot % already exists but belongs to another database. WAL capture cannot work for %. Set pg_flashback.slot_name to a database-unique name.',
                    flashback_effective_slot_name(), current_database();
            END IF;
            BEGIN
                PERFORM pg_create_logical_replication_slot(
                    flashback_effective_slot_name(),
                    'pg_flashback'
                );
                RAISE NOTICE 'pg_flashback: created logical replication slot %',
                    flashback_effective_slot_name();
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'pg_flashback: could not create replication slot % (%). Without a slot, WAL capture would silently miss every change, so tracking is aborted.',
                    flashback_effective_slot_name(), SQLERRM
                    USING HINT = format(
                        'Run flashback_track in a fresh transaction with no prior writes, or create the slot manually first: SELECT pg_create_logical_replication_slot(%L, %L);',
                        flashback_effective_slot_name(), 'pg_flashback');
            END;
        END IF;

        v_stream_id := flashback_ensure_active_wal_stream();
        IF v_stream_id IS NULL THEN
            RAISE EXCEPTION 'pg_flashback: WAL stream could not be activated for slot %',
                flashback_effective_slot_name();
        END IF;

        -- Before a stable lifecycle ID exists, serialize by database plus the
        -- canonical table identity.  Keep this lock until transaction end,
        -- allocate exactly one ID, then acquire the stable-ID lock without an
        -- unlocked handoff.
        PERFORM pg_advisory_xact_lock(
            358943::integer,
            hashtext(format(
                '%s:%s.%s',
                (SELECT oid FROM pg_database
                 WHERE datname = current_database()),
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
            RAISE EXCEPTION 'flashback_track: %.% already has a tracking lifecycle; untrack it before creating a new generation',
                v_schema_name, v_table_name;
        END IF;

        v_tracking_id := nextval('flashback.tracking_id_seq');
        PERFORM pg_advisory_xact_lock(
            358944::integer,
            hashint8(v_tracking_id)
        );

        -- From this point until transaction commit no application DML may
        -- cross the exact base boundary. ALTER below takes a stronger lock,
        -- but acquiring the declared write lock first makes the ordering
        -- contract explicit and avoids a lock-upgrade window.
        PERFORM flashback_admit_local_capacity(v_rel_oid, 'track');
        PERFORM flashback_apply_local_boundary_lock_timeout();
        BEGIN
            EXECUTE format('LOCK TABLE %I.%I IN SHARE ROW EXCLUSIVE MODE', v_schema_name, v_table_name);
        EXCEPTION WHEN lock_not_available THEN
            RAISE EXCEPTION 'pg_flashback: local track lock wait exceeded local_boundary_write_stall_ms'
                USING ERRCODE = 'lock_not_available',
                      HINT = 'Retry when the table is idle, raise the write-stall budget, or use the backup profile.';
        END;

        IF to_regclass(format('%I.%I', v_schema_name, v_table_name))::oid
               IS DISTINCT FROM v_rel_oid
        THEN
            RAISE EXCEPTION 'pg_flashback: table identity changed while first-track lock was acquired'
                USING HINT = 'Retry flashback_track() against the table''s current canonical name.';
        END IF;

        -- Revalidate capacity under the relation lock immediately before CTAS.
        PERFORM flashback_admit_local_capacity(v_rel_oid, 'track');

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
                    IF stale_snap.snapshot_table !~ '^flashback\."?[a-zA-Z0-9_]+"?$' THEN
                        RAISE EXCEPTION 'flashback_track: invalid stale snapshot ref: %',
                            stale_snap.snapshot_table;
                    END IF;
                    PERFORM public.flashback_drop_payload_table(
                        to_regclass(stale_snap.snapshot_table)
                    );
                END IF;
            END LOOP;
            DECLARE old_snap_name text := format('base_snapshot_%s', old_oid::text);
            BEGIN
                PERFORM public.flashback_drop_payload_table(
                    to_regclass(format('flashback.%I', old_snap_name))
                );
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
                IF stale_snap.snapshot_table !~ '^flashback\."?[a-zA-Z0-9_]+"?$' THEN
                    RAISE EXCEPTION 'flashback_track: invalid snapshot ref: %',
                        stale_snap.snapshot_table;
                END IF;
                PERFORM public.flashback_drop_payload_table(
                    to_regclass(stale_snap.snapshot_table)
                );
            END IF;
        END LOOP;
        DELETE FROM flashback.snapshots WHERE rel_oid = v_rel_oid;
        DELETE FROM flashback.delta_log WHERE rel_oid = v_rel_oid;
        DELETE FROM flashback.staging_events WHERE rel_oid = v_rel_oid;
    END;

    PERFORM public.flashback_drop_payload_table(
        to_regclass(format('flashback.%I', v_snapshot_name))
    );
    EXECUTE format('CREATE TABLE flashback.%I AS TABLE %I.%I', v_snapshot_name, v_schema_name, v_table_name);
    PERFORM public.flashback_own_payload_table(
        to_regclass(format('flashback.%I', v_snapshot_name))
    );

    INSERT INTO flashback.tracked_tables (
        tracking_id, rel_oid, schema_name, table_name, base_snapshot_table,
        schema_version, recovery_profile, helper_profile,
        coverage_start_lsn, coverage_end_lsn,
        tracked_since, checkpoint_interval, retention_interval, is_active,
        replica_identity_was, replica_identity_index
    )
    VALUES (
        COALESCE(v_tracking_id, nextval('flashback.tracking_id_seq')),
        v_rel_oid, v_schema_name, v_table_name,
        format('flashback.%I', v_snapshot_name),
        1, 'local_delta', NULL, NULL, NULL,
        now(), interval '15 minutes', interval '7 days', true,
        v_replica_identity_was, v_replica_identity_index
    )
    ON CONFLICT (rel_oid)
    DO UPDATE SET
        schema_name = EXCLUDED.schema_name,
        table_name = EXCLUDED.table_name,
        base_snapshot_table = EXCLUDED.base_snapshot_table,
        schema_version = 1,
        recovery_profile = 'local_delta',
        helper_profile = NULL,
        coverage_start_lsn = NULL,
        coverage_end_lsn = NULL,
        tracked_since = now(),
        is_active = true,
        replica_identity_was = EXCLUDED.replica_identity_was,
        replica_identity_index = EXCLUDED.replica_identity_index;

    SELECT tracked_since INTO v_tracked_since
    FROM flashback.tracked_tables WHERE rel_oid = v_rel_oid;

    SELECT tracking_id INTO v_bound_tracking_id
    FROM flashback.tracked_tables WHERE rel_oid = v_rel_oid;

    IF v_tracking_id IS NOT NULL
       AND v_bound_tracking_id IS DISTINCT FROM v_tracking_id
    THEN
        RAISE EXCEPTION 'pg_flashback: concurrent first-track bound table % to lifecycle %, expected %',
            target_table, v_bound_tracking_id, v_tracking_id;
    END IF;
    v_tracking_id := v_bound_tracking_id;

    IF flashback_effective_capture_mode() = 'wal' THEN
        -- Logical decoding exposes PostgreSQL's 32-bit TransactionId. Keep
        -- every SQL-side correlation key in that same domain; txid_current()
        -- is epoch-expanded and would stop matching after the first wrap.
        v_boundary_xid := (txid_current() % 4294967296)::bigint;
        v_provisional_lsn := pg_current_wal_insert_lsn();
        v_schema_def := COALESCE(flashback_collect_schema_def(v_rel_oid), '{}'::jsonb);
        EXECUTE format('SELECT count(*) FROM flashback.%I', v_snapshot_name)
          INTO v_row_count;

        INSERT INTO flashback.snapshots (
            rel_oid, tracking_id, snapshot_table, snapshot_lsn,
            schema_def, row_count, captured_at
        ) VALUES (
            v_rel_oid, v_tracking_id, format('flashback.%I', v_snapshot_name),
            v_provisional_lsn, v_schema_def, v_row_count, clock_timestamp()
        ) RETURNING snapshot_id INTO v_snapshot_id;

        INSERT INTO flashback.coverage_generations (
            tracking_id, generation_no, stream_id, recovery_profile, state,
            boundary_kind, rel_oid_at_boundary, boundary_snapshot_id,
            boundary_xid, boundary_marker, details
        ) VALUES (
            v_tracking_id, 1, v_stream_id, 'local_delta', 'building',
            'initial_track', v_rel_oid, v_snapshot_id,
            v_boundary_xid, format('initial-track:%s:%s', v_tracking_id, v_boundary_xid),
            jsonb_build_object('provisional_snapshot_lsn', v_provisional_lsn)
        ) RETURNING generation_id INTO v_generation_id;
    END IF;

    DELETE FROM flashback.schema_versions WHERE rel_oid = v_rel_oid;

    INSERT INTO flashback.schema_versions (
        rel_oid, tracking_id, generation_id, stream_id, source_xid,
        schema_version, applied_at, applied_lsn, committed_at, commit_lsn,
        columns, primary_key, constraints, helper_schema_sha256
    )
    SELECT
        v_rel_oid, v_tracking_id, v_generation_id, v_stream_id,
        CASE WHEN v_generation_id IS NOT NULL THEN v_boundary_xid ELSE NULL END,
        1,
        COALESCE(v_tracked_since, clock_timestamp()),
        COALESCE(v_provisional_lsn, pg_current_wal_lsn()),
        CASE WHEN v_generation_id IS NULL THEN COALESCE(v_tracked_since, clock_timestamp()) ELSE NULL END,
        NULL,
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
        ),
        flashback_helper_schema_sha256(v_rel_oid)
    FROM (
        SELECT COALESCE(flashback_collect_schema_def(v_rel_oid), '{}'::jsonb) AS schema_def
    ) s;

    -- Tracking itself only mutates flashback.* metadata, which the output
    -- plugin deliberately filters to avoid a worker feedback loop.  Emit one
    -- transactional marker so the decoder publishes this transaction's real
    -- COMMIT record and the building generation can acquire an exact
    -- COMMIT-LSN boundary.  The marker is metadata-only; it is never replayed
    -- as a table event.
    IF v_generation_id IS NOT NULL THEN
        PERFORM pg_logical_emit_message(
            true,
            'pg_flashback',
            jsonb_build_object(
                'op', 'BOUNDARY',
                'kind', 'initial_track',
                'tracking_id', v_tracking_id,
                'generation_id', v_generation_id
            )::text
        );
    END IF;

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
    v_tracking_id bigint;
    v_schema_name text;
    v_table_name text;
    v_snapshot_id bigint;
    v_snapshot_table_name text;
    v_row_count bigint;
BEGIN
    SELECT tt.rel_oid, tt.tracking_id, tt.schema_name, tt.table_name
      INTO v_rel_oid, v_tracking_id, v_schema_name, v_table_name
    FROM flashback.tracked_tables tt
    WHERE tt.is_active
      AND tt.recovery_profile = 'local_delta'
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

    IF EXISTS (
        SELECT 1 FROM flashback.coverage_generations cg
        WHERE cg.tracking_id = v_tracking_id
    ) THEN
        RAISE EXCEPTION 'flashback_checkpoint: legacy checkpoint API is disabled for correctness-qualified WAL generations'
            USING HINT = 'Use the generation-aware maintenance re-anchor operation when it is available; automatic full-table checkpoints are intentionally disabled.';
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

    PERFORM public.flashback_drop_payload_table(
        to_regclass(format('flashback.%I', v_snapshot_table_name))
    );
    EXECUTE format(
        'CREATE TABLE flashback.%I AS TABLE %I.%I',
        v_snapshot_table_name, v_schema_name, v_table_name
    );
    PERFORM public.flashback_own_payload_table(
        to_regclass(format('flashback.%I', v_snapshot_table_name))
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
          AND tt.recovery_profile = 'local_delta'
          AND NOT EXISTS (
              SELECT 1 FROM flashback.coverage_generations cg
              WHERE cg.tracking_id = tt.tracking_id
          )
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
              AND tt.recovery_profile = 'local_delta'
              AND m.event_time >= tt.tracked_since
        )
        -- event_id assignment must follow capture order: replay's net-effect
        -- computation orders events by event_id.
        ORDER BY m.staging_id;

        GET DIAGNOSTICS v_moved = ROW_COUNT;
        v_total := v_total + v_moved;
        EXIT WHEN v_moved < batch_size;
    END LOOP;

    RETURN v_total;
END;
$$;

-- Consume decoded changes from this database's logical replication slot
-- into delta_log. Normally invoked by the background worker every cycle;
-- callable manually for testing or after worker downtime.
-- Events are stamped with the transaction's REAL commit time (emitted by
-- the output plugin in its commit message) and the change LSN, so PITR
-- stays accurate even when consumption lags behind commits.
-- Returns the number of events inserted into delta_log.
CREATE OR REPLACE FUNCTION flashback_consume_wal(batch_size integer DEFAULT 4096)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_inserted integer := 0;
    v_stream_id bigint;
    v_frontier_lsn pg_lsn;
    v_frontier_time timestamptz;
    v_confirmed_flush_lsn pg_lsn;
    v_restart_lsn pg_lsn;
    v_missing_commits integer;
    v_has_output boolean;
    v_tracked_oids text;
    v_slot_name text;
    v_scan_start_lsn pg_lsn;
    v_upto_lsn pg_lsn;
    v_scan_window_bytes constant bigint := 16777216;
    v_empty_min_advance_bytes constant bigint := 65536;
    v_discarded integer;
    pending record;
    v_gap_inserted integer;
    lock_rec record;
BEGIN
    IF to_regclass('flashback.delta_log') IS NULL THEN
        RETURN 0;
    END IF;

    v_stream_id := flashback_ensure_active_wal_stream();
    IF v_stream_id IS NULL THEN
        RETURN 0;
    END IF;

    -- Freeze the upper bound before reading lifecycle metadata. A tracking or
    -- re-anchor transaction that commits after this point is beyond the fixed
    -- prefix even if a later READ COMMITTED statement can already see its
    -- catalog rows. Conversely, every commit included by this bound was
    -- visible before the tracked-OID snapshot below.
    v_slot_name := flashback_effective_slot_name();
    SELECT confirmed_flush_lsn
      INTO v_scan_start_lsn
    FROM pg_replication_slots
    WHERE slot_name = v_slot_name
      AND database = current_database();

    IF v_scan_start_lsn IS NULL THEN
        RETURN 0;
    END IF;

    v_upto_lsn := LEAST(
        pg_current_wal_insert_lsn(),
        v_scan_start_lsn + v_scan_window_bytes
    );
    IF v_upto_lsn <= v_scan_start_lsn THEN
        RETURN 0;
    END IF;

    -- Decode OIDs belonging to active local_delta/backup lifecycles. Historical
    -- local_delta generation OIDs are retained because a restore swaps the
    -- physical relation while the slot may still contain pre-swap WAL.
    SELECT COALESCE(string_agg(rel_oid::text, ',' ORDER BY rel_oid), '')
      INTO v_tracked_oids
    FROM (
        SELECT tt.rel_oid
        FROM flashback.tracked_tables tt
        WHERE tt.is_active
          AND tt.recovery_profile IN ('local_delta', 'backup')
        UNION
        SELECT cg.rel_oid_at_boundary
        FROM flashback.coverage_generations cg
        JOIN flashback.tracked_tables tt USING (tracking_id)
        WHERE tt.is_active
          AND tt.recovery_profile = 'local_delta'
          AND cg.state IN ('building', 'active', 'sealed')
    ) recoverable_relations;

    -- Preflight the fixed prefix with tuple payload conversion disabled. A
    -- single transaction may decode to more than PostgreSQL's 256 MiB varlena
    -- limit, so never aggregate the peek into one JSONB value. This lightweight
    -- pass exists only to identify lifecycle locks before get_changes: logical
    -- slot advancement is not safely undone by a later PL/pgSQL exception.
    SELECT EXISTS (
        SELECT 1
        FROM pg_logical_slot_peek_changes(
                 v_slot_name, v_upto_lsn, batch_size,
                 'tracked_oids', v_tracked_oids,
                 'metadata_only', 'true'
             ) AS ch(lsn, xid, data)
        WHERE ch.data LIKE '{%'
    ) INTO v_has_output;

    IF NOT v_has_output THEN
        -- Avoid a self-sustaining metadata-WAL loop for tiny internal tails.
        IF pg_wal_lsn_diff(v_upto_lsn, v_scan_start_lsn)
               < v_empty_min_advance_bytes
        THEN
            RETURN 0;
        END IF;

        UPDATE flashback.capture_streams
           SET details = COALESCE(details, '{}'::jsonb)
               || jsonb_build_object(
                    'safe_slot_advance_start_lsn', v_scan_start_lsn,
                    'safe_slot_advance_upto_lsn', v_upto_lsn,
                    'safe_slot_advance_recorded_at', clock_timestamp()
                  )
         WHERE stream_id = v_stream_id
           AND state = 'active';

        SELECT count(*)::integer
          INTO v_discarded
        FROM pg_logical_slot_get_changes(
            v_slot_name, v_upto_lsn, batch_size,
            'tracked_oids', v_tracked_oids,
            'metadata_only', 'true'
        );
        IF v_discarded <> 0 THEN
            -- A transaction can finish COMMIT between two READ COMMITTED
            -- decoding statements while its commit record is already below
            -- the fixed LSN bound. Abort the SQL transaction: PostgreSQL does
            -- not publish get_changes slot advancement until transaction
            -- commit, so the next worker cycle safely peeks and retries the
            -- same prefix with the now-visible transaction.
            RAISE EXCEPTION 'pg_flashback: empty metadata peek/get race (% rows); retrying without slot advancement',
                v_discarded
                USING ERRCODE = 'serialization_failure';
        END IF;
        RETURN 0;
    END IF;

    DROP TABLE IF EXISTS pg_temp._fb_wal_peek;
    CREATE TEMP TABLE _fb_wal_peek (
        change_lsn pg_lsn,
        source_xid bigint,
        data jsonb,
        ord bigint
    ) ON COMMIT DROP;

    INSERT INTO _fb_wal_peek(change_lsn, source_xid, data, ord)
    SELECT ch.lsn, ch.xid::text::bigint, ch.data::jsonb, ch.ord
    FROM pg_logical_slot_peek_changes(
             v_slot_name, v_upto_lsn, batch_size,
             'tracked_oids', v_tracked_oids,
             'metadata_only', 'true'
         )
         WITH ORDINALITY AS ch(lsn, xid, data, ord)
    WHERE ch.data LIKE '{%'
    ORDER BY ch.ord;

    -- Pin only lifecycles touched by this peeked batch (plus building boundary
    -- resolutions and pending protected DDL for commits in the batch). Waiting
    -- on every active lifecycle made an unrelated restore/maintenance hold
    -- head-of-line block capture for other tables.
    --
    -- If any required lifecycle pin is busy, skip without get_changes so the
    -- slot does not advance past rows we are not allowed to promote yet.
    DROP TABLE IF EXISTS pg_temp._fb_wal_lock_ids;
    CREATE TEMP TABLE _fb_wal_lock_ids (
        tracking_id bigint PRIMARY KEY
    ) ON COMMIT DROP;

    INSERT INTO _fb_wal_lock_ids(tracking_id)
    SELECT DISTINCT needed.tracking_id
    FROM (
        SELECT tt.tracking_id
        FROM _fb_wal_peek p
        JOIN flashback.tracked_tables tt
          ON tt.is_active
         AND tt.recovery_profile IN ('local_delta', 'backup')
         AND tt.rel_oid = (p.data->>'oid')::oid
        WHERE (p.data->>'op') IN (
            'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'DROP', 'ALTER'
        )

        UNION

        SELECT cg.tracking_id
        FROM _fb_wal_peek p
        JOIN flashback.coverage_generations cg
          ON cg.rel_oid_at_boundary = (p.data->>'oid')::oid
         AND cg.state IN ('building', 'active', 'sealed')
        JOIN flashback.tracked_tables tt
          ON tt.tracking_id = cg.tracking_id
         AND tt.is_active
         AND tt.recovery_profile = 'local_delta'
        WHERE (p.data->>'op') IN (
            'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'DROP', 'ALTER'
        )

        UNION

        SELECT cg.tracking_id
        FROM flashback.coverage_generations cg
        WHERE cg.state = 'building'
          AND (
              cg.stream_id = v_stream_id
              OR (
                  cg.recovery_profile = 'backup'
                  AND cg.stream_id IS NULL
              )
          )
          AND EXISTS (
              SELECT 1
              FROM _fb_wal_peek p
              WHERE p.data ? 'commit'
                AND (p.data->>'commit')::bigint = cg.boundary_xid
          )

        UNION

        SELECT tt.tracking_id
        FROM flashback.pending_wal_events pend
        JOIN flashback.tracked_tables tt
          ON tt.rel_oid = pend.rel_oid
         AND tt.is_active
        WHERE pend.stream_id = v_stream_id
          AND EXISTS (
              SELECT 1
              FROM _fb_wal_peek p
              WHERE p.data ? 'commit'
                AND (p.data->>'commit')::bigint = pend.source_xid
          )
    ) needed
    WHERE needed.tracking_id IS NOT NULL;

    FOR lock_rec IN
        SELECT tracking_id FROM _fb_wal_lock_ids ORDER BY tracking_id
    LOOP
        IF NOT pg_try_advisory_xact_lock(
            358944::integer, hashint8(lock_rec.tracking_id)
        ) THEN
            RETURN 0;
        END IF;
    END LOOP;

    -- The required locks are now pinned. Record this consumer's exact safe
    -- advancement and fetch the full payload in the same transaction.
    UPDATE flashback.capture_streams
       SET details = COALESCE(details, '{}'::jsonb)
           || jsonb_build_object(
                'safe_slot_advance_start_lsn', v_scan_start_lsn,
                'safe_slot_advance_upto_lsn', v_upto_lsn,
                'safe_slot_advance_recorded_at', clock_timestamp()
              )
     WHERE stream_id = v_stream_id
       AND state = 'active';

    DROP TABLE IF EXISTS pg_temp._fb_wal_batch;
    CREATE TEMP TABLE _fb_wal_batch (
        change_lsn pg_lsn,
        source_xid bigint,
        data jsonb,
        ord bigint
    ) ON COMMIT DROP;

    INSERT INTO _fb_wal_batch(change_lsn, source_xid, data, ord)
    SELECT ch.lsn, ch.xid::text::bigint, ch.data::jsonb, ch.ord
    FROM pg_logical_slot_get_changes(
             v_slot_name, v_upto_lsn, batch_size,
             'tracked_oids', v_tracked_oids
         )
         WITH ORDINALITY AS ch(lsn, xid, data, ord)
    WHERE ch.data LIKE '{%'
    ORDER BY ch.ord;

    -- The full pass must describe the same ordered logical records as the
    -- lightweight preflight. Payload fields intentionally differ.
    IF EXISTS (
        (SELECT change_lsn, source_xid, ord,
                data->>'op', data->>'oid', data->>'xid',
                data->>'commit', data->>'marker'
           FROM _fb_wal_peek
         EXCEPT ALL
         SELECT change_lsn, source_xid, ord,
                data->>'op', data->>'oid', data->>'xid',
                data->>'commit', data->>'marker'
           FROM _fb_wal_batch)
        UNION ALL
        (SELECT change_lsn, source_xid, ord,
                data->>'op', data->>'oid', data->>'xid',
                data->>'commit', data->>'marker'
           FROM _fb_wal_batch
         EXCEPT ALL
         SELECT change_lsn, source_xid, ord,
                data->>'op', data->>'oid', data->>'xid',
                data->>'commit', data->>'marker'
           FROM _fb_wal_peek)
    ) THEN
        RAISE EXCEPTION 'pg_flashback: metadata/full prefix changed during preflight; retrying without slot advancement'
            USING ERRCODE = 'serialization_failure';
    END IF;

    DROP TABLE IF EXISTS pg_temp._fb_wal_commits;
    DROP TABLE IF EXISTS pg_temp._fb_wal_events;
    DROP TABLE IF EXISTS pg_temp._fb_wal_relevant_commits;

    CREATE TEMP TABLE _fb_wal_commits ON COMMIT DROP AS
    SELECT
        (data->>'commit')::bigint AS source_xid,
        (data->>'lsn')::pg_lsn AS commit_lsn,
        TIMESTAMPTZ '2000-01-01 00:00:00+00'
            + (data->>'commit_time')::bigint * interval '1 microsecond' AS committed_at
    FROM _fb_wal_batch
    WHERE data ? 'commit';
    CREATE UNIQUE INDEX ON _fb_wal_commits(source_xid);
    CREATE UNIQUE INDEX ON _fb_wal_commits(commit_lsn);

    CREATE TEMP TABLE _fb_wal_events (
        change_lsn pg_lsn,
        ord bigint,
        event_type text,
        table_name text,
        rel_oid oid,
        source_xid bigint,
        old_data jsonb,
        new_data jsonb,
        ddl_info jsonb,
        msg_schema_version bigint
    ) ON COMMIT DROP;

    INSERT INTO _fb_wal_events (
        change_lsn, ord, event_type, table_name, rel_oid, source_xid,
        old_data, new_data, ddl_info, msg_schema_version
    )
    SELECT
        b.change_lsn,
        b.ord,
        b.data->>'op' AS event_type,
        format('%I.%I', b.data->>'schema', b.data->>'table') AS table_name,
        (b.data->>'oid')::oid AS rel_oid,
        COALESCE(b.source_xid, (b.data->>'xid')::bigint) AS source_xid,
        b.data->'old' AS old_data,
        b.data->'new' AS new_data,
        b.data->'ddl_info' AS ddl_info,
        (b.data->>'schema_version')::bigint AS msg_schema_version
    FROM _fb_wal_batch b
    WHERE b.data->>'op' IN ('INSERT', 'UPDATE', 'DELETE');

    -- Authoritative DDL payload comes only from the protected transactional
    -- table.  A caller-crafted pg_logical_emit_message body can produce the
    -- fixed marker/COMMIT pair, but it can never create one of these rows.
    INSERT INTO _fb_wal_events (
        change_lsn, ord, event_type, table_name, rel_oid, source_xid,
        old_data, new_data, ddl_info, msg_schema_version
    )
    SELECT
        p.event_lsn,
        COALESCE((SELECT max(e.ord) FROM _fb_wal_events e), 0)
            + row_number() OVER (ORDER BY p.pending_event_id),
        p.event_type, p.table_name, p.rel_oid, p.source_xid,
        p.old_data, p.new_data, p.ddl_info, p.schema_version
    FROM flashback.pending_wal_events p
    WHERE p.stream_id = v_stream_id
      AND EXISTS (
          SELECT 1 FROM _fb_wal_commits c
          WHERE c.source_xid = p.source_xid
      )
    ORDER BY p.pending_event_id;

    SELECT count(*) INTO v_missing_commits
    FROM _fb_wal_events e
    LEFT JOIN _fb_wal_commits c USING (source_xid)
    WHERE c.commit_lsn IS NULL;

    IF v_missing_commits > 0 THEN
        PERFORM flashback_mark_capture_stream_broken(
            v_stream_id,
            'decoder_commit_record_missing',
            jsonb_build_object('event_count', v_missing_commits)
        );
        RAISE WARNING 'pg_flashback: % decoded events had no COMMIT record; stream % was frozen and a durable gap was opened',
            v_missing_commits, v_stream_id;
        RETURN 0;
    END IF;

    CREATE TEMP TABLE _fb_wal_relevant_commits ON COMMIT DROP AS
    SELECT c.*
    FROM _fb_wal_commits c
    WHERE EXISTS (
        SELECT 1 FROM _fb_wal_events e WHERE e.source_xid = c.source_xid
    )
       OR EXISTS (
        SELECT 1
        FROM flashback.coverage_generations cg
        WHERE cg.stream_id = v_stream_id
          AND cg.state = 'building'
          AND cg.boundary_xid = c.source_xid
    )
       OR EXISTS (
        SELECT 1
        FROM flashback.coverage_generations cg
        WHERE cg.recovery_profile = 'backup'
          AND cg.stream_id IS NULL
          AND cg.state = 'building'
          AND cg.boundary_xid = c.source_xid
    );
    CREATE UNIQUE INDEX ON _fb_wal_relevant_commits(source_xid);
    CREATE UNIQUE INDEX ON _fb_wal_relevant_commits(commit_lsn);

    INSERT INTO flashback.capture_commits(stream_id, commit_lsn, source_xid, committed_at)
    SELECT v_stream_id, c.commit_lsn, c.source_xid, c.committed_at
    FROM _fb_wal_relevant_commits c
    ORDER BY c.commit_lsn
    ON CONFLICT (stream_id, commit_lsn) DO NOTHING;

    -- Resolve exact boundaries only after their transaction's COMMIT record is
    -- durably present in this same transaction. Initial tracking activates one
    -- generation; a post-restore successor atomically seals its parent.
    -- Backup markers resolve COMMIT coordinates without activating until a
    -- verified FULL backup anchor is installed.
    FOR pending IN
        SELECT
            cg.generation_id, cg.tracking_id, cg.parent_generation_id,
            cg.boundary_snapshot_id, cg.boundary_kind, cg.recovery_profile,
            c.commit_lsn, c.committed_at
        FROM flashback.coverage_generations cg
        JOIN _fb_wal_commits c ON c.source_xid = cg.boundary_xid
        WHERE cg.state = 'building'
          AND (
              cg.stream_id = v_stream_id
              OR (
                  cg.recovery_profile = 'backup'
                  AND cg.stream_id IS NULL
              )
          )
        ORDER BY cg.generation_id
        FOR UPDATE OF cg
    LOOP
        UPDATE flashback.snapshots
           SET snapshot_lsn = pending.commit_lsn,
               captured_at = pending.committed_at
         WHERE snapshot_id = pending.boundary_snapshot_id
           AND tracking_id = pending.tracking_id;

        UPDATE flashback.schema_versions
           SET applied_lsn = pending.commit_lsn,
               committed_at = pending.committed_at,
               commit_lsn = pending.commit_lsn
         WHERE generation_id = pending.generation_id
           AND tracking_id = pending.tracking_id
           AND source_xid = (
               SELECT boundary_xid FROM flashback.coverage_generations
               WHERE generation_id = pending.generation_id
           );

        IF pending.recovery_profile = 'backup' THEN
            IF pending.parent_generation_id IS NOT NULL THEN
                UPDATE flashback.coverage_generations
                   SET state = 'sealed',
                       superseded_before_lsn = pending.commit_lsn,
                       superseded_before_time = pending.committed_at,
                       sealed_at = clock_timestamp(),
                       state_reason = 'successor_boundary_resolved'
                 WHERE generation_id = pending.parent_generation_id
                   AND tracking_id = pending.tracking_id
                   AND state = 'active';

                UPDATE flashback.coverage_generations
                   SET state_reason = 'post_restore_unanchored',
                       details = COALESCE(details, '{}'::jsonb)
                           || jsonb_build_object('tracking_marker_lsn', pending.commit_lsn)
                 WHERE generation_id = pending.generation_id
                   AND state = 'building';

                INSERT INTO flashback.coverage_gaps (
                    tracking_id, source_generation_id, reason,
                    gap_start_lsn, gap_start_time, lower_bound_inclusive,
                    source_xid, details
                )
                SELECT
                    pending.tracking_id, pending.parent_generation_id,
                    'post_restore_unanchored',
                    pending.commit_lsn, pending.committed_at, false,
                    (
                        SELECT boundary_xid FROM flashback.coverage_generations
                        WHERE generation_id = pending.generation_id
                    ),
                    jsonb_build_object(
                        'successor_generation_id', pending.generation_id,
                        'required_next_full_backup', true
                    )
                WHERE NOT EXISTS (
                    SELECT 1 FROM flashback.coverage_gaps g
                    WHERE g.tracking_id = pending.tracking_id
                      AND g.source_generation_id = pending.parent_generation_id
                      AND g.reason = 'post_restore_unanchored'
                      AND g.reanchored_by_generation_id IS NULL
                );
                GET DIAGNOSTICS v_gap_inserted = ROW_COUNT;
                IF v_gap_inserted > 0 THEN
                    RAISE WARNING
                        'pg_flashback: tracking_id % requires action backup_reanchor_required after production swap; take a new qualifying FULL backup',
                        pending.tracking_id;
                    -- Transactional NOTIFY: delivered only if this consume
                    -- transaction commits. Health metadata remains authoritative
                    -- if the notification is lost.
                    PERFORM pg_notify(
                        'pg_flashback_action_required',
                        jsonb_build_object(
                            'tracking_id', pending.tracking_id,
                            'generation_id', pending.generation_id,
                            'action', 'backup_reanchor_required',
                            'reason', 'post_restore_unanchored',
                            'marker_lsn', pending.commit_lsn
                        )::text
                    );
                END IF;
            ELSE
                -- Keep boundary_lsn NULL until activate binds the FULL stop LSN.
                UPDATE flashback.coverage_generations
                   SET state_reason = 'marker_commit_observed',
                       details = COALESCE(details, '{}'::jsonb)
                           || jsonb_build_object('tracking_marker_lsn', pending.commit_lsn)
                 WHERE generation_id = pending.generation_id
                   AND state = 'building';
            END IF;
            CONTINUE;
        END IF;

        IF pending.parent_generation_id IS NOT NULL THEN
            UPDATE flashback.coverage_generations
               SET state = 'sealed',
                   -- A same-stream handoff is continuous and this decoded
                   -- batch proves the parent through the boundary.  A
                   -- cross-stream re-anchor follows a permanent gap: retain
                   -- the old frozen watermark instead of fabricating replay.
                   valid_through_lsn = CASE
                       WHEN stream_id = v_stream_id THEN pending.commit_lsn
                       ELSE valid_through_lsn
                   END,
                   valid_through_time = CASE
                       WHEN stream_id = v_stream_id THEN pending.committed_at
                       ELSE valid_through_time
                   END,
                   superseded_before_lsn = pending.commit_lsn,
                   superseded_before_time = pending.committed_at,
                   sealed_at = clock_timestamp(),
                   state_reason = 'successor_boundary_resolved'
             WHERE generation_id = pending.parent_generation_id
               AND tracking_id = pending.tracking_id
               AND state = 'active';
        END IF;

        UPDATE flashback.coverage_generations
           SET boundary_lsn = pending.commit_lsn,
               boundary_time = pending.committed_at,
               valid_through_lsn = pending.commit_lsn,
               valid_through_time = pending.committed_at,
               state = 'active',
               activated_at = clock_timestamp(),
               state_reason = 'boundary_commit_observed'
         WHERE generation_id = pending.generation_id
           AND state = 'building';

        UPDATE flashback.coverage_gaps
           SET gap_end_lsn = pending.commit_lsn,
               gap_end_time = pending.committed_at,
               reanchored_by_generation_id = pending.generation_id,
               reanchored_at = clock_timestamp()
         WHERE tracking_id = pending.tracking_id
           AND source_generation_id = pending.parent_generation_id
           AND reanchored_by_generation_id IS NULL
           AND gap_start_lsn < pending.commit_lsn;
    END LOOP;

    UPDATE flashback.schema_versions sv
       SET applied_lsn = c.commit_lsn,
           committed_at = c.committed_at,
           commit_lsn = c.commit_lsn
      FROM _fb_wal_commits c
     WHERE sv.stream_id = v_stream_id
       AND sv.source_xid = c.source_xid
       AND sv.commit_lsn IS NULL;

    WITH qualified AS (
        SELECT
            e.*, c.commit_lsn, c.committed_at,
            tt.tracking_id, cg.generation_id, cg.stream_id,
            tt.recovery_profile
        FROM _fb_wal_events e
        JOIN _fb_wal_commits c USING (source_xid)
        JOIN flashback.coverage_generations cg
          ON cg.stream_id = v_stream_id
         AND cg.state IN ('active', 'sealed')
         -- A restore swaps tracked_tables.rel_oid to the new physical OID,
         -- while already-buffered WAL still carries the predecessor OID.
         -- The immutable generation boundary is the ownership identity.
         AND cg.rel_oid_at_boundary = e.rel_oid
         AND c.commit_lsn > cg.boundary_lsn
         AND (cg.superseded_before_lsn IS NULL
              OR c.commit_lsn < cg.superseded_before_lsn)
        JOIN flashback.tracked_tables tt
          ON tt.tracking_id = cg.tracking_id
         AND tt.is_active
         AND tt.recovery_profile = 'local_delta'

        UNION ALL

        SELECT
            e.*, c.commit_lsn, c.committed_at,
            tt.tracking_id, NULL::bigint, NULL::bigint,
            tt.recovery_profile
        FROM _fb_wal_events e
        JOIN _fb_wal_commits c USING (source_xid)
        JOIN flashback.tracked_tables tt
          ON tt.rel_oid = e.rel_oid
         AND tt.is_active
         AND tt.recovery_profile = 'backup'
        WHERE e.event_type IN ('TRUNCATE', 'DROP', 'ALTER')
    ), ins AS (
        INSERT INTO flashback.delta_log (
            event_time, event_type, table_name, rel_oid, source_xid,
            tracking_id, generation_id, stream_id,
            committed_at, commit_lsn, schema_version,
            old_data, new_data, ddl_info, lsn
        )
        SELECT
            q.committed_at, q.event_type, q.table_name, q.rel_oid, q.source_xid,
            CASE WHEN q.recovery_profile = 'local_delta' THEN q.tracking_id END,
            CASE WHEN q.recovery_profile = 'local_delta' THEN q.generation_id END,
            CASE WHEN q.recovery_profile = 'local_delta' THEN q.stream_id END,
            q.committed_at,
            q.commit_lsn,
            COALESCE(q.msg_schema_version, (
                SELECT sv.schema_version
                FROM flashback.schema_versions sv
                WHERE sv.tracking_id = q.tracking_id
                  AND (sv.generation_id = q.generation_id OR sv.generation_id IS NULL)
                  AND (sv.commit_lsn IS NULL OR sv.commit_lsn <= q.commit_lsn)
                ORDER BY sv.schema_version DESC
                LIMIT 1
            ), 1),
            q.old_data, q.new_data, q.ddl_info, q.change_lsn
        FROM qualified q
        ORDER BY q.ord
        RETURNING 1
    )
    SELECT count(*) INTO v_inserted FROM ins;

    DELETE FROM flashback.pending_wal_events p
    USING _fb_wal_commits c
    WHERE p.stream_id = v_stream_id
      AND p.source_xid = c.source_xid;

    SELECT commit_lsn, committed_at
      INTO v_frontier_lsn, v_frontier_time
    FROM _fb_wal_relevant_commits
    ORDER BY commit_lsn DESC
    LIMIT 1;

    SELECT confirmed_flush_lsn, restart_lsn
      INTO v_confirmed_flush_lsn, v_restart_lsn
    FROM pg_replication_slots
    WHERE slot_name = v_slot_name
      AND database = current_database();

    IF v_frontier_lsn IS NOT NULL THEN
        UPDATE flashback.capture_streams
           SET valid_through_lsn = GREATEST(valid_through_lsn, v_frontier_lsn),
               valid_through_time = v_frontier_time,
               confirmed_flush_lsn = v_confirmed_flush_lsn,
               restart_lsn = v_restart_lsn,
               details = COALESCE(details, '{}'::jsonb)
                   - 'safe_slot_advance_start_lsn'
                   - 'safe_slot_advance_upto_lsn'
                   - 'safe_slot_advance_recorded_at'
         WHERE stream_id = v_stream_id
           AND state = 'active';

        -- Advance watermarks for lifecycles pinned for this batch. Independently
        -- try-lock idle lifecycles so an unrelated hold does not freeze their
        -- empty prefix, while a busy restore/untrack still owns its watermark.
        FOR lock_rec IN
            SELECT DISTINCT cg.tracking_id
            FROM flashback.coverage_generations cg
            WHERE cg.stream_id = v_stream_id
              AND cg.state IN ('active', 'sealed')
              AND cg.valid_through_lsn <= v_frontier_lsn
            ORDER BY cg.tracking_id
        LOOP
            IF NOT EXISTS (
                SELECT 1 FROM _fb_wal_lock_ids pinned
                WHERE pinned.tracking_id = lock_rec.tracking_id
            ) AND NOT pg_try_advisory_xact_lock(
                358944::integer, hashint8(lock_rec.tracking_id)
            ) THEN
                CONTINUE;
            END IF;

            UPDATE flashback.coverage_generations
               SET valid_through_lsn = LEAST(
                       v_frontier_lsn,
                       COALESCE(superseded_before_lsn, v_frontier_lsn)
                   ),
                   valid_through_time = CASE
                       WHEN superseded_before_lsn IS NULL
                            OR v_frontier_lsn < superseded_before_lsn
                           THEN v_frontier_time
                       ELSE valid_through_time
                   END
             WHERE stream_id = v_stream_id
               AND tracking_id = lock_rec.tracking_id
               AND state IN ('active', 'sealed')
               AND valid_through_lsn <= v_frontier_lsn;
        END LOOP;
    ELSE
        UPDATE flashback.capture_streams
           SET confirmed_flush_lsn = v_confirmed_flush_lsn,
               restart_lsn = v_restart_lsn,
               details = COALESCE(details, '{}'::jsonb)
                   - 'safe_slot_advance_start_lsn'
                   - 'safe_slot_advance_upto_lsn'
                   - 'safe_slot_advance_recorded_at'
         WHERE stream_id = v_stream_id
           AND state = 'active';
    END IF;

    RETURN v_inserted;
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
        FROM flashback.tracked_tables
        WHERE is_active
          AND NOT EXISTS (
              SELECT 1 FROM flashback.coverage_generations cg
              WHERE cg.tracking_id = tracked_tables.tracking_id
          )
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
                    PERFORM public.flashback_drop_payload_table(
                        to_regclass(snap_rec.snapshot_table)
                    );
                END IF;
            END IF;
            DELETE FROM flashback.snapshots WHERE snapshot_id = snap_rec.snapshot_id;
        END LOOP;
    END LOOP;

    SELECT min(clock_timestamp() - retention_interval)
      INTO v_min_cutoff
    FROM flashback.tracked_tables
    WHERE is_active;

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
                    -- The partition bound expression looks like:
                    --   FOR VALUES FROM ('2026-07-01 ...') TO ('2026-08-01 ...')
                    -- The first quoted value is the LOWER bound; a partition is
                    -- only safe to drop when its UPPER bound (inside "TO (...)")
                    -- is older than the retention cutoff.
                    v_bound_upper := substring(v_bound_text from 'TO \(''([^'']+)''\)')::timestamptz;
                    IF v_bound_upper IS NOT NULL AND v_bound_upper < v_min_cutoff THEN
                        PERFORM public.flashback_drop_payload_table(v_part.oid::regclass);
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
        SELECT tt.rel_oid, tt.tracking_id,
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
        FROM flashback.delta_log d
        WHERE d.tracking_id = rec.tracking_id
           OR (d.tracking_id IS NULL AND d.rel_oid = rec.rel_oid);
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
    v_tracking_id bigint;
    v_pk_cols text[];
    rec record;
BEGIN
    SELECT tt.rel_oid, tt.tracking_id INTO v_rel_oid, v_tracking_id
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
        WHERE (
            (v_tracking_id IS NOT NULL AND d.tracking_id = v_tracking_id)
            OR (v_tracking_id IS NULL AND d.rel_oid = v_rel_oid)
        )
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
    v_tracking_id bigint;
    v_schema_name text;
    v_table_name text;
    v_base_snapshot text;
    v_recovery_profile text;
    v_has_generations boolean := false;
    snap_rec record;
BEGIN
    SELECT tt.rel_oid, tt.tracking_id, tt.schema_name, tt.table_name, tt.base_snapshot_table, tt.recovery_profile
      INTO v_rel_oid, v_tracking_id, v_schema_name, v_table_name, v_base_snapshot, v_recovery_profile
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

    SELECT EXISTS (
        SELECT 1 FROM flashback.coverage_generations cg
        WHERE cg.tracking_id = v_tracking_id
    ) INTO v_has_generations;

    IF v_has_generations THEN
        -- Qualified WAL lifecycle operations use database-stream -> stable
        -- tracking order. Untrack consumes the slot before retiring the
        -- binding, so taking only the tracking key first would deadlock
        -- against the worker (which takes the database key first).
        IF flashback_effective_capture_mode() = 'wal' THEN
            PERFORM pg_advisory_xact_lock(
                358945::integer,
                (SELECT oid::integer FROM pg_database WHERE datname = current_database())
            );
        END IF;
        PERFORM pg_advisory_xact_lock(358944::integer, hashint8(v_tracking_id));
        IF EXISTS (
            SELECT 1 FROM flashback.coverage_generations cg
            WHERE cg.tracking_id = v_tracking_id AND cg.state = 'building'
        ) THEN
            RAISE EXCEPTION 'flashback_untrack: lifecycle % has a pending generation', v_tracking_id
                USING HINT = 'Wait for its boundary COMMIT LSN to resolve before untracking.';
        END IF;
        IF EXISTS (
            SELECT 1
            FROM flashback.generation_payload_retirements r
            WHERE r.tracking_id = v_tracking_id
              AND r.state = 'retiring'
        ) THEN
            RAISE EXCEPTION 'flashback_untrack: lifecycle % has an unfinished retention cleanup',
                v_tracking_id
                USING HINT = 'Resume the durable generation retirement, then retry untrack.';
        END IF;
        IF to_regclass(format('%I.%I', v_schema_name, v_table_name)) IS NOT NULL THEN
            EXECUTE format('LOCK TABLE %I.%I IN SHARE ROW EXCLUSIVE MODE',
                           v_schema_name, v_table_name);
        END IF;
        IF flashback_effective_capture_mode() = 'wal' THEN
            PERFORM flashback_consume_wal(50000);
        END IF;
    END IF;

    -- The backup profile never changes replica identity or installs DML
    -- triggers, so only local_delta needs capture teardown.
    IF v_recovery_profile = 'local_delta' AND flashback_effective_capture_mode() = 'trigger' THEN
        IF to_regclass(format('%I.%I', v_schema_name, v_table_name)) IS NOT NULL THEN
            PERFORM flashback_detach_capture_trigger(v_schema_name, v_table_name);
        END IF;
    ELSIF v_recovery_profile = 'local_delta' THEN
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
        PERFORM public.flashback_drop_payload_table(to_regclass(v_base_snapshot));
    END IF;

    FOR snap_rec IN
        SELECT snapshot_id, snapshot_table
        FROM flashback.snapshots
        WHERE (v_has_generations AND tracking_id = v_tracking_id)
           OR (NOT v_has_generations AND rel_oid = v_rel_oid)
    LOOP
        IF snap_rec.snapshot_table IS NOT NULL AND snap_rec.snapshot_table <> '' THEN
            IF snap_rec.snapshot_table !~ '^flashback\."?[a-zA-Z0-9_]+"?$' THEN
                RAISE WARNING 'flashback_untrack: skipping invalid snapshot ref: %', snap_rec.snapshot_table;
                CONTINUE;
            END IF;
            PERFORM public.flashback_drop_payload_table(
                to_regclass(snap_rec.snapshot_table)
            );
        END IF;
        IF v_has_generations THEN
            UPDATE flashback.snapshots
               SET payload_state = 'retired', retired_at = clock_timestamp()
             WHERE snapshot_id = snap_rec.snapshot_id;
        ELSE
            DELETE FROM flashback.snapshots WHERE snapshot_id = snap_rec.snapshot_id;
        END IF;
    END LOOP;

    DELETE FROM flashback.delta_log
    WHERE (v_has_generations AND tracking_id = v_tracking_id)
       OR (NOT v_has_generations AND rel_oid = v_rel_oid);
    DELETE FROM flashback.staging_events WHERE rel_oid = v_rel_oid;
    DELETE FROM flashback.schema_versions
    WHERE (v_has_generations AND tracking_id = v_tracking_id)
       OR (NOT v_has_generations AND rel_oid = v_rel_oid);

    IF v_has_generations THEN
        UPDATE flashback.coverage_generations
           SET state = 'sealed',
               superseded_before_lsn = valid_through_lsn + 1,
               superseded_before_time = clock_timestamp(),
               sealed_at = clock_timestamp(),
               state_reason = 'untracked'
         WHERE tracking_id = v_tracking_id
           AND state = 'active';
        UPDATE flashback.coverage_generations
           SET state = 'retired',
               retired_at = clock_timestamp(),
               state_reason = 'untracked'
         WHERE tracking_id = v_tracking_id
           AND state = 'sealed';
    END IF;

    DELETE FROM flashback.tracked_tables WHERE rel_oid = v_rel_oid;

    RETURN true;
END;
$$;

-- Capture the recovery boundary before ALTER runs. Local-delta tracking keeps
-- its existing post-ALTER event; backup tracking needs this separate pre-DDL
-- LSN because physical recovery cannot undo an ALTER that has already replayed.
CREATE OR REPLACE FUNCTION flashback_capture_backup_ddl_marker(
    event_type text,
    input_schema text,
    input_table text
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_target text;
    v_rel_oid oid;
    v_tracked record;
BEGIN
    IF upper(event_type) <> 'ALTER' THEN
        RETURN;
    END IF;
    v_target := CASE
        WHEN input_schema IS NULL OR input_schema = '' THEN format('%I', input_table)
        ELSE format('%I.%I', input_schema, input_table)
    END;
    v_rel_oid := flashback_resolve_tracked_backup(v_target);
    IF v_rel_oid IS NULL THEN
        RETURN;
    END IF;

    SELECT rel_oid, schema_name, table_name, schema_version
      INTO v_tracked
    FROM flashback.tracked_tables
    WHERE rel_oid = v_rel_oid
      AND is_active
      AND recovery_profile = 'backup';

    INSERT INTO flashback.delta_log (
        event_time, event_type, table_name, rel_oid, source_xid,
        committed_at, lsn, schema_version, old_data, new_data, ddl_info
    ) VALUES (
        clock_timestamp(), 'ALTER',
        format('%I.%I', v_tracked.schema_name, v_tracked.table_name),
        v_tracked.rel_oid, (txid_current() % 4294967296)::bigint,
        clock_timestamp(), pg_current_wal_insert_lsn(), v_tracked.schema_version,
        NULL, NULL, COALESCE(flashback_collect_schema_def(v_tracked.rel_oid), '{}'::jsonb)
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_capture_ddl_event(
    event_type text,
    input_schema text,
    input_table text
)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, flashback, public
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
    v_generation_id bigint;
    v_stream_id bigint;
    v_stream_state text;
BEGIN
    IF input_table IS NULL OR input_table = '' THEN RETURN; END IF;

    -- After RENAME TABLE the hook fires with the NEW name.
    -- tracked_tables still has the OLD name but same OID.
    -- Try new name first; fall back to OID-based lookup.
    IF input_schema IS NULL OR input_schema = '' THEN
        SELECT tt.rel_oid, tt.tracking_id, tt.schema_name, tt.table_name, tt.schema_version, tt.recovery_profile
          INTO tracked
        FROM flashback.tracked_tables tt
        WHERE tt.table_name = input_table
        ORDER BY tt.is_active DESC, tt.tracked_since DESC LIMIT 1;
    ELSE
        SELECT tt.rel_oid, tt.tracking_id, tt.schema_name, tt.table_name, tt.schema_version, tt.recovery_profile
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
                SELECT tt.rel_oid, tt.tracking_id, tt.schema_name, tt.table_name, tt.schema_version, tt.recovery_profile
                  INTO tracked
                FROM flashback.tracked_tables tt
                WHERE tt.rel_oid = v_oid
                ORDER BY tt.is_active DESC LIMIT 1;
            END IF;
        END;
    END IF;

    IF tracked.rel_oid IS NULL THEN RETURN; END IF;

    IF tracked.recovery_profile = 'local_delta' THEN
        -- DDL has no row trigger to mediate a session-local SUSET override.
        -- Reconcile it synchronously before routing the event; a refused guard
        -- fails the hook closed so the DDL cannot commit against an unrecorded
        -- qualified lifecycle.
        IF NOT flashback_capture_configuration_guard(tracked.rel_oid) THEN
            RAISE EXCEPTION 'pg_flashback: DDL capture refused because capture configuration is disabled or no active WAL epoch exists'
                USING HINT = 'Restore pg_flashback.enabled/capture_mode, then establish a new exact boundary with flashback_reanchor().';
        END IF;

        -- Serialize DDL routing with stream breaks and generation retirement.
        -- The configuration reconciler holds the database-stream key first
        -- and then this key; this path never takes the outer database key.
        PERFORM pg_advisory_xact_lock(358944::integer,
                                      hashint8(tracked.tracking_id));

        -- An existing qualified lifecycle is routed by its durable generation
        -- binding, never by a caller's session-local capture_mode GUC. This
        -- prevents `SET capture_mode=trigger` from silently sending DDL around
        -- the protected WAL path.
        SELECT cg.generation_id, cg.stream_id, cs.state
          INTO v_generation_id, v_stream_id, v_stream_state
        FROM flashback.coverage_generations cg
        JOIN flashback.capture_streams cs ON cs.stream_id = cg.stream_id
        WHERE cg.tracking_id = tracked.tracking_id
          AND cg.state = 'active'
        LIMIT 1;

        IF v_generation_id IS NULL AND (
            EXISTS (
                SELECT 1 FROM flashback.coverage_generations cg
                WHERE cg.tracking_id = tracked.tracking_id
            )
            OR flashback_effective_capture_mode() = 'wal'
        ) THEN
            RAISE EXCEPTION 'pg_flashback: DDL capture refused because tracking lifecycle % has no active WAL generation',
                tracked.tracking_id;
        END IF;
        IF v_generation_id IS NOT NULL AND v_stream_state <> 'active' THEN
            RAISE EXCEPTION 'pg_flashback: DDL capture refused because WAL stream % is %',
                v_stream_id, v_stream_state
                USING HINT = 'Restore pg_flashback.enabled/capture_mode, then establish a new exact boundary with flashback_reanchor().';
        END IF;
    END IF;

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
        IF tracked.recovery_profile = 'local_delta'
           AND v_generation_id IS NULL
           AND flashback_effective_capture_mode() = 'trigger'
        THEN
            PERFORM flashback_detach_capture_trigger(v_actual_schema, v_actual_table);
            PERFORM flashback_attach_capture_trigger(v_actual_schema, v_actual_table);
        END IF;

        -- Use new name for the rest of this function
        tracked.schema_name := v_actual_schema;
        tracked.table_name  := v_actual_table;
    END IF;

    ddl_event_time := clock_timestamp();
    -- Use the insertion position, not pg_current_wal_lsn() (the write
    -- position). The write position may lag and cannot order the pre-DDL
    -- marker against the catalog WAL generated by ALTER.
    ddl_event_lsn := pg_current_wal_insert_lsn();

    IF upper(event_type) = 'ALTER' THEN
        ddl_info := COALESCE(flashback_collect_schema_def(tracked.rel_oid), '{}'::jsonb);
        new_version := COALESCE(tracked.schema_version, 1) + 1;

        UPDATE flashback.tracked_tables
        SET schema_version = new_version
        WHERE rel_oid = tracked.rel_oid;

        INSERT INTO flashback.schema_versions (
            rel_oid, tracking_id, generation_id, stream_id, source_xid,
            schema_version, applied_at, applied_lsn, committed_at, commit_lsn,
            columns, primary_key, constraints, helper_schema_sha256
        )
        SELECT
            tracked.rel_oid,
            CASE WHEN v_generation_id IS NOT NULL THEN tracked.tracking_id END,
            v_generation_id, v_stream_id,
            CASE WHEN v_generation_id IS NOT NULL
                 THEN (txid_current() % 4294967296)::bigint END,
            new_version, ddl_event_time, ddl_event_lsn,
            CASE WHEN v_generation_id IS NULL THEN clock_timestamp() END,
            NULL,
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
            ),
            flashback_helper_schema_sha256(tracked.rel_oid);
    ELSE
        ddl_info := COALESCE(flashback_collect_schema_def(tracked.rel_oid), '{}'::jsonb);
        new_version := COALESCE(tracked.schema_version, 1);
    END IF;

    -- Backup ALTER already has a pre-execution disaster marker. The post hook
    -- is still required to store the new schema version, but a second ALTER
    -- row here would expose an unsafe post-DDL LSN to operators.
    IF tracked.recovery_profile = 'backup' AND upper(event_type) = 'ALTER' THEN
        RETURN;
    END IF;

    IF tracked.recovery_profile = 'backup' THEN
        row_snapshot := NULL;
    ELSE
        DECLARE
            v_row_count bigint;
        BEGIN
        -- Bounded count: stop scanning at 100001 rows so a DDL statement on
        -- a huge table never pays a full-table scan just to decide that the
        -- inline snapshot must be skipped anyway.
        EXECUTE format(
            'SELECT count(*) FROM (SELECT 1 FROM %I.%I LIMIT 100001) q',
            tracked.schema_name, tracked.table_name
        ) INTO v_row_count;
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
    END IF;

    -- In WAL mode the authoritative payload is written to a protected LOGGED
    -- table in this same transaction. pg_logical_emit_message() is PUBLIC in
    -- PostgreSQL, so its body is deliberately ignored by the decoder; the
    -- marker exists only to expose this transaction's real COMMIT record.
    -- Trigger mode retains the direct legacy delta_log path.
    IF tracked.recovery_profile = 'local_delta' AND v_generation_id IS NOT NULL THEN
        INSERT INTO flashback.pending_wal_events (
            tracking_id, generation_id, stream_id, source_xid,
            event_type, table_name, rel_oid, event_lsn, schema_version,
            old_data, new_data, ddl_info
        ) VALUES (
            tracked.tracking_id, v_generation_id, v_stream_id,
            (txid_current() % 4294967296)::bigint, upper(event_type),
            format('%I.%I', tracked.schema_name, tracked.table_name),
            tracked.rel_oid, ddl_event_lsn, new_version,
            row_snapshot, NULL, ddl_info
        );

        PERFORM pg_logical_emit_message(
            true,   -- transactional: tied to current transaction
            'pg_flashback',
            jsonb_build_object(
                'kind', 'commit-marker',
                'source_xid', (txid_current() % 4294967296)::bigint
            )::text
        );
    ELSE
        INSERT INTO flashback.delta_log (
            event_time, event_type, table_name, rel_oid, source_xid,
            committed_at, lsn, schema_version, old_data, new_data, ddl_info
        )
        VALUES (
            ddl_event_time, upper(event_type),
            format('%I.%I', tracked.schema_name, tracked.table_name),
            tracked.rel_oid, (txid_current() % 4294967296)::bigint,
            clock_timestamp(), ddl_event_lsn,
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
    v_part_oid    oid;
    v_is_owned    boolean;
    v_tmp_table   text;
BEGIN
    -- Existing partitions from older releases may not yet be extension
    -- members.  The dependency check is the hot-path: this function runs on
    -- every worker cycle, so an already-owned partition must not take an
    -- ACCESS EXCLUSIVE relation lock every 75 ms.
    v_part_oid := to_regclass(format('flashback.%I', p_part_name));
    IF v_part_oid IS NOT NULL THEN
        SELECT EXISTS (
            SELECT 1
            FROM pg_depend d
            JOIN pg_extension e ON e.oid = d.refobjid
            WHERE d.classid = 'pg_class'::regclass
              AND d.objid = v_part_oid
              AND d.objsubid = 0
              AND d.refclassid = 'pg_extension'::regclass
              AND d.deptype = 'e'
              AND e.extname = 'pg_flashback'
        ) INTO v_is_owned;
        IF NOT v_is_owned THEN
            PERFORM public.flashback_own_payload_table(v_part_oid::regclass);
        END IF;
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
    PERFORM public.flashback_own_payload_table(
        to_regclass(format('flashback.%I', p_part_name))
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
    RAISE WARNING 'flashback: could not create or adopt partition %: %', p_part_name, SQLERRM;
    RAISE;
END;
$$;
