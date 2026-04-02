-- pg_flashback upgrade: 0.3.0 → 0.4.0
-- Run via: ALTER EXTENSION pg_flashback UPDATE TO '0.4.0';
--
-- Changes in 0.4.0:
--   1. Native partitioned table support
--      - flashback_capture_insert_row_trigger() — per-row INSERT for partitioned tables
--      - flashback_capture_delete_row_trigger() — per-row DELETE for partitioned tables
--      - flashback_attach_capture_trigger() updated to detect partitioned parents
--        and automatically use per-row triggers (REFERENCING NEW/OLD TABLE is not
--        supported by PostgreSQL on partitioned tables)
--   2. flashback_restore_parallel() — restore with parallel query hints
--      - Sets max_parallel_workers_per_gather + max_parallel_maintenance_workers
--      - Emits per-partition guidance for partitioned tables
--   3. CI/CD — GitHub Actions pipeline (lint + test matrix PG16/17 + security audit)
--   4. Restore benchmark script (scripts/run_restore_benchmark.sh, 10K–1M rows)
--   5. README rewritten with v0.3.0/v0.4.0 features, correct test count, new GUC docs

-- ================================================================
-- 1. Per-row INSERT trigger for partitioned tables
-- ================================================================
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

    v_rel_oid := COALESCE(to_regclass(v_table_name), TG_RELID);

    INSERT INTO flashback.staging_events
           (event_time, rel_oid, source_xid, event_type, table_name, old_data, new_data)
    VALUES (clock_timestamp(), v_rel_oid, txid_current()::bigint,
            'INSERT', v_table_name, NULL, to_jsonb(NEW));

    RETURN NULL;
END;
$$;

-- ================================================================
-- 2. Per-row DELETE trigger for partitioned tables
-- ================================================================
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

    v_rel_oid := COALESCE(to_regclass(v_table_name), TG_RELID);

    INSERT INTO flashback.staging_events
           (event_time, rel_oid, source_xid, event_type, table_name, old_data, new_data)
    VALUES (clock_timestamp(), v_rel_oid, txid_current()::bigint,
            'DELETE', v_table_name, to_jsonb(OLD), NULL);

    RETURN NULL;
END;
$$;

-- ================================================================
-- 3. Updated flashback_attach_capture_trigger (partition-aware)
-- ================================================================
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

    SELECT c.relkind INTO v_relkind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = input_schema AND c.relname = input_table;

    IF v_relkind = 'p' THEN
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

-- Re-attach triggers on all currently tracked partitioned tables
-- so they use the new per-row variants immediately after upgrade.
DO $$
DECLARE
    tt record;
BEGIN
    FOR tt IN
        SELECT DISTINCT t.schema_name, t.table_name
        FROM flashback.tracked_tables t
        JOIN pg_class c ON c.oid = t.rel_oid
        WHERE t.is_active AND c.relkind = 'p'
    LOOP
        PERFORM flashback_attach_capture_trigger(tt.schema_name, tt.table_name);
        RAISE NOTICE 'pg_flashback upgrade: re-attached partition-aware triggers on %.%',
            tt.schema_name, tt.table_name;
    END LOOP;
END
$$;

-- ================================================================
-- 4. flashback_restore_parallel
-- ================================================================
CREATE OR REPLACE FUNCTION flashback_restore_parallel(
    target_table text,
    target_time  timestamptz,
    num_workers  int DEFAULT 4
)
RETURNS TABLE(restored_table text, events_applied bigint)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_workers    int  := LEAST(GREATEST(num_workers, 1), 8);
    v_rel_oid    oid;
    v_relkind    char;
    v_schema     text;
    v_tblname    text;
    v_applied    bigint;
    v_part       record;
BEGIN
    SELECT c.oid, c.relkind, n.nspname, c.relname
      INTO v_rel_oid, v_relkind, v_schema, v_tblname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = to_regclass(target_table);

    IF v_rel_oid IS NULL THEN
        RAISE EXCEPTION 'flashback_restore_parallel: table % not found', target_table;
    END IF;

    EXECUTE format('SET LOCAL max_parallel_workers_per_gather = %s', v_workers);
    EXECUTE format('SET LOCAL max_parallel_maintenance_workers = %s', v_workers);
    SET LOCAL parallel_leader_participation = on;

    IF v_relkind = 'p' THEN
        RAISE NOTICE
            'flashback_restore_parallel [%]: partitioned table detected — '
            'all events are recorded under parent OID so the parent is restored atomically. '
            'For per-partition parallel execution, run flashback_restore() on each partition '
            'from separate connections.',
            target_table;

        FOR v_part IN
            SELECT n.nspname AS sch, ch.relname AS tbl
            FROM pg_inherits inh
            JOIN pg_class ch ON ch.oid = inh.inhrelid
            JOIN pg_namespace n ON n.oid = ch.relnamespace
            WHERE inh.inhparent = v_rel_oid
            ORDER BY ch.relname
        LOOP
            RAISE NOTICE 'flashback_restore_parallel [%]:   partition → %.%',
                target_table, v_part.sch, v_part.tbl;
        END LOOP;
    END IF;

    v_applied := flashback_restore(target_table, target_time);
    RETURN QUERY SELECT target_table, v_applied;
END;
$$;

-- ================================================================
-- 5. RBAC for new functions
-- ================================================================
DO $$
BEGIN
    REVOKE ALL ON FUNCTION flashback_capture_insert_row_trigger() FROM PUBLIC;
    REVOKE ALL ON FUNCTION flashback_capture_delete_row_trigger() FROM PUBLIC;
EXCEPTION WHEN undefined_function THEN NULL;
END
$$;

DO $$
BEGIN
    GRANT EXECUTE ON FUNCTION flashback_restore_parallel(text, timestamptz, int) TO flashback_admin;
EXCEPTION WHEN undefined_object OR undefined_function THEN NULL;
END
$$;

-- ================================================================
-- 6. Comments
-- ================================================================
COMMENT ON FUNCTION flashback_capture_insert_row_trigger()
    IS '[Internal] Per-row AFTER INSERT trigger for partitioned tables — transition tables not supported on partitioned tables.';
COMMENT ON FUNCTION flashback_capture_delete_row_trigger()
    IS '[Internal] Per-row AFTER DELETE trigger for partitioned tables — transition tables not supported on partitioned tables.';
COMMENT ON FUNCTION flashback_restore_parallel(text, timestamptz, int)
    IS 'Restore a table with parallel-worker hints (max_parallel_workers_per_gather). Also emits per-partition guidance for partitioned tables.';
COMMENT ON FUNCTION flashback_attach_capture_trigger(text, text)
    IS 'Attach INSERT/UPDATE/DELETE capture triggers to a table. Automatically detects partitioned tables and uses per-row triggers. Called internally by flashback_track.';
