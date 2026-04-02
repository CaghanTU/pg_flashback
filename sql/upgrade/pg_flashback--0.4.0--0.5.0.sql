-- pg_flashback upgrade: 0.4.0 → 0.5.0
-- Run via: ALTER EXTENSION pg_flashback UPDATE TO '0.5.0';
--
-- Changes in 0.5.0:
--   1. WAL-based DML capture via logical decoding output plugin
--      - New output plugin: _PG_output_plugin_init (in pg_flashback.so)
--      - Background worker creates replication slot 'pg_flashback_slot'
--      - Worker consumes WAL changes via pg_logical_slot_get_changes()
--      - Requires wal_level = logical
--   2. New GUC: pg_flashback.capture_mode = 'auto' | 'wal' | 'trigger'
--      - 'auto' (default): detect wal_level, use WAL if logical, else triggers
--      - 'wal': force WAL mode (fails if wal_level != logical)
--      - 'trigger': force legacy trigger mode
--   3. flashback_effective_capture_mode() — returns active mode
--   4. flashback_track/untrack skip trigger ops in WAL mode

-- ================================================================
-- 1. Capture mode detection helper
-- ================================================================
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
    v_wal_level := current_setting('wal_level');
    IF v_wal_level = 'logical' THEN RETURN 'wal'; END IF;
    RETURN 'trigger';
END;
$$;

-- ================================================================
-- 2. Updated flashback_track — skip triggers in WAL mode
-- ================================================================
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

    -- In WAL mode, DML capture comes from the logical replication slot.
    IF flashback_effective_capture_mode() = 'wal' THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_replication_slots WHERE slot_name = 'pg_flashback_slot'
        ) THEN
            BEGIN
                PERFORM pg_create_logical_replication_slot('pg_flashback_slot', 'pg_flashback');
                RAISE NOTICE 'pg_flashback: created logical replication slot pg_flashback_slot';
            EXCEPTION WHEN OTHERS THEN
                RAISE WARNING 'pg_flashback: could not create replication slot in this transaction (%), background worker will retry', SQLERRM;
            END;
        END IF;
    ELSE
        PERFORM flashback_attach_capture_trigger(v_schema_name, v_table_name);
    END IF;

    v_snapshot_name := format('base_snapshot_%s', v_rel_oid::text);

    DECLARE
        stale_snap record;
    BEGIN
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

-- ================================================================
-- 3. Updated flashback_untrack — skip trigger detach in WAL mode
-- ================================================================
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

    IF flashback_effective_capture_mode() = 'trigger' THEN
        IF to_regclass(format('%I.%I', v_schema_name, v_table_name)) IS NOT NULL THEN
            PERFORM flashback_detach_capture_trigger(v_schema_name, v_table_name);
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
