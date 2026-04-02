use pgrx::prelude::*;

extension_sql!(
    r#"
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'flashback') THEN
            EXECUTE 'CREATE SCHEMA flashback';
        END IF;
    END
    $$;

    DO $$
    BEGIN
        IF to_regclass('flashback.delta_log') IS NULL THEN
            EXECUTE 'CREATE TABLE flashback.delta_log (
                event_id    BIGSERIAL PRIMARY KEY,
                event_time  TIMESTAMPTZ NOT NULL DEFAULT now(),
                rel_oid     OID,
                source_xid  BIGINT,
                committed_at TIMESTAMPTZ,
                schema_version BIGINT NOT NULL DEFAULT 1,
                event_type  TEXT NOT NULL,
                table_name  TEXT NOT NULL,
                old_data    JSONB,
                new_data    JSONB
            )';
        END IF;
    END
    $$;

    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'flashback'
              AND table_name = 'delta_log'
              AND column_name = 'source_xid'
        ) THEN
            ALTER TABLE flashback.delta_log ADD COLUMN source_xid BIGINT;
        END IF;
    END
    $$;

    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'flashback'
              AND table_name = 'delta_log'
              AND column_name = 'committed_at'
        ) THEN
            ALTER TABLE flashback.delta_log ADD COLUMN committed_at TIMESTAMPTZ;
        END IF;

        UPDATE flashback.delta_log
        SET committed_at = COALESCE(committed_at, event_time)
        WHERE committed_at IS NULL;
    END
    $$;

    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'flashback'
              AND table_name = 'delta_log'
              AND column_name = 'rel_oid'
        ) THEN
            ALTER TABLE flashback.delta_log ADD COLUMN rel_oid OID;
        END IF;
    END
    $$;

    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'flashback'
              AND table_name = 'delta_log'
              AND column_name = 'ddl_info'
        ) THEN
            ALTER TABLE flashback.delta_log ADD COLUMN ddl_info JSONB;
        END IF;
    END
    $$;

    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'flashback'
              AND table_name = 'delta_log'
              AND column_name = 'schema_version'
        ) THEN
            ALTER TABLE flashback.delta_log ADD COLUMN schema_version BIGINT NOT NULL DEFAULT 1;
        END IF;
    END
    $$;

    DO $$
    BEGIN
        IF to_regclass('flashback.tracked_tables') IS NULL THEN
            EXECUTE 'CREATE TABLE flashback.tracked_tables (
                rel_oid              OID PRIMARY KEY,
                schema_name          TEXT NOT NULL,
                table_name           TEXT NOT NULL,
                base_snapshot_table  TEXT NOT NULL,
                schema_version       BIGINT NOT NULL DEFAULT 1,
                tracked_since        TIMESTAMPTZ NOT NULL DEFAULT now(),
                checkpoint_interval  INTERVAL NOT NULL DEFAULT interval ''15 minutes'',
                retention_interval   INTERVAL NOT NULL DEFAULT interval ''7 days'',
                is_active            BOOLEAN NOT NULL DEFAULT true,
                UNIQUE(schema_name, table_name)
            )';
        END IF;
    END
    $$;

    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'flashback'
              AND table_name = 'tracked_tables'
              AND column_name = 'schema_version'
        ) THEN
            ALTER TABLE flashback.tracked_tables
            ADD COLUMN schema_version BIGINT NOT NULL DEFAULT 1;
        END IF;
    END
    $$;

    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'flashback'
              AND table_name = 'tracked_tables'
              AND column_name = 'retention_interval'
        ) THEN
            ALTER TABLE flashback.tracked_tables
            ADD COLUMN retention_interval INTERVAL NOT NULL DEFAULT interval '7 days';
        END IF;
    END
    $$;

    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'flashback'
              AND table_name = 'tracked_tables'
              AND column_name = 'checkpoint_interval'
        ) THEN
            ALTER TABLE flashback.tracked_tables
            ADD COLUMN checkpoint_interval INTERVAL NOT NULL DEFAULT interval '15 minutes';
        END IF;
    END
    $$;

    DO $$
    BEGIN
        IF to_regclass('flashback.snapshots') IS NULL THEN
            EXECUTE 'CREATE TABLE flashback.snapshots (
                snapshot_id    BIGSERIAL PRIMARY KEY,
                rel_oid        OID NOT NULL,
                snapshot_table TEXT NOT NULL,
                snapshot_lsn   PG_LSN NOT NULL,
                schema_def     JSONB NOT NULL,
                row_count      BIGINT NOT NULL,
                captured_at    TIMESTAMPTZ NOT NULL DEFAULT now()
            )';
        END IF;
    END
    $$;

    DO $$
    BEGIN
        IF to_regclass('flashback.schema_versions') IS NULL THEN
            EXECUTE 'CREATE TABLE flashback.schema_versions (
                version_id      BIGSERIAL PRIMARY KEY,
                rel_oid         OID NOT NULL,
                schema_version  BIGINT NOT NULL,
                applied_at      TIMESTAMPTZ NOT NULL,
                applied_lsn     PG_LSN NOT NULL,
                columns         JSONB NOT NULL,
                primary_key     JSONB NOT NULL DEFAULT ''[]''::jsonb,
                constraints     JSONB NOT NULL DEFAULT ''{}''::jsonb,
                UNIQUE(rel_oid, schema_version)
            )';
        END IF;
    END
    $$;

    DO $$
    BEGIN
        IF to_regclass('flashback.schema_versions_rel_oid_applied_at_idx') IS NULL THEN
            EXECUTE 'CREATE INDEX schema_versions_rel_oid_applied_at_idx ON flashback.schema_versions (rel_oid, applied_at DESC)';
        END IF;
    END
    $$;

    DO $$
    BEGIN
        IF to_regclass('flashback.snapshots_rel_oid_captured_at_idx') IS NULL THEN
            EXECUTE 'CREATE INDEX snapshots_rel_oid_captured_at_idx ON flashback.snapshots (rel_oid, captured_at DESC)';
        END IF;
    END
    $$;

    DO $$
    BEGIN
        IF to_regclass('flashback.delta_log_rel_oid_event_time_idx') IS NULL THEN
            EXECUTE 'CREATE INDEX delta_log_rel_oid_event_time_idx ON flashback.delta_log (rel_oid, event_time) WHERE committed_at IS NOT NULL';
        END IF;
    END
    $$;

    DO $$
    BEGIN
        IF to_regclass('flashback.delta_log_rel_oid_committed_at_idx') IS NULL THEN
            EXECUTE 'CREATE INDEX delta_log_rel_oid_committed_at_idx ON flashback.delta_log (rel_oid, committed_at DESC)';
        END IF;
    END
    $$;

    DO $$
    BEGIN
        IF to_regclass('flashback.staging_events') IS NULL THEN
            EXECUTE 'CREATE UNLOGGED TABLE flashback.staging_events (
                staging_id  BIGSERIAL PRIMARY KEY,
                event_time  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                rel_oid     OID NOT NULL,
                source_xid  BIGINT NOT NULL DEFAULT txid_current()::bigint,
                event_type  TEXT NOT NULL,
                table_name  TEXT NOT NULL,
                old_data    JSONB,
                new_data    JSONB
            )';
        END IF;
    END
    $$;

    DO $$
    BEGIN
        IF to_regclass('flashback.restore_log') IS NULL THEN
            EXECUTE 'CREATE TABLE flashback.restore_log (
                restore_id     BIGSERIAL PRIMARY KEY,
                table_name     TEXT NOT NULL,
                target_time    TIMESTAMPTZ NOT NULL,
                restored_by    TEXT NOT NULL DEFAULT current_user,
                restored_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
                rows_affected  BIGINT NOT NULL DEFAULT 0,
                success        BOOLEAN NOT NULL DEFAULT true,
                error_message  TEXT
            )';
        END IF;
    END
    $$;

    -- Monitoring view: single place for DBA to check pg_flashback health
    CREATE OR REPLACE VIEW flashback.pg_stat_flashback AS
    SELECT
        (SELECT count(*) FROM flashback.tracked_tables WHERE is_active) AS tracked_tables,
        (SELECT count(*) FROM flashback.tracked_tables WHERE NOT is_active) AS untracked_tables,
        (SELECT count(*) FROM flashback.staging_events) AS pending_events,
        (SELECT count(*) FROM flashback.delta_log) AS total_deltas,
        (SELECT pg_size_pretty(pg_total_relation_size('flashback.delta_log'))) AS delta_storage,
        (SELECT pg_size_pretty(pg_total_relation_size('flashback.staging_events'))) AS staging_storage,
        (SELECT count(*) FROM flashback.snapshots) AS total_snapshots,
        (SELECT count(*) FROM flashback.restore_log) AS total_restores,
        (SELECT count(*) FROM flashback.restore_log WHERE success) AS successful_restores,
        (SELECT count(*) FROM flashback.restore_log WHERE NOT success) AS failed_restores,
        (SELECT max(restored_at) FROM flashback.restore_log) AS last_restore_at,
        COALESCE(current_setting('pg_flashback.enabled', true), 'on') AS capture_enabled,
        COALESCE(current_setting('pg_flashback.max_row_size', true), '8kB') AS max_row_size,
        COALESCE(current_setting('pg_flashback.worker_interval_ms', true), '75') AS worker_interval_ms;

    -- Register tables for pg_dump data export (extension-owned tables
    -- are not dumped by default; config dump marks their data for export).
    SELECT pg_catalog.pg_extension_config_dump('flashback.tracked_tables', '');
    SELECT pg_catalog.pg_extension_config_dump('flashback.delta_log', '');
    SELECT pg_catalog.pg_extension_config_dump('flashback.snapshots', '');
    SELECT pg_catalog.pg_extension_config_dump('flashback.restore_log', '');
    -- schema_versions is intentionally excluded: auto-populated by DDL hooks
    "#,
    name = "flashback_storage_schema_bootstrap",
    bootstrap,
);

extension_sql!(
    r#"
    -- ================================================================
    -- RBAC: dedicated admin role + least-privilege grants
    -- ================================================================
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flashback_admin') THEN
            EXECUTE 'CREATE ROLE flashback_admin NOLOGIN';
        END IF;
    END
    $$;

    -- Revoke default PUBLIC access on destructive / privileged functions
    REVOKE ALL ON FUNCTION flashback_track(text)                          FROM PUBLIC;
    REVOKE ALL ON FUNCTION flashback_untrack(text)                        FROM PUBLIC;
    REVOKE ALL ON FUNCTION flashback_restore(text, timestamptz)           FROM PUBLIC;
    REVOKE ALL ON FUNCTION flashback_restore(text[], timestamptz)         FROM PUBLIC;
    REVOKE ALL ON FUNCTION flashback_checkpoint(text)                     FROM PUBLIC;
    REVOKE ALL ON FUNCTION flashback_set_restore_in_progress(bool)        FROM PUBLIC;
    REVOKE ALL ON FUNCTION flashback_apply_retention()                    FROM PUBLIC;

    -- Grant admin functions to the dedicated role
    GRANT USAGE ON SCHEMA flashback TO flashback_admin;
    GRANT EXECUTE ON FUNCTION flashback_track(text)                  TO flashback_admin;
    GRANT EXECUTE ON FUNCTION flashback_untrack(text)                TO flashback_admin;
    GRANT EXECUTE ON FUNCTION flashback_restore(text, timestamptz)   TO flashback_admin;
    GRANT EXECUTE ON FUNCTION flashback_restore(text[], timestamptz) TO flashback_admin;
    GRANT EXECUTE ON FUNCTION flashback_checkpoint(text)             TO flashback_admin;
    GRANT EXECUTE ON FUNCTION flashback_apply_retention()            TO flashback_admin;

    -- Read-only monitoring is available to pg_monitor (built-in role)
    GRANT USAGE ON SCHEMA flashback TO pg_monitor;
    GRANT SELECT ON flashback.pg_stat_flashback TO pg_monitor;
    GRANT SELECT ON flashback.restore_log TO pg_monitor;
    GRANT SELECT ON flashback.tracked_tables TO pg_monitor;
    GRANT EXECUTE ON FUNCTION flashback_history(text, interval) TO pg_monitor;
    GRANT EXECUTE ON FUNCTION flashback_retention_status() TO pg_monitor;
    GRANT EXECUTE ON FUNCTION flashback_is_restore_in_progress(oid) TO pg_monitor;

    -- ================================================================
    -- COMMENT ON FUNCTION: \df+ documentation
    -- ================================================================
    COMMENT ON FUNCTION flashback_track(text)
        IS 'Start tracking a table — attaches capture triggers, takes base snapshot, records schema version.';
    COMMENT ON FUNCTION flashback_untrack(text)
        IS 'Stop tracking a table — detaches triggers, drops snapshots, purges all flashback data for that table.';
    COMMENT ON FUNCTION flashback_restore(text, timestamptz)
        IS 'Restore a single table to a point-in-time by replaying deltas from the nearest snapshot.';
    COMMENT ON FUNCTION flashback_restore(text[], timestamptz)
        IS 'Restore multiple tables to a point-in-time, ordered by FK dependency (children first).';
    COMMENT ON FUNCTION flashback_query(text, timestamptz, text)
        IS 'Reconstruct table state at a past timestamp in a temp table and run an arbitrary query against it (SELECT AS OF).';
    COMMENT ON FUNCTION flashback_checkpoint(text)
        IS 'Create an on-demand point-in-time snapshot (checkpoint) of a tracked table. Returns snapshot_id.';
    COMMENT ON FUNCTION flashback_apply_retention()
        IS 'Purge expired delta_log rows, old snapshots, and stale data per each table''s retention_interval.';
    COMMENT ON FUNCTION flashback_retention_status()
        IS 'Show retention health per tracked table: delta counts, restorable window, and a warning flag at >90% consumption.';
    COMMENT ON FUNCTION flashback_history(text, interval)
        IS 'Return change history (INSERT/UPDATE/DELETE events) for a table within a lookback window, with PK-based row identity.';
    COMMENT ON FUNCTION flashback_set_restore_in_progress(bool)
        IS 'Set the process-local restore-in-progress flag. Superuser only. Used internally by flashback_restore.';
    COMMENT ON FUNCTION flashback_is_restore_in_progress(oid)
        IS 'Return whether the current backend has a restore in progress. Safe to call from triggers or monitoring.';
    COMMENT ON FUNCTION flashback_attach_capture_trigger(text, text)
        IS 'Attach INSERT/UPDATE/DELETE capture triggers to a table. Called internally by flashback_track.';
    COMMENT ON FUNCTION flashback_detach_capture_trigger(text, text)
        IS 'Remove all capture triggers from a table. Called internally by flashback_untrack.';
    COMMENT ON FUNCTION flashback_take_due_checkpoints()
        IS 'Auto-checkpoint all tracked tables whose checkpoint_interval has elapsed. Called by the background worker.';
    COMMENT ON FUNCTION flashback_capture_ddl_event(text, text, text)
        IS 'Record a DDL event (ALTER/DROP/TRUNCATE) with a full schema snapshot into delta_log.';
    COMMENT ON FUNCTION flashback_collect_schema_def(oid)
        IS 'Collect the full schema definition (columns, PK, constraints, indexes, triggers, RLS) for a table OID as JSONB.';
    COMMENT ON FUNCTION flashback_build_predicate(oid, jsonb)
        IS 'Build a WHERE-clause predicate from a JSONB row payload, handling arrays, json, and NULL values.';
    COMMENT ON FUNCTION flashback_build_insert_parts(oid, jsonb)
        IS 'Build column-list and values-list from a JSONB row payload for constructing INSERT statements.';
    COMMENT ON FUNCTION flashback_restore_rows_from_snapshot(oid, text, text, jsonb)
        IS 'Insert rows from a JSONB snapshot array into a target table. Used during legacy snapshot restore.';
    COMMENT ON FUNCTION flashback_recreate_table_from_ddl(jsonb)
        IS 'Recreate a table from a DDL definition (columns, PK, constraints, indexes, triggers, RLS, partitions).';
    COMMENT ON FUNCTION flashback_capture_insert_trigger()
        IS 'Statement-level AFTER INSERT trigger — bulk-captures new rows via transition table into staging_events.';
    COMMENT ON FUNCTION flashback_capture_update_trigger()
        IS 'Row-level AFTER UPDATE trigger — captures old and new row data into staging_events.';
    COMMENT ON FUNCTION flashback_capture_delete_trigger()
        IS 'Statement-level AFTER DELETE trigger — bulk-captures deleted rows via transition table into staging_events.';
    "#,
    name = "flashback_rbac_grants",
    requires = [
        "flashback_api_track_capture",
        "flashback_restore_planner_api",
        "flashback_restore_replay_helpers",
        flashback_set_restore_in_progress,
        flashback_is_restore_in_progress,
    ],
    finalize,
);
