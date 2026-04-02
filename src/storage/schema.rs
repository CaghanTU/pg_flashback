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
