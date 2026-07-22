-- pg_flashback 0.1.0 -> 0.2.0
-- Additive catalog/API for local product hardening (Phases 1–6).
-- Downgrade is not supported.

-- Pre-DROP dependency manifests
DO $$
BEGIN
    IF to_regclass('flashback.drop_dependency_manifests') IS NULL THEN
        CREATE TABLE flashback.drop_dependency_manifests (
            manifest_id   BIGSERIAL PRIMARY KEY,
            tracking_id   BIGINT,
            rel_oid       OID NOT NULL,
            schema_name   TEXT NOT NULL,
            table_name    TEXT NOT NULL,
            captured_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            drop_xid      BIGINT,
            manifest      JSONB NOT NULL
        );
        CREATE INDEX drop_dependency_manifests_tracking_idx
            ON flashback.drop_dependency_manifests (tracking_id, captured_at DESC);
    END IF;
END
$$;

-- Operation journal
DO $$
BEGIN
    IF to_regclass('flashback.operations') IS NULL THEN
        CREATE TABLE flashback.operations (
            operation_id      BIGSERIAL PRIMARY KEY,
            command           TEXT NOT NULL,
            database_name     TEXT NOT NULL DEFAULT current_database(),
            session_user_name TEXT NOT NULL DEFAULT session_user::text,
            tracking_id       BIGINT,
            table_name        TEXT,
            plan_version      INTEGER,
            plan_token        TEXT,
            disaster_event_id BIGINT,
            generation_id     BIGINT,
            target_lsn        PG_LSN,
            created_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            details           JSONB NOT NULL DEFAULT '{}'::jsonb
        );
    END IF;
    IF to_regclass('flashback.operation_events') IS NULL THEN
        CREATE TABLE flashback.operation_events (
            event_id       BIGSERIAL PRIMARY KEY,
            operation_id   BIGINT NOT NULL
                REFERENCES flashback.operations(operation_id),
            event_type     TEXT NOT NULL,
            recorded_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            sqlstate       TEXT,
            error_code     TEXT,
            message        TEXT,
            payload        JSONB NOT NULL DEFAULT '{}'::jsonb
        );
    END IF;
END
$$;

-- Unprotect columns + active-name uniqueness
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='flashback' AND table_name='tracked_tables'
          AND column_name='protection_state'
    ) THEN
        ALTER TABLE flashback.tracked_tables
            ADD COLUMN protection_state TEXT NOT NULL DEFAULT 'active';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='flashback' AND table_name='tracked_tables'
          AND column_name='stop_marker_xid'
    ) THEN
        ALTER TABLE flashback.tracked_tables ADD COLUMN stop_marker_xid BIGINT;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='flashback' AND table_name='tracked_tables'
          AND column_name='stop_marker_commit_lsn'
    ) THEN
        ALTER TABLE flashback.tracked_tables ADD COLUMN stop_marker_commit_lsn PG_LSN;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='flashback' AND table_name='tracked_tables'
          AND column_name='unprotected_at'
    ) THEN
        ALTER TABLE flashback.tracked_tables ADD COLUMN unprotected_at TIMESTAMPTZ;
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'flashback.tracked_tables'::regclass
          AND conname = 'tracked_tables_schema_name_table_name_key'
    ) THEN
        ALTER TABLE flashback.tracked_tables
            DROP CONSTRAINT tracked_tables_schema_name_table_name_key;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname='flashback' AND indexname='tracked_tables_active_name_key'
    ) THEN
        CREATE UNIQUE INDEX tracked_tables_active_name_key
            ON flashback.tracked_tables (schema_name, table_name)
            WHERE is_active;
    END IF;
END
$$;

-- Monitoring cache (not admission authority)
DO $$
BEGIN
    IF to_regclass('flashback.storage_summary_cache') IS NULL THEN
        CREATE TABLE flashback.storage_summary_cache (
            database_name     TEXT PRIMARY KEY,
            used_bytes        BIGINT NOT NULL DEFAULT 0,
            reclaimable_bytes BIGINT NOT NULL DEFAULT 0,
            sample_count      BIGINT NOT NULL DEFAULT 0,
            oldest_event_at   TIMESTAMPTZ,
            refreshed_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            details           JSONB NOT NULL DEFAULT '{}'::jsonb
        );
    END IF;
END
$$;

-- NOTE: CREATE OR REPLACE FUNCTION bodies are delivered by the 0.2.0
-- extension SQL generated at package time. Operators must install the
-- matching 0.2.0 shared library + SQL before ALTER EXTENSION UPDATE.
-- This file only ensures additive catalog prerequisites exist.

SELECT pg_catalog.set_config('pg_flashback.upgrade_note',
    '0.1.0->0.2.0 catalog prerequisites applied', true);
