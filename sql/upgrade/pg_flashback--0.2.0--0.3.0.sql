-- pg_flashback upgrade: 0.2.0 → 0.3.0
-- Run via: ALTER EXTENSION pg_flashback UPDATE TO '0.3.0';
--
-- Changes in 0.3.0:
--   1. lz4 compression on delta_log/staging_events JSONB columns
--   2. Diff-only UPDATE capture (PK + changed columns only)
--   3. Batch restore replay for PK tables (net-effect computation)
--   4. Multi-database worker support (target_databases GUC)
--   5. No-op UPDATE skip (to_jsonb(OLD) = to_jsonb(NEW) → skip)
--   6. New helpers: flashback_build_update_set, flashback_replay_batch_pk,
--      flashback_jsonb_concat, flashback_jsonb_merge_agg

-- ================================================================
-- 1. lz4 compression (silently skips if not available)
-- ================================================================
DO $$
BEGIN
    IF to_regclass('flashback.delta_log') IS NOT NULL THEN
        BEGIN
            ALTER TABLE flashback.delta_log ALTER COLUMN old_data SET COMPRESSION lz4;
            ALTER TABLE flashback.delta_log ALTER COLUMN new_data SET COMPRESSION lz4;
        EXCEPTION WHEN feature_not_supported THEN
            RAISE NOTICE 'pg_flashback: lz4 not available for delta_log';
        END;
    END IF;
    IF to_regclass('flashback.staging_events') IS NOT NULL THEN
        BEGIN
            ALTER TABLE flashback.staging_events ALTER COLUMN old_data SET COMPRESSION lz4;
            ALTER TABLE flashback.staging_events ALTER COLUMN new_data SET COMPRESSION lz4;
        EXCEPTION WHEN feature_not_supported THEN
            NULL;
        END;
    END IF;
END
$$;

-- ================================================================
-- 2. New helper functions (created by extension SQL file reload)
-- ================================================================
-- flashback_jsonb_concat, flashback_jsonb_merge_agg,
-- flashback_build_update_set, flashback_replay_batch_pk
-- These are created automatically by the extension SQL files.
-- The upgrade path just needs to update the trigger functions
-- and restore planner, which are also handled by CREATE OR REPLACE.

-- ================================================================
-- 3. RBAC for new internal functions
-- ================================================================
DO $$
BEGIN
    -- Revoke from PUBLIC on new internal functions
    REVOKE ALL ON FUNCTION flashback_build_update_set(oid, jsonb) FROM PUBLIC;
    REVOKE ALL ON FUNCTION flashback_replay_batch_pk(text, text, oid, oid, timestamptz, timestamptz, text) FROM PUBLIC;
    REVOKE ALL ON FUNCTION flashback_jsonb_concat(jsonb, jsonb) FROM PUBLIC;
EXCEPTION WHEN undefined_function THEN
    NULL; -- Functions may not exist yet during upgrade
END
$$;

-- ================================================================
-- 4. Updated documentation
-- ================================================================
COMMENT ON FUNCTION flashback_capture_update_trigger()
    IS 'Row-level AFTER UPDATE trigger — diff-only capture for PK tables (PK + changed columns), full-row for non-PK. Skips no-op updates.';
COMMENT ON FUNCTION flashback_restore(text, timestamptz)
    IS 'Restore a single table to a point-in-time using shadow-table swap. PK tables use batch replay; non-PK uses row-by-row.';
