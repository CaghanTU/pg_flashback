-- =================================================================
-- RBAC: dedicated admin role + least-privilege grants
-- Internal helper functions are REVOKED from all roles.
-- =================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flashback_admin') THEN
        EXECUTE 'CREATE ROLE flashback_admin NOLOGIN';
    END IF;
END
$$;

-- ================================================================
-- Revoke PUBLIC access on all functions
-- ================================================================
REVOKE ALL ON FUNCTION flashback_track(text)                          FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_untrack(text)                        FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_restore(text, timestamptz)           FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_restore(text[], timestamptz)         FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_checkpoint(text)                     FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_set_restore_in_progress(bool)        FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_apply_retention()                    FROM PUBLIC;

-- Internal helper functions: revoke from PUBLIC and flashback_admin
-- These are called internally by restore functions only.
REVOKE ALL ON FUNCTION flashback_build_predicate(oid, jsonb)              FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_build_insert_parts(oid, jsonb)           FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_build_update_set(oid, jsonb)             FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_replay_batch_pk(text, text, oid, oid, timestamptz, timestamptz, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_jsonb_concat(jsonb, jsonb)               FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_collect_schema_def(oid)                  FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_recreate_table_from_ddl(jsonb, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_finalize_shadow_swap(text, text, text, text, jsonb) FROM PUBLIC;
-- Per-row partition trigger functions are internal
REVOKE ALL ON FUNCTION flashback_capture_insert_row_trigger()             FROM PUBLIC;
REVOKE ALL ON FUNCTION flashback_capture_delete_row_trigger()             FROM PUBLIC;

-- ================================================================
-- Grant admin functions to the dedicated role
-- ================================================================
GRANT USAGE, CREATE ON SCHEMA flashback TO flashback_admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA flashback TO flashback_admin;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA flashback TO flashback_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA flashback
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO flashback_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA flashback
    GRANT USAGE, SELECT ON SEQUENCES TO flashback_admin;

-- Public API (for flashback_admin only)
GRANT EXECUTE ON FUNCTION flashback_track(text)                       TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_untrack(text)                     TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_restore(text, timestamptz)        TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_restore(text[], timestamptz)      TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_restore_parallel(text, timestamptz, int) TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_checkpoint(text)                  TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_apply_retention()                 TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_set_restore_in_progress(bool)     TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_attach_capture_trigger(text, text) TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_detach_capture_trigger(text, text) TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_query(text, timestamptz, text)    TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_history(text, interval)           TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_retention_status()                TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_is_restore_in_progress(oid)       TO flashback_admin;

-- NOTE: Internal helpers (build_predicate, build_insert_parts,
-- collect_schema_def, recreate_table_from_ddl, finalize_shadow_swap)
-- are NOT granted to flashback_admin.  They run via SECURITY DEFINER
-- within the restore functions and should never be called directly.

-- Read-only monitoring (pg_monitor built-in role)
GRANT USAGE ON SCHEMA flashback TO pg_monitor;
GRANT SELECT ON flashback.pg_stat_flashback TO pg_monitor;
GRANT SELECT ON flashback.pg_stat_flashback_tables TO pg_monitor;
GRANT SELECT ON flashback.restore_log TO pg_monitor;
GRANT SELECT ON flashback.tracked_tables TO pg_monitor;
GRANT EXECUTE ON FUNCTION flashback_history(text, interval)        TO pg_monitor;
GRANT EXECUTE ON FUNCTION flashback_retention_status()              TO pg_monitor;
GRANT EXECUTE ON FUNCTION flashback_is_restore_in_progress(oid)    TO pg_monitor;

-- ================================================================
-- COMMENT ON FUNCTION: \df+ documentation
-- ================================================================
COMMENT ON FUNCTION flashback_track(text)
    IS 'Start tracking a table — attaches capture triggers, takes base snapshot, records schema version.';
COMMENT ON FUNCTION flashback_untrack(text)
    IS 'Stop tracking a table — detaches triggers, drops snapshots, purges all flashback data for that table.';
COMMENT ON FUNCTION flashback_restore(text, timestamptz)
    IS 'Restore a single table to a point-in-time using shadow-table swap (crash-safe, minimal lock duration).';
COMMENT ON FUNCTION flashback_restore(text[], timestamptz)
    IS 'Restore multiple tables to a point-in-time, ordered by FK dependency (parents first).';
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
    IS '[Internal] Collect full schema definition for a table OID as JSONB. Not callable by users.';
COMMENT ON FUNCTION flashback_build_predicate(oid, jsonb)
    IS '[Internal] Build a WHERE-clause predicate from a JSONB row payload.';
COMMENT ON FUNCTION flashback_build_insert_parts(oid, jsonb)
    IS '[Internal] Build column-list and values-list from a JSONB payload for INSERT.';
COMMENT ON FUNCTION flashback_build_update_set(oid, jsonb)
    IS '[Internal] Build SET clause and PK WHERE clause for UPDATE replay from JSONB new_data.';
COMMENT ON FUNCTION flashback_replay_batch_pk(text, text, oid, oid, timestamptz, timestamptz, text)
    IS '[Internal] Batch replay for PK tables — net-effect computation with bulk DELETE/UPSERT/UPDATE.';
COMMENT ON FUNCTION flashback_jsonb_concat(jsonb, jsonb)
    IS '[Internal] NULL-safe jsonb merge helper for the flashback_jsonb_merge_agg aggregate.';
COMMENT ON FUNCTION flashback_recreate_table_from_ddl(jsonb, text, text)
    IS '[Internal] Recreate table from DDL definition. Supports shadow-table mode for crash-safe restore.';
COMMENT ON FUNCTION flashback_finalize_shadow_swap(text, text, text, text, jsonb)
    IS '[Internal] Atomic swap: DROP original → RENAME shadow. Restores FK, triggers, RLS, ACL. Returns new OID.';
COMMENT ON FUNCTION flashback_capture_insert_trigger()
    IS 'Statement-level AFTER INSERT trigger — bulk-captures new rows via transition table into staging_events. Not used on partitioned tables.';
COMMENT ON FUNCTION flashback_capture_insert_row_trigger()
    IS '[Internal] Per-row AFTER INSERT trigger for partitioned tables — transition tables are not supported on partitioned tables.';
COMMENT ON FUNCTION flashback_capture_update_trigger()
    IS 'Row-level AFTER UPDATE trigger — diff-only capture for PK tables (PK + changed columns), full-row for non-PK. Skips no-op updates.';
COMMENT ON FUNCTION flashback_capture_delete_trigger()
    IS 'Statement-level AFTER DELETE trigger — bulk-captures deleted rows via transition table into staging_events. Not used on partitioned tables.';
COMMENT ON FUNCTION flashback_capture_delete_row_trigger()
    IS '[Internal] Per-row AFTER DELETE trigger for partitioned tables — transition tables are not supported on partitioned tables.';
COMMENT ON FUNCTION flashback_restore_parallel(text, timestamptz, int)
    IS 'Restore a table with parallel-worker hints (max_parallel_workers_per_gather). Also emits per-partition guidance for partitioned tables.';
COMMENT ON FUNCTION flashback_ensure_delta_partition(date)
    IS '[Internal] Ensures monthly delta_log partitions exist for the given date. Called automatically by background worker. No-op on non-partitioned installations.';
REVOKE ALL ON FUNCTION flashback_ensure_delta_partition(date) FROM PUBLIC;
