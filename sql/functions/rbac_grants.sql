-- =================================================================
-- RBAC: dedicated admin role + least-privilege grants
-- Internal helper functions are REVOKED from all roles.
-- =================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flashback_admin') THEN
        EXECUTE 'CREATE ROLE flashback_admin NOLOGIN';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flashback_recovery_agent') THEN
        EXECUTE 'CREATE ROLE flashback_recovery_agent NOLOGIN';
    END IF;
END
$$;

-- ================================================================
-- Deny-by-default routine ACLs
-- ================================================================
-- PostgreSQL grants EXECUTE on newly created routines to PUBLIC by default.
-- An explicit hand-maintained revoke list is unsafe: a newly added SECURITY
-- DEFINER helper would otherwise become callable by every database role.  This
-- finalize block revokes every routine owned by this extension first; the
-- public API allowlist below then grants only the intended role capabilities.
-- Reset the delegated roles too so an upgrade cannot retain a grant removed
-- from a newer allowlist.
DO $$
DECLARE
    v_routine record;
BEGIN
    FOR v_routine IN
        SELECT n.nspname,
               p.proname,
               pg_get_function_identity_arguments(p.oid) AS identity_args
        FROM pg_depend d
        JOIN pg_extension e
          ON e.oid = d.refobjid
         AND d.refclassid = 'pg_extension'::regclass
        JOIN pg_proc p
          ON d.classid = 'pg_proc'::regclass
         AND p.oid = d.objid
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE e.extname = 'pg_flashback'
          AND d.deptype = 'e'
    LOOP
        EXECUTE format(
            'REVOKE ALL ON ROUTINE %I.%I(%s) FROM PUBLIC, flashback_admin, flashback_recovery_agent, pg_monitor',
            v_routine.nspname,
            v_routine.proname,
            v_routine.identity_args
        );
    END LOOP;
END
$$;

-- ================================================================
-- Grant admin functions to the dedicated role
-- ================================================================
-- Upgrade-safe: remove CREATE that older installations granted. SECURITY
-- DEFINER routines search `flashback` before `public`, so allowing a delegated
-- admin to create objects there would permit helper-name shadowing.
REVOKE CREATE ON SCHEMA flashback FROM flashback_admin;
GRANT USAGE ON SCHEMA flashback TO flashback_admin;
-- Delegated administration is API-only. Direct writes could forge coverage,
-- mutate a base snapshot, or bypass the generation/stream guards. Revoke old
-- broad grants upgrade-safely and make sure future runtime payload tables do
-- not inherit them either.
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA flashback FROM flashback_admin;
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA flashback FROM flashback_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA flashback
    REVOKE ALL PRIVILEGES ON TABLES FROM flashback_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA flashback
    REVOKE ALL PRIVILEGES ON SEQUENCES FROM flashback_admin;
REVOKE ALL ON SCHEMA flashback_import FROM PUBLIC;
GRANT USAGE, CREATE ON SCHEMA flashback_import TO flashback_admin;
GRANT USAGE, CREATE ON SCHEMA flashback_import TO flashback_recovery_agent;

-- Public API (for flashback_admin only)
GRANT EXECUTE ON FUNCTION flashback_track(text)                       TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_untrack(text)                     TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_restore(text, timestamptz)        TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_restore(text[], timestamptz)      TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_restore_lsn(text, pg_lsn)         TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_restore_lsn(text[], pg_lsn)       TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_restore_parallel(text, timestamptz, int) TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_recover_deleted(text, timestamptz) TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_recover_deleted_lsn(text, pg_lsn) TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_checkpoint(text)                  TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_reanchor(text)                    TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_flush_staging(integer)            TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_consume_wal(integer)              TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_apply_retention()                 TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_query(text, timestamptz, text)    TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_query_lsn(text, pg_lsn, text)     TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_resolve_target(text, timestamptz) TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_health()                          TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_history(text, interval)           TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_retention_status()                TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_is_restore_in_progress(oid)       TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_track_backup(text, text)          TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_set_backup_coverage(text, pg_lsn, pg_lsn) TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_backup_disaster_points(text, interval) TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_prepare_backup_restore(text, pg_lsn) TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_finalize_backup_restore(text)     TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_fail_backup_restore(text, text, boolean) TO flashback_admin;
GRANT EXECUTE ON FUNCTION flashback_adopt_existing_payload_tables()          TO flashback_admin;

GRANT EXECUTE ON FUNCTION flashback_claim_backup_restore(text)        TO flashback_recovery_agent;
GRANT EXECUTE ON FUNCTION flashback_accept_backup_restore(text, jsonb) TO flashback_recovery_agent;
GRANT EXECUTE ON FUNCTION flashback_fail_backup_restore(text, text, boolean) TO flashback_recovery_agent;

-- NOTE: Internal helpers (build_predicate, build_insert_parts,
-- collect_schema_def, recreate_table_from_ddl, finalize_shadow_swap)
-- are NOT granted to flashback_admin.  They run via SECURITY DEFINER
-- within the restore functions and should never be called directly.

-- Read-only monitoring (pg_monitor built-in role)
GRANT USAGE ON SCHEMA flashback TO pg_monitor;
GRANT SELECT ON flashback.pg_stat_flashback TO pg_monitor;
GRANT SELECT ON flashback.pg_stat_flashback_tables TO pg_monitor;
GRANT SELECT ON flashback.restore_log TO pg_monitor;
GRANT SELECT ON flashback.tracking_lifecycles TO pg_monitor;
GRANT SELECT ON flashback.tracked_tables TO pg_monitor;
GRANT SELECT ON flashback.capture_streams TO pg_monitor;
GRANT SELECT ON flashback.capture_commits TO pg_monitor;
GRANT SELECT ON flashback.backup_anchors TO pg_monitor;
GRANT SELECT ON flashback.coverage_generations TO pg_monitor;
GRANT SELECT ON flashback.coverage_gaps TO pg_monitor;
GRANT SELECT ON flashback.generation_payload_retirements TO pg_monitor;
GRANT SELECT ON flashback.backup_restore_requests TO pg_monitor;
GRANT EXECUTE ON FUNCTION flashback_history(text, interval)        TO pg_monitor;
GRANT EXECUTE ON FUNCTION flashback_retention_status()              TO pg_monitor;
GRANT EXECUTE ON FUNCTION flashback_is_restore_in_progress(oid)    TO pg_monitor;
GRANT EXECUTE ON FUNCTION flashback_health()                       TO pg_monitor;

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
COMMENT ON FUNCTION flashback_restore_lsn(text, pg_lsn)
    IS 'Correctness-qualified restore of one local_delta table to an admitted transaction COMMIT LSN.';
COMMENT ON FUNCTION flashback_restore_lsn(text[], pg_lsn)
    IS 'Correctness-qualified multi-table restore to one explicit transaction COMMIT LSN.';
COMMENT ON FUNCTION flashback_query_lsn(text, pg_lsn, text)
    IS 'Reconstruct a table at an admitted transaction COMMIT LSN without modifying production.';
COMMENT ON FUNCTION flashback_recover_deleted_lsn(text, pg_lsn)
    IS 'Reinsert rows missing from production that existed at an admitted transaction COMMIT LSN.';
COMMENT ON FUNCTION flashback_resolve_target(text, timestamptz)
    IS 'Resolve a wall-clock target only when the observed transaction set is exactly one proven WAL prefix; collision/inversion/frontier ambiguity fails closed.';
COMMENT ON FUNCTION flashback_health()
    IS 'Read-only generation/stream/gap health projection. It never fabricates coverage from function success.';
COMMENT ON FUNCTION flashback_query(text, timestamptz, text)
    IS 'Reconstruct table state at a past timestamp in a temp table and return rows matching an optional WHERE predicate (SELECT AS OF). Runs as SECURITY INVOKER — filter_clause executes with the caller''s privileges, not the extension owner''s.';
COMMENT ON FUNCTION flashback_checkpoint(text)
    IS 'Create an on-demand point-in-time snapshot (checkpoint) of a tracked table. Returns snapshot_id.';
COMMENT ON FUNCTION flashback_reanchor(text)
    IS 'Create an explicit exact local base and pending successor generation; activation waits for the boundary transaction COMMIT LSN.';
COMMENT ON FUNCTION flashback_apply_retention()
    IS 'Advance the two-transaction generation-retirement state machine: resume committed intents, then durably mark newly eligible sealed generations. Active generation payload is never age-pruned.';
COMMENT ON FUNCTION flashback_retention_status()
    IS 'Show retention health per tracked table: delta counts, restorable window, and a warning flag at >90% consumption.';
COMMENT ON FUNCTION flashback_adopt_existing_payload_tables()
    IS 'Upgrade helper: idempotently adopt legacy runtime payload tables as pg_flashback extension members so logical dumps cannot export orphan recovery data.';
COMMENT ON FUNCTION flashback_history(text, interval)
    IS 'Return change history (INSERT/UPDATE/DELETE events) for a table within a lookback window, with PK-based row identity.';
COMMENT ON FUNCTION flashback_set_restore_in_progress(bool)
    IS '[Internal] Set the process-local restore-in-progress flag. Extension-owner execution chain only.';
COMMENT ON FUNCTION flashback_is_restore_in_progress(oid)
    IS 'Return whether the current backend has a restore in progress. Safe to call from triggers or monitoring.';
COMMENT ON FUNCTION flashback_attach_capture_trigger(text, text)
    IS '[Internal] Attach INSERT/UPDATE/DELETE capture triggers to a table. Called only by guarded lifecycle APIs.';
COMMENT ON FUNCTION flashback_detach_capture_trigger(text, text)
    IS '[Internal] Remove capture triggers from a table. Called only by guarded lifecycle APIs.';
COMMENT ON FUNCTION flashback_take_due_checkpoints()
    IS 'Auto-checkpoint all tracked tables whose checkpoint_interval has elapsed. Called by the background worker.';
COMMENT ON FUNCTION flashback_flush_staging(integer)
    IS 'Manually flush staging_events to delta_log. Normally done by the background worker. Useful when the worker is not running (e.g. testing or recovery). Returns number of events promoted.';
COMMENT ON FUNCTION flashback_consume_wal(integer)
    IS 'Consume decoded changes from this database''s logical replication slot into delta_log, stamped with real commit time and LSN. Normally called by the background worker. Returns number of events inserted.';
COMMENT ON FUNCTION flashback_backup_disaster_points(text, interval)
    IS 'List backup-profile DDL markers and safe pre-DDL target LSNs for operator-selected recovery.';
COMMENT ON FUNCTION flashback_capture_backup_ddl_marker(text, text, text)
    IS 'Internal ProcessUtility hook entry point that records a backup-profile pre-ALTER recovery LSN.';
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
