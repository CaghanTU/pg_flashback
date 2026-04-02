-- pg_flashback upgrade: 0.1.0 → 0.2.0
-- Run via: ALTER EXTENSION pg_flashback UPDATE TO '0.2.0';

-- ================================================================
-- COMMENT ON FUNCTION: DBA-visible documentation for \df+
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
COMMENT ON FUNCTION flashback_recreate_table_from_ddl(jsonb)
    IS 'Recreate a table from a DDL definition (columns, PK, constraints, indexes, triggers, RLS, partitions).';
COMMENT ON FUNCTION flashback_capture_insert_trigger()
    IS 'Statement-level AFTER INSERT trigger — bulk-captures new rows via transition table into staging_events.';
COMMENT ON FUNCTION flashback_capture_update_trigger()
    IS 'Row-level AFTER UPDATE trigger — captures old and new row data into staging_events.';
COMMENT ON FUNCTION flashback_capture_delete_trigger()
    IS 'Statement-level AFTER DELETE trigger — bulk-captures deleted rows via transition table into staging_events.';
