-- =================================================================
-- Restore planner: legacy timestamp-based API compatibility stubs.
--
-- Point-in-time recovery and query in this WAL-only architecture use
-- the correctness-qualified LSN APIs (flashback_restore_lsn,
-- flashback_query_lsn, flashback_recover_deleted_lsn, flashback_resolve_target).
-- The legacy timestamp-based API signatures below are retained for
-- backward compatibility but fail closed unconditionally.
-- =================================================================

CREATE OR REPLACE FUNCTION flashback_restore(target_table text, target_time timestamptz)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
BEGIN
    RAISE EXCEPTION 'flashback_restore(timestamp) is disabled for correctness-qualified WAL coverage'
        USING HINT = 'Call flashback_resolve_target(table, timestamp), inspect its pinned frontier, then execute flashback_restore_lsn(table, resolved_lsn).';
END;
$$;

CREATE OR REPLACE FUNCTION flashback_restore(tables text[], target_time timestamptz)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
BEGIN
    RAISE EXCEPTION 'flashback_restore(timestamp) is disabled for correctness-qualified WAL coverage'
        USING HINT = 'Call flashback_resolve_target(table, timestamp), inspect its pinned frontier, then execute flashback_restore_lsn(table, resolved_lsn).';
END;
$$;

CREATE OR REPLACE FUNCTION flashback_query(
    target_table  text,
    target_time   timestamptz,
    filter_clause text DEFAULT NULL
)
RETURNS SETOF record
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, flashback, public
AS $$
BEGIN
    RAISE EXCEPTION 'flashback_query(timestamp) is disabled for correctness-qualified WAL coverage'
        USING HINT = 'Resolve the timestamp with flashback_resolve_target(), then call flashback_query_lsn().';
END;
$$;

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
BEGIN
    RAISE EXCEPTION 'flashback_restore_parallel(timestamp) is disabled for correctness-qualified WAL coverage'
        USING HINT = 'Call flashback_resolve_target(table, timestamp), inspect its pinned frontier, then execute flashback_restore_lsn(table, resolved_lsn).';
END;
$$;

CREATE OR REPLACE FUNCTION flashback_recover_deleted(
    target_table text,
    target_time  timestamptz
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
BEGIN
    RAISE EXCEPTION 'flashback_recover_deleted(timestamp) is disabled for correctness-qualified WAL coverage'
        USING HINT = 'Resolve the timestamp with flashback_resolve_target(), then call flashback_recover_deleted_lsn().';
END;
$$;
