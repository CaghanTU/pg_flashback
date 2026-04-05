-- Test: staging_events must be a LOGGED table (not UNLOGGED)
-- Regression for Bug B9: staging_events was UNLOGGED, causing all unflushed
-- events to be silently lost on server restart / crash.
DO $tv$
BEGIN
    -- Verify staging_events persistence mode
    IF EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'flashback'
          AND c.relname = 'staging_events'
          AND c.relpersistence = 'u'  -- 'u' = unlogged
    ) THEN
        RAISE EXCEPTION
            'staging_events is UNLOGGED — events will be lost on crash. '
            'Run: ALTER TABLE flashback.staging_events SET LOGGED;';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'flashback'
          AND c.relname = 'staging_events'
          AND c.relpersistence = 'p'  -- 'p' = permanent (logged)
    ) THEN
        RAISE EXCEPTION 'staging_events table not found in flashback schema';
    END IF;
END;
$tv$;
