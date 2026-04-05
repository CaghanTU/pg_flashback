-- Regression test: flashback_restore on a previously DROP TABLE'd table
-- Verifies that a table dropped without flashback_untrack can be
-- recovered by calling flashback_restore() with a target_time before the drop.
DO $tv$
DECLARE
    t_before  timestamptz;
    v_count   bigint;
BEGIN
    -- Setup: create and track a table
    DROP TABLE IF EXISTS public.it_restore_dropped CASCADE;
    CREATE TABLE public.it_restore_dropped (
        id   SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );
    PERFORM flashback_track('public.it_restore_dropped');
    PERFORM flashback_test_attach_capture_trigger('public.it_restore_dropped'::regclass);

    INSERT INTO public.it_restore_dropped (name) VALUES ('alice'), ('bob'), ('carol');

    -- Capture a restore point before the drop
    t_before := clock_timestamp();

    -- Simulate accident: DROP TABLE without flashback_untrack
    DROP TABLE public.it_restore_dropped CASCADE;

    -- Confirm table is gone and worker would have set is_active = false
    UPDATE flashback.tracked_tables
       SET is_active = false
     WHERE schema_name = 'public' AND table_name = 'it_restore_dropped';

    -- ── Exercise: restore a dropped table ──────────────────────
    PERFORM flashback_restore('public.it_restore_dropped', t_before);

    -- 1. Table must exist again
    IF to_regclass('public.it_restore_dropped') IS NULL THEN
        RAISE EXCEPTION 'Table should exist after restore, but does not';
    END IF;

    -- 2. Data must be back
    SELECT count(*) INTO v_count FROM public.it_restore_dropped;
    IF v_count <> 3 THEN
        RAISE EXCEPTION 'Expected 3 rows after restore, got %', v_count;
    END IF;

    -- 3. Tracking must be re-activated (is_active = true)
    SELECT count(*) INTO v_count
    FROM flashback.tracked_tables
    WHERE schema_name = 'public' AND table_name = 'it_restore_dropped' AND is_active;
    IF v_count <> 1 THEN
        RAISE EXCEPTION 'Tracking should be re-activated after restore, but is not';
    END IF;

    -- Cleanup
    PERFORM flashback_untrack('public.it_restore_dropped');
    DROP TABLE IF EXISTS public.it_restore_dropped CASCADE;
END;
$tv$;
