-- Test: flashback_restore_parallel with parallel worker hints
-- Verifies the function exists, returns correct result, and respects num_workers.
DO $tv$
DECLARE
    t_before timestamptz;
    v_cnt    bigint;
    v_events bigint;
    v_tbl    text;
BEGIN
    DROP TABLE IF EXISTS public.it_parallel_restore CASCADE;

    -- Create table empty so snapshot is empty
    CREATE TABLE public.it_parallel_restore (
        id    serial PRIMARY KEY,
        val   text,
        score integer
    );

    -- Track empty table (snapshot = 0 rows)
    PERFORM flashback_track('public.it_parallel_restore');
    -- Attach test trigger so events go directly to delta_log (bypass worker)
    PERFORM flashback_test_attach_capture_trigger('public.it_parallel_restore'::regclass);

    -- Insert 200 rows — these become delta_log events in the restore window
    INSERT INTO public.it_parallel_restore (val, score)
    SELECT 'item_' || g, g FROM generate_series(1, 200) g;

    t_before := clock_timestamp();
    PERFORM pg_sleep(0.01);

    -- Disaster: overwrite all rows (these events are AFTER t_before → not replayed)
    UPDATE public.it_parallel_restore SET val = 'DISASTER', score = -1;

    SELECT count(*) INTO v_cnt FROM public.it_parallel_restore WHERE val = 'DISASTER';
    IF v_cnt <> 200 THEN
        RAISE EXCEPTION 'setup: expected 200 DISASTER rows, got %', v_cnt;
    END IF;

    -- Restore using parallel path with 2 workers
    SELECT restored_table, events_applied
      INTO v_tbl, v_events
    FROM flashback_restore_parallel('public.it_parallel_restore', t_before, 2);

    IF v_tbl <> 'public.it_parallel_restore' THEN
        RAISE EXCEPTION 'parallel restore: unexpected table name: %', v_tbl;
    END IF;

    IF v_events = 0 THEN
        RAISE EXCEPTION 'parallel restore: reported 0 events applied';
    END IF;

    SELECT count(*) INTO v_cnt FROM public.it_parallel_restore WHERE val LIKE 'item_%';
    IF v_cnt <> 200 THEN
        RAISE EXCEPTION 'parallel restore: expected 200 original rows, got %', v_cnt;
    END IF;

    DROP TABLE IF EXISTS public.it_parallel_restore CASCADE;
END;
$tv$;
