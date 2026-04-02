-- Test: PITR event_time vs committed_at filtering
-- Verifies that only committed events within the correct time range are replayed
DO $tv$
DECLARE t0 timestamptz;
        t1 timestamptz;
        t2 timestamptz;
        v_cnt bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_pitr CASCADE;
    CREATE TABLE public.it_pitr (id int PRIMARY KEY, val text);
    PERFORM flashback_track('public.it_pitr');
    PERFORM flashback_test_attach_capture_trigger('public.it_pitr'::regclass);

    -- Phase 1: insert rows at t0
    t0 := clock_timestamp();
    INSERT INTO public.it_pitr VALUES (1, 'first');
    t1 := clock_timestamp();

    -- Phase 2: more inserts
    INSERT INTO public.it_pitr VALUES (2, 'second');
    INSERT INTO public.it_pitr VALUES (3, 'third');
    t2 := clock_timestamp();

    -- Phase 3: destructive changes
    DELETE FROM public.it_pitr;

    -- Restore to t1: should have only id=1
    PERFORM flashback_restore('public.it_pitr', t1);
    SELECT count(*) INTO v_cnt FROM public.it_pitr;
    IF v_cnt <> 1 THEN
        RAISE EXCEPTION 'restore to t1: expected 1, got %', v_cnt;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.it_pitr WHERE id = 1 AND val = 'first') THEN
        RAISE EXCEPTION 'restore to t1: id=1 missing';
    END IF;

    -- After restore the table was DROP+CREATE'd; re-attach test trigger
    PERFORM flashback_test_attach_capture_trigger('public.it_pitr'::regclass);

    -- Now make more changes and restore to a different point
    INSERT INTO public.it_pitr VALUES (10, 'new');
    t2 := clock_timestamp();
    DELETE FROM public.it_pitr WHERE id = 10;

    PERFORM flashback_restore('public.it_pitr', t2);
    SELECT count(*) INTO v_cnt FROM public.it_pitr;
    IF v_cnt <> 2 THEN
        RAISE EXCEPTION 'restore to t2: expected 2, got %', v_cnt;
    END IF;

    -- Verify committed_at filter: no NULL committed_at rows in delta_log
    -- (they would be staging rows not yet flushed — in test mode we write committed_at directly)
    SELECT count(*) INTO v_cnt
    FROM flashback.delta_log
    WHERE rel_oid = 'public.it_pitr'::regclass::oid
      AND committed_at IS NULL;
    IF v_cnt <> 0 THEN
        RAISE EXCEPTION 'delta_log has uncommitted rows: %', v_cnt;
    END IF;
END;
$tv$;
