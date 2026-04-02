-- Test: sub-transaction rollback — only committed changes should appear in delta_log
-- Verifies that rolled-back savepoint changes do NOT affect restore
DO $tv$
DECLARE t_before timestamptz;
        v_cnt   bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_subtxn CASCADE;
    CREATE TABLE public.it_subtxn (id int PRIMARY KEY, val text);
    PERFORM flashback_track('public.it_subtxn');
    PERFORM flashback_test_attach_capture_trigger('public.it_subtxn'::regclass);

    INSERT INTO public.it_subtxn VALUES (1, 'committed');
    t_before := clock_timestamp();

    -- This block uses exception handling which internally creates a subtransaction.
    -- The INSERT inside the inner block will be rolled back, but the outer
    -- INSERT should remain.
    BEGIN
        INSERT INTO public.it_subtxn VALUES (2, 'will_rollback');
        -- Force a rollback of this subtransaction
        RAISE EXCEPTION 'intentional_rollback';
    EXCEPTION WHEN OTHERS THEN
        -- swallow
        NULL;
    END;

    INSERT INTO public.it_subtxn VALUES (3, 'also_committed');

    -- At this point table has id=1,3 (id=2 was rolled back)
    SELECT count(*) INTO v_cnt FROM public.it_subtxn;
    IF v_cnt <> 2 THEN
        RAISE EXCEPTION 'pre-check: expected 2 rows, got %', v_cnt;
    END IF;

    -- Verify delta_log does NOT contain the rolled-back insert
    -- (trigger fires but PG rolls back the trigger's INSERT too)
    SELECT count(*) INTO v_cnt
    FROM flashback.delta_log
    WHERE rel_oid = 'public.it_subtxn'::regclass::oid
      AND event_type = 'INSERT'
      AND new_data->>'val' = 'will_rollback';
    IF v_cnt <> 0 THEN
        RAISE EXCEPTION 'delta_log contains rolled-back row (val=will_rollback), count=%', v_cnt;
    END IF;

    -- Now delete id=3 and restore
    DELETE FROM public.it_subtxn WHERE id = 3;

    PERFORM flashback_restore('public.it_subtxn', t_before);

    -- After restore to t_before: only id=1 should exist
    SELECT count(*) INTO v_cnt FROM public.it_subtxn;
    IF v_cnt <> 1 THEN
        RAISE EXCEPTION 'post-restore: expected 1 row, got %', v_cnt;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM public.it_subtxn WHERE id = 1 AND val = 'committed') THEN
        RAISE EXCEPTION 'post-restore: id=1 missing';
    END IF;
END;
$tv$;
