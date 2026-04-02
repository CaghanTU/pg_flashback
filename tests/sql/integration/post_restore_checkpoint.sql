-- Test: post-restore checkpoint prevents duplicate key on second restore
DO $tv$
DECLARE t1 timestamptz;
        t2 timestamptz;
        v_cnt bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_post_ckpt CASCADE;
    CREATE TABLE public.it_post_ckpt (id int PRIMARY KEY, val text);
    PERFORM flashback_track('public.it_post_ckpt');
    PERFORM flashback_test_attach_capture_trigger('public.it_post_ckpt'::regclass);

    INSERT INTO public.it_post_ckpt VALUES (1, 'A'), (2, 'B'), (3, 'C');
    t1 := clock_timestamp();

    DELETE FROM public.it_post_ckpt WHERE id = 3;
    t2 := clock_timestamp();

    UPDATE public.it_post_ckpt SET val = 'X' WHERE id = 1;

    -- 1st restore: go back to t2 (should drop id=3 delete, keep update)
    PERFORM flashback_restore('public.it_post_ckpt', t2);

    SELECT count(*) INTO v_cnt FROM public.it_post_ckpt WHERE id = 3;
    IF v_cnt <> 0 THEN
        RAISE EXCEPTION '1st restore: id=3 should be deleted at t2, got %', v_cnt;
    END IF;

    -- After restore the table was DROP+CREATE'd; re-attach test trigger
    PERFORM flashback_test_attach_capture_trigger('public.it_post_ckpt'::regclass);

    -- Now do more changes
    INSERT INTO public.it_post_ckpt VALUES (4, 'D');
    t2 := clock_timestamp();
    DELETE FROM public.it_post_ckpt WHERE id = 4;

    -- 2nd restore: should work without duplicate key (post-restore checkpoint exists)
    PERFORM flashback_restore('public.it_post_ckpt', t2);

    SELECT count(*) INTO v_cnt FROM public.it_post_ckpt WHERE id = 4;
    IF v_cnt <> 1 THEN
        RAISE EXCEPTION '2nd restore: expected id=4, got count=%', v_cnt;
    END IF;
END;
$tv$;
