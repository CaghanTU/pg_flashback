DO $tv$
DECLARE t_before timestamptz;
BEGIN
    DROP TABLE IF EXISTS public.it_ckpt_after;
    CREATE TABLE public.it_ckpt_after (id int primary key, v text);
    INSERT INTO public.it_ckpt_after VALUES (1,'a');
    PERFORM flashback_track('public.it_ckpt_after');
    PERFORM flashback_test_attach_capture_trigger('public.it_ckpt_after'::regclass);
    PERFORM flashback_checkpoint('public.it_ckpt_after');
    t_before := clock_timestamp();
    UPDATE public.it_ckpt_after SET v='b' WHERE id=1;
    PERFORM flashback_restore('public.it_ckpt_after', t_before);
    IF NOT EXISTS (SELECT 1 FROM public.it_ckpt_after WHERE id=1 AND v='a') THEN
      RAISE EXCEPTION 'checkpoint restore failed';
    END IF;
END;
$tv$;
