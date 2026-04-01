DO $tv$
DECLARE t_target timestamptz;
BEGIN
    DROP TABLE IF EXISTS public.it_ckpt_between;
    CREATE TABLE public.it_ckpt_between (id int primary key, v text);
    INSERT INTO public.it_ckpt_between VALUES (1,'v0');
    PERFORM flashback_track('public.it_ckpt_between');
    PERFORM flashback_test_attach_capture_trigger('public.it_ckpt_between'::regclass);
    UPDATE public.it_ckpt_between SET v='v1' WHERE id=1;
    PERFORM flashback_checkpoint('public.it_ckpt_between');
    t_target := clock_timestamp();
    UPDATE public.it_ckpt_between SET v='v2' WHERE id=1;
    PERFORM flashback_restore('public.it_ckpt_between', t_target);
    IF NOT EXISTS (SELECT 1 FROM public.it_ckpt_between WHERE id=1 AND v='v1') THEN
      RAISE EXCEPTION 'checkpoint between restore failed';
    END IF;
END;
$tv$;
