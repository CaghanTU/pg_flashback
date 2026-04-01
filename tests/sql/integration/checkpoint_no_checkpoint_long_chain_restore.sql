DO $tv$
DECLARE i int; t_before timestamptz;
BEGIN
    DROP TABLE IF EXISTS public.it_ckpt_long;
    CREATE TABLE public.it_ckpt_long (id int primary key, v int);
    INSERT INTO public.it_ckpt_long VALUES (1,0);
    PERFORM flashback_track('public.it_ckpt_long');
    PERFORM flashback_test_attach_capture_trigger('public.it_ckpt_long'::regclass);
    t_before := clock_timestamp();
    FOR i IN 1..60 LOOP
      EXECUTE format('UPDATE public.it_ckpt_long SET v=%s WHERE id=1', i);
    END LOOP;
    PERFORM flashback_restore('public.it_ckpt_long', t_before);
    IF NOT EXISTS (SELECT 1 FROM public.it_ckpt_long WHERE id=1 AND v=0) THEN
      RAISE EXCEPTION 'no-checkpoint long-chain restore failed';
    END IF;
END;
$tv$;
