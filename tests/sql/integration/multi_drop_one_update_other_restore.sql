DO $tv$
DECLARE t_before timestamptz;
BEGIN
    DROP TABLE IF EXISTS public.it_multi_drop_b;
    DROP TABLE IF EXISTS public.it_multi_drop_a;
    CREATE TABLE public.it_multi_drop_a (id int primary key, v text);
    CREATE TABLE public.it_multi_drop_b (id int primary key, v text);
    INSERT INTO public.it_multi_drop_a VALUES (1,'a0');
    INSERT INTO public.it_multi_drop_b VALUES (1,'b0');
    PERFORM flashback_track('public.it_multi_drop_a');
    PERFORM flashback_test_attach_capture_trigger('public.it_multi_drop_a'::regclass);
    PERFORM flashback_track('public.it_multi_drop_b');
    PERFORM flashback_test_attach_capture_trigger('public.it_multi_drop_b'::regclass);
    t_before := clock_timestamp();
    DROP TABLE public.it_multi_drop_a;
    UPDATE public.it_multi_drop_b SET v='b1' WHERE id=1;
    PERFORM flashback_restore(ARRAY['public.it_multi_drop_a','public.it_multi_drop_b'], t_before);
    IF to_regclass('public.it_multi_drop_a') IS NULL THEN RAISE EXCEPTION 'dropped table not restored'; END IF;
    IF NOT EXISTS (SELECT 1 FROM public.it_multi_drop_b WHERE id=1 AND v='b0') THEN RAISE EXCEPTION 'updated table not rolled back'; END IF;
END;
$tv$;
