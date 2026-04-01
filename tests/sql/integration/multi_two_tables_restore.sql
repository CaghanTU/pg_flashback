DO $tv$
DECLARE t_before timestamptz;
BEGIN
    DROP TABLE IF EXISTS public.it_multi_a;
    DROP TABLE IF EXISTS public.it_multi_b;
    CREATE TABLE public.it_multi_a (id int primary key, v text);
    CREATE TABLE public.it_multi_b (id int primary key, v text);
    INSERT INTO public.it_multi_a VALUES (1,'a0');
    INSERT INTO public.it_multi_b VALUES (1,'b0');
    PERFORM flashback_track('public.it_multi_a');
    PERFORM flashback_test_attach_capture_trigger('public.it_multi_a'::regclass);
    PERFORM flashback_track('public.it_multi_b');
    PERFORM flashback_test_attach_capture_trigger('public.it_multi_b'::regclass);
    t_before := clock_timestamp();
    UPDATE public.it_multi_a SET v='a1' WHERE id=1;
    UPDATE public.it_multi_b SET v='b1' WHERE id=1;
    PERFORM flashback_restore('public.it_multi_a', t_before);
    PERFORM flashback_restore('public.it_multi_b', t_before);
    IF NOT EXISTS (SELECT 1 FROM public.it_multi_a WHERE id=1 AND v='a0') THEN RAISE EXCEPTION 'multi a failed'; END IF;
    IF NOT EXISTS (SELECT 1 FROM public.it_multi_b WHERE id=1 AND v='b0') THEN RAISE EXCEPTION 'multi b failed'; END IF;
END;
$tv$;
