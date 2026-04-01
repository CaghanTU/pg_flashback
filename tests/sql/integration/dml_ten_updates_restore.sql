DO $tv$
DECLARE i int; t_before timestamptz;
BEGIN
    DROP TABLE IF EXISTS public.it_dml_ten_updates;
    CREATE TABLE public.it_dml_ten_updates (id int primary key, status text);
    INSERT INTO public.it_dml_ten_updates VALUES (1, 'v0');
    PERFORM flashback_track('public.it_dml_ten_updates');
    PERFORM flashback_test_attach_capture_trigger('public.it_dml_ten_updates'::regclass);
    t_before := clock_timestamp();
    FOR i IN 1..10 LOOP
        EXECUTE format($q$UPDATE public.it_dml_ten_updates SET status=%L WHERE id=1$q$, 'v' || i::text);
    END LOOP;
    PERFORM flashback_restore('public.it_dml_ten_updates', t_before);
    IF NOT EXISTS (SELECT 1 FROM public.it_dml_ten_updates WHERE id=1 AND status='v0') THEN
        RAISE EXCEPTION 'ten updates restore failed';
    END IF;
END;
$tv$;
