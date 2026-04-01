DO $tv$
DECLARE t_before timestamptz;
BEGIN
    DROP TABLE IF EXISTS public.it_ddl_drop;
    CREATE TABLE public.it_ddl_drop (id int primary key, status text);
    INSERT INTO public.it_ddl_drop VALUES (1,'a'),(2,'b');
    PERFORM flashback_track('public.it_ddl_drop');
    PERFORM flashback_test_attach_capture_trigger('public.it_ddl_drop'::regclass);
    t_before := clock_timestamp();
    DROP TABLE public.it_ddl_drop;
    PERFORM flashback_restore('public.it_ddl_drop', t_before);
    IF to_regclass('public.it_ddl_drop') IS NULL THEN RAISE EXCEPTION 'drop restore did not recreate table'; END IF;
    IF (SELECT count(*) FROM public.it_ddl_drop) <> 2 THEN RAISE EXCEPTION 'drop restore row count mismatch'; END IF;
END;
$tv$;
