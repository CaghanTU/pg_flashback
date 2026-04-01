DO $tv$
DECLARE t_before timestamptz; c bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_ddl_truncate;
    CREATE TABLE public.it_ddl_truncate (id int primary key, status text);
    INSERT INTO public.it_ddl_truncate VALUES (1,'a'),(2,'b');
    PERFORM flashback_track('public.it_ddl_truncate');
    PERFORM flashback_test_attach_capture_trigger('public.it_ddl_truncate'::regclass);
    t_before := clock_timestamp();
    TRUNCATE TABLE public.it_ddl_truncate;
    PERFORM flashback_restore('public.it_ddl_truncate', t_before);
    SELECT count(*) INTO c FROM public.it_ddl_truncate;
    IF c <> 2 THEN RAISE EXCEPTION 'truncate restore failed'; END IF;
END;
$tv$;
