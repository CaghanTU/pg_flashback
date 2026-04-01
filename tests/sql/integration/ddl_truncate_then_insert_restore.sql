DO $tv$
DECLARE t_before timestamptz; c bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_ddl_trunc_ins;
    CREATE TABLE public.it_ddl_trunc_ins (id int primary key, status text);
    INSERT INTO public.it_ddl_trunc_ins VALUES (1,'a'),(2,'b');
    PERFORM flashback_track('public.it_ddl_trunc_ins');
    PERFORM flashback_test_attach_capture_trigger('public.it_ddl_trunc_ins'::regclass);
    t_before := clock_timestamp();
    TRUNCATE TABLE public.it_ddl_trunc_ins;
    INSERT INTO public.it_ddl_trunc_ins VALUES (99,'z');
    PERFORM flashback_restore('public.it_ddl_trunc_ins', t_before);
    SELECT count(*) INTO c FROM public.it_ddl_trunc_ins;
    IF c <> 2 THEN RAISE EXCEPTION 'truncate+insert restore failed'; END IF;
END;
$tv$;
