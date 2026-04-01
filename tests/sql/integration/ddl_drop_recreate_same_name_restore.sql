DO $tv$
DECLARE t_before timestamptz;
BEGIN
    DROP TABLE IF EXISTS public.it_ddl_drop_recreate;
    CREATE TABLE public.it_ddl_drop_recreate (id int primary key, status text);
    INSERT INTO public.it_ddl_drop_recreate VALUES (1,'old');
    PERFORM flashback_track('public.it_ddl_drop_recreate');
    PERFORM flashback_test_attach_capture_trigger('public.it_ddl_drop_recreate'::regclass);
    t_before := clock_timestamp();
    DROP TABLE public.it_ddl_drop_recreate;
    CREATE TABLE public.it_ddl_drop_recreate (id int primary key, status text, note text);
    INSERT INTO public.it_ddl_drop_recreate VALUES (2,'new','x');
    PERFORM flashback_restore('public.it_ddl_drop_recreate', t_before);
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='it_ddl_drop_recreate' AND column_name='note') THEN
      RAISE EXCEPTION 'restored table still has recreated shape';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.it_ddl_drop_recreate WHERE id=1 AND status='old') THEN
      RAISE EXCEPTION 'old table content not restored';
    END IF;
END;
$tv$;
