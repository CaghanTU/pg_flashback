DO $tv$
DECLARE t_old timestamptz;
BEGIN
    DROP TABLE IF EXISTS public.it_schema_multi;
    CREATE TABLE public.it_schema_multi (id int primary key, a text);
    PERFORM flashback_track('public.it_schema_multi');
    PERFORM flashback_test_attach_capture_trigger('public.it_schema_multi'::regclass);
    INSERT INTO public.it_schema_multi VALUES (1,'x');
    t_old := clock_timestamp();
    ALTER TABLE public.it_schema_multi ADD COLUMN b int DEFAULT 0;
    ALTER TABLE public.it_schema_multi ALTER COLUMN a TYPE varchar(50);
    ALTER TABLE public.it_schema_multi DROP COLUMN b;
    PERFORM flashback_restore('public.it_schema_multi', t_old);
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='it_schema_multi' AND column_name='b') THEN
      RAISE EXCEPTION 'b should not exist at oldest time';
    END IF;
END;
$tv$;
