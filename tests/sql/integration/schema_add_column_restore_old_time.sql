DO $tv$
DECLARE t_before timestamptz;
BEGIN
    DROP TABLE IF EXISTS public.it_schema_add;
    CREATE TABLE public.it_schema_add (id int primary key, name text, status text);
    PERFORM flashback_track('public.it_schema_add');
    PERFORM flashback_test_attach_capture_trigger('public.it_schema_add'::regclass);
    INSERT INTO public.it_schema_add VALUES (1,'n','a');
    t_before := clock_timestamp();
    ALTER TABLE public.it_schema_add ADD COLUMN discount numeric DEFAULT 0;
    UPDATE public.it_schema_add SET discount=10 WHERE id=1;
    PERFORM flashback_restore('public.it_schema_add', t_before);
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='it_schema_add' AND column_name='discount') THEN
      RAISE EXCEPTION 'discount should not exist at old time';
    END IF;
END;
$tv$;
