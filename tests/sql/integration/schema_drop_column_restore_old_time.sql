DO $tv$
DECLARE t_before timestamptz;
BEGIN
    DROP TABLE IF EXISTS public.it_schema_dropcol;
    CREATE TABLE public.it_schema_dropcol (id int primary key, name text, status text);
    PERFORM flashback_track('public.it_schema_dropcol');
    PERFORM flashback_test_attach_capture_trigger('public.it_schema_dropcol'::regclass);
    INSERT INTO public.it_schema_dropcol VALUES (1,'n','a');
    t_before := clock_timestamp();
    ALTER TABLE public.it_schema_dropcol DROP COLUMN status;
    UPDATE public.it_schema_dropcol SET name='x' WHERE id=1;
    PERFORM flashback_restore('public.it_schema_dropcol', t_before);
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='it_schema_dropcol' AND column_name='status') THEN
      RAISE EXCEPTION 'status should be present at old time';
    END IF;
END;
$tv$;
