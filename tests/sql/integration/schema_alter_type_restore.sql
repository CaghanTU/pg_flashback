DO $tv$
DECLARE t_before timestamptz;
BEGIN
    DROP TABLE IF EXISTS public.it_schema_type;
    CREATE TABLE public.it_schema_type (id int primary key, amount int);
    PERFORM flashback_track('public.it_schema_type');
    PERFORM flashback_test_attach_capture_trigger('public.it_schema_type'::regclass);
    INSERT INTO public.it_schema_type VALUES (1, 10);
    t_before := clock_timestamp();
    ALTER TABLE public.it_schema_type ALTER COLUMN amount TYPE numeric USING amount::numeric;
    UPDATE public.it_schema_type SET amount=12.5 WHERE id=1;
    PERFORM flashback_restore('public.it_schema_type', t_before);
    IF (SELECT data_type FROM information_schema.columns WHERE table_schema='public' AND table_name='it_schema_type' AND column_name='amount') <> 'integer' THEN
      RAISE EXCEPTION 'amount should be integer at old time';
    END IF;
END;
$tv$;
