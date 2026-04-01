DO $tv$
DECLARE t_before timestamptz; c bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_dml_batch;
    CREATE TABLE public.it_dml_batch (id int primary key, payload text);
    PERFORM flashback_track('public.it_dml_batch');
    PERFORM flashback_test_attach_capture_trigger('public.it_dml_batch'::regclass);
    t_before := clock_timestamp();
    INSERT INTO public.it_dml_batch SELECT g, repeat('x', 20) FROM generate_series(1,1000) g;
    PERFORM flashback_restore('public.it_dml_batch', t_before);
    SELECT count(*) INTO c FROM public.it_dml_batch;
    IF c <> 0 THEN
        RAISE EXCEPTION 'batch insert restore failed, count=%', c;
    END IF;
END;
$tv$;
