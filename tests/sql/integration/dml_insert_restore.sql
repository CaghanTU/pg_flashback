DO $tv$
DECLARE t_before timestamptz;
BEGIN
    DROP TABLE IF EXISTS public.it_dml_insert;
    CREATE TABLE public.it_dml_insert (id int primary key, name text, status text);
    PERFORM flashback_track('public.it_dml_insert');
    PERFORM flashback_test_attach_capture_trigger('public.it_dml_insert'::regclass);
    t_before := clock_timestamp();
    INSERT INTO public.it_dml_insert VALUES (1, 'n1', 'active');
    PERFORM flashback_restore('public.it_dml_insert', t_before);
    IF EXISTS (SELECT 1 FROM public.it_dml_insert) THEN
        RAISE EXCEPTION 'insert restore failed';
    END IF;
END;
$tv$;
