DO $tv$
DECLARE t_before timestamptz;
BEGIN
    DROP TABLE IF EXISTS public.it_dml_delete;
    CREATE TABLE public.it_dml_delete (id int primary key, name text, status text);
    INSERT INTO public.it_dml_delete VALUES (1, 'n1', 'new');
    PERFORM flashback_track('public.it_dml_delete');
    PERFORM flashback_test_attach_capture_trigger('public.it_dml_delete'::regclass);
    t_before := clock_timestamp();
    DELETE FROM public.it_dml_delete WHERE id=1;
    PERFORM flashback_restore('public.it_dml_delete', t_before);
    IF NOT EXISTS (SELECT 1 FROM public.it_dml_delete WHERE id=1) THEN
        RAISE EXCEPTION 'delete restore failed';
    END IF;
END;
$tv$;
