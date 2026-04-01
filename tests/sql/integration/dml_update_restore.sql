DO $tv$
DECLARE t_before timestamptz;
BEGIN
    DROP TABLE IF EXISTS public.it_dml_update;
    CREATE TABLE public.it_dml_update (id int primary key, name text, status text);
    INSERT INTO public.it_dml_update VALUES (1, 'n1', 'new');
    PERFORM flashback_track('public.it_dml_update');
    PERFORM flashback_test_attach_capture_trigger('public.it_dml_update'::regclass);
    t_before := clock_timestamp();
    UPDATE public.it_dml_update SET status='done' WHERE id=1;
    PERFORM flashback_restore('public.it_dml_update', t_before);
    IF NOT EXISTS (SELECT 1 FROM public.it_dml_update WHERE id=1 AND status='new') THEN
        RAISE EXCEPTION 'update restore failed';
    END IF;
END;
$tv$;
