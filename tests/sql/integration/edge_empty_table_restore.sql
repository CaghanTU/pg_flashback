DO $tv$
DECLARE t_before timestamptz;
BEGIN
    DROP TABLE IF EXISTS public.it_edge_empty;
    CREATE TABLE public.it_edge_empty (id int primary key, v text);
    PERFORM flashback_track('public.it_edge_empty');
    PERFORM flashback_test_attach_capture_trigger('public.it_edge_empty'::regclass);
    t_before := clock_timestamp();
    INSERT INTO public.it_edge_empty VALUES (1,'x');
    PERFORM flashback_restore('public.it_edge_empty', t_before);
    IF EXISTS (SELECT 1 FROM public.it_edge_empty) THEN RAISE EXCEPTION 'empty table restore failed'; END IF;
END;
$tv$;
