DO $tv$
DECLARE t_before timestamptz;
BEGIN
    DROP TABLE IF EXISTS public.it_edge_partial;
    CREATE TABLE public.it_edge_partial (id int primary key, v text);
    INSERT INTO public.it_edge_partial VALUES (1,'x');
    PERFORM flashback_track('public.it_edge_partial');
    PERFORM flashback_test_attach_capture_trigger('public.it_edge_partial'::regclass);
    t_before := clock_timestamp();
    DROP TABLE public.it_edge_partial;
    PERFORM flashback_restore('public.it_edge_partial', t_before);
    IF to_regclass('public.it_edge_partial') IS NULL THEN
      RAISE EXCEPTION 'partial drop restore failed';
    END IF;
END;
$tv$;
