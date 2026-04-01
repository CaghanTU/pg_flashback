DO $tv$
DECLARE t_before timestamptz;
BEGIN
    DROP TABLE IF EXISTS public.it_edge_nulls;
    CREATE TABLE public.it_edge_nulls (id int primary key, v text, note text);
    INSERT INTO public.it_edge_nulls VALUES (1, NULL, 'n0');
    PERFORM flashback_track('public.it_edge_nulls');
    PERFORM flashback_test_attach_capture_trigger('public.it_edge_nulls'::regclass);
    t_before := clock_timestamp();
    UPDATE public.it_edge_nulls SET v='x', note=NULL WHERE id=1;
    PERFORM flashback_restore('public.it_edge_nulls', t_before);
    IF NOT EXISTS (SELECT 1 FROM public.it_edge_nulls WHERE id=1 AND v IS NULL AND note='n0') THEN
      RAISE EXCEPTION 'null restore failed';
    END IF;
END;
$tv$;
