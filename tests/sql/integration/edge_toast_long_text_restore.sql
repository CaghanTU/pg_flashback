DO $tv$
DECLARE t_before timestamptz; longtxt text;
BEGIN
    longtxt := repeat('L', 200000);
    DROP TABLE IF EXISTS public.it_edge_toast;
    CREATE TABLE public.it_edge_toast (id int primary key, payload text);
    EXECUTE format($q$INSERT INTO public.it_edge_toast VALUES (1, %L)$q$, longtxt);
    PERFORM flashback_track('public.it_edge_toast');
    PERFORM flashback_test_attach_capture_trigger('public.it_edge_toast'::regclass);
    t_before := clock_timestamp();
    UPDATE public.it_edge_toast SET payload='short' WHERE id=1;
    PERFORM flashback_restore('public.it_edge_toast', t_before);
    IF (SELECT length(payload) FROM public.it_edge_toast WHERE id=1) <> 200000 THEN
      RAISE EXCEPTION 'toast restore failed';
    END IF;
END;
$tv$;
