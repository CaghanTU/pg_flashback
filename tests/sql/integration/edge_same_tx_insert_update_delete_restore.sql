DROP TABLE IF EXISTS public.it_edge_same_tx;
CREATE TABLE public.it_edge_same_tx (id int primary key, status text);
SELECT flashback_track('public.it_edge_same_tx');
SELECT flashback_test_attach_capture_trigger('public.it_edge_same_tx'::regclass);

DO $tv$
DECLARE t_before timestamptz;
BEGIN
    t_before := clock_timestamp();
    INSERT INTO public.it_edge_same_tx VALUES (1, 'a');
    UPDATE public.it_edge_same_tx SET status='b' WHERE id=1;
    DELETE FROM public.it_edge_same_tx WHERE id=1;
    PERFORM flashback_restore('public.it_edge_same_tx', t_before);
    IF EXISTS (SELECT 1 FROM public.it_edge_same_tx) THEN
      RAISE EXCEPTION 'same tx I/U/D restore failed';
    END IF;
END;
$tv$;
