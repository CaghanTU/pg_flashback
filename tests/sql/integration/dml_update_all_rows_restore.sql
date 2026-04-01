DO $tv$
DECLARE t_before timestamptz; c bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_dml_update_all;
    CREATE TABLE public.it_dml_update_all (id int primary key, status text);
    INSERT INTO public.it_dml_update_all VALUES (1,'a'),(2,'a'),(3,'a');
    PERFORM flashback_track('public.it_dml_update_all');
    PERFORM flashback_test_attach_capture_trigger('public.it_dml_update_all'::regclass);
    t_before := clock_timestamp();
    UPDATE public.it_dml_update_all SET status='z';
    PERFORM flashback_restore('public.it_dml_update_all', t_before);
    SELECT count(*) INTO c FROM public.it_dml_update_all WHERE status='a';
    IF c <> 3 THEN
        RAISE EXCEPTION 'update all restore failed';
    END IF;
END;
$tv$;
