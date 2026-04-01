DO $tv$
DECLARE t_before timestamptz;
BEGIN
    CREATE TABLE IF NOT EXISTS public.it_fk_parent (id int primary key, status text);
    CREATE TABLE IF NOT EXISTS public.it_fk_child (id int primary key, parent_id int references public.it_fk_parent(id), qty int);
    TRUNCATE TABLE public.it_fk_child, public.it_fk_parent;
    INSERT INTO public.it_fk_parent VALUES (1,'p0');
    INSERT INTO public.it_fk_child VALUES (10,1,1);
    PERFORM flashback_track('public.it_fk_parent');
    PERFORM flashback_test_attach_capture_trigger('public.it_fk_parent'::regclass);
    PERFORM flashback_track('public.it_fk_child');
    PERFORM flashback_test_attach_capture_trigger('public.it_fk_child'::regclass);
    t_before := clock_timestamp();
    PERFORM set_config('session_replication_role', 'replica', true);
    PERFORM flashback_restore('public.it_fk_child', t_before);
    PERFORM flashback_restore('public.it_fk_parent', t_before);
    PERFORM set_config('session_replication_role', 'origin', true);
    IF NOT EXISTS (SELECT 1 FROM public.it_fk_parent WHERE id=1 AND status='p0') THEN RAISE EXCEPTION 'fk parent failed'; END IF;
    IF NOT EXISTS (SELECT 1 FROM public.it_fk_child WHERE id=10 AND qty=1) THEN RAISE EXCEPTION 'fk child failed'; END IF;
END;
$tv$;
