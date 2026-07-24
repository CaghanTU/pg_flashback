DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_boundary_lsn pg_lsn;
    i int;
    v_events jsonb := '[]'::jsonb;
    v_old text := 'v0';
    v_new text;
BEGIN
    DROP TABLE IF EXISTS public.it_dml_ten_updates;
    CREATE TABLE public.it_dml_ten_updates (id int primary key, status text);
    INSERT INTO public.it_dml_ten_updates VALUES (1, 'v0');

    SELECT flashback_test_bootstrap_lifecycle('public.it_dml_ten_updates') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;
    v_boundary_lsn := (v_boot->>'boundary_lsn')::pg_lsn;

    FOR i IN 1..10 LOOP
        v_new := 'v' || i::text;
        EXECUTE format($q$UPDATE public.it_dml_ten_updates SET status=%L WHERE id=1$q$, v_new);
        v_events := v_events || jsonb_build_array(
            jsonb_build_object(
                'op', 'UPDATE',
                'old', jsonb_build_object('id', 1, 'status', v_old),
                'new', jsonb_build_object('id', 1, 'status', v_new)
            )
        );
        v_old := v_new;
    END LOOP;

    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        924001,
        v_events
    );

    PERFORM flashback_test_restore_lsn('public.it_dml_ten_updates', v_boundary_lsn);
    IF NOT EXISTS (SELECT 1 FROM public.it_dml_ten_updates WHERE id=1 AND status='v0') THEN
        RAISE EXCEPTION 'ten updates restore failed';
    END IF;
END;
$tv$;
