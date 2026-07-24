DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_boundary_lsn pg_lsn;
BEGIN
    DROP TABLE IF EXISTS public.it_dml_update;
    CREATE TABLE public.it_dml_update (id int primary key, name text, status text);
    INSERT INTO public.it_dml_update VALUES (1, 'n1', 'new');

    SELECT flashback_test_bootstrap_lifecycle('public.it_dml_update') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;
    v_boundary_lsn := (v_boot->>'boundary_lsn')::pg_lsn;

    UPDATE public.it_dml_update SET status='done' WHERE id=1;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        921001,
        jsonb_build_array(
            jsonb_build_object(
                'op', 'UPDATE',
                'old', '{"id":1,"name":"n1","status":"new"}'::jsonb,
                'new', '{"id":1,"name":"n1","status":"done"}'::jsonb
            )
        )
    );

    PERFORM flashback_restore_lsn('public.it_dml_update', v_boundary_lsn);
    IF NOT EXISTS (SELECT 1 FROM public.it_dml_update WHERE id=1 AND status='new') THEN
        RAISE EXCEPTION 'update restore failed';
    END IF;
END;
$tv$;
