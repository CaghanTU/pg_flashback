DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_boundary_lsn pg_lsn;
BEGIN
    DROP TABLE IF EXISTS public.it_dml_delete;
    CREATE TABLE public.it_dml_delete (id int primary key, name text, status text);
    INSERT INTO public.it_dml_delete VALUES (1, 'n1', 'new');

    SELECT flashback_test_bootstrap_lifecycle('public.it_dml_delete') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;
    v_boundary_lsn := (v_boot->>'boundary_lsn')::pg_lsn;

    DELETE FROM public.it_dml_delete WHERE id=1;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        922001,
        jsonb_build_array(
            jsonb_build_object(
                'op', 'DELETE',
                'old', '{"id":1,"name":"n1","status":"new"}'::jsonb
            )
        )
    );

    PERFORM flashback_test_restore_lsn('public.it_dml_delete', v_boundary_lsn);
    IF NOT EXISTS (SELECT 1 FROM public.it_dml_delete WHERE id=1) THEN
        RAISE EXCEPTION 'delete restore failed';
    END IF;
END;
$tv$;
