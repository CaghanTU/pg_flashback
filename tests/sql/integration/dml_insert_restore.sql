DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_boundary_lsn pg_lsn;
BEGIN
    DROP TABLE IF EXISTS public.it_dml_insert;
    CREATE TABLE public.it_dml_insert (id int primary key, name text, status text);

    SELECT flashback_test_bootstrap_lifecycle('public.it_dml_insert') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;
    v_boundary_lsn := (v_boot->>'boundary_lsn')::pg_lsn;

    INSERT INTO public.it_dml_insert VALUES (1, 'n1', 'active');
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        920001,
        jsonb_build_array(
            jsonb_build_object(
                'op', 'INSERT',
                'new', '{"id":1,"name":"n1","status":"active"}'::jsonb
            )
        )
    );

    PERFORM flashback_restore_lsn('public.it_dml_insert', v_boundary_lsn);
    IF EXISTS (SELECT 1 FROM public.it_dml_insert) THEN
        RAISE EXCEPTION 'insert restore failed';
    END IF;
END;
$tv$;
