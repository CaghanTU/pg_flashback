DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_boundary_lsn pg_lsn;
    c bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_dml_update_all;
    CREATE TABLE public.it_dml_update_all (id int primary key, status text);
    INSERT INTO public.it_dml_update_all VALUES (1, 'a'), (2, 'a'), (3, 'a');

    SELECT flashback_test_bootstrap_lifecycle('public.it_dml_update_all') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;
    v_boundary_lsn := (v_boot->>'boundary_lsn')::pg_lsn;

    UPDATE public.it_dml_update_all SET status = 'z';
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        927001,
        jsonb_build_array(
            jsonb_build_object(
                'op', 'UPDATE',
                'old', '{"id":1,"status":"a"}'::jsonb,
                'new', '{"id":1,"status":"z"}'::jsonb
            ),
            jsonb_build_object(
                'op', 'UPDATE',
                'old', '{"id":2,"status":"a"}'::jsonb,
                'new', '{"id":2,"status":"z"}'::jsonb
            ),
            jsonb_build_object(
                'op', 'UPDATE',
                'old', '{"id":3,"status":"a"}'::jsonb,
                'new', '{"id":3,"status":"z"}'::jsonb
            )
        )
    );

    PERFORM flashback_test_restore_lsn('public.it_dml_update_all', v_boundary_lsn);
    SELECT count(*) INTO c FROM public.it_dml_update_all WHERE status = 'a';
    IF c <> 3 THEN
        RAISE EXCEPTION 'update all restore failed';
    END IF;
END;
$tv$;
