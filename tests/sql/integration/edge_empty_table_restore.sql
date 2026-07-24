DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_boundary_lsn pg_lsn;
BEGIN
    DROP TABLE IF EXISTS public.it_edge_empty;
    CREATE TABLE public.it_edge_empty (id int primary key, v text);

    SELECT flashback_test_bootstrap_lifecycle('public.it_edge_empty') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;
    v_boundary_lsn := (v_boot->>'boundary_lsn')::pg_lsn;

    INSERT INTO public.it_edge_empty VALUES (1,'x');
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        926001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"v":"x"}'::jsonb)
        )
    );

    PERFORM flashback_restore_lsn('public.it_edge_empty', v_boundary_lsn);
    IF EXISTS (SELECT 1 FROM public.it_edge_empty) THEN RAISE EXCEPTION 'empty table restore failed'; END IF;
END;
$tv$;
