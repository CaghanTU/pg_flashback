DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_boundary_lsn pg_lsn;
    c bigint;
    v_events jsonb;
BEGIN
    DROP TABLE IF EXISTS public.it_dml_batch;
    CREATE TABLE public.it_dml_batch (id int primary key, payload text);

    SELECT flashback_test_bootstrap_lifecycle('public.it_dml_batch') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;
    v_boundary_lsn := (v_boot->>'boundary_lsn')::pg_lsn;

    INSERT INTO public.it_dml_batch
    SELECT gs.id, repeat('x', 20) FROM generate_series(1, 1000) AS gs(id);

    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'op', 'INSERT',
            'new', jsonb_build_object('id', gs.id, 'payload', repeat('x', 20))
        )
        ORDER BY gs.id
    ), '[]'::jsonb)
      INTO v_events
    FROM generate_series(1, 1000) AS gs(id);

    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        925001,
        v_events
    );

    PERFORM flashback_test_restore_lsn('public.it_dml_batch', v_boundary_lsn);
    SELECT count(*) INTO c FROM public.it_dml_batch;
    IF c <> 0 THEN
        RAISE EXCEPTION 'batch insert restore failed, count=%', c;
    END IF;
END;
$tv$;
