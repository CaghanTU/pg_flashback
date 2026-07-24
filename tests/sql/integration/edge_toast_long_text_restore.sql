DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_boundary_lsn pg_lsn;
    longtxt text;
BEGIN
    longtxt := repeat('L', 200000);
    DROP TABLE IF EXISTS public.it_edge_toast;
    CREATE TABLE public.it_edge_toast (id int primary key, payload text);
    EXECUTE format($q$INSERT INTO public.it_edge_toast VALUES (1, %L)$q$, longtxt);

    SELECT flashback_test_bootstrap_lifecycle('public.it_edge_toast') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;
    v_boundary_lsn := (v_boot->>'boundary_lsn')::pg_lsn;

    UPDATE public.it_edge_toast SET payload='short' WHERE id=1;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        928001,
        jsonb_build_array(
            jsonb_build_object(
                'op', 'UPDATE',
                'old', jsonb_build_object('id', 1, 'payload', longtxt),
                'new', '{"id":1,"payload":"short"}'::jsonb
            )
        )
    );

    PERFORM flashback_restore_lsn('public.it_edge_toast', v_boundary_lsn);
    IF (SELECT length(payload) FROM public.it_edge_toast WHERE id=1) <> 200000 THEN
      RAISE EXCEPTION 'toast restore failed';
    END IF;
END;
$tv$;
