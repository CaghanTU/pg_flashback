DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_boundary_lsn pg_lsn;
BEGIN
    DROP TABLE IF EXISTS public.it_edge_nulls;
    CREATE TABLE public.it_edge_nulls (id int primary key, v text, note text);
    INSERT INTO public.it_edge_nulls VALUES (1, NULL, 'n0');

    SELECT flashback_test_bootstrap_lifecycle('public.it_edge_nulls') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;
    v_boundary_lsn := (v_boot->>'boundary_lsn')::pg_lsn;

    UPDATE public.it_edge_nulls SET v='x', note=NULL WHERE id=1;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        927001,
        jsonb_build_array(
            jsonb_build_object(
                'op', 'UPDATE',
                'old', '{"id":1,"v":null,"note":"n0"}'::jsonb,
                'new', '{"id":1,"v":"x","note":null}'::jsonb
            )
        )
    );

    PERFORM flashback_test_restore_lsn('public.it_edge_nulls', v_boundary_lsn);
    IF NOT EXISTS (SELECT 1 FROM public.it_edge_nulls WHERE id=1 AND v IS NULL AND note='n0') THEN
      RAISE EXCEPTION 'null restore failed';
    END IF;
END;
$tv$;
