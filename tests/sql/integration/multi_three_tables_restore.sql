DO $tv$
DECLARE
    v_boot_a jsonb;
    v_boot_b jsonb;
    v_boot_c jsonb;
    v_tracking_a bigint;
    v_tracking_b bigint;
    v_tracking_c bigint;
    v_boundary_a pg_lsn;
    v_boundary_b pg_lsn;
    v_boundary_c pg_lsn;
BEGIN
    DROP TABLE IF EXISTS public.it_m3_c;
    DROP TABLE IF EXISTS public.it_m3_b;
    DROP TABLE IF EXISTS public.it_m3_a;
    CREATE TABLE public.it_m3_a (id int primary key, v text);
    CREATE TABLE public.it_m3_b (id int primary key, v text);
    CREATE TABLE public.it_m3_c (id int primary key, v text);
    INSERT INTO public.it_m3_a VALUES (1, 'a0');
    INSERT INTO public.it_m3_b VALUES (1, 'b0');
    INSERT INTO public.it_m3_c VALUES (1, 'c0');

    SELECT flashback_test_bootstrap_lifecycle('public.it_m3_a') INTO v_boot_a;
    v_tracking_a := (v_boot_a->>'tracking_id')::bigint;
    v_boundary_a := (v_boot_a->>'boundary_lsn')::pg_lsn;
    SELECT flashback_test_bootstrap_lifecycle('public.it_m3_b') INTO v_boot_b;
    v_tracking_b := (v_boot_b->>'tracking_id')::bigint;
    v_boundary_b := (v_boot_b->>'boundary_lsn')::pg_lsn;
    SELECT flashback_test_bootstrap_lifecycle('public.it_m3_c') INTO v_boot_c;
    v_tracking_c := (v_boot_c->>'tracking_id')::bigint;
    v_boundary_c := (v_boot_c->>'boundary_lsn')::pg_lsn;

    UPDATE public.it_m3_a SET v = 'a1' WHERE id = 1;
    UPDATE public.it_m3_b SET v = 'b1' WHERE id = 1;
    UPDATE public.it_m3_c SET v = 'c1' WHERE id = 1;
    PERFORM flashback_test_inject_commit(
        v_tracking_a, '0/2000'::pg_lsn, clock_timestamp(), 938001,
        jsonb_build_array(
            jsonb_build_object(
                'op', 'UPDATE',
                'old', '{"id":1,"v":"a0"}'::jsonb,
                'new', '{"id":1,"v":"a1"}'::jsonb
            )
        )
    );
    PERFORM flashback_test_inject_commit(
        v_tracking_b, '0/3000'::pg_lsn, clock_timestamp(), 938002,
        jsonb_build_array(
            jsonb_build_object(
                'op', 'UPDATE',
                'old', '{"id":1,"v":"b0"}'::jsonb,
                'new', '{"id":1,"v":"b1"}'::jsonb
            )
        )
    );
    PERFORM flashback_test_inject_commit(
        v_tracking_c, '0/4000'::pg_lsn, clock_timestamp(), 938003,
        jsonb_build_array(
            jsonb_build_object(
                'op', 'UPDATE',
                'old', '{"id":1,"v":"c0"}'::jsonb,
                'new', '{"id":1,"v":"c1"}'::jsonb
            )
        )
    );

    PERFORM flashback_restore_lsn('public.it_m3_a', v_boundary_a);
    PERFORM flashback_restore_lsn('public.it_m3_b', v_boundary_b);
    PERFORM flashback_restore_lsn('public.it_m3_c', v_boundary_c);
    IF NOT EXISTS (SELECT 1 FROM public.it_m3_a WHERE v = 'a0') THEN
        RAISE EXCEPTION 'm3 a failed';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.it_m3_b WHERE v = 'b0') THEN
        RAISE EXCEPTION 'm3 b failed';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.it_m3_c WHERE v = 'c0') THEN
        RAISE EXCEPTION 'm3 c failed';
    END IF;
END;
$tv$;
