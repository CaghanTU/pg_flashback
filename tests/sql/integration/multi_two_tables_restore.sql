DO $tv$
DECLARE
    v_boot_a jsonb;
    v_boot_b jsonb;
    v_tracking_a bigint;
    v_tracking_b bigint;
    v_boundary_a pg_lsn;
    v_boundary_b pg_lsn;
BEGIN
    DROP TABLE IF EXISTS public.it_multi_a;
    DROP TABLE IF EXISTS public.it_multi_b;
    CREATE TABLE public.it_multi_a (id int primary key, v text);
    CREATE TABLE public.it_multi_b (id int primary key, v text);
    INSERT INTO public.it_multi_a VALUES (1, 'a0');
    INSERT INTO public.it_multi_b VALUES (1, 'b0');

    SELECT flashback_test_bootstrap_lifecycle('public.it_multi_a') INTO v_boot_a;
    v_tracking_a := (v_boot_a->>'tracking_id')::bigint;
    v_boundary_a := (v_boot_a->>'boundary_lsn')::pg_lsn;
    SELECT flashback_test_bootstrap_lifecycle('public.it_multi_b') INTO v_boot_b;
    v_tracking_b := (v_boot_b->>'tracking_id')::bigint;
    v_boundary_b := (v_boot_b->>'boundary_lsn')::pg_lsn;

    UPDATE public.it_multi_a SET v = 'a1' WHERE id = 1;
    UPDATE public.it_multi_b SET v = 'b1' WHERE id = 1;
    PERFORM flashback_test_inject_commit(
        v_tracking_a,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        929001,
        jsonb_build_array(
            jsonb_build_object(
                'op', 'UPDATE',
                'old', '{"id":1,"v":"a0"}'::jsonb,
                'new', '{"id":1,"v":"a1"}'::jsonb
            )
        )
    );
    PERFORM flashback_test_inject_commit(
        v_tracking_b,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        929002,
        jsonb_build_array(
            jsonb_build_object(
                'op', 'UPDATE',
                'old', '{"id":1,"v":"b0"}'::jsonb,
                'new', '{"id":1,"v":"b1"}'::jsonb
            )
        )
    );

    PERFORM flashback_test_restore_lsn('public.it_multi_a', v_boundary_a);
    PERFORM flashback_test_restore_lsn('public.it_multi_b', v_boundary_b);
    IF NOT EXISTS (SELECT 1 FROM public.it_multi_a WHERE id = 1 AND v = 'a0') THEN
        RAISE EXCEPTION 'multi a failed';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.it_multi_b WHERE id = 1 AND v = 'b0') THEN
        RAISE EXCEPTION 'multi b failed';
    END IF;
END;
$tv$;
