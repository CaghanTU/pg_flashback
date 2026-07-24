DO $tv$
DECLARE
    v_boot_a jsonb;
    v_boot_b jsonb;
    v_tracking_a bigint;
    v_tracking_b bigint;
    v_boundary_a pg_lsn;
    v_boundary_b pg_lsn;
    v_xid bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_multi_drop_b;
    DROP TABLE IF EXISTS public.it_multi_drop_a;
    CREATE TABLE public.it_multi_drop_a (id int primary key, v text);
    CREATE TABLE public.it_multi_drop_b (id int primary key, v text);
    INSERT INTO public.it_multi_drop_a VALUES (1, 'a0');
    INSERT INTO public.it_multi_drop_b VALUES (1, 'b0');

    SELECT flashback_test_bootstrap_lifecycle('public.it_multi_drop_a') INTO v_boot_a;
    v_tracking_a := (v_boot_a->>'tracking_id')::bigint;
    v_boundary_a := (v_boot_a->>'boundary_lsn')::pg_lsn;
    SELECT flashback_test_bootstrap_lifecycle('public.it_multi_drop_b') INTO v_boot_b;
    v_tracking_b := (v_boot_b->>'tracking_id')::bigint;
    v_boundary_b := (v_boot_b->>'boundary_lsn')::pg_lsn;

    PERFORM flashback_capture_drop_dependency_manifest('public', 'it_multi_drop_a', false);
    v_xid := (txid_current() % 4294967296)::bigint;
    DROP TABLE public.it_multi_drop_a;
    UPDATE public.it_multi_drop_b SET v = 'b1' WHERE id = 1;

    PERFORM flashback_test_inject_ddl_commit(
        v_tracking_a,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        v_xid,
        'DROP'
    );
    PERFORM flashback_test_inject_commit(
        v_tracking_b,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        v_xid + 1,
        jsonb_build_array(
            jsonb_build_object(
                'op', 'UPDATE',
                'old', '{"id":1,"v":"b0"}'::jsonb,
                'new', '{"id":1,"v":"b1"}'::jsonb
            )
        )
    );
    PERFORM flashback_bind_drop_dependency_manifests();

    PERFORM flashback_test_restore_lsn('public.it_multi_drop_a', v_boundary_a);
    PERFORM flashback_test_restore_lsn('public.it_multi_drop_b', v_boundary_b);
    IF to_regclass('public.it_multi_drop_a') IS NULL THEN
        RAISE EXCEPTION 'dropped table not restored';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM public.it_multi_drop_b WHERE id = 1 AND v = 'b0'
    ) THEN
        RAISE EXCEPTION 'updated table not rolled back';
    END IF;
END;
$tv$;
