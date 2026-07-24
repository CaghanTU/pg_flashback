DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/2000'::pg_lsn;
    v_count  bigint;
    v_name   text;
    v_boot_drop jsonb;
    v_tracking_drop bigint;
    v_stream_drop bigint;
    v_gen_drop bigint;
    v_drop_lsn pg_lsn := '0/3000'::pg_lsn;
    v_drop_count bigint;
    v_exc boolean := false;
    v_xid bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_fb_query;
    CREATE TABLE public.it_fb_query (id int primary key, name text, price numeric);
    INSERT INTO public.it_fb_query VALUES (1, 'alpha', 10), (2, 'beta', 20), (3, 'gamma', 30);

    SELECT flashback_test_bootstrap_lifecycle('public.it_fb_query') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;
    v_point_lsn := (v_boot->>'boundary_lsn')::pg_lsn;

    UPDATE public.it_fb_query SET price = 999 WHERE id = 1;
    DELETE FROM public.it_fb_query WHERE id = 2;
    INSERT INTO public.it_fb_query VALUES (4, 'delta', 40);
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/4000'::pg_lsn,
        clock_timestamp(),
        951001,
        jsonb_build_array(
            jsonb_build_object('op', 'UPDATE', 'old', '{"id":1,"name":"alpha","price":10}'::jsonb, 'new', '{"id":1,"name":"alpha","price":999}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":2,"name":"beta","price":20}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":4,"name":"delta","price":40}'::jsonb)
        )
    );

    SELECT count(*) INTO v_count
    FROM flashback_query_lsn('public.it_fb_query', v_point_lsn)
         AS t(id int, name text, price numeric);

    IF v_count <> 3 THEN
        RAISE EXCEPTION 'flashback_query_lsn: expected 3 rows, got %', v_count;
    END IF;

    SELECT t.name INTO v_name
    FROM flashback_query_lsn('public.it_fb_query', v_point_lsn)
         AS t(id int, name text, price numeric)
    WHERE id = 1;

    IF v_name <> 'alpha' THEN
        RAISE EXCEPTION 'flashback_query_lsn: expected alpha, got %', v_name;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM public.it_fb_query WHERE id = 4) THEN
        RAISE EXCEPTION 'flashback_query_lsn should not modify the actual table';
    END IF;

    DROP TABLE IF EXISTS public.it_fb_query_drop;
    CREATE TABLE public.it_fb_query_drop (id int primary key, val text);
    SELECT flashback_test_bootstrap_lifecycle('public.it_fb_query_drop') INTO v_boot_drop;
    v_tracking_drop := (v_boot_drop->>'tracking_id')::bigint;
    v_stream_drop := (v_boot_drop->>'stream_id')::bigint;
    v_gen_drop := (v_boot_drop->>'generation_id')::bigint;

    INSERT INTO public.it_fb_query_drop VALUES (1, 'exists');
    PERFORM flashback_test_inject_commit(
        v_tracking_drop,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        951002,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"val":"exists"}'::jsonb)
        )
    );

    PERFORM flashback_capture_drop_dependency_manifest(
        'public', 'it_fb_query_drop', false
    );
    v_xid := (txid_current() % 4294967296)::bigint;
    DROP TABLE public.it_fb_query_drop;
    PERFORM flashback_test_inject_ddl_commit(
        v_tracking_drop,
        v_drop_lsn,
        clock_timestamp(),
        v_xid,
        'DROP'
    );
    PERFORM flashback_bind_drop_dependency_manifests();

    SELECT count(*) INTO v_drop_count
    FROM flashback_query_lsn('public.it_fb_query_drop', v_drop_lsn)
         AS t(id int, val text);

    IF v_drop_count <> 0 THEN
        RAISE EXCEPTION 'flashback_query_lsn after DROP: expected 0 rows, got %', v_drop_count;
    END IF;

    DROP TABLE IF EXISTS public.it_fb_query_drop;

    SELECT count(*) INTO v_count
    FROM flashback_query_lsn('public.it_fb_query', v_point_lsn)
         AS t(id int, name text, price numeric);

    IF v_count <> 3 THEN
        RAISE EXCEPTION 'flashback_query_lsn schema-evolution check: expected 3, got %', v_count;
    END IF;

    BEGIN
        SELECT t.id INTO v_count
        FROM flashback_query_lsn('public.it_fb_query', v_point_lsn, 'id = 1; DELETE FROM pg_class')
             AS t(id int, name text, price numeric) LIMIT 1;
    EXCEPTION WHEN OTHERS THEN
        v_exc := true;
    END;
    IF NOT v_exc THEN
        RAISE EXCEPTION 'flashback_query_lsn: filter_clause should have raised an exception';
    END IF;
END;
$tv$;
