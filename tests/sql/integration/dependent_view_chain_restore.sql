-- Test: A chained view dependency survives flashback_restore_lsn().
DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/2000'::pg_lsn;
    v_cnt bigint;
    v_exist1 bool;
    v_exist2 bool;
BEGIN
    DROP TABLE IF EXISTS public.it_vchain_base CASCADE;
    CREATE TABLE public.it_vchain_base (
        id      int PRIMARY KEY,
        dept    text,
        salary  numeric
    );

    CREATE VIEW public.it_vchain_view1 AS
        SELECT id, dept, salary
        FROM public.it_vchain_base
        WHERE salary > 50000;

    CREATE VIEW public.it_vchain_view2 AS
        SELECT dept, count(*) AS headcount, avg(salary) AS avg_salary
        FROM public.it_vchain_view1
        GROUP BY dept;

    SELECT flashback_test_bootstrap_lifecycle('public.it_vchain_base') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO public.it_vchain_base VALUES
        (1, 'Eng',   90000),
        (2, 'Eng',   75000),
        (3, 'Sales', 45000),
        (4, 'Sales', 60000);
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        v_point_lsn,
        clock_timestamp(),
        954001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"dept":"Eng","salary":90000}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":2,"dept":"Eng","salary":75000}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":3,"dept":"Sales","salary":45000}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":4,"dept":"Sales","salary":60000}'::jsonb)
        )
    );

    DELETE FROM public.it_vchain_base;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        954002,
        jsonb_build_array(
            jsonb_build_object('op', 'DELETE', 'old', '{"id":1,"dept":"Eng","salary":90000}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":2,"dept":"Eng","salary":75000}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":3,"dept":"Sales","salary":45000}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":4,"dept":"Sales","salary":60000}'::jsonb)
        )
    );

    PERFORM flashback_restore_lsn('public.it_vchain_base', v_point_lsn);

    SELECT count(*) INTO v_cnt FROM public.it_vchain_base;
    IF v_cnt <> 4 THEN
        RAISE EXCEPTION 'base table: expected 4 rows, got %', v_cnt;
    END IF;

    SELECT EXISTS (
        SELECT 1 FROM pg_views
        WHERE schemaname = 'public' AND viewname = 'it_vchain_view1'
    ) INTO v_exist1;

    IF NOT v_exist1 THEN
        RAISE EXCEPTION 'it_vchain_view1 was not recreated';
    END IF;

    SELECT EXISTS (
        SELECT 1 FROM pg_views
        WHERE schemaname = 'public' AND viewname = 'it_vchain_view2'
    ) INTO v_exist2;

    IF NOT v_exist2 THEN
        RAISE EXCEPTION 'it_vchain_view2 was not recreated';
    END IF;

    SELECT count(*) INTO v_cnt FROM public.it_vchain_view2;
    IF v_cnt <> 2 THEN
        RAISE EXCEPTION 'view chain broken: it_vchain_view2 returned % rows, expected 2', v_cnt;
    END IF;

    DROP VIEW IF EXISTS public.it_vchain_view2;
    DROP VIEW IF EXISTS public.it_vchain_view1;
    DROP TABLE IF EXISTS public.it_vchain_base CASCADE;
END;
$tv$;
