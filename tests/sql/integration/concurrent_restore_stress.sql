-- Test: concurrent restore stress — verify advisory lock prevents conflicts
DO $tv$
DECLARE
    v_boot_a jsonb;
    v_boot_b jsonb;
    v_boot_c jsonb;
    v_tracking_a bigint;
    v_tracking_b bigint;
    v_tracking_c bigint;
    v_point_lsn pg_lsn := '0/2000'::pg_lsn;
    v_mid_lsn pg_lsn := '0/8000'::pg_lsn;
    v_cnt bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_conc_a CASCADE;
    DROP TABLE IF EXISTS public.it_conc_b CASCADE;
    DROP TABLE IF EXISTS public.it_conc_c CASCADE;

    CREATE TABLE public.it_conc_a (id int PRIMARY KEY, val text);
    CREATE TABLE public.it_conc_b (id int PRIMARY KEY, val text);
    CREATE TABLE public.it_conc_c (id int PRIMARY KEY, val text);

    SELECT public.flashback_test_bootstrap_lifecycle('public.it_conc_a') INTO v_boot_a;
    v_tracking_a := (v_boot_a->>'tracking_id')::bigint;
    SELECT public.flashback_test_bootstrap_lifecycle('public.it_conc_b') INTO v_boot_b;
    v_tracking_b := (v_boot_b->>'tracking_id')::bigint;
    SELECT public.flashback_test_bootstrap_lifecycle('public.it_conc_c') INTO v_boot_c;
    v_tracking_c := (v_boot_c->>'tracking_id')::bigint;

    INSERT INTO public.it_conc_a VALUES (1, 'a1'), (2, 'a2'), (3, 'a3');
    INSERT INTO public.it_conc_b VALUES (10, 'b1'), (20, 'b2');
    INSERT INTO public.it_conc_c VALUES (100, 'c1'), (200, 'c2'), (300, 'c3'), (400, 'c4');

    PERFORM public.flashback_test_inject_commit(
        v_tracking_a, v_point_lsn, clock_timestamp(), 959001,
        jsonb_build_array(
            jsonb_build_object('op','INSERT','new','{"id":1,"val":"a1"}'::jsonb),
            jsonb_build_object('op','INSERT','new','{"id":2,"val":"a2"}'::jsonb),
            jsonb_build_object('op','INSERT','new','{"id":3,"val":"a3"}'::jsonb)
        )
    );
    PERFORM public.flashback_test_inject_commit(
        v_tracking_b, '0/3000'::pg_lsn, clock_timestamp(), 959002,
        jsonb_build_array(
            jsonb_build_object('op','INSERT','new','{"id":10,"val":"b1"}'::jsonb),
            jsonb_build_object('op','INSERT','new','{"id":20,"val":"b2"}'::jsonb)
        )
    );
    PERFORM public.flashback_test_inject_commit(
        v_tracking_c, '0/4000'::pg_lsn, clock_timestamp(), 959003,
        jsonb_build_array(
            jsonb_build_object('op','INSERT','new','{"id":100,"val":"c1"}'::jsonb),
            jsonb_build_object('op','INSERT','new','{"id":200,"val":"c2"}'::jsonb),
            jsonb_build_object('op','INSERT','new','{"id":300,"val":"c3"}'::jsonb),
            jsonb_build_object('op','INSERT','new','{"id":400,"val":"c4"}'::jsonb)
        )
    );

    DELETE FROM public.it_conc_a;
    DELETE FROM public.it_conc_b;
    UPDATE public.it_conc_c SET val = 'DESTROYED';

    PERFORM public.flashback_test_restore_lsn('public.it_conc_a', '0/2000'::pg_lsn);
    PERFORM public.flashback_test_resolve_post_restore_boundary(v_tracking_a, '0/4500'::pg_lsn);
    PERFORM public.flashback_finalize_recover_operations();
    SELECT count(*) INTO v_cnt FROM public.it_conc_a;
    IF v_cnt <> 3 THEN
        RAISE EXCEPTION 'conc_a: expected 3 rows, got %', v_cnt;
    END IF;

    PERFORM public.flashback_test_restore_lsn('public.it_conc_b', '0/3000'::pg_lsn);
    PERFORM public.flashback_test_resolve_post_restore_boundary(v_tracking_b, '0/5500'::pg_lsn);
    PERFORM public.flashback_finalize_recover_operations();
    SELECT count(*) INTO v_cnt FROM public.it_conc_b;
    IF v_cnt <> 2 THEN
        RAISE EXCEPTION 'conc_b: expected 2 rows, got %', v_cnt;
    END IF;

    PERFORM public.flashback_test_restore_lsn('public.it_conc_c', '0/4000'::pg_lsn);
    PERFORM public.flashback_test_resolve_post_restore_boundary(v_tracking_c, '0/6500'::pg_lsn);
    PERFORM public.flashback_finalize_recover_operations();
    SELECT count(*) INTO v_cnt FROM public.it_conc_c WHERE val <> 'DESTROYED';
    IF v_cnt <> 4 THEN
        RAISE EXCEPTION 'conc_c: expected 4 non-DESTROYED rows, got %', v_cnt;
    END IF;

    DELETE FROM public.it_conc_a WHERE id = 1;
    DELETE FROM public.it_conc_b WHERE id = 10;
    DELETE FROM public.it_conc_c WHERE id = 100;
    PERFORM public.flashback_test_inject_commit(
        v_tracking_a, v_mid_lsn, clock_timestamp(), 959004,
        jsonb_build_array(jsonb_build_object('op','DELETE','old','{"id":1,"val":"a1"}'::jsonb))
    );
    PERFORM public.flashback_test_inject_commit(
        v_tracking_b, '0/9000'::pg_lsn, clock_timestamp(), 959005,
        jsonb_build_array(jsonb_build_object('op','DELETE','old','{"id":10,"val":"b1"}'::jsonb))
    );
    PERFORM public.flashback_test_inject_commit(
        v_tracking_c, '0/10000'::pg_lsn, clock_timestamp(), 959006,
        jsonb_build_array(jsonb_build_object('op','DELETE','old','{"id":100,"val":"c1"}'::jsonb))
    );

    DELETE FROM public.it_conc_a;
    DELETE FROM public.it_conc_b;
    DELETE FROM public.it_conc_c;

    PERFORM public.flashback_test_restore_lsn('public.it_conc_a', v_mid_lsn);
    PERFORM public.flashback_test_restore_lsn('public.it_conc_b', '0/9000'::pg_lsn);
    PERFORM public.flashback_test_restore_lsn('public.it_conc_c', '0/10000'::pg_lsn);

    SELECT count(*) INTO v_cnt FROM public.it_conc_a;
    IF v_cnt <> 2 THEN
        RAISE EXCEPTION 'multi-restore conc_a: expected 2, got %', v_cnt;
    END IF;

    SELECT count(*) INTO v_cnt FROM public.it_conc_b;
    IF v_cnt <> 1 THEN
        RAISE EXCEPTION 'multi-restore conc_b: expected 1, got %', v_cnt;
    END IF;

    SELECT count(*) INTO v_cnt FROM public.it_conc_c;
    IF v_cnt <> 3 THEN
        RAISE EXCEPTION 'multi-restore conc_c: expected 3, got %', v_cnt;
    END IF;

    -- pg_test runs this whole scenario in one transaction, so the synthetic
    -- post-restore boundary cannot become independently COMMIT-qualified.
    -- The journal must therefore retain the restores as pending and the
    -- terminal restore_log projection must not claim premature success.
    SELECT count(*) INTO v_cnt
    FROM flashback.operation_current_state
    WHERE command = 'restore_lsn'
      AND state = 'applied_coverage_pending';
    IF v_cnt < 6 THEN
        RAISE EXCEPTION 'expected at least 6 pending restore journal entries, got %', v_cnt;
    END IF;

    SELECT count(*) INTO v_cnt FROM flashback.restore_log;
    IF v_cnt <> 0 THEN
        RAISE EXCEPTION 'restore_log must exclude unverified restores, got % rows', v_cnt;
    END IF;
END;
$tv$;
