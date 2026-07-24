-- Test: PITR commit LSN filtering via the WAL seam
DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_lsn_t1 pg_lsn := '0/2000'::pg_lsn;
    v_lsn_t2 pg_lsn := '0/5000'::pg_lsn;
    v_cnt bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_pitr CASCADE;
    CREATE TABLE public.it_pitr (id int PRIMARY KEY, val text);

    SELECT flashback_test_bootstrap_lifecycle('public.it_pitr') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO public.it_pitr VALUES (1, 'first');
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        v_lsn_t1,
        clock_timestamp(),
        948001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"val":"first"}'::jsonb)
        )
    );

    INSERT INTO public.it_pitr VALUES (2, 'second');
    INSERT INTO public.it_pitr VALUES (3, 'third');
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        948002,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":2,"val":"second"}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":3,"val":"third"}'::jsonb)
        )
    );

    DELETE FROM public.it_pitr;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/4000'::pg_lsn,
        clock_timestamp(),
        948003,
        jsonb_build_array(
            jsonb_build_object('op', 'DELETE', 'old', '{"id":1,"val":"first"}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":2,"val":"second"}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":3,"val":"third"}'::jsonb)
        )
    );

    PERFORM flashback_restore_lsn('public.it_pitr', v_lsn_t1);
    PERFORM flashback_test_resolve_post_restore_boundary(v_tracking_id, '0/4500'::pg_lsn);
    SELECT count(*) INTO v_cnt FROM public.it_pitr;
    IF v_cnt <> 1 THEN
        RAISE EXCEPTION 'restore to t1: expected 1, got %', v_cnt;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.it_pitr WHERE id = 1 AND val = 'first') THEN
        RAISE EXCEPTION 'restore to t1: id=1 missing';
    END IF;

    INSERT INTO public.it_pitr VALUES (10, 'new');
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        v_lsn_t2,
        clock_timestamp(),
        948004,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":10,"val":"new"}'::jsonb)
        )
    );

    DELETE FROM public.it_pitr WHERE id = 10;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/6000'::pg_lsn,
        clock_timestamp(),
        948005,
        jsonb_build_array(
            jsonb_build_object('op', 'DELETE', 'old', '{"id":10,"val":"new"}'::jsonb)
        )
    );

    PERFORM flashback_restore_lsn('public.it_pitr', v_lsn_t2);
    SELECT count(*) INTO v_cnt FROM public.it_pitr;
    IF v_cnt <> 2 THEN
        RAISE EXCEPTION 'restore to t2: expected 2, got %', v_cnt;
    END IF;

    SELECT count(*) INTO v_cnt
    FROM flashback.delta_log
    WHERE tracking_id = v_tracking_id
      AND committed_at IS NULL;
    IF v_cnt <> 0 THEN
        RAISE EXCEPTION 'delta_log has uncommitted rows: %', v_cnt;
    END IF;
END;
$tv$;
