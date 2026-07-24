-- Test: post-restore checkpoint prevents duplicate key on second restore
DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_lsn_t2 pg_lsn := '0/3000'::pg_lsn;
    v_lsn_phase2 pg_lsn := '0/6000'::pg_lsn;
    v_cnt bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_post_ckpt CASCADE;
    CREATE TABLE public.it_post_ckpt (id int PRIMARY KEY, val text);

    SELECT flashback_test_bootstrap_lifecycle('public.it_post_ckpt') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO public.it_post_ckpt VALUES (1, 'A'), (2, 'B'), (3, 'C');
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        949001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"val":"A"}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":2,"val":"B"}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":3,"val":"C"}'::jsonb)
        )
    );

    DELETE FROM public.it_post_ckpt WHERE id = 3;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        v_lsn_t2,
        clock_timestamp(),
        949002,
        jsonb_build_array(
            jsonb_build_object('op', 'DELETE', 'old', '{"id":3,"val":"C"}'::jsonb)
        )
    );

    UPDATE public.it_post_ckpt SET val = 'X' WHERE id = 1;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/4000'::pg_lsn,
        clock_timestamp(),
        949003,
        jsonb_build_array(
            jsonb_build_object('op', 'UPDATE', 'old', '{"id":1,"val":"A"}'::jsonb, 'new', '{"id":1,"val":"X"}'::jsonb)
        )
    );

    PERFORM flashback_test_restore_lsn('public.it_post_ckpt', v_lsn_t2);
    PERFORM flashback_test_resolve_post_restore_boundary(v_tracking_id, '0/4500'::pg_lsn);

    SELECT count(*) INTO v_cnt FROM public.it_post_ckpt WHERE id = 3;
    IF v_cnt <> 0 THEN
        RAISE EXCEPTION '1st restore: id=3 should be deleted at t2, got %', v_cnt;
    END IF;

    INSERT INTO public.it_post_ckpt VALUES (4, 'D');
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        v_lsn_phase2,
        clock_timestamp(),
        949004,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":4,"val":"D"}'::jsonb)
        )
    );

    DELETE FROM public.it_post_ckpt WHERE id = 4;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/7000'::pg_lsn,
        clock_timestamp(),
        949005,
        jsonb_build_array(
            jsonb_build_object('op', 'DELETE', 'old', '{"id":4,"val":"D"}'::jsonb)
        )
    );

    PERFORM flashback_test_restore_lsn('public.it_post_ckpt', v_lsn_phase2);

    SELECT count(*) INTO v_cnt FROM public.it_post_ckpt WHERE id = 4;
    IF v_cnt <> 1 THEN
        RAISE EXCEPTION '2nd restore: expected id=4, got count=%', v_cnt;
    END IF;
END;
$tv$;
