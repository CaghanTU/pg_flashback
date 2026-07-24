-- Test: sub-transaction rollback — only committed changes should appear in delta_log
-- Verifies that rolled-back savepoint changes do NOT affect restore
DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/2000'::pg_lsn;
    v_cnt bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_subtxn CASCADE;
    CREATE TABLE public.it_subtxn (id int PRIMARY KEY, val text);

    SELECT flashback_test_bootstrap_lifecycle('public.it_subtxn') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO public.it_subtxn VALUES (1, 'committed');
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        v_point_lsn,
        clock_timestamp(),
        942001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"val":"committed"}'::jsonb)
        )
    );

    BEGIN
        INSERT INTO public.it_subtxn VALUES (2, 'will_rollback');
        RAISE EXCEPTION 'intentional_rollback';
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;

    INSERT INTO public.it_subtxn VALUES (3, 'also_committed');
    DELETE FROM public.it_subtxn WHERE id = 3;

    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        942002,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":3,"val":"also_committed"}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":3,"val":"also_committed"}'::jsonb)
        )
    );

    SELECT count(*) INTO v_cnt FROM public.it_subtxn;
    IF v_cnt <> 1 THEN
        RAISE EXCEPTION 'pre-check: expected 1 row, got %', v_cnt;
    END IF;

    SELECT count(*) INTO v_cnt
    FROM flashback.delta_log
    WHERE tracking_id = v_tracking_id
      AND event_type = 'INSERT'
      AND new_data->>'val' = 'will_rollback';
    IF v_cnt <> 0 THEN
        RAISE EXCEPTION 'delta_log contains rolled-back row (val=will_rollback), count=%', v_cnt;
    END IF;

    PERFORM flashback_test_restore_lsn('public.it_subtxn', v_point_lsn);

    SELECT count(*) INTO v_cnt FROM public.it_subtxn;
    IF v_cnt <> 1 THEN
        RAISE EXCEPTION 'post-restore: expected 1 row, got %', v_cnt;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM public.it_subtxn WHERE id = 1 AND val = 'committed') THEN
        RAISE EXCEPTION 'post-restore: id=1 missing';
    END IF;
END;
$tv$;
