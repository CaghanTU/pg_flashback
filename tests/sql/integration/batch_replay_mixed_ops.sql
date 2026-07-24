-- Test: batch replay with mixed operations (INSERT/UPDATE/DELETE).
DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/2000'::pg_lsn;
    v_count bigint;
    v_row record;
BEGIN
    DROP TABLE IF EXISTS public.it_batch_replay;
    CREATE TABLE public.it_batch_replay (
        id int PRIMARY KEY, name text, score int
    );
    INSERT INTO public.it_batch_replay VALUES
        (1, 'a', 10), (2, 'b', 20), (3, 'c', 30),
        (4, 'd', 40), (5, 'e', 50);

    SELECT flashback_test_bootstrap_lifecycle('public.it_batch_replay') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    UPDATE public.it_batch_replay SET score = 100 WHERE id = 1;
    DELETE FROM public.it_batch_replay WHERE id = 2;
    INSERT INTO public.it_batch_replay VALUES (6, 'f', 60);
    UPDATE public.it_batch_replay SET score = 300 WHERE id = 3;
    UPDATE public.it_batch_replay SET name = 'charlie' WHERE id = 3;
    DELETE FROM public.it_batch_replay WHERE id = 4;
    INSERT INTO public.it_batch_replay VALUES (4, 'delta', 400);

    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        947001,
        jsonb_build_array(
            jsonb_build_object('op', 'UPDATE', 'old', '{"id": 1, "score": 10}'::jsonb, 'new', '{"id": 1, "score": 100}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id": 2, "name": "b", "score": 20}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id": 6, "name": "f", "score": 60}'::jsonb),
            jsonb_build_object('op', 'UPDATE', 'old', '{"id": 3, "score": 30}'::jsonb, 'new', '{"id": 3, "score": 300}'::jsonb),
            jsonb_build_object('op', 'UPDATE', 'old', '{"id": 3, "name": "c"}'::jsonb, 'new', '{"id": 3, "name": "charlie"}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id": 4, "name": "d", "score": 40}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id": 4, "name": "delta", "score": 400}'::jsonb)
        )
    );

    PERFORM flashback_test_restore_lsn('public.it_batch_replay', v_point_lsn);

    SELECT count(*) INTO v_count FROM public.it_batch_replay;
    IF v_count <> 5 THEN
        RAISE EXCEPTION 'batch replay restore: expected 5 rows, got %', v_count;
    END IF;

    SELECT * INTO v_row FROM public.it_batch_replay WHERE id = 1;
    IF v_row.score <> 10 THEN
        RAISE EXCEPTION 'batch replay: id=1 score should be 10, got %', v_row.score;
    END IF;

    SELECT * INTO v_row FROM public.it_batch_replay WHERE id = 2;
    IF v_row IS NULL THEN
        RAISE EXCEPTION 'batch replay: id=2 should exist after restore';
    END IF;

    SELECT * INTO v_row FROM public.it_batch_replay WHERE id = 3;
    IF v_row.name <> 'c' OR v_row.score <> 30 THEN
        RAISE EXCEPTION 'batch replay: id=3 should be (c, 30), got (%, %)',
            v_row.name, v_row.score;
    END IF;

    SELECT * INTO v_row FROM public.it_batch_replay WHERE id = 4;
    IF v_row.name <> 'd' OR v_row.score <> 40 THEN
        RAISE EXCEPTION 'batch replay: id=4 should be (d, 40), got (%, %)',
            v_row.name, v_row.score;
    END IF;

    IF EXISTS (SELECT 1 FROM public.it_batch_replay WHERE id = 6) THEN
        RAISE EXCEPTION 'batch replay: id=6 should not exist after restore';
    END IF;
END;
$tv$;
