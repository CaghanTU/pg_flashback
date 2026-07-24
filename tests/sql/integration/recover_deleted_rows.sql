-- Regression test: flashback_recover_deleted_lsn()
-- Verifies that accidentally deleted rows are recovered without touching
-- surviving rows, while rows added after the recovery point are left alone.
DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/3000'::pg_lsn;
    v_count bigint;
    recovered bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_recover_deleted CASCADE;
    CREATE TABLE public.it_recover_deleted (
        id    SERIAL PRIMARY KEY,
        name  TEXT NOT NULL,
        score INT  NOT NULL DEFAULT 0
    );

    SELECT flashback_test_bootstrap_lifecycle('public.it_recover_deleted') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO public.it_recover_deleted (id, name, score)
    VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'carol', 30), (4, 'dave', 40), (5, 'eve', 50);
    PERFORM setval(pg_get_serial_sequence('public.it_recover_deleted', 'id'), 5, true);

    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        930001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"name":"alice","score":10}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":2,"name":"bob","score":20}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":3,"name":"carol","score":30}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":4,"name":"dave","score":40}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":5,"name":"eve","score":50}'::jsonb)
        )
    );

    DELETE FROM public.it_recover_deleted WHERE name IN ('bob', 'carol', 'dave');
    INSERT INTO public.it_recover_deleted (id, name, score) VALUES (6, 'frank', 60);
    PERFORM setval(pg_get_serial_sequence('public.it_recover_deleted', 'id'), 6, true);

    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/4000'::pg_lsn,
        clock_timestamp(),
        930002,
        jsonb_build_array(
            jsonb_build_object('op', 'DELETE', 'old', '{"id":2,"name":"bob","score":20}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":3,"name":"carol","score":30}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":4,"name":"dave","score":40}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":6,"name":"frank","score":60}'::jsonb)
        )
    );

    SELECT count(*) INTO v_count FROM public.it_recover_deleted;
    IF v_count <> 3 THEN
        RAISE EXCEPTION 'Expected 3 rows before recovery, got %', v_count;
    END IF;

    SELECT flashback_recover_deleted_lsn('public.it_recover_deleted', v_point_lsn)
      INTO recovered;

    IF recovered <> 3 THEN
        RAISE EXCEPTION 'Expected recover_deleted to return 3, got %', recovered;
    END IF;

    SELECT count(*) INTO v_count
    FROM public.it_recover_deleted
    WHERE name IN ('bob', 'carol', 'dave');
    IF v_count <> 3 THEN
        RAISE EXCEPTION 'Expected bob/carol/dave to be recovered, got %', v_count;
    END IF;

    SELECT count(*) INTO v_count
    FROM public.it_recover_deleted
    WHERE name IN ('alice', 'eve');
    IF v_count <> 2 THEN
        RAISE EXCEPTION 'Expected exactly 1 alice and 1 eve, got %', v_count;
    END IF;

    SELECT count(*) INTO v_count
    FROM public.it_recover_deleted WHERE name = 'frank';
    IF v_count <> 1 THEN
        RAISE EXCEPTION 'frank (post-point insert) should survive recover_deleted, got %', v_count;
    END IF;

    SELECT count(*) INTO v_count FROM public.it_recover_deleted;
    IF v_count <> 6 THEN
        RAISE EXCEPTION 'Expected 6 total rows after recovery, got %', v_count;
    END IF;

    PERFORM flashback_untrack('public.it_recover_deleted');
    DROP TABLE IF EXISTS public.it_recover_deleted CASCADE;
END;
$tv$;
