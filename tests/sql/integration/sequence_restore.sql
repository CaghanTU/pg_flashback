-- Test: SERIAL / SEQUENCE values are restored correctly after flashback_restore_lsn().
-- After restore, the sequence's current value must reflect the highest ID in the
-- restored table so new INSERTs don't collide with restored rows.
DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/2000'::pg_lsn;
    v_cnt bigint;
    v_new_id int;
BEGIN
    DROP TABLE IF EXISTS public.it_seq CASCADE;
    CREATE TABLE public.it_seq (
        id   serial PRIMARY KEY,
        note text
    );

    SELECT flashback_test_bootstrap_lifecycle('public.it_seq') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO public.it_seq (id, note) VALUES (1, 'first'), (2, 'second'), (3, 'third');
    PERFORM setval(pg_get_serial_sequence('public.it_seq', 'id'), 3, true);
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        v_point_lsn,
        clock_timestamp(),
        944001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"note":"first"}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":2,"note":"second"}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":3,"note":"third"}'::jsonb)
        )
    );

    DELETE FROM public.it_seq;
    INSERT INTO public.it_seq (id, note) VALUES (4, 'post1'), (5, 'post2');
    PERFORM setval(pg_get_serial_sequence('public.it_seq', 'id'), 5, true);
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        944002,
        jsonb_build_array(
            jsonb_build_object('op', 'DELETE', 'old', '{"id":1,"note":"first"}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":2,"note":"second"}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":3,"note":"third"}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":4,"note":"post1"}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":5,"note":"post2"}'::jsonb)
        )
    );

    PERFORM flashback_restore_lsn('public.it_seq', v_point_lsn);

    SELECT count(*) INTO v_cnt FROM public.it_seq;
    IF v_cnt <> 3 THEN
        RAISE EXCEPTION 'expected 3 rows after restore, got %', v_cnt;
    END IF;

    BEGIN
        INSERT INTO public.it_seq (note) VALUES ('after_restore') RETURNING id INTO v_new_id;
    EXCEPTION WHEN unique_violation THEN
        RAISE EXCEPTION 'sequence conflict: new INSERT collided with restored PK id %', v_new_id;
    END;

    IF v_new_id <= 3 THEN
        RAISE EXCEPTION 'sequence not advanced: new id % should be > 3', v_new_id;
    END IF;

    DROP TABLE IF EXISTS public.it_seq CASCADE;
END;
$tv$;
