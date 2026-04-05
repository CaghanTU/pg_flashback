-- Test: SERIAL / SEQUENCE values are restored correctly after flashback_restore().
-- After restore, the sequence's current value must reflect the highest ID in the
-- restored table so new INSERTs don't collide with restored rows.
DO $tv$
DECLARE
    t_before  timestamptz;
    v_cnt     bigint;
    v_new_id  int;
    v_seq_val bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_seq CASCADE;
    CREATE TABLE public.it_seq (
        id   serial PRIMARY KEY,
        note text
    );

    PERFORM flashback_track('public.it_seq');
    PERFORM flashback_test_attach_capture_trigger('public.it_seq'::regclass);

    INSERT INTO public.it_seq (note) VALUES ('first'), ('second'), ('third');
    -- After 3 inserts, max id = 3

    t_before := clock_timestamp();

    -- Delete all rows
    DELETE FROM public.it_seq;

    -- Insert more rows to advance the sequence past 3
    INSERT INTO public.it_seq (note) VALUES ('post1'), ('post2');
    -- Sequence is now at 5

    PERFORM flashback_restore('public.it_seq', t_before);

    -- Restored table should have 3 rows with ids 1,2,3
    SELECT count(*) INTO v_cnt FROM public.it_seq;
    IF v_cnt <> 3 THEN
        RAISE EXCEPTION 'expected 3 rows after restore, got %', v_cnt;
    END IF;

    -- Inserting a new row must not conflict with restored ids
    BEGIN
        INSERT INTO public.it_seq (note) VALUES ('after_restore') RETURNING id INTO v_new_id;
    EXCEPTION WHEN unique_violation THEN
        RAISE EXCEPTION 'sequence conflict: new INSERT collided with restored PK id %', v_new_id;
    END;

    -- New id must be > 3 (sequence advanced past restored max)
    IF v_new_id <= 3 THEN
        RAISE EXCEPTION 'sequence not advanced: new id % should be > 3', v_new_id;
    END IF;

    DROP TABLE IF EXISTS public.it_seq CASCADE;
END;
$tv$;
