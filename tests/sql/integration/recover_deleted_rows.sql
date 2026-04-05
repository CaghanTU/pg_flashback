-- Regression test: flashback_recover_deleted()
-- Verifies that accidentally deleted rows are recovered without touching
-- surviving rows, while rows added after the recovery point are left alone.
DO $tv$
DECLARE
    t_before  timestamptz;
    v_count   bigint;
    recovered bigint;
BEGIN
    -- Setup
    DROP TABLE IF EXISTS public.it_recover_deleted CASCADE;
    CREATE TABLE public.it_recover_deleted (
        id    SERIAL PRIMARY KEY,
        name  TEXT NOT NULL,
        score INT  NOT NULL DEFAULT 0
    );
    PERFORM flashback_track('public.it_recover_deleted');
    PERFORM flashback_test_attach_capture_trigger('public.it_recover_deleted'::regclass);

    -- Insert 5 rows, then record a restore point
    INSERT INTO public.it_recover_deleted (name, score)
    VALUES ('alice', 10), ('bob', 20), ('carol', 30), ('dave', 40), ('eve', 50);

    t_before := clock_timestamp();

    -- Simulate an accident: delete 3, also add 1 new row after the point
    DELETE FROM public.it_recover_deleted WHERE name IN ('bob', 'carol', 'dave');
    INSERT INTO public.it_recover_deleted (name, score) VALUES ('frank', 60);

    -- Sanity: 3 rows before recovery (alice + eve + frank)
    SELECT count(*) INTO v_count FROM public.it_recover_deleted;
    IF v_count <> 3 THEN
        RAISE EXCEPTION 'Expected 3 rows before recovery, got %', v_count;
    END IF;

    -- Exercise: recover deleted rows
    SELECT flashback_recover_deleted('public.it_recover_deleted', t_before) INTO recovered;

    -- 1. Function must report 3 rows recovered
    IF recovered <> 3 THEN
        RAISE EXCEPTION 'Expected recover_deleted to return 3, got %', recovered;
    END IF;

    -- 2. bob, carol, dave must be back
    SELECT count(*) INTO v_count
    FROM public.it_recover_deleted
    WHERE name IN ('bob', 'carol', 'dave');
    IF v_count <> 3 THEN
        RAISE EXCEPTION 'Expected bob/carol/dave to be recovered, got %', v_count;
    END IF;

    -- 3. alice and eve must not be duplicated
    SELECT count(*) INTO v_count
    FROM public.it_recover_deleted
    WHERE name IN ('alice', 'eve');
    IF v_count <> 2 THEN
        RAISE EXCEPTION 'Expected exactly 1 alice and 1 eve, got %', v_count;
    END IF;

    -- 4. frank (inserted AFTER restore point) must still be there
    SELECT count(*) INTO v_count
    FROM public.it_recover_deleted WHERE name = 'frank';
    IF v_count <> 1 THEN
        RAISE EXCEPTION 'frank (post-point insert) should survive recover_deleted, got %', v_count;
    END IF;

    -- 5. Total: alice + bob + carol + dave + eve + frank = 6
    SELECT count(*) INTO v_count FROM public.it_recover_deleted;
    IF v_count <> 6 THEN
        RAISE EXCEPTION 'Expected 6 total rows after recovery, got %', v_count;
    END IF;

    -- Cleanup
    PERFORM flashback_untrack('public.it_recover_deleted');
    DROP TABLE IF EXISTS public.it_recover_deleted CASCADE;
END;
$tv$;
