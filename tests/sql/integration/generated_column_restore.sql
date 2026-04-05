-- Test: GENERATED ALWAYS AS (stored) columns survive flashback_restore().
-- The shadow table must be created with the same generated column definition,
-- and restore must succeed without trying to set the generated column directly.
DO $tv$
DECLARE
    t_before  timestamptz;
    v_full    text;
    v_cnt     bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_gen_col CASCADE;
    CREATE TABLE public.it_gen_col (
        id        int PRIMARY KEY,
        first_nm  text NOT NULL,
        last_nm   text NOT NULL,
        full_nm   text GENERATED ALWAYS AS (first_nm || ' ' || last_nm) STORED
    );

    PERFORM flashback_track('public.it_gen_col');
    PERFORM flashback_test_attach_capture_trigger('public.it_gen_col'::regclass);

    INSERT INTO public.it_gen_col (id, first_nm, last_nm) VALUES
        (1, 'Ada', 'Lovelace'),
        (2, 'Grace', 'Hopper');

    t_before := clock_timestamp();

    -- Corrupt data
    DELETE FROM public.it_gen_col WHERE id = 1;
    UPDATE public.it_gen_col SET first_nm = 'X' WHERE id = 2;

    PERFORM flashback_restore('public.it_gen_col', t_before);

    -- Verify row count
    SELECT count(*) INTO v_cnt FROM public.it_gen_col;
    IF v_cnt <> 2 THEN
        RAISE EXCEPTION 'expected 2 rows after restore, got %', v_cnt;
    END IF;

    -- Verify generated column value is correct
    SELECT full_nm INTO v_full FROM public.it_gen_col WHERE id = 1;
    IF v_full <> 'Ada Lovelace' THEN
        RAISE EXCEPTION 'generated column wrong: expected ''Ada Lovelace'', got ''%''', v_full;
    END IF;

    SELECT full_nm INTO v_full FROM public.it_gen_col WHERE id = 2;
    IF v_full <> 'Grace Hopper' THEN
        RAISE EXCEPTION 'generated column wrong: expected ''Grace Hopper'', got ''%''', v_full;
    END IF;

    DROP TABLE IF EXISTS public.it_gen_col CASCADE;
END;
$tv$;
