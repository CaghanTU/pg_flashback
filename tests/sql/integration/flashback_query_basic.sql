DO $tv$
DECLARE
    t_before timestamptz;
    v_count  bigint;
    v_name   text;
BEGIN
    DROP TABLE IF EXISTS public.it_fb_query;
    CREATE TABLE public.it_fb_query (id int primary key, name text, price numeric);
    INSERT INTO public.it_fb_query VALUES (1, 'alpha', 10), (2, 'beta', 20), (3, 'gamma', 30);
    PERFORM flashback_track('public.it_fb_query');
    PERFORM flashback_test_attach_capture_trigger('public.it_fb_query'::regclass);

    t_before := clock_timestamp();

    -- Modify the table
    UPDATE public.it_fb_query SET price = 999 WHERE id = 1;
    DELETE FROM public.it_fb_query WHERE id = 2;
    INSERT INTO public.it_fb_query VALUES (4, 'delta', 40);

    -- Use flashback_query to see the table as it was before modifications
    SELECT count(*) INTO v_count
    FROM flashback_query('public.it_fb_query', t_before)
         AS t(id int, name text, price numeric);

    IF v_count <> 3 THEN
        RAISE EXCEPTION 'flashback_query: expected 3 rows, got %', v_count;
    END IF;

    -- Verify specific data at the old point in time
    SELECT t.name INTO v_name
    FROM flashback_query('public.it_fb_query', t_before, 'SELECT * FROM $FB_TABLE WHERE id = 1')
         AS t(id int, name text, price numeric);

    IF v_name <> 'alpha' THEN
        RAISE EXCEPTION 'flashback_query: expected alpha, got %', v_name;
    END IF;

    -- Verify current table is still modified
    IF NOT EXISTS (SELECT 1 FROM public.it_fb_query WHERE id = 4) THEN
        RAISE EXCEPTION 'flashback_query should not modify the actual table';
    END IF;
END;
$tv$;
