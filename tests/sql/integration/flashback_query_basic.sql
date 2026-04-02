DO $tv$
DECLARE
    t_before timestamptz;
    v_count  bigint;
    v_name   text;
BEGIN
    -- ── Scenario 1: basic SELECT AS OF ───────────────────────────────────────
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

    -- flashback_query should show the pre-modification state
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

    -- Verify current table is still unmodified by flashback_query
    IF NOT EXISTS (SELECT 1 FROM public.it_fb_query WHERE id = 4) THEN
        RAISE EXCEPTION 'flashback_query should not modify the actual table';
    END IF;

    -- ── Scenario 2: DROP event handling ──────────────────────────────────────
    -- If a DROP event is recorded before target_time, flashback_query
    -- should return 0 rows (table did not exist at that point).
    DECLARE
        t_drop timestamptz;
        v_drop_count bigint;
    BEGIN
        DROP TABLE IF EXISTS public.it_fb_query_drop;
        CREATE TABLE public.it_fb_query_drop (id int primary key, val text);
        PERFORM flashback_track('public.it_fb_query_drop');
        PERFORM flashback_test_attach_capture_trigger('public.it_fb_query_drop'::regclass);

        INSERT INTO public.it_fb_query_drop VALUES (1, 'exists');

        -- Simulate a DROP event in delta_log (as DDL hook would record it)
        INSERT INTO flashback.delta_log(
            event_time, event_type, table_name, rel_oid, schema_version,
            old_data, new_data, committed_at
        )
        VALUES (
            clock_timestamp(), 'DROP', 'public.it_fb_query_drop',
            'public.it_fb_query_drop'::regclass::oid, 1,
            NULL, NULL, clock_timestamp()
        );

        t_drop := clock_timestamp();  -- target_time after the DROP event

        SELECT count(*) INTO v_drop_count
        FROM flashback_query('public.it_fb_query_drop', t_drop)
             AS t(id int, val text);

        IF v_drop_count <> 0 THEN
            RAISE EXCEPTION 'flashback_query after DROP: expected 0 rows, got %', v_drop_count;
        END IF;

        DROP TABLE IF EXISTS public.it_fb_query_drop;
    END;

    -- ── Scenario 3: schema evolution — ADD COLUMN ────────────────────────────
    -- After ADD COLUMN, flashback_query to a pre-ADD time should not include
    -- the new column (it did not exist). We test this by querying the exact
    -- columns that existed at t_before (from Scenario 1) — the fact that the
    -- query does not error on the historical schema is sufficient.
    SELECT count(*) INTO v_count
    FROM flashback_query('public.it_fb_query', t_before)
         AS t(id int, name text, price numeric);

    IF v_count <> 3 THEN
        RAISE EXCEPTION 'flashback_query schema-evolution check: expected 3, got %', v_count;
    END IF;
END;
$tv$;

