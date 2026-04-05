-- Regression test: B16 — flashback_restore of a parent table with classical
-- INHERITS children must NOT drop child tables (no DROP CASCADE side-effect).
-- After restore the inheritance relationship is re-established automatically.
DO $tv$
DECLARE
    t_snap   timestamptz;
    v_count  bigint;
    v_exists boolean;
BEGIN
    -- Setup parent + two classical-INHERITS children
    DROP TABLE IF EXISTS public.it_inh_cars    CASCADE;
    DROP TABLE IF EXISTS public.it_inh_trucks  CASCADE;
    DROP TABLE IF EXISTS public.it_inh_fleet   CASCADE;

    CREATE TABLE public.it_inh_fleet (
        id    SERIAL PRIMARY KEY,
        make  TEXT NOT NULL,
        year  INT  NOT NULL
    );
    CREATE TABLE public.it_inh_cars (
        doors INT DEFAULT 4
    ) INHERITS (public.it_inh_fleet);
    CREATE TABLE public.it_inh_trucks (
        payload_t NUMERIC
    ) INHERITS (public.it_inh_fleet);

    PERFORM flashback_track('public.it_inh_fleet');
    PERFORM flashback_test_attach_capture_trigger('public.it_inh_fleet'::regclass);

    -- Insert one row in the parent and one in each child
    INSERT INTO public.it_inh_fleet  (make, year) VALUES ('GenericCo', 2019);
    INSERT INTO public.it_inh_cars   (make, year, doors)     VALUES ('ToyotaCo',  2022, 4);
    INSERT INTO public.it_inh_trucks (make, year, payload_t) VALUES ('FordCo',    2023, 1.5);

    t_snap := clock_timestamp();

    -- ── Assert: restore parent must NOT destroy children ──────────
    PERFORM flashback_restore('public.it_inh_fleet', t_snap);

    -- 1. Child tables must still exist
    SELECT to_regclass('public.it_inh_cars') IS NOT NULL INTO v_exists;
    IF NOT v_exists THEN
        RAISE EXCEPTION 'B16: it_inh_cars was dropped by restore';
    END IF;

    SELECT to_regclass('public.it_inh_trucks') IS NOT NULL INTO v_exists;
    IF NOT v_exists THEN
        RAISE EXCEPTION 'B16: it_inh_trucks was dropped by restore';
    END IF;

    -- 2. Inheritance relationship must be intact: querying parent shows all rows
    SELECT count(*) INTO v_count FROM public.it_inh_fleet;
    IF v_count <> 3 THEN
        RAISE EXCEPTION 'B16: expected 3 rows visible via parent, got % (inheritance broken)', v_count;
    END IF;

    -- 3. Child-specific rows are accessible via child tables
    SELECT count(*) INTO v_count FROM public.it_inh_cars WHERE make = 'ToyotaCo';
    IF v_count <> 1 THEN
        RAISE EXCEPTION 'B16: ToyotaCo row missing from it_inh_cars after restore, got %', v_count;
    END IF;

    -- Cleanup
    PERFORM flashback_untrack('public.it_inh_fleet');
    DROP TABLE IF EXISTS public.it_inh_cars    CASCADE;
    DROP TABLE IF EXISTS public.it_inh_trucks  CASCADE;
    DROP TABLE IF EXISTS public.it_inh_fleet   CASCADE;
END;
$tv$;
