-- Test: A chained view dependency (view2 depends on view1 depends on base table)
-- survives flashback_restore(). Both views must be recreated in correct order
-- so that view2 (which references view1) can be created after view1 exists.
--
-- Note: pg_flashback sorts by schema+name before recreating; since alphabetically
-- 'it_vchain_view1' < 'it_vchain_view2' the order is preserved in this test.
DO $tv$
DECLARE
    t_before timestamptz;
    v_cnt    bigint;
    v_exist1 bool;
    v_exist2 bool;
BEGIN
    -- ── Base table ────────────────────────────────────────────
    DROP TABLE IF EXISTS public.it_vchain_base CASCADE;
    CREATE TABLE public.it_vchain_base (
        id      int PRIMARY KEY,
        dept    text,
        salary  numeric
    );

    -- ── view1: filters high-earners ───────────────────────────
    CREATE VIEW public.it_vchain_view1 AS
        SELECT id, dept, salary
        FROM public.it_vchain_base
        WHERE salary > 50000;

    -- ── view2: aggregates view1 ───────────────────────────────
    CREATE VIEW public.it_vchain_view2 AS
        SELECT dept, count(*) AS headcount, avg(salary) AS avg_salary
        FROM public.it_vchain_view1
        GROUP BY dept;

    -- ── Track ─────────────────────────────────────────────────
    PERFORM flashback_track('public.it_vchain_base');
    PERFORM flashback_test_attach_capture_trigger('public.it_vchain_base'::regclass);

    INSERT INTO public.it_vchain_base VALUES
        (1, 'Eng',   90000),
        (2, 'Eng',   75000),
        (3, 'Sales', 45000),
        (4, 'Sales', 60000);

    t_before := clock_timestamp();
    DELETE FROM public.it_vchain_base;

    PERFORM flashback_restore('public.it_vchain_base', t_before);

    -- ── Base data check ───────────────────────────────────────
    SELECT count(*) INTO v_cnt FROM public.it_vchain_base;
    IF v_cnt <> 4 THEN
        RAISE EXCEPTION 'base table: expected 4 rows, got %', v_cnt;
    END IF;

    -- ── view1 still exists ────────────────────────────────────
    SELECT EXISTS (
        SELECT 1 FROM pg_views
        WHERE schemaname = 'public' AND viewname = 'it_vchain_view1'
    ) INTO v_exist1;

    IF NOT v_exist1 THEN
        RAISE EXCEPTION 'it_vchain_view1 was not recreated';
    END IF;

    -- ── view2 still exists and is queryable ───────────────────
    SELECT EXISTS (
        SELECT 1 FROM pg_views
        WHERE schemaname = 'public' AND viewname = 'it_vchain_view2'
    ) INTO v_exist2;

    IF NOT v_exist2 THEN
        RAISE EXCEPTION 'it_vchain_view2 was not recreated';
    END IF;

    -- view2 must return correct data through the chain
    SELECT count(*) INTO v_cnt FROM public.it_vchain_view2;
    -- Eng(2 high-earners) + Sales(1 high-earner above 50k) = 2 dept rows
    IF v_cnt <> 2 THEN
        RAISE EXCEPTION 'view chain broken: it_vchain_view2 returned % rows, expected 2', v_cnt;
    END IF;

    -- ── Cleanup ───────────────────────────────────────────────
    DROP VIEW  IF EXISTS public.it_vchain_view2;
    DROP VIEW  IF EXISTS public.it_vchain_view1;
    DROP TABLE IF EXISTS public.it_vchain_base CASCADE;
END;
$tv$;
