-- Test: Materialized view with indexes and populated state survives flashback_restore().
-- Verifies: (1) matview is recreated, (2) its index is rebuilt, (3) it is REFRESHED
-- automatically when it was populated before the restore.
DO $tv$
DECLARE
    t_before   timestamptz;
    v_idx_cnt  bigint;
    v_row_cnt  bigint;
    v_populated bool;
BEGIN
    -- ── Base table ───────────────────────────────────────────
    DROP TABLE IF EXISTS public.it_mview_base CASCADE;
    CREATE TABLE public.it_mview_base (id int PRIMARY KEY, region text, amount numeric);

    INSERT INTO public.it_mview_base VALUES
        (1, 'EU', 100), (2, 'US', 200), (3, 'EU', 150);

    -- ── Populated materialized view with an index ─────────────
    CREATE MATERIALIZED VIEW public.it_mview_summary AS
        SELECT region, sum(amount) AS total
        FROM public.it_mview_base
        GROUP BY region;

    CREATE UNIQUE INDEX it_mview_summary_region_idx
        ON public.it_mview_summary (region);

    -- ── Track ─────────────────────────────────────────────────
    PERFORM flashback_track('public.it_mview_base');
    PERFORM flashback_test_attach_capture_trigger('public.it_mview_base'::regclass);

    -- Snapshot containing (EU=250, US=200)
    t_before := clock_timestamp();

    -- Wipe base table — matview becomes stale but was populated
    DELETE FROM public.it_mview_base;

    PERFORM flashback_restore('public.it_mview_base', t_before);

    -- ── Verify base data ─────────────────────────────────────
    IF (SELECT count(*) FROM public.it_mview_base) <> 3 THEN
        RAISE EXCEPTION 'base table not restored (expected 3 rows)';
    END IF;

    -- ── Verify matview was recreated ─────────────────────────
    IF NOT EXISTS (
        SELECT 1 FROM pg_matviews
        WHERE schemaname = 'public' AND matviewname = 'it_mview_summary'
    ) THEN
        RAISE EXCEPTION 'materialized view it_mview_summary was not recreated';
    END IF;

    -- ── Verify matview is populated (relispopulated = true) ───
    SELECT relispopulated INTO v_populated
    FROM pg_class WHERE oid = 'public.it_mview_summary'::regclass;

    IF v_populated IS NOT TRUE THEN
        RAISE EXCEPTION 'matview it_mview_summary is not populated after restore';
    END IF;

    -- ── Verify matview data is correct ───────────────────────
    SELECT count(*) INTO v_row_cnt FROM public.it_mview_summary;
    IF v_row_cnt <> 2 THEN
        RAISE EXCEPTION 'matview has % rows, expected 2', v_row_cnt;
    END IF;

    -- ── Verify index was rebuilt ─────────────────────────────
    SELECT count(*) INTO v_idx_cnt
    FROM pg_indexes
    WHERE schemaname = 'public' AND tablename = 'it_mview_summary'
      AND indexname = 'it_mview_summary_region_idx';

    IF v_idx_cnt = 0 THEN
        RAISE EXCEPTION 'matview index it_mview_summary_region_idx was not rebuilt';
    END IF;

    -- ── Cleanup ───────────────────────────────────────────────
    DROP MATERIALIZED VIEW IF EXISTS public.it_mview_summary;
    DROP TABLE IF EXISTS public.it_mview_base CASCADE;
END;
$tv$;
