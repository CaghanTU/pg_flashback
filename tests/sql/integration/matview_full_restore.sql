-- Test: Materialized view with indexes and populated state survives flashback_restore_lsn().
DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_boundary_lsn pg_lsn;
    v_idx_cnt bigint;
    v_row_cnt bigint;
    v_populated bool;
BEGIN
    DROP TABLE IF EXISTS public.it_mview_base CASCADE;
    CREATE TABLE public.it_mview_base (id int PRIMARY KEY, region text, amount numeric);

    INSERT INTO public.it_mview_base VALUES
        (1, 'EU', 100), (2, 'US', 200), (3, 'EU', 150);

    CREATE MATERIALIZED VIEW public.it_mview_summary AS
        SELECT region, sum(amount) AS total
        FROM public.it_mview_base
        GROUP BY region;

    CREATE UNIQUE INDEX it_mview_summary_region_idx
        ON public.it_mview_summary (region);

    SELECT flashback_test_bootstrap_lifecycle('public.it_mview_base') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;
    v_boundary_lsn := (v_boot->>'boundary_lsn')::pg_lsn;

    DELETE FROM public.it_mview_base;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        956001,
        jsonb_build_array(
            jsonb_build_object('op', 'DELETE', 'old', '{"id":1,"region":"EU","amount":100}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":2,"region":"US","amount":200}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":3,"region":"EU","amount":150}'::jsonb)
        )
    );

    PERFORM flashback_restore_lsn('public.it_mview_base', v_boundary_lsn);

    IF (SELECT count(*) FROM public.it_mview_base) <> 3 THEN
        RAISE EXCEPTION 'base table not restored (expected 3 rows)';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_matviews
        WHERE schemaname = 'public' AND matviewname = 'it_mview_summary'
    ) THEN
        RAISE EXCEPTION 'materialized view it_mview_summary was not recreated';
    END IF;

    SELECT relispopulated INTO v_populated
    FROM pg_class WHERE oid = 'public.it_mview_summary'::regclass;

    IF v_populated IS NOT TRUE THEN
        RAISE EXCEPTION 'matview it_mview_summary is not populated after restore';
    END IF;

    SELECT count(*) INTO v_row_cnt FROM public.it_mview_summary;
    IF v_row_cnt <> 2 THEN
        RAISE EXCEPTION 'matview has % rows, expected 2', v_row_cnt;
    END IF;

    SELECT count(*) INTO v_idx_cnt
    FROM pg_indexes
    WHERE schemaname = 'public' AND tablename = 'it_mview_summary'
      AND indexname = 'it_mview_summary_region_idx';

    IF v_idx_cnt = 0 THEN
        RAISE EXCEPTION 'matview index it_mview_summary_region_idx was not rebuilt';
    END IF;

    DROP MATERIALIZED VIEW IF EXISTS public.it_mview_summary;
    DROP TABLE IF EXISTS public.it_mview_base CASCADE;
END;
$tv$;
