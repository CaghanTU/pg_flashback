-- Test: ACL (GRANTs) on dependent views/matviews are automatically restored
-- after flashback_restore() drops and recreates them via DROP TABLE CASCADE.
-- Covers: plain GRANT, PUBLIC grant, WITH GRANT OPTION, multiple grantees.
DO $tv$
DECLARE
    t_before   timestamptz;
    v_sel_cnt  bigint;
    v_wgo_cnt  bigint;
    v_pub_cnt  bigint;
BEGIN
    -- ── Setup roles ──────────────────────────────────────────
    DROP ROLE IF EXISTS it_vacl_reader;
    DROP ROLE IF EXISTS it_vacl_writer;
    CREATE ROLE it_vacl_reader;
    CREATE ROLE it_vacl_writer;

    -- ── Base table ───────────────────────────────────────────
    DROP TABLE IF EXISTS public.it_vacl_base CASCADE;
    CREATE TABLE public.it_vacl_base (id int PRIMARY KEY, val text);

    -- ── Dependent view with two grantees + PUBLIC on matview ─
    CREATE VIEW public.it_vacl_view AS
        SELECT id, val FROM public.it_vacl_base;

    GRANT SELECT ON public.it_vacl_view TO it_vacl_reader;
    GRANT SELECT, INSERT ON public.it_vacl_view TO it_vacl_writer WITH GRANT OPTION;

    CREATE MATERIALIZED VIEW public.it_vacl_mview AS
        SELECT id, val FROM public.it_vacl_base WITH NO DATA;

    GRANT SELECT ON public.it_vacl_mview TO PUBLIC;

    -- ── Track + populate ─────────────────────────────────────
    PERFORM flashback_track('public.it_vacl_base');
    PERFORM flashback_test_attach_capture_trigger('public.it_vacl_base'::regclass);

    INSERT INTO public.it_vacl_base VALUES (1, 'alpha'), (2, 'beta');
    t_before := clock_timestamp();
    DELETE FROM public.it_vacl_base;

    PERFORM flashback_restore('public.it_vacl_base', t_before);

    -- ── Verify data ──────────────────────────────────────────
    IF (SELECT count(*) FROM public.it_vacl_base) <> 2 THEN
        RAISE EXCEPTION 'data not restored (expected 2 rows)';
    END IF;

    -- ── Verify view still exists ─────────────────────────────
    IF NOT EXISTS (
        SELECT 1 FROM pg_views
        WHERE schemaname = 'public' AND viewname = 'it_vacl_view'
    ) THEN
        RAISE EXCEPTION 'dependent view it_vacl_view was not recreated';
    END IF;

    -- ── Verify it_vacl_reader has SELECT on view ─────────────
    SELECT count(*) INTO v_sel_cnt
    FROM (SELECT (aclexplode(relacl)).* FROM pg_class
          WHERE oid = 'public.it_vacl_view'::regclass) a
    WHERE a.grantee = (SELECT oid FROM pg_roles WHERE rolname = 'it_vacl_reader')
      AND a.privilege_type = 'SELECT';

    IF v_sel_cnt = 0 THEN
        RAISE EXCEPTION 'ACL lost: it_vacl_reader SELECT on it_vacl_view not restored';
    END IF;

    -- ── Verify it_vacl_writer has WITH GRANT OPTION ──────────
    SELECT count(*) INTO v_wgo_cnt
    FROM (SELECT (aclexplode(relacl)).* FROM pg_class
          WHERE oid = 'public.it_vacl_view'::regclass) a
    WHERE a.grantee   = (SELECT oid FROM pg_roles WHERE rolname = 'it_vacl_writer')
      AND a.is_grantable = true;

    IF v_wgo_cnt = 0 THEN
        RAISE EXCEPTION 'ACL lost: it_vacl_writer WITH GRANT OPTION on it_vacl_view not restored';
    END IF;

    -- ── Verify PUBLIC SELECT on matview ──────────────────────
    SELECT count(*) INTO v_pub_cnt
    FROM (SELECT (aclexplode(relacl)).* FROM pg_class
          WHERE oid = 'public.it_vacl_mview'::regclass) a
    WHERE a.grantee = 0  -- 0 = PUBLIC
      AND a.privilege_type = 'SELECT';

    IF v_pub_cnt = 0 THEN
        RAISE EXCEPTION 'ACL lost: PUBLIC SELECT on it_vacl_mview not restored';
    END IF;

    -- ── Cleanup ───────────────────────────────────────────────
    DROP MATERIALIZED VIEW IF EXISTS public.it_vacl_mview;
    DROP VIEW IF EXISTS public.it_vacl_view;
    DROP TABLE IF EXISTS public.it_vacl_base CASCADE;
    DROP ROLE IF EXISTS it_vacl_reader;
    DROP ROLE IF EXISTS it_vacl_writer;
END;
$tv$;
