-- Test: ACL (GRANTs) on dependent views/matviews are automatically restored
-- after flashback_test_restore_lsn() drops and recreates them via DROP TABLE CASCADE.
DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/2000'::pg_lsn;
    v_sel_cnt bigint;
    v_wgo_cnt bigint;
    v_pub_cnt bigint;
BEGIN
    DROP ROLE IF EXISTS it_vacl_reader;
    DROP ROLE IF EXISTS it_vacl_writer;
    CREATE ROLE it_vacl_reader;
    CREATE ROLE it_vacl_writer;

    DROP TABLE IF EXISTS public.it_vacl_base CASCADE;
    CREATE TABLE public.it_vacl_base (id int PRIMARY KEY, val text);

    CREATE VIEW public.it_vacl_view AS
        SELECT id, val FROM public.it_vacl_base;

    GRANT SELECT ON public.it_vacl_view TO it_vacl_reader;
    GRANT SELECT, INSERT ON public.it_vacl_view TO it_vacl_writer WITH GRANT OPTION;

    CREATE MATERIALIZED VIEW public.it_vacl_mview AS
        SELECT id, val FROM public.it_vacl_base WITH NO DATA;

    GRANT SELECT ON public.it_vacl_mview TO PUBLIC;

    SELECT flashback_test_bootstrap_lifecycle('public.it_vacl_base') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO public.it_vacl_base VALUES (1, 'alpha'), (2, 'beta');
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        v_point_lsn,
        clock_timestamp(),
        955001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"val":"alpha"}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":2,"val":"beta"}'::jsonb)
        )
    );

    DELETE FROM public.it_vacl_base;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        955002,
        jsonb_build_array(
            jsonb_build_object('op', 'DELETE', 'old', '{"id":1,"val":"alpha"}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":2,"val":"beta"}'::jsonb)
        )
    );

    PERFORM flashback_test_restore_lsn('public.it_vacl_base', v_point_lsn);

    IF (SELECT count(*) FROM public.it_vacl_base) <> 2 THEN
        RAISE EXCEPTION 'data not restored (expected 2 rows)';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_views
        WHERE schemaname = 'public' AND viewname = 'it_vacl_view'
    ) THEN
        RAISE EXCEPTION 'dependent view it_vacl_view was not recreated';
    END IF;

    SELECT count(*) INTO v_sel_cnt
    FROM (SELECT (aclexplode(relacl)).* FROM pg_class
          WHERE oid = 'public.it_vacl_view'::regclass) a
    WHERE a.grantee = (SELECT oid FROM pg_roles WHERE rolname = 'it_vacl_reader')
      AND a.privilege_type = 'SELECT';

    IF v_sel_cnt = 0 THEN
        RAISE EXCEPTION 'ACL lost: it_vacl_reader SELECT on it_vacl_view not restored';
    END IF;

    SELECT count(*) INTO v_wgo_cnt
    FROM (SELECT (aclexplode(relacl)).* FROM pg_class
          WHERE oid = 'public.it_vacl_view'::regclass) a
    WHERE a.grantee   = (SELECT oid FROM pg_roles WHERE rolname = 'it_vacl_writer')
      AND a.is_grantable = true;

    IF v_wgo_cnt = 0 THEN
        RAISE EXCEPTION 'ACL lost: it_vacl_writer WITH GRANT OPTION on it_vacl_view not restored';
    END IF;

    SELECT count(*) INTO v_pub_cnt
    FROM (SELECT (aclexplode(relacl)).* FROM pg_class
          WHERE oid = 'public.it_vacl_mview'::regclass) a
    WHERE a.grantee = 0
      AND a.privilege_type = 'SELECT';

    IF v_pub_cnt = 0 THEN
        RAISE EXCEPTION 'ACL lost: PUBLIC SELECT on it_vacl_mview not restored';
    END IF;

    DROP MATERIALIZED VIEW IF EXISTS public.it_vacl_mview;
    DROP VIEW IF EXISTS public.it_vacl_view;
    -- No terminal DROP TABLE: pg_test rolls back this whole transaction, and
    -- the restore just performed leaves the successor generation "building"
    -- (not yet active) until that rollback/commit is observed, so a
    -- same-transaction DROP here would trip the schema-contract guard.
    DROP ROLE IF EXISTS it_vacl_reader;
    DROP ROLE IF EXISTS it_vacl_writer;
END;
$tv$;
