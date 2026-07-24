-- Test: flashback_untrack() followed by re-bootstrap on the same table
-- does not corrupt state or leave stale metadata.
DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/5000'::pg_lsn;
    v_cnt bigint;
    v_tracked bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_retrack CASCADE;
    CREATE TABLE public.it_retrack (id int PRIMARY KEY, val text);

    SELECT flashback_test_bootstrap_lifecycle('public.it_retrack') INTO v_boot;
    PERFORM flashback_test_inject_commit(
        (v_boot->>'tracking_id')::bigint,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        950001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"val":"first"}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":2,"val":"track"}'::jsonb)
        )
    );
    DELETE FROM public.it_retrack;
    PERFORM flashback_test_inject_commit(
        (v_boot->>'tracking_id')::bigint,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        950002,
        jsonb_build_array(
            jsonb_build_object('op', 'DELETE', 'old', '{"id":1,"val":"first"}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":2,"val":"track"}'::jsonb)
        )
    );

    PERFORM flashback_untrack('public.it_retrack');

    SELECT count(*) INTO v_tracked
    FROM flashback.tracked_tables
    WHERE rel_oid = 'public.it_retrack'::regclass::oid AND is_active;

    IF v_tracked <> 0 THEN
        RAISE EXCEPTION 'table still appears tracked after untrack';
    END IF;

    SELECT flashback_test_bootstrap_lifecycle('public.it_retrack') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO public.it_retrack VALUES (10, 'second'), (20, 'track'), (30, 'session');
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        v_point_lsn,
        clock_timestamp(),
        950003,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":10,"val":"second"}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":20,"val":"track"}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":30,"val":"session"}'::jsonb)
        )
    );
    DELETE FROM public.it_retrack;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/6000'::pg_lsn,
        clock_timestamp(),
        950004,
        jsonb_build_array(
            jsonb_build_object('op', 'DELETE', 'old', '{"id":10,"val":"second"}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":20,"val":"track"}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":30,"val":"session"}'::jsonb)
        )
    );

    PERFORM flashback_test_restore_lsn('public.it_retrack', v_point_lsn);

    SELECT count(*) INTO v_cnt FROM public.it_retrack;
    IF v_cnt <> 3 THEN
        RAISE EXCEPTION 'expected 3 rows from second session, got %', v_cnt;
    END IF;

    IF EXISTS (SELECT 1 FROM public.it_retrack WHERE id IN (1, 2)) THEN
        RAISE EXCEPTION 'stale rows from first tracking session leaked into restore';
    END IF;

    DROP TABLE IF EXISTS public.it_retrack CASCADE;
END;
$tv$;
