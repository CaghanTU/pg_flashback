-- Test: View reloptions (security_barrier, check_option) survive flashback_restore_lsn().
DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/2000'::pg_lsn;
    v_barrier bool;
    v_check_opt text;
BEGIN
    DROP TABLE IF EXISTS public.it_vopts_base CASCADE;
    CREATE TABLE public.it_vopts_base (id int PRIMARY KEY, active bool, val text);

    CREATE VIEW public.it_vopts_secure WITH (security_barrier = true) AS
        SELECT id, val FROM public.it_vopts_base WHERE active = true;

    CREATE VIEW public.it_vopts_check AS
        SELECT id, val FROM public.it_vopts_base WHERE active = true
        WITH LOCAL CHECK OPTION;

    SELECT flashback_test_bootstrap_lifecycle('public.it_vopts_base') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO public.it_vopts_base VALUES (1, true, 'visible'), (2, false, 'hidden');
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        v_point_lsn,
        clock_timestamp(),
        953001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"active":true,"val":"visible"}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":2,"active":false,"val":"hidden"}'::jsonb)
        )
    );

    DELETE FROM public.it_vopts_base;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        953002,
        jsonb_build_array(
            jsonb_build_object('op', 'DELETE', 'old', '{"id":1,"active":true,"val":"visible"}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":2,"active":false,"val":"hidden"}'::jsonb)
        )
    );

    PERFORM flashback_restore_lsn('public.it_vopts_base', v_point_lsn);

    IF (SELECT count(*) FROM public.it_vopts_base) <> 2 THEN
        RAISE EXCEPTION 'data not restored (expected 2 rows)';
    END IF;

    SELECT (reloptions::text[] @> ARRAY['security_barrier=true'])
      INTO v_barrier
    FROM pg_class
    WHERE oid = 'public.it_vopts_secure'::regclass;

    IF v_barrier IS NOT TRUE THEN
        RAISE EXCEPTION 'security_barrier option lost after restore';
    END IF;

    SELECT option_name || '=' || option_value INTO v_check_opt
    FROM pg_options_to_table(
        (SELECT reloptions FROM pg_class WHERE oid = 'public.it_vopts_check'::regclass)
    )
    WHERE option_name = 'check_option';

    IF v_check_opt IS NULL THEN
        RAISE EXCEPTION 'check_option not preserved after restore';
    END IF;

    DROP VIEW IF EXISTS public.it_vopts_check;
    DROP VIEW IF EXISTS public.it_vopts_secure;
    DROP TABLE IF EXISTS public.it_vopts_base CASCADE;
END;
$tv$;
