-- Test: ACL preservation after restore — table owner/grants survive DROP+CREATE
DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/2000'::pg_lsn;
    v_acl_count bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_acl_test CASCADE;
    CREATE TABLE public.it_acl_test (id int PRIMARY KEY, val text);

    DO $inner$
    BEGIN
        EXECUTE 'DROP ROLE IF EXISTS it_acl_reader';
        EXECUTE 'CREATE ROLE it_acl_reader';
    EXCEPTION WHEN duplicate_object THEN NULL;
    END $inner$;

    GRANT SELECT ON public.it_acl_test TO it_acl_reader;

    SELECT flashback_test_bootstrap_lifecycle('public.it_acl_test') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO public.it_acl_test VALUES (1, 'A'), (2, 'B');
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        v_point_lsn,
        clock_timestamp(),
        945001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"val":"A"}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":2,"val":"B"}'::jsonb)
        )
    );

    DELETE FROM public.it_acl_test;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        945002,
        jsonb_build_array(
            jsonb_build_object('op', 'DELETE', 'old', '{"id":1,"val":"A"}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":2,"val":"B"}'::jsonb)
        )
    );

    PERFORM flashback_restore_lsn('public.it_acl_test', v_point_lsn);

    IF (SELECT count(*) FROM public.it_acl_test) <> 2 THEN
        RAISE EXCEPTION 'data not restored';
    END IF;

    SELECT count(*) INTO v_acl_count
    FROM (SELECT (aclexplode(relacl)).grantee FROM pg_class WHERE oid = 'public.it_acl_test'::regclass) sub
    WHERE grantee = (SELECT oid FROM pg_roles WHERE rolname = 'it_acl_reader');

    IF v_acl_count = 0 THEN
        RAISE EXCEPTION 'ACL lost: it_acl_reader no longer has access after restore';
    END IF;
END;
$tv$;
