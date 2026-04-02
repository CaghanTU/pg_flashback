-- Test: ACL preservation after restore — table owner/grants survive DROP+CREATE
DO $tv$
DECLARE t_before timestamptz;
        v_acl_count bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_acl_test CASCADE;
    CREATE TABLE public.it_acl_test (id int PRIMARY KEY, val text);

    -- Grant some ACLs
    DO $inner$
    BEGIN
        EXECUTE 'DROP ROLE IF EXISTS it_acl_reader';
        EXECUTE 'CREATE ROLE it_acl_reader';
    EXCEPTION WHEN duplicate_object THEN NULL;
    END $inner$;

    GRANT SELECT ON public.it_acl_test TO it_acl_reader;

    PERFORM flashback_track('public.it_acl_test');
    PERFORM flashback_test_attach_capture_trigger('public.it_acl_test'::regclass);

    INSERT INTO public.it_acl_test VALUES (1, 'A'), (2, 'B');
    t_before := clock_timestamp();

    DELETE FROM public.it_acl_test;

    PERFORM flashback_restore('public.it_acl_test', t_before);

    -- Check data restored
    IF (SELECT count(*) FROM public.it_acl_test) <> 2 THEN
        RAISE EXCEPTION 'data not restored';
    END IF;

    -- Check ACL preserved: it_acl_reader should still have SELECT
    SELECT count(*) INTO v_acl_count
    FROM (SELECT (aclexplode(relacl)).grantee FROM pg_class WHERE oid = 'public.it_acl_test'::regclass) sub
    WHERE grantee = (SELECT oid FROM pg_roles WHERE rolname = 'it_acl_reader');

    IF v_acl_count = 0 THEN
        RAISE EXCEPTION 'ACL lost: it_acl_reader no longer has access after restore';
    END IF;
END;
$tv$;
