-- Test: Row-Level Security policies survive flashback_test_restore_lsn().
DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/2000'::pg_lsn;
    v_cnt bigint;
    v_rls_on bool;
    v_pol_cnt bigint;
BEGIN
    DROP ROLE IF EXISTS it_rls_alice;
    DROP ROLE IF EXISTS it_rls_bob;
    CREATE ROLE it_rls_alice;
    CREATE ROLE it_rls_bob;

    DROP TABLE IF EXISTS public.it_rls CASCADE;
    CREATE TABLE public.it_rls (
        id      int  PRIMARY KEY,
        owner   text NOT NULL,
        secret  text
    );

    ALTER TABLE public.it_rls ENABLE ROW LEVEL SECURITY;
    ALTER TABLE public.it_rls FORCE ROW LEVEL SECURITY;

    CREATE POLICY it_rls_owner_policy ON public.it_rls
        USING (owner = current_user);

    SELECT flashback_test_bootstrap_lifecycle('public.it_rls') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO public.it_rls VALUES
        (1, 'it_rls_alice', 'alice_secret'),
        (2, 'it_rls_bob',   'bob_secret'),
        (3, 'it_rls_alice', 'alice_secret2');
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        v_point_lsn,
        clock_timestamp(),
        952001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"owner":"it_rls_alice","secret":"alice_secret"}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":2,"owner":"it_rls_bob","secret":"bob_secret"}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":3,"owner":"it_rls_alice","secret":"alice_secret2"}'::jsonb)
        )
    );

    DELETE FROM public.it_rls;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        952002,
        jsonb_build_array(
            jsonb_build_object('op', 'DELETE', 'old', '{"id":1,"owner":"it_rls_alice","secret":"alice_secret"}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":2,"owner":"it_rls_bob","secret":"bob_secret"}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"id":3,"owner":"it_rls_alice","secret":"alice_secret2"}'::jsonb)
        )
    );

    PERFORM flashback_test_restore_lsn('public.it_rls', v_point_lsn);

    SELECT count(*) INTO v_cnt FROM public.it_rls;
    IF v_cnt <> 3 THEN
        RAISE EXCEPTION 'expected 3 rows after restore, got %', v_cnt;
    END IF;

    SELECT relrowsecurity INTO v_rls_on
    FROM pg_class WHERE oid = 'public.it_rls'::regclass;

    IF v_rls_on IS NOT TRUE THEN
        RAISE EXCEPTION 'RLS was disabled after restore';
    END IF;

    SELECT count(*) INTO v_pol_cnt
    FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename  = 'it_rls'
      AND policyname = 'it_rls_owner_policy';

    IF v_pol_cnt = 0 THEN
        RAISE EXCEPTION 'RLS policy it_rls_owner_policy was lost after restore';
    END IF;

    -- No terminal DROP TABLE: pg_test rolls back this whole transaction, and
    -- the restore just performed leaves the successor generation "building"
    -- (not yet active) until that rollback/commit is observed, so a
    -- same-transaction DROP here would trip the schema-contract guard.
    DROP ROLE IF EXISTS it_rls_alice;
    DROP ROLE IF EXISTS it_rls_bob;
END;
$tv$;
