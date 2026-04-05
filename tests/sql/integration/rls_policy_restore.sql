-- Test: Row-Level Security policies survive flashback_restore().
-- After restore the table must still have RLS enabled and the same policies
-- so that role-based row visibility is not accidentally widened.
DO $tv$
DECLARE
    t_before  timestamptz;
    v_cnt     bigint;
    v_rls_on  bool;
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

    -- Enable RLS
    ALTER TABLE public.it_rls ENABLE ROW LEVEL SECURITY;
    ALTER TABLE public.it_rls FORCE ROW LEVEL SECURITY;

    -- Policy: each role sees only their own rows
    CREATE POLICY it_rls_owner_policy ON public.it_rls
        USING (owner = current_user);

    PERFORM flashback_track('public.it_rls');
    PERFORM flashback_test_attach_capture_trigger('public.it_rls'::regclass);

    INSERT INTO public.it_rls VALUES
        (1, 'it_rls_alice', 'alice_secret'),
        (2, 'it_rls_bob',   'bob_secret'),
        (3, 'it_rls_alice', 'alice_secret2');

    t_before := clock_timestamp();

    DELETE FROM public.it_rls;

    PERFORM flashback_restore('public.it_rls', t_before);

    -- Verify data restored
    SELECT count(*) INTO v_cnt FROM public.it_rls;
    IF v_cnt <> 3 THEN
        RAISE EXCEPTION 'expected 3 rows after restore, got %', v_cnt;
    END IF;

    -- Verify RLS is still enabled
    SELECT relrowsecurity INTO v_rls_on
    FROM pg_class WHERE oid = 'public.it_rls'::regclass;

    IF v_rls_on IS NOT TRUE THEN
        RAISE EXCEPTION 'RLS was disabled after restore';
    END IF;

    -- Verify the policy still exists
    SELECT count(*) INTO v_pol_cnt
    FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename  = 'it_rls'
      AND policyname = 'it_rls_owner_policy';

    IF v_pol_cnt = 0 THEN
        RAISE EXCEPTION 'RLS policy it_rls_owner_policy was lost after restore';
    END IF;

    DROP TABLE IF EXISTS public.it_rls CASCADE;
    DROP ROLE IF EXISTS it_rls_alice;
    DROP ROLE IF EXISTS it_rls_bob;
END;
$tv$;
