-- A3: flashback_build_actual_restore_proof's digest must come from the live
-- catalog (flashback_canonical_inventory_from_relation), not be recomputed
-- from the same schema_def the expected side already used -- otherwise
-- verification can never fail for real catalog drift, only for schema_def
-- disagreeing with itself. Prove it by corrupting the just-swapped-in
-- relation's catalog metadata (index, ACL) between materialize and verify,
-- via dedicated test_restore_failpoint values, and confirming restore
-- raises and rolls back rather than reporting a false "verified" success.
DO $setup$
DECLARE
    v_boot jsonb;
BEGIN
    DROP TABLE IF EXISTS public.it_adv_index CASCADE;
    CREATE TABLE public.it_adv_index (id int PRIMARY KEY, v text);
    CREATE INDEX it_adv_index_v_idx ON public.it_adv_index (v);
    INSERT INTO public.it_adv_index VALUES (1, 'a'), (2, 'b');
    v_boot := flashback_test_bootstrap_lifecycle('public.it_adv_index');

    DROP TABLE IF EXISTS public.it_adv_acl CASCADE;
    CREATE TABLE public.it_adv_acl (id int PRIMARY KEY, v text);
    INSERT INTO public.it_adv_acl VALUES (1, 'a');
    GRANT SELECT ON public.it_adv_acl TO PUBLIC;
    v_boot := flashback_test_bootstrap_lifecycle('public.it_adv_acl');

    DROP TABLE IF EXISTS public.it_adv_comment CASCADE;
    CREATE TABLE public.it_adv_comment (id int PRIMARY KEY, v text);
    COMMENT ON TABLE public.it_adv_comment IS 'table comment survives DROP recovery';
    COMMENT ON COLUMN public.it_adv_comment.v IS 'column comment survives DROP recovery';
    INSERT INTO public.it_adv_comment VALUES (1, 'a');
    v_boot := flashback_test_bootstrap_lifecycle('public.it_adv_comment');
END;
$setup$;

-- Scenario: a secondary index silently missing after restore must fail
-- verification, not pass because both sides tautologically re-derive the
-- same schema_def-based expectation.
DO $index_scenario$
DECLARE
    v_tid bigint;
    v_xid bigint;
    v_failed boolean := false;
    v_err text;
BEGIN
    SELECT tracking_id INTO v_tid FROM flashback.tracked_tables WHERE table_name = 'it_adv_index';

    PERFORM flashback_capture_drop_dependency_manifest('public', 'it_adv_index', false);
    v_xid := (txid_current() % 4294967296)::bigint;
    DROP TABLE public.it_adv_index;
    PERFORM flashback_test_inject_ddl_commit(v_tid, '0/9300'::pg_lsn, clock_timestamp(), v_xid, 'DROP');
    PERFORM flashback_bind_drop_dependency_manifests();

    PERFORM set_config('pg_flashback.test_restore_failpoint', 'after_swap_drop_index', true);
    BEGIN
        PERFORM flashback_test_restore_lsn('public.it_adv_index', '0/1040'::pg_lsn);
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
        GET STACKED DIAGNOSTICS v_err = MESSAGE_TEXT;
    END;
    PERFORM set_config('pg_flashback.test_restore_failpoint', '', true);

    IF NOT v_failed THEN
        RAISE EXCEPTION 'restore_verify_adversarial: missing-index restore was not rejected by verification';
    END IF;
    IF v_err NOT ILIKE '%inventory digest mismatch%' THEN
        RAISE EXCEPTION 'restore_verify_adversarial: expected inventory digest mismatch, got: %', v_err;
    END IF;

    -- A clean retry (no failpoint) must succeed, proving the failpoint --
    -- not some unrelated setup problem -- caused the earlier rejection.
    PERFORM flashback_test_restore_lsn('public.it_adv_index', '0/1040'::pg_lsn);
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = 'it_adv_index'
          AND indexname = 'it_adv_index_v_idx'
    ) THEN
        RAISE EXCEPTION 'restore_verify_adversarial: clean retry did not recreate the secondary index';
    END IF;
END;
$index_scenario$;

-- Scenario: a table ACL silently revoked after restore must fail
-- verification the same way.
DO $acl_scenario$
DECLARE
    v_tid bigint;
    v_xid bigint;
    v_failed boolean := false;
    v_err text;
BEGIN
    SELECT tracking_id INTO v_tid FROM flashback.tracked_tables WHERE table_name = 'it_adv_acl';

    PERFORM flashback_capture_drop_dependency_manifest('public', 'it_adv_acl', false);
    v_xid := (txid_current() % 4294967296)::bigint;
    DROP TABLE public.it_adv_acl;
    PERFORM flashback_test_inject_ddl_commit(v_tid, '0/9400'::pg_lsn, clock_timestamp(), v_xid, 'DROP');
    PERFORM flashback_bind_drop_dependency_manifests();

    PERFORM set_config('pg_flashback.test_restore_failpoint', 'after_swap_revoke_acl', true);
    BEGIN
        PERFORM flashback_test_restore_lsn('public.it_adv_acl', '0/1080'::pg_lsn);
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
        GET STACKED DIAGNOSTICS v_err = MESSAGE_TEXT;
    END;
    PERFORM set_config('pg_flashback.test_restore_failpoint', '', true);

    IF NOT v_failed THEN
        RAISE EXCEPTION 'restore_verify_adversarial: revoked-ACL restore was not rejected by verification';
    END IF;
    IF v_err NOT ILIKE '%inventory digest mismatch%' THEN
        RAISE EXCEPTION 'restore_verify_adversarial: expected inventory digest mismatch, got: %', v_err;
    END IF;

    PERFORM flashback_test_restore_lsn('public.it_adv_acl', '0/1080'::pg_lsn);
    IF NOT has_table_privilege('public', 'public.it_adv_acl', 'SELECT') THEN
        RAISE EXCEPTION 'restore_verify_adversarial: clean retry did not restore the PUBLIC SELECT grant';
    END IF;
END;
$acl_scenario$;

-- Scenario: table and column comments must round-trip through a real DROP
-- recovery. Comments were never captured into schema_def before, so both
-- the expected and actual sides were always trivially empty=empty --
-- passing regardless of whether the original comment was preserved or
-- silently lost. Prove capture+recreate actually happened, not just that
-- comparison didn't fail.
DO $comment_scenario$
DECLARE
    v_tid bigint;
    v_xid bigint;
BEGIN
    SELECT tracking_id INTO v_tid FROM flashback.tracked_tables WHERE table_name = 'it_adv_comment';

    PERFORM flashback_capture_drop_dependency_manifest('public', 'it_adv_comment', false);
    v_xid := (txid_current() % 4294967296)::bigint;
    DROP TABLE public.it_adv_comment;
    PERFORM flashback_test_inject_ddl_commit(v_tid, '0/9500'::pg_lsn, clock_timestamp(), v_xid, 'DROP');
    PERFORM flashback_bind_drop_dependency_manifests();

    PERFORM flashback_test_restore_lsn('public.it_adv_comment', '0/1140'::pg_lsn);

    IF (SELECT obj_description('public.it_adv_comment'::regclass, 'pg_class'))
       IS DISTINCT FROM 'table comment survives DROP recovery' THEN
        RAISE EXCEPTION 'restore_verify_adversarial: table comment did not survive restore: %',
            obj_description('public.it_adv_comment'::regclass, 'pg_class');
    END IF;
    IF (SELECT col_description('public.it_adv_comment'::regclass, attnum)
        FROM pg_attribute WHERE attrelid = 'public.it_adv_comment'::regclass AND attname = 'v')
       IS DISTINCT FROM 'column comment survives DROP recovery' THEN
        RAISE EXCEPTION 'restore_verify_adversarial: column comment did not survive restore';
    END IF;
END;
$comment_scenario$;
