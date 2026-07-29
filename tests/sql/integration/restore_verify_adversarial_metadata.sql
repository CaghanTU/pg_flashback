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

    DROP TABLE IF EXISTS public.it_adv_comment2 CASCADE;
    CREATE TABLE public.it_adv_comment2 (id int PRIMARY KEY, v text);
    COMMENT ON TABLE public.it_adv_comment2 IS 'adversarial comment table';
    INSERT INTO public.it_adv_comment2 VALUES (1, 'a');
    v_boot := flashback_test_bootstrap_lifecycle('public.it_adv_comment2');

    DROP TABLE IF EXISTS public.it_adv_pk CASCADE;
    CREATE TABLE public.it_adv_pk (id int PRIMARY KEY, v text);
    INSERT INTO public.it_adv_pk VALUES (1, 'a');
    v_boot := flashback_test_bootstrap_lifecycle('public.it_adv_pk');

    DROP TABLE IF EXISTS public.it_adv_constraint CASCADE;
    CREATE TABLE public.it_adv_constraint (
        id int PRIMARY KEY, v text, u text UNIQUE, c int CHECK (c > 0)
    );
    INSERT INTO public.it_adv_constraint VALUES (1, 'a', 'u1', 5);
    v_boot := flashback_test_bootstrap_lifecycle('public.it_adv_constraint');

    DROP TABLE IF EXISTS public.it_adv_owner CASCADE;
    CREATE TABLE public.it_adv_owner (id int PRIMARY KEY, v text);
    INSERT INTO public.it_adv_owner VALUES (1, 'a');
    v_boot := flashback_test_bootstrap_lifecycle('public.it_adv_owner');

    DROP TABLE IF EXISTS public.it_adv_rls CASCADE;
    CREATE TABLE public.it_adv_rls (id int PRIMARY KEY, v text);
    ALTER TABLE public.it_adv_rls ENABLE ROW LEVEL SECURITY;
    CREATE POLICY it_adv_rls_pol ON public.it_adv_rls USING (true);
    INSERT INTO public.it_adv_rls VALUES (1, 'a');
    v_boot := flashback_test_bootstrap_lifecycle('public.it_adv_rls');

    DROP TABLE IF EXISTS public.it_adv_force_rls CASCADE;
    CREATE TABLE public.it_adv_force_rls (id int PRIMARY KEY, v text);
    ALTER TABLE public.it_adv_force_rls ENABLE ROW LEVEL SECURITY;
    ALTER TABLE public.it_adv_force_rls FORCE ROW LEVEL SECURITY;
    CREATE POLICY it_adv_force_rls_pol ON public.it_adv_force_rls USING (true);
    INSERT INTO public.it_adv_force_rls VALUES (1, 'a');
    v_boot := flashback_test_bootstrap_lifecycle('public.it_adv_force_rls');

    DROP TABLE IF EXISTS public.it_adv_replident CASCADE;
    CREATE TABLE public.it_adv_replident (id int PRIMARY KEY, v text);
    INSERT INTO public.it_adv_replident VALUES (1, 'a');
    v_boot := flashback_test_bootstrap_lifecycle('public.it_adv_replident');
END;
$setup$;

-- Shared helper pattern for every scenario below: real DROP, inject the
-- commit, bind the manifest, corrupt one specific catalog-level property of
-- the just-restored relation via a dedicated test_restore_failpoint, confirm
-- restore rejects and rolls back with an inventory digest mismatch (never a
-- false "verified"), then confirm a clean retry (no failpoint) both succeeds
-- and genuinely restores the property -- proving the failpoint, not an
-- unrelated setup problem, caused the rejection.

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

    v_xid := (txid_current() % 4294967296)::bigint;
    -- The DDL hook already captures this literal DROP for real (manifest +
    -- pending event under the current transaction's real xid); only finalize
    -- that pending event here, never restage a second, competing one.
    DROP TABLE public.it_adv_index;
    PERFORM flashback_test_inject_commit(v_tid, '0/9300'::pg_lsn, clock_timestamp(), v_xid, '[]'::jsonb);
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

    v_xid := (txid_current() % 4294967296)::bigint;
    -- The DDL hook already captures this literal DROP for real (manifest +
    -- pending event under the current transaction's real xid); only finalize
    -- that pending event here, never restage a second, competing one.
    DROP TABLE public.it_adv_acl;
    PERFORM flashback_test_inject_commit(v_tid, '0/9400'::pg_lsn, clock_timestamp(), v_xid, '[]'::jsonb);
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

    v_xid := (txid_current() % 4294967296)::bigint;
    -- The DDL hook already captures this literal DROP for real (manifest +
    -- pending event under the current transaction's real xid); only finalize
    -- that pending event here, never restage a second, competing one.
    DROP TABLE public.it_adv_comment;
    PERFORM flashback_test_inject_commit(v_tid, '0/9500'::pg_lsn, clock_timestamp(), v_xid, '[]'::jsonb);
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

-- Scenario: primary key dropped post-swap. Verified via the inventory's
-- separate 'primary_key' column-name array (distinct code path from the
-- general 'constraints' array, which excludes contype='p' to match
-- schema_def's own collection).
DO $pk_scenario$
DECLARE
    v_tid bigint;
    v_xid bigint;
    v_failed boolean := false;
    v_err text;
    v_boundary_lsn pg_lsn;
    v_drop_lsn pg_lsn;
    v_target_lsn pg_lsn;
BEGIN
    SELECT tracking_id INTO v_tid FROM flashback.tracked_tables WHERE table_name = 'it_adv_pk';
    SELECT boundary_lsn INTO v_boundary_lsn FROM flashback.coverage_generations
     WHERE tracking_id = v_tid AND state = 'active';
    -- Offsets are relative to the REAL current WAL position, not the
    -- table's own (small, synthetic) boundary_lsn: earlier scenarios'
    -- successful retries perform real restores that advance the shared
    -- test stream's frontier using pg_current_wal_insert_lsn(), which by
    -- this point in the test already exceeds any small boundary-relative
    -- offset. Anchoring to the real WAL position keeps every subsequent
    -- scenario's LSNs monotonically increasing regardless of run order.
    v_target_lsn := pg_current_wal_insert_lsn() + 500000;
    v_drop_lsn := pg_current_wal_insert_lsn() + 1000000;

    v_xid := (txid_current() % 4294967296)::bigint;
    -- The DDL hook already captures this literal DROP for real (manifest +
    -- pending event under the current transaction's real xid); only finalize
    -- that pending event here, never restage a second, competing one.
    DROP TABLE public.it_adv_pk;
    PERFORM flashback_test_inject_commit(v_tid, v_drop_lsn, clock_timestamp(), v_xid, '[]'::jsonb);
    PERFORM flashback_bind_drop_dependency_manifests();

    PERFORM set_config('pg_flashback.test_restore_failpoint', 'after_swap_drop_pk', true);
    BEGIN
        PERFORM flashback_test_restore_lsn('public.it_adv_pk', v_target_lsn);
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
        GET STACKED DIAGNOSTICS v_err = MESSAGE_TEXT;
    END;
    PERFORM set_config('pg_flashback.test_restore_failpoint', '', true);

    IF NOT v_failed THEN
        RAISE EXCEPTION 'restore_verify_adversarial: dropped-PK restore was not rejected by verification';
    END IF;
    IF v_err NOT ILIKE '%inventory digest mismatch%' THEN
        RAISE EXCEPTION 'restore_verify_adversarial: expected inventory digest mismatch, got: %', v_err;
    END IF;

    PERFORM flashback_test_restore_lsn('public.it_adv_pk', v_target_lsn);
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.it_adv_pk'::regclass AND contype = 'p'
    ) THEN
        RAISE EXCEPTION 'restore_verify_adversarial: clean retry did not recreate the primary key';
    END IF;
END;
$pk_scenario$;

-- Scenario: a UNIQUE/CHECK constraint dropped post-swap (the general
-- 'constraints' array, separate from primary_key above).
DO $constraint_scenario$
DECLARE
    v_tid bigint;
    v_xid bigint;
    v_failed boolean := false;
    v_err text;
    v_boundary_lsn pg_lsn;
    v_drop_lsn pg_lsn;
    v_target_lsn pg_lsn;
BEGIN
    SELECT tracking_id INTO v_tid FROM flashback.tracked_tables WHERE table_name = 'it_adv_constraint';
    SELECT boundary_lsn INTO v_boundary_lsn FROM flashback.coverage_generations
     WHERE tracking_id = v_tid AND state = 'active';
    -- Offsets are relative to the REAL current WAL position, not the
    -- table's own (small, synthetic) boundary_lsn: earlier scenarios'
    -- successful retries perform real restores that advance the shared
    -- test stream's frontier using pg_current_wal_insert_lsn(), which by
    -- this point in the test already exceeds any small boundary-relative
    -- offset. Anchoring to the real WAL position keeps every subsequent
    -- scenario's LSNs monotonically increasing regardless of run order.
    v_target_lsn := pg_current_wal_insert_lsn() + 500000;
    v_drop_lsn := pg_current_wal_insert_lsn() + 1000000;

    v_xid := (txid_current() % 4294967296)::bigint;
    -- The DDL hook already captures this literal DROP for real (manifest +
    -- pending event under the current transaction's real xid); only finalize
    -- that pending event here, never restage a second, competing one.
    DROP TABLE public.it_adv_constraint;
    PERFORM flashback_test_inject_commit(v_tid, v_drop_lsn, clock_timestamp(), v_xid, '[]'::jsonb);
    PERFORM flashback_bind_drop_dependency_manifests();

    PERFORM set_config('pg_flashback.test_restore_failpoint', 'after_swap_drop_constraint', true);
    BEGIN
        PERFORM flashback_test_restore_lsn('public.it_adv_constraint', v_target_lsn);
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
        GET STACKED DIAGNOSTICS v_err = MESSAGE_TEXT;
    END;
    PERFORM set_config('pg_flashback.test_restore_failpoint', '', true);

    IF NOT v_failed THEN
        RAISE EXCEPTION 'restore_verify_adversarial: dropped-constraint restore was not rejected by verification';
    END IF;
    IF v_err NOT ILIKE '%inventory digest mismatch%' THEN
        RAISE EXCEPTION 'restore_verify_adversarial: expected inventory digest mismatch, got: %', v_err;
    END IF;

    PERFORM flashback_test_restore_lsn('public.it_adv_constraint', v_target_lsn);
    IF (SELECT count(*) FROM pg_constraint
        WHERE conrelid = 'public.it_adv_constraint'::regclass AND contype IN ('u', 'c')) <> 2
    THEN
        RAISE EXCEPTION 'restore_verify_adversarial: clean retry did not recreate both constraints';
    END IF;
END;
$constraint_scenario$;

-- Scenario: owner changed post-swap (distinct from ACL revoke -- 'owner' is
-- its own key in the inventory).
DO $owner_scenario$
DECLARE
    v_tid bigint;
    v_xid bigint;
    v_failed boolean := false;
    v_err text;
    v_orig_owner text;
    v_boundary_lsn pg_lsn;
    v_drop_lsn pg_lsn;
    v_target_lsn pg_lsn;
BEGIN
    SELECT tracking_id INTO v_tid FROM flashback.tracked_tables WHERE table_name = 'it_adv_owner';
    SELECT boundary_lsn INTO v_boundary_lsn FROM flashback.coverage_generations
     WHERE tracking_id = v_tid AND state = 'active';
    -- Offsets are relative to the REAL current WAL position, not the
    -- table's own (small, synthetic) boundary_lsn: earlier scenarios'
    -- successful retries perform real restores that advance the shared
    -- test stream's frontier using pg_current_wal_insert_lsn(), which by
    -- this point in the test already exceeds any small boundary-relative
    -- offset. Anchoring to the real WAL position keeps every subsequent
    -- scenario's LSNs monotonically increasing regardless of run order.
    v_target_lsn := pg_current_wal_insert_lsn() + 500000;
    v_drop_lsn := pg_current_wal_insert_lsn() + 1000000;
    v_orig_owner := (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'public.it_adv_owner'::regclass);

    v_xid := (txid_current() % 4294967296)::bigint;
    -- The DDL hook already captures this literal DROP for real (manifest +
    -- pending event under the current transaction's real xid); only finalize
    -- that pending event here, never restage a second, competing one.
    DROP TABLE public.it_adv_owner;
    PERFORM flashback_test_inject_commit(v_tid, v_drop_lsn, clock_timestamp(), v_xid, '[]'::jsonb);
    PERFORM flashback_bind_drop_dependency_manifests();

    PERFORM set_config('pg_flashback.test_restore_failpoint', 'after_swap_change_owner', true);
    BEGIN
        PERFORM flashback_test_restore_lsn('public.it_adv_owner', v_target_lsn);
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
        GET STACKED DIAGNOSTICS v_err = MESSAGE_TEXT;
    END;
    PERFORM set_config('pg_flashback.test_restore_failpoint', '', true);

    IF NOT v_failed THEN
        RAISE EXCEPTION 'restore_verify_adversarial: changed-owner restore was not rejected by verification';
    END IF;
    IF v_err NOT ILIKE '%inventory digest mismatch%' THEN
        RAISE EXCEPTION 'restore_verify_adversarial: expected inventory digest mismatch, got: %', v_err;
    END IF;

    PERFORM flashback_test_restore_lsn('public.it_adv_owner', v_target_lsn);
    IF (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'public.it_adv_owner'::regclass)
       IS DISTINCT FROM v_orig_owner
    THEN
        RAISE EXCEPTION 'restore_verify_adversarial: clean retry did not restore the original owner';
    END IF;
END;
$owner_scenario$;

-- Scenario: RLS disabled post-swap.
DO $rls_scenario$
DECLARE
    v_tid bigint;
    v_xid bigint;
    v_failed boolean := false;
    v_err text;
    v_boundary_lsn pg_lsn;
    v_drop_lsn pg_lsn;
    v_target_lsn pg_lsn;
BEGIN
    SELECT tracking_id INTO v_tid FROM flashback.tracked_tables WHERE table_name = 'it_adv_rls';
    SELECT boundary_lsn INTO v_boundary_lsn FROM flashback.coverage_generations
     WHERE tracking_id = v_tid AND state = 'active';
    -- Offsets are relative to the REAL current WAL position, not the
    -- table's own (small, synthetic) boundary_lsn: earlier scenarios'
    -- successful retries perform real restores that advance the shared
    -- test stream's frontier using pg_current_wal_insert_lsn(), which by
    -- this point in the test already exceeds any small boundary-relative
    -- offset. Anchoring to the real WAL position keeps every subsequent
    -- scenario's LSNs monotonically increasing regardless of run order.
    v_target_lsn := pg_current_wal_insert_lsn() + 500000;
    v_drop_lsn := pg_current_wal_insert_lsn() + 1000000;

    v_xid := (txid_current() % 4294967296)::bigint;
    -- The DDL hook already captures this literal DROP for real (manifest +
    -- pending event under the current transaction's real xid); only finalize
    -- that pending event here, never restage a second, competing one.
    DROP TABLE public.it_adv_rls;
    PERFORM flashback_test_inject_commit(v_tid, v_drop_lsn, clock_timestamp(), v_xid, '[]'::jsonb);
    PERFORM flashback_bind_drop_dependency_manifests();

    PERFORM set_config('pg_flashback.test_restore_failpoint', 'after_swap_disable_rls', true);
    BEGIN
        PERFORM flashback_test_restore_lsn('public.it_adv_rls', v_target_lsn);
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
        GET STACKED DIAGNOSTICS v_err = MESSAGE_TEXT;
    END;
    PERFORM set_config('pg_flashback.test_restore_failpoint', '', true);

    IF NOT v_failed THEN
        RAISE EXCEPTION 'restore_verify_adversarial: disabled-RLS restore was not rejected by verification';
    END IF;
    IF v_err NOT ILIKE '%inventory digest mismatch%' THEN
        RAISE EXCEPTION 'restore_verify_adversarial: expected inventory digest mismatch, got: %', v_err;
    END IF;

    PERFORM flashback_test_restore_lsn('public.it_adv_rls', v_target_lsn);
    IF NOT (SELECT relrowsecurity FROM pg_class WHERE oid = 'public.it_adv_rls'::regclass) THEN
        RAISE EXCEPTION 'restore_verify_adversarial: clean retry did not re-enable RLS';
    END IF;
END;
$rls_scenario$;

-- Scenario: FORCE RLS disabled post-swap (distinct from RLS enable/disable
-- above -- relforcerowsecurity is its own inventory key).
DO $force_rls_scenario$
DECLARE
    v_tid bigint;
    v_xid bigint;
    v_failed boolean := false;
    v_err text;
    v_boundary_lsn pg_lsn;
    v_drop_lsn pg_lsn;
    v_target_lsn pg_lsn;
BEGIN
    SELECT tracking_id INTO v_tid FROM flashback.tracked_tables WHERE table_name = 'it_adv_force_rls';
    SELECT boundary_lsn INTO v_boundary_lsn FROM flashback.coverage_generations
     WHERE tracking_id = v_tid AND state = 'active';
    -- Offsets are relative to the REAL current WAL position, not the
    -- table's own (small, synthetic) boundary_lsn: earlier scenarios'
    -- successful retries perform real restores that advance the shared
    -- test stream's frontier using pg_current_wal_insert_lsn(), which by
    -- this point in the test already exceeds any small boundary-relative
    -- offset. Anchoring to the real WAL position keeps every subsequent
    -- scenario's LSNs monotonically increasing regardless of run order.
    v_target_lsn := pg_current_wal_insert_lsn() + 500000;
    v_drop_lsn := pg_current_wal_insert_lsn() + 1000000;

    v_xid := (txid_current() % 4294967296)::bigint;
    -- The DDL hook already captures this literal DROP for real (manifest +
    -- pending event under the current transaction's real xid); only finalize
    -- that pending event here, never restage a second, competing one.
    DROP TABLE public.it_adv_force_rls;
    PERFORM flashback_test_inject_commit(v_tid, v_drop_lsn, clock_timestamp(), v_xid, '[]'::jsonb);
    PERFORM flashback_bind_drop_dependency_manifests();

    PERFORM set_config('pg_flashback.test_restore_failpoint', 'after_swap_disable_force_rls', true);
    BEGIN
        PERFORM flashback_test_restore_lsn('public.it_adv_force_rls', v_target_lsn);
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
        GET STACKED DIAGNOSTICS v_err = MESSAGE_TEXT;
    END;
    PERFORM set_config('pg_flashback.test_restore_failpoint', '', true);

    IF NOT v_failed THEN
        RAISE EXCEPTION 'restore_verify_adversarial: disabled-FORCE-RLS restore was not rejected by verification';
    END IF;
    IF v_err NOT ILIKE '%inventory digest mismatch%' THEN
        RAISE EXCEPTION 'restore_verify_adversarial: expected inventory digest mismatch, got: %', v_err;
    END IF;

    PERFORM flashback_test_restore_lsn('public.it_adv_force_rls', v_target_lsn);
    IF NOT (SELECT relforcerowsecurity FROM pg_class WHERE oid = 'public.it_adv_force_rls'::regclass) THEN
        RAISE EXCEPTION 'restore_verify_adversarial: clean retry did not re-force RLS';
    END IF;
END;
$force_rls_scenario$;

-- Scenario: replica identity changed away from FULL post-swap. Restore
-- always forces FULL while a table is actively tracked; DEFAULT is a
-- detectable drift against flashback_build_expected_restore_proof's
-- hardcoded expected value ('f'), not a value schema_def ever supplies.
DO $replident_scenario$
DECLARE
    v_tid bigint;
    v_xid bigint;
    v_failed boolean := false;
    v_err text;
    v_boundary_lsn pg_lsn;
    v_drop_lsn pg_lsn;
    v_target_lsn pg_lsn;
BEGIN
    SELECT tracking_id INTO v_tid FROM flashback.tracked_tables WHERE table_name = 'it_adv_replident';
    SELECT boundary_lsn INTO v_boundary_lsn FROM flashback.coverage_generations
     WHERE tracking_id = v_tid AND state = 'active';
    -- Offsets are relative to the REAL current WAL position, not the
    -- table's own (small, synthetic) boundary_lsn: earlier scenarios'
    -- successful retries perform real restores that advance the shared
    -- test stream's frontier using pg_current_wal_insert_lsn(), which by
    -- this point in the test already exceeds any small boundary-relative
    -- offset. Anchoring to the real WAL position keeps every subsequent
    -- scenario's LSNs monotonically increasing regardless of run order.
    v_target_lsn := pg_current_wal_insert_lsn() + 500000;
    v_drop_lsn := pg_current_wal_insert_lsn() + 1000000;

    v_xid := (txid_current() % 4294967296)::bigint;
    -- The DDL hook already captures this literal DROP for real (manifest +
    -- pending event under the current transaction's real xid); only finalize
    -- that pending event here, never restage a second, competing one.
    DROP TABLE public.it_adv_replident;
    PERFORM flashback_test_inject_commit(v_tid, v_drop_lsn, clock_timestamp(), v_xid, '[]'::jsonb);
    PERFORM flashback_bind_drop_dependency_manifests();

    PERFORM set_config('pg_flashback.test_restore_failpoint', 'after_swap_change_replica_identity', true);
    BEGIN
        PERFORM flashback_test_restore_lsn('public.it_adv_replident', v_target_lsn);
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
        GET STACKED DIAGNOSTICS v_err = MESSAGE_TEXT;
    END;
    PERFORM set_config('pg_flashback.test_restore_failpoint', '', true);

    IF NOT v_failed THEN
        RAISE EXCEPTION 'restore_verify_adversarial: changed-replica-identity restore was not rejected by verification';
    END IF;
    IF v_err NOT ILIKE '%inventory digest mismatch%' THEN
        RAISE EXCEPTION 'restore_verify_adversarial: expected inventory digest mismatch, got: %', v_err;
    END IF;

    PERFORM flashback_test_restore_lsn('public.it_adv_replident', v_target_lsn);
    IF (SELECT relreplident FROM pg_class WHERE oid = 'public.it_adv_replident'::regclass) <> 'f' THEN
        RAISE EXCEPTION 'restore_verify_adversarial: clean retry did not force replica identity FULL';
    END IF;
END;
$replident_scenario$;

-- Scenario: table comment silently dropped post-swap (adversarial, distinct
-- from the positive round-trip $comment_scenario above which merely proves
-- capture+recreate happened, not that corruption is caught).
DO $comment_adversarial_scenario$
DECLARE
    v_tid bigint;
    v_xid bigint;
    v_failed boolean := false;
    v_err text;
    v_boundary_lsn pg_lsn;
    v_drop_lsn pg_lsn;
    v_target_lsn pg_lsn;
BEGIN
    SELECT tracking_id INTO v_tid FROM flashback.tracked_tables WHERE table_name = 'it_adv_comment2';
    SELECT boundary_lsn INTO v_boundary_lsn FROM flashback.coverage_generations
     WHERE tracking_id = v_tid AND state = 'active';
    -- Offsets are relative to the REAL current WAL position, not the
    -- table's own (small, synthetic) boundary_lsn: earlier scenarios'
    -- successful retries perform real restores that advance the shared
    -- test stream's frontier using pg_current_wal_insert_lsn(), which by
    -- this point in the test already exceeds any small boundary-relative
    -- offset. Anchoring to the real WAL position keeps every subsequent
    -- scenario's LSNs monotonically increasing regardless of run order.
    v_target_lsn := pg_current_wal_insert_lsn() + 500000;
    v_drop_lsn := pg_current_wal_insert_lsn() + 1000000;

    v_xid := (txid_current() % 4294967296)::bigint;
    -- The DDL hook already captures this literal DROP for real (manifest +
    -- pending event under the current transaction's real xid); only finalize
    -- that pending event here, never restage a second, competing one.
    DROP TABLE public.it_adv_comment2;
    PERFORM flashback_test_inject_commit(v_tid, v_drop_lsn, clock_timestamp(), v_xid, '[]'::jsonb);
    PERFORM flashback_bind_drop_dependency_manifests();

    PERFORM set_config('pg_flashback.test_restore_failpoint', 'after_swap_drop_comment', true);
    BEGIN
        PERFORM flashback_test_restore_lsn('public.it_adv_comment2', v_target_lsn);
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
        GET STACKED DIAGNOSTICS v_err = MESSAGE_TEXT;
    END;
    PERFORM set_config('pg_flashback.test_restore_failpoint', '', true);

    IF NOT v_failed THEN
        RAISE EXCEPTION 'restore_verify_adversarial: dropped-comment restore was not rejected by verification';
    END IF;
    IF v_err NOT ILIKE '%inventory digest mismatch%' THEN
        RAISE EXCEPTION 'restore_verify_adversarial: expected inventory digest mismatch, got: %', v_err;
    END IF;

    PERFORM flashback_test_restore_lsn('public.it_adv_comment2', v_target_lsn);
    IF (SELECT obj_description('public.it_adv_comment2'::regclass, 'pg_class'))
       IS DISTINCT FROM 'adversarial comment table'
    THEN
        RAISE EXCEPTION 'restore_verify_adversarial: clean retry did not restore the table comment';
    END IF;
END;
$comment_adversarial_scenario$;
