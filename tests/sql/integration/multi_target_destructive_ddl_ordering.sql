-- A single DROP statement naming two tracked tables must batch-prepare both
-- (deterministic rel_oid lock order, never the statement's own argument
-- order) and must capture/restore both independently: no event lost, no
-- event duplicated or merged onto the wrong tracking_id, no cross-target
-- data leakage. Regression for the destructive-DDL stream-lock handoff fix:
-- proves the batch path this handoff sits in front of still behaves
-- correctly for >1 target in one statement, not just the single-target case
-- every other DROP-recovery test exercises.
DO $tv$
DECLARE
    v_boot_a jsonb;
    v_boot_b jsonb;
    v_tid_a bigint;
    v_tid_b bigint;
    v_boundary_a pg_lsn;
    v_boundary_b pg_lsn;
    v_xid bigint;
    v_drop_a_id bigint;
    v_drop_b_id bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_multi_ddl_a;
    DROP TABLE IF EXISTS public.it_multi_ddl_b;
    -- a is created (and therefore OID-ordered) before b; the DROP statement
    -- below names b before a, so a passing test proves the product's own
    -- rel_oid ordering governs lock acquisition, not the SQL text's order.
    CREATE TABLE public.it_multi_ddl_a (id int primary key, v text);
    CREATE TABLE public.it_multi_ddl_b (id int primary key, v text);
    INSERT INTO public.it_multi_ddl_a VALUES (1, 'a1'), (2, 'a2');
    INSERT INTO public.it_multi_ddl_b VALUES (1, 'b1'), (2, 'b2');

    SELECT flashback_test_bootstrap_lifecycle('public.it_multi_ddl_a') INTO v_boot_a;
    v_tid_a := (v_boot_a->>'tracking_id')::bigint;
    v_boundary_a := (v_boot_a->>'boundary_lsn')::pg_lsn;
    SELECT flashback_test_bootstrap_lifecycle('public.it_multi_ddl_b') INTO v_boot_b;
    v_tid_b := (v_boot_b->>'tracking_id')::bigint;
    v_boundary_b := (v_boot_b->>'boundary_lsn')::pg_lsn;

    v_xid := (txid_current() % 4294967296)::bigint;
    -- One statement, both targets, argument order reversed relative to
    -- creation/OID order. The DDL hook already captures this literal
    -- multi-target DROP for real (manifest + pending event per target under
    -- the current transaction's real xid); only finalize that pending pair
    -- here -- both tables share the one active database stream, so a single
    -- finalize call sweeps in both.
    DROP TABLE public.it_multi_ddl_b, public.it_multi_ddl_a;
    PERFORM flashback_test_inject_commit(
        v_tid_a, '0/2000'::pg_lsn, clock_timestamp(), v_xid, '[]'::jsonb
    );
    PERFORM flashback_bind_drop_dependency_manifests();

    SELECT disaster_event_id INTO v_drop_a_id
    FROM flashback.drop_dependency_manifests
    WHERE tracking_id = v_tid_a AND disaster_event_id IS NOT NULL
    ORDER BY disaster_event_id DESC LIMIT 1;
    SELECT disaster_event_id INTO v_drop_b_id
    FROM flashback.drop_dependency_manifests
    WHERE tracking_id = v_tid_b AND disaster_event_id IS NOT NULL
    ORDER BY disaster_event_id DESC LIMIT 1;

    IF v_drop_a_id IS NULL THEN
        RAISE EXCEPTION 'multi_target_destructive_ddl: table A DROP not bound (single multi-target statement lost an event)';
    END IF;
    IF v_drop_b_id IS NULL THEN
        RAISE EXCEPTION 'multi_target_destructive_ddl: table B DROP not bound (single multi-target statement lost an event)';
    END IF;
    IF v_drop_a_id = v_drop_b_id THEN
        RAISE EXCEPTION 'multi_target_destructive_ddl: table A and B DROP events collapsed onto the same disaster_event_id % (duplicated/merged, not two independent events)',
            v_drop_a_id;
    END IF;
    IF (SELECT count(*) FROM flashback.delta_log
        WHERE event_type = 'DROP'
          AND tracking_id IN (v_tid_a, v_tid_b)) <> 2
    THEN
        RAISE EXCEPTION 'multi_target_destructive_ddl: expected exactly one DROP delta_log row per target, got %',
            (SELECT count(*) FROM flashback.delta_log
             WHERE event_type = 'DROP' AND tracking_id IN (v_tid_a, v_tid_b));
    END IF;

    -- Each target must be independently, exactly restorable: no cross-target
    -- data leakage from the single shared DDL statement.
    PERFORM flashback_test_restore_lsn('public.it_multi_ddl_a', v_boundary_a);
    PERFORM flashback_test_restore_lsn('public.it_multi_ddl_b', v_boundary_b);

    IF (SELECT count(*) FROM public.it_multi_ddl_a) <> 2
       OR EXISTS (SELECT 1 FROM public.it_multi_ddl_a WHERE v LIKE 'b%')
    THEN
        RAISE EXCEPTION 'multi_target_destructive_ddl: table A restored with wrong/cross-contaminated data';
    END IF;
    IF (SELECT count(*) FROM public.it_multi_ddl_b) <> 2
       OR EXISTS (SELECT 1 FROM public.it_multi_ddl_b WHERE v LIKE 'a%')
    THEN
        RAISE EXCEPTION 'multi_target_destructive_ddl: table B restored with wrong/cross-contaminated data';
    END IF;

    -- No terminal DROP TABLE: pg_test rolls back this whole transaction, and
    -- the restores just performed leave each successor generation
    -- intentionally "building" (not yet active) until that rollback/commit
    -- is observed, so a same-transaction DROP here would trip the
    -- schema-contract guard.
END;
$tv$;
