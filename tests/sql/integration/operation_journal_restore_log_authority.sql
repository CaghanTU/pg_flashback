-- A7: flashback.restore_log must be a genuine projection of the operation
-- journal's single authority (flashback.operations/operation_events), not a
-- second, independent writer. Before this fix, flashback_internal_restore_lsn_core
-- wrote a restore_log row with success=true unconditionally, synchronously,
-- for every restore including audited ones -- before the operation journal's
-- own async finalizer had decided whether the recover operation actually
-- reached 'verified'. Since restore_log only ever wrote true (a data-level
-- verification failure raises and rolls back the whole transaction instead
-- of reaching that INSERT), its 'success' column -- and the pg_stat_flashback
-- 'failed_restores' count derived from it -- were structurally incapable of
-- ever reflecting a real failed/abandoned recover. This test proves the
-- audited path now writes restore_log only from the journal's own terminal
-- transition, with the correct success value in each case.
DO $setup$
DECLARE
    v_boot jsonb;
BEGIN
    DROP TABLE IF EXISTS public.it_a7_journal CASCADE;
    CREATE TABLE public.it_a7_journal (id int PRIMARY KEY, v text);
    INSERT INTO public.it_a7_journal VALUES (1, 'a');
    v_boot := flashback_test_bootstrap_lifecycle('public.it_a7_journal');

    DROP TABLE IF EXISTS public.it_a7_journal_abandoned CASCADE;
    CREATE TABLE public.it_a7_journal_abandoned (id int PRIMARY KEY, v text);
    INSERT INTO public.it_a7_journal_abandoned VALUES (1, 'a');
    v_boot := flashback_test_bootstrap_lifecycle('public.it_a7_journal_abandoned');
END;
$setup$;

-- Scenario: an audited recover whose real flashback_restore_lsn call fails
-- (missing physical slot, unavoidable under pg_test -- see
-- audited_recover_context_failclosed.sql's identical exception_clears
-- scenario) must not have left any restore_log row from the core-restore
-- attempt itself; flashback_recover_mark_failed (the client/reconciler,
-- called separately as production requires) is what records the outcome.
DO $failed_scenario$
DECLARE
    v_tid bigint;
    v_xid bigint;
    v_plan jsonb;
    v_begin jsonb;
    v_op bigint;
    v_failed boolean := false;
    v_before_count bigint;
    v_after_count bigint;
BEGIN
    SELECT tracking_id INTO v_tid FROM flashback.tracked_tables WHERE table_name = 'it_a7_journal';

    PERFORM flashback_capture_drop_dependency_manifest('public', 'it_a7_journal', false);
    v_xid := (txid_current() % 4294967296)::bigint;
    DROP TABLE public.it_a7_journal;
    PERFORM flashback_test_inject_commit(v_tid, '0/9600'::pg_lsn, clock_timestamp(), v_xid, '[]'::jsonb);
    PERFORM flashback_bind_drop_dependency_manifests();

    v_plan := flashback_recover_plan('public.it_a7_journal');
    IF COALESCE(v_plan->>'status', '') <> 'restorable' THEN
        RAISE EXCEPTION 'operation_journal_restore_log_authority: plan not restorable: %', v_plan;
    END IF;
    v_begin := flashback_recover_begin('public.it_a7_journal', v_plan->>'plan_token');
    v_op := (v_begin->>'operation_id')::bigint;

    SELECT count(*) INTO v_before_count
    FROM flashback.restore_log WHERE table_name ILIKE '%it_a7_journal%';

    BEGIN
        PERFORM flashback_recover_execute(
            'public.it_a7_journal', v_plan->>'plan_token',
            p_operation_id => v_op
        );
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'operation_journal_restore_log_authority: flashback_recover_execute unexpectedly succeeded in the test environment';
    END IF;

    SELECT count(*) INTO v_after_count
    FROM flashback.restore_log WHERE table_name ILIKE '%it_a7_journal%';
    IF v_after_count <> v_before_count THEN
        RAISE EXCEPTION 'operation_journal_restore_log_authority: a failed audited restore attempt wrote to restore_log before the journal decided the outcome';
    END IF;

    PERFORM flashback_recover_mark_failed(
        v_op, NULL, 'test_forced_failure',
        'operation_journal_restore_log_authority forced failure'
    );

    IF NOT EXISTS (
        SELECT 1 FROM flashback.operation_current_state
        WHERE operation_id = v_op AND state = 'failed'
    ) THEN
        RAISE EXCEPTION 'operation_journal_restore_log_authority: mark_failed did not reach failed state';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM flashback.restore_log
        WHERE table_name ILIKE '%it_a7_journal%'
          AND success = false
          AND error_message = 'operation_journal_restore_log_authority forced failure'
    ) THEN
        RAISE EXCEPTION 'operation_journal_restore_log_authority: restore_log did not record the failed outcome from the operation journal';
    END IF;

    -- Idempotent retry (the reconciler may call this more than once): must
    -- not write a second row for the same terminal outcome.
    SELECT count(*) INTO v_after_count
    FROM flashback.restore_log WHERE table_name ILIKE '%it_a7_journal%' AND success = false;
    PERFORM flashback_recover_mark_failed(
        v_op, NULL, 'test_forced_failure',
        'operation_journal_restore_log_authority forced failure'
    );
    IF (SELECT count(*) FROM flashback.restore_log
        WHERE table_name ILIKE '%it_a7_journal%' AND success = false) <> v_after_count
    THEN
        RAISE EXCEPTION 'operation_journal_restore_log_authority: idempotent mark_failed retry wrote a duplicate restore_log row';
    END IF;
END;
$failed_scenario$;

-- Scenario: a recover that began but was never executed (client crashed
-- between begin and execute) must be classified abandoned by the
-- reconciler, not left silently unrecorded in restore_log forever.
DO $abandoned_scenario$
DECLARE
    v_tid bigint;
    v_xid bigint;
    v_plan jsonb;
    v_begin jsonb;
    v_op bigint;
    v_reconciled integer;
BEGIN
    SELECT tracking_id INTO v_tid
    FROM flashback.tracked_tables WHERE table_name = 'it_a7_journal_abandoned';

    PERFORM flashback_capture_drop_dependency_manifest('public', 'it_a7_journal_abandoned', false);
    v_xid := (txid_current() % 4294967296)::bigint;
    DROP TABLE public.it_a7_journal_abandoned;
    PERFORM flashback_test_inject_commit(v_tid, '0/9700'::pg_lsn, clock_timestamp(), v_xid, '[]'::jsonb);
    PERFORM flashback_bind_drop_dependency_manifests();

    v_plan := flashback_recover_plan('public.it_a7_journal_abandoned');
    IF COALESCE(v_plan->>'status', '') <> 'restorable' THEN
        RAISE EXCEPTION 'operation_journal_restore_log_authority: abandoned-scenario plan not restorable: %', v_plan;
    END IF;
    v_begin := flashback_recover_begin('public.it_a7_journal_abandoned', v_plan->>'plan_token');
    v_op := (v_begin->>'operation_id')::bigint;

    -- Never executed: force immediate staleness instead of waiting out the
    -- real default window.
    v_reconciled := flashback_reconcile_recover_operations(interval '0 seconds');
    IF v_reconciled < 1 THEN
        RAISE EXCEPTION 'operation_journal_restore_log_authority: reconciler did not process the stale started operation';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM flashback.operation_current_state
        WHERE operation_id = v_op AND state = 'abandoned'
    ) THEN
        RAISE EXCEPTION 'operation_journal_restore_log_authority: reconciler did not mark operation abandoned';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM flashback.restore_log
        WHERE table_name ILIKE '%it_a7_journal_abandoned%'
          AND success = false
    ) THEN
        RAISE EXCEPTION 'operation_journal_restore_log_authority: restore_log did not record the abandoned outcome';
    END IF;
END;
$abandoned_scenario$;
