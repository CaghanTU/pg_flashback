-- A4: the audited-recover operation_id is a backend-local Rust execution
-- context (flashback_internal_set/get/clear_audited_recover_context), never
-- a user-settable GUC. This proves the trust boundary end to end: the old
-- GUC name is gone, a raw restore forged with an unrelated/stale/terminal
-- operation_id is rejected, the context cannot survive a caught exception or
-- a same-connection later restore, and the real begin->execute audited path
-- still works.
--
-- All flashback_test_bootstrap_lifecycle calls share ONE synthetic test
-- capture stream (flashback_internal_open_capture_stream reuses the active
-- stream for the database), and each bootstrap call advances that stream's
-- valid_through_lsn to its own small tracking_id-derived boundary LSN. A
-- later bootstrap call can never move that watermark backward, so every
-- table this file needs is tracked up front, before any scenario injects its
-- own (much larger) DROP-commit LSN and advances the shared stream past
-- where a later bootstrap's tiny boundary would land.
CREATE TEMP TABLE it_audit_boot (
    label text PRIMARY KEY,
    tracking_id bigint NOT NULL,
    boundary_lsn pg_lsn NOT NULL
) ON COMMIT DROP;

DO $setup$
DECLARE
    v_boot jsonb;
    v_label text;
BEGIN
    FOREACH v_label IN ARRAY ARRAY[
        'happy', 'old_guc', 'a', 'b', 'wronglsn', 'terminal', 'failpoint'
    ]
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS public.it_audit_%s CASCADE', v_label);
        EXECUTE format('CREATE TABLE public.it_audit_%s (id int PRIMARY KEY)', v_label);
        v_boot := flashback_test_bootstrap_lifecycle(format('public.it_audit_%s', v_label));
        INSERT INTO it_audit_boot (label, tracking_id, boundary_lsn)
        VALUES (v_label, (v_boot->>'tracking_id')::bigint, (v_boot->>'boundary_lsn')::pg_lsn);
    END LOOP;
END;
$setup$;

-- Scenario: happy path. flashback_recover_plan -> flashback_recover_begin
-- creates a real, correctly-bound operation header (the same header
-- flashback_recover_execute would set the context to and hand to
-- flashback_restore_lsn). The actual destructive restore step is driven
-- through flashback_test_restore_lsn instead of the public
-- flashback_restore_lsn: the public entrypoint additionally requires a real
-- physical logical replication slot via flashback_ensure_active_wal_stream,
-- which pg_test's synthetic bootstrap stream does not provide (a pre-existing
-- test-harness limitation, not something A4 changed -- no test in this suite
-- calls the public flashback_restore_lsn for the same reason). Both entry
-- points share the exact same flashback_restore_lsn_lock_phase and
-- flashback_internal_restore_lsn_core that carry A4's binding checks, so this
-- still proves a legitimately audited context is accepted end to end, not
-- just that forged ones are rejected.
DO $happy$
DECLARE
    v_tid bigint;
    v_xid bigint;
    v_plan jsonb;
    v_begin jsonb;
    v_op bigint;
    v_rows bigint;
    v_binding jsonb;
BEGIN
    SELECT tracking_id INTO v_tid FROM it_audit_boot WHERE label = 'happy';

    -- A2 fixed the DDL hook to capture this literal DROP for real (it runs
    -- via SPI/QUERY context, same as every pg_test statement): the hook's own
    -- capture_drop_dependency_manifests + flashback_stage_local_delta_ddl_event
    -- already stage the exact manifest and a pending DROP event under the
    -- current transaction's real xid. Only finalize that pending event here
    -- (flashback_test_inject_commit, not the "_ddl_" variant that would stage
    -- a second, competing DROP event for the same DROP statement).
    v_xid := (txid_current() % 4294967296)::bigint;
    DROP TABLE public.it_audit_happy;
    PERFORM flashback_test_inject_commit(v_tid, '0/9000'::pg_lsn, clock_timestamp(), v_xid, '[]'::jsonb);
    PERFORM flashback_bind_drop_dependency_manifests();

    v_plan := flashback_recover_plan('public.it_audit_happy');
    IF COALESCE(v_plan->>'status', '') <> 'restorable' THEN
        RAISE EXCEPTION 'audited_recover_context: setup plan not restorable: %', v_plan;
    END IF;

    v_begin := flashback_recover_begin('public.it_audit_happy', v_plan->>'plan_token');
    IF COALESCE(v_begin->>'status', '') <> 'started' THEN
        RAISE EXCEPTION 'audited_recover_context: begin did not start: %', v_begin;
    END IF;
    v_op := (v_begin->>'operation_id')::bigint;

    IF flashback_operation_state(v_op) <> 'started' THEN
        RAISE EXCEPTION 'audited_recover_context: operation % not started', v_op;
    END IF;

    -- Mirrors flashback_recover_execute's own set-restore-clear contract.
    PERFORM flashback_internal_set_audited_recover_context(v_op);
    BEGIN
        v_rows := flashback_test_restore_lsn('public.it_audit_happy', (v_begin->>'target_lsn')::pg_lsn);
    EXCEPTION WHEN OTHERS THEN
        PERFORM flashback_internal_clear_audited_recover_context();
        RAISE;
    END;
    PERFORM flashback_internal_clear_audited_recover_context();

    IF to_regclass('public.it_audit_happy') IS NULL THEN
        RAISE EXCEPTION 'audited_recover_context: table not restored via legitimately audited context';
    END IF;

    SELECT e.payload->'successor' INTO v_binding
    FROM flashback.operation_events e
    WHERE e.operation_id = v_op AND e.event_type = 'applied_coverage_pending'
    ORDER BY e.event_id DESC LIMIT 1;
    IF v_binding IS NULL OR (v_binding->>'tracking_id')::bigint IS DISTINCT FROM v_tid THEN
        RAISE EXCEPTION 'audited_recover_context: applied_coverage_pending binding missing/wrong for operation %: %',
            v_op, v_binding;
    END IF;

    IF flashback_internal_get_audited_recover_context() IS NOT NULL THEN
        RAISE EXCEPTION 'audited_recover_context: context still set after clearing';
    END IF;
END;
$happy$;

-- Scenario: the old GUC name is gone from the extension's own registry (no
-- GucRegistry::define_string_guc call left in src/storage/worker.rs -- see
-- scripts/check_no_audited_recover_guc_surface.sh for the static check).
-- Postgres itself still accepts SET/set_config on any dotted "extension.name"
-- string as an inert placeholder GUC even when no extension claims that exact
-- suffix, so set_config() on the legacy name does not raise here -- that is
-- expected, not a gap: the point is that this placeholder has zero effect,
-- because nothing in pg_flashback reads it anymore. Prove the inertness
-- directly: the real accessor (flashback_internal_get_audited_recover_context,
-- the only thing flashback_restore_lsn ever consults) must not observe it,
-- and locking a live restore in must not pick up its value as a disaster
-- binding either.
DO $old_guc$
DECLARE
    v_tid bigint;
    v_boundary pg_lsn;
    v_op bigint;
    v_locked record;
BEGIN
    SELECT tracking_id, boundary_lsn INTO v_tid, v_boundary FROM it_audit_boot WHERE label = 'old_guc';

    v_op := flashback_operation_begin(
        'recover', 'public.it_audit_old_guc', v_tid,
        1, 'old-guc-token',
        NULL, NULL, v_boundary,
        jsonb_build_object('test', 'audited_recover_context_old_guc')
    );

    PERFORM set_config('pg_flashback.audited_recover_operation_id', v_op::text, true);
    IF flashback_internal_get_audited_recover_context() IS NOT NULL THEN
        RAISE EXCEPTION 'audited_recover_context: legacy GUC value leaked into the real context';
    END IF;

    -- The table is still live (never dropped), so lock_phase's own
    -- "v_audited_op IS NOT NULL" branch must be skipped entirely -- the
    -- legacy GUC's value must not surface as a disaster_event_id binding
    -- from an operation header it was never actually read from.
    SELECT * INTO v_locked
    FROM flashback_restore_lsn_lock_phase('public.it_audit_old_guc', v_boundary);
    IF v_locked.out_disaster_event_id IS NOT NULL THEN
        RAISE EXCEPTION 'audited_recover_context: legacy GUC value produced a disaster_event_id binding: %',
            v_locked.out_disaster_event_id;
    END IF;

    PERFORM set_config('pg_flashback.audited_recover_operation_id', '', true);
END;
$old_guc$;

-- Scenario: PUBLIC/flashback_admin cannot call the internal setter directly
-- (superuser-only Rust guard, independent of the SQL grant).
DO $priv$
DECLARE
    v_failed boolean := false;
BEGIN
    SET LOCAL ROLE flashback_admin;
    BEGIN
        PERFORM flashback_internal_set_audited_recover_context(1);
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
    END;
    RESET ROLE;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'audited_recover_context: flashback_admin could set the audited context directly';
    END IF;
END;
$priv$;

-- Scenario: a forged operation_id belonging to a DIFFERENT tracking_id must
-- be rejected, never silently borrow that lifecycle's disaster identity.
DO $cross_table$
DECLARE
    v_tid_a bigint;
    v_tid_b bigint;
    v_boundary_b pg_lsn;
    v_xid bigint;
    v_drop_a_id bigint;
    v_forged_op bigint;
    v_failed boolean := false;
BEGIN
    SELECT tracking_id INTO v_tid_a FROM it_audit_boot WHERE label = 'a';
    SELECT tracking_id, boundary_lsn INTO v_tid_b, v_boundary_b FROM it_audit_boot WHERE label = 'b';

    v_xid := (txid_current() % 4294967296)::bigint;
    DROP TABLE public.it_audit_a;
    PERFORM flashback_test_inject_ddl_commit(v_tid_a, '0/9100'::pg_lsn, clock_timestamp(), v_xid, 'DROP');
    PERFORM flashback_bind_drop_dependency_manifests();

    SELECT disaster_event_id INTO v_drop_a_id
    FROM flashback.drop_dependency_manifests
    WHERE tracking_id = v_tid_a AND disaster_event_id IS NOT NULL
    ORDER BY disaster_event_id DESC LIMIT 1;
    IF v_drop_a_id IS NULL THEN
        RAISE EXCEPTION 'audited_recover_context: setup: table A DROP not bound';
    END IF;

    -- Forge an operation header for table B that claims table A's DROP.
    v_forged_op := flashback_operation_begin(
        'recover', 'public.it_audit_b', v_tid_b,
        1, 'forged-cross-table-token',
        v_drop_a_id, NULL, v_boundary_b,
        jsonb_build_object('test', 'audited_recover_context_cross_table')
    );
    PERFORM flashback_internal_set_audited_recover_context(v_forged_op);

    BEGIN
        PERFORM flashback_restore_lsn_lock_phase('public.it_audit_b', v_boundary_b);
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
    END;
    PERFORM flashback_internal_clear_audited_recover_context();
    IF NOT v_failed THEN
        RAISE EXCEPTION 'audited_recover_context: cross-table forged disaster_event_id was accepted';
    END IF;
END;
$cross_table$;

-- Scenario: same lifecycle, but a target_lsn that does not match the audited
-- operation's target_lsn must be rejected.
DO $wrong_lsn$
DECLARE
    v_tid bigint;
    v_boundary pg_lsn;
    v_op bigint;
    v_failed boolean := false;
BEGIN
    SELECT tracking_id, boundary_lsn INTO v_tid, v_boundary FROM it_audit_boot WHERE label = 'wronglsn';

    v_op := flashback_operation_begin(
        'recover', 'public.it_audit_wronglsn', v_tid,
        1, 'wrong-lsn-token',
        NULL, NULL, '0/1234'::pg_lsn,
        jsonb_build_object('test', 'audited_recover_context_wrong_lsn')
    );
    PERFORM flashback_internal_set_audited_recover_context(v_op);

    BEGIN
        -- Deliberately restore to a DIFFERENT LSN than the operation header.
        PERFORM flashback_restore_lsn_lock_phase('public.it_audit_wronglsn', v_boundary);
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
    END;
    PERFORM flashback_internal_clear_audited_recover_context();
    IF NOT v_failed THEN
        RAISE EXCEPTION 'audited_recover_context: mismatched target_lsn was accepted';
    END IF;
END;
$wrong_lsn$;

-- Scenario: a terminal (already verified/failed) operation must be rejected,
-- and an unknown operation_id must be rejected.
DO $terminal$
DECLARE
    v_tid bigint;
    v_boundary pg_lsn;
    v_op bigint;
    v_failed boolean := false;
BEGIN
    SELECT tracking_id, boundary_lsn INTO v_tid, v_boundary FROM it_audit_boot WHERE label = 'terminal';

    v_op := flashback_operation_begin(
        'recover', 'public.it_audit_terminal', v_tid,
        1, 'terminal-token',
        NULL, NULL, v_boundary,
        jsonb_build_object('test', 'audited_recover_context_terminal')
    );
    PERFORM flashback_operation_append_event(v_op, 'failed', NULL, 'synthetic', 'forced terminal for test', '{}'::jsonb);

    PERFORM flashback_internal_set_audited_recover_context(v_op);
    BEGIN
        PERFORM flashback_restore_lsn_lock_phase('public.it_audit_terminal', v_boundary);
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
    END;
    PERFORM flashback_internal_clear_audited_recover_context();
    IF NOT v_failed THEN
        RAISE EXCEPTION 'audited_recover_context: terminal operation state was accepted';
    END IF;

    v_failed := false;
    PERFORM flashback_internal_set_audited_recover_context(999999999);
    BEGIN
        PERFORM flashback_restore_lsn_lock_phase('public.it_audit_terminal', v_boundary);
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
    END;
    PERFORM flashback_internal_clear_audited_recover_context();
    IF NOT v_failed THEN
        RAISE EXCEPTION 'audited_recover_context: unknown operation_id was accepted';
    END IF;
END;
$terminal$;

-- Scenario: exception inside the audited restore call must leave the
-- backend-local context cleared (flashback_recover_execute's own
-- BEGIN/EXCEPTION), and a later raw call in the same connection/transaction
-- must not be treated as audited. flashback_recover_execute calls the real
-- flashback_restore_lsn, which deterministically raises in pg_test (no real
-- physical logical replication slot for flashback_ensure_active_wal_stream to
-- find -- the same pre-existing test-harness limitation noted above); that
-- failure is exactly what exercises the BEGIN/EXCEPTION clear-on-exception
-- path under test here, so no synthetic failpoint is needed.
DO $exception_clears$
DECLARE
    v_tid bigint;
    v_boundary pg_lsn;
    v_xid bigint;
    v_plan jsonb;
    v_begin jsonb;
    v_op bigint;
    v_op_event_count bigint;
    v_failed boolean := false;
BEGIN
    SELECT tracking_id, boundary_lsn INTO v_tid, v_boundary FROM it_audit_boot WHERE label = 'failpoint';

    -- See the "happy" scenario above: finalize the hook's own pending DROP
    -- capture rather than staging a second, competing one.
    v_xid := (txid_current() % 4294967296)::bigint;
    DROP TABLE public.it_audit_failpoint;
    PERFORM flashback_test_inject_commit(v_tid, '0/9200'::pg_lsn, clock_timestamp(), v_xid, '[]'::jsonb);
    PERFORM flashback_bind_drop_dependency_manifests();

    v_plan := flashback_recover_plan('public.it_audit_failpoint');
    IF COALESCE(v_plan->>'status', '') <> 'restorable' THEN
        RAISE EXCEPTION 'audited_recover_context: exception-clears setup plan not restorable: %', v_plan;
    END IF;
    v_begin := flashback_recover_begin('public.it_audit_failpoint', v_plan->>'plan_token');
    v_op := (v_begin->>'operation_id')::bigint;

    BEGIN
        PERFORM flashback_recover_execute(
            'public.it_audit_failpoint', v_plan->>'plan_token',
            p_operation_id => v_op
        );
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
    END;

    IF NOT v_failed THEN
        RAISE EXCEPTION 'audited_recover_context: flashback_recover_execute unexpectedly succeeded in the test environment';
    END IF;
    IF flashback_internal_get_audited_recover_context() IS NOT NULL THEN
        RAISE EXCEPTION 'audited_recover_context: context leaked after flashback_recover_execute exception';
    END IF;

    -- Subtransaction rollback alone (the exception above) does not clear
    -- Rust process-local state by itself -- flashback_recover_execute's own
    -- explicit clear in its EXCEPTION handler is what did it, not the
    -- implicit savepoint rollback. Prove that distinction directly: setting
    -- the context and then hitting a caught exception in an unrelated nested
    -- block, with no explicit clear anywhere in between, leaves the context
    -- exactly as an owner-only caller left it.
    PERFORM flashback_internal_set_audited_recover_context(v_op);
    BEGIN
        RAISE EXCEPTION 'synthetic unrelated failure';
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;
    IF flashback_internal_get_audited_recover_context() IS DISTINCT FROM v_op THEN
        RAISE EXCEPTION 'audited_recover_context: unrelated subtransaction rollback unexpectedly cleared the context';
    END IF;
    PERFORM flashback_internal_clear_audited_recover_context();

    -- Same connection, later raw restore: must not be treated as audited.
    -- pg_test globally sets pg_flashback.allow_unaudited_restore=on (see
    -- src/lib.rs postgresql_conf_options) so flashback_restore_lsn's own
    -- "requires audited recover context" gate is deliberately disabled for
    -- the whole suite and cannot be exercised here; what must still hold is
    -- that lock_phase does not silently reuse v_op's now-stale binding for
    -- an unrelated later call. It doesn't: v_op's own event history must be
    -- unchanged by this later, unaudited call.
    SELECT count(*) INTO v_op_event_count
    FROM flashback.operation_events WHERE operation_id = v_op;

    PERFORM flashback_restore_lsn_lock_phase('public.it_audit_failpoint', v_boundary);

    IF (SELECT count(*) FROM flashback.operation_events WHERE operation_id = v_op) <> v_op_event_count THEN
        RAISE EXCEPTION 'audited_recover_context: unrelated raw lock_phase call appended an event to operation %', v_op;
    END IF;
END;
$exception_clears$;
