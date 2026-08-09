-- Step 9 Phase 2: SQL authority tests for the online external_zstd
-- initial-protection entrypoints (protect_online.sql):
--   flashback_protect_begin / flashback_protect_prepare_replica_identity /
--   flashback_protect_external_copy / flashback_protect_external_publish /
--   flashback_protect_finalize
--
-- Two harness walls this file works honestly around rather than papering
-- over (both already established elsewhere in this stage):
--
--  * pg_flashback.snapshot_storage_backend is GucContext::Sighup (src/
--    storage/worker.rs) -- deliberately not SUSET, so no session/transaction
--    can ever change it, and this test cluster's postgresql_conf_options()
--    (src/lib.rs) does not set it, so it is heap_v1 for every test in this
--    binary. That means flashback_protect_begin's "reject unless
--    external_zstd" branch IS exercised for real below (the harness's
--    actual, permanent state already satisfies its precondition), but the
--    symmetric new guard in flashback_track (reject *when* external_zstd)
--    can never be reached by a real call here -- it is identical in shape
--    to the branch already proven for flashback_protect_begin, just with
--    the condition polarity flipped, and is not independently exercised by
--    an automated test in this harness.
--
--  * flashback_protect_begin's own reservation phase ends with the exact
--    same durable state (tracked_tables 'starting' row + a parentless
--    online generation reservation + a 'protect'/'started' operation
--    journal row) that this file constructs directly below via
--    flashback_internal_reserve_online_generation + flashback_operation_
--    begin -- deliberately skipping flashback_protect_begin's own
--    preflight (topology/compatibility, which flashback_require_supported_
--    local_table/flashback_require_local_compatibility already have their
--    own dedicated test coverage for, and worker-admission/slot-creation,
--    which no committed test in this codebase drives for real -- the exact
--    same "cannot create a logical replication slot outside a bare first-
--    statement session" wall flashback_track's own tests have always had).
--    flashback_protect_external_copy itself additionally launches the real
--    owner-only marker transaction and a real background copier worker,
--    which is exercised by a #[pg_test] Rust function instead (external_
--    zstd_coordinator.rs), not this SQL-level file. flashback_internal_
--    finalize_external_snapshot needs a real staged artifact on disk that a
--    pg_test/probe-mode build never writes (external_zstd_coordinator.rs's
--    #[cfg(not(feature = "pg_test"))] comment documents the same
--    limitation), so flashback_protect_external_publish's own guard logic
--    is proven here by driving the underlying SnapshotStore authority
--    functions directly with synthetic-but-shape-valid evidence -- the
--    exact technique external_snapshot_activation_authority.sql already
--    uses -- and flashback_protect_finalize is called directly rather than
--    only through flashback_protect_external_publish.
--
--  * flashback_protect_finalize's 'activated' terminal additionally
--    requires flashback_internal_protect_activation_readiness (protect_
--    online.sql) to report ready=true, which in turn requires the staged
--    external artifact to pass real payload-health verification -- no
--    sql_test!/pg_test harness in this codebase can produce a real
--    artifact on disk (the same wall documented above for flashback_
--    internal_finalize_external_snapshot). This file proves finalize
--    correctly refuses to activate on that real (not fabricated) negative
--    evidence once the generation is genuinely 'active', and correctly
--    classifies it as a hard failure; it does not and cannot claim the
--    'activated' terminal itself is reached here -- that real happy-path
--    proof is scripts/run_protect_online_happy_path_e2e.sh, against a
--    real isolated PostgreSQL instance with a real external_snapshot_root,
--    a real logical slot, and a real capture worker.
--
--  * This file also proves the single centralized recoverability
--    authority (flashback_internal_lifecycle_actively_protected,
--    coverage_runtime.sql) directly, adversarially: once the generation
--    above is genuinely 'active' while tracked_tables.protection_state is
--    still 'starting' (the exact unsafe state a caller bypassing protect_
--    online.sql's own atomicity, or an older/unfixed build, could
--    produce), flashback_internal_lifecycle_actively_protected,
--    flashback_is_actively_protected, and flashback_admit_lsn_target must
--    all still refuse to treat it as protected/admissible.
DO $test$
DECLARE
    v_db oid := (SELECT oid FROM pg_database WHERE datname = current_database());
    v_stream bigint;
    v_tracking bigint;
    v_rel oid;
    v_begin jsonb;
    v_op bigint;
    v_gen_id bigint;
    v_snap_id bigint;
    v_tt flashback.tracked_tables%ROWTYPE;
    v_op_row record;
    v_raised boolean;
    v_msg text;
    v_bound record;
    v_lsn pg_lsn := '0/B01000'::pg_lsn;
    v_locator jsonb;
    v_schema_hash text;
    v_snap flashback.snapshots%ROWTYPE;
    v_finalize jsonb;
    v_publish jsonb;
BEGIN
    CREATE TABLE public.it_protect_begin (id integer PRIMARY KEY, note text);
    v_rel := 'public.it_protect_begin'::regclass;

    -- 0. flashback_protect_begin() requires pg_flashback.snapshot_storage_
    -- backend = external_zstd; this test cluster's real, permanent setting
    -- is heap_v1 (see header comment), so it must fail closed here with an
    -- actionable hint every time, not just under some specific toggle.
    v_raised := false;
    BEGIN
        PERFORM public.flashback_protect_begin('public.it_protect_begin');
    EXCEPTION WHEN invalid_parameter_value THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT ILIKE '%external_zstd%' THEN
        RAISE EXCEPTION 'flashback_protect_begin must fail closed when snapshot_storage_backend is not external_zstd, got raised=%, msg=%',
            v_raised, v_msg;
    END IF;

    -- 1. Reproduce flashback_protect_begin's own durable reservation
    -- end-state directly (see header comment for exactly why): a 'starting'
    -- tracked_tables row, a parentless generation_no=1 external_zstd
    -- reservation, and a 'protect'/'started' operation journal row.
    v_stream := public.flashback_internal_create_capture_stream(
        p_database_oid => v_db, p_initial_state => 'active',
        p_slot_name => 'it_protect_begin_slot', p_plugin_name => 'pg_flashback_decoder'
    );
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table,
        recovery_profile, is_active, replica_identity_was, protection_state
    ) VALUES (
        v_rel, 'public', 'it_protect_begin', NULL,
        'local_delta', true, 'd', 'starting'
    ) RETURNING tracking_id INTO v_tracking;

    SELECT * INTO v_op_row
    FROM public.flashback_internal_reserve_online_generation(
        p_tracking_id => v_tracking, p_rel_oid => v_rel,
        p_stream_id => v_stream, p_generation_no => 1,
        p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
        p_operation_nonce => 940001
    );
    v_gen_id := v_op_row.generation_id;
    v_snap_id := v_op_row.snapshot_id;

    v_op := public.flashback_operation_begin(
        p_command => 'protect', p_table => 'public.it_protect_begin',
        p_tracking_id => v_tracking, p_generation_id => v_gen_id,
        p_details => jsonb_build_object(
            'snapshot_id', v_snap_id, 'rel_oid', v_rel,
            'operation_nonce', 940001, 'storage_backend', 'external_zstd'
        )
    );

    SELECT * INTO v_tt FROM flashback.tracked_tables WHERE tracking_id = v_tracking;
    IF v_tt.tracking_id IS NULL
       OR v_tt.protection_state IS DISTINCT FROM 'starting'
       OR NOT v_tt.is_active
       OR v_tt.base_snapshot_table IS NOT NULL
       OR v_tt.rel_oid IS DISTINCT FROM v_rel
       OR v_tt.replica_identity_was IS DISTINCT FROM 'd'
    THEN
        RAISE EXCEPTION 'tracked_tables fixture does not have the expected starting shape: %', row_to_json(v_tt);
    END IF;

    SELECT generation_no, state, storage_backend, parent_generation_id
      INTO v_op_row
    FROM flashback.coverage_generations
    WHERE generation_id = v_gen_id AND tracking_id = v_tracking;
    IF v_op_row.generation_no IS DISTINCT FROM 1
       OR v_op_row.state IS DISTINCT FROM 'building'
       OR v_op_row.storage_backend IS DISTINCT FROM 'external_zstd'
       OR v_op_row.parent_generation_id IS NOT NULL
    THEN
        RAISE EXCEPTION 'reserved generation does not have the expected parentless shape: %', v_op_row;
    END IF;

    SELECT s.state, o.command INTO v_op_row
    FROM flashback.operation_current_state s
    JOIN flashback.operations o ON o.operation_id = s.operation_id
    WHERE s.operation_id = v_op;
    IF v_op_row.state IS DISTINCT FROM 'started' OR v_op_row.command IS DISTINCT FROM 'protect' THEN
        RAISE EXCEPTION 'operation journal fixture is not started/protect: %', v_op_row;
    END IF;

    -- 3. flashback_protect_prepare_replica_identity and flashback_protect_
    -- external_copy both reject a wrong/unknown operation id, without ever
    -- touching the real ALTER TABLE or the real marker transaction.
    v_raised := false;
    BEGIN
        PERFORM public.flashback_protect_prepare_replica_identity(-1);
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'flashback_protect_prepare_replica_identity must reject an unknown operation_id';
    END IF;
    v_raised := false;
    BEGIN
        PERFORM public.flashback_protect_external_copy(-1);
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'flashback_protect_external_copy must reject an unknown operation_id';
    END IF;
    -- flashback_protect_external_copy must also refuse to proceed past its
    -- own REPLICA IDENTITY FULL precondition check for a real, still-
    -- 'started' operation whose identity was never prepared -- it must
    -- never silently re-derive/re-apply that step itself under a lock the
    -- copier will need (see protect_online.sql's header comment for why
    -- that must be a separate, already-committed transaction).
    v_raised := false;
    BEGIN
        PERFORM public.flashback_protect_external_copy(v_op);
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT ILIKE '%REPLICA IDENTITY FULL%' THEN
        RAISE EXCEPTION 'flashback_protect_external_copy must refuse a not-yet-FULL identity rather than re-deriving it, got raised=%, msg=%',
            v_raised, v_msg;
    END IF;

    -- 4. flashback_protect_external_publish before the boundary LSN is
    -- resolved must report 'pending', not raise and not touch the real
    -- finalizer.
    v_publish := public.flashback_protect_external_publish(v_op);
    IF v_publish->>'status' IS DISTINCT FROM 'pending' THEN
        RAISE EXCEPTION 'flashback_protect_external_publish before boundary resolution must be pending, got: %', v_publish;
    END IF;

    -- 5. flashback_protect_finalize before the generation is 'active' must
    -- also report 'pending', never raise, never touch protection_state.
    v_finalize := public.flashback_protect_finalize(v_op);
    IF v_finalize->>'status' IS DISTINCT FROM 'pending' THEN
        RAISE EXCEPTION 'flashback_protect_finalize before generation activation must be pending, got: %', v_finalize;
    END IF;
    IF (SELECT protection_state FROM flashback.tracked_tables WHERE tracking_id = v_tracking) IS DISTINCT FROM 'starting' THEN
        RAISE EXCEPTION 'a pending finalize must never move protection_state off starting';
    END IF;

    -- Drive the generation to 'active' the same way external_snapshot_
    -- activation_authority.sql does: bind the boundary directly (simulating
    -- what the real marker transaction would have done under its own
    -- SHARE ROW EXCLUSIVE lock), inject its boundary commit, then publish
    -- with synthetic-but-shape-valid evidence and activate. This proves
    -- flashback_protect_finalize's own orchestration/protection_state
    -- transition, independent of the real artifact-writing pipeline.
    SELECT * INTO v_bound
    FROM public.flashback_internal_bind_online_boundary(v_gen_id, v_tracking, v_snap_id, v_rel);
    PERFORM public.flashback_test_inject_commit(
        v_tracking, v_lsn, clock_timestamp(), v_bound.boundary_xid, '[]'::jsonb
    );
    SELECT * INTO v_snap FROM flashback.snapshots WHERE snapshot_id = v_snap_id;

    v_locator := jsonb_build_object(
        'system_identifier', (pg_control_system()).system_identifier::text,
        'database_oid', v_db::text,
        'tracking_id', v_tracking::text,
        'snapshot_id', v_snap_id::text,
        'nonce', '940001'
    );
    v_schema_hash := public.flashback_sha256(v_snap.schema_def::text);
    -- Deliberately not calling flashback_protect_external_publish here: once
    -- the boundary LSN is resolved, it immediately attempts the real
    -- flashback_internal_finalize_external_snapshot (see header comment),
    -- which needs pg_flashback.external_snapshot_root -- a POSTMASTER-
    -- context GUC this shared test cluster does not set. Publication /
    -- activation is driven directly with synthetic-but-shape-valid
    -- evidence instead, exactly like external_snapshot_activation_
    -- authority.sql, so flashback_protect_finalize's own guard/transition
    -- logic (items 7-9 below) is proven independent of that real pipeline.
    IF NOT public.flashback_internal_publish_external_snapshot(
        v_snap_id, v_tracking, v_gen_id, 940001,
        v_locator, 0, 'zstd', 1, 1, 1, repeat('a', 64),
        v_snap.external_column_contract, v_schema_hash
    ) THEN
        RAISE EXCEPTION 'synthetic external snapshot publication was rejected';
    END IF;
    PERFORM public.flashback_internal_activate_external_generation(v_gen_id, v_tracking, v_snap_id);
    IF (SELECT state FROM flashback.coverage_generations WHERE generation_id = v_gen_id) IS DISTINCT FROM 'active' THEN
        RAISE EXCEPTION 'synthetic activation did not reach active';
    END IF;

    -- 7. flashback_protect_finalize with the generation genuinely 'active':
    -- still 'pending'/hard_failure=true here, not because of a generation-
    -- state gate but because of REAL evidence -- flashback_internal_protect_
    -- activation_readiness's own artifact payload-health check (step 3 of
    -- that function; see protect_online.sql), which fires because this
    -- fixture's publish evidence is synthetic (a fake locator/checksum, no
    -- real file on disk to verify -- the same "cannot construct a real
    -- artifact in this harness" wall documented in this file's header
    -- comment). This assertion proves flashback_protect_finalize correctly
    -- refuses to activate on real (not fabricated) negative evidence and
    -- correctly classifies it as a hard failure, not merely "still
    -- pending"; it does NOT prove the 'activated' terminal is reachable in
    -- this harness, and no test here may be cited as proving that (the
    -- real happy-path proof is scripts/run_protect_online_happy_path_e2e.sh,
    -- against a real isolated instance).
    v_finalize := public.flashback_protect_finalize(v_op);
    IF v_finalize->>'status' IS DISTINCT FROM 'pending'
       OR (v_finalize->>'hard_failure')::boolean IS DISTINCT FROM true
       OR v_finalize->>'reason' NOT ILIKE '%payload-health%'
    THEN
        RAISE EXCEPTION 'flashback_protect_finalize with a synthetic/unverifiable artifact must be pending/hard_failure with a payload-health reason, got: %', v_finalize;
    END IF;
    IF (SELECT protection_state FROM flashback.tracked_tables WHERE tracking_id = v_tracking) IS DISTINCT FROM 'starting' THEN
        RAISE EXCEPTION 'flashback_protect_finalize must never move protection_state off starting without real activation evidence';
    END IF;

    -- 7b. Adversarial: this session now holds the EXACT dangerous state the
    -- root-cause report described -- coverage_generations.state = 'active'
    -- while tracked_tables.protection_state is still 'starting' (produced
    -- above by calling the raw SnapshotStore authority functions directly,
    -- bypassing flashback_protect_external_publish's atomicity subtrans-
    -- action entirely, exactly as a caller who skipped the protect_online.
    -- sql entrypoints -- or an older, unfixed build -- could produce it).
    -- The single centralized recoverability authority (coverage_runtime.
    -- sql) must refuse to treat this as protected from every angle: not
    -- as "actively protected", not as an admissible restore/query target,
    -- even though the generation itself really is 'active'.
    IF (SELECT state FROM flashback.coverage_generations WHERE generation_id = v_gen_id) IS DISTINCT FROM 'active' THEN
        RAISE EXCEPTION 'fixture setup failed: expected the generation to be active for the adversarial check';
    END IF;
    IF public.flashback_internal_lifecycle_actively_protected(v_tracking) THEN
        RAISE EXCEPTION 'flashback_internal_lifecycle_actively_protected must be false for an active generation whose lifecycle is still starting';
    END IF;
    IF public.flashback_is_actively_protected('public.it_protect_begin') THEN
        RAISE EXCEPTION 'flashback_is_actively_protected must be false for an active generation whose lifecycle is still starting';
    END IF;
    v_raised := false;
    BEGIN
        PERFORM public.flashback_admit_lsn_target('public.it_protect_begin', v_lsn);
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT ILIKE '%actively protected%' THEN
        RAISE EXCEPTION 'flashback_admit_lsn_target must refuse a target on an active generation whose lifecycle is still starting, got raised=%, msg=%',
            v_raised, v_msg;
    END IF;

    -- 8. Idempotent under the not-yet-ready path too: a repeat call while
    -- still pending must not raise, duplicate a journal event, or otherwise
    -- mutate durable state.
    v_finalize := public.flashback_protect_finalize(v_op);
    IF v_finalize->>'status' IS DISTINCT FROM 'pending' THEN
        RAISE EXCEPTION 'a repeat flashback_protect_finalize call while still pending must stay pending, got: %', v_finalize;
    END IF;
    SELECT s.state INTO v_msg
    FROM flashback.operation_current_state s
    WHERE s.operation_id = v_op;
    IF v_msg IS DISTINCT FROM 'started' THEN
        RAISE EXCEPTION 'a pending finalize must never advance the operation journal past started: %', v_msg;
    END IF;

    -- 9. Illegal operation-journal transitions for 'protect' fail closed:
    -- from 'started', only 'activated'/'failed'/'abandoned' are legal, and
    -- no other event_type is.
    v_raised := false;
    BEGIN
        PERFORM public.flashback_operation_append_event(v_op, 'stopping');
    EXCEPTION WHEN object_not_in_prerequisite_state OR invalid_parameter_value THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'an illegal event_type on a terminal protect operation must be rejected';
    END IF;

    -- 10. RBAC: the five new entrypoints are public-facing (flashback_admin
    -- only), unlike the flashback_internal_* primitives they orchestrate.
    IF has_function_privilege('public', 'public.flashback_protect_begin(text)'::regprocedure, 'EXECUTE') THEN
        RAISE EXCEPTION 'RBAC Failed: PUBLIC must not EXECUTE flashback_protect_begin';
    END IF;
    IF has_function_privilege('public', 'public.flashback_protect_prepare_replica_identity(bigint)'::regprocedure, 'EXECUTE') THEN
        RAISE EXCEPTION 'RBAC Failed: PUBLIC must not EXECUTE flashback_protect_prepare_replica_identity';
    END IF;
    IF has_function_privilege('public', 'public.flashback_protect_external_copy(bigint)'::regprocedure, 'EXECUTE') THEN
        RAISE EXCEPTION 'RBAC Failed: PUBLIC must not EXECUTE flashback_protect_external_copy';
    END IF;
    IF has_function_privilege('public', 'public.flashback_protect_external_publish(bigint)'::regprocedure, 'EXECUTE') THEN
        RAISE EXCEPTION 'RBAC Failed: PUBLIC must not EXECUTE flashback_protect_external_publish';
    END IF;
    IF has_function_privilege('public', 'public.flashback_protect_finalize(bigint)'::regprocedure, 'EXECUTE') THEN
        RAISE EXCEPTION 'RBAC Failed: PUBLIC must not EXECUTE flashback_protect_finalize';
    END IF;
    IF has_function_privilege('pg_monitor', 'public.flashback_protect_begin(text)'::regprocedure, 'EXECUTE') THEN
        RAISE EXCEPTION 'RBAC Failed: pg_monitor must not EXECUTE flashback_protect_begin';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flashback_admin')
       AND NOT has_function_privilege('flashback_admin', 'public.flashback_protect_begin(text)'::regprocedure, 'EXECUTE')
    THEN
        RAISE EXCEPTION 'RBAC Failed: flashback_admin must be able to EXECUTE flashback_protect_begin';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flashback_admin')
       AND NOT has_function_privilege('flashback_admin', 'public.flashback_protect_finalize(bigint)'::regprocedure, 'EXECUTE')
    THEN
        RAISE EXCEPTION 'RBAC Failed: flashback_admin must be able to EXECUTE flashback_protect_finalize';
    END IF;

    PERFORM flashback_set_restore_in_progress(true);
    BEGIN
        DROP TABLE IF EXISTS public.it_protect_begin CASCADE;
    EXCEPTION WHEN OTHERS THEN
        PERFORM flashback_set_restore_in_progress(false);
        RAISE;
    END;
    PERFORM flashback_set_restore_in_progress(false);
END;
$test$;
