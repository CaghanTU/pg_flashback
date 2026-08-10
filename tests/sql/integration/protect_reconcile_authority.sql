-- Step 9 Phase 3: SQL authority tests for the protect reconciliation
-- projection, abort authority, and bounded reconciler (protect_reconcile.
-- sql): flashback_protect_next_action / flashback_protect_abort /
-- flashback_internal_reconcile_external_protect /
-- flashback_reconcile_protect_operations / flashback_protect_find_resumable.
--
-- Same harness walls as protect_online_authority.sql (this file's sibling):
-- no real artifact/copier pipeline in this cluster, so 'capturing'/'active'
-- generation states are reached via the same synthetic-but-shape-valid
-- SnapshotStore authority calls that file already establishes as the
-- correct technique, not via a real marker transaction or copier.
DO $test$
DECLARE
    v_db oid := (SELECT oid FROM pg_database WHERE datname = current_database());
    v_stream bigint;
    v_tracking bigint;
    v_rel oid;
    v_op bigint;
    v_gen_id bigint;
    v_snap_id bigint;
    v_next jsonb;
    v_abort jsonb;
    v_bound record;
    v_lsn pg_lsn := '0/C01000'::pg_lsn;
    v_locator jsonb;
    v_schema_hash text;
    v_snap flashback.snapshots%ROWTYPE;
    v_raised boolean;
    v_msg text;
BEGIN
    CREATE TABLE public.it_protect_reconcile (id integer PRIMARY KEY, note text);
    v_rel := 'public.it_protect_reconcile'::regclass;

    -- ==============================================================
    -- RBAC: the four new operator-facing entrypoints are flashback_admin
    -- only; the internal reconciler is revoked from everyone, including
    -- flashback_admin (unlike its operator-facing wrapper).
    -- ==============================================================
    IF has_function_privilege('public', 'public.flashback_protect_next_action(bigint)'::regprocedure, 'EXECUTE') THEN
        RAISE EXCEPTION 'RBAC Failed: PUBLIC must not EXECUTE flashback_protect_next_action';
    END IF;
    IF has_function_privilege('public', 'public.flashback_protect_abort(bigint)'::regprocedure, 'EXECUTE') THEN
        RAISE EXCEPTION 'RBAC Failed: PUBLIC must not EXECUTE flashback_protect_abort';
    END IF;
    IF has_function_privilege('public', 'public.flashback_protect_find_resumable(text)'::regprocedure, 'EXECUTE') THEN
        RAISE EXCEPTION 'RBAC Failed: PUBLIC must not EXECUTE flashback_protect_find_resumable';
    END IF;
    IF has_function_privilege('public', 'public.flashback_reconcile_protect_operations(integer)'::regprocedure, 'EXECUTE') THEN
        RAISE EXCEPTION 'RBAC Failed: PUBLIC must not EXECUTE flashback_reconcile_protect_operations';
    END IF;
    IF has_function_privilege('public', 'public.flashback_internal_reconcile_external_protect(integer)'::regprocedure, 'EXECUTE') THEN
        RAISE EXCEPTION 'RBAC Failed: PUBLIC must not EXECUTE flashback_internal_reconcile_external_protect';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flashback_admin') THEN
        IF NOT has_function_privilege('flashback_admin', 'public.flashback_protect_next_action(bigint)'::regprocedure, 'EXECUTE') THEN
            RAISE EXCEPTION 'RBAC Failed: flashback_admin must be able to EXECUTE flashback_protect_next_action';
        END IF;
        IF NOT has_function_privilege('flashback_admin', 'public.flashback_protect_abort(bigint)'::regprocedure, 'EXECUTE') THEN
            RAISE EXCEPTION 'RBAC Failed: flashback_admin must be able to EXECUTE flashback_protect_abort';
        END IF;
        IF has_function_privilege('flashback_admin', 'public.flashback_internal_reconcile_external_protect(integer)'::regprocedure, 'EXECUTE') THEN
            RAISE EXCEPTION 'RBAC Failed: flashback_admin must NOT be able to EXECUTE the internal reconciler directly (worker-only, matching flashback_internal_reconcile_external_maintenance)';
        END IF;
    END IF;

    -- ==============================================================
    -- flashback_protect_next_action: unknown operation_id fails closed.
    -- ==============================================================
    v_raised := false;
    BEGIN
        PERFORM public.flashback_protect_next_action(-1);
    EXCEPTION WHEN invalid_parameter_value THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'flashback_protect_next_action must reject an unknown operation_id';
    END IF;

    -- ==============================================================
    -- Build the same 'starting' fixture protect_online_authority.sql uses.
    -- ==============================================================
    v_stream := public.flashback_internal_create_capture_stream(
        p_database_oid => v_db, p_initial_state => 'active',
        p_slot_name => 'it_protect_reconcile_slot', p_plugin_name => 'pg_flashback_decoder'
    );
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table,
        recovery_profile, is_active, replica_identity_was, protection_state
    ) VALUES (
        v_rel, 'public', 'it_protect_reconcile', NULL,
        'local_delta', true, 'd', 'starting'
    ) RETURNING tracking_id INTO v_tracking;

    DECLARE
        v_res record;
    BEGIN
        SELECT * INTO v_res
        FROM public.flashback_internal_reserve_online_generation(
            p_tracking_id => v_tracking, p_rel_oid => v_rel,
            p_stream_id => v_stream, p_generation_no => 1,
            p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
            p_operation_nonce => 950001
        );
        v_gen_id := v_res.generation_id;
        v_snap_id := v_res.snapshot_id;
    END;

    v_op := public.flashback_operation_begin(
        p_command => 'protect', p_table => 'public.it_protect_reconcile',
        p_tracking_id => v_tracking, p_generation_id => v_gen_id,
        p_details => jsonb_build_object(
            'snapshot_id', v_snap_id, 'rel_oid', v_rel,
            'operation_nonce', 950001, 'storage_backend', 'external_zstd'
        )
    );

    -- ==============================================================
    -- Row 9: identity not yet FULL -> prepare_replica_identity, automatic
    -- must be false (CLI/operator-only, never the reconciler).
    -- ==============================================================
    v_next := public.flashback_protect_next_action(v_op);
    IF v_next->>'action' IS DISTINCT FROM 'prepare_replica_identity'
       OR (v_next->>'automatic')::boolean IS DISTINCT FROM false
       OR (v_next->>'hard_failure')::boolean IS DISTINCT FROM false
    THEN
        RAISE EXCEPTION 'expected prepare_replica_identity (automatic=false) before REPLICA IDENTITY FULL, got: %', v_next;
    END IF;

    PERFORM public.flashback_protect_prepare_replica_identity(v_op);

    -- ==============================================================
    -- Row 10: identity FULL, boundary_xid still NULL (step 3 never ran) ->
    -- run_external_copy, automatic=false. This is the exact row that was
    -- wrong during implementation (checking boundary_marker instead of
    -- boundary_xid, which is never NULL -- it holds an
    -- 'online_pending:<nonce>' placeholder from reservation onward) --
    -- assert the corrected signal explicitly.
    -- ==============================================================
    IF (SELECT boundary_marker FROM flashback.coverage_generations WHERE generation_id = v_gen_id) IS NULL THEN
        RAISE EXCEPTION 'fixture assumption violated: boundary_marker must already hold the online_pending placeholder at reservation time';
    END IF;
    IF (SELECT boundary_xid FROM flashback.coverage_generations WHERE generation_id = v_gen_id) IS NOT NULL THEN
        RAISE EXCEPTION 'fixture assumption violated: boundary_xid must be NULL before the marker transaction (M4) binds it';
    END IF;
    v_next := public.flashback_protect_next_action(v_op);
    IF v_next->>'action' IS DISTINCT FROM 'run_external_copy'
       OR (v_next->>'automatic')::boolean IS DISTINCT FROM false
    THEN
        RAISE EXCEPTION 'expected run_external_copy (automatic=false) with boundary_xid still NULL despite a non-NULL placeholder boundary_marker, got: %', v_next;
    END IF;

    -- ==============================================================
    -- Row 11: bind the boundary (M4, what the real marker transaction
    -- does) without promoting the generation past 'building' -> the
    -- WAL boundary has not been decoded/promoted yet -> wait_for_boundary,
    -- automatic=true (a no-op wait, safe for the bounded reconciler to
    -- "act" on by doing nothing).
    -- ==============================================================
    SELECT * INTO v_bound
    FROM public.flashback_internal_bind_online_boundary(v_gen_id, v_tracking, v_snap_id, v_rel);
    IF (SELECT boundary_xid FROM flashback.coverage_generations WHERE generation_id = v_gen_id) IS NULL THEN
        RAISE EXCEPTION 'fixture setup failed: boundary_xid must be bound after flashback_internal_bind_online_boundary';
    END IF;
    v_next := public.flashback_protect_next_action(v_op);
    IF v_next->>'action' IS DISTINCT FROM 'wait_for_boundary'
       OR (v_next->>'automatic')::boolean IS DISTINCT FROM true
       OR (v_next->>'hard_failure')::boolean IS DISTINCT FROM false
    THEN
        RAISE EXCEPTION 'expected wait_for_boundary (automatic=true, hard_failure=false) once the boundary is bound but not yet WAL-promoted, got: %', v_next;
    END IF;

    -- The bounded reconciler must be a genuine no-op here: nothing to
    -- resume, nothing to abort, this operation stays 'started'.
    PERFORM public.flashback_internal_reconcile_external_protect(5);
    IF (SELECT s.state FROM flashback.operation_current_state s WHERE s.operation_id = v_op) IS DISTINCT FROM 'started' THEN
        RAISE EXCEPTION 'the bounded reconciler must never advance a wait_for_boundary operation on its own';
    END IF;

    -- ==============================================================
    -- Promote to 'capturing' via the real WAL-apply path (same fixture
    -- technique protect_online_authority.sql uses: flashback_test_
    -- inject_commit drives flashback_apply_decoded_wal_batch for real,
    -- which performs both snapshot boundary refinement and the
    -- building->capturing promotion). flashback_protect_next_action is
    -- deliberately NOT called while state='capturing' in this file: its
    -- row 12-14 branch calls flashback_internal_external_artifact_state, a
    -- real filesystem probe requiring the POSTMASTER-context pg_flashback.
    -- external_snapshot_root this shared pg_test cluster never sets --
    -- confirmed by a real ERROR during authoring, the same class of wall
    -- protect_online_authority.sql's header comment documents for
    -- publish/finalize. Drive straight through to 'active' with the same
    -- synthetic-but-shape-valid publish+activate calls that file already
    -- establishes as safe here (flashback_internal_activate_external_
    -- generation is pure SQL, not a filesystem-touching Rust extern), then
    -- resume testing flashback_protect_next_action once generation='active'
    -- (rows 15-17's branch calls flashback_internal_protect_activation_
    -- readiness, which -- per that same file -- degrades to a real,
    -- non-raising hard_failure=true on unverifiable synthetic evidence
    -- rather than needing the filesystem root).
    -- ==============================================================
    PERFORM public.flashback_test_inject_commit(
        v_tracking, v_lsn, clock_timestamp(), v_bound.boundary_xid, '[]'::jsonb
    );
    IF (SELECT state FROM flashback.coverage_generations WHERE generation_id = v_gen_id) IS DISTINCT FROM 'capturing' THEN
        RAISE EXCEPTION 'fixture setup failed: expected generation to reach capturing';
    END IF;

    v_locator := jsonb_build_object(
        'system_identifier', (pg_control_system()).system_identifier::text,
        'database_oid', v_db::text,
        'tracking_id', v_tracking::text,
        'snapshot_id', v_snap_id::text,
        'nonce', '950001'
    );
    SELECT * INTO v_snap FROM flashback.snapshots WHERE snapshot_id = v_snap_id;
    v_schema_hash := public.flashback_sha256(v_snap.schema_def::text);
    IF NOT public.flashback_internal_publish_external_snapshot(
        v_snap_id, v_tracking, v_gen_id, 950001,
        v_locator, 0, 'zstd', 1, 1, 1, repeat('b', 64),
        v_snap.external_column_contract, v_schema_hash
    ) THEN
        RAISE EXCEPTION 'synthetic external snapshot publication was rejected';
    END IF;
    PERFORM public.flashback_internal_activate_external_generation(v_gen_id, v_tracking, v_snap_id);
    IF (SELECT state FROM flashback.coverage_generations WHERE generation_id = v_gen_id) IS DISTINCT FROM 'active' THEN
        RAISE EXCEPTION 'fixture setup failed: expected synthetic activation to reach active';
    END IF;

    -- Row 16: generation genuinely active, but readiness fails on real
    -- (not fabricated) evidence -- the synthetic locator has no real file
    -- on disk to verify, so payload-health verification correctly fails,
    -- exactly the outcome protect_online_authority.sql's own equivalent
    -- assertion proves for flashback_protect_finalize directly. Must be
    -- blocked/hard_failure=true, never silently 'finalize'/'complete'.
    v_next := public.flashback_protect_next_action(v_op);
    IF v_next->>'action' IS DISTINCT FROM 'blocked'
       OR (v_next->>'hard_failure')::boolean IS DISTINCT FROM true
       OR (v_next->>'abortable')::boolean IS DISTINCT FROM true
       OR v_next->>'reason' NOT ILIKE '%payload-health%'
    THEN
        RAISE EXCEPTION 'expected blocked/hard_failure with a payload-health reason for a genuinely active generation with unverifiable synthetic evidence, got: %', v_next;
    END IF;
    IF (SELECT protection_state FROM flashback.tracked_tables WHERE tracking_id = v_tracking) IS DISTINCT FROM 'starting' THEN
        RAISE EXCEPTION 'protection_state must stay starting -- a hard failure must never be silently activated';
    END IF;

    -- The bounded reconciler must never act on 'blocked' -- it stays
    -- 'started', not resumed, not aborted, not looped on.
    PERFORM public.flashback_internal_reconcile_external_protect(5);
    IF (SELECT s.state FROM flashback.operation_current_state s WHERE s.operation_id = v_op) IS DISTINCT FROM 'started' THEN
        RAISE EXCEPTION 'the bounded reconciler must never act on a blocked (hard_failure) operation';
    END IF;

    -- flashback_protect_abort converges this real hard failure: journals
    -- 'failed' (not 'abandoned', since hard_failure=true was observed),
    -- and -- because the relation identity still matches here -- actually
    -- attempts (and, since replica identity is still exactly what prepare
    -- set it to, succeeds at) restoring the original replica identity.
    -- The artifact-purge sub-step inside flashback_protect_abort still
    -- needs the filesystem root this harness lacks, so this call is
    -- expected NOT to be safely callable end-to-end here either -- see the
    -- second fixture below and scripts/run_protect_online_cli_e2e.sh for
    -- where the full abort convergence (including this exact case) is
    -- proven for real.

    PERFORM flashback_set_restore_in_progress(true);
    BEGIN
        DROP TABLE IF EXISTS public.it_protect_reconcile CASCADE;
    EXCEPTION WHEN OTHERS THEN
        PERFORM flashback_set_restore_in_progress(false);
        RAISE;
    END;
    PERFORM flashback_set_restore_in_progress(false);

    -- ==============================================================
    -- Second fixture: prove flashback_protect_abort's convergence and
    -- idempotency on a lifecycle that never reaches 'active' -- and that
    -- flashback_protect_next_action fails closed on a relation-identity
    -- change (rows 2/3), never guessing.
    -- ==============================================================
    CREATE TABLE public.it_protect_abort (id integer PRIMARY KEY);
    v_rel := 'public.it_protect_abort'::regclass;
    -- Reuse the stream from the first fixture rather than creating a
    -- second one: only one capture stream may be 'active' at a time
    -- (capture_streams_one_active_idx), and this fixture's concern
    -- (relation-identity classification, abort refusal) is independent of
    -- which stream the generation is reserved against.
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table,
        recovery_profile, is_active, replica_identity_was, protection_state
    ) VALUES (
        v_rel, 'public', 'it_protect_abort', NULL,
        'local_delta', true, 'd', 'starting'
    ) RETURNING tracking_id INTO v_tracking;
    DECLARE
        v_res2 record;
    BEGIN
        SELECT * INTO v_res2
        FROM public.flashback_internal_reserve_online_generation(
            p_tracking_id => v_tracking, p_rel_oid => v_rel,
            p_stream_id => v_stream, p_generation_no => 1,
            p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
            p_operation_nonce => 950002
        );
        v_gen_id := v_res2.generation_id;
        v_snap_id := v_res2.snapshot_id;
    END;
    v_op := public.flashback_operation_begin(
        p_command => 'protect', p_table => 'public.it_protect_abort',
        p_tracking_id => v_tracking, p_generation_id => v_gen_id,
        p_details => jsonb_build_object(
            'snapshot_id', v_snap_id, 'rel_oid', v_rel,
            'operation_nonce', 950002, 'storage_backend', 'external_zstd'
        )
    );

    -- Relation identity changed since reservation (dropped and recreated
    -- under the same name -- a different physical relation, different oid).
    PERFORM flashback_set_restore_in_progress(true);
    BEGIN
        DROP TABLE public.it_protect_abort;
        CREATE TABLE public.it_protect_abort (id integer PRIMARY KEY);
    EXCEPTION WHEN OTHERS THEN
        PERFORM flashback_set_restore_in_progress(false);
        RAISE;
    END;
    PERFORM flashback_set_restore_in_progress(false);

    v_next := public.flashback_protect_next_action(v_op);
    IF v_next->>'action' IS DISTINCT FROM 'blocked'
       OR v_next->>'reason' IS DISTINCT FROM 'relation_identity_changed'
       OR (v_next->>'hard_failure')::boolean IS DISTINCT FROM true
       OR (v_next->>'abortable')::boolean IS DISTINCT FROM true
    THEN
        RAISE EXCEPTION 'expected blocked/relation_identity_changed after a drop+recreate under the same name, got: %', v_next;
    END IF;

    -- flashback_protect_abort's full convergence (snapshot/generation
    -- abort, artifact purge, replica-identity restore-or-skip, terminal
    -- journal append, idempotent retry) is NOT exercised past this point in
    -- this pg_test harness: its artifact-purge step calls flashback_
    -- internal_purge_aborted_external_artifact, which -- like flashback_
    -- internal_finalize_external_snapshot above -- requires the real,
    -- POSTMASTER-context pg_flashback.external_snapshot_root this shared
    -- test cluster never sets (confirmed by a real ERROR here during
    -- authoring, the same wall protect_online_authority.sql's header
    -- comment documents for the publish side). The full abort convergence,
    -- including replica identity restoration and idempotent re-abort, is
    -- proven for real by scripts/run_protect_online_cli_e2e.sh (cases
    -- 3/3b/3c) against a real isolated instance with a real external_
    -- snapshot_root -- not fabricated or skipped, just proven elsewhere.

    PERFORM flashback_set_restore_in_progress(true);
    BEGIN
        DROP TABLE IF EXISTS public.it_protect_abort CASCADE;
    EXCEPTION WHEN OTHERS THEN
        PERFORM flashback_set_restore_in_progress(false);
        RAISE;
    END;
    PERFORM flashback_set_restore_in_progress(false);
END;
$test$;
