-- =================================================================
-- Step 9 Phase 2: production initial-protection reservation for
-- external_zstd -- the online, non-blocking counterpart to
-- flashback_track()'s heap_v1 CTAS bootstrap (lifecycle_bootstrap_core.sql).
--
-- flashback_track() holds a SHARE ROW EXCLUSIVE lock across a synchronous
-- CTAS of the whole table; at scale that risks exactly the incident this
-- phase exists to close (a 1 GiB table exceeding local_boundary_write_
-- stall_ms). This path never takes a long lock and never creates a heap
-- snapshot: it reserves a parentless coverage generation, runs the real
-- M1-M7 marker transaction under a brief SHARE ROW EXCLUSIVE lock (the
-- same primitive flashback_maintain_external_copy already uses for
-- re-anchor), lets the background copier stream the artifact to
-- external_zstd storage while the table stays fully writable, then
-- publishes and activates once WAL consumption resolves the boundary.
--
-- FOUR separate server-side transactions -- the caller must COMMIT each
-- one before calling the next -- driven by the operation journal's
-- 'protect' command (operation_journal.sql):
--   1. flashback_protect_begin(table)
--        Fast, lock-light reservation: no table lock at all.
--   2. flashback_protect_prepare_replica_identity(op_id)
--        Its own committed transaction (see that function's header comment
--        for exactly why this cannot share a transaction with step 3 --
--        a real deadlock, found empirically, not merely a style choice).
--        Captures/sets REPLICA IDENTITY FULL under SHARE ROW EXCLUSIVE.
--   3. flashback_protect_external_copy(op_id)
--        SHARE ROW EXCLUSIVE marker transaction (M1-M7) + background copy.
--        Its response describes what THIS call's transaction will commit,
--        not what has already committed -- nothing is durable until the
--        caller itself commits this call.
--   4. flashback_protect_external_publish(op_id)
--        Publishes the staged artifact and, only if activation-readiness
--        (below) is fully satisfied, activates the generation and flips
--        protection_state 'starting' -> 'active' in the SAME committed
--        transaction. See "ATOMICITY" below for exactly what a committed
--        call of this step may and may not leave behind.
--
-- No stage here ever calls flashback_internal_snapshot_create or writes a
-- base_snapshot_table: this path has no heap CTAS at any point. Mirrors
-- flashback_maintain_begin / flashback_maintain_external_copy /
-- flashback_maintain_external_publish / flashback_maintain_finalize
-- (maintain_uninstall.sql) closely, adapted for a brand-new, parentless
-- lifecycle instead of a re-anchor of an already-tracked one:
--   * flashback_protect_begin also performs flashback_track()'s topology /
--     compatibility / dedicated-txn / slot-creation preflight, since there
--     is no existing tracked_tables row to reuse.
--   * flashback_protect_prepare_replica_identity has no maintain_*
--     equivalent at all -- a re-anchored table already has FULL identity
--     from its original heap_v1 track; a never-tracked table does not.
--   * flashback_protect_finalize additionally transitions
--     tracked_tables.protection_state 'starting' -> 'active'.
--
-- RECOVERABILITY AUTHORITY
-- tracked_tables.is_active alone (checked by every table-name resolver in
-- coverage_runtime.sql) is NOT sufficient to admit a lifecycle for
-- historical read/destructive recovery: is_active is set true at
-- flashback_protect_begin's reservation, long before the lifecycle is
-- genuinely, verifiedly protected. flashback_internal_lifecycle_actively_
-- protected(tracking_id) (coverage_runtime.sql, immediately above the
-- resolvers) is the one centralized predicate additionally requiring
-- protection_state = 'active'; flashback_admit_lsn_target (the shared
-- admission primitive behind restore/query/recover and timestamp
-- resolution) and flashback_is_actively_protected both call it. A
-- 'starting' lifecycle stays visible to status/doctor/capture/reconciler
-- (nothing here touches is_active-only lookups used for that), but can
-- never be planned, admitted, queried, or restored.
--
-- ATOMICITY
-- After ANY committed call to flashback_protect_external_publish, only two
-- outcomes are legal for this lifecycle:
--   A. Complete: snapshot available, generation active, protection_state
--      active, 'protect' operation 'activated'.
--   B. Pending/failed: protection_state still 'starting', operation still
--      'started', and the generation is NOT flashback_internal_lifecycle_
--      actively_protected-admitted (not active, not recoverable).
-- coverage_generations.state = 'active' with protection_state still
-- 'starting' must never be observable after this call commits.
-- flashback_protect_external_publish enforces this with a PL/pgSQL
-- subtransaction (SAVEPOINT-backed BEGIN/EXCEPTION block): it runs the
-- real publish (flashback_internal_finalize_external_snapshot, which
-- itself both publishes the snapshot and activates the generation) and
-- then flashback_protect_finalize's own activation-readiness check
-- (flashback_internal_protect_activation_readiness below -- exact facts
-- only, never the generic flashback_health() aggregate, which mixes in
-- operational warnings unrelated to this lifecycle's own correctness) all
-- inside that subtransaction; if readiness is not satisfied, the whole
-- subtransaction is rolled back, undoing the publish+activate, before the
-- caller's transaction ever gets a chance to commit them. Filesystem
-- publication (already durable on disk by the time the SQL-level publish
-- runs) is not rolled back by this -- it is not part of the SQL
-- transaction at all -- and does not need to be: flashback_internal_
-- publish_external_snapshot's own idempotent-retry branch recognizes an
-- already-published locator on the next attempt and republishes/
-- reactivates from the database side without touching the filesystem
-- again, so an orphaned-but-resumable artifact is an accepted, already-
-- documented part of the external artifact contract, not a correctness
-- gap.
-- =================================================================

CREATE OR REPLACE FUNCTION flashback_protect_begin(p_table text)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_rel_oid oid;
    v_schema_name text;
    v_table_name text;
    v_name text;
    v_backend text;
    v_db_oid oid;
    v_replica_identity_was "char" := 'd';
    v_replica_identity_index text := NULL;
    v_stream_id bigint;
    v_tracking_id bigint;
    v_operation_id bigint;
    v_operation_nonce bigint;
    v_gen record;
BEGIN
    PERFORM flashback_require_primary('flashback_protect_begin');
    -- Fail closed before any metadata: only wal is legal (raises for trigger/auto).
    PERFORM flashback_effective_capture_mode();

    v_backend := lower(COALESCE(
        NULLIF(current_setting('pg_flashback.snapshot_storage_backend', true), ''),
        'heap_v1'
    ));
    IF v_backend IS DISTINCT FROM 'external_zstd' THEN
        RAISE EXCEPTION 'pg_flashback: flashback_protect_begin() requires pg_flashback.snapshot_storage_backend = external_zstd (currently %)',
            v_backend
            USING ERRCODE = 'invalid_parameter_value',
                  HINT = 'Use flashback_track() for heap_v1, or set pg_flashback.snapshot_storage_backend = ''external_zstd'' and reload.';
    END IF;

    SELECT c.oid, n.nspname, c.relname
      INTO v_rel_oid, v_schema_name, v_table_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = to_regclass(p_table);
    IF v_rel_oid IS NULL THEN
        RAISE EXCEPTION 'flashback_protect_begin: table % does not exist', p_table;
    END IF;
    v_name := format('%I.%I', v_schema_name, v_table_name);

    PERFORM flashback_require_supported_local_table(v_rel_oid);
    -- Same broader feature-by-feature compatibility surface flashback_track()
    -- enforces (docs/SUPPORT.md is generated from this contract).
    PERFORM flashback_require_local_compatibility(v_rel_oid);

    -- Clean-txn / isolation gates apply only after the table is known to be
    -- supportable, matching flashback_track()'s own ordering.
    IF txid_current_if_assigned() IS NOT NULL THEN
        RAISE EXCEPTION 'pg_flashback: flashback_protect_begin() must run before any write in a dedicated transaction'
            USING HINT = 'COMMIT or ROLLBACK, then call flashback_protect_begin() as the first write in a new READ COMMITTED transaction.';
    END IF;
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'pg_flashback: flashback_protect_begin() requires READ COMMITTED isolation for a fresh post-lock snapshot';
    END IF;

    -- Fail closed when this database has no admitted, running capture worker.
    PERFORM flashback_require_admitted_capture_worker('flashback_protect_begin()');

    v_db_oid := (SELECT oid FROM pg_database WHERE datname = current_database());

    -- Canonical lock order: database stream -> canonical pre-identity key ->
    -- tracking ID -> relation. Take the stream lock before slot creation /
    -- the name-collision advisory lock, exactly like flashback_track().
    PERFORM flashback_internal_lock_database_stream(v_db_oid);

    -- Same name-collision advisory namespace (358943) and hash key shape
    -- flashback_bootstrap_local_delta_lifecycle_core uses for flashback_
    -- track(), so a heap_v1 track and an external_zstd protect racing on the
    -- same table name serialize against each other too, not just against
    -- their own kind.
    PERFORM pg_advisory_xact_lock(
        358943::integer,
        hashtext(format('%s:%s.%s', v_db_oid, v_schema_name, v_table_name))
    );

    IF EXISTS (
        SELECT 1 FROM flashback.tracked_tables tt
        WHERE tt.is_active
          AND (tt.rel_oid = v_rel_oid
               OR (tt.schema_name = v_schema_name AND tt.table_name = v_table_name))
    ) THEN
        RAISE EXCEPTION 'pg_flashback: % is already an active tracked lifecycle', v_name
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    -- Capture the current replica identity BEFORE the marker phase changes
    -- it, exactly like flashback_track() does for heap_v1, so unprotect can
    -- restore it later. flashback_protect_external_copy() applies REPLICA
    -- IDENTITY FULL under its own SHARE ROW EXCLUSIVE lock, not here: this
    -- reservation call takes no table lock at all.
    SELECT c.relreplident INTO v_replica_identity_was
    FROM pg_class c WHERE c.oid = v_rel_oid;
    IF v_replica_identity_was = 'i' THEN
        SELECT ic.relname INTO v_replica_identity_index
        FROM pg_index i
        JOIN pg_class ic ON ic.oid = i.indexrelid
        WHERE i.indrelid = v_rel_oid
          AND i.indisreplident;
    END IF;

    -- Logical slots are database-specific; ensure one exists, exactly like
    -- flashback_track(). pg_create_logical_replication_slot requires a
    -- transaction that has not performed writes yet, so this must stay
    -- ahead of the tracked_tables INSERT below.
    IF NOT EXISTS (
        SELECT 1 FROM pg_replication_slots
        WHERE slot_name = flashback_effective_slot_name()
          AND database = current_database()
    ) THEN
        IF EXISTS (
            SELECT 1 FROM pg_replication_slots
            WHERE slot_name = flashback_effective_slot_name()
        ) THEN
            RAISE EXCEPTION 'pg_flashback: replication slot % already exists but belongs to another database. WAL capture cannot work for %. Set pg_flashback.slot_name to a database-unique name.',
                flashback_effective_slot_name(), current_database();
        END IF;
        BEGIN
            PERFORM pg_create_logical_replication_slot(
                flashback_effective_slot_name(),
                'pg_flashback'
            );
            RAISE NOTICE 'pg_flashback: created logical replication slot %',
                flashback_effective_slot_name();
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'pg_flashback: could not create replication slot % (%). Without a slot, WAL capture would silently miss every change, so protection is aborted.',
                flashback_effective_slot_name(), SQLERRM
                USING HINT = format(
                    'Run flashback_protect_begin in a fresh transaction with no prior writes, or create the slot manually first: SELECT pg_create_logical_replication_slot(%L, %L);',
                    flashback_effective_slot_name(), 'pg_flashback');
        END;
    END IF;

    v_stream_id := flashback_ensure_active_wal_stream();
    IF v_stream_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: WAL stream could not be activated for slot %',
            flashback_effective_slot_name();
    END IF;

    -- External artifact capacity, never the heap CTAS admission check: this
    -- path writes a compressed external_zstd artifact, not a heap snapshot
    -- table. Deliberately AFTER slot creation above, matching flashback_
    -- track()'s own deferred-capacity ordering (flashback_bootstrap_local_
    -- delta_lifecycle_core admits capacity only after its own slot check):
    -- found empirically that some capacity-check code path along the way
    -- (GUC-parsing exception handling) opens a PL/pgSQL subtransaction,
    -- which assigns this session a real transaction id -- and pg_create_
    -- logical_replication_slot refuses once one exists, exactly the same
    -- restriction a real prior write would trigger. Running admission after
    -- slot creation sidesteps that entirely without weakening the check
    -- itself: it still runs, still fails closed, just one step later.
    PERFORM flashback_admit_external_snapshot_capacity(v_rel_oid::regclass);

    v_tracking_id := nextval('flashback.tracking_id_seq');
    PERFORM flashback_internal_lock_lifecycle(v_tracking_id);

    -- 'starting': enrolled (is_active = true, so flashback_consume_wal()'s
    -- tracked_oids computation already stages its WAL) but not yet
    -- protected -- no coverage generation is 'active' yet, and REPLICA
    -- IDENTITY is not yet FULL. base_snapshot_table stays NULL forever on
    -- this path: no heap snapshot is ever created. schema_version starts at
    -- 1 like heap_v1; flashback.schema_versions gets its real row from
    -- flashback_internal_bind_online_boundary in the marker phase.
    INSERT INTO flashback.tracked_tables (
        tracking_id, rel_oid, schema_name, table_name, base_snapshot_table,
        schema_version, recovery_profile, helper_profile,
        coverage_start_lsn, coverage_end_lsn,
        tracked_since, checkpoint_interval, retention_interval, is_active,
        replica_identity_was, replica_identity_index, protection_state
    ) VALUES (
        v_tracking_id, v_rel_oid, v_schema_name, v_table_name, NULL,
        1, 'local_delta', NULL, NULL, NULL,
        now(), interval '15 minutes', interval '7 days', true,
        v_replica_identity_was, v_replica_identity_index, 'starting'
    );

    v_operation_nonce := txid_current();
    SELECT * INTO v_gen
    FROM flashback_internal_reserve_online_generation(
        v_tracking_id, v_rel_oid, v_stream_id, 1, NULL,
        'external_zstd', v_operation_nonce, 'local_delta',
        jsonb_build_object('source', 'protect')
    );

    v_operation_id := flashback_operation_begin(
        p_command => 'protect',
        p_table => v_name,
        p_tracking_id => v_tracking_id,
        p_generation_id => v_gen.generation_id,
        p_details => jsonb_build_object(
            'snapshot_id', v_gen.snapshot_id,
            'rel_oid', v_rel_oid,
            'operation_nonce', v_operation_nonce,
            'storage_backend', 'external_zstd'
        )
    );

    RETURN jsonb_build_object(
        'schema_version', 1,
        'status', 'reserved',
        'code', 'external_copy_required',
        'operation_id', v_operation_id,
        'table_name', v_name,
        'tracking_id', v_tracking_id,
        'generation_id', v_gen.generation_id,
        'snapshot_id', v_gen.snapshot_id,
        'note', 'reservation committed; call flashback_protect_prepare_replica_identity(operation_id) next, in a new transaction'
    );
END;
$$;

-- Sets REPLICA IDENTITY FULL, in its own transaction, separate from
-- flashback_protect_external_copy below. Discovered empirically (a real
-- deadlock, not a test artifact): ALTER TABLE ... REPLICA IDENTITY takes
-- AccessExclusiveLock in PostgreSQL, not ShareRowExclusiveLock -- doing the
-- ALTER inside the SAME transaction as the marker transaction means that
-- AccessExclusiveLock is still held (PostgreSQL never downgrades a lock
-- mid-transaction) while M6 waits for the background copier to open its own
-- read cursor, which needs only AccessShareLock but can never acquire it
-- until the marker transaction commits -- and the marker transaction never
-- commits until the copier signals, which it can never do. Splitting the
-- ALTER into its own committed transaction first removes the conflict
-- entirely without weakening the invariant: REPLICA IDENTITY FULL, once
-- committed here, applies to every subsequent write regardless of what
-- happens afterward, and flashback_protect_external_copy's own boundary
-- bind (M4) is necessarily causally after this commit -- there is no
-- window where a write assigned to this generation could have been
-- WAL-logged before FULL took effect. Idempotent: a repeat call once
-- identity is already FULL is a no-op.
CREATE OR REPLACE FUNCTION flashback_protect_prepare_replica_identity(p_operation_id bigint)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_op record;
    v_details jsonb;
    v_rel_oid oid;
    v_tt flashback.tracked_tables%ROWTYPE;
    v_current_oid oid;
    v_current_relident "char";
BEGIN
    -- Never trust caller-supplied identity independently: everything below
    -- is re-derived from the operation journal / tracked_tables, keyed only
    -- by p_operation_id.
    SELECT s.*, o.details
      INTO v_op
    FROM flashback.operation_current_state s
    JOIN flashback.operations o ON o.operation_id = s.operation_id
    WHERE s.operation_id = p_operation_id
      AND s.command = 'protect'
      AND s.state = 'started';
    IF v_op.operation_id IS NULL
       OR v_op.details->>'storage_backend' IS DISTINCT FROM 'external_zstd'
    THEN
        RAISE EXCEPTION 'pg_flashback: operation % is not a pending external protect copy',
            p_operation_id USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    v_details := v_op.details;
    v_rel_oid := (v_details->>'rel_oid')::oid;

    SELECT * INTO v_tt
    FROM flashback.tracked_tables
    WHERE tracking_id = v_op.tracking_id
    FOR SHARE;
    IF v_tt.tracking_id IS NULL
       OR v_tt.protection_state IS DISTINCT FROM 'starting'
       OR NOT v_tt.is_active
       OR v_tt.rel_oid IS DISTINCT FROM v_rel_oid
    THEN
        RAISE EXCEPTION 'pg_flashback: tracking % is not a pending protect-starting lifecycle for %',
            v_op.tracking_id, v_rel_oid
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    PERFORM flashback_apply_local_boundary_lock_timeout();
    EXECUTE format('LOCK TABLE %I.%I IN SHARE ROW EXCLUSIVE MODE', v_tt.schema_name, v_tt.table_name);

    SELECT c.oid, c.relreplident INTO v_current_oid, v_current_relident
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = v_tt.schema_name AND c.relname = v_tt.table_name;
    IF v_current_oid IS DISTINCT FROM v_rel_oid THEN
        RAISE EXCEPTION 'pg_flashback: %.% identity changed since reservation (expected oid %, found %); protect aborted',
            v_tt.schema_name, v_tt.table_name, v_rel_oid, v_current_oid
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    -- This lifecycle is already is_active=true (set at reservation, so
    -- flashback_consume_wal's tracked_oids computation can see it) but has
    -- no 'active' generation yet -- pg_flashback's own native DDL hook
    -- (src/capture/ddl_hook.rs -> flashback_internal_prepare_metadata_ddl ->
    -- flashback_capture_configuration_guard) does not know this shape and
    -- refuses ordinary-looking DDL on a tracked table without one, exactly
    -- as it should for a real user ALTER on an already-broken lifecycle.
    -- This ALTER is pg_flashback's own trusted internal bootstrap step, not
    -- user DDL, so it is exempted from that guard the same established way
    -- unprotect/cleanup's own internal DROP TABLE calls already are.
    IF v_current_relident IS DISTINCT FROM 'f' THEN
        PERFORM flashback_set_restore_in_progress(true);
        BEGIN
            EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY FULL', v_tt.schema_name, v_tt.table_name);
        EXCEPTION WHEN OTHERS THEN
            PERFORM flashback_set_restore_in_progress(false);
            RAISE;
        END;
        PERFORM flashback_set_restore_in_progress(false);
    END IF;

    RETURN jsonb_build_object(
        'schema_version', 1,
        'status', 'identity_ready',
        'operation_id', p_operation_id,
        'tracking_id', v_op.tracking_id,
        'replica_identity_was', v_current_relident,
        'note', 'REPLICA IDENTITY FULL is committed; call flashback_protect_external_copy(operation_id) next, in a new transaction'
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_protect_external_copy(p_operation_id bigint)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_op record;
    v_details jsonb;
    v_snapshot_id bigint;
    v_rel_oid oid;
    v_tt flashback.tracked_tables%ROWTYPE;
    v_current_oid oid;
    v_current_relident "char";
    v_result jsonb;
BEGIN
    -- Never trust caller-supplied identity independently: everything below
    -- is re-derived from the operation journal / tracked_tables /
    -- coverage_generations, keyed only by p_operation_id.
    SELECT s.*, o.details
      INTO v_op
    FROM flashback.operation_current_state s
    JOIN flashback.operations o ON o.operation_id = s.operation_id
    WHERE s.operation_id = p_operation_id
      AND s.command = 'protect'
      AND s.state = 'started';
    IF v_op.operation_id IS NULL
       OR v_op.details->>'storage_backend' IS DISTINCT FROM 'external_zstd'
    THEN
        RAISE EXCEPTION 'pg_flashback: operation % is not a pending external protect copy',
            p_operation_id USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    v_details := v_op.details;
    v_snapshot_id := (v_details->>'snapshot_id')::bigint;
    v_rel_oid := (v_details->>'rel_oid')::oid;

    SELECT * INTO v_tt
    FROM flashback.tracked_tables
    WHERE tracking_id = v_op.tracking_id
    FOR SHARE;
    IF v_tt.tracking_id IS NULL
       OR v_tt.protection_state IS DISTINCT FROM 'starting'
       OR NOT v_tt.is_active
       OR v_tt.rel_oid IS DISTINCT FROM v_rel_oid
    THEN
        RAISE EXCEPTION 'pg_flashback: tracking % is not a pending protect-starting lifecycle for %',
            v_op.tracking_id, v_rel_oid
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    PERFORM flashback_apply_local_boundary_lock_timeout();
    EXECUTE format('LOCK TABLE %I.%I IN SHARE ROW EXCLUSIVE MODE', v_tt.schema_name, v_tt.table_name);

    SELECT c.oid, c.relreplident INTO v_current_oid, v_current_relident
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = v_tt.schema_name AND c.relname = v_tt.table_name;
    IF v_current_oid IS DISTINCT FROM v_rel_oid THEN
        RAISE EXCEPTION 'pg_flashback: %.% identity changed since reservation (expected oid %, found %); protect aborted',
            v_tt.schema_name, v_tt.table_name, v_rel_oid, v_current_oid
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    -- flashback_protect_prepare_replica_identity must have already committed
    -- FULL identity (its own, separate transaction -- see that function's
    -- header comment for exactly why this cannot be one transaction with
    -- the marker transaction below). Fail closed rather than silently
    -- re-deriving/re-altering it here under a lock the copier will need.
    IF v_current_relident IS DISTINCT FROM 'f' THEN
        RAISE EXCEPTION 'pg_flashback: %.% is not yet REPLICA IDENTITY FULL; call flashback_protect_prepare_replica_identity(%) first',
            v_tt.schema_name, v_tt.table_name, p_operation_id
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    SELECT public.flashback_internal_run_external_marker_transaction(
        v_op.tracking_id, v_rel_oid::bigint, v_op.generation_id, v_snapshot_id
    ) INTO v_result;

    RETURN jsonb_build_object(
        'schema_version', 1,
        'status', 'copy_in_progress',
        'operation_id', p_operation_id,
        'tracking_id', v_op.tracking_id,
        'generation_id', v_op.generation_id,
        'snapshot_id', v_snapshot_id,
        'copy', v_result,
        'note', 'the WAL boundary marker will commit when this call''s transaction commits; once committed, the background copier may still be writing its staged artifact -- call flashback_protect_external_publish(operation_id) next, in a new transaction, once it is done'
    );
END;
$$;

-- =================================================================
-- Step 9 Phase 2 correction: exact activation-readiness contract.
--
-- Replaces the previous, incorrect gate (flashback_health() = 'healthy')
-- with an explicit checklist of exact facts. flashback_health() mixes
-- durable coverage faults with purely operational/soft warnings
-- (capture_worker_missing, maintenance_worker_missing, slot_at_risk,
-- slot_lag_warning, local_budget_exhausted -- health_runtime.sql's own
-- CASE/ELSIF priority order) in ONE aggregate column; using it as the
-- sole activation predicate meant a merely-absent maintenance worker or a
-- soft WAL-lag warning -- neither of which says anything about whether
-- THIS artifact is real, verified, and durably recoverable -- could make
-- a genuinely valid initial artifact impossible to ever activate.
--
-- hard_failure distinguishes two different kinds of "not ready yet":
--   hard_failure = true  -- a real integrity/identity/slot-loss fault.
--                            This lifecycle's artifact cannot become
--                            recoverable from this evidence; retrying
--                            without operator intervention will not help.
--   hard_failure = false -- everything checked is consistent with success
--                            so far, but WAL consumption or the copier
--                            has not yet caught up. A later retry can
--                            succeed on its own once that finishes.
-- Neither case ever returns ready = true with anything skipped: readiness
-- requires every check to pass, in order, stopping at the first failure.
--
-- VOLATILE, not STABLE: this must observe generation/snapshot mutations
-- made immediately before it in the same outer flashback_protect_external_
-- publish call (flashback_internal_finalize_external_snapshot's publish +
-- activate), live pg_replication_slots state (flashback_slot_status_
-- snapshot), and filesystem-backed payload health (flashback_internal_
-- snapshot_payload_healthy) -- none of which STABLE's "same results for
-- the same arguments within one statement" contract may assume, and a
-- planner is free to fold/cache under that contract in ways that would
-- silently mask a caller's own prior writes.
CREATE OR REPLACE FUNCTION flashback_internal_protect_activation_readiness(
    p_tracking_id bigint,
    p_generation_id bigint,
    p_snapshot_id bigint
)
RETURNS TABLE (ready boolean, hard_failure boolean, reason text)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_cg flashback.coverage_generations%ROWTYPE;
    v_snap flashback.snapshots%ROWTYPE;
    v_cs flashback.capture_streams%ROWTYPE;
    v_slot record;
BEGIN
    -- 1. Exact generation identity: active, and bound to exactly this
    -- snapshot -- never a different or stale artifact.
    SELECT * INTO v_cg
    FROM flashback.coverage_generations
    WHERE generation_id = p_generation_id AND tracking_id = p_tracking_id;
    IF NOT FOUND
       OR v_cg.state IS DISTINCT FROM 'active'
       OR v_cg.storage_backend IS DISTINCT FROM 'external_zstd'
       OR v_cg.boundary_snapshot_id IS DISTINCT FROM p_snapshot_id
       OR v_cg.boundary_lsn IS NULL
    THEN
        ready := false; hard_failure := true;
        reason := 'generation is not active or does not match the expected snapshot identity';
        RETURN NEXT; RETURN;
    END IF;

    -- 2. Exact artifact identity: available, and its LSN agrees exactly
    -- with the generation's own boundary -- never a near-miss.
    SELECT * INTO v_snap
    FROM flashback.snapshots
    WHERE snapshot_id = p_snapshot_id AND tracking_id = p_tracking_id;
    IF NOT FOUND
       OR v_snap.storage_backend IS DISTINCT FROM 'external_zstd'
       OR v_snap.payload_state IS DISTINCT FROM 'available'
       OR v_snap.snapshot_lsn IS DISTINCT FROM v_cg.boundary_lsn
    THEN
        ready := false; hard_failure := true;
        reason := 'external artifact is not available or its snapshot LSN does not match the generation boundary';
        RETURN NEXT; RETURN;
    END IF;

    -- 3. Artifact verified, not merely marked available -- the same
    -- payload-health check flashback_admit_lsn_target itself requires
    -- before ever admitting an already-active generation for restore.
    IF NOT EXISTS (
        SELECT 1
        FROM public.flashback_internal_snapshot_payload_healthy(p_snapshot_id, p_tracking_id, false) h
        WHERE h.status = 'healthy'
    ) THEN
        ready := false; hard_failure := true;
        reason := 'external artifact failed payload-health verification';
        RETURN NEXT; RETURN;
    END IF;

    -- 4. Capture stream/slot has not been durably lost. A missing or
    -- explicitly broken-on-slot-loss stream can never self-heal; a slot
    -- that simply has not caught up yet is a separate, non-hard case (5).
    SELECT * INTO v_cs FROM flashback.capture_streams WHERE stream_id = v_cg.stream_id;
    IF NOT FOUND
       OR (
           v_cs.state = 'broken'
           AND COALESCE(v_cs.invalidation_reason, '') ~*
               '(slot|replication_slot|missing.slot|wal_status)'
       )
    THEN
        ready := false; hard_failure := true;
        reason := COALESCE(v_cs.invalidation_reason, 'capture stream is broken or missing');
        RETURN NEXT; RETURN;
    END IF;

    SELECT * INTO v_slot FROM public.flashback_slot_status_snapshot() LIMIT 1;
    IF COALESCE(v_slot.wal_status, '') = 'lost'
       OR COALESCE(v_slot.wal_status, '') = 'missing'
    THEN
        ready := false; hard_failure := true;
        reason := format('logical replication slot %s', v_slot.wal_status);
        RETURN NEXT; RETURN;
    END IF;

    -- 5. Frontier reaches at least the boundary: the stream has actually
    -- consumed WAL up to (or past) this generation's own boundary point,
    -- not merely been administratively marked active. Not yet reaching it
    -- is expected, ordinary, retriable progress -- never a hard failure.
    IF v_cs.valid_through_lsn IS NULL OR v_cs.valid_through_lsn < v_cg.boundary_lsn THEN
        ready := false; hard_failure := false;
        reason := 'capture stream frontier has not reached the generation boundary yet';
        RETURN NEXT; RETURN;
    END IF;

    -- 6. No open coverage gap invalidates the boundary (a parentless
    -- generation has no predecessor gap of its own, but a stream-wide
    -- discontinuity recorded against this lifecycle still must not be
    -- silently ignored).
    IF EXISTS (
        SELECT 1 FROM flashback.coverage_gaps gap
        WHERE gap.tracking_id = p_tracking_id
          AND gap.reanchored_by_generation_id IS NULL
    ) THEN
        ready := false; hard_failure := true;
        reason := 'an open coverage gap invalidates this boundary';
        RETURN NEXT; RETURN;
    END IF;

    ready := true; hard_failure := false; reason := NULL;
    RETURN NEXT;
END;
$$;

-- Publishes the staged external_zstd artifact and, only if activation-
-- readiness is fully satisfied, activates the generation and flips
-- protection_state 'starting' -> 'active' -- all inside one PL/pgSQL
-- subtransaction (SAVEPOINT-backed BEGIN/EXCEPTION block), so a committed
-- call of this function can never leave "generation active, protection_
-- state starting" behind: see this file's header comment ("ATOMICITY")
-- for the full argument. Filesystem publication (already durable by the
-- time the real publish call below runs) is untouched by this rollback --
-- an accepted, idempotently-resumable orphan, not a correctness gap.
CREATE OR REPLACE FUNCTION flashback_protect_external_publish(p_operation_id bigint)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_op record;
    v_snapshot_id bigint;
    v_publish jsonb;
    v_finalize jsonb;
    v_rolled_back boolean := false;
BEGIN
    SELECT s.*, o.details
      INTO v_op
    FROM flashback.operation_current_state s
    JOIN flashback.operations o ON o.operation_id = s.operation_id
    WHERE s.operation_id = p_operation_id
      AND s.command = 'protect'
      AND s.state = 'started';
    IF v_op.operation_id IS NULL
       OR v_op.details->>'storage_backend' IS DISTINCT FROM 'external_zstd'
    THEN
        RAISE EXCEPTION 'pg_flashback: operation % is not a pending external protect publish',
            p_operation_id USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    v_snapshot_id := (v_op.details->>'snapshot_id')::bigint;
    IF NOT EXISTS (
        SELECT 1
        FROM flashback.snapshots s
        WHERE s.snapshot_id = v_snapshot_id
          AND s.tracking_id = v_op.tracking_id
          AND s.payload_state IN ('creating', 'available')
          AND s.snapshot_lsn IS NOT NULL
    ) THEN
        -- No mutation has happened yet on this call; a plain early return
        -- is safe (nothing to roll back).
        RETURN jsonb_build_object(
            'schema_version', 1,
            'status', 'pending',
            'operation_id', p_operation_id,
            'reason', 'external boundary COMMIT LSN is not resolved yet'
        );
    END IF;

    BEGIN
        SELECT public.flashback_internal_finalize_external_snapshot(
            v_op.tracking_id, v_op.generation_id, v_snapshot_id
        ) INTO v_publish;
        v_finalize := public.flashback_protect_finalize(p_operation_id);
        IF COALESCE(v_finalize->>'status', '') <> 'activated' THEN
            -- Deliberately distinct SQLSTATE from every other exception
            -- this call chain can raise (including flashback_internal_
            -- publish_external_snapshot's own object_not_in_prerequisite_
            -- state / serialization_failure paths for a genuine identity/
            -- evidence fault, and flashback_internal_activate_external_
            -- generation's own object_not_in_prerequisite_state): only
            -- THIS specific, deliberately-raised condition may be
            -- absorbed as "roll back and report pending/failed" -- any
            -- other error here is a real fault and must propagate.
            RAISE EXCEPTION 'pg_flashback: protect activation-readiness not satisfied'
                USING ERRCODE = 'PF001', DETAIL = v_finalize::text;
        END IF;
    EXCEPTION WHEN SQLSTATE 'PF001' THEN
        v_rolled_back := true;
    END;

    IF v_rolled_back THEN
        RETURN jsonb_build_object(
            'schema_version', 1,
            'status', COALESCE(v_finalize->>'status', 'pending'),
            'operation_id', p_operation_id,
            'reason', v_finalize->>'reason',
            'hard_failure', COALESCE((v_finalize->>'hard_failure')::boolean, false),
            'note', 'external snapshot publication/generation activation for this call were rolled back; protection_state remains starting and the operation remains started/retriable -- the filesystem artifact, if already written, is an idempotently resumable receipt, not a correctness issue'
        );
    END IF;

    RETURN jsonb_build_object(
        'schema_version', 1,
        'status', 'activated',
        'operation_id', p_operation_id,
        'publish', v_publish,
        'finalize', v_finalize
    );
END;
$$;

-- Activate: the only authority that ever moves a Step 9 protect lifecycle's
-- protection_state out of 'starting'. Idempotent -- a repeat call once the
-- operation is already 'activated'/'failed'/'abandoned' just reports that
-- terminal state rather than re-running any of this. Also safely callable
-- on its own (not only from within flashback_protect_external_publish's
-- subtransaction): given a generation that is not yet 'active' at all, it
-- returns 'pending' and mutates nothing, exactly as before.
CREATE OR REPLACE FUNCTION flashback_protect_finalize(p_operation_id bigint)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_op record;
    v_tracking_id bigint;
    v_generation_id bigint;
    v_snapshot_id bigint;
    v_cg record;
    v_readiness record;
    v_n integer;
BEGIN
    SELECT s.*, o.details INTO v_op
    FROM flashback.operation_current_state s
    JOIN flashback.operations o ON o.operation_id = s.operation_id
    WHERE s.operation_id = p_operation_id
      AND s.command = 'protect';
    IF v_op.operation_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: unknown protect operation_id %', p_operation_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF v_op.state IN ('activated', 'failed', 'abandoned') THEN
        RETURN jsonb_build_object(
            'operation_id', p_operation_id, 'status', v_op.state,
            'note', 'already finalized'
        );
    END IF;

    v_tracking_id := v_op.tracking_id;
    v_generation_id := v_op.generation_id;
    v_snapshot_id := (v_op.details->>'snapshot_id')::bigint;

    SELECT cg.* INTO v_cg
    FROM flashback.coverage_generations cg
    WHERE cg.generation_id = v_generation_id
      AND cg.tracking_id = v_tracking_id;
    IF v_cg.generation_id IS NULL OR v_cg.state IS DISTINCT FROM 'active' THEN
        RETURN jsonb_build_object(
            'operation_id', p_operation_id,
            'status', 'pending',
            'hard_failure', false,
            'reason', 'generation boundary not activated yet',
            'generation_state', v_cg.state
        );
    END IF;

    -- Exact activation-readiness facts (above), never the generic
    -- flashback_health() aggregate: a soft operational warning
    -- (maintenance worker absence, slot lag, capacity) must never block a
    -- correctly verified activation, and a hard fault must never be
    -- reported as merely pending.
    SELECT * INTO v_readiness
    FROM public.flashback_internal_protect_activation_readiness(
        v_tracking_id, v_generation_id, v_snapshot_id
    );
    IF NOT v_readiness.ready THEN
        RETURN jsonb_build_object(
            'operation_id', p_operation_id,
            'status', 'pending',
            'hard_failure', v_readiness.hard_failure,
            'reason', v_readiness.reason,
            'note', CASE WHEN v_readiness.hard_failure
                THEN 'a real fault blocks activation; operator intervention (recreate slot, repair artifact, reanchor) is required, not merely a retry'
                ELSE 'generation is active but not yet fully verified; retry finalize later'
            END
        );
    END IF;

    -- CAS'd on the exact expected prior value, matching every other
    -- protection_state mutation in unprotect_cleanup.sql: no raw
    -- unconditional UPDATE bypass.
    UPDATE flashback.tracked_tables
       SET protection_state = 'active'
     WHERE tracking_id = v_tracking_id
       AND protection_state = 'starting';
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> 1 THEN
        RAISE EXCEPTION 'pg_flashback: tracking % protection_state was not ''starting'' at activation (raced?)',
            v_tracking_id
            USING ERRCODE = 'serialization_failure';
    END IF;

    PERFORM flashback_operation_append_event(
        p_operation_id, 'activated', NULL, NULL,
        'external artifact published and generation activated; protection_state starting -> active',
        jsonb_build_object('tracking_id', v_tracking_id, 'generation_id', v_generation_id)
    );

    RETURN jsonb_build_object(
        'schema_version', 1,
        'operation_id', p_operation_id,
        'status', 'activated',
        'tracking_id', v_tracking_id,
        'generation_id', v_generation_id,
        'note', 'table is now actively protected under external_zstd'
    );
END;
$$;
