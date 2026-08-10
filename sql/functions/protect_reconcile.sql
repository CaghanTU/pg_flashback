-- =================================================================
-- Step 9 Phase 3: durable-state projection, bounded reconciler, and
-- crash-resumable abort authority for the online (external_zstd) protect
-- lifecycle (protect_online.sql). Nothing here redesigns SnapshotStore,
-- marker coordination, WAL promotion, or the artifact format -- it only
-- reads their durable state and drives the four already-accepted step
-- functions (flashback_protect_begin/prepare_replica_identity/
-- external_copy/external_publish) or the abort authority below.
--
-- SINGLE SOURCE OF TRUTH: flashback_protect_next_action is the only place
-- that classifies "what should happen next" for a protect operation. The
-- CLI, flashback_doctor(), status, and the bounded reconciler all call it
-- rather than re-deriving their own state logic.
--
-- SOLE TERMINAL JOURNAL WRITER: flashback_protect_abort is the only
-- function that ever appends a 'failed' or 'abandoned' event for a
-- 'protect' operation, and it does so as its last step, after every
-- durable cleanup (snapshot/generation/artifact/replica-identity) has
-- already converged. There is no code path where a 'started' protect
-- operation becomes terminal while cleanup is still outstanding -- a
-- hard failure is a durable, non-terminal 'blocked' classification until
-- flashback_protect_abort (operator- or reconciler-invoked, see below)
-- finishes converging it. Elapsed time never drives a state transition
-- anywhere in this file; it is surfaced only as an informational field.
--
-- PHASE OWNERSHIP: the bounded maintenance reconciler
-- (flashback_internal_reconcile_external_protect) never takes a relation
-- lock, never launches a copier process, and never runs
-- flashback_protect_prepare_replica_identity or
-- flashback_protect_external_copy -- those remain CLI/operator-only. It
-- only ever calls the already-cheap, DB-only publish pair
-- (flashback_internal_finalize_external_snapshot +
-- flashback_protect_finalize) or converges an already-decided abort
-- (flashback_protect_abort, itself idempotent). Everything else is
-- reported, never acted on -- so a 'blocked' operation is never retried
-- automatically by anything, ever.
-- =================================================================

-- ------------------------------------------------------------------
-- flashback_protect_next_action: ordered, first-match-wins truth table.
-- Every branch is evaluated in the order below; the first match is
-- returned. The final ELSE is an explicit fail-closed catch-all -- an
-- unrecognized combination of durable state is never guessed into one of
-- the earlier rows.
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_protect_next_action(p_operation_id bigint)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_op record;
    v_tt flashback.tracked_tables%ROWTYPE;
    v_cg flashback.coverage_generations%ROWTYPE;
    v_snap flashback.snapshots%ROWTYPE;
    v_rel_oid oid;
    v_current_oid oid;
    v_current_relident "char";
    v_artifact jsonb;
    v_artifact_status text;
    v_readiness record;
    v_elapsed_seconds double precision;
    v_stale boolean;
    v_action text;
    v_hard_failure boolean;
    v_abortable boolean;
    v_automatic boolean;
    v_reason text;
BEGIN
    IF p_operation_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: flashback_protect_next_action requires operation_id'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT s.*, o.details
      INTO v_op
    FROM flashback.operation_current_state s
    JOIN flashback.operations o ON o.operation_id = s.operation_id
    WHERE s.operation_id = p_operation_id
      AND s.command = 'protect';
    IF v_op.operation_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: unknown protect operation_id %', p_operation_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    v_elapsed_seconds := extract(epoch FROM (clock_timestamp() - v_op.created_at));
    -- Informational threshold only -- never an input to any branch below.
    v_stale := v_elapsed_seconds > 300;
    v_rel_oid := (v_op.details->>'rel_oid')::oid;

    -- Row 1: terminal states report themselves; nothing else matters once
    -- an operation is activated/failed/abandoned.
    IF v_op.state IN ('activated', 'failed', 'abandoned') THEN
        RETURN jsonb_build_object(
            'operation_id', p_operation_id,
            'tracking_id', v_op.tracking_id,
            'generation_id', v_op.generation_id,
            'snapshot_id', (v_op.details->>'snapshot_id')::bigint,
            'action', CASE WHEN v_op.state = 'activated' THEN 'complete' ELSE v_op.state END,
            'hard_failure', false,
            'abortable', false,
            'automatic', null,
            'reason', null,
            'protection_state', null,
            'generation_state', null,
            'artifact_state', null,
            'elapsed_seconds', v_elapsed_seconds,
            'stale', v_stale
        );
    END IF;

    -- tracked_tables is fetched before the identity check below because the
    -- identity check needs its live schema_name/table_name (kept current by
    -- the DDL hook, unlike operations.table_name which is an immutable
    -- point-in-time label captured once at flashback_protect_begin and
    -- would go stale -- and falsely read as "identity changed" -- across a
    -- legitimate rename).
    SELECT * INTO v_tt FROM flashback.tracked_tables WHERE tracking_id = v_op.tracking_id;
    IF v_tt.tracking_id IS NULL THEN
        RETURN jsonb_build_object(
            'operation_id', p_operation_id, 'tracking_id', v_op.tracking_id,
            'generation_id', v_op.generation_id,
            'snapshot_id', (v_op.details->>'snapshot_id')::bigint,
            'action', 'blocked', 'hard_failure', true, 'abortable', true, 'automatic', false,
            'reason', 'lifecycle_missing_or_inconsistent',
            'protection_state', null, 'generation_state', null, 'artifact_state', null,
            'elapsed_seconds', v_elapsed_seconds, 'stale', v_stale
        );
    END IF;

    -- Rows 2-3: exact live relation identity, re-derived the same way the
    -- accepted step functions themselves do (protect_online.sql M2/M3
    -- style) -- never trusted from operations.table_name.
    SELECT c.oid INTO v_current_oid
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = v_tt.schema_name AND c.relname = v_tt.table_name;

    IF v_current_oid IS NULL THEN
        v_action := 'blocked'; v_hard_failure := true; v_abortable := true; v_automatic := false;
        v_reason := 'relation_missing';
    ELSIF v_current_oid IS DISTINCT FROM v_rel_oid THEN
        v_action := 'blocked'; v_hard_failure := true; v_abortable := true; v_automatic := false;
        v_reason := 'relation_identity_changed';
    -- Row 4 (remaining case): tracked_tables was found but is not the
    -- expected live shape for a 'started' protect op.
    ELSIF NOT v_tt.is_active AND v_tt.protection_state NOT IN ('starting', 'abandoned') THEN
        v_action := 'blocked'; v_hard_failure := true; v_abortable := true; v_automatic := false;
        v_reason := format('protection_state_inconsistent:%s', v_tt.protection_state);
    -- Row 5: abort cleanup already converged (is_active=false,
    -- protection_state='abandoned'); only the terminal journal append is
    -- outstanding -- cheap, idempotent, reconciler-eligible.
    ELSIF v_tt.protection_state = 'abandoned' THEN
        v_action := 'abort_finalize'; v_hard_failure := false; v_abortable := true; v_automatic := true;
        v_reason := 'abort_cleanup_converged_journal_pending';
    -- Row 6: any other non-'starting' protection_state while operation_
    -- current_state (read earlier, at function entry) still showed
    -- 'started' is USUALLY a race, not a corruption: two separate
    -- statements under READ COMMITTED each take their own snapshot, so the
    -- reconciler's finalize (protection_state 'starting'->'active' AND the
    -- 'activated' journal event, committed together as one transaction --
    -- see flashback_protect_finalize) can commit in the gap between this
    -- function's v_op read and its v_tt read, leaving v_op stale. Re-check
    -- fresh before concluding "inconsistent": if the operation has in fact
    -- reached a terminal state since v_op was read, report that terminal
    -- state (matching row 1) rather than a false 'blocked'.
    ELSIF v_tt.protection_state IS DISTINCT FROM 'starting' THEN
        SELECT s.state INTO v_op.state
        FROM flashback.operation_current_state s
        WHERE s.operation_id = p_operation_id;
        IF v_op.state IN ('activated', 'failed', 'abandoned') THEN
            RETURN jsonb_build_object(
                'operation_id', p_operation_id, 'tracking_id', v_op.tracking_id,
                'generation_id', v_op.generation_id,
                'snapshot_id', (v_op.details->>'snapshot_id')::bigint,
                'action', CASE WHEN v_op.state = 'activated' THEN 'complete' ELSE v_op.state END,
                'hard_failure', false, 'abortable', false, 'automatic', null, 'reason', null,
                'protection_state', v_tt.protection_state, 'generation_state', null, 'artifact_state', null,
                'elapsed_seconds', v_elapsed_seconds, 'stale', v_stale
            );
        END IF;
        v_action := 'blocked'; v_hard_failure := true; v_abortable := true; v_automatic := false;
        v_reason := format('protection_state_inconsistent:%s', COALESCE(v_tt.protection_state, 'active'));
    ELSE
        -- Normal path: identity ok, protection_state='starting'. Fetch the
        -- generation and snapshot once for everything below.
        SELECT * INTO v_cg
        FROM flashback.coverage_generations
        WHERE generation_id = v_op.generation_id AND tracking_id = v_op.tracking_id;
        SELECT * INTO v_snap
        FROM flashback.snapshots
        WHERE snapshot_id = (v_op.details->>'snapshot_id')::bigint AND tracking_id = v_op.tracking_id;

        SELECT c.relreplident INTO v_current_relident
        FROM pg_class c WHERE c.oid = v_current_oid;

        IF v_cg.generation_id IS NULL OR v_cg.state IN ('sealed', 'retired') THEN
            -- Row 7.
            v_action := 'blocked'; v_hard_failure := true; v_abortable := true; v_automatic := false;
            v_reason := format('generation_state_unexpected:%s', COALESCE(v_cg.state, 'missing'));
        ELSIF v_cg.state = 'aborted' THEN
            -- Row 8: generation cleanup already converged, protection_state
            -- CAS to 'abandoned' + journal append still outstanding.
            v_action := 'abort_finalize'; v_hard_failure := false; v_abortable := true; v_automatic := true;
            v_reason := 'generation_already_aborted_journal_pending';
        ELSIF v_snap.snapshot_id IS NULL OR v_snap.payload_state = 'aborted' THEN
            -- Mid-abort crash: flashback_protect_abort's snapshot-abort
            -- step (4) committed but its generation-abort step (5) has not
            -- caught up yet (or the snapshot row is simply gone). Not row 8
            -- (that is keyed off generation.state='aborted' specifically)
            -- but the same convergent remedy applies: re-invoking
            -- flashback_protect_abort is idempotent and safe here too.
            v_action := 'abort_finalize'; v_hard_failure := false; v_abortable := true; v_automatic := true;
            v_reason := 'snapshot_already_aborted_generation_pending';
        ELSIF v_snap.payload_state NOT IN ('creating', 'available') THEN
            -- Genuinely unrecognized: 'retiring'/'retired'/'missing' has no
            -- legitimate meaning before this lifecycle has ever activated.
            -- Never guessed into a copy/publish action.
            v_action := 'blocked'; v_hard_failure := true; v_abortable := true; v_automatic := false;
            v_reason := format(
                'unrecognized_state_combination:generation=%s,snapshot=%s',
                v_cg.state, v_snap.payload_state
            );
        ELSIF v_snap.payload_state = 'available' AND v_cg.state <> 'active' THEN
            -- The snapshot is already durably published even though the
            -- generation has not yet caught up to 'active' -- confirmed
            -- reachable empirically: flashback_internal_finalize_external_
            -- snapshot's publish (creating->available) and its activation
            -- (capturing->active) are separate committed sub-steps (see the
            -- finalizer_after_db_available / finalizer_after_activation
            -- failpoints, each its own durability boundary), so a
            -- concurrent reader can observe 'available' while the
            -- generation still reads 'building'/'capturing'. The correct,
            -- safe action regardless of which exact sub-step landed is the
            -- same idempotent publish/finalize retry -- never re-inspect
            -- the filesystem artifact once the database already shows the
            -- snapshot published. `AND v_cg.state <> 'active'` is load-
            -- bearing, confirmed by a real test failure during authoring:
            -- once the generation itself has already reached 'active',
            -- payload_state stays 'available' forever (it never reverts),
            -- so without this guard every subsequent call would keep
            -- returning 'publish' and skip the row 15-17 readiness check
            -- entirely -- silently masking a genuine hard failure (row 16)
            -- behind an endless "safe to retry publish" classification.
            v_action := 'publish'; v_hard_failure := false;
            v_abortable := true; v_automatic := true; v_reason := null;
        ELSIF v_cg.state = 'building' THEN
            IF v_current_relident IS DISTINCT FROM 'f' THEN
                -- Row 9.
                v_action := 'prepare_replica_identity'; v_hard_failure := false;
                v_abortable := true; v_automatic := false; v_reason := null;
            ELSIF v_cg.boundary_xid IS NULL THEN
                -- Row 10: step 3 never committed, or rolled back -- same
                -- reservation, safe to retry. boundary_marker is NOT a
                -- reliable "step 3 ran" signal by itself: it already holds
                -- a placeholder ('online_pending:<nonce>') from reservation
                -- time onward (flashback_internal_reserve_online_generation,
                -- snapshot_store.sql:1447) and is only overwritten by M4
                -- (flashback_internal_bind_online_boundary) inside step 3's
                -- marker transaction. boundary_xid, by contrast, is CAS'd
                -- from NULL only by that same M4 UPDATE
                -- (snapshot_store.sql:1652-1657) -- it is null-until-M4 by
                -- construction, exactly the signal needed here.
                v_action := 'run_external_copy'; v_hard_failure := false;
                v_abortable := true; v_automatic := false; v_reason := null;
            ELSE
                -- Row 11: step 3 committed; WAL has not yet promoted the
                -- boundary. Never synthesize an LSN -- just wait.
                v_action := 'wait_for_boundary'; v_hard_failure := false;
                v_abortable := true; v_automatic := true; v_reason := null;
            END IF;
        ELSIF v_cg.state = 'capturing' THEN
            v_artifact := public.flashback_internal_external_artifact_state(
                v_op.tracking_id, v_op.generation_id, (v_op.details->>'snapshot_id')::bigint
            );
            v_artifact_status := v_artifact->>'status';
            IF v_artifact_status IN ('absent', 'staging_incomplete') THEN
                -- Row 12: boundary is immutably bound and WAL-promoted, but
                -- no usable artifact exists and the copier is not actively
                -- writing one. Re-running the marker transaction would
                -- re-bind an already-bound boundary -- unrecoverable on the
                -- accepted contract without a fresh protect attempt.
                v_action := 'blocked'; v_hard_failure := true; v_abortable := true; v_automatic := false;
                v_reason := 'external_copier_crashed_after_boundary_bound_unrecoverable';
            ELSIF v_artifact_status = 'staging_active' THEN
                -- Row 13.
                v_action := 'wait_for_copy'; v_hard_failure := false;
                v_abortable := true; v_automatic := true; v_reason := null;
            ELSIF v_artifact_status IN ('staging_committed', 'published') THEN
                -- Row 14.
                v_action := 'publish'; v_hard_failure := false;
                v_abortable := true; v_automatic := true; v_reason := null;
            ELSE
                v_action := 'blocked'; v_hard_failure := true; v_abortable := true; v_automatic := false;
                v_reason := format('unrecognized_artifact_state:%s', COALESCE(v_artifact_status, 'null'));
            END IF;
        ELSIF v_cg.state = 'active' THEN
            SELECT * INTO v_readiness
            FROM public.flashback_internal_protect_activation_readiness(
                v_op.tracking_id, v_op.generation_id, (v_op.details->>'snapshot_id')::bigint
            );
            IF v_readiness.ready THEN
                -- Row 15.
                v_action := 'finalize'; v_hard_failure := false;
                v_abortable := true; v_automatic := true; v_reason := null;
            ELSIF v_readiness.hard_failure THEN
                -- Row 16.
                v_action := 'blocked'; v_hard_failure := true; v_abortable := true; v_automatic := false;
                v_reason := v_readiness.reason;
            ELSE
                -- Row 17: stream frontier has not caught up to the boundary
                -- yet -- ordinary, retriable, never a hard failure.
                v_action := 'wait_for_boundary'; v_hard_failure := false;
                v_abortable := true; v_automatic := true; v_reason := v_readiness.reason;
            END IF;
        ELSE
            -- Row 18: anything not matched above.
            v_action := 'blocked'; v_hard_failure := true; v_abortable := true; v_automatic := false;
            v_reason := 'unrecognized_state_combination';
        END IF;

        RETURN jsonb_build_object(
            'operation_id', p_operation_id, 'tracking_id', v_op.tracking_id,
            'generation_id', v_op.generation_id,
            'snapshot_id', (v_op.details->>'snapshot_id')::bigint,
            'action', v_action, 'hard_failure', v_hard_failure,
            'abortable', v_abortable, 'automatic', v_automatic, 'reason', v_reason,
            'protection_state', v_tt.protection_state,
            'generation_state', v_cg.state,
            'artifact_state', v_artifact_status,
            'elapsed_seconds', v_elapsed_seconds, 'stale', v_stale
        );
    END IF;

    RETURN jsonb_build_object(
        'operation_id', p_operation_id, 'tracking_id', v_op.tracking_id,
        'generation_id', v_op.generation_id,
        'snapshot_id', (v_op.details->>'snapshot_id')::bigint,
        'action', v_action, 'hard_failure', v_hard_failure,
        'abortable', v_abortable, 'automatic', v_automatic, 'reason', v_reason,
        'protection_state', v_tt.protection_state, 'generation_state', null, 'artifact_state', null,
        'elapsed_seconds', v_elapsed_seconds, 'stale', v_stale
    );
END;
$$;

COMMENT ON FUNCTION flashback_protect_next_action(bigint) IS
    'Single source of truth for what should happen next for a protect operation. Ordered, first-match-wins classification of durable state; unrecognized combinations fail closed to blocked. Never mutates. Elapsed time is informational only.';

-- ------------------------------------------------------------------
-- flashback_protect_find_resumable: CLI resume-by-identity entrypoint.
-- Thin operator-facing wrapper around the canonical, ambiguity-safe
-- active-lifecycle resolver (flashback_internal_resolve_tracked_table,
-- the same one flashback_unprotect/flashback_recover_plan already use) --
-- never a table_name text match against operations.table_name, never
-- "latest by name". Raises (fails closed) on an ambiguous identifier or
-- on a protection_state='starting' lifecycle with no matching 'started'
-- protect operation, rather than guessing.
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_protect_find_resumable(p_table text)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_row record;
    v_protection_state text;
    v_op_id bigint;
BEGIN
    SELECT * INTO v_row FROM public.flashback_internal_resolve_tracked_table(p_table);
    IF v_row.tracking_id IS NULL THEN
        RETURN jsonb_build_object('found', false);
    END IF;

    SELECT tt.protection_state INTO v_protection_state
    FROM flashback.tracked_tables tt WHERE tt.tracking_id = v_row.tracking_id;
    IF COALESCE(v_protection_state, 'active') IS DISTINCT FROM 'starting' THEN
        RETURN jsonb_build_object('found', false);
    END IF;

    SELECT s.operation_id INTO v_op_id
    FROM flashback.operation_current_state s
    WHERE s.tracking_id = v_row.tracking_id
      AND s.command = 'protect'
      AND s.state = 'started';
    IF v_op_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: tracking % is protection_state=starting but has no started protect operation', v_row.tracking_id
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    RETURN jsonb_build_object(
        'found', true,
        'tracking_id', v_row.tracking_id,
        'operation_id', v_op_id,
        'table_name', format('%I.%I', v_row.schema_name, v_row.table_name)
    );
END;
$$;

COMMENT ON FUNCTION flashback_protect_find_resumable(text) IS
    'CLI resume-by-identity: finds the unique started protect operation for the currently active tracked lifecycle matching this table, via the canonical ambiguity-safe resolver. Never a table_name text match.';

-- ------------------------------------------------------------------
-- flashback_protect_abort: crash-resumable abort/cleanup authority.
-- The ONLY function that ever appends a terminal 'failed'/'abandoned'
-- event for a protect operation, and only as its last step. Every
-- sub-step below is independently idempotent (CAS or explicit no-op
-- guard), so a repeated call after a crash at any point converges
-- without redoing already-complete work.
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_protect_abort(p_operation_id bigint)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_op record;
    v_tt flashback.tracked_tables%ROWTYPE;
    v_cg flashback.coverage_generations%ROWTYPE;
    v_snapshot_id bigint;
    v_rel_oid oid;
    v_db_oid oid;
    v_current_oid oid;
    v_current_relident "char";
    v_target_relident "char";
    v_index_exists boolean;
    v_identity_restored boolean := false;
    v_identity_skip_reason text := null;
    v_purged boolean;
    v_pre_action jsonb;
    v_hard_failure boolean := false;
    v_terminal_state text;
    v_n integer;
BEGIN
    IF p_operation_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: flashback_protect_abort requires operation_id'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- Step 1: idempotent terminal short-circuit, before taking any lock.
    SELECT s.*, o.details INTO v_op
    FROM flashback.operation_current_state s
    JOIN flashback.operations o ON o.operation_id = s.operation_id
    WHERE s.operation_id = p_operation_id AND s.command = 'protect';
    IF v_op.operation_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: unknown protect operation_id %', p_operation_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF v_op.state = 'activated' THEN
        RAISE EXCEPTION 'pg_flashback: protect operation % is already activated and cannot be aborted', p_operation_id
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    IF v_op.state IN ('failed', 'abandoned') THEN
        RETURN jsonb_build_object(
            'operation_id', p_operation_id, 'status', v_op.state,
            'note', 'already converged; no-op'
        );
    END IF;

    v_rel_oid := (v_op.details->>'rel_oid')::oid;
    v_snapshot_id := (v_op.details->>'snapshot_id')::bigint;
    v_db_oid := (SELECT oid FROM pg_database WHERE datname = current_database());

    -- Step 2: canonical lock order, blocking (operator-invoked, bounded,
    -- single-lifecycle -- not the reconciler's non-blocking discipline).
    PERFORM flashback_internal_lock_database_stream(v_db_oid);
    PERFORM flashback_internal_lock_lifecycle(v_op.tracking_id);

    -- Step 3: re-read under lock; refuse if a concurrent finalize won the
    -- race between step 1's pre-lock check and now.
    SELECT * INTO v_tt FROM flashback.tracked_tables WHERE tracking_id = v_op.tracking_id FOR UPDATE;
    IF v_tt.tracking_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: tracking % not found for protect operation %', v_op.tracking_id, p_operation_id
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    SELECT * INTO v_cg
    FROM flashback.coverage_generations
    WHERE generation_id = v_op.generation_id AND tracking_id = v_op.tracking_id
    FOR UPDATE;
    IF v_tt.protection_state = 'active' OR (v_cg.generation_id IS NOT NULL AND v_cg.state = 'active') THEN
        RAISE EXCEPTION 'pg_flashback: protect operation % raced to activation and cannot be aborted', p_operation_id
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    -- Evidence for the terminal classification in step 9, captured before
    -- any mutation below changes the picture.
    v_pre_action := public.flashback_protect_next_action(p_operation_id);
    v_hard_failure := COALESCE((v_pre_action->>'hard_failure')::boolean, false);

    -- Step 4: snapshot -- CAS from 'creating'; no-op if already 'aborted'.
    IF v_snapshot_id IS NOT NULL THEN
        PERFORM public.flashback_internal_snapshot_abort(v_snapshot_id, v_op.tracking_id);
    END IF;

    -- Step 5: generation -- CAS from its observed current state; no-op if
    -- already 'aborted'.
    IF v_cg.generation_id IS NOT NULL AND v_cg.state IN ('building', 'capturing') THEN
        PERFORM public.flashback_internal_transition_coverage_generation(
            v_cg.generation_id, v_op.tracking_id, v_cg.state, 'aborted',
            'operator_protect_abort', NULL, NULL, NULL, NULL, NULL, NULL,
            jsonb_build_object('protect_abort_operation_id', p_operation_id)
        );
    END IF;

    -- Step 6: artifact purge, idempotency-receipted exactly like the
    -- reconciler's own Phase 1 cleanup.
    IF v_snapshot_id IS NOT NULL AND v_cg.generation_id IS NOT NULL
       AND NOT EXISTS (
           SELECT 1 FROM flashback.external_artifact_cleanup_receipts
           WHERE snapshot_id = v_snapshot_id
       )
    THEN
        BEGIN
            v_purged := public.flashback_internal_purge_aborted_external_artifact(
                v_op.tracking_id, v_cg.generation_id, v_snapshot_id
            );
            INSERT INTO flashback.external_artifact_cleanup_receipts (
                snapshot_id, tracking_id, generation_id, operation_nonce, artifact_was_present
            ) VALUES (
                v_snapshot_id, v_op.tracking_id, v_cg.generation_id,
                (v_op.details->>'operation_nonce')::bigint, COALESCE(v_purged, false)
            ) ON CONFLICT (snapshot_id) DO NOTHING;
        EXCEPTION WHEN OTHERS THEN
            -- A live copier lease or transient IO error: leave the receipt
            -- absent so a later retry (of this same protect_abort call)
            -- tries again. Never falsify a receipt.
            NULL;
        END;
    END IF;

    -- Step 7: replica identity restoration -- explicit, never a blind
    -- ALTER.
    SELECT c.oid, c.relreplident INTO v_current_oid, v_current_relident
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = v_tt.schema_name AND c.relname = v_tt.table_name;

    IF v_current_oid IS DISTINCT FROM v_rel_oid THEN
        v_identity_skip_reason := 'relation_identity_changed';
    ELSE
        v_target_relident := COALESCE(v_tt.replica_identity_was, 'd');
        IF v_current_relident = v_target_relident THEN
            v_identity_restored := true; -- already restored (prior crash/retry)
        ELSIF v_current_relident IS DISTINCT FROM 'f' THEN
            v_identity_skip_reason := 'replica_identity_changed_by_third_party';
        ELSIF v_target_relident = 'i' THEN
            SELECT EXISTS (
                SELECT 1
                FROM pg_index i
                JOIN pg_class ic ON ic.oid = i.indexrelid
                WHERE i.indrelid = v_current_oid
                  AND ic.relname = v_tt.replica_identity_index
            ) INTO v_index_exists;
            IF NOT v_index_exists THEN
                v_identity_skip_reason := 'original_replica_identity_index_missing';
            ELSE
                PERFORM flashback_set_restore_in_progress(true);
                BEGIN
                    EXECUTE format(
                        'ALTER TABLE %I.%I REPLICA IDENTITY USING INDEX %I',
                        v_tt.schema_name, v_tt.table_name, v_tt.replica_identity_index
                    );
                EXCEPTION WHEN OTHERS THEN
                    PERFORM flashback_set_restore_in_progress(false);
                    RAISE;
                END;
                PERFORM flashback_set_restore_in_progress(false);
                v_identity_restored := true;
            END IF;
        ELSE
            PERFORM flashback_set_restore_in_progress(true);
            BEGIN
                IF v_target_relident = 'n' THEN
                    EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY NOTHING', v_tt.schema_name, v_tt.table_name);
                ELSE
                    EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY DEFAULT', v_tt.schema_name, v_tt.table_name);
                END IF;
            EXCEPTION WHEN OTHERS THEN
                PERFORM flashback_set_restore_in_progress(false);
                RAISE;
            END;
            PERFORM flashback_set_restore_in_progress(false);
            v_identity_restored := true;
        END IF;
    END IF;

    -- Step 8: deactivate the lifecycle -- CAS, kept (not deleted).
    UPDATE flashback.tracked_tables
       SET protection_state = 'abandoned', is_active = false
     WHERE tracking_id = v_op.tracking_id
       AND protection_state = 'starting';
    GET DIAGNOSTICS v_n = ROW_COUNT;
    -- v_n = 0 means a prior crashed attempt already made this CAS; that is
    -- an expected, idempotent no-op, not an error.

    -- Step 9: exactly one terminal journal event, last, unconditionally.
    v_terminal_state := CASE WHEN v_hard_failure THEN 'failed' ELSE 'abandoned' END;
    PERFORM flashback_operation_append_event(
        p_operation_id, v_terminal_state, NULL, NULL,
        CASE WHEN v_hard_failure
             THEN 'protect aborted: durable hard failure, cleaned up'
             ELSE 'protect aborted by operator' END,
        jsonb_build_object(
            'tracking_id', v_op.tracking_id,
            'generation_id', v_cg.generation_id,
            'snapshot_id', v_snapshot_id,
            'pre_abort_reason', v_pre_action->>'reason',
            'identity_restored', v_identity_restored,
            'identity_restore_skipped_reason', v_identity_skip_reason
        )
    );

    RETURN jsonb_build_object(
        'operation_id', p_operation_id,
        'status', v_terminal_state,
        'tracking_id', v_op.tracking_id,
        'identity_restored', v_identity_restored,
        'identity_restore_skipped_reason', v_identity_skip_reason,
        'note', 'generation/snapshot/artifact cleanup converged; lifecycle deactivated'
    );
END;
$$;

COMMENT ON FUNCTION flashback_protect_abort(bigint) IS
    'Crash-resumable abort of an incomplete protect operation. Only function that ever journals protect failed/abandoned, always as its last step. Restores replica identity only when the live relation still matches and no third party changed it first.';

-- ------------------------------------------------------------------
-- flashback_internal_reconcile_external_protect: bounded, worker-invoked.
-- Never takes a relation lock, never launches a copier, never calls
-- flashback_protect_prepare_replica_identity or
-- flashback_protect_external_copy, never calls flashback_consume_wal.
-- Only ever executes an already-cheap publish or an already-converged
-- abort_finalize; everything else is reported as resume_required.
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_internal_reconcile_external_protect(
    p_limit integer DEFAULT 5
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_op record;
    v_next jsonb;
    v_resumed integer := 0;
    v_finalized integer := 0;
    v_deferred integer := 0;
    v_errors integer := 0;
    v_last_error text;
BEGIN
    IF p_limit IS NULL OR p_limit < 1 OR p_limit > 100 THEN
        RAISE EXCEPTION 'pg_flashback: external protect reconcile limit must be between 1 and 100'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- Canonical order begins with the database-stream lock, non-blocking:
    -- capture correctness always wins over maintenance progress. This is
    -- the SAME lock (namespace 358945) the delta worker's own WAL-consume
    -- self-serialization uses; taking it here does not itself consume WAL
    -- (flashback_consume_wal is never called anywhere in this function).
    IF NOT pg_try_advisory_xact_lock(
        public.flashback_internal_lock_ns_stream(),
        (SELECT oid::integer FROM pg_database WHERE datname = current_database())
    ) THEN
        RETURN jsonb_build_object('status', 'busy', 'resumed', 0, 'finalized', 0, 'deferred', 1, 'errors', 0);
    END IF;

    FOR v_op IN
        SELECT s.operation_id, s.tracking_id, s.created_at
        FROM flashback.operation_current_state s
        JOIN flashback.operations o ON o.operation_id = s.operation_id
        WHERE s.command = 'protect'
          AND s.state = 'started'
          AND o.details->>'storage_backend' = 'external_zstd'
        ORDER BY s.created_at, s.operation_id
        LIMIT p_limit
    LOOP
        IF NOT public.flashback_internal_try_lock_lifecycle(v_op.tracking_id) THEN
            v_deferred := v_deferred + 1;
            CONTINUE;
        END IF;

        BEGIN
            v_next := public.flashback_protect_next_action(v_op.operation_id);
            IF COALESCE((v_next->>'automatic')::boolean, false) THEN
                IF v_next->>'action' = 'publish' THEN
                    PERFORM public.flashback_internal_finalize_external_snapshot(
                        v_op.tracking_id, (v_next->>'generation_id')::bigint, (v_next->>'snapshot_id')::bigint
                    );
                    PERFORM public.flashback_protect_finalize(v_op.operation_id);
                    v_resumed := v_resumed + 1;
                ELSIF v_next->>'action' = 'abort_finalize' THEN
                    PERFORM public.flashback_protect_abort(v_op.operation_id);
                    v_finalized := v_finalized + 1;
                ELSE
                    -- wait_for_boundary / wait_for_copy: a genuine no-op --
                    -- nothing to do but observe.
                    v_deferred := v_deferred + 1;
                END IF;
            ELSE
                -- prepare_replica_identity / run_external_copy / blocked:
                -- CLI/operator-only. Never executed here.
                v_deferred := v_deferred + 1;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            -- Preserve the started reservation for retry. Never widen to a
            -- terminal write from this catch-all.
            v_deferred := v_deferred + 1;
            v_errors := v_errors + 1;
            v_last_error := SQLSTATE || ': ' || SQLERRM;
        END;
    END LOOP;

    RETURN jsonb_build_object(
        'status', CASE WHEN v_errors > 0 THEN 'partial' ELSE 'ok' END,
        'resumed', v_resumed,
        'finalized', v_finalized,
        'deferred', v_deferred,
        'errors', v_errors,
        'last_error', v_last_error
    );
END;
$$;

COMMENT ON FUNCTION flashback_internal_reconcile_external_protect(integer) IS
    '[Internal] Bounded protect reconciler: only ever resumes an already-cheap publish or converges an already-decided abort_finalize. Never drives prepare_replica_identity, external_copy, or flashback_consume_wal.';

-- Thin operator-facing wrapper, matching the existing
-- flashback_reconcile_recover_operations() surface used by doctor
-- --reconcile.
CREATE OR REPLACE FUNCTION flashback_reconcile_protect_operations(
    p_limit integer DEFAULT 5
)
RETURNS jsonb
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
    SELECT flashback_internal_reconcile_external_protect(p_limit);
$$;

COMMENT ON FUNCTION flashback_reconcile_protect_operations(integer) IS
    'Operator-facing wrapper around the internal bounded protect reconciler. Safe to call from doctor --reconcile or ad hoc.';

REVOKE ALL ON FUNCTION public.flashback_internal_reconcile_external_protect(integer) FROM PUBLIC;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flashback_admin') THEN
        REVOKE ALL ON FUNCTION public.flashback_internal_reconcile_external_protect(integer) FROM flashback_admin;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pg_monitor') THEN
        REVOKE ALL ON FUNCTION public.flashback_internal_reconcile_external_protect(integer) FROM pg_monitor;
    END IF;
END
$$;
