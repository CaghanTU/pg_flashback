-- =================================================================
-- Operator maintenance, local_delta storage budget/freeze, and safe
-- uninstall preparation.
--
-- STORAGE_POLICY (documented here, the single source of truth for the
-- retained-payload budget used by flashback_lifecycle_storage_metrics() /
-- flashback_storage_freeze_scan()):
--   * pg_flashback.local_max_snapshot_bytes / local_max_restore_peak_bytes /
--     local_min_filesystem_bytes (local_capacity.sql) already gate the
--     POINT-IN-TIME cost of one track/re-anchor/restore. They say nothing
--     about how much history a lifecycle is allowed to *retain* over time
--     (sealed snapshots + delta_log rows across many generations).
--   * pg_flashback.local_max_retained_payload_bytes is the explicit,
--     optional hard ceiling on retained_local_payload_bytes (see
--     flashback_measure_local_capacity()) for one lifecycle. When unset, the
--     hard ceiling defaults to 8x the configured local_max_snapshot_bytes —
--     "roughly one active base plus several sealed-but-not-yet-retired
--     generations" — never unbounded.
--   * pg_flashback.local_retained_payload_soft_bytes is the optional soft
--     warning threshold. When unset it defaults to 75% of the hard ceiling.
--   * pg_flashback.local_min_filesystem_bytes (shared with local_capacity.sql)
--     is also enforced here as the filesystem reserve: a lifecycle freezes
--     if satisfying it would require consuming reserve space that admission
--     for other lifecycles/operations depends on.
--   * max_slot_wal_keep_size is a *cluster* fuse (bounds one shared logical
--     slot's on-disk WAL across every lifecycle sharing it). It is never a
--     per-lifecycle retained-payload budget and is not read here; see
--     flashback_doctor() for the operator warning when it is unbounded (-1).
-- =================================================================

CREATE OR REPLACE FUNCTION flashback_storage_budget_policy()
RETURNS TABLE (
    hard_bytes bigint,
    soft_bytes bigint,
    fs_reserve_bytes bigint,
    hard_source text,
    soft_source text
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_hard bigint;
    v_soft bigint;
    v_snapshot_budget bigint;
    v_hard_explicit bigint;
    v_soft_explicit bigint;
BEGIN
    v_hard_explicit := flashback_local_guc_bytes('pg_flashback.local_max_retained_payload_bytes');
    v_soft_explicit := flashback_local_guc_bytes('pg_flashback.local_retained_payload_soft_bytes');
    v_snapshot_budget := flashback_local_guc_bytes('pg_flashback.local_max_snapshot_bytes');

    v_hard := v_hard_explicit;
    IF v_hard IS NULL AND v_snapshot_budget IS NOT NULL THEN
        -- See STORAGE_POLICY above: documented default, not a physical law.
        v_hard := v_snapshot_budget * 8;
    END IF;

    v_soft := v_soft_explicit;
    IF v_soft IS NULL AND v_hard IS NOT NULL THEN
        v_soft := (v_hard * 3) / 4;
    END IF;

    hard_bytes := v_hard;
    soft_bytes := v_soft;
    fs_reserve_bytes := flashback_local_guc_bytes('pg_flashback.local_min_filesystem_bytes');
    hard_source := CASE WHEN v_hard_explicit IS NOT NULL THEN 'explicit' ELSE 'derived_8x_snapshot_budget' END;
    soft_source := CASE WHEN v_soft_explicit IS NOT NULL THEN 'explicit' ELSE 'derived_75pct_of_hard' END;
    RETURN NEXT;
END;
$$;

COMMENT ON FUNCTION flashback_storage_budget_policy() IS
    'Local_delta retained-payload storage budget (soft/hard/fs-reserve). See STORAGE_POLICY comment at the top of maintain_uninstall.sql.';

-- Operator/CLI-facing storage projection for one lifecycle. Read-only.
CREATE OR REPLACE FUNCTION flashback_lifecycle_storage_metrics(p_table text)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_name text := flashback_resolve_lifecycle_name(p_table);
    v_tracking_id bigint;
    v_rel_oid oid;
    v_cap record;
    -- record variables raise "record ... is not assigned yet" on ANY field
    -- access (even inside `v_cap IS NULL OR ...`) until they have been the
    -- target of a successful SELECT INTO at least once; a plain `v_cap IS
    -- NULL` check is not sufficient to guard later `v_cap.field` access when
    -- v_rel_oid's relation no longer exists (e.g. mid-DROP), so track
    -- assignment explicitly instead.
    v_cap_ok boolean := false;
    v_policy record;
    v_status text;
    v_frozen boolean := false;
    v_frozen_reason text;
    v_fs_status text;
BEGIN
    SELECT tt.tracking_id, tt.rel_oid
      INTO v_tracking_id, v_rel_oid
    FROM flashback.tracked_tables tt
    WHERE tt.is_active
      AND tt.recovery_profile = 'local_delta'
      AND format('%I.%I', tt.schema_name, tt.table_name) = v_name;

    IF v_tracking_id IS NULL THEN
        RETURN jsonb_build_object(
            'schema_version', 1,
            'table_name', v_name,
            'status', 'not_protected'
        );
    END IF;

    SELECT * INTO v_policy FROM flashback_storage_budget_policy();

    IF v_rel_oid IS NOT NULL AND EXISTS (SELECT 1 FROM pg_class WHERE oid = v_rel_oid) THEN
        BEGIN
            SELECT * INTO v_cap FROM flashback_measure_local_capacity(v_rel_oid);
            v_cap_ok := true;
        EXCEPTION WHEN OTHERS THEN
            v_cap_ok := false;
        END;
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM flashback.coverage_gaps g
        JOIN flashback.coverage_generations cg
          ON cg.generation_id = g.source_generation_id AND cg.tracking_id = g.tracking_id
        WHERE g.tracking_id = v_tracking_id
          AND g.reanchored_by_generation_id IS NULL
          AND COALESCE((g.details->>'permanent_gap')::boolean, false)
    ), (
        SELECT g.reason
        FROM flashback.coverage_gaps g
        WHERE g.tracking_id = v_tracking_id
          AND g.reanchored_by_generation_id IS NULL
          AND COALESCE((g.details->>'permanent_gap')::boolean, false)
        ORDER BY g.detected_at DESC
        LIMIT 1
    ) INTO v_frozen, v_frozen_reason;

    IF v_frozen THEN
        v_status := 'blocked';
    ELSIF NOT v_cap_ok THEN
        v_status := 'unknown';
    ELSIF v_policy.fs_reserve_bytes IS NOT NULL
       AND v_cap.filesystem_available_bytes IS NOT NULL
       AND v_cap.filesystem_available_bytes < v_policy.fs_reserve_bytes
    THEN
        v_status := 'fs_reserve_breached';
    ELSIF v_policy.hard_bytes IS NOT NULL
       AND v_cap.retained_local_payload_bytes > v_policy.hard_bytes
    THEN
        v_status := 'hard';
    ELSIF v_policy.soft_bytes IS NOT NULL
       AND v_cap.retained_local_payload_bytes > v_policy.soft_bytes
    THEN
        v_status := 'soft';
    ELSE
        v_status := 'ok';
    END IF;

    v_fs_status := CASE
        WHEN NOT v_cap_ok OR v_policy.fs_reserve_bytes IS NULL THEN 'unknown'
        WHEN v_cap.filesystem_available_bytes < v_policy.fs_reserve_bytes THEN 'breached'
        ELSE 'ok'
    END;

    RETURN jsonb_build_object(
        'schema_version', 1,
        'table_name', v_name,
        'tracking_id', v_tracking_id,
        'status', v_status,
        'frozen', v_frozen,
        'frozen_reason', v_frozen_reason,
        'retained_local_payload_bytes', CASE WHEN v_cap_ok THEN v_cap.retained_local_payload_bytes END,
        'filesystem_available_bytes', CASE WHEN v_cap_ok THEN v_cap.filesystem_available_bytes END,
        'filesystem_reserve_status', v_fs_status,
        'policy', jsonb_build_object(
            'hard_bytes', v_policy.hard_bytes,
            'soft_bytes', v_policy.soft_bytes,
            'fs_reserve_bytes', v_policy.fs_reserve_bytes,
            'hard_source', v_policy.hard_source,
            'soft_source', v_policy.soft_source
        ),
        'note', 'status ok|soft|hard|fs_reserve_breached|blocked|unknown; blocked means a permanent storage-exhaustion gap already froze this lifecycle'
    );
END;
$$;

COMMENT ON FUNCTION flashback_lifecycle_storage_metrics(text) IS
    'Operator/CLI storage budget projection for one local_delta lifecycle: retained payload vs soft/hard budget, filesystem reserve, and frozen state.';

-- Mutating: durably freeze one lifecycle's active generation with a permanent
-- coverage gap when its storage budget is exhausted and re-anchoring would
-- only consume more of the same exhausted budget. Idempotent: calling this
-- again while already frozen is a no-op. Scoped to a single tracking_id —
-- other lifecycles, and the shared WAL stream itself, are never touched.
CREATE OR REPLACE FUNCTION flashback_storage_freeze_lifecycle(
    p_tracking_id bigint,
    p_reason text DEFAULT 'local_retained_payload_storage_exhausted'
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_gen record;
    v_already boolean;
BEGIN
    IF p_tracking_id IS NULL THEN
        RAISE EXCEPTION 'flashback_storage_freeze_lifecycle: tracking_id is required';
    END IF;

    PERFORM flashback_internal_lock_lifecycle(p_tracking_id);

    SELECT cg.* INTO v_gen
    FROM flashback.coverage_generations cg
    WHERE cg.tracking_id = p_tracking_id AND cg.state = 'active'
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'tracking_id', p_tracking_id, 'frozen', false,
            'reason', 'no_active_generation'
        );
    END IF;

    SELECT EXISTS (
        SELECT 1 FROM flashback.coverage_gaps g
        WHERE g.tracking_id = p_tracking_id
          AND g.source_generation_id = v_gen.generation_id
          AND g.reanchored_by_generation_id IS NULL
          AND COALESCE((g.details->>'permanent_gap')::boolean, false)
    ) INTO v_already;

    IF v_already THEN
        RETURN jsonb_build_object(
            'tracking_id', p_tracking_id,
            'generation_id', v_gen.generation_id,
            'frozen', true,
            'already_frozen', true
        );
    END IF;

    -- Same transaction as the caller's slot-consume batch (see
    -- consume_wal_changes() in src/storage/worker.rs): the gap record and any
    -- WAL this tick already applied for other lifecycles commit atomically
    -- together, or neither does.
    --
    -- coverage_generations.details is immutable once a generation leaves
    -- 'building' (see schema_bootstrap.sql trigger). state_reason has no
    -- such restriction and is the durable signal flashback_health() reads
    -- (COALESCE(generation_state_reason,'') = 'coverage_frozen_storage_exhausted');
    -- the freeze/permanent-gap payload itself lives on the coverage_gaps row
    -- inserted below, which is the source of truth for
    -- flashback_lifecycle_storage_metrics()'s frozen/blocked determination.
    UPDATE flashback.coverage_generations
       SET state_reason = 'coverage_frozen_storage_exhausted'
     WHERE generation_id = v_gen.generation_id
       AND tracking_id = p_tracking_id;

    INSERT INTO flashback.coverage_gaps (
        tracking_id, source_generation_id, reason,
        gap_start_time, gap_start_lsn, lower_bound_inclusive, details
    ) VALUES (
        p_tracking_id, v_gen.generation_id, p_reason,
        COALESCE(v_gen.valid_through_time, clock_timestamp()),
        v_gen.valid_through_lsn,
        false,
        jsonb_build_object('permanent_gap', true, 'trigger', 'storage_budget_exhausted')
    );

    RETURN jsonb_build_object(
        'tracking_id', p_tracking_id,
        'generation_id', v_gen.generation_id,
        'frozen', true,
        'already_frozen', false,
        'valid_through_lsn', v_gen.valid_through_lsn,
        'reason', p_reason
    );
END;
$$;

COMMENT ON FUNCTION flashback_storage_freeze_lifecycle(bigint, text) IS
    'Fail-closed: durably freeze one local_delta lifecycle (permanent coverage_gaps row + generation state_reason) when its storage budget is exhausted. Never touches other lifecycles or the shared capture stream.';

-- Scan every active local_delta lifecycle and freeze the ones whose retained
-- payload is over the hard budget (or whose filesystem reserve is breached)
-- and are not already frozen. Intended to run every WAL-consume tick from the
-- Rust worker so gap recording and slot consumption share one transaction;
-- see consume_wal_changes() in src/storage/worker.rs. Cheap no-op when
-- nothing is over budget. Never raises for one bad lifecycle: an unexpected
-- error freezing lifecycle N must not stop the scan from checking lifecycle
-- N+1 ("other lifecycles continue").
CREATE OR REPLACE FUNCTION flashback_storage_freeze_scan()
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    rec record;
    v_policy record;
    v_cap record;
    v_result jsonb;
    v_count integer := 0;
BEGIN
    SELECT * INTO v_policy FROM flashback_storage_budget_policy();
    IF v_policy.hard_bytes IS NULL AND v_policy.fs_reserve_bytes IS NULL THEN
        RETURN 0;
    END IF;

    FOR rec IN
        SELECT tt.tracking_id, tt.rel_oid
        FROM flashback.tracked_tables tt
        WHERE tt.is_active
          AND tt.recovery_profile = 'local_delta'
          AND tt.rel_oid IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM flashback.coverage_gaps g
              WHERE g.tracking_id = tt.tracking_id
                AND g.reanchored_by_generation_id IS NULL
                AND COALESCE((g.details->>'permanent_gap')::boolean, false)
          )
    LOOP
        BEGIN
            v_cap := NULL;
            IF EXISTS (SELECT 1 FROM pg_class WHERE oid = rec.rel_oid) THEN
                SELECT * INTO v_cap FROM flashback_measure_local_capacity(rec.rel_oid);
            END IF;
            IF v_cap IS NULL THEN
                CONTINUE;
            END IF;

            IF (
                v_policy.fs_reserve_bytes IS NOT NULL
                AND v_cap.filesystem_available_bytes < v_policy.fs_reserve_bytes
            ) OR (
                v_policy.hard_bytes IS NOT NULL
                AND v_cap.retained_local_payload_bytes > v_policy.hard_bytes
            ) THEN
                v_result := flashback_storage_freeze_lifecycle(rec.tracking_id);
                IF COALESCE((v_result->>'frozen')::boolean, false)
                   AND NOT COALESCE((v_result->>'already_frozen')::boolean, false)
                THEN
                    v_count := v_count + 1;
                END IF;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            -- Fail-open for the scan loop itself (never block other
            -- lifecycles); the underlying budget check remains fail-closed
            -- because an ungapped lifecycle is simply retried next tick.
            RAISE WARNING 'pg_flashback: flashback_storage_freeze_scan failed for tracking_id %: %',
                rec.tracking_id, SQLERRM;
        END;
    END LOOP;

    RETURN v_count;
END;
$$;

COMMENT ON FUNCTION flashback_storage_freeze_scan() IS
    'Per-tick storage-exhaustion sweep over active local_delta lifecycles. Called from the same transaction as WAL slot consumption; returns count of lifecycles newly frozen.';

CREATE OR REPLACE FUNCTION flashback_maintain_plan(p_table text)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_name text := flashback_resolve_lifecycle_name(p_table);
    v_health text;
    v_advise record;
    v_pending_restore boolean;
    v_building boolean;
    v_storage jsonb;
    v_storage_status text;
    v_maintenance_status text;
    v_recommended boolean;
    v_required boolean;
BEGIN
    IF NOT flashback_is_actively_protected(v_name) THEN
        RETURN jsonb_build_object(
            'schema_version', 1,
            'table_name', v_name,
            'status', 'error',
            'code', 'not_protected',
            'maintenance_status', 'blocked',
            'recommended', false,
            'required', false
        );
    END IF;

    v_health := flashback_lifecycle_health(v_name);
    -- Scoped to this lifecycle's own tracking_id: an unrelated table's
    -- in-flight recover/restore_lsn operation elsewhere in the same database
    -- must never block (or even be visible to) this table's maintain plan.
    SELECT EXISTS (
        SELECT 1
        FROM flashback.operation_current_state s
        JOIN flashback.tracked_tables tt ON tt.tracking_id = s.tracking_id
        WHERE tt.is_active
          AND format('%I.%I', tt.schema_name, tt.table_name) = v_name
          AND s.command IN ('recover', 'restore_lsn')
          AND s.state IN ('started', 'applied_coverage_pending')
    ) INTO v_pending_restore;

    SELECT EXISTS (
        SELECT 1
        FROM flashback.tracked_tables tt
        JOIN flashback.coverage_generations cg ON cg.tracking_id = tt.tracking_id
        WHERE tt.is_active
          AND format('%I.%I', tt.schema_name, tt.table_name) = v_name
          AND cg.state = 'building'
    ) INTO v_building;

    BEGIN
        SELECT * INTO v_advise FROM flashback_advise(v_name::regclass);
    EXCEPTION WHEN OTHERS THEN
        v_advise := NULL;
    END;

    v_storage := flashback_lifecycle_storage_metrics(v_name);
    v_storage_status := COALESCE(v_storage->>'status', 'unknown');

    -- blocked: cannot run reanchor at all right now (protect state, pending
    --   restore, an in-flight boundary, or storage already frozen).
    -- required: a durable coverage fault, or over the hard storage budget /
    --   filesystem reserve breached while still safe to reanchor.
    -- recommended: soft storage warning, or coverage health just wants a
    --   fresh boundary (existing "recommended" signal).
    -- not_needed: none of the above.
    v_maintenance_status := CASE
        WHEN v_pending_restore OR v_building OR v_storage_status = 'blocked' THEN 'blocked'
        WHEN v_health IN ('timeline_mismatch', 'repository_anchor_missing', 'slot_lost', 'reanchor_recommended')
        THEN 'required'
        WHEN v_storage_status IN ('hard', 'fs_reserve_breached') THEN 'required'
        WHEN v_storage_status = 'soft' THEN 'recommended'
        WHEN v_health IS DISTINCT FROM 'healthy' THEN 'recommended'
        ELSE 'not_needed'
    END;
    v_recommended := v_maintenance_status IN ('recommended', 'required');
    v_required := v_maintenance_status = 'required';

    RETURN jsonb_build_object(
        'schema_version', 1,
        'table_name', v_name,
        'status', CASE
            WHEN v_pending_restore THEN 'error'
            WHEN v_building THEN 'error'
            ELSE 'ok'
        END,
        'code', CASE
            WHEN v_pending_restore THEN 'pending_restore'
            WHEN v_building THEN 'generation_building'
            ELSE 'ok'
        END,
        'coverage_health', v_health,
        'maintenance_status', v_maintenance_status,
        'recommended', v_recommended,
        'required', v_required,
        'pending_restore', v_pending_restore,
        'generation_building', v_building,
        'storage', v_storage,
        'capacity', CASE WHEN v_advise IS NULL THEN NULL ELSE to_jsonb(v_advise) END,
        'action', CASE
            WHEN v_storage_status = 'blocked' THEN 'lifecycle is frozen by a permanent storage gap; raise budgets/free disk, then track a fresh lifecycle'
            WHEN v_storage_status IN ('hard', 'fs_reserve_breached') THEN 'flashback_maintain_begin urgently: retained payload/filesystem reserve is over budget'
            ELSE 'flashback_maintain_begin creates a new WAL-aligned base/generation; predecessor stays retained until successor is healthy'
        END,
        'note', 'dry-run only; execute via flashback_maintain_begin + flashback_maintain_finalize (or CLI maintain --yes)'
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_maintain_begin(p_table text)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_plan jsonb;
    v_name text;
    v_tracking_id bigint;
    v_predecessor_generation_id bigint;
    v_new_generation_id bigint;
    v_operation_id bigint;
BEGIN
    v_plan := flashback_maintain_plan(p_table);
    IF COALESCE(v_plan->>'status', '') <> 'ok' THEN
        RAISE EXCEPTION 'pg_flashback: maintain refused (%)', COALESCE(v_plan->>'code', 'unknown')
            USING ERRCODE = 'invalid_parameter_value',
                  DETAIL = v_plan::text;
    END IF;
    IF COALESCE(v_plan->>'maintenance_status', '') = 'blocked' THEN
        RAISE EXCEPTION 'pg_flashback: maintain blocked for % (%)',
            v_plan->>'table_name',
            COALESCE(v_plan->'storage'->>'frozen_reason', 'storage/filesystem budget exceeded')
            USING ERRCODE = 'disk_full',
                  DETAIL = v_plan::text;
    END IF;

    v_name := v_plan->>'table_name';

    SELECT tt.tracking_id INTO v_tracking_id
    FROM flashback.tracked_tables tt
    WHERE tt.is_active
      AND tt.recovery_profile = 'local_delta'
      AND format('%I.%I', tt.schema_name, tt.table_name) = v_name;
    IF v_tracking_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: % is not an active local_delta lifecycle', v_name;
    END IF;

    SELECT cg.generation_id INTO v_predecessor_generation_id
    FROM flashback.coverage_generations cg
    WHERE cg.tracking_id = v_tracking_id AND cg.state = 'active';

    -- flashback_reanchor() must be the first write in this transaction
    -- (enforced inside via txid_current_if_assigned()). Everything else in
    -- this function — including the operations-journal insert below — must
    -- come after it.
    v_new_generation_id := flashback_reanchor(v_name);

    v_operation_id := flashback_operation_begin(
        p_command => 'maintain',
        p_table => v_name,
        p_tracking_id => v_tracking_id,
        p_generation_id => v_new_generation_id,
        p_details => jsonb_build_object(
            'predecessor_generation_id', v_predecessor_generation_id,
            'successor_generation_id', v_new_generation_id
        )
    );

    RETURN jsonb_build_object(
        'schema_version', 1,
        'status', 'started',
        'code', 'ok',
        'operation_id', v_operation_id,
        'table_name', v_name,
        'tracking_id', v_tracking_id,
        'predecessor_generation_id', v_predecessor_generation_id,
        'new_generation_id', v_new_generation_id,
        'note', 'reanchor and this operation row committed together; call flashback_maintain_finalize(operation_id) once successor coverage is healthy — predecessor stays retained (not deleted) until then'
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_maintain_finalize(p_operation_id bigint)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_op record;
    v_details jsonb;
    v_tracking_id bigint;
    v_generation_id bigint;
    v_predecessor_generation_id bigint;
    v_cg record;
    v_predecessor record;
    v_health record;
BEGIN
    SELECT s.* INTO v_op
    FROM flashback.operation_current_state s
    WHERE s.operation_id = p_operation_id
      AND s.command = 'maintain';
    IF v_op.operation_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: unknown maintain operation_id %', p_operation_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF v_op.state IN ('sealed', 'failed', 'abandoned') THEN
        RETURN jsonb_build_object(
            'operation_id', p_operation_id, 'status', v_op.state,
            'note', 'already finalized'
        );
    END IF;

    SELECT o.details INTO v_details
    FROM flashback.operations o
    WHERE o.operation_id = p_operation_id;
    v_tracking_id := v_op.tracking_id;
    v_generation_id := v_op.generation_id;
    v_predecessor_generation_id := NULLIF(v_details->>'predecessor_generation_id', '')::bigint;

    SELECT cg.* INTO v_cg
    FROM flashback.coverage_generations cg
    WHERE cg.generation_id = v_generation_id
      AND cg.tracking_id = v_tracking_id;

    IF v_cg.generation_id IS NULL OR v_cg.state IS DISTINCT FROM 'active' THEN
        RETURN jsonb_build_object(
            'operation_id', p_operation_id,
            'status', 'pending',
            'reason', 'successor generation boundary not resolved yet',
            'generation_state', v_cg.state
        );
    END IF;

    SELECT h.* INTO v_health
    FROM flashback_health() h
    WHERE h.tracking_id = v_tracking_id
      AND h.generation_id = v_generation_id;
    IF v_health.tracking_id IS NULL OR v_health.health IS DISTINCT FROM 'healthy' THEN
        RETURN jsonb_build_object(
            'operation_id', p_operation_id,
            'status', 'pending',
            'reason', COALESCE(v_health.health, 'no health row yet'),
            'note', 'successor coverage is not healthy yet; predecessor generation remains retained; retry finalize later'
        );
    END IF;

    IF v_predecessor_generation_id IS NOT NULL THEN
        SELECT cg.* INTO v_predecessor
        FROM flashback.coverage_generations cg
        WHERE cg.generation_id = v_predecessor_generation_id
          AND cg.tracking_id = v_tracking_id;

        -- Normal path: the WAL worker's boundary-resolution already sealed
        -- the predecessor as part of activating the successor generation.
        -- This transition is only a defensive backstop for that race.
        IF v_predecessor.state = 'active' THEN
            PERFORM flashback_internal_transition_coverage_generation(
                v_predecessor_generation_id,
                v_tracking_id,
                'active',
                'sealed',
                COALESCE(v_predecessor.state_reason, 'maintain_finalize_successor_healthy'),
                NULL,
                NULL,
                NULL,
                NULL,
                COALESCE(v_predecessor.superseded_before_lsn, v_cg.boundary_lsn),
                COALESCE(v_predecessor.superseded_before_time, v_cg.boundary_time),
                '{}'::jsonb
            );
        END IF;
    END IF;

    PERFORM flashback_operation_append_event(
        p_operation_id, 'sealed', NULL, NULL,
        'successor coverage healthy; predecessor sealed (retained, not deleted)',
        jsonb_build_object(
            'tracking_id', v_tracking_id,
            'generation_id', v_generation_id,
            'predecessor_generation_id', v_predecessor_generation_id
        )
    );

    RETURN jsonb_build_object(
        'schema_version', 1,
        'operation_id', p_operation_id,
        'status', 'sealed',
        'tracking_id', v_tracking_id,
        'new_generation_id', v_generation_id,
        'predecessor_generation_id', v_predecessor_generation_id,
        'note', 'predecessor generation is sealed and retained; it becomes eligible for normal retention/retirement later per the configured retention_interval — this call never deletes payload or waits on that interval'
    );
END;
$$;

-- Back-compat convenience: begin only. Callers (CLI: begin -> poll finalize)
-- must call flashback_maintain_finalize(operation_id) once the successor is
-- healthy; this function intentionally does not wait or poll.
CREATE OR REPLACE FUNCTION flashback_maintain_execute(p_table text)
RETURNS jsonb
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
    SELECT flashback_maintain_begin(p_table);
$$;

CREATE OR REPLACE FUNCTION flashback_prepare_uninstall(p_execute boolean DEFAULT false)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_active bigint;
    v_pending bigint;
    v_slots text[];
    v_dbs text[];
BEGIN
    SELECT count(*) INTO v_active
    FROM flashback.tracked_tables tt
    WHERE tt.is_active;

    SELECT count(*) INTO v_pending
    FROM flashback.operation_current_state s
    WHERE s.state IN ('started', 'applied_coverage_pending');

    SELECT coalesce(array_agg(slot_name ORDER BY slot_name), ARRAY[]::text[])
      INTO v_slots
    FROM pg_replication_slots
    WHERE slot_name LIKE 'pg_flashback%';

    SELECT coalesce(array_agg(x ORDER BY x), ARRAY[]::text[])
      INTO v_dbs
    FROM flashback_canonical_target_databases() AS x;

    IF v_active > 0 OR v_pending > 0 THEN
        RETURN jsonb_build_object(
            'schema_version', 1,
            'status', 'error',
            'code', 'active_lifecycle_or_pending_restore',
            'active_lifecycles', v_active,
            'pending_operations', v_pending,
            'slots', to_jsonb(v_slots),
            'configured_databases', to_jsonb(v_dbs),
            'action', 'unprotect/cleanup all lifecycles and wait for pending recover/unprotect operations before uninstall'
        );
    END IF;

    IF NOT p_execute THEN
        RETURN jsonb_build_object(
            'schema_version', 1,
            'status', 'ok',
            'code', 'ready_plan',
            'active_lifecycles', v_active,
            'pending_operations', v_pending,
            'slots', to_jsonb(v_slots),
            'configured_databases', to_jsonb(v_dbs),
            'note', 're-run with p_execute=true / CLI --yes to drop idle flashback slots',
            'next_steps', jsonb_build_array(
                'pg_flashback prepare-uninstall --yes',
                'DROP EXTENSION pg_flashback;',
                'repeat in every configured target database',
                'remove pg_flashback from shared_preload_libraries and restart',
                'drop leftover roles if unused'
            )
        );
    END IF;

    -- Controlled slot drop only when no active lifecycle remains.
    PERFORM pg_drop_replication_slot(s)
    FROM unnest(v_slots) AS s
    WHERE EXISTS (SELECT 1 FROM pg_replication_slots r WHERE r.slot_name = s);

    SELECT coalesce(array_agg(slot_name ORDER BY slot_name), ARRAY[]::text[])
      INTO v_slots
    FROM pg_replication_slots
    WHERE slot_name LIKE 'pg_flashback%';

    RETURN jsonb_build_object(
        'schema_version', 1,
        'status', 'ok',
        'code', 'ready_for_drop_extension',
        'active_lifecycles', v_active,
        'pending_operations', v_pending,
        'remaining_slots', to_jsonb(v_slots),
        'configured_databases', to_jsonb(v_dbs),
        'cluster_residue', jsonb_build_object(
            'roles', jsonb_build_array('flashback_admin'),
            'gucs', 'postgresql.conf pg_flashback.* and shared_preload_libraries still need manual cleanup after DROP EXTENSION',
            'note', 'DROP EXTENSION removes SQL objects in this database; repeat per configured database'
        ),
        'next_steps', jsonb_build_array(
            'DROP EXTENSION pg_flashback;',
            'repeat in every configured target database',
            'remove pg_flashback from shared_preload_libraries and restart',
            'drop leftover roles if unused'
        )
    );
END;
$$;

COMMENT ON FUNCTION flashback_maintain_plan(text) IS
    'Read-only maintain/reanchor plan with capacity, storage-budget and pending-restore guards. maintenance_status is one of not_needed|recommended|required|blocked.';
COMMENT ON FUNCTION flashback_maintain_begin(text) IS
    'Reanchor + durable operations-journal row in one transaction (reanchor is the required first write). Returns operation_id for flashback_maintain_finalize.';
COMMENT ON FUNCTION flashback_maintain_finalize(bigint) IS
    'Second phase: once successor coverage is healthy, seal the predecessor generation (retained, not deleted) and mark the maintain operation sealed. Safe to call repeatedly before that.';
COMMENT ON FUNCTION flashback_maintain_execute(text) IS
    'Back-compat alias for flashback_maintain_begin(); callers must still call flashback_maintain_finalize(operation_id).';
COMMENT ON FUNCTION flashback_prepare_uninstall(boolean) IS
    'Fail-closed uninstall preparation: refuse active lifecycles/pending restores; drop orphan slots when idle.';
