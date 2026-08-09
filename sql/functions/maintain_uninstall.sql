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
    v_backend text;
    v_capacity jsonb;
BEGIN
    v_backend := lower(COALESCE(
        NULLIF(current_setting('pg_flashback.snapshot_storage_backend', true), ''),
        'heap_v1'
    ));
    IF v_backend NOT IN ('heap_v1', 'external_zstd') THEN
        RAISE EXCEPTION 'pg_flashback: invalid snapshot_storage_backend %', v_backend
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
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
          AND cg.state IN ('building', 'capturing')
    ) INTO v_building;

    IF v_backend = 'external_zstd' THEN
        BEGIN
            SELECT to_jsonb(m) INTO v_capacity
            FROM flashback_measure_external_snapshot_capacity(v_name::regclass) m;
        EXCEPTION WHEN OTHERS THEN
            v_capacity := jsonb_build_object(
                'admissible', false,
                'error', SQLERRM
            );
        END;
    ELSE
        BEGIN
            SELECT * INTO v_advise FROM flashback_advise(v_name::regclass);
            v_capacity := CASE WHEN v_advise IS NULL THEN NULL ELSE to_jsonb(v_advise) END;
        EXCEPTION WHEN OTHERS THEN
            v_advise := NULL;
            v_capacity := jsonb_build_object(
                'admissible', false,
                'error', SQLERRM
            );
        END;
    END IF;

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
        WHEN v_backend = 'external_zstd'
             AND NOT COALESCE((v_capacity->>'admissible')::boolean, false)
        THEN 'blocked'
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
            WHEN v_backend = 'external_zstd'
                 AND NOT COALESCE((v_capacity->>'admissible')::boolean, false)
            THEN 'external_capacity_insufficient'
            ELSE 'ok'
        END,
        'coverage_health', v_health,
        'maintenance_status', v_maintenance_status,
        'recommended', v_recommended,
        'required', v_required,
        'snapshot_storage_backend', v_backend,
        'pending_restore', v_pending_restore,
        'generation_building', v_building,
        'storage', v_storage,
        'capacity', v_capacity,
        'action', CASE
            WHEN v_backend = 'external_zstd'
                 AND NOT COALESCE((v_capacity->>'admissible')::boolean, false)
            THEN 'configure a safe external_snapshot_root and free-space budgets before maintain'
            WHEN v_storage_status = 'blocked' THEN 'lifecycle is frozen by a permanent storage gap; raise budgets/free disk, then track a fresh lifecycle'
            WHEN v_storage_status IN ('hard', 'fs_reserve_breached') THEN 'flashback_maintain_begin urgently: retained payload/filesystem reserve is over budget'
            ELSE 'flashback_maintain_begin creates a new WAL-aligned base/generation; predecessor stays retained until successor is healthy'
        END,
        'note', 'dry-run only; execute via flashback_maintain_begin + flashback_maintain_finalize (or CLI maintain --yes)'
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_internal_reserve_external_maintenance(p_table text)
RETURNS TABLE (
    tracking_id bigint,
    rel_oid oid,
    parent_generation_id bigint,
    generation_id bigint,
    snapshot_id bigint,
    operation_nonce bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_tracking_id bigint;
    v_rel_oid oid;
    v_schema text;
    v_table text;
    v_stream_id bigint;
    v_parent bigint;
    v_generation_no bigint;
    v_generation bigint;
    v_snapshot bigint;
    v_nonce bigint;
BEGIN
    PERFORM public.flashback_require_primary('flashback_maintain_begin');
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'pg_flashback: external maintenance requires READ COMMITTED isolation';
    END IF;

    SELECT r.tracking_id, r.rel_oid, r.schema_name, r.table_name
      INTO v_tracking_id, v_rel_oid, v_schema, v_table
    FROM public.flashback_internal_resolve_tracked_table(p_table) r;
    IF v_tracking_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: % is not an active local_delta lifecycle', p_table;
    END IF;

    v_stream_id := public.flashback_ensure_active_wal_stream();
    IF v_stream_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: no usable logical slot exists for external maintenance';
    END IF;
    PERFORM public.flashback_internal_lock_lifecycle(v_tracking_id);

    SELECT tt.rel_oid, tt.schema_name, tt.table_name
      INTO v_rel_oid, v_schema, v_table
    FROM flashback.tracked_tables tt
    WHERE tt.tracking_id = v_tracking_id
      AND tt.is_active
      AND tt.recovery_profile = 'local_delta'
    FOR UPDATE;
    IF NOT FOUND
       OR to_regclass(format('%I.%I', v_schema, v_table))::oid IS DISTINCT FROM v_rel_oid
    THEN
        RAISE EXCEPTION 'pg_flashback: tracked table identity changed before external reservation';
    END IF;
    IF EXISTS (
        SELECT 1 FROM flashback.coverage_generations
        WHERE flashback.coverage_generations.tracking_id = v_tracking_id
          AND state IN ('building', 'capturing')
    ) THEN
        RAISE EXCEPTION 'pg_flashback: lifecycle % already has a pending generation',
            v_tracking_id USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    SELECT cg.generation_id INTO v_parent
    FROM flashback.coverage_generations cg
    WHERE cg.tracking_id = v_tracking_id AND cg.state = 'active'
    FOR UPDATE;
    IF v_parent IS NULL THEN
        SELECT cg.generation_id INTO v_parent
        FROM flashback.coverage_generations cg
        WHERE cg.tracking_id = v_tracking_id AND cg.state = 'aborted'
        ORDER BY cg.generation_no DESC
        LIMIT 1
        FOR UPDATE;
    END IF;
    IF v_parent IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: lifecycle % has no active or aborted predecessor',
            v_tracking_id;
    END IF;

    PERFORM public.flashback_admit_external_snapshot_capacity(v_rel_oid::regclass);
    SELECT COALESCE(max(cg.generation_no), 0) + 1
      INTO v_generation_no
    FROM flashback.coverage_generations cg
    WHERE cg.tracking_id = v_tracking_id;
    v_nonce := txid_current();

    SELECT r.generation_id, r.snapshot_id
      INTO v_generation, v_snapshot
    FROM public.flashback_internal_reserve_online_generation(
        v_tracking_id, v_rel_oid, v_stream_id, v_generation_no, v_parent,
        'external_zstd', v_nonce, 'local_delta',
        jsonb_build_object('source', 'maintain')
    ) r;

    tracking_id := v_tracking_id;
    rel_oid := v_rel_oid;
    parent_generation_id := v_parent;
    generation_id := v_generation;
    snapshot_id := v_snapshot;
    operation_nonce := v_nonce;
    RETURN NEXT;
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
    v_backend text;
    v_snapshot_id bigint;
    v_rel_oid oid;
    v_operation_nonce bigint;
BEGIN
    IF txid_current_if_assigned() IS NOT NULL THEN
        RAISE EXCEPTION 'pg_flashback: flashback_maintain_begin() must be the first write in a dedicated transaction'
            USING HINT = 'COMMIT or ROLLBACK, then retry pg_flashback maintain.';
    END IF;
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
    v_backend := COALESCE(v_plan->>'snapshot_storage_backend', 'heap_v1');

    IF v_backend = 'external_zstd' THEN
        SELECT r.tracking_id, r.rel_oid, r.parent_generation_id,
               r.generation_id, r.snapshot_id, r.operation_nonce
          INTO v_tracking_id, v_rel_oid, v_predecessor_generation_id,
               v_new_generation_id, v_snapshot_id, v_operation_nonce
        FROM public.flashback_internal_reserve_external_maintenance(v_name) r;

        v_operation_id := flashback_operation_begin(
            p_command => 'maintain',
            p_table => v_name,
            p_tracking_id => v_tracking_id,
            p_generation_id => v_new_generation_id,
            p_details => jsonb_build_object(
                'predecessor_generation_id', v_predecessor_generation_id,
                'successor_generation_id', v_new_generation_id,
                'snapshot_id', v_snapshot_id,
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
            'predecessor_generation_id', v_predecessor_generation_id,
            'new_generation_id', v_new_generation_id,
            'snapshot_id', v_snapshot_id,
            'rel_oid', v_rel_oid,
            'operation_nonce', v_operation_nonce,
            'storage_backend', 'external_zstd',
            'note', 'reservation committed; run the external copy/marker phase in a new transaction'
        );
    END IF;

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

CREATE OR REPLACE FUNCTION flashback_maintain_external_copy(p_operation_id bigint)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_op record;
    v_details jsonb;
    v_result jsonb;
    v_snapshot_id bigint;
    v_rel_oid bigint;
BEGIN
    SELECT s.*, o.details
      INTO v_op
    FROM flashback.operation_current_state s
    JOIN flashback.operations o ON o.operation_id = s.operation_id
    WHERE s.operation_id = p_operation_id
      AND s.command = 'maintain'
      AND s.state = 'started';
    IF v_op.operation_id IS NULL
       OR v_op.details->>'storage_backend' IS DISTINCT FROM 'external_zstd'
    THEN
        RAISE EXCEPTION 'pg_flashback: operation % is not a pending external maintenance copy',
            p_operation_id USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    v_details := v_op.details;
    v_snapshot_id := (v_details->>'snapshot_id')::bigint;
    v_rel_oid := (v_details->>'rel_oid')::bigint;
    SELECT public.flashback_internal_run_external_marker_transaction(
        v_op.tracking_id, v_rel_oid, v_op.generation_id, v_snapshot_id
    ) INTO v_result;
    RETURN jsonb_build_object(
        'schema_version', 1,
        'status', 'copy_in_progress',
        'operation_id', p_operation_id,
        'tracking_id', v_op.tracking_id,
        'generation_id', v_op.generation_id,
        'snapshot_id', v_snapshot_id,
        'copy', v_result,
        'note', 'the WAL boundary marker committed; the background copier may still be writing its staged artifact'
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_maintain_external_publish(p_operation_id bigint)
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
BEGIN
    SELECT s.*, o.details
      INTO v_op
    FROM flashback.operation_current_state s
    JOIN flashback.operations o ON o.operation_id = s.operation_id
    WHERE s.operation_id = p_operation_id
      AND s.command = 'maintain'
      AND s.state = 'started';
    IF v_op.operation_id IS NULL
       OR v_op.details->>'storage_backend' IS DISTINCT FROM 'external_zstd'
    THEN
        RAISE EXCEPTION 'pg_flashback: operation % is not a pending external maintenance publish',
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
        RETURN jsonb_build_object(
            'schema_version', 1,
            'status', 'pending',
            'operation_id', p_operation_id,
            'reason', 'external boundary COMMIT LSN is not resolved yet'
        );
    END IF;
    SELECT public.flashback_internal_finalize_external_snapshot(
        v_op.tracking_id, v_op.generation_id, v_snapshot_id
    ) INTO v_publish;
    v_finalize := public.flashback_maintain_finalize(p_operation_id);
    RETURN jsonb_build_object(
        'schema_version', 1,
        'status', COALESCE(v_finalize->>'status', 'pending'),
        'operation_id', p_operation_id,
        'publish', v_publish,
        'finalize', v_finalize
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

-- Reconcile external maintenance across the transaction/filesystem boundary.
-- Cleanup intentionally precedes reservation processing: a reservation
-- aborted below may be physically removed only by a later committed call.
CREATE OR REPLACE FUNCTION flashback_internal_reconcile_external_maintenance(
    p_stale_after interval DEFAULT interval '5 minutes',
    p_limit integer DEFAULT 1
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_op record;
    v_current record;
    v_cleanup record;
    v_artifact jsonb;
    v_finalize jsonb;
    v_purged boolean;
    v_cleaned integer := 0;
    v_resumed integer := 0;
    v_aborted integer := 0;
    v_deferred integer := 0;
    v_errors integer := 0;
    v_reason text;
    v_last_error text;
BEGIN
    IF p_stale_after IS NULL OR p_stale_after < interval '0 seconds' THEN
        RAISE EXCEPTION 'pg_flashback: external reconcile stale interval must be non-negative'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_limit IS NULL OR p_limit < 1 OR p_limit > 100 THEN
        RAISE EXCEPTION 'pg_flashback: external reconcile limit must be between 1 and 100'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- Canonical order begins with the database-stream lock. Do not wait:
    -- capture correctness always wins over maintenance progress.
    IF NOT pg_try_advisory_xact_lock(
        public.flashback_internal_lock_ns_stream(),
        (SELECT oid::integer FROM pg_database WHERE datname = current_database())
    ) THEN
        RETURN jsonb_build_object(
            'status', 'busy', 'cleaned', 0, 'resumed', 0,
            'aborted', 0, 'deferred', 1, 'errors', 0
        );
    END IF;

    -- Phase 1: physical cleanup only for an abort that was already durable
    -- before this transaction. A receipt makes absent-after-crash retries
    -- finite and prevents an old tombstone starving later work.
    FOR v_cleanup IN
        SELECT s.operation_id, s.tracking_id, s.generation_id,
               (o.details->>'snapshot_id')::bigint AS snapshot_id,
               (o.details->>'operation_nonce')::bigint AS operation_nonce
        FROM flashback.operation_current_state s
        JOIN flashback.operations o ON o.operation_id = s.operation_id
        JOIN flashback.coverage_generations cg
          ON cg.generation_id = s.generation_id
         AND cg.tracking_id = s.tracking_id
        JOIN flashback.snapshots sn
          ON sn.snapshot_id = (o.details->>'snapshot_id')::bigint
         AND sn.tracking_id = s.tracking_id
        LEFT JOIN flashback.external_artifact_cleanup_receipts r
          ON r.snapshot_id = sn.snapshot_id
        WHERE s.command = 'maintain'
          AND s.state IN ('failed', 'abandoned')
          AND o.details->>'storage_backend' = 'external_zstd'
          AND cg.state = 'aborted'
          AND sn.payload_state = 'aborted'
          AND r.snapshot_id IS NULL
        ORDER BY s.created_at, s.operation_id
        LIMIT p_limit
    LOOP
        BEGIN
            v_purged := public.flashback_internal_purge_aborted_external_artifact(
                v_cleanup.tracking_id, v_cleanup.generation_id,
                v_cleanup.snapshot_id
            );
            INSERT INTO flashback.external_artifact_cleanup_receipts (
                snapshot_id, tracking_id, generation_id, operation_nonce,
                artifact_was_present
            ) VALUES (
                v_cleanup.snapshot_id, v_cleanup.tracking_id,
                v_cleanup.generation_id, v_cleanup.operation_nonce, v_purged
            ) ON CONFLICT (snapshot_id) DO NOTHING;
            v_cleaned := v_cleaned + 1;
        EXCEPTION WHEN OTHERS THEN
            -- A live copier lease, lock timeout, unsafe root, or transient IO
            -- error is retryable. Never broaden deletion or falsify a receipt.
            v_deferred := v_deferred + 1;
            v_errors := v_errors + 1;
            v_last_error := SQLSTATE || ': ' || SQLERRM;
        END;
    END LOOP;

    -- Phase 2: reconcile started operations. The operation header is
    -- immutable, so all exact identities come from its durable details.
    FOR v_op IN
        SELECT s.operation_id, s.tracking_id, s.generation_id,
               s.created_at, o.details
        FROM flashback.operation_current_state s
        JOIN flashback.operations o ON o.operation_id = s.operation_id
        WHERE s.command = 'maintain'
          AND s.state = 'started'
          AND o.details->>'storage_backend' = 'external_zstd'
        ORDER BY s.created_at, s.operation_id
        LIMIT p_limit
    LOOP
        v_artifact := NULL;
        v_reason := NULL;
        IF NOT public.flashback_internal_try_lock_lifecycle(v_op.tracking_id) THEN
            v_deferred := v_deferred + 1;
            CONTINUE;
        END IF;

        SELECT s.state AS operation_state, cg.state AS generation_state,
               sn.payload_state, sn.snapshot_lsn, cs.state AS stream_state,
               (o.details->>'snapshot_id')::bigint AS snapshot_id
          INTO v_current
        FROM flashback.operation_current_state s
        JOIN flashback.operations o ON o.operation_id = s.operation_id
        JOIN flashback.coverage_generations cg
          ON cg.generation_id = s.generation_id
         AND cg.tracking_id = s.tracking_id
        JOIN flashback.snapshots sn
          ON sn.snapshot_id = (o.details->>'snapshot_id')::bigint
         AND sn.tracking_id = s.tracking_id
        LEFT JOIN flashback.capture_streams cs ON cs.stream_id = cg.stream_id
        WHERE s.operation_id = v_op.operation_id
        FOR UPDATE OF cg, sn;

        IF v_current.operation_state IS DISTINCT FROM 'started' THEN
            CONTINUE;
        END IF;

        BEGIN
            IF v_current.payload_state = 'available'
               AND v_current.generation_state = 'building'
            THEN
                PERFORM public.flashback_internal_activate_external_generation(
                    v_op.generation_id, v_op.tracking_id, v_current.snapshot_id
                );
                v_finalize := public.flashback_maintain_finalize(v_op.operation_id);
                v_resumed := v_resumed + 1;
                CONTINUE;
            ELSIF v_current.payload_state = 'available'
                  AND v_current.generation_state = 'active'
            THEN
                v_finalize := public.flashback_maintain_finalize(v_op.operation_id);
                v_resumed := v_resumed + 1;
                CONTINUE;
            ELSIF v_current.generation_state = 'aborted'
                  OR v_current.payload_state = 'aborted'
            THEN
                v_reason := 'external_reservation_already_aborted';
            ELSIF v_current.generation_state <> 'building'
                  OR v_current.payload_state <> 'creating'
            THEN
                v_reason := format(
                    'external_reservation_state_mismatch:generation=%s,snapshot=%s',
                    v_current.generation_state, v_current.payload_state
                );
            ELSE
                v_artifact := public.flashback_internal_external_artifact_state(
                    v_op.tracking_id, v_op.generation_id,
                    v_current.snapshot_id
                );
                IF v_artifact->>'status' = 'staging_active' THEN
                    v_deferred := v_deferred + 1;
                    CONTINUE;
                ELSIF v_artifact->>'status' IN ('staging_committed', 'published')
                      AND v_current.snapshot_lsn IS NOT NULL
                THEN
                    PERFORM public.flashback_internal_finalize_external_snapshot(
                        v_op.tracking_id, v_op.generation_id,
                        v_current.snapshot_id
                    );
                    v_finalize := public.flashback_maintain_finalize(v_op.operation_id);
                    v_resumed := v_resumed + 1;
                    CONTINUE;
                ELSIF v_artifact->>'status' IN ('staging_committed', 'published')
                      AND v_current.stream_state = 'active'
                THEN
                    -- Complete immutable bytes await only the marker COMMIT
                    -- coordinate. Let capture consume it.
                    v_deferred := v_deferred + 1;
                    CONTINUE;
                ELSIF v_artifact->>'status' IN ('staging_committed', 'published') THEN
                    v_reason := 'external_boundary_unresolvable_stream_unavailable';
                ELSIF v_op.created_at > clock_timestamp() - p_stale_after THEN
                    v_deferred := v_deferred + 1;
                    CONTINUE;
                ELSE
                    v_reason := CASE v_artifact->>'status'
                        WHEN 'absent' THEN 'external_copier_never_started'
                        WHEN 'staging_incomplete' THEN 'external_copier_crashed_before_receipt'
                        ELSE 'external_artifact_unknown_state'
                    END;
                END IF;
            END IF;

            IF v_current.payload_state = 'creating' THEN
                PERFORM public.flashback_internal_snapshot_abort(
                    v_current.snapshot_id, v_op.tracking_id
                );
            END IF;
            IF v_current.generation_state = 'building' THEN
                PERFORM public.flashback_internal_transition_coverage_generation(
                    v_op.generation_id, v_op.tracking_id,
                    'building', 'aborted', v_reason,
                    NULL, NULL, NULL, NULL, NULL, NULL,
                    jsonb_build_object('external_reconciler', true)
                );
            END IF;
            PERFORM public.flashback_operation_append_event(
                v_op.operation_id, 'failed', NULL, v_reason,
                'external snapshot maintenance could not be resumed safely',
                jsonb_build_object(
                    'tracking_id', v_op.tracking_id,
                    'generation_id', v_op.generation_id,
                    'snapshot_id', v_current.snapshot_id,
                    'artifact_state', COALESCE(v_artifact->>'status', 'unknown')
                )
            );
            v_aborted := v_aborted + 1;
        EXCEPTION WHEN OTHERS THEN
            -- Preserve the started reservation for retry. The subtransaction
            -- rolls back any partial DB finalization/activation.
            v_deferred := v_deferred + 1;
            v_errors := v_errors + 1;
            v_last_error := SQLSTATE || ': ' || SQLERRM;
        END;
    END LOOP;

    RETURN jsonb_build_object(
        'status', CASE WHEN v_errors > 0 THEN 'partial' ELSE 'ok' END,
        'cleaned', v_cleaned,
        'resumed', v_resumed,
        'aborted', v_aborted,
        'deferred', v_deferred,
        'errors', v_errors,
        'last_error', v_last_error
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
    'Backend-aware maintenance begin: heap_v1 reanchors immediately; external_zstd durably reserves an online generation and returns an operation for copy/publish.';
COMMENT ON FUNCTION flashback_maintain_external_copy(bigint) IS
    'External maintenance phase two: run the online copy and transactional WAL boundary marker for the exact reserved operation.';
COMMENT ON FUNCTION flashback_maintain_external_publish(bigint) IS
    'External maintenance phase three: publish the verified artifact, activate the successor, and finalize the operation.';
COMMENT ON FUNCTION flashback_maintain_finalize(bigint) IS
    'Second phase: once successor coverage is healthy, seal the predecessor generation (retained, not deleted) and mark the maintain operation sealed. Safe to call repeatedly before that.';
COMMENT ON FUNCTION flashback_internal_reconcile_external_maintenance(interval, integer) IS
    '[Internal] Resume or durably abort stale external maintenance; physical orphan purge occurs only in a later transaction.';
COMMENT ON FUNCTION flashback_maintain_execute(text) IS
    'Back-compat alias for flashback_maintain_begin(); callers must still call flashback_maintain_finalize(operation_id).';
COMMENT ON FUNCTION flashback_prepare_uninstall(boolean) IS
    'Fail-closed uninstall preparation: refuse active lifecycles/pending restores; drop orphan slots when idle.';

REVOKE ALL ON FUNCTION public.flashback_internal_reserve_external_maintenance(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_reconcile_external_maintenance(interval, integer) FROM PUBLIC;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flashback_admin') THEN
        REVOKE ALL ON FUNCTION public.flashback_internal_reserve_external_maintenance(text) FROM flashback_admin;
        REVOKE ALL ON FUNCTION public.flashback_internal_reconcile_external_maintenance(interval, integer) FROM flashback_admin;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pg_monitor') THEN
        REVOKE ALL ON FUNCTION public.flashback_internal_reserve_external_maintenance(text) FROM pg_monitor;
        REVOKE ALL ON FUNCTION public.flashback_internal_reconcile_external_maintenance(interval, integer) FROM pg_monitor;
    END IF;
END
$$;
