-- =================================================================
-- Operator diagnosis: flashback_doctor() and local disaster-point discovery.
-- Read-only. Never mutates coverage, slots or settings.
-- =================================================================

CREATE OR REPLACE FUNCTION flashback_doctor()
RETURNS TABLE (
    scope text,
    check_name text,
    status text,
    observed text,
    expected text,
    action text
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_workers record;
    v_slot record;
    v_mode text;
    v_wal_level text;
    v_preload text;
    v_override boolean;
    v_snap bigint;
    v_peak bigint;
    v_min_fs bigint;
    v_reserve bigint;
    v_tracked bigint;
    v_unhealthy bigint;
    v_gaps bigint;
    v_pending bigint;
    v_pending_verify bigint;
    v_slot_keep text;
BEGIN
    -- Read the configured GUC directly so doctor can report an actionable error
    -- row when capture_mode is illegal (effective_capture_mode() raises).
    v_mode := NULLIF(btrim(COALESCE(current_setting('pg_flashback.capture_mode', true), '')), '');
    IF v_mode IS NULL THEN
        v_mode := 'wal';
    END IF;
    v_wal_level := current_setting('wal_level');
    v_preload := current_setting('shared_preload_libraries');
    SELECT * INTO STRICT v_workers FROM flashback_worker_readiness();
    SELECT * INTO v_slot FROM flashback_slot_status_snapshot() LIMIT 1;
    v_override := flashback_local_capacity_override_active();
    v_snap := flashback_local_guc_bytes('pg_flashback.local_max_snapshot_bytes', NULL);
    v_peak := flashback_local_guc_bytes('pg_flashback.local_max_restore_peak_bytes', NULL);
    v_min_fs := flashback_local_guc_bytes('pg_flashback.local_min_filesystem_bytes', NULL);
    v_reserve := COALESCE(
        flashback_local_guc_bytes('pg_flashback.local_safety_reserve_bytes', '0'),
        0
    );
    v_slot_keep := current_setting('max_slot_wal_keep_size', true);

    -- preload / capture mode
    scope := 'cluster'; check_name := 'shared_preload_libraries';
    IF v_preload ~ '(^|,) *pg_flashback *(,|$)' THEN
        status := 'ok'; observed := v_preload; expected := 'contains pg_flashback';
        action := 'none';
    ELSE
        status := 'error'; observed := v_preload; expected := 'contains pg_flashback';
        action := 'add pg_flashback to shared_preload_libraries and restart PostgreSQL';
    END IF;
    RETURN NEXT;

    scope := 'cluster'; check_name := 'effective_capture_mode';
    observed := v_mode; expected := 'wal';
    IF v_mode = 'wal' THEN
        status := 'ok'; action := 'none';
    ELSE
        status := 'error';
        action := 'set pg_flashback.capture_mode=wal and wal_level=logical; trigger and auto are not supported';
    END IF;
    RETURN NEXT;

    scope := 'cluster'; check_name := 'wal_level';
    observed := v_wal_level; expected := 'logical';
    IF v_wal_level = 'logical' THEN
        status := 'ok'; action := 'none';
    ELSE
        status := 'error';
        action := 'set wal_level=logical in postgresql.conf and restart PostgreSQL';
    END IF;
    RETURN NEXT;

    -- worker admission
    scope := 'database'; check_name := 'worker_admission';
    observed := format('%s; capture_pid=%s; maintenance_pid=%s',
                       v_workers.admission_state,
                       COALESCE(v_workers.capture_worker_pid::text, 'null'),
                       COALESCE(v_workers.maintenance_worker_pid::text, 'null'));
    expected := 'ready';
    IF v_workers.admission_state = 'ready' THEN
        status := 'ok'; action := 'none';
    ELSIF v_workers.admission_state = 'maintenance_missing' THEN
        status := 'warning';
        action := 'restore the maintenance worker; capture may continue but retention/checkpoints are degraded';
    ELSE
        status := 'error';
        action := v_workers.reason;
    END IF;
    RETURN NEXT;

    scope := 'cluster'; check_name := 'worker_pair_capacity';
    observed := format('admitted_pairs=%s slots_required=%s max_workers=%s max_worker_processes=%s configured_demand=%s',
                       v_workers.admitted_pair_count, v_workers.bgworker_slots_required,
                       v_workers.max_workers, v_workers.max_worker_processes,
                       v_workers.configured_pair_demand);
    expected := 'slots_required <= max_worker_processes AND configured_demand <= max_workers';
    IF v_workers.configured_pair_demand > v_workers.max_workers THEN
        status := 'warning';
        action := 'databases beyond max_workers are not admitted; raise max_workers or shrink target_databases';
    ELSIF v_workers.bgworker_slots_required > v_workers.max_worker_processes THEN
        status := 'error';
        action := 'raise max_worker_processes to at least admitted pairs times two';
    ELSE
        status := 'ok'; action := 'none';
    END IF;
    RETURN NEXT;

    -- logical slot
    scope := 'database'; check_name := 'logical_slot';
    observed := format('name=%s wal_status=%s retained_bytes=%s safe_wal_size=%s',
                       COALESCE(v_slot.slot_name, 'null'),
                       COALESCE(v_slot.wal_status, 'null'),
                       COALESCE(v_slot.retained_wal_bytes::text, 'null'),
                       COALESCE(v_slot.safe_wal_size::text, 'null'));
    expected := 'slot present for current database with wal_status not lost/missing when tracking is active';
    SELECT count(*) INTO v_tracked FROM flashback.tracked_tables WHERE is_active;
    IF v_tracked = 0 THEN
        status := 'ok'; action := 'none';
        observed := observed || '; no active tracking';
    ELSIF COALESCE(v_slot.wal_status, '') IN ('missing', 'lost') THEN
        status := 'error';
        action := 'recreate the logical slot and re-anchor affected lifecycles';
    ELSE
        status := 'ok'; action := 'none';
    END IF;
    RETURN NEXT;

    -- capacity budgets
    scope := 'database'; check_name := 'local_capacity_budgets';
    observed := format('max_snapshot=%s max_restore_peak=%s min_filesystem=%s safety_reserve=%s',
                       COALESCE(v_snap::text, 'unset'),
                       COALESCE(v_peak::text, 'unset'),
                       COALESCE(v_min_fs::text, 'unset'),
                       v_reserve);
    expected := 'all three mandatory budgets configured (>0)';
    IF COALESCE(v_snap, 0) <= 0 OR COALESCE(v_peak, 0) <= 0 OR COALESCE(v_min_fs, 0) <= 0 THEN
        status := 'error';
        action := 'run SELECT flashback_config_recommend(); or pg_flashback config recommend, copy the capacity GUC lines into postgresql.conf, then restart PostgreSQL';
    ELSE
        status := 'ok'; action := 'none';
    END IF;
    RETURN NEXT;

    scope := 'cluster'; check_name := 'max_slot_wal_keep_size';
    observed := COALESCE(v_slot_keep, 'unset');
    expected := 'finite value (not -1/unbounded)';
    IF v_slot_keep IS NULL OR v_slot_keep IN ('-1', '') THEN
        status := 'warning';
        action := 'max_slot_wal_keep_size is unlimited; a stalled/lost consumer can grow WAL retention without bound. This is a cluster-wide fuse across every logical slot, not a per-lifecycle storage budget — set a finite cap (see: pg_flashback config recommend) and pair it with pg_flashback.local_max_retained_payload_bytes for the local_delta retained-payload budget.';
    ELSE
        status := 'ok'; action := 'none';
    END IF;
    RETURN NEXT;

    scope := 'database'; check_name := 'local_capacity_override';
    observed := v_override::text; expected := 'false';
    IF v_override THEN
        status := 'warning';
        action := 'local_capacity_override is active; disable after emergency use';
    ELSE
        status := 'ok'; action := 'none';
    END IF;
    RETURN NEXT;

    -- lifecycle health
    SELECT count(*) FILTER (
               WHERE h.health IS DISTINCT FROM 'healthy'
           ),
           COALESCE(sum(h.open_gap_count), 0),
           count(*) FILTER (
               WHERE h.generation_state = 'building'
                  OR h.recommended_action = 'wait_for_boundary_commit_resolution'
           )
      INTO v_unhealthy, v_gaps, v_pending
    FROM flashback_health() h;

    scope := 'database'; check_name := 'tracked_lifecycle_health';
    observed := format('active=%s unhealthy=%s open_gaps=%s pending_boundaries=%s',
                       v_tracked, COALESCE(v_unhealthy, 0), COALESCE(v_gaps, 0),
                       COALESCE(v_pending, 0));
    expected := 'active lifecycles healthy with no open gaps or pending boundaries';
    IF v_tracked = 0 THEN
        status := 'ok'; action := 'none';
    ELSIF COALESCE(v_unhealthy, 0) > 0 OR COALESCE(v_gaps, 0) > 0 THEN
        status := 'error';
        action := 'inspect flashback_health() and repair gaps/worker/slot issues before restore';
    ELSIF COALESCE(v_pending, 0) > 0 THEN
        status := 'warning';
        action := 'wait for pending boundary COMMIT LSN resolution';
    ELSE
        status := 'ok'; action := 'none';
    END IF;
    RETURN NEXT;

    -- Stale recover projections: worker/finalizer should append verified/failed.
    -- Doctor remains read-only; operators heal via doctor --reconcile / maintenance.
    v_pending_verify := 0;
    IF to_regclass('flashback.operation_current_state') IS NOT NULL THEN
        SELECT count(*) INTO v_pending_verify
        FROM flashback.operation_current_state s
        WHERE s.command IN ('recover', 'restore_lsn')
          AND s.state = 'applied_coverage_pending';
    END IF;
    scope := 'database'; check_name := 'recover_verification_pending';
    observed := format('applied_coverage_pending=%s', COALESCE(v_pending_verify, 0));
    expected := '0 pending recover finalizations';
    IF COALESCE(v_pending_verify, 0) = 0 THEN
        status := 'ok'; action := 'none';
    ELSE
        status := 'warning';
        action := 'wait for maintenance finalizer or run: pg_flashback doctor --reconcile';
    END IF;
    RETURN NEXT;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_disaster_points(
    target_table text,
    lookback interval DEFAULT interval '24 hours'
)
RETURNS TABLE (
    table_name text,
    event_type text,
    disaster_commit_lsn pg_lsn,
    disaster_time timestamptz,
    safe_target_lsn pg_lsn,
    safe_target_time timestamptz,
    generation_id bigint,
    status text,
    reason text
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_tracking_id bigint;
    v_schema text;
    v_table text;
    v_qual text;
    rec record;
    v_safe_lsn pg_lsn;
    v_safe_time timestamptz;
    v_status text;
    v_reason text;
    v_gen record;
    v_gen_id bigint;
    v_admit_ok boolean;
BEGIN
    IF lookback IS NULL OR lookback <= interval '0' THEN
        RAISE EXCEPTION 'flashback_disaster_points: lookback must be positive';
    END IF;

    -- Historical-fallback mode of the single canonical resolver: post-DROP
    -- discovery works when the live OID is gone but the tracking lifecycle
    -- row remains (is_active stays true across a DROP -- only unprotect
    -- clears it), and this diagnostic must also still be able to report a
    -- lifecycle's disaster history after it was unprotected and never
    -- retracked. An ambiguous unqualified name (active or historical) fails
    -- closed instead of silently picking one schema's table.
    SELECT r.tracking_id, r.schema_name, r.table_name
      INTO v_tracking_id, v_schema, v_table
    FROM public.flashback_internal_resolve_tracked_table_any(target_table) r;

    IF v_tracking_id IS NULL THEN
        RAISE EXCEPTION 'flashback_disaster_points: no local_delta tracking lifecycle for %',
            target_table;
    END IF;

    v_qual := format('%I.%I', v_schema, v_table);

    FOR rec IN
        SELECT d.event_type, d.commit_lsn, d.committed_at, d.event_time,
               d.generation_id, d.stream_id, d.source_xid
        FROM flashback.delta_log d
        WHERE d.tracking_id = v_tracking_id
          AND d.event_type IN ('DROP', 'TRUNCATE', 'ALTER')
          AND COALESCE(d.committed_at, d.event_time) >= statement_timestamp() - lookback
        ORDER BY COALESCE(d.committed_at, d.event_time) DESC, d.event_id DESC
    LOOP
        v_safe_lsn := NULL;
        v_safe_time := NULL;
        v_status := 'non_restorable';
        v_reason := NULL;
        v_gen := NULL;
        v_gen_id := NULL;
        v_admit_ok := false;

        IF rec.commit_lsn IS NULL THEN
            v_reason := 'disaster commit_lsn is not yet resolved (pending boundary)';
        ELSIF EXISTS (
            SELECT 1 FROM flashback.coverage_generations g
            WHERE g.tracking_id = v_tracking_id
              AND g.state = 'building'
        ) THEN
            v_reason := 'unresolved building generation boundary present';
        ELSIF EXISTS (
            SELECT 1 FROM flashback.coverage_generations g
            WHERE g.tracking_id = v_tracking_id
              AND g.state IN ('active', 'sealed')
              AND g.state_reason = 'anchor_missing'
        ) THEN
            v_reason := 'frozen watermark / missing anchor on an eligible generation';
        ELSE
            -- Fail closed when multiple generations match: never pick newest
            -- generation_no and hope. A single matching generation is required.
            IF (
                SELECT count(*)::integer
                FROM flashback.coverage_generations g
                WHERE g.tracking_id = v_tracking_id
                  AND g.recovery_profile = 'local_delta'
                  AND g.state IN ('active', 'sealed')
                  AND (
                      (rec.generation_id IS NOT NULL AND g.generation_id = rec.generation_id)
                      OR (
                          rec.generation_id IS NULL
                          AND g.boundary_lsn < rec.commit_lsn
                          AND (g.superseded_before_lsn IS NULL
                               OR rec.commit_lsn <= g.superseded_before_lsn)
                      )
                  )
            ) > 1 THEN
                v_reason := 'ambiguous_coverage_generation: multiple eligible generations for the disaster event';
            ELSE
                SELECT g.* INTO v_gen
                FROM flashback.coverage_generations g
                WHERE g.tracking_id = v_tracking_id
                  AND g.recovery_profile = 'local_delta'
                  AND g.state IN ('active', 'sealed')
                  AND (
                      (rec.generation_id IS NOT NULL AND g.generation_id = rec.generation_id)
                      OR (
                          rec.generation_id IS NULL
                          AND g.boundary_lsn < rec.commit_lsn
                          AND (g.superseded_before_lsn IS NULL
                               OR rec.commit_lsn <= g.superseded_before_lsn)
                      )
                  )
                ORDER BY g.generation_no DESC
                LIMIT 1;
                IF FOUND THEN
                    v_gen_id := v_gen.generation_id;
                ELSE
                    v_gen_id := NULL;
                END IF;

            IF v_gen_id IS NULL THEN
                IF v_reason IS NULL THEN
                    v_reason := 'ambiguous or missing coverage generation for the disaster event';
                END IF;
            ELSIF EXISTS (
                SELECT 1
                FROM flashback.coverage_gaps gap
                WHERE gap.tracking_id = v_tracking_id
                  AND gap.reanchored_by_generation_id IS NULL
                  AND gap.gap_start_lsn IS NOT NULL
                  AND gap.gap_start_lsn < rec.commit_lsn
                  AND (gap.gap_end_lsn IS NULL OR gap.gap_end_lsn >= v_gen.boundary_lsn)
            ) THEN
                v_reason := 'open coverage gap intersects the pre-disaster prefix';
            ELSE
                -- Last complete admitted COMMIT prefix before the disaster TX.
                SELECT cc.commit_lsn, cc.committed_at
                  INTO v_safe_lsn, v_safe_time
                FROM flashback.capture_commits cc
                WHERE cc.stream_id = COALESCE(rec.stream_id, v_gen.stream_id)
                  AND cc.commit_lsn < rec.commit_lsn
                  AND cc.commit_lsn >= v_gen.boundary_lsn
                  AND (v_gen.superseded_before_lsn IS NULL
                       OR cc.commit_lsn < v_gen.superseded_before_lsn)
                ORDER BY cc.commit_lsn DESC
                LIMIT 1;

                IF v_safe_lsn IS NULL AND v_gen.boundary_lsn < rec.commit_lsn THEN
                    v_safe_lsn := v_gen.boundary_lsn;
                    v_safe_time := v_gen.boundary_time;
                END IF;

                IF v_safe_lsn IS NULL THEN
                    v_reason := 'no predecessor COMMIT-LSN prefix before the disaster transaction';
                ELSIF EXISTS (
                    SELECT 1
                    FROM flashback.coverage_gaps gap
                    WHERE gap.tracking_id = v_tracking_id
                      AND gap.reanchored_by_generation_id IS NULL
                      AND gap.gap_start_lsn IS NOT NULL
                      AND ((gap.lower_bound_inclusive AND v_safe_lsn >= gap.gap_start_lsn)
                           OR (NOT gap.lower_bound_inclusive AND v_safe_lsn > gap.gap_start_lsn))
                      AND (gap.gap_end_lsn IS NULL OR v_safe_lsn < gap.gap_end_lsn)
                ) THEN
                    v_reason := 'safe target LSN falls inside an open coverage gap';
                    v_safe_lsn := NULL;
                    v_safe_time := NULL;
                ELSE
                    -- Soft admission probe without raising; never calls legacy
                    -- timestamp restore.
                    BEGIN
                        SELECT EXISTS (
                            SELECT 1 FROM flashback_admit_lsn_target(v_qual, v_safe_lsn)
                        ) INTO v_admit_ok;
                    EXCEPTION WHEN OTHERS THEN
                        v_admit_ok := false;
                        v_reason := format('safe target failed admission: %s', SQLERRM);
                    END;
                    IF v_admit_ok THEN
                        v_status := 'restorable';
                        v_reason := 'last complete admitted COMMIT-LSN prefix before the disaster transaction';
                    ELSIF v_reason IS NULL THEN
                        v_reason := 'safe target was not admitted by flashback_admit_lsn_target';
                        v_safe_lsn := NULL;
                        v_safe_time := NULL;
                    ELSE
                        v_safe_lsn := NULL;
                        v_safe_time := NULL;
                    END IF;
                END IF;
            END IF;
            END IF; -- count > 1 vs single generation
        END IF;

        table_name := v_qual;
        event_type := rec.event_type;
        disaster_commit_lsn := rec.commit_lsn;
        disaster_time := COALESCE(rec.committed_at, rec.event_time);
        safe_target_lsn := v_safe_lsn;
        safe_target_time := v_safe_time;
        generation_id := COALESCE(rec.generation_id, v_gen_id);
        status := v_status;
        reason := v_reason;
        RETURN NEXT;
    END LOOP;
END;
$$;

COMMENT ON FUNCTION flashback_doctor() IS
    'Read-only operational diagnosis. Returns one row per check with status ok|warning|error. Never mutates state.';
COMMENT ON FUNCTION flashback_disaster_points(text, interval) IS
    'List recent local_delta DROP/TRUNCATE/ALTER disasters and the last complete admitted COMMIT-LSN prefix before each disaster transaction. Fail-closed/non-restorable when gaps, pending boundaries, or ambiguous generations prevent a safe target. Does not use legacy timestamp restore.';
