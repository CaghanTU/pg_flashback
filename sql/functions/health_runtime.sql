-- =================================================================
-- Actionable coverage / slot health projection.
-- Read-only: never mutates state. Durable gap/generation metadata remains
-- authoritative if NOTIFY delivery is lost.
-- Channel for committed action-required events: pg_flashback_action_required
-- =================================================================

CREATE OR REPLACE FUNCTION flashback_slot_status_snapshot()
RETURNS TABLE (
    slot_name text,
    restart_lsn pg_lsn,
    confirmed_flush_lsn pg_lsn,
    current_wal_lsn pg_lsn,
    retained_wal_bytes bigint,
    wal_status text,
    safe_wal_size bigint
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_slot text := public.flashback_effective_slot_name();
    v_has_wal_status boolean;
    v_has_safe_wal_size boolean;
    v_sql text;
    v_found boolean := false;
    r record;
BEGIN
    SELECT EXISTS (
               SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'pg_catalog'
                 AND table_name = 'pg_replication_slots'
                 AND column_name = 'wal_status'
           ),
           EXISTS (
               SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'pg_catalog'
                 AND table_name = 'pg_replication_slots'
                 AND column_name = 'safe_wal_size'
           )
      INTO v_has_wal_status, v_has_safe_wal_size;

    v_sql := format(
        $q$
        SELECT
            s.slot_name::text AS slot_name,
            s.restart_lsn,
            s.confirmed_flush_lsn,
            pg_current_wal_lsn() AS current_wal_lsn,
            CASE
                WHEN s.restart_lsn IS NULL THEN NULL
                ELSE pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn)
            END::bigint AS retained_wal_bytes,
            %s AS wal_status,
            %s AS safe_wal_size
        FROM pg_replication_slots s
        WHERE s.slot_name = %L
          AND s.database = current_database()
        $q$,
        CASE WHEN v_has_wal_status THEN 's.wal_status::text' ELSE 'NULL::text' END,
        CASE WHEN v_has_safe_wal_size THEN 's.safe_wal_size::bigint' ELSE 'NULL::bigint' END,
        v_slot
    );

    FOR r IN EXECUTE v_sql LOOP
        v_found := true;
        slot_name := r.slot_name;
        restart_lsn := r.restart_lsn;
        confirmed_flush_lsn := r.confirmed_flush_lsn;
        current_wal_lsn := r.current_wal_lsn;
        retained_wal_bytes := r.retained_wal_bytes;
        wal_status := r.wal_status;
        safe_wal_size := r.safe_wal_size;
        RETURN NEXT;
    END LOOP;

    IF NOT v_found THEN
        slot_name := v_slot;
        restart_lsn := NULL;
        confirmed_flush_lsn := NULL;
        current_wal_lsn := pg_current_wal_lsn();
        retained_wal_bytes := NULL;
        wal_status := 'missing';
        safe_wal_size := NULL;
        RETURN NEXT;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_health()
RETURNS TABLE (
    tracking_id bigint,
    table_name text,
    recovery_profile text,
    health text,
    recommended_action text,
    generation_id bigint,
    generation_state text,
    stream_id bigint,
    stream_state text,
    valid_through_lsn pg_lsn,
    valid_through_time timestamptz,
    open_gap_count bigint,
    slot_name text,
    restart_lsn pg_lsn,
    confirmed_flush_lsn pg_lsn,
    current_wal_lsn pg_lsn,
    retained_wal_bytes bigint,
    wal_status text,
    safe_wal_size bigint,
    local_snapshot_bytes bigint,
    retained_delta_bytes bigint,
    configured_capacity_budget bigint,
    capacity_override boolean,
    reason text
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    rec record;
    slot record;
    v_health text;
    v_action text;
    v_reason text;
    v_cap record;
    v_lag_warn bigint;
    v_lag_risk bigint;
    v_budget_exhausted boolean;
    v_post_restore_gap boolean;
BEGIN
    SELECT * INTO slot FROM flashback_slot_status_snapshot() LIMIT 1;

    v_lag_warn := COALESCE(
        flashback_local_guc_bytes('pg_flashback.slot_lag_warning_bytes', '256MB'),
        pg_size_bytes('256MB')
    );
    v_lag_risk := COALESCE(
        flashback_local_guc_bytes('pg_flashback.slot_lag_at_risk_bytes', '1GB'),
        pg_size_bytes('1GB')
    );

    FOR rec IN
        SELECT
            tt.tracking_id,
            format('%I.%I', tt.schema_name, tt.table_name) AS table_name,
            tt.recovery_profile,
            tt.rel_oid,
            cg.generation_id,
            cg.state AS generation_state,
            cg.stream_id,
            cs.state AS stream_state,
            cg.valid_through_lsn,
            cg.valid_through_time,
            cg.state_reason AS generation_state_reason,
            cg.details AS generation_details,
            ba.backup_label,
            ba.backup_stop_lsn,
            ba.details AS anchor_details,
            pending.generation_id AS pending_generation_id,
            pending.state_reason AS pending_state_reason,
            pending.boundary_kind AS pending_boundary_kind,
            COALESCE(gaps.open_gap_count, 0) AS open_gap_count,
            COALESCE(gaps.timeline_gap_count, 0) AS timeline_gap_count,
            COALESCE(gaps.post_restore_gap_count, 0) AS post_restore_gap_count,
            COALESCE(frozen_history.frozen_count, 0) AS frozen_count,
            COALESCE(retirement.retiring_count, 0) AS retiring_count,
            COALESCE(retention_block.blocked, false) AS retention_blocked,
            cs.invalidation_reason
        FROM flashback.tracked_tables tt
        LEFT JOIN LATERAL (
            SELECT g.* FROM flashback.coverage_generations g
            WHERE g.tracking_id = tt.tracking_id AND g.state = 'active'
            LIMIT 1
        ) cg ON true
        LEFT JOIN flashback.backup_anchors ba
          ON ba.backup_anchor_id = cg.backup_anchor_id
         AND ba.tracking_id = cg.tracking_id
        LEFT JOIN flashback.capture_streams cs ON cs.stream_id = cg.stream_id
        LEFT JOIN LATERAL (
            SELECT g.generation_id, g.state_reason, g.boundary_kind
            FROM flashback.coverage_generations g
            WHERE g.tracking_id = tt.tracking_id AND g.state = 'building'
            ORDER BY g.generation_no DESC
            LIMIT 1
        ) pending ON true
        LEFT JOIN LATERAL (
            SELECT
                count(*) AS open_gap_count,
                count(*) FILTER (WHERE gap.reason = 'timeline_mismatch') AS timeline_gap_count,
                count(*) FILTER (WHERE gap.reason = 'post_restore_unanchored') AS post_restore_gap_count
            FROM flashback.coverage_gaps gap
            WHERE gap.tracking_id = tt.tracking_id
              AND gap.reanchored_by_generation_id IS NULL
        ) gaps ON true
        LEFT JOIN LATERAL (
            SELECT count(*) AS frozen_count
            FROM flashback.coverage_generations frozen
            WHERE frozen.tracking_id = tt.tracking_id
              AND frozen.state IN ('active', 'sealed')
              AND frozen.state_reason = 'anchor_missing'
        ) frozen_history ON true
        LEFT JOIN LATERAL (
            SELECT count(*) AS retiring_count
            FROM flashback.generation_payload_retirements r
            WHERE r.tracking_id = tt.tracking_id
              AND r.state = 'retiring'
        ) retirement ON true
        LEFT JOIN LATERAL (
            SELECT EXISTS (
                SELECT 1
                FROM flashback.coverage_generations sealed
                WHERE sealed.tracking_id = tt.tracking_id
                  AND sealed.state = 'sealed'
                  AND sealed.sealed_at <= statement_timestamp() - tt.retention_interval
                  AND NOT EXISTS (
                      SELECT 1
                      FROM flashback.generation_payload_retirements r
                      WHERE r.generation_id = sealed.generation_id
                  )
                  AND (
                      (
                          sealed.superseded_before_lsn IS NULL
                          OR sealed.valid_through_lsn IS NULL
                          OR sealed.valid_through_lsn < sealed.superseded_before_lsn
                          AND NOT EXISTS (
                              SELECT 1
                              FROM flashback.coverage_gaps closed_gap
                              WHERE closed_gap.tracking_id = sealed.tracking_id
                                AND closed_gap.source_generation_id = sealed.generation_id
                                AND closed_gap.gap_start_lsn IS NOT NULL
                                AND closed_gap.gap_end_lsn IS NOT NULL
                                AND closed_gap.reanchored_by_generation_id IS NOT NULL
                                AND closed_gap.gap_start_lsn <= sealed.valid_through_lsn
                                AND closed_gap.gap_end_lsn >= sealed.superseded_before_lsn
                          )
                      )
                      OR NOT EXISTS (
                          SELECT 1
                          FROM flashback.coverage_generations successor
                          JOIN flashback.snapshots successor_snapshot
                            ON successor_snapshot.snapshot_id = successor.boundary_snapshot_id
                           AND successor_snapshot.tracking_id = successor.tracking_id
                          WHERE successor.tracking_id = sealed.tracking_id
                            AND successor.state = 'active'
                            AND (
                                successor.stream_id IS DISTINCT FROM sealed.stream_id
                                OR successor.boundary_lsn >= sealed.superseded_before_lsn
                            )
                            AND successor_snapshot.payload_state = 'available'
                            AND to_regclass(successor_snapshot.snapshot_table) IS NOT NULL
                            AND public.flashback_payload_is_owned(
                                    to_regclass(successor_snapshot.snapshot_table)
                                )
                      )
                  )
            ) AS blocked
        ) retention_block ON true
        WHERE tt.is_active
        ORDER BY tt.tracking_id
    LOOP
        v_budget_exhausted := false;
        v_cap := NULL;
        BEGIN
            SELECT * INTO v_cap
            FROM flashback_measure_local_capacity(rec.rel_oid);
            IF rec.recovery_profile = 'local_delta'
               AND (
                   COALESCE(v_cap.configured_max_snapshot_bytes, 0) = 0
                   OR COALESCE(v_cap.configured_max_restore_peak_bytes, 0) = 0
                   OR COALESCE(v_cap.configured_min_filesystem_bytes, 0) = 0
                   OR v_cap.projected_base_snapshot_bytes
                        > COALESCE(v_cap.configured_max_snapshot_bytes, 0)
                   OR v_cap.projected_restore_peak_bytes
                        > COALESCE(v_cap.configured_max_restore_peak_bytes, 0)
                   OR v_cap.filesystem_available_bytes
                        < (GREATEST(v_cap.projected_track_bytes, v_cap.projected_restore_peak_bytes)
                           + COALESCE(v_cap.configured_min_filesystem_bytes, 0))
               )
            THEN
                v_budget_exhausted := true;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            v_cap := NULL;
        END;

        v_post_restore_gap := rec.post_restore_gap_count > 0
            OR COALESCE(rec.pending_state_reason, '') = 'post_restore_unanchored';

        IF (
               rec.recovery_profile = 'local_delta'
               AND (
                   COALESCE(slot.wal_status, '') = 'lost'
                   OR (
                       COALESCE(slot.wal_status, '') = 'missing'
                       AND rec.stream_id IS NOT NULL
                   )
                   OR (
                       rec.stream_state = 'broken'
                       AND COALESCE(rec.invalidation_reason, '') ~*
                           '(slot|replication_slot|missing.slot|wal_status)'
                   )
               )
           )
        THEN
            v_health := 'slot_lost';
            v_action := 'recreate_logical_slot_and_reanchor';
            v_reason := COALESCE(rec.invalidation_reason, slot.wal_status, 'logical slot lost');
        ELSIF COALESCE(rec.generation_state_reason, '') = 'timeline_mismatch_frontier_frozen'
           OR rec.timeline_gap_count > 0
        THEN
            v_health := 'timeline_mismatch';
            v_action := 'take_new_verified_full_backup_anchor';
            v_reason := COALESCE(
                rec.invalidation_reason,
                'backup frontier frozen after timeline mismatch; requires a new verified FULL anchor'
            );
        ELSIF COALESCE(rec.generation_state_reason, '') IN (
                  'repository_verification_failed', 'anchor_missing'
              )
           OR rec.frozen_count > 0
        THEN
            v_health := 'repository_anchor_missing';
            v_action := 'restore_repository_anchor_or_reanchor';
            v_reason := 'backup repository proof is unavailable; restore admission is frozen';
        ELSIF v_post_restore_gap
           OR (
               rec.recovery_profile = 'backup'
               AND rec.pending_generation_id IS NOT NULL
               AND rec.generation_id IS NULL
           )
        THEN
            v_health := 'backup_reanchor_required';
            v_action := 'take_and_verify_fresh_full_after_marker';
            v_reason := format(
                'tracking_id %s is unanchored after production swap; a fresh FULL started after the swap marker is required (retained pre-swap FULLs are ineligible)',
                rec.tracking_id
            );
        ELSIF slot.safe_wal_size IS NOT NULL
           AND slot.safe_wal_size <= v_lag_risk
        THEN
            -- safe_wal_size is remaining WAL budget before PostgreSQL may
            -- invalidate the slot (NULL when max_slot_wal_keep_size is -1).
            v_health := 'slot_at_risk';
            v_action := 'drain_capture_or_reduce_wal_pressure';
            v_reason := format(
                'slot remaining safe_wal_size %s bytes is at or below at-risk budget %s',
                slot.safe_wal_size, v_lag_risk
            );
        ELSIF slot.retained_wal_bytes IS NOT NULL
           AND slot.retained_wal_bytes >= v_lag_risk
        THEN
            v_health := 'slot_at_risk';
            v_action := 'drain_capture_or_reduce_wal_pressure';
            v_reason := format(
                'slot retained WAL %s bytes meets or exceeds at-risk budget %s',
                slot.retained_wal_bytes, v_lag_risk
            );
        ELSIF slot.safe_wal_size IS NOT NULL
           AND slot.safe_wal_size <= v_lag_warn
        THEN
            v_health := 'slot_lag_warning';
            v_action := 'inspect_capture_worker_and_slot_lag';
            v_reason := format(
                'slot remaining safe_wal_size %s bytes is at or below warning budget %s',
                slot.safe_wal_size, v_lag_warn
            );
        ELSIF slot.retained_wal_bytes IS NOT NULL
           AND slot.retained_wal_bytes >= v_lag_warn
        THEN
            v_health := 'slot_lag_warning';
            v_action := 'inspect_capture_worker_and_slot_lag';
            v_reason := format(
                'slot retained WAL %s bytes exceeds warning budget %s',
                slot.retained_wal_bytes, v_lag_warn
            );
        ELSIF v_budget_exhausted THEN
            v_health := 'local_budget_exhausted';
            v_action := 'raise_local_budgets_or_use_backup_profile';
            v_reason := 'configured local capacity budgets cannot admit track/re-anchor/restore';
        ELSIF rec.stream_state = 'broken' OR rec.open_gap_count > 0 THEN
            v_health := 'reanchor_recommended';
            v_action := 'flashback_reanchor';
            v_reason := COALESCE(
                rec.invalidation_reason,
                'open coverage gap; establish a new exact local boundary'
            );
        ELSIF rec.pending_generation_id IS NOT NULL
           OR rec.retiring_count > 0
           OR rec.retention_blocked
        THEN
            v_health := 'maintenance_required';
            v_action := CASE
                WHEN rec.pending_boundary_kind = 'full_reanchor'
                    THEN 'run_reconcile_anchors_or_verify_anchor'
                WHEN rec.pending_generation_id IS NOT NULL
                    THEN 'wait_for_boundary_commit_resolution'
                WHEN rec.retention_blocked
                    THEN 'run_reconcile_anchors_to_retire_predecessors'
                ELSE 'wait_for_payload_retirement'
            END;
            v_reason := COALESCE(
                CASE WHEN rec.pending_boundary_kind = 'full_reanchor'
                     THEN 'backup FULL re-anchor building; run helper reconcile-anchors (never creates a FULL)' END,
                CASE WHEN rec.pending_generation_id IS NOT NULL
                     THEN 'generation boundary awaiting COMMIT LSN' END,
                CASE WHEN rec.retiring_count > 0
                     THEN 'generation payload retirement in progress' END,
                CASE WHEN rec.retention_blocked
                     THEN 'sealed predecessor exclusive range is past retention; run reconcile-anchors to retire' END
            );
        ELSIF rec.generation_id IS NULL THEN
            IF rec.recovery_profile = 'backup' THEN
                v_health := 'backup_reanchor_required';
                v_action := 'activate_eligible_retained_or_fresh_full';
                v_reason := 'no eligible coverage generation; activate a retained FULL with continuous WAL or verify a fresh FULL after the marker';
            ELSE
                v_health := 'reanchor_recommended';
                v_action := 'flashback_reanchor';
                v_reason := 'no eligible coverage generation';
            END IF;
        ELSIF rec.stream_state = 'active'
           OR (rec.recovery_profile = 'backup' AND rec.generation_state = 'active')
        THEN
            IF rec.recovery_profile = 'backup'
               AND COALESCE(rec.generation_details->>'activation_mode', '')
                   = 'retained_full_plus_wal'
               AND rec.backup_stop_lsn IS NOT NULL
               AND rec.valid_through_lsn IS NOT NULL
               AND (rec.valid_through_lsn - rec.backup_stop_lsn) > pg_size_bytes('1GB')
            THEN
                v_health := 'healthy';
                v_action := 'consider_fresher_full_anchor';
                v_reason := format(
                    'active retained FULL %s; replay distance from backup_stop %s through %s may imply poor RTO',
                    rec.backup_label, rec.backup_stop_lsn, rec.valid_through_lsn
                );
            ELSIF rec.recovery_profile = 'backup' AND rec.backup_label IS NOT NULL THEN
                v_health := 'healthy';
                v_action := 'none';
                v_reason := format(
                    'preferred FULL %s mode=%s valid_through=%s; schedule helper reconcile-anchors to discover newer FULLs (never creates backups)',
                    rec.backup_label,
                    COALESCE(
                        rec.generation_details->>'activation_mode',
                        'fresh_full_after_marker'
                    ),
                    rec.valid_through_lsn
                );
            ELSE
                v_health := 'healthy';
                v_action := 'none';
                v_reason := NULL;
            END IF;
        ELSE
            v_health := 'maintenance_required';
            v_action := 'inspect_coverage_state';
            v_reason := 'coverage state is not currently healthy';
        END IF;

        tracking_id := rec.tracking_id;
        table_name := rec.table_name;
        recovery_profile := rec.recovery_profile;
        health := v_health;
        recommended_action := v_action;
        generation_id := rec.generation_id;
        generation_state := rec.generation_state;
        stream_id := rec.stream_id;
        stream_state := rec.stream_state;
        valid_through_lsn := rec.valid_through_lsn;
        valid_through_time := rec.valid_through_time;
        open_gap_count := rec.open_gap_count;
        slot_name := slot.slot_name;
        restart_lsn := slot.restart_lsn;
        confirmed_flush_lsn := slot.confirmed_flush_lsn;
        current_wal_lsn := slot.current_wal_lsn;
        retained_wal_bytes := slot.retained_wal_bytes;
        wal_status := slot.wal_status;
        safe_wal_size := slot.safe_wal_size;
        local_snapshot_bytes := COALESCE(v_cap.projected_base_snapshot_bytes, 0);
        retained_delta_bytes := COALESCE(v_cap.retained_local_payload_bytes, 0);
        configured_capacity_budget := COALESCE(
            v_cap.configured_max_restore_peak_bytes,
            v_cap.configured_max_snapshot_bytes
        );
        capacity_override := COALESCE(v_cap.capacity_override, false);
        reason := v_reason;
        RETURN NEXT;
    END LOOP;
END;
$$;

COMMENT ON FUNCTION flashback_health() IS
    'Read-only actionable coverage/slot health. Durable metadata remains authoritative if NOTIFY on channel pg_flashback_action_required is lost.';

COMMENT ON FUNCTION flashback_slot_status_snapshot() IS
    'Version-tolerant logical-slot lag/status snapshot for health projection.';
