-- =================================================================
-- Server-side recovery plan (authority) + plan-token execute.
-- plan_token is canonical identity / stale-selection guard, not a capability.
-- Authorization is RBAC. Execute fully recomputes and compares under lock.
-- =================================================================

CREATE OR REPLACE FUNCTION flashback_recover_plan(
    p_table text,
    p_lookback interval DEFAULT interval '24 hours',
    p_disaster_event_id bigint DEFAULT NULL,
    p_at timestamptz DEFAULT NULL,
    p_lsn pg_lsn DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_plan_version integer := 1;
    v_row record;
    v_tracking_id bigint;
    v_live_oid oid;
    v_tracked_oid oid;
    v_identity_conflict boolean := false;
    v_manifest jsonb;
    v_status text;
    v_code text;
    v_token text;
    v_canonical text;
    v_selection text;
    v_resolved pg_lsn;
BEGIN
    -- Resolve tracking identity
    SELECT tt.tracking_id, tt.rel_oid, format('%I.%I', tt.schema_name, tt.table_name)
      INTO v_tracking_id, v_tracked_oid, v_canonical
    FROM flashback.tracked_tables tt
    WHERE tt.recovery_profile = 'local_delta'
      AND (
          (to_regclass(p_table) IS NOT NULL AND tt.rel_oid = to_regclass(p_table))
          OR format('%I.%I', tt.schema_name, tt.table_name) = p_table
          OR (position('.' IN p_table) = 0 AND tt.table_name = p_table)
      )
    ORDER BY tt.is_active DESC, tt.tracked_since DESC
    LIMIT 1;

    IF v_tracking_id IS NULL THEN
        RETURN jsonb_build_object(
            'schema_version', v_plan_version,
            'status', 'error',
            'code', 'no_lifecycle',
            'table', p_table,
            'blockers', jsonb_build_array('no local_delta tracking lifecycle')
        );
    END IF;

    v_live_oid := to_regclass(v_canonical);
    IF v_live_oid IS NOT NULL AND v_live_oid IS DISTINCT FROM v_tracked_oid THEN
        v_identity_conflict := true;
    END IF;

    IF p_lsn IS NOT NULL THEN
        v_selection := 'advanced_lsn';
        v_row := NULL;
        -- Advanced LSN: synthesize a plan row from admission.
        BEGIN
            PERFORM 1 FROM flashback_admit_lsn_target(v_canonical, p_lsn);
            v_status := 'restorable';
            v_code := 'ok';
        EXCEPTION WHEN OTHERS THEN
            v_status := 'non_restorable';
            v_code := 'lsn_not_admitted';
        END;
        v_token := flashback_sha256(format('%s|%s|%s|%s|%s',
            v_plan_version, v_tracking_id, 'lsn', p_lsn::text, v_canonical));
        RETURN jsonb_build_object(
            'schema_version', v_plan_version,
            'plan_version', v_plan_version,
            'plan_token', v_token,
            'selection', v_selection,
            'table_name', v_canonical,
            'tracking_id', v_tracking_id,
            'disaster_event_id', NULL,
            'generation_id', NULL,
            'target_lsn', p_lsn,
            'status', v_status,
            'code', v_code,
            'identity_conflict', v_identity_conflict,
            'duration_estimate', 'unknown',
            'note', 'plan_token is identity/stale guard only; RBAC authorizes execute'
        );
    END IF;

    IF p_disaster_event_id IS NOT NULL THEN
        v_selection := 'explicit_event';
        SELECT
            d.table_name, d.event_type, d.disaster_commit_lsn, d.disaster_time,
            d.safe_target_lsn, d.safe_target_time, d.generation_id, d.status, d.reason,
            -- expose delta event_id via join
            (SELECT dl.event_id FROM flashback.delta_log dl
              WHERE dl.tracking_id = v_tracking_id
                AND dl.event_type = 'DROP'
                AND dl.commit_lsn IS NOT DISTINCT FROM d.disaster_commit_lsn
              ORDER BY dl.event_id DESC LIMIT 1) AS disaster_event_id
          INTO v_row
        FROM flashback_disaster_points(v_canonical, p_lookback) d
        WHERE d.event_type = 'DROP'
          AND EXISTS (
              SELECT 1 FROM flashback.delta_log dl
              WHERE dl.event_id = p_disaster_event_id
                AND dl.tracking_id = v_tracking_id
                AND dl.commit_lsn IS NOT DISTINCT FROM d.disaster_commit_lsn
          )
        LIMIT 1;
    ELSIF p_at IS NOT NULL THEN
        v_selection := 'timestamp';
        -- Timestamp UX: resolve to proven LSN prefix via resolve_target, then
        -- bind to the DROP whose commit_lsn is the latest <= that coordinate
        -- evaluating that DROP alone (no silent older fallback).
        BEGIN
            SELECT target_lsn INTO v_resolved
            FROM flashback_resolve_target(v_canonical, p_at)
            LIMIT 1;
            SELECT
                d.table_name, d.event_type, d.disaster_commit_lsn, d.disaster_time,
                d.safe_target_lsn, d.safe_target_time, d.generation_id, d.status, d.reason,
                (SELECT dl.event_id FROM flashback.delta_log dl
                  WHERE dl.tracking_id = v_tracking_id
                    AND dl.event_type = 'DROP'
                    AND dl.commit_lsn IS NOT DISTINCT FROM d.disaster_commit_lsn
                  ORDER BY dl.event_id DESC LIMIT 1) AS disaster_event_id
              INTO v_row
            FROM flashback_disaster_points(v_canonical, p_lookback) d
            WHERE d.event_type = 'DROP'
              AND d.disaster_commit_lsn IS NOT NULL
              AND d.disaster_commit_lsn <= COALESCE(v_resolved, '0/0'::pg_lsn)
            ORDER BY d.disaster_commit_lsn DESC
            LIMIT 1;
        EXCEPTION WHEN OTHERS THEN
            v_row := NULL;
        END;
    ELSE
        v_selection := 'latest_drop';
        SELECT
            d.table_name, d.event_type, d.disaster_commit_lsn, d.disaster_time,
            d.safe_target_lsn, d.safe_target_time, d.generation_id, d.status, d.reason,
            (SELECT dl.event_id FROM flashback.delta_log dl
              WHERE dl.tracking_id = v_tracking_id
                AND dl.event_type = 'DROP'
                AND dl.commit_lsn IS NOT DISTINCT FROM d.disaster_commit_lsn
              ORDER BY dl.event_id DESC LIMIT 1) AS disaster_event_id
          INTO v_row
        FROM flashback_disaster_points(v_canonical, p_lookback) d
        WHERE d.event_type = 'DROP'
        ORDER BY d.disaster_commit_lsn DESC NULLS LAST, d.disaster_time DESC NULLS LAST
        LIMIT 1;
    END IF;

    IF v_row IS NULL OR v_row.table_name IS NULL THEN
        RETURN jsonb_build_object(
            'schema_version', v_plan_version,
            'plan_version', v_plan_version,
            'selection', v_selection,
            'table_name', v_canonical,
            'tracking_id', v_tracking_id,
            'status', 'error',
            'code', 'no_drop_event',
            'identity_conflict', v_identity_conflict,
            'duration_estimate', 'unknown',
            'blockers', jsonb_build_array('no DROP event for selection')
        );
    END IF;

    SELECT m.manifest INTO v_manifest
    FROM flashback.drop_dependency_manifests m
    WHERE m.tracking_id = v_tracking_id
    ORDER BY m.captured_at DESC
    LIMIT 1;

    v_status := v_row.status;
    v_code := CASE
        WHEN v_identity_conflict THEN 'identity_conflict'
        WHEN v_status = 'restorable' THEN 'ok'
        WHEN v_row.reason ILIKE '%ambiguous%' THEN 'ambiguous_coverage_generation'
        WHEN v_row.reason ILIKE '%gap%' THEN 'coverage_gap'
        ELSE 'non_restorable'
    END;
    -- Prefer an operator-visible identity conflict reason over the DROP's
    -- coverage reason when the live OID no longer matches the tracked lifecycle.
    IF v_identity_conflict THEN
        v_status := 'non_restorable';
        v_row.reason := format(
            'identity conflict: live relation OID differs from tracked lifecycle OID for %s',
            v_canonical
        );
    END IF;

    v_token := flashback_sha256(format('%s|%s|%s|%s|%s|%s',
        v_plan_version,
        v_tracking_id,
        COALESCE(v_row.disaster_event_id::text, ''),
        COALESCE(v_row.generation_id::text, ''),
        COALESCE(v_row.safe_target_lsn::text, ''),
        v_canonical));

    RETURN jsonb_build_object(
        'schema_version', v_plan_version,
        'plan_version', v_plan_version,
        'plan_token', v_token,
        'selection', v_selection,
        'table_name', v_canonical,
        'tracking_id', v_tracking_id,
        'disaster_event_id', v_row.disaster_event_id,
        'generation_id', v_row.generation_id,
        'target_lsn', v_row.safe_target_lsn,
        'disaster_commit_lsn', v_row.disaster_commit_lsn,
        'disaster_time', v_row.disaster_time,
        'status', v_status,
        'code', v_code,
        'reason', v_row.reason,
        'identity_conflict', v_identity_conflict,
        'dependency_manifest', v_manifest,
        'duration_estimate', 'unknown',
        'note', 'plan_token is identity/stale guard only; RBAC authorizes execute; dry-run does not authorize'
    );
END;
$$;

-- Mutating execute: recompute plan under lock and compare token.
CREATE OR REPLACE FUNCTION flashback_recover_execute(
    p_table text,
    p_plan_token text,
    p_lookback interval DEFAULT interval '24 hours',
    p_disaster_event_id bigint DEFAULT NULL,
    p_at timestamptz DEFAULT NULL,
    p_lsn pg_lsn DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_plan jsonb;
    v_op bigint;
    v_rows bigint;
    v_token text;
    v_lsn pg_lsn;
    v_tracking_id bigint;
    v_table text;
BEGIN
    PERFORM flashback_require_primary('flashback_recover_execute');

    -- Serialize against concurrent recover/replan for the same lifecycle.
    SELECT tt.tracking_id INTO v_tracking_id
    FROM flashback.tracked_tables tt
    WHERE tt.recovery_profile = 'local_delta'
      AND (
          (to_regclass(p_table) IS NOT NULL AND tt.rel_oid = to_regclass(p_table))
          OR format('%I.%I', tt.schema_name, tt.table_name) = p_table
          OR (position('.' IN p_table) = 0 AND tt.table_name = p_table)
      )
    ORDER BY tt.is_active DESC, tt.tracked_since DESC
    LIMIT 1
    FOR UPDATE OF tt;

    IF v_tracking_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: no local_delta lifecycle for %', p_table
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- Fully recompute under lock; plan_token is stale-selection guard only.
    v_plan := flashback_recover_plan(p_table, p_lookback, p_disaster_event_id, p_at, p_lsn);
    v_token := v_plan->>'plan_token';
    IF v_token IS NULL OR v_token IS DISTINCT FROM p_plan_token THEN
        RAISE EXCEPTION 'pg_flashback: recover plan_token mismatch (stale selection); replan required'
            USING ERRCODE = 'serialization_failure',
                  HINT = 'Call flashback_recover_plan again and retry execute with the new plan_token.';
    END IF;
    IF COALESCE(v_plan->>'status', '') <> 'restorable' THEN
        RAISE EXCEPTION 'pg_flashback: recover plan is not restorable (%)',
            COALESCE(v_plan->>'code', 'unknown')
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    v_lsn := (v_plan->>'target_lsn')::pg_lsn;
    v_tracking_id := (v_plan->>'tracking_id')::bigint;
    v_table := v_plan->>'table_name';

    -- Attempt header in this transaction; verified is appended later by finalizer.
    v_op := flashback_operation_begin(
        'recover', v_table, v_tracking_id,
        (v_plan->>'plan_version')::integer, v_token,
        (v_plan->>'disaster_event_id')::bigint,
        (v_plan->>'generation_id')::bigint,
        v_lsn,
        jsonb_build_object('plan', v_plan)
    );

    BEGIN
        v_rows := flashback_restore_lsn(v_table, v_lsn);
        PERFORM flashback_operation_append_event(
            v_op, 'applied_coverage_pending', NULL, NULL,
            'restore applied; waiting for successor coverage',
            jsonb_build_object('rows_affected', v_rows)
        );
    EXCEPTION WHEN OTHERS THEN
        PERFORM flashback_operation_append_event(
            v_op, 'failed', SQLSTATE, 'restore_failed', SQLERRM,
            jsonb_build_object('sqlerrm', SQLERRM)
        );
        RAISE;
    END;

    RETURN jsonb_build_object(
        'schema_version', 1,
        'status', 'applied_coverage_pending',
        'code', 'ok',
        'operation_id', v_op,
        'table_name', v_table,
        'rows_affected', v_rows,
        'plan_token', v_token,
        'note', 'verified is written by worker/finalizer after healthy successor coverage'
    );
END;
$$;
