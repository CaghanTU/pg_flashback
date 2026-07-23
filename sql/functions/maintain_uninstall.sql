-- =================================================================
-- Operator maintenance and safe uninstall preparation (local_delta).
-- =================================================================

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
BEGIN
    IF NOT flashback_is_actively_protected(v_name) THEN
        RETURN jsonb_build_object(
            'schema_version', 1,
            'table_name', v_name,
            'status', 'error',
            'code', 'not_protected',
            'recommended', false,
            'required', false
        );
    END IF;

    v_health := flashback_lifecycle_health(v_name);
    SELECT EXISTS (
        SELECT 1 FROM flashback.operation_current_state s
        WHERE s.command IN ('recover', 'restore_lsn')
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
        'recommended', (v_health IS DISTINCT FROM 'healthy'),
        'required', false,
        'pending_restore', v_pending_restore,
        'generation_building', v_building,
        'capacity', CASE WHEN v_advise IS NULL THEN NULL ELSE to_jsonb(v_advise) END,
        'action', 'flashback_reanchor creates a new WAL-aligned base/generation; predecessor stays retained until successor is healthy',
        'note', 'dry-run only; execute via flashback_maintain_execute / CLI --yes'
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_maintain_execute(p_table text)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_plan jsonb;
    v_gen bigint;
BEGIN
    v_plan := flashback_maintain_plan(p_table);
    IF COALESCE(v_plan->>'status', '') <> 'ok' THEN
        RAISE EXCEPTION 'pg_flashback: maintain refused (%)', COALESCE(v_plan->>'code', 'unknown')
            USING ERRCODE = 'invalid_parameter_value',
                  DETAIL = v_plan::text;
    END IF;

    -- Reanchor must be first write in its transaction (checked inside).
    v_gen := flashback_reanchor(v_plan->>'table_name');
    RETURN jsonb_build_object(
        'schema_version', 1,
        'status', 'started',
        'code', 'ok',
        'table_name', v_plan->>'table_name',
        'new_generation_id', v_gen,
        'note', 'wait for successor coverage healthy before sealing/retiring predecessor'
    );
END;
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
            'roles', jsonb_build_array('flashback_admin', 'flashback_recovery_agent'),
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
    'Read-only maintain/reanchor plan with capacity and pending-restore guards.';
COMMENT ON FUNCTION flashback_maintain_execute(text) IS
    'Create a new WAL-aligned generation via flashback_reanchor after plan admission.';
COMMENT ON FUNCTION flashback_prepare_uninstall(boolean) IS
    'Fail-closed uninstall preparation: refuse active lifecycles/pending restores; drop orphan slots when idle.';
