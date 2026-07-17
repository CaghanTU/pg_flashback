-- Retained FULL + continuous WAL activation contract (SQL-level).
-- Proves dual-path consume, overlapping rejection, and fail-closed WAL through.

DO $$
DECLARE
    v_tracking_id bigint;
    v_generation_id bigint;
    v_marker_lsn pg_lsn;
    v_sysid numeric;
    v_timeline bigint;
    v_proof_id bigint;
    v_old_oid oid;
    v_schema_def jsonb;
    v_schema_hash text;
    v_mode text;
    v_boundary pg_lsn;
    v_valid_through pg_lsn;
BEGIN
    DROP TABLE IF EXISTS public.it_retained_a CASCADE;
    CREATE TABLE public.it_retained_a (id int PRIMARY KEY, note text NOT NULL);
    INSERT INTO public.it_retained_a VALUES (1, 'before-track');

    v_old_oid := 'public.it_retained_a'::regclass;
    v_tracking_id := nextval('flashback.tracking_id_seq');
    v_marker_lsn := pg_current_wal_insert_lsn();
    v_schema_def := COALESCE(flashback_collect_schema_def(v_old_oid), '{}'::jsonb);
    v_schema_hash := flashback_helper_schema_sha256(v_old_oid);
    SELECT system_identifier INTO v_sysid FROM pg_control_system();
    SELECT timeline_id INTO v_timeline FROM pg_control_checkpoint();

    INSERT INTO flashback.tracked_tables (
        tracking_id, rel_oid, schema_name, table_name, base_snapshot_table,
        schema_version, recovery_profile, helper_profile,
        coverage_start_lsn, coverage_end_lsn,
        tracked_since, checkpoint_interval, retention_interval, is_active
    ) VALUES (
        v_tracking_id, v_old_oid, 'public', 'it_retained_a', NULL,
        1, 'backup', 'retained_helper',
        NULL, NULL,
        clock_timestamp(), interval '15 minutes', interval '7 days', true
    );
    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, stream_id, recovery_profile, state,
        boundary_kind, rel_oid_at_boundary, boundary_xid, boundary_marker,
        details
    ) VALUES (
        v_tracking_id, 1, NULL, 'backup', 'building',
        'initial_track', v_old_oid, (txid_current() % 4294967296)::bigint,
        format('initial-backup-track:%s:seed', v_tracking_id),
        jsonb_build_object('tracking_marker_lsn', v_marker_lsn)
    );
    INSERT INTO flashback.schema_versions (
        rel_oid, tracking_id, generation_id, schema_version,
        applied_at, applied_lsn, columns, primary_key, constraints,
        helper_schema_sha256
    )
    SELECT
        v_old_oid, v_tracking_id, cg.generation_id, 1,
        clock_timestamp(), v_marker_lsn,
        COALESCE(v_schema_def -> 'columns', '[]'::jsonb),
        COALESCE(v_schema_def -> 'primary_key', '[]'::jsonb),
        jsonb_build_object(
            'check_unique_fk', COALESCE(v_schema_def -> 'constraints', '[]'::jsonb),
            'indexes', COALESCE(v_schema_def -> 'indexes', '[]'::jsonb),
            'triggers', '[]'::jsonb,
            'rls_policies', '[]'::jsonb,
            'rls_enabled', false
        ),
        v_schema_hash
    FROM flashback.coverage_generations cg
    WHERE cg.tracking_id = v_tracking_id;

    -- Overlapping FULL (start <= marker < stop) must not activate as retained.
    BEGIN
        v_proof_id := flashback_install_verified_backup_proof(
            'retained-overlap', v_tracking_id, 'retained_helper', 'repo', 'stanza',
            '20260717OverlapF', v_sysid, v_timeline, 'manifest-o', repeat('aa', 32),
            v_marker_lsn - 1, v_marker_lsn + 10,
            clock_timestamp(),
            jsonb_build_object(
                'activation_mode', 'retained_full_plus_wal',
                'wal_verified_through_lsn', (v_marker_lsn + 20)::text
            )
        );
        PERFORM flashback_consume_verified_backup_proof(v_proof_id);
        RAISE EXCEPTION 'overlapping retained FULL must be rejected';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM = 'overlapping retained FULL must be rejected' THEN
            RAISE;
        END IF;
    END;

    -- Retained without wal_verified_through_lsn fails closed.
    BEGIN
        v_proof_id := flashback_install_verified_backup_proof(
            'retained-no-wal', v_tracking_id, 'retained_helper', 'repo', 'stanza',
            '20260717NoWalF', v_sysid, v_timeline, 'manifest-n', repeat('bb', 32),
            v_marker_lsn - 40, v_marker_lsn - 20,
            clock_timestamp(),
            jsonb_build_object('activation_mode', 'retained_full_plus_wal')
        );
        PERFORM flashback_consume_verified_backup_proof(v_proof_id);
        RAISE EXCEPTION 'retained FULL without WAL through must be rejected';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM = 'retained FULL without WAL through must be rejected' THEN
            RAISE;
        END IF;
    END;

    -- Eligible retained FULL + WAL through marker activates coverage at marker.
    v_proof_id := flashback_install_verified_backup_proof(
        'retained-ok', v_tracking_id, 'retained_helper', 'repo', 'stanza',
        '20260717RetainedF', v_sysid, v_timeline, 'manifest-r', repeat('cc', 32),
        v_marker_lsn - 40, v_marker_lsn - 20,
        clock_timestamp(),
        jsonb_build_object(
            'activation_mode', 'retained_full_plus_wal',
            'wal_verified_through_lsn', (v_marker_lsn + 30)::text,
            'dependency_pin_id', 'test-pin',
            'required_dependencies', jsonb_build_array(
                jsonb_build_object('kind', 'full_backup', 'label', '20260717RetainedF'),
                jsonb_build_object(
                    'kind', 'archived_wal_range',
                    'start_lsn', (v_marker_lsn - 20)::text,
                    'stop_lsn', (v_marker_lsn + 30)::text
                )
            )
        )
    );
    v_generation_id := flashback_consume_verified_backup_proof(v_proof_id);

    SELECT cg.details->>'activation_mode', cg.boundary_lsn, cg.valid_through_lsn
      INTO v_mode, v_boundary, v_valid_through
    FROM flashback.coverage_generations cg
    WHERE cg.generation_id = v_generation_id;

    IF v_mode IS DISTINCT FROM 'retained_full_plus_wal' THEN
        RAISE EXCEPTION 'activation_mode not retained: %', v_mode;
    END IF;
    IF v_boundary IS DISTINCT FROM (v_marker_lsn - 20) THEN
        RAISE EXCEPTION 'retained boundary must equal backup_stop %, got %',
            v_marker_lsn - 20, v_boundary;
    END IF;
    IF v_valid_through IS DISTINCT FROM (v_marker_lsn + 30) THEN
        RAISE EXCEPTION 'valid_through must be WAL frontier, got %', v_valid_through;
    END IF;
    IF (
        SELECT coverage_start_lsn FROM flashback.tracked_tables
        WHERE tracking_id = v_tracking_id
    ) IS DISTINCT FROM v_marker_lsn THEN
        RAISE EXCEPTION 'tracked coverage_start must be marker for retained activation';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM flashback.backup_anchors
        WHERE tracking_id = v_tracking_id
          AND backup_label = '20260717RetainedF'
          AND backup_stop_lsn = v_marker_lsn - 20
          AND backup_stop_lsn <= tracking_marker_lsn
    ) THEN
        RAISE EXCEPTION 'retained backup_anchors row missing or ineligible';
    END IF;
    IF (
        SELECT recommended_action FROM flashback_health()
        WHERE tracking_id = v_tracking_id
    ) IS DISTINCT FROM 'none'
       AND (
        SELECT health FROM flashback_health()
        WHERE tracking_id = v_tracking_id
    ) IS DISTINCT FROM 'healthy'
    THEN
        -- healthy with optional fresher-anchor advice is acceptable; anything
        -- else is a regression.
        IF NOT EXISTS (
            SELECT 1 FROM flashback_health()
            WHERE tracking_id = v_tracking_id
              AND health = 'healthy'
        ) THEN
            RAISE EXCEPTION 'retained activation must report healthy coverage';
        END IF;
    END IF;

    -- Active pin protects expire.
    IF (
        SELECT flashback_begin_backup_expire('retained_helper', 'repo', 'stanza')
            ->> 'status'
    ) IS DISTINCT FROM 'protected' THEN
        RAISE EXCEPTION 'active retained FULL must protect expire';
    END IF;
END;
$$;
