-- Retained coverage lower bound and post_restore retained rejection.

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
    v_failed boolean;
    v_request jsonb;
BEGIN
    DROP TABLE IF EXISTS public.it_route_a CASCADE;
    CREATE TABLE public.it_route_a (id int PRIMARY KEY, note text NOT NULL);
    INSERT INTO public.it_route_a VALUES (1, 'seed');

    v_old_oid := 'public.it_route_a'::regclass;
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
        v_tracking_id, v_old_oid, 'public', 'it_route_a', NULL,
        1, 'backup', 'route_helper',
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

    v_proof_id := flashback_install_verified_backup_proof(
        'route-retained-ok', v_tracking_id, 'route_helper', 'repo', 'stanza',
        '20260717RouteRF', v_sysid, v_timeline, 'manifest-r', repeat('dd', 32),
        v_marker_lsn - 40, v_marker_lsn - 20,
        clock_timestamp(),
        jsonb_build_object(
            'activation_mode', 'retained_full_plus_wal',
            'wal_verified_through_lsn', (v_marker_lsn + 50)::text
        )
    );
    v_generation_id := flashback_consume_verified_backup_proof(v_proof_id);

    -- Target between FULL stop and marker must be rejected.
    v_failed := false;
    BEGIN
        v_request := flashback_prepare_backup_restore(
            'public.it_route_a', v_marker_lsn - 10
        );
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'pre-marker retained target must be rejected, got %', v_request;
    END IF;

    -- Target at/after marker within valid_through is admitted.
    v_request := flashback_prepare_backup_restore(
        'public.it_route_a', v_marker_lsn + 10
    );
    IF (v_request->>'generation_id')::bigint IS DISTINCT FROM v_generation_id THEN
        RAISE EXCEPTION 'unexpected generation for in-coverage target';
    END IF;

    -- post_restore building cannot consume retained proof.
    UPDATE flashback.coverage_generations
       SET state = 'sealed',
           sealed_at = clock_timestamp(),
           superseded_before_lsn = v_marker_lsn + 50
     WHERE generation_id = v_generation_id;
    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, stream_id, recovery_profile, state,
        boundary_kind, rel_oid_at_boundary, boundary_xid, boundary_marker,
        details
    ) VALUES (
        v_tracking_id, 2, NULL, 'backup', 'building',
        'post_restore', v_old_oid, (txid_current() % 4294967296)::bigint,
        format('post-restore:%s', v_tracking_id),
        jsonb_build_object('tracking_marker_lsn', v_marker_lsn + 60)
    );
    v_failed := false;
    BEGIN
        v_proof_id := flashback_install_verified_backup_proof(
            'route-post-retained-bad', v_tracking_id, 'route_helper', 'repo', 'stanza',
            '20260717RouteBadF', v_sysid, v_timeline, 'manifest-bad', repeat('ee', 32),
            v_marker_lsn + 20, v_marker_lsn + 40,
            clock_timestamp(),
            jsonb_build_object(
                'activation_mode', 'retained_full_plus_wal',
                'wal_verified_through_lsn', (v_marker_lsn + 80)::text
            )
        );
        PERFORM flashback_consume_verified_backup_proof(v_proof_id);
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'post_restore must reject retained_full_plus_wal';
    END IF;
END;
$$;
