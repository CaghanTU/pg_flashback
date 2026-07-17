-- Backup coverage contract (pgrx harness).
--
-- The harness runs each test in one write-dirty transaction, so a successful
-- flashback_track_backup() (dedicated-txn + BOUNDARY COMMIT resolve) is
-- impossible here by design. This file verifies:
--   1. track_backup fails closed on a write-dirty transaction
--   2. legacy set_backup_coverage is rejected
--   3. activate / prepare / finalize against a seeded building marker + FULL
--      anchor, including post-swap zero-active unanchored successor behavior
--   4. identity protection on finalize
--
-- Real marker COMMIT resolution and worker-driven post-swap sealing run in
-- scripts/run_backup_coverage_e2e.sh.

DO $tv$
DECLARE
    v_raised boolean := false;
BEGIN
    DROP TABLE IF EXISTS public.it_backup_failclosed CASCADE;
    CREATE TABLE public.it_backup_failclosed (id int PRIMARY KEY, note text NOT NULL);
    BEGIN
        PERFORM flashback_track_backup('public.it_backup_failclosed', 'test_helper');
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM LIKE '%dedicated transaction%' THEN
            v_raised := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'flashback_track_backup must fail closed in a write-dirty transaction';
    END IF;
    IF EXISTS (
        SELECT 1 FROM flashback.tracked_tables
        WHERE table_name = 'it_backup_failclosed' AND is_active
    ) THEN
        RAISE EXCEPTION 'failed track_backup must not leave an active binding';
    END IF;
    DROP TABLE public.it_backup_failclosed;
END;
$tv$;

DROP TABLE IF EXISTS public.it_backup_profile CASCADE;

CREATE TABLE public.it_backup_profile (
    id integer PRIMARY KEY,
    note text NOT NULL
);
INSERT INTO public.it_backup_profile VALUES (1, 'before'), (2, 'keep');
GRANT SELECT ON public.it_backup_profile TO flashback_admin;

DO $$
DECLARE
    v_old_oid oid := 'public.it_backup_profile'::regclass;
    v_tracking_id bigint;
    v_generation_id bigint;
    v_target_lsn pg_lsn;
    v_marker_lsn pg_lsn;
    v_request jsonb;
    v_request_id text;
    v_artifact_table text;
    v_artifact_oid oid;
    v_artifact_schema_hash text;
    v_fingerprint text;
    v_result jsonb;
    v_new_oid oid;
    v_sysid numeric;
    v_timeline bigint;
    v_active_count integer;
    v_schema_def jsonb;
    v_schema_hash text;
BEGIN
    BEGIN
        PERFORM flashback_set_backup_coverage(
            'public.it_backup_profile', '0/1'::pg_lsn, '0/2'::pg_lsn
        );
        RAISE EXCEPTION 'legacy set_backup_coverage must fail closed';
    EXCEPTION WHEN feature_not_supported THEN
        NULL;
    END;

    -- Seed the immutable lifecycle + resolved building marker that a dedicated
    -- flashback_track_backup() + consume_wal path would leave before activation.
    v_tracking_id := nextval('flashback.tracking_id_seq');
    v_marker_lsn := pg_current_wal_insert_lsn();
    v_schema_def := COALESCE(flashback_collect_schema_def(v_old_oid), '{}'::jsonb);
    v_schema_hash := flashback_helper_schema_sha256(v_old_oid);

    INSERT INTO flashback.tracked_tables (
        tracking_id, rel_oid, schema_name, table_name, base_snapshot_table,
        schema_version, recovery_profile, helper_profile,
        coverage_start_lsn, coverage_end_lsn,
        tracked_since, checkpoint_interval, retention_interval, is_active
    ) VALUES (
        v_tracking_id, v_old_oid, 'public', 'it_backup_profile', NULL,
        1, 'backup', 'test_helper',
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
    ) RETURNING generation_id INTO v_generation_id;

    INSERT INTO flashback.schema_versions (
        rel_oid, tracking_id, generation_id, schema_version,
        applied_at, applied_lsn, columns, primary_key, constraints,
        helper_schema_sha256
    ) VALUES (
        v_old_oid, v_tracking_id, v_generation_id, 1,
        clock_timestamp(), v_marker_lsn,
        COALESCE(v_schema_def -> 'columns', '[]'::jsonb),
        COALESCE(v_schema_def -> 'primary_key', '[]'::jsonb),
        jsonb_build_object(
            'check_unique_fk', COALESCE(v_schema_def -> 'constraints', '[]'::jsonb),
            'indexes', COALESCE(v_schema_def -> 'indexes', '[]'::jsonb),
            'triggers', COALESCE(v_schema_def -> 'triggers', '[]'::jsonb),
            'rls_policies', COALESCE(v_schema_def -> 'rls_policies', '[]'::jsonb),
            'rls_enabled', false
        ),
        v_schema_hash
    );

    IF EXISTS (SELECT 1 FROM flashback.snapshots WHERE rel_oid = v_old_oid) THEN
        RAISE EXCEPTION 'backup profile must not create row snapshots';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = v_old_oid
          AND NOT tgisinternal
          AND tgname LIKE 'flashback_capture%'
    ) THEN
        RAISE EXCEPTION 'backup profile must not install DML capture triggers';
    END IF;

    UPDATE public.it_backup_profile SET note = 'not-captured' WHERE id = 2;
    PERFORM flashback_flush_staging(1000);
    IF EXISTS (
        SELECT 1 FROM flashback.delta_log
        WHERE rel_oid = v_old_oid AND event_type IN ('INSERT', 'UPDATE', 'DELETE')
    ) THEN
        RAISE EXCEPTION 'backup profile duplicated row DML into delta_log';
    END IF;
    UPDATE public.it_backup_profile SET note = 'keep' WHERE id = 2;

    SELECT system_identifier INTO v_sysid FROM pg_control_system();
    SELECT timeline_id INTO v_timeline FROM pg_control_checkpoint();
    v_target_lsn := v_marker_lsn + 100;
    v_generation_id := flashback_activate_backup_anchor(
        'public.it_backup_profile',
        'test-repo',
        'test-stanza',
        '20260717-000001F',
        v_sysid,
        v_timeline,
        'backup/20260717-000001F/backup.manifest',
        repeat('ab', 32),
        v_marker_lsn + 1,
        v_target_lsn
    );
    IF NOT EXISTS (
        SELECT 1 FROM flashback.coverage_generations
        WHERE generation_id = v_generation_id
          AND state = 'active'
          AND backup_anchor_id IS NOT NULL
          AND valid_through_lsn = v_target_lsn
    ) THEN
        RAISE EXCEPTION 'backup generation was not activated at the verified stop LSN';
    END IF;

    BEGIN
        PERFORM flashback_activate_backup_anchor(
            'public.it_backup_profile',
            'test-repo',
            'test-stanza',
            '20260717-overlapF',
            v_sysid,
            v_timeline,
            'backup/20260717-overlapF/backup.manifest',
            repeat('cd', 32),
            v_marker_lsn,
            v_marker_lsn + 10
        );
        RAISE EXCEPTION 'overlapping backup start must be rejected';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM = 'overlapping backup start must be rejected' THEN
            RAISE;
        END IF;
    END;

    DROP TABLE public.it_backup_profile;

    v_request := flashback_prepare_backup_restore(
        'public.it_backup_profile',
        v_target_lsn
    );
    v_request_id := v_request ->> 'request_id';
    IF v_request ->> 'generation_id' IS NULL THEN
        RAISE EXCEPTION 'prepare must pin an admitted generation';
    END IF;
    IF flashback_claim_backup_restore(v_request_id) <> v_request THEN
        RAISE EXCEPTION 'claimed helper request differs from prepared contract';
    END IF;

    v_artifact_table := 'r_' || left(flashback_sha256(v_request_id), 16);
    EXECUTE format(
        'CREATE TABLE flashback_import.%I (id integer PRIMARY KEY, note text NOT NULL)',
        v_artifact_table
    );
    EXECUTE format(
        'INSERT INTO flashback_import.%I VALUES (1, %L), (2, %L)',
        v_artifact_table, 'before', 'keep'
    );
    v_artifact_oid := to_regclass(format('flashback_import.%I', v_artifact_table));
    v_artifact_schema_hash := flashback_helper_schema_sha256(v_artifact_oid);
    EXECUTE format(
        'SELECT count(*)::text || ''|'' || COALESCE(bit_xor(hashtextextended(row_to_json(t)::text, 0)), 0)::text FROM flashback_import.%I AS t',
        v_artifact_table
    ) INTO v_fingerprint;

    v_result := jsonb_build_object(
        'result_format_version', 3,
        'helper_version', '0.1.0',
        'status', 'completed',
        'request', v_request,
        'profile', 'test_helper',
        'cleanup_complete', true,
        'recovered_owner', session_user,
        'recovered_acl', jsonb_build_array(jsonb_build_object(
            'grantee', 'flashback_admin',
            'privilege', 'SELECT',
            'is_grantable', false
        )),
        'recovered_schema_sha256', v_request ->> 'expected_schema_sha256',
        'artifact_schema', 'flashback_import',
        'artifact_table', v_artifact_table,
        'artifact_schema_sha256', v_artifact_schema_hash,
        'artifact_sha256', repeat('a', 64),
        'recovered_fingerprint', v_fingerprint
    );
    BEGIN
        PERFORM flashback_accept_backup_restore(v_request_id, v_result - 'request');
        RAISE EXCEPTION 'manifest without immutable request was accepted';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM = 'manifest without immutable request was accepted' THEN
            RAISE;
        END IF;
    END;
    PERFORM flashback_accept_backup_restore(v_request_id, v_result);

    v_new_oid := flashback_finalize_backup_restore(v_request_id);
    IF v_new_oid IS NULL OR v_new_oid = v_old_oid THEN
        RAISE EXCEPTION 'backup restore did not install a new table identity';
    END IF;
    IF (SELECT array_agg(note ORDER BY id) FROM public.it_backup_profile)
       <> ARRAY['before', 'keep']::text[] THEN
        RAISE EXCEPTION 'backup restore did not recover expected rows';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM flashback.coverage_generations
        WHERE recovery_profile = 'backup'
          AND state = 'building'
          AND boundary_kind = 'post_restore'
          AND rel_oid_at_boundary = v_new_oid
    ) THEN
        RAISE EXCEPTION 'backup finalize must leave a building unanchored successor';
    END IF;

    UPDATE flashback.coverage_generations
       SET state_reason = 'post_restore_unanchored',
           details = COALESCE(details, '{}'::jsonb)
               || jsonb_build_object(
                   'tracking_marker_lsn', pg_current_wal_insert_lsn()
               )
     WHERE recovery_profile = 'backup'
       AND state = 'building'
       AND boundary_kind = 'post_restore';

    UPDATE flashback.coverage_generations parent
       SET state = 'sealed',
           superseded_before_lsn = (child.details ->> 'tracking_marker_lsn')::pg_lsn,
           superseded_before_time = clock_timestamp(),
           sealed_at = clock_timestamp(),
           state_reason = 'successor_boundary_resolved'
      FROM flashback.coverage_generations child
     WHERE child.recovery_profile = 'backup'
       AND child.state = 'building'
       AND child.boundary_kind = 'post_restore'
       AND parent.generation_id = child.parent_generation_id
       AND parent.state = 'active';

    INSERT INTO flashback.coverage_gaps (
        tracking_id, source_generation_id, reason,
        gap_start_lsn, gap_start_time, lower_bound_inclusive, details
    )
    SELECT
        child.tracking_id, child.parent_generation_id, 'post_restore_unanchored',
        (child.details ->> 'tracking_marker_lsn')::pg_lsn, clock_timestamp(), false,
        jsonb_build_object('successor_generation_id', child.generation_id)
    FROM flashback.coverage_generations child
    WHERE child.recovery_profile = 'backup'
      AND child.state = 'building'
      AND child.boundary_kind = 'post_restore'
      AND NOT EXISTS (
          SELECT 1 FROM flashback.coverage_gaps g
          WHERE g.tracking_id = child.tracking_id
            AND g.reason = 'post_restore_unanchored'
            AND g.reanchored_by_generation_id IS NULL
      );

    SELECT count(*) INTO v_active_count
    FROM flashback.coverage_generations
    WHERE tracking_id = v_tracking_id
      AND state = 'active';
    IF v_active_count <> 0 THEN
        RAISE EXCEPTION 'post-swap backup state must have zero active generations';
    END IF;

    BEGIN
        PERFORM flashback_prepare_backup_restore(
            'public.it_backup_profile',
            (
                SELECT (details ->> 'tracking_marker_lsn')::pg_lsn + 1
                  FROM flashback.coverage_generations
                 WHERE state = 'building' AND boundary_kind = 'post_restore'
                 ORDER BY generation_id DESC LIMIT 1
            )
        );
        RAISE EXCEPTION 'unanchored post-swap target must be rejected';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM = 'unanchored post-swap target must be rejected' THEN
            RAISE;
        END IF;
    END;
END;
$$;

DROP TABLE public.it_backup_profile CASCADE;

CREATE TABLE public.it_backup_identity (
    id integer PRIMARY KEY,
    note text NOT NULL
);
INSERT INTO public.it_backup_identity VALUES (1, 'original');

DO $$
DECLARE
    v_old_oid oid := 'public.it_backup_identity'::regclass;
    v_tracking_id bigint;
    v_generation_id bigint;
    v_target_lsn pg_lsn;
    v_marker_lsn pg_lsn;
    v_request jsonb;
    v_request_id text;
    v_artifact_table text;
    v_artifact_oid oid;
    v_artifact_schema_hash text;
    v_fingerprint text;
    v_result jsonb;
    v_refused boolean := false;
    v_sysid numeric;
    v_timeline bigint;
    v_schema_def jsonb;
    v_schema_hash text;
BEGIN
    v_tracking_id := nextval('flashback.tracking_id_seq');
    v_marker_lsn := pg_current_wal_insert_lsn();
    v_schema_def := COALESCE(flashback_collect_schema_def(v_old_oid), '{}'::jsonb);
    v_schema_hash := flashback_helper_schema_sha256(v_old_oid);

    INSERT INTO flashback.tracked_tables (
        tracking_id, rel_oid, schema_name, table_name, base_snapshot_table,
        schema_version, recovery_profile, helper_profile,
        coverage_start_lsn, coverage_end_lsn,
        tracked_since, checkpoint_interval, retention_interval, is_active
    ) VALUES (
        v_tracking_id, v_old_oid, 'public', 'it_backup_identity', NULL,
        1, 'backup', 'test_helper',
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
    ) RETURNING generation_id INTO v_generation_id;

    INSERT INTO flashback.schema_versions (
        rel_oid, tracking_id, generation_id, schema_version,
        applied_at, applied_lsn, columns, primary_key, constraints,
        helper_schema_sha256
    ) VALUES (
        v_old_oid, v_tracking_id, v_generation_id, 1,
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
    );

    SELECT system_identifier INTO v_sysid FROM pg_control_system();
    SELECT timeline_id INTO v_timeline FROM pg_control_checkpoint();
    v_target_lsn := v_marker_lsn + 50;
    PERFORM flashback_activate_backup_anchor(
        'public.it_backup_identity',
        'test-repo',
        'test-stanza',
        '20260717-identityF',
        v_sysid,
        v_timeline,
        'backup/20260717-identityF/backup.manifest',
        repeat('ef', 32),
        v_marker_lsn + 1,
        v_target_lsn
    );
    v_request := flashback_prepare_backup_restore(
        'public.it_backup_identity', v_target_lsn
    );
    v_request_id := v_request ->> 'request_id';
    PERFORM flashback_claim_backup_restore(v_request_id);

    v_artifact_table := 'r_' || left(flashback_sha256(v_request_id), 16);
    EXECUTE format(
        'CREATE TABLE flashback_import.%I (id integer PRIMARY KEY, note text NOT NULL)',
        v_artifact_table
    );
    EXECUTE format(
        'INSERT INTO flashback_import.%I VALUES (1, %L)',
        v_artifact_table, 'original'
    );
    v_artifact_oid := to_regclass(format('flashback_import.%I', v_artifact_table));
    v_artifact_schema_hash := flashback_helper_schema_sha256(v_artifact_oid);
    EXECUTE format(
        'SELECT count(*)::text || ''|'' || COALESCE(bit_xor(hashtextextended(row_to_json(t)::text, 0)), 0)::text FROM flashback_import.%I AS t',
        v_artifact_table
    ) INTO v_fingerprint;
    v_result := jsonb_build_object(
        'result_format_version', 3,
        'helper_version', '0.1.0',
        'status', 'completed',
        'request', v_request,
        'profile', 'test_helper',
        'cleanup_complete', true,
        'recovered_owner', session_user,
        'recovered_acl', '[]'::jsonb,
        'recovered_schema_sha256', v_request ->> 'expected_schema_sha256',
        'artifact_schema', 'flashback_import',
        'artifact_table', v_artifact_table,
        'artifact_schema_sha256', v_artifact_schema_hash,
        'artifact_sha256', repeat('b', 64),
        'recovered_fingerprint', v_fingerprint
    );
    PERFORM flashback_accept_backup_restore(v_request_id, v_result);

    DROP TABLE public.it_backup_identity;
    CREATE TABLE public.it_backup_identity (id integer PRIMARY KEY, note text NOT NULL);
    INSERT INTO public.it_backup_identity VALUES (99, 'unrelated');

    BEGIN
        PERFORM flashback_finalize_backup_restore(v_request_id);
    EXCEPTION WHEN OTHERS THEN
        IF position('live table identity changed' IN SQLERRM) > 0 THEN
            v_refused := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_refused THEN
        RAISE EXCEPTION 'replacement table was overwritten by backup finalization';
    END IF;
    IF (SELECT note FROM public.it_backup_identity WHERE id = 99) <> 'unrelated' THEN
        RAISE EXCEPTION 'replacement table contents changed';
    END IF;

    DROP TABLE public.it_backup_identity;
    PERFORM flashback_drop_payload_table(
        to_regclass(format('flashback_import.%I', v_artifact_table))
    );
END;
$$;
