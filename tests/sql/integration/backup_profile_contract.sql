-- Backup-backed profile contract: no row capture/base snapshot, immutable
-- helper request, manifest acceptance, imported-shadow verification and swap.

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
    v_target_lsn pg_lsn;
    v_request jsonb;
    v_request_id text;
    v_artifact_table text;
    v_artifact_oid oid;
    v_artifact_schema_hash text;
    v_fingerprint text;
    v_result jsonb;
    v_new_oid oid;
BEGIN
    PERFORM flashback_track_backup('public.it_backup_profile', 'test_helper');

    IF NOT EXISTS (
        SELECT 1 FROM flashback.tracked_tables
        WHERE rel_oid = v_old_oid
          AND recovery_profile = 'backup'
          AND helper_profile = 'test_helper'
          AND base_snapshot_table IS NULL
    ) THEN
        RAISE EXCEPTION 'backup profile metadata was not recorded correctly';
    END IF;
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
    IF length(flashback_helper_schema_sha256(v_old_oid)) <> 64 THEN
        RAISE EXCEPTION 'helper schema fingerprint is not SHA-256';
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

    SELECT applied_lsn INTO v_target_lsn
    FROM flashback.schema_versions
    WHERE rel_oid = v_old_oid AND schema_version = 1;
    PERFORM flashback_set_backup_coverage(
        'public.it_backup_profile',
        v_target_lsn,
        pg_current_wal_insert_lsn()
    );

    -- The normal request is created after the accident, when to_regclass()
    -- can no longer resolve the live relation. The backup metadata resolver
    -- must still find the exact schema-qualified tracked identity.
    DROP TABLE public.it_backup_profile;

    v_request := flashback_prepare_backup_restore(
        'public.it_backup_profile',
        v_target_lsn
    );
    v_request_id := v_request ->> 'request_id';
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
    IF (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = v_new_oid) <> session_user
       OR NOT has_table_privilege('flashback_admin', v_new_oid, 'SELECT')
    THEN
        RAISE EXCEPTION 'backup restore did not recover owner and ACL metadata';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM flashback.tracked_tables
        WHERE rel_oid = v_new_oid AND recovery_profile = 'backup'
    ) THEN
        RAISE EXCEPTION 'tracking metadata was not rebound to restored table';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM flashback.backup_restore_requests
        WHERE request_id = v_request_id AND status = 'completed'
    ) THEN
        RAISE EXCEPTION 'request did not reach completed state';
    END IF;
END;
$$;

DROP TABLE public.it_backup_profile CASCADE;

-- A table recreated under the same name after the request was prepared is a
-- different identity. Finalization must never overwrite it.
CREATE TABLE public.it_backup_identity (
    id integer PRIMARY KEY,
    note text NOT NULL
);
INSERT INTO public.it_backup_identity VALUES (1, 'original');

DO $$
DECLARE
    v_old_oid oid := 'public.it_backup_identity'::regclass;
    v_target_lsn pg_lsn;
    v_request jsonb;
    v_request_id text;
    v_artifact_table text;
    v_artifact_oid oid;
    v_artifact_schema_hash text;
    v_fingerprint text;
    v_result jsonb;
    v_refused boolean := false;
BEGIN
    PERFORM flashback_track_backup('public.it_backup_identity', 'test_helper');
    SELECT applied_lsn INTO v_target_lsn
    FROM flashback.schema_versions
    WHERE rel_oid = v_old_oid AND schema_version = 1;
    PERFORM flashback_set_backup_coverage(
        'public.it_backup_identity', v_target_lsn, pg_current_wal_insert_lsn()
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
    PERFORM flashback_untrack('it_backup_identity');
END;
$$;
