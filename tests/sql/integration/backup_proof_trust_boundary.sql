-- Adversarial coverage for the verified backup proof trust boundary.
-- Raw activate/advance stay closed; proofs are one-time and lifecycle-bound.

DO $$
DECLARE
    v_tracking_id bigint;
    v_other_tracking_id bigint;
    v_generation_id bigint;
    v_marker_lsn pg_lsn;
    v_sysid numeric;
    v_timeline bigint;
    v_proof_id bigint;
    v_frontier jsonb;
    v_old_oid oid;
    v_schema_def jsonb;
    v_schema_hash text;
    v_failed boolean;
    v_expire_lease jsonb;
BEGIN
    DROP TABLE IF EXISTS public.it_proof_a CASCADE;
    DROP TABLE IF EXISTS public.it_proof_b CASCADE;
    CREATE TABLE public.it_proof_a (id int PRIMARY KEY, note text NOT NULL);
    CREATE TABLE public.it_proof_b (id int PRIMARY KEY, note text NOT NULL);
    INSERT INTO public.it_proof_a VALUES (1, 'a');
    INSERT INTO public.it_proof_b VALUES (1, 'b');

    -- flashback_admin must not EXECUTE proof installation (recovery_agent only).
    PERFORM set_config('role', 'flashback_admin', true);
    BEGIN
        PERFORM flashback_install_verified_backup_proof(
            'proof-admin-forbidden', 1, 'x', 'r', 's', 'L',
            1, 1, 'm', repeat('aa', 32), '0/1'::pg_lsn, '0/2'::pg_lsn
        );
        RAISE EXCEPTION 'flashback_admin must not install backup proofs';
    EXCEPTION WHEN insufficient_privilege THEN
        NULL;
    END;
    PERFORM set_config('role', 'none', true);

    BEGIN
        PERFORM flashback_activate_backup_anchor(
            'public.it_proof_a', 'r', 's', 'L', 1, 1, 'm',
            repeat('aa', 32), '0/1'::pg_lsn, '0/2'::pg_lsn
        );
        RAISE EXCEPTION 'raw activate must remain feature_not_supported';
    EXCEPTION WHEN feature_not_supported THEN
        NULL;
    END;

    BEGIN
        PERFORM flashback_advance_backup_frontier('public.it_proof_a', '0/2'::pg_lsn, 1);
        RAISE EXCEPTION 'raw frontier advance must remain feature_not_supported';
    EXCEPTION WHEN feature_not_supported THEN
        NULL;
    END;

    v_old_oid := 'public.it_proof_a'::regclass;
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
        v_tracking_id, v_old_oid, 'public', 'it_proof_a', NULL,
        1, 'backup', 'proof_helper',
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

    -- Fake digest shape is rejected at install.
    BEGIN
        PERFORM flashback_install_verified_backup_proof(
            'proof-bad-digest', v_tracking_id, 'proof_helper', 'repo', 'stanza',
            'badDigestF', v_sysid, v_timeline, 'manifest', 'not-a-sha',
            v_marker_lsn + 1, v_marker_lsn + 20
        );
        RAISE EXCEPTION 'invalid manifest digest must be rejected';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM = 'invalid manifest digest must be rejected' THEN
            RAISE;
        END IF;
    END;

    -- Profile mismatch is rejected.
    BEGIN
        PERFORM flashback_install_verified_backup_proof(
            'proof-bad-profile', v_tracking_id, 'other_profile', 'repo', 'stanza',
            'badProfileF', v_sysid, v_timeline, 'manifest', repeat('11', 32),
            v_marker_lsn + 1, v_marker_lsn + 20
        );
        RAISE EXCEPTION 'helper profile mismatch must be rejected';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM = 'helper profile mismatch must be rejected' THEN
            RAISE;
        END IF;
    END;

    v_proof_id := flashback_install_verified_backup_proof(
        'proof-ok-a', v_tracking_id, 'proof_helper', 'repo', 'stanza',
        '20260717ProofAF', v_sysid, v_timeline, 'manifest-a', repeat('22', 32),
        v_marker_lsn + 1, v_marker_lsn + 50
    );

    -- Expire admission and generation activation are serialized in the
    -- database, not only by a helper-local file lock. The durable lease must
    -- survive transactions and block activation until explicitly completed.
    v_expire_lease := flashback_begin_backup_expire('proof_helper', 'repo', 'stanza');
    IF v_expire_lease ->> 'status' IS DISTINCT FROM 'started' THEN
        RAISE EXCEPTION 'expire lease did not start: %', v_expire_lease;
    END IF;
    BEGIN
        PERFORM flashback_consume_verified_backup_proof(v_proof_id);
        RAISE EXCEPTION 'active expire lease must block generation activation';
    EXCEPTION WHEN object_in_use THEN
        NULL;
    END;
    PERFORM flashback_complete_backup_expire(
        (v_expire_lease ->> 'lease_id')::bigint,
        'proof_helper', 'repo', 'stanza'
    );
    v_generation_id := flashback_consume_verified_backup_proof(v_proof_id);
    IF NOT EXISTS (
        SELECT 1 FROM flashback.coverage_generations
        WHERE generation_id = v_generation_id AND state = 'active'
    ) THEN
        RAISE EXCEPTION 'verified proof did not activate coverage';
    END IF;

    -- One-time consume.
    BEGIN
        PERFORM flashback_consume_verified_backup_proof(v_proof_id);
        RAISE EXCEPTION 'consumed proof must not be reusable';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM = 'consumed proof must not be reusable' THEN
            RAISE;
        END IF;
    END;

    -- Same verification_request_id cannot be installed twice.
    BEGIN
        PERFORM flashback_install_verified_backup_proof(
            'proof-ok-a', v_tracking_id, 'proof_helper', 'repo', 'stanza',
            '20260717ProofA2F', v_sysid, v_timeline, 'manifest-a2', repeat('33', 32),
            v_marker_lsn + 1, v_marker_lsn + 60
        );
        RAISE EXCEPTION 'duplicate verification_request_id must be rejected';
    EXCEPTION WHEN unique_violation THEN
        NULL;
    END;

    -- Seed a second lifecycle and refuse cross-lifecycle consumption by
    -- installing a proof for B that references A's tracking id only via
    -- mismatched table identity checks on consume (proof is bound to tracking_id).
    v_other_tracking_id := nextval('flashback.tracking_id_seq');
    INSERT INTO flashback.tracked_tables (
        tracking_id, rel_oid, schema_name, table_name, base_snapshot_table,
        schema_version, recovery_profile, helper_profile,
        coverage_start_lsn, coverage_end_lsn,
        tracked_since, checkpoint_interval, retention_interval, is_active
    ) VALUES (
        v_other_tracking_id, 'public.it_proof_b'::regclass, 'public', 'it_proof_b', NULL,
        1, 'backup', 'proof_helper',
        NULL, NULL,
        clock_timestamp(), interval '15 minutes', interval '7 days', true
    );
    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, stream_id, recovery_profile, state,
        boundary_kind, rel_oid_at_boundary, boundary_xid, boundary_marker,
        details
    ) VALUES (
        v_other_tracking_id, 1, NULL, 'backup', 'building',
        'initial_track', 'public.it_proof_b'::regclass,
        (txid_current() % 4294967296)::bigint,
        format('initial-backup-track:%s:seed', v_other_tracking_id),
        jsonb_build_object('tracking_marker_lsn', v_marker_lsn)
    );

    -- Proof for B cannot activate while A already holds the only building? B has
    -- its own building generation; install for B then ensure A's active gen is
    -- untouched and B activates independently.
    v_proof_id := flashback_install_verified_backup_proof(
        'proof-ok-b', v_other_tracking_id, 'proof_helper', 'repo', 'stanza',
        '20260717ProofBF', v_sysid, v_timeline, 'manifest-b', repeat('44', 32),
        v_marker_lsn + 1, v_marker_lsn + 40
    );
    PERFORM flashback_consume_verified_backup_proof(v_proof_id);
    IF (
        SELECT count(*) FROM flashback.coverage_generations
        WHERE tracking_id IN (v_tracking_id, v_other_tracking_id)
          AND state = 'active'
    ) <> 2 THEN
        RAISE EXCEPTION 'each lifecycle must consume only its own proof';
    END IF;

    -- Timeline mismatch freezes durably without rolling back the gap.
    v_proof_id := flashback_install_verified_wal_frontier_proof(
        'frontier-mismatch-1',
        v_tracking_id,
        v_generation_id,
        'proof_helper',
        'repo',
        'stanza',
        v_timeline + 99,
        v_marker_lsn + 80,
        repeat('55', 32)
    );
    v_frontier := flashback_consume_verified_wal_frontier_proof(v_proof_id);
    IF v_frontier ->> 'status' IS DISTINCT FROM 'timeline_mismatch' THEN
        RAISE EXCEPTION 'timeline mismatch must return structured failure, got %', v_frontier;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM flashback.coverage_gaps
        WHERE tracking_id = v_tracking_id
          AND reason = 'timeline_mismatch'
          AND reanchored_by_generation_id IS NULL
    ) THEN
        RAISE EXCEPTION 'timeline mismatch gap must remain durable after consume returns';
    END IF;
    IF (
        SELECT valid_through_lsn FROM flashback.coverage_generations
        WHERE generation_id = v_generation_id
    ) IS DISTINCT FROM (v_marker_lsn + 50) THEN
        RAISE EXCEPTION 'frontier must not advance on timeline mismatch';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM flashback_health()
        WHERE tracking_id = v_tracking_id
          AND health = 'timeline_mismatch'
          AND reason LIKE '%timeline%'
    ) THEN
        RAISE EXCEPTION 'health must surface timeline freeze';
    END IF;

    v_failed := false;
    BEGIN
        PERFORM flashback_prepare_backup_restore(
            'public.it_proof_a', v_marker_lsn + 40
        );
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'prepare must reject targets after timeline freeze/gap';
    END IF;
END;
$$;
