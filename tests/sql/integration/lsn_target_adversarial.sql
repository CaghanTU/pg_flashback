-- Adversarial contract for T-01 option A.  Wall-clock timestamps are only a
-- convenience planner; COMMIT LSN is the execution coordinate.  The planner
-- must reject timestamp collisions/inversions and admission must reject every
-- target beyond a frozen frontier or inside a durable gap.
DROP TABLE IF EXISTS public.it_lsn_adversarial CASCADE;
DROP TABLE IF EXISTS flashback.base_snapshot_987654320 CASCADE;

CREATE TABLE public.it_lsn_adversarial (id integer PRIMARY KEY, note text);
INSERT INTO public.it_lsn_adversarial VALUES (1, 'boundary');
CREATE TABLE flashback.base_snapshot_987654320
AS TABLE public.it_lsn_adversarial;
SELECT flashback_own_payload_table(
    'flashback.base_snapshot_987654320'::regclass
);

DO $test$
DECLARE
    v_tracking_id bigint;
    v_stream_id bigint;
    v_snapshot_id bigint;
    v_generation_id bigint;
    v_resolved_lsn pg_lsn;
    v_base timestamptz := TIMESTAMPTZ '2025-01-01 00:00:00+00';
    v_failed boolean;
BEGIN
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table,
        recovery_profile, is_active
    ) VALUES (
        'public.it_lsn_adversarial'::regclass,
        'public', 'it_lsn_adversarial',
        'flashback.base_snapshot_987654320',
        'local_delta', true
    ) RETURNING tracking_id INTO v_tracking_id;

    INSERT INTO flashback.capture_streams (
        database_oid, database_name, epoch_no, capture_mode, timeline_id,
        slot_name, plugin_name, state, valid_through_time, valid_through_lsn,
        confirmed_flush_lsn, restart_lsn, activated_at
    ) VALUES (
        (SELECT oid FROM pg_database WHERE datname = current_database()),
        current_database(), 1, 'wal', 1,
        'it_lsn_adversarial_slot', 'pg_flashback', 'active',
        v_base + interval '4 seconds', '0/5000', '0/5000', '0/1000',
        clock_timestamp()
    ) RETURNING stream_id INTO v_stream_id;

    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn,
        schema_def, row_count, captured_at
    ) VALUES (
        'public.it_lsn_adversarial'::regclass, v_tracking_id,
        'flashback.base_snapshot_987654320', '0/1000',
        '{}'::jsonb, 1, v_base
    ) RETURNING snapshot_id INTO v_snapshot_id;

    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, stream_id, recovery_profile, state,
        boundary_kind, rel_oid_at_boundary, boundary_snapshot_id,
        boundary_time, boundary_lsn, valid_through_time, valid_through_lsn,
        activated_at
    ) VALUES (
        v_tracking_id, 1, v_stream_id, 'local_delta', 'active',
        'adversarial_fixture', 'public.it_lsn_adversarial'::regclass,
        v_snapshot_id, v_base, '0/1000',
        v_base + interval '4 seconds', '0/5000', clock_timestamp()
    ) RETURNING generation_id INTO v_generation_id;

    INSERT INTO flashback.capture_commits(
        stream_id, commit_lsn, source_xid, committed_at
    ) VALUES
        (v_stream_id, '0/1000', 1001, v_base),
        (v_stream_id, '0/2000', 1002, v_base + interval '1 second'),
        (v_stream_id, '0/3000', 1003, v_base + interval '2 seconds'),
        (v_stream_id, '0/4000', 1004, v_base + interval '3 seconds'),
        (v_stream_id, '0/5000', 1005, v_base + interval '4 seconds');

    SELECT resolved_lsn INTO STRICT v_resolved_lsn
    FROM flashback_resolve_target(
        'public.it_lsn_adversarial', v_base + interval '2.5 seconds'
    );
    IF v_resolved_lsn <> '0/3000'::pg_lsn THEN
        RAISE EXCEPTION 'monotonic timestamp resolved to %, expected 0/3000',
            v_resolved_lsn;
    END IF;

    IF (SELECT count(*) FROM flashback_admit_lsn_target(
            'public.it_lsn_adversarial', '0/1000')) <> 1
    THEN
        RAISE EXCEPTION 'exact generation boundary was not admitted';
    END IF;

    -- Same-microsecond commits at the requested timestamp are deliberately
    -- fail-closed; callers can select the intended prefix with an explicit LSN.
    UPDATE flashback.capture_commits
       SET committed_at = v_base + interval '1 second'
     WHERE stream_id = v_stream_id
       AND commit_lsn IN ('0/2000', '0/3000');
    v_failed := false;
    BEGIN
        PERFORM * FROM flashback_resolve_target(
            'public.it_lsn_adversarial', v_base + interval '1 second'
        );
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM LIKE '%maps to 2 commits%' THEN
            v_failed := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'same-microsecond timestamp collision was admitted';
    END IF;

    -- WAL order: 0/3000 has a later clock reading than 0/4000.  No timestamp
    -- filter can represent the requested state as one contiguous WAL prefix.
    UPDATE flashback.capture_commits
       SET committed_at = CASE commit_lsn
           WHEN '0/2000'::pg_lsn THEN v_base + interval '1 second'
           WHEN '0/3000'::pg_lsn THEN v_base + interval '3 seconds'
           WHEN '0/4000'::pg_lsn THEN v_base + interval '2 seconds'
           ELSE committed_at
       END
     WHERE stream_id = v_stream_id;
    v_failed := false;
    BEGIN
        PERFORM * FROM flashback_resolve_target(
            'public.it_lsn_adversarial', v_base + interval '2.5 seconds'
        );
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM LIKE '%not a WAL-prefix cut%' THEN
            v_failed := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'timestamp inversion was admitted as a WAL prefix';
    END IF;

    -- Restore execution may never fall back to the legacy per-event timestamp
    -- filter once a qualified generation exists.
    v_failed := false;
    BEGIN
        PERFORM flashback_restore(
            'public.it_lsn_adversarial', v_base + interval '2 seconds'
        );
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM LIKE '%disabled for correctness-qualified WAL coverage%' THEN
            v_failed := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'legacy timestamp restore accepted a qualified generation';
    END IF;

    v_failed := false;
    BEGIN
        PERFORM * FROM flashback_admit_lsn_target(
            'public.it_lsn_adversarial', '0/6000'
        );
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM LIKE '%owned by 0 eligible generations%' THEN
            v_failed := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'target beyond frozen watermark was admitted';
    END IF;

    -- Breaking a stream freezes it; already-proven targets remain usable,
    -- while health becomes degraded and a durable open gap begins strictly
    -- after the last proven LSN.
    PERFORM flashback_mark_capture_stream_broken(
        v_stream_id, 'adversarial_slot_loss', '{}'::jsonb
    );
    IF (SELECT count(*) FROM flashback_admit_lsn_target(
            'public.it_lsn_adversarial', '0/4000')) <> 1
    THEN
        RAISE EXCEPTION 'pre-gap target on a broken stream was not admitted';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM flashback_health()
        WHERE tracking_id = v_tracking_id
          AND health = 'slot_lost'
          AND open_gap_count = 1
          AND reason = 'adversarial_slot_loss'
    ) THEN
        RAISE EXCEPTION 'broken stream was not visible as slot_lost health';
    END IF;

    -- An independent permanent gap inside the frozen prefix must override the
    -- otherwise valid stream/generation watermarks.
    INSERT INTO flashback.coverage_gaps (
        tracking_id, source_generation_id, reason,
        gap_start_time, gap_start_lsn, lower_bound_inclusive
    ) VALUES (
        v_tracking_id, v_generation_id, 'adversarial_missing_commit',
        v_base + interval '1.5 seconds', '0/2800', true
    );
    v_failed := false;
    BEGIN
        PERFORM * FROM flashback_admit_lsn_target(
            'public.it_lsn_adversarial', '0/3000'
        );
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM LIKE '%owned by 0 eligible generations%' THEN
            v_failed := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'target inside a durable gap was admitted';
    END IF;
END;
$test$;
