-- Internal decoded-batch promote core shared by flashback_consume_wal() and
-- the pg_test injection seam. Not a public API: no EXECUTE grants.
-- Caller must populate pg_temp._fb_wal_batch and hold any required lifecycle locks.

CREATE OR REPLACE FUNCTION flashback_apply_decoded_wal_batch(
    p_stream_id bigint,
    p_confirmed_flush_lsn pg_lsn DEFAULT NULL,
    p_restart_lsn pg_lsn DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_inserted bigint := 0;
    v_missing_commits bigint := 0;
    v_gap_inserted bigint := 0;
    pending record;
    lock_rec record;
    v_frontier_lsn pg_lsn;
    v_frontier_time timestamptz;
    v_confirmed_flush_lsn pg_lsn := p_confirmed_flush_lsn;
    v_restart_lsn pg_lsn := p_restart_lsn;
BEGIN
    IF p_stream_id IS NULL THEN
        RAISE EXCEPTION 'flashback_apply_decoded_wal_batch: stream_id is required';
    END IF;

    IF to_regclass('pg_temp._fb_wal_batch') IS NULL THEN
        RAISE EXCEPTION 'flashback_apply_decoded_wal_batch: pg_temp._fb_wal_batch is missing';
    END IF;

    -- consume_wal() pre-creates and pins this set. The injection seam may call
    -- the promote core directly; an empty table means watermark updates use
    -- try-lock (callers that already hold lifecycle locks succeed).
    IF to_regclass('pg_temp._fb_wal_lock_ids') IS NULL THEN
        CREATE TEMP TABLE _fb_wal_lock_ids (
            tracking_id bigint PRIMARY KEY
        ) ON COMMIT DROP;
    END IF;

    DROP TABLE IF EXISTS pg_temp._fb_wal_commits;
    DROP TABLE IF EXISTS pg_temp._fb_wal_events;
    DROP TABLE IF EXISTS pg_temp._fb_wal_relevant_commits;

    CREATE TEMP TABLE _fb_wal_commits ON COMMIT DROP AS
    SELECT
        (data->>'commit')::bigint AS source_xid,
        (data->>'lsn')::pg_lsn AS commit_lsn,
        TIMESTAMPTZ '2000-01-01 00:00:00+00'
            + (data->>'commit_time')::bigint * interval '1 microsecond' AS committed_at
    FROM _fb_wal_batch
    WHERE data ? 'commit';
    CREATE UNIQUE INDEX ON _fb_wal_commits(source_xid);
    CREATE UNIQUE INDEX ON _fb_wal_commits(commit_lsn);

    CREATE TEMP TABLE _fb_wal_events (
        change_lsn pg_lsn,
        ord bigint,
        event_type text,
        table_name text,
        rel_oid oid,
        source_xid bigint,
        old_data jsonb,
        new_data jsonb,
        ddl_info jsonb,
        msg_schema_version bigint
    ) ON COMMIT DROP;

    INSERT INTO _fb_wal_events (
        change_lsn, ord, event_type, table_name, rel_oid, source_xid,
        old_data, new_data, ddl_info, msg_schema_version
    )
    SELECT
        b.change_lsn,
        b.ord,
        b.data->>'op' AS event_type,
        format('%I.%I', b.data->>'schema', b.data->>'table') AS table_name,
        (b.data->>'oid')::oid AS rel_oid,
        COALESCE(b.source_xid, (b.data->>'xid')::bigint) AS source_xid,
        b.data->'old' AS old_data,
        b.data->'new' AS new_data,
        b.data->'ddl_info' AS ddl_info,
        (b.data->>'schema_version')::bigint AS msg_schema_version
    FROM _fb_wal_batch b
    WHERE b.data->>'op' IN ('INSERT', 'UPDATE', 'DELETE');

    -- Authoritative DDL payload comes only from the protected transactional
    -- table.  A caller-crafted pg_logical_emit_message body can produce the
    -- fixed marker/COMMIT pair, but it can never create one of these rows.
    INSERT INTO _fb_wal_events (
        change_lsn, ord, event_type, table_name, rel_oid, source_xid,
        old_data, new_data, ddl_info, msg_schema_version
    )
    SELECT
        p.event_lsn,
        COALESCE((SELECT max(e.ord) FROM _fb_wal_events e), 0)
            + row_number() OVER (ORDER BY p.pending_event_id),
        p.event_type, p.table_name, p.rel_oid, p.source_xid,
        p.old_data, p.new_data, p.ddl_info, p.schema_version
    FROM flashback.pending_wal_events p
    WHERE p.stream_id = p_stream_id
      AND EXISTS (
          SELECT 1 FROM _fb_wal_commits c
          WHERE c.source_xid = p.source_xid
      )
    ORDER BY p.pending_event_id;

    SELECT count(*) INTO v_missing_commits
    FROM _fb_wal_events e
    LEFT JOIN _fb_wal_commits c USING (source_xid)
    WHERE c.commit_lsn IS NULL;

    IF v_missing_commits > 0 THEN
        PERFORM flashback_mark_capture_stream_broken(
            p_stream_id,
            'decoder_commit_record_missing',
            jsonb_build_object('event_count', v_missing_commits)
        );
        RAISE WARNING 'pg_flashback: % decoded events had no COMMIT record; stream % was frozen and a durable gap was opened',
            v_missing_commits, p_stream_id;
        RETURN 0;
    END IF;

    CREATE TEMP TABLE _fb_wal_relevant_commits ON COMMIT DROP AS
    SELECT c.*
    FROM _fb_wal_commits c
    WHERE EXISTS (
        SELECT 1 FROM _fb_wal_events e WHERE e.source_xid = c.source_xid
    )
       OR EXISTS (
        SELECT 1
        FROM flashback.coverage_generations cg
        WHERE cg.stream_id = p_stream_id
          AND cg.state = 'building'
          AND cg.boundary_xid = c.source_xid
    )
       OR EXISTS (
        SELECT 1
        FROM flashback.coverage_generations cg
        WHERE cg.recovery_profile = 'backup'
          AND cg.stream_id IS NULL
          AND cg.state = 'building'
          AND cg.boundary_xid = c.source_xid
    )
       OR EXISTS (
        SELECT 1
        FROM flashback.tracked_tables tt
        WHERE COALESCE(tt.protection_state, 'active') = 'stopping'
          AND tt.stop_marker_xid = c.source_xid
    );
    CREATE UNIQUE INDEX ON _fb_wal_relevant_commits(source_xid);
    CREATE UNIQUE INDEX ON _fb_wal_relevant_commits(commit_lsn);

    INSERT INTO flashback.capture_commits(stream_id, commit_lsn, source_xid, committed_at)
    SELECT p_stream_id, c.commit_lsn, c.source_xid, c.committed_at
    FROM _fb_wal_relevant_commits c
    ORDER BY c.commit_lsn
    ON CONFLICT (stream_id, commit_lsn) DO NOTHING;

    -- Resolve exact boundaries only after their transaction's COMMIT record is
    -- durably present in this same transaction. Initial tracking activates one
    -- generation; a post-restore successor atomically seals its parent.
    -- Backup markers resolve COMMIT coordinates without activating until a
    -- verified FULL backup anchor is installed.
    FOR pending IN
        SELECT
            cg.generation_id, cg.tracking_id, cg.parent_generation_id,
            cg.boundary_snapshot_id, cg.boundary_kind, cg.recovery_profile,
            c.commit_lsn, c.committed_at
        FROM flashback.coverage_generations cg
        JOIN _fb_wal_commits c ON c.source_xid = cg.boundary_xid
        WHERE cg.state = 'building'
          AND (
              cg.stream_id = p_stream_id
              OR (
                  cg.recovery_profile = 'backup'
                  AND cg.stream_id IS NULL
              )
          )
        ORDER BY cg.generation_id
        FOR UPDATE OF cg
    LOOP
        UPDATE flashback.snapshots
           SET snapshot_lsn = pending.commit_lsn,
               captured_at = pending.committed_at
         WHERE snapshot_id = pending.boundary_snapshot_id
           AND tracking_id = pending.tracking_id;

        UPDATE flashback.schema_versions
           SET applied_lsn = pending.commit_lsn,
               committed_at = pending.committed_at,
               commit_lsn = pending.commit_lsn
         WHERE generation_id = pending.generation_id
           AND tracking_id = pending.tracking_id
           AND source_xid = (
               SELECT boundary_xid FROM flashback.coverage_generations
               WHERE generation_id = pending.generation_id
           );

        IF pending.recovery_profile = 'backup' THEN
            IF pending.parent_generation_id IS NOT NULL THEN
                UPDATE flashback.coverage_generations
                   SET state = 'sealed',
                       superseded_before_lsn = pending.commit_lsn,
                       superseded_before_time = pending.committed_at,
                       sealed_at = clock_timestamp(),
                       state_reason = 'successor_boundary_resolved'
                 WHERE generation_id = pending.parent_generation_id
                   AND tracking_id = pending.tracking_id
                   AND state = 'active';

                UPDATE flashback.coverage_generations
                   SET state_reason = 'post_restore_unanchored',
                       details = COALESCE(details, '{}'::jsonb)
                           || jsonb_build_object('tracking_marker_lsn', pending.commit_lsn)
                 WHERE generation_id = pending.generation_id
                   AND state = 'building';

                INSERT INTO flashback.coverage_gaps (
                    tracking_id, source_generation_id, reason,
                    gap_start_lsn, gap_start_time, lower_bound_inclusive,
                    source_xid, details
                )
                SELECT
                    pending.tracking_id, pending.parent_generation_id,
                    'post_restore_unanchored',
                    pending.commit_lsn, pending.committed_at, false,
                    (
                        SELECT boundary_xid FROM flashback.coverage_generations
                        WHERE generation_id = pending.generation_id
                    ),
                    jsonb_build_object(
                        'successor_generation_id', pending.generation_id,
                        'required_next_full_backup', true
                    )
                WHERE NOT EXISTS (
                    SELECT 1 FROM flashback.coverage_gaps g
                    WHERE g.tracking_id = pending.tracking_id
                      AND g.source_generation_id = pending.parent_generation_id
                      AND g.reason = 'post_restore_unanchored'
                      AND g.reanchored_by_generation_id IS NULL
                );
                GET DIAGNOSTICS v_gap_inserted = ROW_COUNT;
                IF v_gap_inserted > 0 THEN
                    RAISE WARNING
                        'pg_flashback: tracking_id % requires action backup_reanchor_required after production swap; take a new qualifying FULL backup',
                        pending.tracking_id;
                    -- Transactional NOTIFY: delivered only if this consume
                    -- transaction commits. Health metadata remains authoritative
                    -- if the notification is lost.
                    PERFORM pg_notify(
                        'pg_flashback_action_required',
                        jsonb_build_object(
                            'tracking_id', pending.tracking_id,
                            'generation_id', pending.generation_id,
                            'action', 'backup_reanchor_required',
                            'reason', 'post_restore_unanchored',
                            'marker_lsn', pending.commit_lsn
                        )::text
                    );
                END IF;
            ELSE
                -- Keep boundary_lsn NULL until activate binds the FULL stop LSN.
                UPDATE flashback.coverage_generations
                   SET state_reason = 'marker_commit_observed',
                       details = COALESCE(details, '{}'::jsonb)
                           || jsonb_build_object('tracking_marker_lsn', pending.commit_lsn)
                 WHERE generation_id = pending.generation_id
                   AND state = 'building';
            END IF;
            CONTINUE;
        END IF;

        IF pending.parent_generation_id IS NOT NULL THEN
            UPDATE flashback.coverage_generations
               SET state = 'sealed',
                   -- A same-stream handoff is continuous and this decoded
                   -- batch proves the parent through the boundary.  A
                   -- cross-stream re-anchor follows a permanent gap: retain
                   -- the old frozen watermark instead of fabricating replay.
                   valid_through_lsn = CASE
                       WHEN stream_id = p_stream_id THEN pending.commit_lsn
                       ELSE valid_through_lsn
                   END,
                   valid_through_time = CASE
                       WHEN stream_id = p_stream_id THEN pending.committed_at
                       ELSE valid_through_time
                   END,
                   superseded_before_lsn = pending.commit_lsn,
                   superseded_before_time = pending.committed_at,
                   sealed_at = clock_timestamp(),
                   state_reason = 'successor_boundary_resolved'
             WHERE generation_id = pending.parent_generation_id
               AND tracking_id = pending.tracking_id
               AND state = 'active';
        END IF;

        UPDATE flashback.coverage_generations
           SET boundary_lsn = pending.commit_lsn,
               boundary_time = pending.committed_at,
               valid_through_lsn = pending.commit_lsn,
               valid_through_time = pending.committed_at,
               state = 'active',
               activated_at = clock_timestamp(),
               state_reason = 'boundary_commit_observed'
         WHERE generation_id = pending.generation_id
           AND state = 'building';

        UPDATE flashback.coverage_gaps
           SET gap_end_lsn = pending.commit_lsn,
               gap_end_time = pending.committed_at,
               reanchored_by_generation_id = pending.generation_id,
               reanchored_at = clock_timestamp()
         WHERE tracking_id = pending.tracking_id
           AND source_generation_id = pending.parent_generation_id
           AND reanchored_by_generation_id IS NULL
           AND gap_start_lsn < pending.commit_lsn;
    END LOOP;

    UPDATE flashback.schema_versions sv
       SET applied_lsn = c.commit_lsn,
           committed_at = c.committed_at,
           commit_lsn = c.commit_lsn
      FROM _fb_wal_commits c
     WHERE sv.stream_id = p_stream_id
       AND sv.source_xid = c.source_xid
       AND sv.commit_lsn IS NULL;

    WITH qualified AS (
        SELECT
            e.*, c.commit_lsn, c.committed_at,
            tt.tracking_id, cg.generation_id, cg.stream_id,
            tt.recovery_profile
        FROM _fb_wal_events e
        JOIN _fb_wal_commits c USING (source_xid)
        JOIN flashback.coverage_generations cg
          ON cg.stream_id = p_stream_id
         AND cg.state IN ('active', 'sealed')
         -- A restore swaps tracked_tables.rel_oid to the new physical OID,
         -- while already-buffered WAL still carries the predecessor OID.
         -- The immutable generation boundary is the ownership identity.
         AND cg.rel_oid_at_boundary = e.rel_oid
         AND c.commit_lsn > cg.boundary_lsn
         AND (cg.superseded_before_lsn IS NULL
              OR c.commit_lsn < cg.superseded_before_lsn)
        JOIN flashback.tracked_tables tt
          ON tt.tracking_id = cg.tracking_id
         AND tt.is_active
         AND tt.recovery_profile = 'local_delta'

        UNION ALL

        SELECT
            e.*, c.commit_lsn, c.committed_at,
            tt.tracking_id, NULL::bigint, NULL::bigint,
            tt.recovery_profile
        FROM _fb_wal_events e
        JOIN _fb_wal_commits c USING (source_xid)
        JOIN flashback.tracked_tables tt
          ON tt.rel_oid = e.rel_oid
         AND tt.is_active
         AND tt.recovery_profile = 'backup'
        WHERE e.event_type IN ('TRUNCATE', 'DROP', 'ALTER')
    ), ins AS (
        INSERT INTO flashback.delta_log (
            event_time, event_type, table_name, rel_oid, source_xid,
            tracking_id, generation_id, stream_id,
            committed_at, commit_lsn, schema_version,
            old_data, new_data, ddl_info, lsn
        )
        SELECT
            q.committed_at, q.event_type, q.table_name, q.rel_oid, q.source_xid,
            CASE WHEN q.recovery_profile = 'local_delta' THEN q.tracking_id END,
            CASE WHEN q.recovery_profile = 'local_delta' THEN q.generation_id END,
            CASE WHEN q.recovery_profile = 'local_delta' THEN q.stream_id END,
            q.committed_at,
            q.commit_lsn,
            COALESCE(q.msg_schema_version, (
                SELECT sv.schema_version
                FROM flashback.schema_versions sv
                WHERE sv.tracking_id = q.tracking_id
                  AND (sv.generation_id = q.generation_id OR sv.generation_id IS NULL)
                  AND (sv.commit_lsn IS NULL OR sv.commit_lsn <= q.commit_lsn)
                ORDER BY sv.schema_version DESC
                LIMIT 1
            ), 1),
            q.old_data, q.new_data, q.ddl_info, q.change_lsn
        FROM qualified q
        ORDER BY q.ord
        RETURNING 1
    )
    SELECT count(*) INTO v_inserted FROM ins;

    -- Bind pre-DROP manifests (captured with source_xid in the DROP TX) to the
    -- exact committed DROP event so plan/execute never use a newer unrelated
    -- manifest for the same tracking_id.
    IF to_regprocedure('public.flashback_bind_drop_dependency_manifests()') IS NOT NULL THEN
        PERFORM flashback_bind_drop_dependency_manifests();
    END IF;

    DELETE FROM flashback.pending_wal_events p
    USING _fb_wal_commits c
    WHERE p.stream_id = p_stream_id
      AND p.source_xid = c.source_xid;

    SELECT commit_lsn, committed_at
      INTO v_frontier_lsn, v_frontier_time
    FROM _fb_wal_relevant_commits
    ORDER BY commit_lsn DESC
    LIMIT 1;

    IF p_confirmed_flush_lsn IS NOT NULL THEN
        v_confirmed_flush_lsn := p_confirmed_flush_lsn;
        v_restart_lsn := p_restart_lsn;
    END IF;

    IF v_frontier_lsn IS NOT NULL THEN
        UPDATE flashback.capture_streams
           SET valid_through_lsn = GREATEST(valid_through_lsn, v_frontier_lsn),
               valid_through_time = v_frontier_time,
               confirmed_flush_lsn = COALESCE(v_confirmed_flush_lsn, confirmed_flush_lsn),
               restart_lsn = COALESCE(v_restart_lsn, restart_lsn),
               details = COALESCE(details, '{}'::jsonb)
                   - 'safe_slot_advance_start_lsn'
                   - 'safe_slot_advance_upto_lsn'
                   - 'safe_slot_advance_recorded_at'
         WHERE stream_id = p_stream_id
           AND state = 'active';

        -- Advance watermarks for lifecycles pinned for this batch. Independently
        -- try-lock idle lifecycles so an unrelated hold does not freeze their
        -- empty prefix, while a busy restore/untrack still owns its watermark.
        FOR lock_rec IN
            SELECT DISTINCT cg.tracking_id
            FROM flashback.coverage_generations cg
            WHERE cg.stream_id = p_stream_id
              AND cg.state IN ('active', 'sealed')
              AND cg.valid_through_lsn <= v_frontier_lsn
            ORDER BY cg.tracking_id
        LOOP
            IF NOT EXISTS (
                SELECT 1 FROM _fb_wal_lock_ids pinned
                WHERE pinned.tracking_id = lock_rec.tracking_id
            ) AND NOT pg_try_advisory_xact_lock(
                358944::integer, hashint8(lock_rec.tracking_id)
            ) THEN
                CONTINUE;
            END IF;

            UPDATE flashback.coverage_generations
               SET valid_through_lsn = LEAST(
                       v_frontier_lsn,
                       COALESCE(superseded_before_lsn, v_frontier_lsn)
                   ),
                   valid_through_time = CASE
                       WHEN superseded_before_lsn IS NULL
                            OR v_frontier_lsn < superseded_before_lsn
                           THEN v_frontier_time
                       ELSE valid_through_time
                   END
             WHERE stream_id = p_stream_id
               AND tracking_id = lock_rec.tracking_id
               AND state IN ('active', 'sealed')
               AND valid_through_lsn <= v_frontier_lsn;
        END LOOP;
    ELSIF v_confirmed_flush_lsn IS NOT NULL THEN
        UPDATE flashback.capture_streams
           SET confirmed_flush_lsn = v_confirmed_flush_lsn,
               restart_lsn = COALESCE(v_restart_lsn, restart_lsn),
               details = COALESCE(details, '{}'::jsonb)
                   - 'safe_slot_advance_start_lsn'
                   - 'safe_slot_advance_upto_lsn'
                   - 'safe_slot_advance_recorded_at'
         WHERE stream_id = p_stream_id
           AND state = 'active';
    END IF;

    RETURN v_inserted;
END;
$$;

REVOKE ALL ON FUNCTION flashback_apply_decoded_wal_batch(bigint, pg_lsn, pg_lsn) FROM PUBLIC;
