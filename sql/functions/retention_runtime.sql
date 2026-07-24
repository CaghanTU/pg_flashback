-- =================================================================
-- Generation-aware retention runtime
-- =================================================================

CREATE OR REPLACE FUNCTION flashback_begin_generation_retirement(
    p_generation_id bigint,
    p_reason text DEFAULT 'retention_window_elapsed'
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    gen record;
    v_tracking_id bigint;
    v_retirement_id bigint;
    v_delta_rows bigint;
    v_schema_rows bigint;
    v_first_lsn pg_lsn;
    v_last_lsn pg_lsn;
BEGIN
    IF p_reason IS NULL OR btrim(p_reason) = '' THEN
        RAISE EXCEPTION 'flashback_begin_generation_retirement: reason is required';
    END IF;

    -- All qualified WAL operations acquire the database stream key before a
    -- stable lifecycle key.  Retention must follow that order as well: the
    -- worker consumes WAL under database -> lifecycle, while an older version
    -- of this function took lifecycle first and later requested the database
    -- partition key, allowing a retention/worker deadlock.
    PERFORM flashback_internal_lock_database_stream(
        (SELECT oid FROM pg_database WHERE datname = current_database())
    );

    SELECT cg.tracking_id INTO v_tracking_id
    FROM flashback.coverage_generations cg
    WHERE cg.generation_id = p_generation_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_flashback: generation % does not exist', p_generation_id;
    END IF;

    PERFORM flashback_internal_lock_lifecycle(v_tracking_id);

    SELECT
        cg.generation_id, cg.tracking_id, cg.stream_id, cg.state, cg.sealed_at,
        cg.valid_through_lsn, cg.superseded_before_lsn,
        tt.retention_interval,
        snap.snapshot_id, snap.snapshot_table, snap.rel_oid, snap.row_count,
        snap.schema_def, snap.payload_state
      INTO gen
    FROM flashback.coverage_generations cg
    JOIN flashback.tracked_tables tt
      ON tt.tracking_id = cg.tracking_id
     AND tt.is_active
    JOIN flashback.snapshots snap
      ON snap.snapshot_id = cg.boundary_snapshot_id
     AND snap.tracking_id = cg.tracking_id
    WHERE cg.generation_id = p_generation_id
      AND cg.recovery_profile = 'local_delta'
    FOR UPDATE OF cg, snap;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_flashback: generation % is not an active local_delta lifecycle payload',
            p_generation_id;
    END IF;

    SELECT retirement_id INTO v_retirement_id
    FROM flashback.generation_payload_retirements
    WHERE generation_id = p_generation_id;
    IF FOUND THEN
        RETURN v_retirement_id;
    END IF;

    IF gen.state <> 'sealed' THEN
        RAISE EXCEPTION 'pg_flashback: generation % must be sealed before retirement (state=%)',
            p_generation_id, gen.state;
    END IF;
    IF gen.sealed_at IS NULL
       OR gen.sealed_at > clock_timestamp() - gen.retention_interval
    THEN
        RAISE EXCEPTION 'pg_flashback: generation % is still inside the requested retention window',
            p_generation_id;
    END IF;
    IF gen.superseded_before_lsn IS NULL
       OR gen.valid_through_lsn IS NULL
    THEN
        RAISE EXCEPTION 'pg_flashback: generation % backlog has not drained through its immutable upper bound',
            p_generation_id;
    END IF;
    IF gen.valid_through_lsn < gen.superseded_before_lsn
       AND NOT EXISTS (
           -- A cross-stream re-anchor cannot advance the predecessor's
           -- watermark into the new stream.  It is nevertheless complete
           -- when the durable, closed gap covers the whole hand-off interval:
           -- targets in that interval are permanently rejected and no old
           -- stream payload can still arrive there.
           SELECT 1
           FROM flashback.coverage_gaps gap
           WHERE gap.tracking_id = gen.tracking_id
             AND gap.source_generation_id = gen.generation_id
             AND gap.gap_start_lsn IS NOT NULL
             AND gap.gap_end_lsn IS NOT NULL
             AND gap.reanchored_by_generation_id IS NOT NULL
             AND gap.gap_start_lsn <= gen.valid_through_lsn
             AND gap.gap_end_lsn >= gen.superseded_before_lsn
       )
    THEN
        RAISE EXCEPTION 'pg_flashback: generation % backlog has not drained through its immutable upper bound',
            p_generation_id;
    END IF;
    IF gen.payload_state <> 'available'
       OR to_regclass(gen.snapshot_table) IS NULL
       OR NOT flashback_payload_is_owned(to_regclass(gen.snapshot_table))
    THEN
        RAISE EXCEPTION 'pg_flashback: generation % snapshot payload is not available',
            p_generation_id;
    END IF;
    IF EXISTS (
        SELECT 1 FROM flashback.pending_wal_events pending
        WHERE pending.generation_id = p_generation_id
    ) THEN
        RAISE EXCEPTION 'pg_flashback: generation % still owns pending WAL payload',
            p_generation_id;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM flashback.coverage_generations successor
        JOIN flashback.snapshots successor_snapshot
          ON successor_snapshot.snapshot_id = successor.boundary_snapshot_id
         AND successor_snapshot.tracking_id = successor.tracking_id
        WHERE successor.tracking_id = gen.tracking_id
          AND successor.state = 'active'
          AND (
              successor.stream_id IS DISTINCT FROM gen.stream_id
              OR successor.boundary_lsn >= gen.superseded_before_lsn
          )
          AND successor_snapshot.payload_state = 'available'
          AND to_regclass(successor_snapshot.snapshot_table) IS NOT NULL
          AND flashback_payload_is_owned(
                  to_regclass(successor_snapshot.snapshot_table)
              )
          AND NOT EXISTS (
              SELECT 1
              FROM flashback.generation_payload_retirements successor_retirement
              WHERE successor_retirement.generation_id = successor.generation_id
          )
    ) THEN
        RAISE EXCEPTION 'pg_flashback: generation % has no newer active retained anchor',
            p_generation_id;
    END IF;

    SELECT count(*), min(commit_lsn), max(commit_lsn)
      INTO v_delta_rows, v_first_lsn, v_last_lsn
    FROM flashback.delta_log
    WHERE generation_id = p_generation_id;
    SELECT count(*) INTO v_schema_rows
    FROM flashback.schema_versions
    WHERE generation_id = p_generation_id;

    v_retirement_id := flashback_internal_create_retirement_intent(
        p_generation_id => p_generation_id,
        p_tracking_id => gen.tracking_id,
        p_reason => p_reason,
        p_snapshot_id => gen.snapshot_id,
        p_snapshot_table => gen.snapshot_table,
        p_snapshot_rel_oid => to_regclass(gen.snapshot_table)::oid,
        p_snapshot_row_count => gen.row_count,
        p_snapshot_schema_fingerprint => flashback_payload_schema_fingerprint(
            to_regclass(gen.snapshot_table)
        ),
        p_expected_delta_rows => v_delta_rows,
        p_expected_schema_rows => v_schema_rows,
        p_first_delta_lsn => v_first_lsn,
        p_last_delta_lsn => v_last_lsn,
        p_details => jsonb_build_object(
            'sealed_at', gen.sealed_at,
            'valid_through_lsn', gen.valid_through_lsn,
            'superseded_before_lsn', gen.superseded_before_lsn,
            'retention_interval', gen.retention_interval
        )
    );

    RETURN v_retirement_id;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_resume_generation_retirement(
    p_generation_id bigint
)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    retirement record;
    v_tracking_id bigint;
    v_current_delta_rows bigint;
    v_current_schema_rows bigint;
    v_removed_delta_rows bigint;
    v_removed_schema_rows bigint;
BEGIN
    -- Keep the same database-stream -> lifecycle ordering as WAL consume,
    -- restore, and configuration reconciliation.  The intent row is durable
    -- across transactions, but each retry still participates in the common
    -- lock protocol before inspecting or deleting payload.
    PERFORM flashback_internal_lock_database_stream(
        (SELECT oid FROM pg_database WHERE datname = current_database())
    );

    SELECT tracking_id INTO v_tracking_id
    FROM flashback.generation_payload_retirements
    WHERE generation_id = p_generation_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_flashback: generation % has no durable retirement intent',
            p_generation_id;
    END IF;

    PERFORM flashback_internal_lock_lifecycle(v_tracking_id);

    SELECT r.*, cg.state AS generation_state,
           cg.stream_id AS generation_stream_id,
           cg.superseded_before_lsn AS generation_superseded_before_lsn,
           snap.payload_state, snap.schema_def
      INTO retirement
    FROM flashback.generation_payload_retirements r
    JOIN flashback.coverage_generations cg
      ON cg.generation_id = r.generation_id
     AND cg.tracking_id = r.tracking_id
    JOIN flashback.snapshots snap
      ON snap.snapshot_id = r.snapshot_id
     AND snap.tracking_id = r.tracking_id
    WHERE r.generation_id = p_generation_id
    FOR UPDATE OF r, cg, snap;

    IF retirement.state = 'removed' THEN
        RETURN 0;
    END IF;
    IF retirement.intent_txid = txid_current() THEN
        RAISE EXCEPTION 'pg_flashback: retirement % intent must commit before payload removal',
            retirement.retirement_id
            USING HINT = 'COMMIT this transaction; the next retention cycle will resume idempotent cleanup.';
    END IF;
    IF retirement.generation_state <> 'sealed' THEN
        RAISE EXCEPTION 'pg_flashback: retirement % expected a sealed generation, found %',
            retirement.retirement_id, retirement.generation_state;
    END IF;
    IF retirement.payload_state <> 'available'
       OR to_regclass(retirement.snapshot_table) IS NULL
       OR to_regclass(retirement.snapshot_table)::oid
              IS DISTINCT FROM retirement.snapshot_rel_oid
       OR NOT flashback_payload_is_owned(
              to_regclass(retirement.snapshot_table)
          )
       OR flashback_payload_schema_fingerprint(
              to_regclass(retirement.snapshot_table)
          ) <> retirement.snapshot_schema_fingerprint
    THEN
        RAISE EXCEPTION 'pg_flashback: retirement % snapshot evidence changed before cleanup',
            retirement.retirement_id;
    END IF;

    -- Deletion verifies IDENTITY and NEED, not content: the right physical
    -- payload object (catalog identity + payload naming/namespace contract)
    -- and a still-valid newer anchor. Content integrity is a use-time
    -- property; re-scanning a payload we are about to discard proves nothing
    -- and would make cleanup cost proportional to table size.
    -- snapshot_row_count in the retirement evidence stays as the intent-time
    -- forensic record and is deliberately not re-verified here.
    -- Re-verify at removal time what the durable intent verified at intent
    -- time: a newer active generation still anchors this table's coverage.
    -- If the successor was lost between intent and resume (slot loss,
    -- abort), this sealed payload may be the only remaining evidence for
    -- pre-gap targets — refuse to destroy it.
    IF NOT EXISTS (
        SELECT 1
        FROM flashback.coverage_generations successor
        JOIN flashback.snapshots successor_snapshot
          ON successor_snapshot.snapshot_id = successor.boundary_snapshot_id
         AND successor_snapshot.tracking_id = successor.tracking_id
        WHERE successor.tracking_id = retirement.tracking_id
          AND successor.state = 'active'
          AND (
              successor.stream_id IS DISTINCT FROM retirement.generation_stream_id
              OR successor.boundary_lsn >= retirement.generation_superseded_before_lsn
          )
          AND successor_snapshot.payload_state = 'available'
          AND to_regclass(successor_snapshot.snapshot_table) IS NOT NULL
          AND flashback_payload_is_owned(
                  to_regclass(successor_snapshot.snapshot_table)
              )
          AND NOT EXISTS (
              SELECT 1
              FROM flashback.generation_payload_retirements successor_retirement
              WHERE successor_retirement.generation_id = successor.generation_id
          )
    ) THEN
        RAISE EXCEPTION 'pg_flashback: retirement % lost its newer active retained anchor; refusing payload removal',
            retirement.retirement_id
            USING HINT = 'Re-anchor coverage (flashback_reanchor) before retiring the last sealed generation.';
    END IF;

    SELECT count(*) INTO v_current_delta_rows
    FROM flashback.delta_log
    WHERE generation_id = p_generation_id;
    SELECT count(*) INTO v_current_schema_rows
    FROM flashback.schema_versions
    WHERE generation_id = p_generation_id;
    IF v_current_delta_rows <> retirement.expected_delta_rows
       OR v_current_schema_rows <> retirement.expected_schema_rows
    THEN
        RAISE EXCEPTION 'pg_flashback: retirement % payload counts changed after durable intent',
            retirement.retirement_id
            USING DETAIL = format(
                'delta expected/current=%s/%s, schema expected/current=%s/%s',
                retirement.expected_delta_rows, v_current_delta_rows,
                retirement.expected_schema_rows, v_current_schema_rows
            );
    END IF;

    PERFORM flashback_drop_payload_table(
        to_regclass(retirement.snapshot_table)
    );

    DELETE FROM flashback.delta_log
    WHERE generation_id = p_generation_id;
    GET DIAGNOSTICS v_removed_delta_rows = ROW_COUNT;
    DELETE FROM flashback.schema_versions
    WHERE generation_id = p_generation_id;
    GET DIAGNOSTICS v_removed_schema_rows = ROW_COUNT;

    UPDATE flashback.snapshots
       SET payload_state = 'retired',
           retired_at = clock_timestamp()
     WHERE snapshot_id = retirement.snapshot_id
       AND tracking_id = retirement.tracking_id;

    IF to_regclass(retirement.snapshot_table) IS NOT NULL
       OR EXISTS (
           SELECT 1 FROM flashback.delta_log
           WHERE generation_id = p_generation_id
       )
       OR EXISTS (
           SELECT 1 FROM flashback.schema_versions
           WHERE generation_id = p_generation_id
       )
       OR v_removed_delta_rows::bigint <> retirement.expected_delta_rows
       OR v_removed_schema_rows::bigint <> retirement.expected_schema_rows
    THEN
        RAISE EXCEPTION 'pg_flashback: retirement % could not verify complete payload removal',
            retirement.retirement_id;
    END IF;

    PERFORM flashback_internal_transition_retirement(
        retirement.retirement_id,
        'retiring',
        'removed',
        v_removed_delta_rows,
        v_removed_schema_rows
    );

    IF NOT flashback_internal_transition_coverage_generation(
        p_generation_id,
        retirement.tracking_id,
        'sealed',
        'retired',
        'payload_retired',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        '{}'::jsonb
    ) THEN
        RAISE EXCEPTION 'pg_flashback: generation % retirement transition failed',
            p_generation_id;
    END IF;

    RETURN 1;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_drop_empty_delta_partitions()
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_part record;
    v_bound_upper timestamptz;
    v_has_rows boolean;
    v_dropped integer := 0;
BEGIN
    IF to_regclass('flashback.delta_log') IS NULL THEN
        RETURN 0;
    END IF;

    -- Partition ownership is database-wide rather than lifecycle-scoped.
    -- Serialize concurrent retention callers, then still lock-and-recheck each
    -- relation before dropping it.
    PERFORM pg_advisory_xact_lock(
        358946::integer,
        (SELECT oid::integer FROM pg_database WHERE datname=current_database())
    );

    FOR v_part IN
        SELECT c.oid,
               format('%I.%I', n.nspname, c.relname) AS part_name,
               pg_get_expr(c.relpartbound, c.oid) AS bound_text
        FROM pg_inherits i
        JOIN pg_class p ON p.oid = i.inhparent
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE p.oid = 'flashback.delta_log'::regclass
          AND c.relname <> 'delta_log_default'
        ORDER BY c.oid
    LOOP
        BEGIN
            v_bound_upper := substring(
                v_part.bound_text from 'TO \(''([^'']+)''\)'
            )::timestamptz;
        EXCEPTION WHEN invalid_datetime_format THEN
            v_bound_upper := NULL;
        END;
        IF v_bound_upper IS NULL
           OR v_bound_upper >= date_trunc('month', clock_timestamp())
        THEN
            CONTINUE;
        END IF;

        -- The pre-lock emptiness observation would be racy. ACCESS EXCLUSIVE
        -- first, then recheck while inserts are blocked; only an actually
        -- empty old partition can be detached and dropped.
        BEGIN
            EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE', v_part.part_name);
        EXCEPTION WHEN undefined_table THEN
            CONTINUE;
        END;
        EXECUTE format(
            'SELECT EXISTS (SELECT 1 FROM %s LIMIT 1)',
            v_part.part_name
        ) INTO v_has_rows;
        IF NOT v_has_rows THEN
            PERFORM flashback_drop_payload_table(v_part.oid::regclass);
            v_dropped := v_dropped + 1;
        END IF;
    END LOOP;
    RETURN v_dropped;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_apply_retention()
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    rec record;
    snap_rec record;
    v_actions integer := 0;
    v_rows bigint := 0;
    v_eligible boolean;
BEGIN
    -- Retention is a database-wide coordinator.  Acquire the stream key once
    -- before touching any lifecycle so every path observes database ->
    -- tracking lock order, including installations that still have only
    -- legacy trigger rows and therefore skip the generation loops below.
    PERFORM flashback_internal_lock_database_stream(
        (SELECT oid FROM pg_database WHERE datname = current_database())
    );

    -- Resume intents that were committed by an earlier worker transaction.
    -- Newly created intents below are intentionally not processed until the
    -- next invocation, proving the durable intent precedes destructive work.
    FOR rec IN
        SELECT r.generation_id, r.tracking_id
        FROM flashback.generation_payload_retirements r
        WHERE r.state = 'retiring'
        ORDER BY r.requested_at, r.retirement_id
    LOOP
        -- A restore or untrack may own this lifecycle key. Do not allow that
        -- one table to head-of-line block cleanup for other tables; the
        -- database stream key above is already held, preserving the common
        -- database -> lifecycle lock order.
        IF NOT flashback_internal_try_lock_lifecycle(rec.tracking_id) THEN
            CONTINUE;
        END IF;
        v_actions := v_actions
            + flashback_resume_generation_retirement(rec.generation_id);
    END LOOP;

    FOR rec IN
        SELECT cg.generation_id, cg.tracking_id
        FROM flashback.coverage_generations cg
        JOIN flashback.tracked_tables tt USING (tracking_id)
        WHERE tt.is_active
          AND cg.recovery_profile = 'local_delta'
          AND cg.state = 'sealed'
          AND cg.sealed_at <= clock_timestamp() - tt.retention_interval
          AND (
              cg.valid_through_lsn >= cg.superseded_before_lsn
              OR EXISTS (
                  SELECT 1
                  FROM flashback.coverage_gaps gap
                  WHERE gap.tracking_id = cg.tracking_id
                    AND gap.source_generation_id = cg.generation_id
                    AND gap.gap_start_lsn IS NOT NULL
                    AND gap.gap_end_lsn IS NOT NULL
                    AND gap.reanchored_by_generation_id IS NOT NULL
                    AND gap.gap_start_lsn <= cg.valid_through_lsn
                    AND gap.gap_end_lsn >= cg.superseded_before_lsn
              )
          )
          AND NOT EXISTS (
              SELECT 1
              FROM flashback.generation_payload_retirements r
              WHERE r.generation_id = cg.generation_id
          )
        ORDER BY cg.tracking_id, cg.generation_no
    LOOP
        -- The candidate list is only a hint: a worker may observe a sealed
        -- generation while its successor is still building, while its
        -- watermark is still draining, or after an operator has changed the
        -- retention window.  Take the common lifecycle pin before the final
        -- eligibility check so those expected states cannot abort retention
        -- for every other lifecycle.  The explicit begin function remains
        -- strict for callers that request one generation directly.
        IF NOT flashback_internal_try_lock_lifecycle(rec.tracking_id) THEN
            CONTINUE;
        END IF;

        SELECT EXISTS (
            SELECT 1
            FROM flashback.coverage_generations cg
            JOIN flashback.tracked_tables tt
              ON tt.tracking_id = cg.tracking_id
             AND tt.is_active
             AND tt.recovery_profile = 'local_delta'
            JOIN flashback.snapshots snap
              ON snap.snapshot_id = cg.boundary_snapshot_id
             AND snap.tracking_id = cg.tracking_id
            WHERE cg.generation_id = rec.generation_id
              AND cg.tracking_id = rec.tracking_id
              AND cg.recovery_profile = 'local_delta'
              AND cg.state = 'sealed'
              AND cg.sealed_at <= clock_timestamp() - tt.retention_interval
              AND cg.superseded_before_lsn IS NOT NULL
              AND (
                  cg.valid_through_lsn >= cg.superseded_before_lsn
                  OR EXISTS (
                      SELECT 1
                      FROM flashback.coverage_gaps closed_gap
                      WHERE closed_gap.tracking_id = cg.tracking_id
                        AND closed_gap.source_generation_id = cg.generation_id
                        AND closed_gap.gap_start_lsn IS NOT NULL
                        AND closed_gap.gap_end_lsn IS NOT NULL
                        AND closed_gap.reanchored_by_generation_id IS NOT NULL
                        AND closed_gap.gap_start_lsn <= cg.valid_through_lsn
                        AND closed_gap.gap_end_lsn >= cg.superseded_before_lsn
                  )
              )
              AND snap.payload_state = 'available'
              AND to_regclass(snap.snapshot_table) IS NOT NULL
              AND flashback_payload_is_owned(
                      to_regclass(snap.snapshot_table)
                  )
              AND NOT EXISTS (
                  SELECT 1
                  FROM flashback.pending_wal_events pending
                  WHERE pending.generation_id = cg.generation_id
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM flashback.generation_payload_retirements retirement
                  WHERE retirement.generation_id = cg.generation_id
              )
              AND EXISTS (
                  SELECT 1
                  FROM flashback.coverage_generations successor
                  JOIN flashback.snapshots successor_snapshot
                    ON successor_snapshot.snapshot_id = successor.boundary_snapshot_id
                   AND successor_snapshot.tracking_id = successor.tracking_id
                  WHERE successor.tracking_id = cg.tracking_id
                    AND successor.state = 'active'
                    AND (
                        successor.stream_id IS DISTINCT FROM cg.stream_id
                        OR successor.boundary_lsn >= cg.superseded_before_lsn
                    )
                    AND successor_snapshot.payload_state = 'available'
                    AND to_regclass(successor_snapshot.snapshot_table) IS NOT NULL
                    AND flashback_payload_is_owned(
                            to_regclass(successor_snapshot.snapshot_table)
                        )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM flashback.generation_payload_retirements successor_retirement
                        WHERE successor_retirement.generation_id = successor.generation_id
                    )
              )
        ) INTO v_eligible;

        IF NOT v_eligible THEN
            CONTINUE;
        END IF;

        PERFORM flashback_begin_generation_retirement(
            rec.generation_id, 'retention_window_elapsed'
        );
        v_actions := v_actions + 1;
    END LOOP;

    -- Legacy trigger lifecycles have no qualified generations. Keep their old
    -- best-effort age policy isolated from the WAL-local correctness path.
    FOR rec IN
        SELECT rel_oid, retention_interval
        FROM flashback.tracked_tables
        WHERE is_active
          AND NOT EXISTS (
              SELECT 1 FROM flashback.coverage_generations cg
              WHERE cg.tracking_id = tracked_tables.tracking_id
          )
    LOOP
        DELETE FROM flashback.delta_log d
        WHERE d.rel_oid = rec.rel_oid
          AND d.committed_at < clock_timestamp() - rec.retention_interval;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        IF v_rows > 0 THEN
            v_actions := v_actions + 1;
        END IF;

        IF v_rows > 0 THEN
            UPDATE flashback.tracked_tables
               SET retention_cutoff = GREATEST(
                   retention_cutoff,
                   clock_timestamp() - rec.retention_interval
               )
             WHERE rel_oid = rec.rel_oid;
        END IF;

        FOR snap_rec IN
            SELECT snapshot_id, snapshot_table
            FROM flashback.snapshots s
            WHERE s.rel_oid = rec.rel_oid
              AND s.tracking_id IS NULL
              AND s.captured_at < clock_timestamp() - rec.retention_interval
        LOOP
            IF snap_rec.snapshot_table IS NOT NULL
               AND snap_rec.snapshot_table ~ '^flashback\\."?[a-zA-Z0-9_]+"?$'
            THEN
                PERFORM flashback_drop_payload_table(
                    to_regclass(snap_rec.snapshot_table)
                );
            END IF;
            DELETE FROM flashback.snapshots
            WHERE snapshot_id = snap_rec.snapshot_id;
            v_actions := v_actions + 1;
        END LOOP;
    END LOOP;

    v_actions := v_actions + flashback_drop_empty_delta_partitions();
    RETURN v_actions;
END;
$$;
