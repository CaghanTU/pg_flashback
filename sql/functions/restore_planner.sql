-- =================================================================
-- Restore planner: shadow-table based point-in-time restore,
-- multi-table restore, and flashback_query (SELECT AS OF).
-- =================================================================

-- ----------------------------------------------------------------
-- flashback_restore (single table)
-- ----------------------------------------------------------------
-- Uses shadow-table pattern for crash safety and minimal lock time:
--   1. Create shadow table in flashback schema
--   2. Load snapshot + replay deltas into shadow (no lock on original)
--   3. Atomic swap: DROP original → RENAME shadow (brief lock)
--   4. Restore sequences, re-attach triggers, post-restore checkpoint
-- If anything fails before the swap, the original table is untouched.
-- ----------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_restore(target_table text, target_time timestamptz)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_rel_oid oid;
    v_schema_name text;
    v_table_name text;
    v_base_snapshot_table text;
    v_tracked_since timestamptz;
    v_start_at timestamptz;
    v_start_table text;
    v_target_schema_version bigint;
    v_target_schema_def jsonb;
    v_start_schema_def jsonb;
    v_skipped_defaults jsonb;
    -- Shadow table variables
    v_shadow_schema text := 'flashback';
    v_shadow_name   text;
    shadow_oid      oid;
    current_rel_oid oid;
    rec record;
    def_rec record;
    pred text;
    cols text;
    vals text;
    v_set_clause text;
    v_pk_pred text;
    applied bigint := 0;
    v_total_events bigint;
    v_restore_start timestamptz;
    v_has_pk boolean;
    v_has_ddl boolean;
BEGIN
    v_restore_start := clock_timestamp();

    -- Serialize concurrent restores with an advisory lock.
    PERFORM pg_advisory_xact_lock(358944::integer, to_regclass(target_table)::oid::integer);

    PERFORM flashback_set_restore_in_progress(true);

    SELECT tt.rel_oid, tt.schema_name, tt.table_name, tt.base_snapshot_table, tt.tracked_since
        INTO v_rel_oid, v_schema_name, v_table_name, v_base_snapshot_table, v_tracked_since
    FROM flashback.tracked_tables tt
    WHERE tt.is_active
      AND (
          tt.rel_oid = to_regclass(target_table)::oid
          OR format('%I.%I', tt.schema_name, tt.table_name) = target_table
          OR (position('.' IN target_table) = 0 AND tt.table_name = target_table)
      )
    ORDER BY
        (tt.rel_oid = to_regclass(target_table)::oid) DESC,
        (format('%I.%I', tt.schema_name, tt.table_name) = target_table) DESC,
        tt.tracked_since DESC
    LIMIT 1;

    IF v_rel_oid IS NULL THEN
        RAISE EXCEPTION 'flashback_restore: table % is not tracked', target_table;
    END IF;

    IF target_time < v_tracked_since THEN
        RAISE EXCEPTION 'flashback_restore: target_time % is before tracked_since %', target_time, v_tracked_since;
    END IF;

    SELECT
        sv.schema_version,
        jsonb_build_object(
            'schema', v_schema_name,
            'table', v_table_name,
            'columns', COALESCE(sv.columns, '[]'::jsonb),
            'primary_key', COALESCE(sv.primary_key, '[]'::jsonb),
            'constraints', COALESCE(sv.constraints -> 'check_unique_fk', '[]'::jsonb),
            'indexes', COALESCE(sv.constraints -> 'indexes', '[]'::jsonb),
            'partition_by', sv.constraints -> 'partition_by',
            'partitions', sv.constraints -> 'partitions',
            'triggers', COALESCE(sv.constraints -> 'triggers', '[]'::jsonb),
            'rls_policies', COALESCE(sv.constraints -> 'rls_policies', '[]'::jsonb),
            'rls_enabled', COALESCE((sv.constraints -> 'rls_enabled')::boolean, false)
        )
      INTO v_target_schema_version, v_target_schema_def
    FROM flashback.schema_versions sv
    WHERE sv.rel_oid = v_rel_oid
      AND sv.applied_at <= target_time
    ORDER BY sv.schema_version DESC
    LIMIT 1;

    IF v_target_schema_def IS NULL THEN
        v_target_schema_version := 1;
        v_target_schema_def := COALESCE(flashback_collect_schema_def(v_rel_oid), '{}'::jsonb);
    END IF;

    SELECT s.captured_at, s.snapshot_table, s.schema_def
      INTO v_start_at, v_start_table, v_start_schema_def
    FROM flashback.snapshots s
    WHERE s.rel_oid = v_rel_oid
      AND s.captured_at <= target_time
    ORDER BY s.captured_at DESC
    LIMIT 1;

    IF v_start_table IS NULL OR v_start_table = '' THEN
        v_start_table := v_base_snapshot_table;
        v_start_at := v_tracked_since;

        SELECT
            jsonb_build_object(
                'schema', v_schema_name,
                'table', v_table_name,
                'columns', COALESCE(sv.columns, '[]'::jsonb),
                'primary_key', COALESCE(sv.primary_key, '[]'::jsonb),
                'constraints', COALESCE(sv.constraints -> 'check_unique_fk', '[]'::jsonb),
                'indexes', COALESCE(sv.constraints -> 'indexes', '[]'::jsonb),
                'partition_by', sv.constraints -> 'partition_by',
                'partitions', sv.constraints -> 'partitions',
                'triggers', COALESCE(sv.constraints -> 'triggers', '[]'::jsonb),
                'rls_policies', COALESCE(sv.constraints -> 'rls_policies', '[]'::jsonb),
                'rls_enabled', COALESCE((sv.constraints -> 'rls_enabled')::boolean, false)
            )
          INTO v_start_schema_def
        FROM flashback.schema_versions sv
        WHERE sv.rel_oid = v_rel_oid
        ORDER BY sv.schema_version ASC
        LIMIT 1;
    END IF;

    IF v_start_table IS NULL OR v_start_table = '' THEN
        RAISE EXCEPTION 'flashback_restore: no base snapshot/checkpoint found for %', target_table;
    END IF;

    -- ────────────────────────────────────────────────────────────
    -- Phase 1: Create shadow table (original table is untouched)
    -- ────────────────────────────────────────────────────────────
    v_shadow_name := '__fb_shadow_' || v_rel_oid::text;

    v_skipped_defaults := flashback_recreate_table_from_ddl(
        v_target_schema_def, v_shadow_schema, v_shadow_name
    );

    shadow_oid := to_regclass(format('%I.%I', v_shadow_schema, v_shadow_name))::oid;
    IF shadow_oid IS NULL THEN
        RAISE EXCEPTION 'flashback_restore: failed to create shadow table %.%',
            v_shadow_schema, v_shadow_name;
    END IF;

    RAISE NOTICE 'flashback_restore [%]: shadow table created as %.%',
        target_table, v_shadow_schema, v_shadow_name;

    -- ────────────────────────────────────────────────────────────
    -- Phase 2: Load snapshot into shadow table (no indexes = fast bulk load)
    -- ────────────────────────────────────────────────────────────
    -- Shadow table has no PK or secondary indexes at this point.
    -- This makes INSERT...SELECT significantly faster for large tables
    -- because there's no index maintenance overhead during load.
    PERFORM set_config('work_mem',
        COALESCE(NULLIF(current_setting('pg_flashback.restore_work_mem', true), ''), '256MB'),
        true);

    EXECUTE format(
        'INSERT INTO %I.%I SELECT * FROM %s',
        v_shadow_schema, v_shadow_name, v_start_table
    );
    RAISE NOTICE 'flashback_restore [%]: snapshot loaded into shadow from %',
        target_table, v_start_table;

    -- ────────────────────────────────────────────────────────────
    -- Phase 2b: Add PK to shadow (needed for batch delta replay)
    -- ────────────────────────────────────────────────────────────
    -- Building PK index AFTER bulk load is much faster than maintaining
    -- it during each INSERT (single sort+write vs per-row updates).
    PERFORM flashback_apply_deferred_pk(
        v_shadow_schema, v_shadow_name, v_target_schema_def
    );

    -- ────────────────────────────────────────────────────────────
    -- Phase 3: Replay delta events into shadow table
    -- ────────────────────────────────────────────────────────────
    -- Choose replay strategy:
    --   • PK table with no DDL barriers → batch (net-effect) path
    --   • Otherwise → row-by-row path (handles DDL events safely)

    -- Check for DDL events in the replay range
    SELECT EXISTS(
        SELECT 1 FROM flashback.delta_log d
        WHERE d.rel_oid = v_rel_oid
          AND d.committed_at IS NOT NULL
          AND d.committed_at > v_start_at  -- partition pruning lower bound
          AND d.event_time <= target_time   -- accurate PITR: use tx commit time, not worker flush time
          AND d.event_type IN ('ALTER', 'DROP', 'TRUNCATE')
    ) INTO v_has_ddl;

    -- Check if shadow table has a primary key
    SELECT EXISTS(
        SELECT 1 FROM pg_index i
        WHERE i.indrelid = shadow_oid AND i.indisprimary
    ) INTO v_has_pk;

    IF v_has_pk AND NOT v_has_ddl THEN
        -- ── Batch replay path (PK, pure DML) ────────────────────
        RAISE NOTICE 'flashback_restore [%]: using batch replay (PK table, no DDL events)',
            target_table;

        applied := flashback_replay_batch_pk(
            v_shadow_schema, v_shadow_name, shadow_oid,
            v_rel_oid, v_start_at, target_time, target_table
        );
    ELSIF v_has_pk AND v_has_ddl THEN
        -- ── Batch replay with DDL barriers ──────────────────────
        -- The batch function handles TRUNCATE/DROP barriers internally
        RAISE NOTICE 'flashback_restore [%]: using batch replay (PK table, with DDL barriers)',
            target_table;

        applied := flashback_replay_batch_pk(
            v_shadow_schema, v_shadow_name, shadow_oid,
            v_rel_oid, v_start_at, target_time, target_table
        );
    ELSE
        -- ── Row-by-row replay path (no PK) ──────────────────────
        SELECT count(*) INTO v_total_events
        FROM flashback.delta_log d
        WHERE d.rel_oid = v_rel_oid
          AND d.committed_at IS NOT NULL
          AND d.committed_at > v_start_at  -- partition pruning lower bound
          AND d.event_time <= target_time;  -- accurate PITR: tx commit time

        RAISE NOTICE 'flashback_restore [%]: using row-by-row replay (no PK, % events)',
            target_table, v_total_events;

        FOR rec IN
            SELECT d.event_id, d.event_type, d.old_data, d.new_data
            FROM flashback.delta_log d
            WHERE d.rel_oid = v_rel_oid
              AND d.committed_at IS NOT NULL
              AND d.committed_at > v_start_at  -- partition pruning lower bound
              AND d.event_time <= target_time   -- accurate PITR: tx commit time
            ORDER BY d.event_id ASC
        LOOP
            shadow_oid := to_regclass(format('%I.%I', v_shadow_schema, v_shadow_name))::oid;

            IF rec.event_type = 'ALTER' THEN
                CONTINUE;
            ELSIF rec.event_type = 'DROP' THEN
                EXECUTE format('DROP TABLE IF EXISTS %I.%I', v_shadow_schema, v_shadow_name);
            ELSIF rec.event_type = 'TRUNCATE' THEN
                IF shadow_oid IS NOT NULL THEN
                    EXECUTE format('TRUNCATE TABLE %I.%I', v_shadow_schema, v_shadow_name);
                END IF;
            ELSIF rec.event_type = 'INSERT' THEN
                IF shadow_oid IS NULL THEN CONTINUE; END IF;
                SELECT col_list, val_list
                  INTO cols, vals
                FROM flashback_build_insert_parts(shadow_oid, rec.new_data);
                IF cols IS NOT NULL AND cols <> '' THEN
                    EXECUTE format(
                        'INSERT INTO %I.%I (%s) VALUES (%s)',
                        v_shadow_schema, v_shadow_name, cols, vals);
                END IF;
            ELSIF rec.event_type = 'DELETE' THEN
                IF shadow_oid IS NULL THEN CONTINUE; END IF;
                pred := flashback_build_predicate(shadow_oid, rec.old_data);
                IF pred IS NOT NULL AND pred <> '' THEN
                    EXECUTE format(
                        'DELETE FROM %I.%I WHERE (tableoid, ctid) IN (SELECT tableoid, ctid FROM %I.%I WHERE %s LIMIT 1)',
                        v_shadow_schema, v_shadow_name,
                        v_shadow_schema, v_shadow_name, pred);
                END IF;
            ELSIF rec.event_type = 'UPDATE' THEN
                IF shadow_oid IS NULL THEN CONTINUE; END IF;
                -- Full-row UPDATE: DELETE old + INSERT new (no PK tables)
                pred := flashback_build_predicate(shadow_oid, rec.old_data);
                IF pred IS NOT NULL AND pred <> '' THEN
                    EXECUTE format(
                        'DELETE FROM %I.%I WHERE (tableoid, ctid) IN (SELECT tableoid, ctid FROM %I.%I WHERE %s LIMIT 1)',
                        v_shadow_schema, v_shadow_name,
                        v_shadow_schema, v_shadow_name, pred);
                END IF;
                SELECT col_list, val_list
                  INTO cols, vals
                FROM flashback_build_insert_parts(shadow_oid, rec.new_data);
                IF cols IS NOT NULL AND cols <> '' THEN
                    EXECUTE format(
                        'INSERT INTO %I.%I (%s) VALUES (%s)',
                        v_shadow_schema, v_shadow_name, cols, vals);
                END IF;
            END IF;

            applied := applied + 1;
            IF applied % 10000 = 0 THEN
                RAISE NOTICE 'flashback_restore [%]: % / % events applied (% %%)',
                    target_table, applied, v_total_events,
                    round(100.0 * applied / GREATEST(v_total_events, 1), 1);
            END IF;
        END LOOP;
    END IF;

    -- ────────────────────────────────────────────────────────────
    -- Phase 3b: Build deferred secondary indexes on shadow table
    -- ────────────────────────────────────────────────────────────
    -- Creating indexes AFTER data load + delta replay is much faster
    -- than maintaining them during each operation. For a 10M row table,
    -- this can be 3-10x faster than building indexes before data load.
    PERFORM flashback_apply_deferred_indexes(
        v_schema_name, v_table_name,
        v_shadow_schema, v_shadow_name,
        v_target_schema_def
    );

    RAISE NOTICE 'flashback_restore [%]: deferred indexes built on shadow table',
        target_table;

    -- ────────────────────────────────────────────────────────────
    -- Phase 4: Atomic swap — brief exclusive lock on original
    -- ────────────────────────────────────────────────────────────
    RAISE NOTICE 'flashback_restore [%]: swapping shadow → original (brief lock) ...',
        target_table;

    current_rel_oid := flashback_finalize_shadow_swap(
        v_schema_name, v_table_name,
        v_shadow_schema, v_shadow_name,
        v_target_schema_def
    );

    IF current_rel_oid IS NULL THEN
        RAISE EXCEPTION 'flashback_restore: shadow swap failed for %.%',
            v_schema_name, v_table_name;
    END IF;

    -- Propagate new OID to all flashback metadata
    IF current_rel_oid <> v_rel_oid THEN
        UPDATE flashback.tracked_tables  SET rel_oid = current_rel_oid WHERE rel_oid = v_rel_oid;
        UPDATE flashback.delta_log       SET rel_oid = current_rel_oid WHERE rel_oid = v_rel_oid;
        UPDATE flashback.snapshots       SET rel_oid = current_rel_oid WHERE rel_oid = v_rel_oid;
        UPDATE flashback.schema_versions SET rel_oid = current_rel_oid WHERE rel_oid = v_rel_oid;
        v_rel_oid := current_rel_oid;
    END IF;

    -- ────────────────────────────────────────────────────────────
    -- Phase 5: Restore serial defaults (after swap — table has original name)
    -- ────────────────────────────────────────────────────────────
    IF to_regclass(format('%I.%I', v_schema_name, v_table_name)) IS NOT NULL THEN
        FOR def_rec IN
            SELECT (elem->>'col') AS col_name, (elem->>'default_expr') AS default_expr
            FROM jsonb_array_elements(COALESCE(v_skipped_defaults, '[]'::jsonb)) AS elem
        LOOP
            DECLARE
                v_seq_name text;
                v_seq_schema text;
                v_seq_bare text;
                v_max_val bigint;
            BEGIN
                v_seq_name := substring(def_rec.default_expr FROM $re$nextval\('([^']+)'$re$);
                IF v_seq_name IS NULL THEN
                    v_seq_name := substring(def_rec.default_expr FROM $re$nextval\("([^"]+)"$re$);
                END IF;

                IF v_seq_name IS NOT NULL THEN
                    v_seq_name := regexp_replace(v_seq_name, '::[a-zA-Z_ ]+$', '');

                    IF position('.' IN v_seq_name) > 0 THEN
                        v_seq_schema := split_part(v_seq_name, '.', 1);
                        v_seq_bare   := split_part(v_seq_name, '.', 2);
                    ELSE
                        v_seq_schema := v_schema_name;
                        v_seq_bare   := v_seq_name;
                    END IF;

                    IF to_regclass(format('%I.%I', v_seq_schema, v_seq_bare)) IS NULL THEN
                        EXECUTE format('CREATE SEQUENCE %I.%I', v_seq_schema, v_seq_bare);
                    END IF;

                    EXECUTE format(
                        'SELECT COALESCE(MAX(%I), 0) FROM %I.%I',
                        def_rec.col_name, v_schema_name, v_table_name
                    ) INTO v_max_val;

                    IF v_max_val > 0 THEN
                        EXECUTE format('SELECT setval(%L, %s)',
                            format('%I.%I', v_seq_schema, v_seq_bare), v_max_val);
                    END IF;

                    EXECUTE format(
                        'ALTER SEQUENCE %I.%I OWNED BY %I.%I.%I',
                        v_seq_schema, v_seq_bare,
                        v_schema_name, v_table_name, def_rec.col_name
                    );
                END IF;

                EXECUTE format(
                    'ALTER TABLE %I.%I ALTER COLUMN %I SET DEFAULT %s',
                    v_schema_name, v_table_name,
                    def_rec.col_name, def_rec.default_expr
                );
            END;
        END LOOP;
    END IF;

    -- Re-attach capture after the swap
    -- In trigger mode: install the statement-level triggers on the restored table.
    -- In WAL mode: do NOT attach triggers (staging_events is skipped by the worker
    -- in WAL mode, so any trigger-captured rows would accumulate and never reach
    -- delta_log). Instead, re-assert REPLICA IDENTITY FULL because the shadow swap
    -- creates a new OID and the table relation is fresh.
    IF flashback_effective_capture_mode() = 'trigger' THEN
        PERFORM flashback_attach_capture_trigger(v_schema_name, v_table_name);
    ELSE
        EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY FULL', v_schema_name, v_table_name);
    END IF;

    -- Post-restore checkpoint
    DECLARE
        v_post_snap_id bigint;
        v_post_snap_tbl text;
        v_post_row_count bigint;
    BEGIN
        INSERT INTO flashback.snapshots (
            rel_oid, snapshot_table, snapshot_lsn, schema_def, row_count, captured_at
        ) VALUES (
            v_rel_oid, '', pg_current_wal_lsn(),
            COALESCE(flashback_collect_schema_def(v_rel_oid), '{}'::jsonb),
            0, clock_timestamp()
        ) RETURNING snapshot_id INTO v_post_snap_id;

        v_post_snap_tbl := format('snap_%s_%s', v_rel_oid::text, v_post_snap_id::text);

        EXECUTE format('DROP TABLE IF EXISTS flashback.%I', v_post_snap_tbl);
        EXECUTE format(
            'CREATE TABLE flashback.%I AS TABLE %I.%I',
            v_post_snap_tbl, v_schema_name, v_table_name
        );
        EXECUTE format('SELECT count(*) FROM flashback.%I', v_post_snap_tbl)
          INTO v_post_row_count;

        UPDATE flashback.snapshots
           SET snapshot_table = format('flashback.%I', v_post_snap_tbl),
               row_count      = v_post_row_count,
               captured_at    = clock_timestamp()
         WHERE snapshot_id = v_post_snap_id;

        RAISE NOTICE 'flashback_restore [%]: post-restore checkpoint created (snap_id=%, rows=%)',
            target_table, v_post_snap_id, v_post_row_count;
    END;

    -- Log successful restore
    IF to_regclass('flashback.restore_log') IS NOT NULL THEN
        INSERT INTO flashback.restore_log (table_name, target_time, rows_affected, success, error_message)
        VALUES (target_table, target_time, applied, true, NULL);
    END IF;

    RAISE NOTICE 'flashback_restore [%]: complete — % events applied, duration %',
        target_table, applied,
        (clock_timestamp() - v_restore_start)::interval;

    PERFORM flashback_set_restore_in_progress(false);
    RETURN applied;
EXCEPTION WHEN OTHERS THEN
    -- Crash safety: clean up shadow table if it exists, original is untouched
    IF v_shadow_name IS NOT NULL THEN
        EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', v_shadow_schema, v_shadow_name);
    END IF;

    IF to_regclass('flashback.restore_log') IS NOT NULL THEN
        INSERT INTO flashback.restore_log (table_name, target_time, rows_affected, success, error_message)
        VALUES (target_table, target_time, 0, false, SQLERRM);
    END IF;

    PERFORM flashback_set_restore_in_progress(false);
    RAISE;
END;
$$;

-- ----------------------------------------------------------------
-- flashback_restore (multi-table)
-- ----------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_restore(tables text[], target_time timestamptz)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    tbl text;
    ordered_tables text[];
    restored_total bigint := 0;
BEGIN
    IF tables IS NULL OR array_length(tables, 1) IS NULL THEN
        RAISE EXCEPTION 'flashback_restore: tables array is empty';
    END IF;

    WITH RECURSIVE rels AS (
        SELECT t.tbl,
               t.ord,
               to_regclass(t.tbl) AS relid
        FROM unnest(tables) WITH ORDINALITY AS t(tbl, ord)
    ),
    edges AS (
        SELECT c.conrelid AS child_relid,
               c.confrelid AS parent_relid
        FROM pg_constraint c
        JOIN rels child_r ON child_r.relid = c.conrelid
        JOIN rels parent_r ON parent_r.relid = c.confrelid
        WHERE c.contype = 'f'
    ),
    walk AS (
        SELECT r.relid AS start_relid,
               r.relid AS current_relid,
               0::int AS depth
        FROM rels r
        WHERE r.relid IS NOT NULL

        UNION ALL

        SELECT w.start_relid,
               e.parent_relid AS current_relid,
               w.depth + 1
        FROM walk w
        JOIN edges e
          ON e.child_relid = w.current_relid
        WHERE w.depth < 100
    ),
    max_depth AS (
        SELECT w.start_relid AS relid,
               max(w.depth) AS depth
        FROM walk w
        GROUP BY w.start_relid
    )
    SELECT array_agg(r.tbl ORDER BY COALESCE(md.depth, 0) ASC, r.ord)
    INTO ordered_tables
    FROM rels r
    LEFT JOIN max_depth md
      ON md.relid = r.relid;

    IF ordered_tables IS NULL OR array_length(ordered_tables, 1) IS NULL THEN
        ordered_tables := tables;
    END IF;

    PERFORM set_config('session_replication_role', 'replica', true);

    BEGIN
        FOREACH tbl IN ARRAY ordered_tables
        LOOP
            restored_total := restored_total + flashback_restore(tbl, target_time);
        END LOOP;

        PERFORM set_config('session_replication_role', 'origin', true);
        RETURN restored_total;
    EXCEPTION WHEN OTHERS THEN
        PERFORM set_config('session_replication_role', 'origin', true);
        RAISE;
    END;
END;
$$;

-- ----------------------------------------------------------------
-- flashback_query: point-in-time SELECT without modifying the table
-- ----------------------------------------------------------------
-- Creates a temp table using the *historical* schema at target_time
-- (from schema_versions), not LIKE the current live table. This means
-- schema evolution (ADD/DROP/ALTER COLUMN) is reflected correctly.
--
-- Parameters:
--   target_table  – tracked table name (schema.table or bare name)
--   target_time   – point in time to query
--   filter_clause – optional WHERE condition (e.g. 'id = 5 AND status = ''active''').
--                   ONLY a boolean predicate — NOT a full SQL query.
--                   The condition is appended as: SELECT * FROM <temp> WHERE <filter_clause>
--                   Semicolons and DML/DDL keywords are rejected for safety.
--
-- Limitations:
--   • ALTER events in the replay range are replayed as data events only
--     (column set is fixed at target_time schema). Columns that were
--     added or removed between the snapshot and target_time are handled
--     by the column lists in old_data/new_data, not by schema mutation.
--   • DROP TABLE events: if the table was dropped before target_time,
--     the function returns 0 rows with a NOTICE (the table did not exist
--     at that point in time).
--   • filter_clause is passed to EXECUTE — callers should not interpolate
--     untrusted user input directly into filter_clause without parameterisation.
-- ----------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_query(
    target_table  text,
    target_time   timestamptz,
    filter_clause text DEFAULT NULL
)
RETURNS SETOF record
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_rel_oid oid;
    v_schema_name text;
    v_table_name text;
    v_base_snapshot_table text;
    v_tracked_since timestamptz;
    v_start_at timestamptz;
    v_start_table text;
    v_target_schema_def jsonb;
    v_col_defs text;
    v_tmp text;
    rec record;
    pred text;
    cols text;
    vals text;
    v_set_clause text;
    v_pk_pred text;
    v_query text;
    v_table_dropped boolean := false;
BEGIN
    SELECT tt.rel_oid, tt.schema_name, tt.table_name, tt.base_snapshot_table, tt.tracked_since
      INTO v_rel_oid, v_schema_name, v_table_name, v_base_snapshot_table, v_tracked_since
    FROM flashback.tracked_tables tt
    WHERE tt.is_active
      AND (
          tt.rel_oid = to_regclass(target_table)::oid
          OR format('%I.%I', tt.schema_name, tt.table_name) = target_table
          OR (position('.' IN target_table) = 0 AND tt.table_name = target_table)
      )
    ORDER BY
        (tt.rel_oid = to_regclass(target_table)::oid) DESC,
        (format('%I.%I', tt.schema_name, tt.table_name) = target_table) DESC,
        tt.tracked_since DESC
    LIMIT 1;

    IF v_rel_oid IS NULL THEN
        RAISE EXCEPTION 'flashback_query: table % is not tracked', target_table;
    END IF;
    IF target_time < v_tracked_since THEN
        RAISE EXCEPTION 'flashback_query: target_time % is before tracked_since %', target_time, v_tracked_since;
    END IF;

    -- ── Historical schema at target_time ────────────────────────
    -- Use schema_versions snapshot instead of LIKE <live table> so that
    -- ADD/DROP/ALTER COLUMN changes are reflected correctly.
    SELECT
        jsonb_build_object(
            'schema', v_schema_name,
            'table',  v_table_name,
            'columns', COALESCE(sv.columns, '[]'::jsonb),
            'primary_key', COALESCE(sv.primary_key, '[]'::jsonb)
        )
      INTO v_target_schema_def
    FROM flashback.schema_versions sv
    WHERE sv.rel_oid = v_rel_oid
      AND sv.applied_at <= target_time
    ORDER BY sv.schema_version DESC
    LIMIT 1;

    -- Fall back to collecting current live schema if no schema_version entry
    IF v_target_schema_def IS NULL THEN
        v_target_schema_def := COALESCE(
            flashback_collect_schema_def(v_rel_oid),
            jsonb_build_object(
                'schema', v_schema_name,
                'table', v_table_name,
                'columns', '[]'::jsonb,
                'primary_key', '[]'::jsonb
            )
        );
    END IF;

    -- Build column definitions for the temp table from historical schema
    SELECT string_agg(
        format('%I %s%s',
            col->>'name',
            col->>'type',
            CASE WHEN COALESCE((col->>'not_null')::boolean, false) THEN ' NOT NULL' ELSE '' END
        ),
        ', '
        ORDER BY ord
    )
      INTO v_col_defs
    FROM jsonb_array_elements(v_target_schema_def->'columns') WITH ORDINALITY AS t(col, ord)
    WHERE col->>'name' IS NOT NULL
      AND col->>'generated' IS DISTINCT FROM 'always';  -- skip generated columns

    -- If schema_versions has no column info, fall back to LIKE current table
    IF v_col_defs IS NULL OR v_col_defs = '' THEN
        v_col_defs := NULL;
    END IF;

    -- ── Find nearest snapshot ────────────────────────────────────
    SELECT s.captured_at, s.snapshot_table
      INTO v_start_at, v_start_table
    FROM flashback.snapshots s
    WHERE s.rel_oid = v_rel_oid AND s.captured_at <= target_time
    ORDER BY s.captured_at DESC LIMIT 1;

    IF v_start_table IS NULL OR v_start_table = '' THEN
        v_start_table := v_base_snapshot_table;
        v_start_at := v_tracked_since;
    END IF;
    IF v_start_table IS NULL THEN
        RAISE EXCEPTION 'flashback_query: no snapshot for %', target_table;
    END IF;

    -- ── Build temp table from historical column list ─────────────
    v_tmp := '_fb_query_' || pg_backend_pid()::text
             || '_' || extract(epoch FROM clock_timestamp())::bigint::text
             || '_' || (random() * 100000)::integer::text;

    IF v_col_defs IS NOT NULL THEN
        EXECUTE format('CREATE TEMP TABLE %I (%s) ON COMMIT DROP', v_tmp, v_col_defs);
    ELSE
        -- Last-resort fallback: LIKE current live table (schema may differ from target_time)
        RAISE NOTICE 'flashback_query: no schema_versions entry for % — using current table schema (results may differ for schema-evolved tables)', target_table;
        EXECUTE format('CREATE TEMP TABLE %I (LIKE %I.%I) ON COMMIT DROP', v_tmp, v_schema_name, v_table_name);
    END IF;
    EXECUTE format('INSERT INTO %I SELECT * FROM %s', v_tmp, v_start_table);

    -- ── Replay delta events ──────────────────────────────────────
    FOR rec IN
        SELECT d.event_type, d.old_data, d.new_data, d.event_id
        FROM flashback.delta_log d
        WHERE d.rel_oid = v_rel_oid
          AND d.committed_at IS NOT NULL
          AND d.committed_at > v_start_at  -- partition pruning lower bound
          AND d.event_time <= target_time   -- accurate PITR: tx commit time
        ORDER BY d.event_id ASC
    LOOP
        -- DROP: table did not exist at target_time → return 0 rows
        IF rec.event_type = 'DROP' THEN
            RAISE NOTICE 'flashback_query: table % was dropped at or before % (event_id=%) — returning empty result',
                target_table, target_time, rec.event_id;
            EXECUTE format('TRUNCATE %I', v_tmp);
            v_table_dropped := true;
            EXIT;  -- no further replay possible after a DROP

        ELSIF rec.event_type = 'TRUNCATE' THEN
            EXECUTE format('TRUNCATE %I', v_tmp);

        ELSIF rec.event_type = 'ALTER' THEN
            -- ALTER events change column shape; the temp table was built from the
            -- target_time schema so column definitions are already correct.
            -- Skip the DDL replay — data events after this ALTER will use the
            -- updated old_data/new_data field sets automatically.
            RAISE NOTICE 'flashback_query: ALTER event skipped (temp table built from target_time schema)';

        ELSIF rec.event_type = 'INSERT' THEN
            SELECT col_list, val_list INTO cols, vals
            FROM flashback_build_insert_parts(v_rel_oid, rec.new_data);
            IF cols IS NOT NULL AND cols <> '' THEN
                EXECUTE format('INSERT INTO %I (%s) VALUES (%s)', v_tmp, cols, vals);
            END IF;

        ELSIF rec.event_type = 'DELETE' THEN
            pred := flashback_build_predicate(v_rel_oid, rec.old_data);
            IF pred IS NOT NULL AND pred <> '' THEN
                EXECUTE format(
                    'DELETE FROM %I WHERE (tableoid, ctid) IN (SELECT tableoid, ctid FROM %I WHERE %s LIMIT 1)',
                    v_tmp, v_tmp, pred);
            END IF;

        ELSIF rec.event_type = 'UPDATE' THEN
            SELECT us.set_clause, us.pk_predicate
              INTO v_set_clause, v_pk_pred
            FROM flashback_build_update_set(v_rel_oid, rec.new_data) us;

            IF v_set_clause IS NOT NULL AND v_set_clause <> ''
               AND v_pk_pred IS NOT NULL AND v_pk_pred <> '' THEN
                EXECUTE format('UPDATE %I SET %s WHERE %s', v_tmp, v_set_clause, v_pk_pred);
            ELSE
                -- No PK fallback: DELETE + INSERT
                pred := flashback_build_predicate(v_rel_oid, rec.old_data);
                IF pred IS NOT NULL AND pred <> '' THEN
                    EXECUTE format(
                        'DELETE FROM %I WHERE (tableoid, ctid) IN (SELECT tableoid, ctid FROM %I WHERE %s LIMIT 1)',
                        v_tmp, v_tmp, pred);
                END IF;
                SELECT col_list, val_list INTO cols, vals
                FROM flashback_build_insert_parts(v_rel_oid, rec.new_data);
                IF cols IS NOT NULL AND cols <> '' THEN
                    EXECUTE format('INSERT INTO %I (%s) VALUES (%s)', v_tmp, cols, vals);
                END IF;
            END IF;
        END IF;
    END LOOP;

    IF filter_clause IS NOT NULL THEN
        -- Safety: reject semicolons (statement stacking) and DML/DDL keywords at
        -- the statement level. filter_clause is a WHERE predicate, not a full query.
        IF filter_clause ~ ';' THEN
            RAISE EXCEPTION 'flashback_query: filter_clause must not contain semicolons';
        END IF;
        IF filter_clause ~* '\m(INSERT|UPDATE|DELETE|TRUNCATE|DROP|CREATE|ALTER|CALL|DO)\M' THEN
            RAISE EXCEPTION 'flashback_query: filter_clause must not contain DML or DDL keywords';
        END IF;
        v_query := format('SELECT * FROM %I WHERE %s', v_tmp, filter_clause);
    ELSE
        v_query := format('SELECT * FROM %I', v_tmp);
    END IF;

    RETURN QUERY EXECUTE v_query;
END;
$$;

-- ----------------------------------------------------------------
-- flashback_restore_parallel
-- ----------------------------------------------------------------
-- Parallel-hint restore: enables PostgreSQL's parallel query workers
-- for the batch replay CTEs, reducing restore time on large tables
-- on multi-core hardware.
--
-- For partitioned tables it also prints per-partition guidance so
-- operators know which partitions they can restore concurrently from
-- separate sessions.
--
-- Parameters
--   target_table  – table to restore (same syntax as flashback_restore)
--   target_time   – point-in-time target
--   num_workers   – hint for max parallel workers (1–8, default 4)
--
-- Returns: (restored_table text, events_applied bigint)
-- ----------------------------------------------------------------
CREATE OR REPLACE FUNCTION flashback_restore_parallel(
    target_table text,
    target_time  timestamptz,
    num_workers  int DEFAULT 4
)
RETURNS TABLE(restored_table text, events_applied bigint)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_workers    int  := LEAST(GREATEST(num_workers, 1), 8);
    v_rel_oid    oid;
    v_relkind    char;
    v_schema     text;
    v_tblname    text;
    v_applied    bigint;
    v_part       record;
BEGIN
    -- Resolve the table
    SELECT c.oid, c.relkind, n.nspname, c.relname
      INTO v_rel_oid, v_relkind, v_schema, v_tblname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = to_regclass(target_table);

    IF v_rel_oid IS NULL THEN
        RAISE EXCEPTION 'flashback_restore_parallel: table % not found', target_table;
    END IF;

    -- Enable parallel query for hash joins and sequential scans used by
    -- flashback_replay_batch_pk's CTEs.  SET LOCAL so it is scoped to
    -- the current transaction only.
    EXECUTE format('SET LOCAL max_parallel_workers_per_gather = %s', v_workers);
    EXECUTE format('SET LOCAL max_parallel_maintenance_workers = %s', v_workers);
    -- Allow the leader to also execute scan work (not just coordinate)
    SET LOCAL parallel_leader_participation = on;

    -- For partitioned tables: print per-partition guidance.
    -- Individual partitions share the same delta_log (parent rel_oid),
    -- so the parent restore is the correct call.  However, if you want
    -- to restore multiple partitions truly in parallel, open separate
    -- database connections and call:
    --   SELECT flashback_restore('schema.partition_name', target_time);
    -- for each partition simultaneously.
    IF v_relkind = 'p' THEN
        RAISE NOTICE
            'flashback_restore_parallel [%]: partitioned table detected — '
            'all events are recorded under parent OID so the parent is restored atomically. '
            'For per-partition parallel execution, run flashback_restore() on each partition '
            'from separate connections (they are independent after flashback_untrack + re-track).',
            target_table;

        FOR v_part IN
            SELECT n.nspname AS sch, ch.relname AS tbl
            FROM pg_inherits inh
            JOIN pg_class ch ON ch.oid = inh.inhrelid
            JOIN pg_namespace n ON n.oid = ch.relnamespace
            WHERE inh.inhparent = v_rel_oid
            ORDER BY ch.relname
        LOOP
            RAISE NOTICE 'flashback_restore_parallel [%]:   partition → %.%',
                target_table, v_part.sch, v_part.tbl;
        END LOOP;
    END IF;

    -- Execute the standard restore (which already uses batch/set-based replay
    -- and will now benefit from the parallel worker GUCs set above).
    v_applied := flashback_restore(target_table, target_time);

    RETURN QUERY SELECT target_table, v_applied;
END;
$$;
