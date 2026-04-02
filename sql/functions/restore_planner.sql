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
    -- Phase 2: Load snapshot into shadow table
    -- ────────────────────────────────────────────────────────────
    EXECUTE format(
        'INSERT INTO %I.%I SELECT * FROM %s',
        v_shadow_schema, v_shadow_name, v_start_table
    );
    RAISE NOTICE 'flashback_restore [%]: snapshot loaded into shadow from %',
        target_table, v_start_table;

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
          AND d.event_time > v_start_at
          AND d.event_time <= target_time
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
          AND d.event_time > v_start_at
          AND d.event_time <= target_time;

        RAISE NOTICE 'flashback_restore [%]: using row-by-row replay (no PK, % events)',
            target_table, v_total_events;

        FOR rec IN
            SELECT d.event_id, d.event_type, d.old_data, d.new_data
            FROM flashback.delta_log d
            WHERE d.rel_oid = v_rel_oid
              AND d.committed_at IS NOT NULL
              AND d.event_time > v_start_at
              AND d.event_time <= target_time
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

    -- Re-attach capture triggers on the restored table
    PERFORM flashback_attach_capture_trigger(v_schema_name, v_table_name);

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
CREATE OR REPLACE FUNCTION flashback_query(
    target_table text,
    target_time  timestamptz,
    query        text DEFAULT NULL
)
RETURNS SETOF record
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
    v_start_schema_def jsonb;
    v_tmp text;
    rec record;
    pred text;
    cols text;
    vals text;
    v_set_clause text;
    v_pk_pred text;
    v_query text;
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

    v_tmp := '_fb_query_' || pg_backend_pid()::text || '_' || extract(epoch FROM clock_timestamp())::bigint::text || '_' || (random() * 100000)::integer::text;
    EXECUTE format('CREATE TEMP TABLE %I (LIKE %I.%I INCLUDING ALL) ON COMMIT DROP', v_tmp, v_schema_name, v_table_name);
    EXECUTE format('INSERT INTO %I SELECT * FROM %s', v_tmp, v_start_table);

    FOR rec IN
        SELECT d.event_type, d.old_data, d.new_data
        FROM flashback.delta_log d
        WHERE d.rel_oid = v_rel_oid
          AND d.committed_at IS NOT NULL
          AND d.event_time > v_start_at
          AND d.event_time <= target_time
        ORDER BY d.event_id ASC
    LOOP
        IF rec.event_type = 'TRUNCATE' THEN
            EXECUTE format('TRUNCATE %s', v_tmp);
        ELSIF rec.event_type = 'INSERT' THEN
            SELECT col_list, val_list INTO cols, vals
            FROM flashback_build_insert_parts(
                to_regclass(format('%I.%I', v_schema_name, v_table_name))::oid,
                rec.new_data
            );
            IF cols IS NOT NULL AND cols <> '' THEN
                EXECUTE format('INSERT INTO %s (%s) VALUES (%s)', v_tmp, cols, vals);
            END IF;
        ELSIF rec.event_type = 'DELETE' THEN
            pred := flashback_build_predicate(
                to_regclass(format('%I.%I', v_schema_name, v_table_name))::oid,
                rec.old_data
            );
            IF pred IS NOT NULL AND pred <> '' THEN
                EXECUTE format('DELETE FROM %s WHERE (tableoid, ctid) IN (SELECT tableoid, ctid FROM %s WHERE %s LIMIT 1)', v_tmp, v_tmp, pred);
            END IF;
        ELSIF rec.event_type = 'UPDATE' THEN
            -- Try PK-based UPDATE SET (works for both diff and full format)
            SELECT us.set_clause, us.pk_predicate
              INTO v_set_clause, v_pk_pred
            FROM flashback_build_update_set(
                to_regclass(format('%I.%I', v_schema_name, v_table_name))::oid,
                rec.new_data
            ) us;

            IF v_set_clause IS NOT NULL AND v_set_clause <> ''
               AND v_pk_pred IS NOT NULL AND v_pk_pred <> '' THEN
                EXECUTE format('UPDATE %s SET %s WHERE %s', v_tmp, v_set_clause, v_pk_pred);
            ELSE
                -- No PK fallback: DELETE + INSERT
                pred := flashback_build_predicate(
                    to_regclass(format('%I.%I', v_schema_name, v_table_name))::oid,
                    rec.old_data
                );
                IF pred IS NOT NULL AND pred <> '' THEN
                    EXECUTE format('DELETE FROM %s WHERE (tableoid, ctid) IN (SELECT tableoid, ctid FROM %s WHERE %s LIMIT 1)', v_tmp, v_tmp, pred);
                END IF;
                SELECT col_list, val_list INTO cols, vals
                FROM flashback_build_insert_parts(
                    to_regclass(format('%I.%I', v_schema_name, v_table_name))::oid,
                    rec.new_data
                );
                IF cols IS NOT NULL AND cols <> '' THEN
                    EXECUTE format('INSERT INTO %s (%s) VALUES (%s)', v_tmp, cols, vals);
                END IF;
            END IF;
        END IF;
    END LOOP;

    IF query IS NOT NULL THEN
        v_query := replace(query, '$FB_TABLE', quote_ident(v_tmp));
    ELSE
        v_query := format('SELECT * FROM %I', v_tmp);
    END IF;

    RETURN QUERY EXECUTE v_query;
END;
$$;
