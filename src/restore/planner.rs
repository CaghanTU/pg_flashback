use pgrx::prelude::*;

extension_sql!(
    r#"
    CREATE OR REPLACE FUNCTION flashback_restore(target_table text, target_time timestamptz)
    RETURNS bigint
    LANGUAGE plpgsql
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
        current_rel_oid oid;
        rec record;
        def_rec record;
        pred text;
        cols text;
        vals text;
        applied bigint := 0;
        v_total_events bigint;
    BEGIN
        -- Serialize concurrent restores with an advisory lock.
        -- Uses xact-level lock: released automatically on COMMIT/ROLLBACK.
        -- Lock key is per-table (rel_oid), so unrelated tables can restore concurrently.
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

        v_skipped_defaults := flashback_recreate_table_from_ddl(v_target_schema_def);

        current_rel_oid := to_regclass(format('%I.%I', v_schema_name, v_table_name))::oid;
        IF current_rel_oid IS NULL THEN
            RAISE EXCEPTION 'flashback_restore: failed to create target table %.%', v_schema_name, v_table_name;
        END IF;

        -- Bulk-load snapshot using INSERT...SELECT (no row-by-row, no jsonb blob)
        EXECUTE format(
            'INSERT INTO %I.%I SELECT * FROM %s',
            v_schema_name, v_table_name, v_start_table
        );
        RAISE NOTICE 'flashback_restore [%]: snapshot loaded from %', target_table, v_start_table;

        -- Count total events to replay for progress reporting
        SELECT count(*) INTO v_total_events
        FROM flashback.delta_log d
        WHERE d.rel_oid = v_rel_oid
          AND d.committed_at IS NOT NULL
          AND d.event_time > v_start_at
          AND d.event_time <= target_time;

        RAISE NOTICE 'flashback_restore [%]: replaying % events ...', target_table, v_total_events;

        FOR rec IN
            SELECT d.event_id, d.event_type, d.old_data, d.new_data
            FROM flashback.delta_log d
            WHERE d.rel_oid = v_rel_oid
                            AND d.committed_at IS NOT NULL
              AND d.event_time > v_start_at
              AND d.event_time <= target_time
            ORDER BY d.event_id ASC
        LOOP
            current_rel_oid := to_regclass(format('%I.%I', v_schema_name, v_table_name))::oid;

            IF rec.event_type = 'ALTER' THEN
                CONTINUE;
            ELSIF rec.event_type = 'DROP' THEN
                EXECUTE format('DROP TABLE IF EXISTS %I.%I', v_schema_name, v_table_name);
            ELSIF rec.event_type = 'TRUNCATE' THEN
                IF current_rel_oid IS NOT NULL THEN
                    EXECUTE format('TRUNCATE TABLE %I.%I', v_schema_name, v_table_name);
                END IF;
            ELSIF rec.event_type = 'INSERT' THEN
                IF current_rel_oid IS NULL THEN
                    CONTINUE;
                END IF;

                SELECT col_list, val_list
                  INTO cols, vals
                FROM flashback_build_insert_parts(current_rel_oid, rec.new_data);

                IF cols IS NOT NULL AND cols <> '' THEN
                    EXECUTE format(
                        'INSERT INTO %I.%I (%s) VALUES (%s)',
                        v_schema_name,
                        v_table_name,
                        cols,
                        vals
                    );
                END IF;
            ELSIF rec.event_type = 'DELETE' THEN
                IF current_rel_oid IS NULL THEN
                    CONTINUE;
                END IF;

                pred := flashback_build_predicate(current_rel_oid, rec.old_data);
                IF pred IS NOT NULL AND pred <> '' THEN
                    EXECUTE format(
                        'DELETE FROM %I.%I WHERE (tableoid, ctid) IN (SELECT tableoid, ctid FROM %I.%I WHERE %s LIMIT 1)',
                        v_schema_name,
                        v_table_name,
                        v_schema_name,
                        v_table_name,
                        pred
                    );
                END IF;
            ELSIF rec.event_type = 'UPDATE' THEN
                IF current_rel_oid IS NULL THEN
                    CONTINUE;
                END IF;

                pred := flashback_build_predicate(current_rel_oid, rec.old_data);
                IF pred IS NOT NULL AND pred <> '' THEN
                    EXECUTE format(
                        'DELETE FROM %I.%I WHERE (tableoid, ctid) IN (SELECT tableoid, ctid FROM %I.%I WHERE %s LIMIT 1)',
                        v_schema_name,
                        v_table_name,
                        v_schema_name,
                        v_table_name,
                        pred
                    );
                END IF;

                SELECT col_list, val_list
                  INTO cols, vals
                                FROM flashback_build_insert_parts(current_rel_oid, rec.new_data);

                IF cols IS NOT NULL AND cols <> '' THEN
                    EXECUTE format(
                        'INSERT INTO %I.%I (%s) VALUES (%s)',
                        v_schema_name,
                        v_table_name,
                        cols,
                        vals
                    );
                END IF;
            END IF;

            applied := applied + 1;

            IF applied % 10000 = 0 THEN
                RAISE NOTICE 'flashback_restore [%]: % / % events applied (% %%)',
                    target_table, applied, v_total_events,
                    round(100.0 * applied / GREATEST(v_total_events, 1), 1);
            END IF;
        END LOOP;

        -- Restore serial defaults that were deferred to avoid DDL-hook recursion
        -- during CREATE TABLE.  Safe here: table exists, data is loaded, guard active.
        -- We must also recreate any sequences dropped by the earlier DROP TABLE CASCADE.
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
                    -- Extract bare sequence name from nextval('seq_name'::regclass) or nextval('schema.seq_name'::regclass)
                    v_seq_name := substring(def_rec.default_expr FROM $re$nextval\('([^']+)'$re$);
                    IF v_seq_name IS NULL THEN
                        v_seq_name := substring(def_rec.default_expr FROM $re$nextval\("([^"]+)"$re$);
                    END IF;

                    IF v_seq_name IS NOT NULL THEN
                        -- Remove ::regclass cast suffix if present
                        v_seq_name := regexp_replace(v_seq_name, '::[a-zA-Z_ ]+$', '');

                        -- Determine schema-qualified sequence name
                        IF position('.' IN v_seq_name) > 0 THEN
                            v_seq_schema := split_part(v_seq_name, '.', 1);
                            v_seq_bare   := split_part(v_seq_name, '.', 2);
                        ELSE
                            v_seq_schema := v_schema_name;
                            v_seq_bare   := v_seq_name;
                        END IF;

                        -- Recreate the sequence if it was dropped along with the table
                        IF to_regclass(format('%I.%I', v_seq_schema, v_seq_bare)) IS NULL THEN
                            EXECUTE format('CREATE SEQUENCE %I.%I', v_seq_schema, v_seq_bare);
                        END IF;

                        -- Advance sequence past current max column value to avoid PK conflicts
                        EXECUTE format(
                            'SELECT COALESCE(MAX(%I), 0) FROM %I.%I',
                            def_rec.col_name, v_schema_name, v_table_name
                        ) INTO v_max_val;

                        IF v_max_val > 0 THEN
                            EXECUTE format('SELECT setval(%L, %s)', format('%I.%I', v_seq_schema, v_seq_bare), v_max_val);
                        END IF;

                        -- Own the sequence to the column so DROP TABLE CASCADE works
                        EXECUTE format(
                            'ALTER SEQUENCE %I.%I OWNED BY %I.%I.%I',
                            v_seq_schema, v_seq_bare,
                            v_schema_name, v_table_name, def_rec.col_name
                        );
                    END IF;

                    -- Restore the DEFAULT expression on the column
                    EXECUTE format(
                        'ALTER TABLE %I.%I ALTER COLUMN %I SET DEFAULT %s',
                        v_schema_name,
                        v_table_name,
                        def_rec.col_name,
                        def_rec.default_expr
                    );
                END;
            END LOOP;
        END IF;

        -- Log successful restore to audit table
        IF to_regclass('flashback.restore_log') IS NOT NULL THEN
            INSERT INTO flashback.restore_log (table_name, target_time, rows_affected, success, error_message)
            VALUES (target_table, target_time, applied, true, NULL);
        END IF;

        RAISE NOTICE 'flashback_restore [%]: complete — % events applied', target_table, applied;
        PERFORM flashback_set_restore_in_progress(false);
        RETURN applied;
    EXCEPTION WHEN OTHERS THEN
        -- Log failed restore to audit table
        IF to_regclass('flashback.restore_log') IS NOT NULL THEN
            INSERT INTO flashback.restore_log (table_name, target_time, rows_affected, success, error_message)
            VALUES (target_table, target_time, 0, false, SQLERRM);
        END IF;

        PERFORM flashback_set_restore_in_progress(false);
        RAISE;
    END;
    $$;

    CREATE OR REPLACE FUNCTION flashback_restore(tables text[], target_time timestamptz)
    RETURNS bigint
    LANGUAGE plpgsql
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

        -- Restore child tables before parents to avoid FK dependency failures
        -- while DROP/CREATE is performed inside flashback_restore(tbl, ...).
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
        SELECT array_agg(r.tbl ORDER BY COALESCE(md.depth, 0) DESC, r.ord)
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
    -- Reconstructs the table state at `target_time` into a temporary
    -- table and executes the given query against it.  The query must
    -- contain the token `$FB_TABLE` which is replaced with the temp
    -- table name.  Returns SETOF record via a refcursor so the caller
    -- can iterate the result set.
    --
    -- Usage:
    --   SELECT * FROM flashback_query(
    --       'public.orders',
    --       '2025-01-01 12:00:00'::timestamptz,
    --       'SELECT * FROM $FB_TABLE WHERE total > 100'
    --   ) AS t(id int, total numeric, status text);
    -- ----------------------------------------------------------------
    CREATE OR REPLACE FUNCTION flashback_query(
        target_table text,
        target_time  timestamptz,
        query        text DEFAULT NULL
    )
    RETURNS SETOF record
    LANGUAGE plpgsql
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

        -- Find nearest snapshot <= target_time
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

        -- Create temp table with same structure and load snapshot
        v_tmp := '_fb_query_' || pg_backend_pid()::text || '_' || extract(epoch FROM clock_timestamp())::bigint::text || '_' || (random() * 100000)::integer::text;
        EXECUTE format('CREATE TEMP TABLE %I (LIKE %I.%I INCLUDING ALL) ON COMMIT DROP', v_tmp, v_schema_name, v_table_name);
        EXECUTE format('INSERT INTO %I SELECT * FROM %s', v_tmp, v_start_table);

        -- Replay delta events
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
        END LOOP;

        -- Execute user query or default SELECT *
        IF query IS NOT NULL THEN
            v_query := replace(query, '$FB_TABLE', quote_ident(v_tmp));
        ELSE
            v_query := format('SELECT * FROM %I', v_tmp);
        END IF;

        RETURN QUERY EXECUTE v_query;
    END;
    $$;
    "#,
    name = "flashback_restore_planner_api",
    requires = [
        "flashback_storage_schema_bootstrap",
        "flashback_restore_replay_helpers"
    ],
);
