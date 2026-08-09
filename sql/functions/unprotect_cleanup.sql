-- =================================================================
-- Two-phase unprotect + durable cleanup by tracking_id.
-- Phase A (unprotect): emit stopping marker; decoder filtering stays on
-- until that transaction's COMMIT LSN is consumed; then seal inactive.
-- Phase B (cleanup): retire payload for a sealed unprotected lifecycle.
-- flashback_untrack remains for SQL compatibility but is deprecated.
-- =================================================================

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flashback' AND table_name = 'tracked_tables'
          AND column_name = 'protection_state'
    ) THEN
        ALTER TABLE flashback.tracked_tables
            ADD COLUMN protection_state TEXT NOT NULL DEFAULT 'active';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flashback' AND table_name = 'tracked_tables'
          AND column_name = 'stop_marker_xid'
    ) THEN
        ALTER TABLE flashback.tracked_tables
            ADD COLUMN stop_marker_xid BIGINT;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flashback' AND table_name = 'tracked_tables'
          AND column_name = 'stop_marker_commit_lsn'
    ) THEN
        ALTER TABLE flashback.tracked_tables
            ADD COLUMN stop_marker_commit_lsn PG_LSN;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flashback' AND table_name = 'tracked_tables'
          AND column_name = 'unprotected_at'
    ) THEN
        ALTER TABLE flashback.tracked_tables
            ADD COLUMN unprotected_at TIMESTAMPTZ;
    END IF;

    -- Allow re-protect (= new lifecycle) while retaining inactive rows for cleanup.
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'flashback.tracked_tables'::regclass
          AND conname = 'tracked_tables_schema_name_table_name_key'
    ) THEN
        ALTER TABLE flashback.tracked_tables
            DROP CONSTRAINT tracked_tables_schema_name_table_name_key;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'flashback'
          AND indexname = 'tracked_tables_active_name_key'
    ) THEN
        EXECUTE $idx$
            CREATE UNIQUE INDEX tracked_tables_active_name_key
                ON flashback.tracked_tables (schema_name, table_name)
                WHERE is_active
        $idx$;
    END IF;

    -- Replace global rel_oid primary key with tracking_id PK + active-only
    -- uniqueness on rel_oid so inactive historical rows can coexist.
    IF EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)
        WHERE c.conrelid = 'flashback.tracked_tables'::regclass
          AND c.contype = 'p'
          AND a.attname = 'rel_oid'
    ) THEN
        ALTER TABLE flashback.tracked_tables DROP CONSTRAINT tracked_tables_pkey;
        IF EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid = 'flashback.tracked_tables'::regclass
              AND conname = 'tracked_tables_tracking_id_key'
        ) THEN
            ALTER TABLE flashback.tracked_tables
                DROP CONSTRAINT tracked_tables_tracking_id_key;
        END IF;
        ALTER TABLE flashback.tracked_tables
            ADD CONSTRAINT tracked_tables_pkey PRIMARY KEY (tracking_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'flashback'
          AND indexname = 'tracked_tables_active_rel_oid_key'
    ) THEN
        EXECUTE $idx$
            CREATE UNIQUE INDEX tracked_tables_active_rel_oid_key
                ON flashback.tracked_tables (rel_oid)
                WHERE is_active
        $idx$;
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION flashback_unprotect(target_table text)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_tracking_id bigint;
    v_schema text;
    v_table text;
    v_rel_oid oid;
    v_state text;
    v_op bigint;
    v_xid bigint;
BEGIN
    PERFORM flashback_require_primary('flashback_unprotect');

    -- Resolve once via the canonical resolver, then lock by tracking_id and
    -- re-read the row under lock -- never re-search by name a second time.
    SELECT r.tracking_id INTO v_tracking_id
    FROM public.flashback_internal_resolve_tracked_table(target_table) r;

    IF v_tracking_id IS NULL THEN
        RAISE EXCEPTION 'flashback_unprotect: no active local_delta lifecycle for %', target_table
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT tt.schema_name, tt.table_name, tt.rel_oid,
           COALESCE(tt.protection_state, 'active')
      INTO v_schema, v_table, v_rel_oid, v_state
    FROM flashback.tracked_tables tt
    WHERE tt.tracking_id = v_tracking_id
    FOR UPDATE OF tt;

    IF v_state = 'stopping' THEN
        RETURN jsonb_build_object(
            'schema_version', 1,
            'status', 'stopping',
            'code', 'already_stopping',
            'tracking_id', v_tracking_id,
            'table_name', format('%I.%I', v_schema, v_table),
            'note', 'waiting for stop marker COMMIT LSN consumption'
        );
    END IF;

    IF v_state = 'unprotected' THEN
        RETURN jsonb_build_object(
            'schema_version', 1,
            'status', 'unprotected',
            'code', 'already_unprotected',
            'tracking_id', v_tracking_id,
            'table_name', format('%I.%I', v_schema, v_table)
        );
    END IF;

    IF EXISTS (
        SELECT 1 FROM flashback.coverage_generations cg
        WHERE cg.tracking_id = v_tracking_id AND cg.state IN ('building', 'capturing')
    ) THEN
        RAISE EXCEPTION 'flashback_unprotect: lifecycle % has a pending generation', v_tracking_id
            USING HINT = 'Wait for boundary COMMIT LSN resolution before unprotect.';
    END IF;

    v_xid := pg_current_xact_id()::text::bigint;

    UPDATE flashback.tracked_tables
       SET protection_state = 'stopping',
           stop_marker_xid = v_xid,
           stop_marker_commit_lsn = NULL,
           unprotected_at = NULL
     WHERE tracking_id = v_tracking_id;

    -- Marker makes this transaction's COMMIT visible to the decoder; filtering
    -- remains on (is_active) until finalize consumes that COMMIT LSN.
    PERFORM pg_logical_emit_message(
        true,
        'pg_flashback',
        jsonb_build_object(
            'op', 'STOPPING',
            'kind', 'unprotect',
            'tracking_id', v_tracking_id
        )::text
    );

    v_op := flashback_operation_begin(
        'unprotect', format('%I.%I', v_schema, v_table), v_tracking_id,
        NULL, NULL, NULL, NULL, NULL,
        jsonb_build_object('stop_marker_xid', v_xid)
    );
    PERFORM flashback_operation_append_event(
        v_op, 'stopping', NULL, NULL,
        'stop marker emitted; decoder filtering remains until COMMIT consumed',
        jsonb_build_object('stop_marker_xid', v_xid)
    );

    RETURN jsonb_build_object(
        'schema_version', 1,
        'status', 'stopping',
        'code', 'ok',
        'tracking_id', v_tracking_id,
        'table_name', format('%I.%I', v_schema, v_table),
        'operation_id', v_op,
        'stop_marker_xid', v_xid,
        'note', 'sealed unprotected state is written by worker/finalizer after marker COMMIT'
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_finalize_unprotect_operations()
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    r record;
    v_commit pg_lsn;
    v_flush pg_lsn;
    v_slot text;
    v_n integer := 0;
    v_op bigint;
    v_original_ri "char";
    v_original_ri_idx text;
    v_ri_clause text;
    gen_rec record;
BEGIN
    v_slot := flashback_effective_slot_name();
    SELECT confirmed_flush_lsn INTO v_flush
    FROM pg_replication_slots
    WHERE slot_name = v_slot AND database = current_database();

    FOR r IN
        SELECT tt.*
        FROM flashback.tracked_tables tt
        WHERE tt.is_active
          AND COALESCE(tt.protection_state, 'active') = 'stopping'
          AND tt.stop_marker_xid IS NOT NULL
        FOR UPDATE OF tt
    LOOP
        SELECT c.commit_lsn INTO v_commit
        FROM flashback.capture_commits c
        WHERE c.source_xid = r.stop_marker_xid
        ORDER BY c.commit_lsn
        LIMIT 1;

        IF v_commit IS NULL THEN
            CONTINUE;
        END IF;
        IF v_flush IS NULL OR v_flush < v_commit THEN
            CONTINUE;
        END IF;

        -- Backlog proven: no pending WAL events for this relation.
        IF EXISTS (
            SELECT 1 FROM flashback.pending_wal_events p
            WHERE p.rel_oid = r.rel_oid
        ) THEN
            CONTINUE;
        END IF;

        -- Restore replica identity while relation still exists; keep payload.
        IF to_regclass(format('%I.%I', r.schema_name, r.table_name)) IS NOT NULL
           AND r.recovery_profile = 'local_delta'
        THEN
            SELECT tt.replica_identity_was, tt.replica_identity_index
              INTO v_original_ri, v_original_ri_idx
            FROM flashback.tracked_tables tt
            WHERE tt.tracking_id = r.tracking_id;

            v_ri_clause := CASE COALESCE(v_original_ri, 'd')
                WHEN 'f' THEN 'FULL'
                WHEN 'n' THEN 'NOTHING'
                WHEN 'i' THEN
                    CASE WHEN v_original_ri_idx IS NOT NULL
                         THEN 'USING INDEX ' || quote_ident(v_original_ri_idx)
                         ELSE 'DEFAULT'
                    END
                ELSE 'DEFAULT'
            END;
            -- Runs against a still-actively-tracked row (this loop iteration
            -- is what seals it inactive), so A2's corrected nested-DDL
            -- capture would otherwise treat this internal maintenance ALTER
            -- as user DDL. Bypass via the explicit backend-local flag.
            PERFORM public.flashback_set_restore_in_progress(true);
            BEGIN
                EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY %s',
                    r.schema_name, r.table_name, v_ri_clause);
            EXCEPTION WHEN OTHERS THEN
                NULL; -- seal still proceeds; RI restore is best-effort
            END;
            PERFORM public.flashback_set_restore_in_progress(false);
        END IF;

        FOR gen_rec IN
            SELECT generation_id, tracking_id, valid_through_lsn
            FROM flashback.coverage_generations
            WHERE tracking_id = r.tracking_id
              AND state = 'active'
            ORDER BY generation_id
        LOOP
            PERFORM flashback_internal_transition_coverage_generation(
                gen_rec.generation_id,
                gen_rec.tracking_id,
                'active',
                'sealed',
                'unprotected',
                NULL,
                NULL,
                NULL,
                NULL,
                COALESCE(gen_rec.valid_through_lsn, v_commit) + 1,
                clock_timestamp(),
                '{}'::jsonb
            );
        END LOOP;

        UPDATE flashback.tracked_tables
           SET is_active = false,
               protection_state = 'unprotected',
               stop_marker_commit_lsn = v_commit,
               unprotected_at = clock_timestamp()
         WHERE tracking_id = r.tracking_id;

        SELECT s.operation_id INTO v_op
        FROM flashback.operation_current_state s
        WHERE s.command = 'unprotect'
          AND s.tracking_id = r.tracking_id
          AND s.state = 'stopping'
        ORDER BY s.operation_id DESC
        LIMIT 1;

        IF v_op IS NOT NULL THEN
            PERFORM flashback_operation_append_event(
                v_op, 'unprotected', NULL, NULL,
                'stop marker COMMIT consumed; lifecycle sealed inactive',
                jsonb_build_object(
                    'stop_marker_commit_lsn', v_commit,
                    'finalizer', 'flashback_finalize_unprotect_operations'
                )
            );
        END IF;

        v_n := v_n + 1;
    END LOOP;

    RETURN v_n;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_cleanup(
    p_tracking_id bigint,
    p_dry_run boolean DEFAULT false
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    r record;
    v_op bigint;
    v_snap_count bigint;
    v_delta_count bigint;
    v_manifest_count bigint;
    gen_rec record;
    snap_rec record;
    v_pending_external integer := 0;
BEGIN
    PERFORM flashback_require_primary('flashback_cleanup');

    SELECT * INTO r
    FROM flashback.tracked_tables tt
    WHERE tt.tracking_id = p_tracking_id
    FOR UPDATE OF tt;

    IF r.tracking_id IS NULL THEN
        RAISE EXCEPTION 'flashback_cleanup: unknown tracking_id %', p_tracking_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    IF r.is_active OR COALESCE(r.protection_state, 'active') NOT IN ('unprotected', 'cleaned') THEN
        RAISE EXCEPTION
            'flashback_cleanup: tracking_id % is not sealed unprotected (active=%s state=%s)',
            p_tracking_id, r.is_active, COALESCE(r.protection_state, 'active')
            USING HINT = 'Run flashback_unprotect and wait for finalizer before cleanup.';
    END IF;

    SELECT count(*) INTO v_snap_count FROM flashback.snapshots WHERE tracking_id = p_tracking_id;
    SELECT count(*) INTO v_delta_count FROM flashback.delta_log WHERE tracking_id = p_tracking_id;
    v_manifest_count := 0;
    IF to_regclass('flashback.drop_dependency_manifests') IS NOT NULL THEN
        SELECT count(*) INTO v_manifest_count
        FROM flashback.drop_dependency_manifests WHERE tracking_id = p_tracking_id;
    END IF;

    IF p_dry_run THEN
        RETURN jsonb_build_object(
            'schema_version', 1,
            'status', 'dry_run',
            'code', 'ok',
            'tracking_id', p_tracking_id,
            'table_name', format('%I.%I', r.schema_name, r.table_name),
            'would_retire_snapshots', v_snap_count,
            'would_delete_delta_rows', v_delta_count,
            'would_delete_manifests', v_manifest_count,
            'note', 'dry-run does not mutate; operation journal is retained'
        );
    END IF;

    -- heap_v1 retirement is transactional. external_zstd retirement is
    -- deliberately two-call: this transaction commits available->retiring;
    -- only a later cleanup invocation or maintenance cycle may unlink bytes.
    FOR snap_rec IN
        SELECT snapshot_id, payload_state, storage_backend
        FROM flashback.snapshots
        WHERE tracking_id = p_tracking_id
          AND payload_state IN ('available', 'retiring')
        ORDER BY snapshot_id
    LOOP
        IF snap_rec.storage_backend = 'external_zstd'
           AND snap_rec.payload_state = 'available'
        THEN
            PERFORM flashback_internal_snapshot_retire_begin(
                snap_rec.snapshot_id, p_tracking_id
            );
            v_pending_external := v_pending_external + 1;
        ELSIF snap_rec.storage_backend = 'external_zstd'
              AND snap_rec.payload_state = 'retiring'
        THEN
            BEGIN
                PERFORM flashback_internal_snapshot_retire_purge(
                    snap_rec.snapshot_id, p_tracking_id
                );
                PERFORM flashback_internal_snapshot_retire_finish(
                    snap_rec.snapshot_id, p_tracking_id, 'retired'
                );
            EXCEPTION WHEN OTHERS THEN
                -- Most commonly a concurrent restore holds the shared file
                -- lock. Preserve retiring and let a later transaction retry.
                v_pending_external := v_pending_external + 1;
            END;
        ELSIF snap_rec.payload_state = 'available' THEN
            PERFORM flashback_internal_snapshot_retire(
                snap_rec.snapshot_id, p_tracking_id, 'retired'
            );
        END IF;
    END LOOP;

    IF v_pending_external > 0 THEN
        RETURN jsonb_build_object(
            'schema_version', 1,
            'status', 'cleanup_pending',
            'code', 'external_retirement_pending',
            'tracking_id', p_tracking_id,
            'table_name', format('%I.%I', r.schema_name, r.table_name),
            'pending_snapshots', v_pending_external,
            'action', 'retry flashback_cleanup in a new transaction'
        );
    END IF;

    v_op := flashback_operation_begin(
        'cleanup', format('%I.%I', r.schema_name, r.table_name), p_tracking_id,
        NULL, NULL, NULL, NULL, NULL,
        jsonb_build_object('dry_run', false)
    );

    DELETE FROM flashback.delta_log WHERE tracking_id = p_tracking_id;
    DELETE FROM flashback.schema_versions WHERE tracking_id = p_tracking_id;
    IF to_regclass('flashback.drop_dependency_manifests') IS NOT NULL THEN
        DELETE FROM flashback.drop_dependency_manifests WHERE tracking_id = p_tracking_id;
    END IF;

    -- Forbidden: active → retired. Seal any remaining active generations first,
    -- then retire sealed generations.
    FOR gen_rec IN
        SELECT generation_id, tracking_id, valid_through_lsn
        FROM flashback.coverage_generations
        WHERE tracking_id = p_tracking_id
          AND state = 'active'
        ORDER BY generation_id
    LOOP
        PERFORM flashback_internal_transition_coverage_generation(
            gen_rec.generation_id,
            gen_rec.tracking_id,
            'active',
            'sealed',
            'cleaned',
            NULL,
            NULL,
            NULL,
            NULL,
            CASE WHEN gen_rec.valid_through_lsn IS NOT NULL
                 THEN gen_rec.valid_through_lsn + 1
                 ELSE NULL END,
            clock_timestamp(),
            '{}'::jsonb
        );
    END LOOP;

    FOR gen_rec IN
        SELECT generation_id, tracking_id
        FROM flashback.coverage_generations
        WHERE tracking_id = p_tracking_id
          AND state = 'sealed'
        ORDER BY generation_id
    LOOP
        PERFORM flashback_internal_transition_coverage_generation(
            gen_rec.generation_id,
            gen_rec.tracking_id,
            'sealed',
            'retired',
            'cleaned',
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            '{}'::jsonb
        );
    END LOOP;

    UPDATE flashback.tracked_tables
       SET protection_state = 'cleaned',
           base_snapshot_table = NULL
     WHERE tracking_id = p_tracking_id;

    PERFORM flashback_operation_append_event(
        v_op, 'cleaned', NULL, NULL,
        'payload retired; operation journal retained',
        jsonb_build_object(
            'retired_snapshots', v_snap_count,
            'deleted_delta_rows', v_delta_count,
            'deleted_manifests', v_manifest_count
        )
    );

    RETURN jsonb_build_object(
        'schema_version', 1,
        'status', 'cleaned',
        'code', 'ok',
        'tracking_id', p_tracking_id,
        'table_name', format('%I.%I', r.schema_name, r.table_name),
        'operation_id', v_op,
        'retired_snapshots', v_snap_count,
        'deleted_delta_rows', v_delta_count
    );
END;
$$;

COMMENT ON FUNCTION flashback_unprotect(text) IS
    'Begin two-phase unprotect: emit stopping marker; retain decoder filtering until marker COMMIT is consumed; worker seals inactive. Does not delete recovery payload.';
COMMENT ON FUNCTION flashback_cleanup(bigint, boolean) IS
    'Retire recovery payload for a sealed unprotected tracking_id. dry_run=true reports only. Never deletes required recovery payload for active/stopping lifecycles. Operation journal is retained.';
COMMENT ON FUNCTION flashback_untrack(text) IS
    'DEPRECATED for operators: destructive single-shot untrack. Prefer flashback_unprotect then flashback_cleanup(tracking_id). Kept for SQL compatibility.';
