-- =================================================================
-- Server-side recovery plan (authority) + plan-token execute.
-- plan_token is canonical identity / stale-selection guard, not a capability.
-- Authorization is RBAC. Execute fully recomputes and compares under lock.
-- =================================================================

CREATE OR REPLACE FUNCTION flashback_recover_plan(
    p_table text,
    p_lookback interval DEFAULT interval '24 hours',
    p_disaster_event_id bigint DEFAULT NULL,
    p_at timestamptz DEFAULT NULL,
    p_lsn pg_lsn DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_plan_version integer := 1;
    v_row record;
    v_tracking_id bigint;
    v_live_oid oid;
    v_tracked_oid oid;
    v_identity_conflict boolean := false;
    v_manifest jsonb;
    v_manifest_row record;
    v_status text;
    v_code text;
    v_token text;
    v_canonical text;
    v_selection text;
    v_resolved pg_lsn;
    v_frontier pg_lsn;
    v_stream_restart pg_lsn;
    v_stream_state text;
    v_pending_wal boolean := false;
    v_epoch_schema_def jsonb;
    v_epoch_compat jsonb;
BEGIN
    -- Resolve tracking identity through the single canonical, ambiguity-safe
    -- resolver (flashback_internal_resolve_tracked_table). No other query in
    -- this function, or in flashback_recover_begin/flashback_recover_execute
    -- below, re-implements the name-matching algorithm. flashback_recover_plan
    -- keeps its "always returns JSON, never raises for a bad table argument"
    -- contract, so an ambiguous unqualified name is translated into a
    -- structured error response here instead of propagating the exception.
    BEGIN
        SELECT r.tracking_id, r.rel_oid, format('%I.%I', r.schema_name, r.table_name)
          INTO v_tracking_id, v_tracked_oid, v_canonical
        FROM public.flashback_internal_resolve_tracked_table(p_table) r;
    EXCEPTION WHEN too_many_rows THEN
        RETURN jsonb_build_object(
            'schema_version', v_plan_version,
            'status', 'error',
            'code', 'ambiguous_table',
            'table', p_table,
            'blockers', jsonb_build_array(SQLERRM),
            'action', 'use a schema-qualified name (schema.table)'
        );
    END;

    IF v_tracking_id IS NULL THEN
        RETURN jsonb_build_object(
            'schema_version', v_plan_version,
            'status', 'error',
            'code', 'no_lifecycle',
            'table', p_table,
            'blockers', jsonb_build_array('no local_delta tracking lifecycle')
        );
    END IF;

    v_live_oid := to_regclass(v_canonical);
    IF v_live_oid IS NOT NULL AND v_live_oid IS DISTINCT FROM v_tracked_oid THEN
        v_identity_conflict := true;
    END IF;

    IF p_lsn IS NOT NULL THEN
        v_selection := 'advanced_lsn';
        v_row := NULL;
        -- Advanced LSN: synthesize a plan row from admission.
        BEGIN
            PERFORM 1 FROM flashback_admit_lsn_target(v_canonical, p_lsn);
            v_status := 'restorable';
            v_code := 'ok';
        EXCEPTION WHEN OTHERS THEN
            v_status := 'non_restorable';
            v_code := 'lsn_not_admitted';
        END;
        v_token := flashback_sha256(format('%s|%s|%s|%s|%s',
            v_plan_version, v_tracking_id, 'lsn', p_lsn::text, v_canonical));
        RETURN jsonb_build_object(
            'schema_version', v_plan_version,
            'plan_version', v_plan_version,
            'plan_token', v_token,
            'selection', v_selection,
            'table_name', v_canonical,
            'tracking_id', v_tracking_id,
            'disaster_event_id', NULL,
            'generation_id', NULL,
            'target_lsn', p_lsn,
            'status', v_status,
            'code', v_code,
            'identity_conflict', v_identity_conflict,
            'duration_estimate', 'unknown',
            'note', 'plan_token is identity/stale guard only; RBAC authorizes execute'
        );
    END IF;

    IF p_disaster_event_id IS NOT NULL THEN
        v_selection := 'explicit_event';
        SELECT
            d.table_name, d.event_type, d.disaster_commit_lsn, d.disaster_time,
            d.safe_target_lsn, d.safe_target_time, d.generation_id, d.status, d.reason,
            -- expose delta event_id via join
            (SELECT dl.event_id FROM flashback.delta_log dl
              WHERE dl.tracking_id = v_tracking_id
                AND dl.event_type = 'DROP'
                AND dl.commit_lsn IS NOT DISTINCT FROM d.disaster_commit_lsn
              ORDER BY dl.event_id DESC LIMIT 1) AS disaster_event_id
          INTO v_row
        FROM flashback_disaster_points(v_canonical, p_lookback) d
        WHERE d.event_type = 'DROP'
          AND EXISTS (
              SELECT 1 FROM flashback.delta_log dl
              WHERE dl.event_id = p_disaster_event_id
                AND dl.tracking_id = v_tracking_id
                AND dl.commit_lsn IS NOT DISTINCT FROM d.disaster_commit_lsn
          )
        LIMIT 1;
    ELSIF p_at IS NOT NULL THEN
        v_selection := 'timestamp';
        -- Timestamp UX: resolve to proven LSN prefix via resolve_target, then
        -- bind to the DROP whose commit_lsn is the latest <= that coordinate
        -- evaluating that DROP alone (no silent older fallback).
        BEGIN
            SELECT target_lsn INTO v_resolved
            FROM flashback_resolve_target(v_canonical, p_at)
            LIMIT 1;
            SELECT
                d.table_name, d.event_type, d.disaster_commit_lsn, d.disaster_time,
                d.safe_target_lsn, d.safe_target_time, d.generation_id, d.status, d.reason,
                (SELECT dl.event_id FROM flashback.delta_log dl
                  WHERE dl.tracking_id = v_tracking_id
                    AND dl.event_type = 'DROP'
                    AND dl.commit_lsn IS NOT DISTINCT FROM d.disaster_commit_lsn
                  ORDER BY dl.event_id DESC LIMIT 1) AS disaster_event_id
              INTO v_row
            FROM flashback_disaster_points(v_canonical, p_lookback) d
            WHERE d.event_type = 'DROP'
              AND d.disaster_commit_lsn IS NOT NULL
              AND d.disaster_commit_lsn <= COALESCE(v_resolved, '0/0'::pg_lsn)
            ORDER BY d.disaster_commit_lsn DESC
            LIMIT 1;
        EXCEPTION WHEN OTHERS THEN
            v_row := NULL;
        END;
    ELSE
        v_selection := 'latest_drop';
        SELECT
            d.table_name, d.event_type, d.disaster_commit_lsn, d.disaster_time,
            d.safe_target_lsn, d.safe_target_time, d.generation_id, d.status, d.reason,
            (SELECT dl.event_id FROM flashback.delta_log dl
              WHERE dl.tracking_id = v_tracking_id
                AND dl.event_type = 'DROP'
                AND dl.commit_lsn IS NOT DISTINCT FROM d.disaster_commit_lsn
              ORDER BY dl.event_id DESC LIMIT 1) AS disaster_event_id
          INTO v_row
        FROM flashback_disaster_points(v_canonical, p_lookback) d
        WHERE d.event_type = 'DROP'
        ORDER BY d.disaster_commit_lsn DESC NULLS LAST, d.disaster_time DESC NULLS LAST
        LIMIT 1;
    END IF;

    IF v_row IS NULL OR v_row.table_name IS NULL THEN
        -- Distinguish true absence of DROP from capture frontier lag using the
        -- durable, transactionally-consistent flashback.capture_streams
        -- watermark -- never the raw pg_replication_slots row. PostgreSQL
        -- advances a slot's confirmed_flush_lsn as a non-transactional side
        -- effect of pg_logical_slot_get_changes itself (see the comment in
        -- flashback_consume_wal and flashback_apply_decoded_wal_batch), so
        -- the raw slot value can race ahead of flashback.delta_log's own
        -- commit of the same decoded batch: a concurrent reader could see the
        -- slot already "caught up" to the DROP's LSN while the DROP row is
        -- still invisible (uncommitted) in delta_log, and wrongly conclude
        -- no_drop_event. flashback.capture_streams.confirmed_flush_lsn is
        -- only advanced inside the same committed transaction as the
        -- delta_log insert it describes (flashback_apply_decoded_wal_batch),
        -- so comparing against it cannot report "no DROP" while a DROP is
        -- still mid-decode.
        SELECT cs.confirmed_flush_lsn, cs.restart_lsn, cs.state
          INTO v_frontier, v_stream_restart, v_stream_state
        FROM flashback.capture_streams cs
        WHERE cs.database_oid = (SELECT oid FROM pg_database WHERE datname = current_database())
        ORDER BY cs.epoch_no DESC
        LIMIT 1;

        IF NOT FOUND OR v_stream_state IS DISTINCT FROM 'active' THEN
            -- No provably active capture stream for this database: never
            -- invent a DROP and never treat this as ordinary catch-up
            -- (which the CLI polls indefinitely up to its timeout). Slot
            -- loss, broken streams and missing streams are hard failures.
            RETURN jsonb_build_object(
                'schema_version', v_plan_version,
                'plan_version', v_plan_version,
                'selection', v_selection,
                'table_name', v_canonical,
                'tracking_id', v_tracking_id,
                'status', 'error',
                'code', 'capture_stream_unavailable',
                'identity_conflict', v_identity_conflict,
                'capture_frontier_lsn', v_frontier,
                'current_wal_lsn', pg_current_wal_lsn(),
                'duration_estimate', 'unknown',
                'blockers', jsonb_build_array(
                    format('capture stream is not active (state=%s); cannot prove capture has reached any DROP',
                           COALESCE(v_stream_state, 'missing'))
                ),
                'action', 'run pg_flashback doctor to diagnose the capture stream before retrying recovery'
            );
        END IF;

        v_frontier := COALESCE(v_frontier, v_stream_restart);
        v_pending_wal := COALESCE(
            pg_wal_lsn_diff(pg_current_wal_lsn(), COALESCE(v_frontier, '0/0'::pg_lsn)) > 0,
            true
        );
        IF v_pending_wal THEN
            RETURN jsonb_build_object(
                'schema_version', v_plan_version,
                'plan_version', v_plan_version,
                'selection', v_selection,
                'table_name', v_canonical,
                'tracking_id', v_tracking_id,
                'status', 'error',
                'code', 'capture_catchup_pending',
                'identity_conflict', v_identity_conflict,
                'capture_frontier_lsn', v_frontier,
                'current_wal_lsn', pg_current_wal_lsn(),
                'duration_estimate', 'unknown',
                'blockers', jsonb_build_array(
                    'no DROP event observed yet; capture worker has not reached the current WAL frontier'
                ),
                'action', 'wait for flashback capture/slot catch-up then replan; run flashback_doctor()/status'
            );
        END IF;
        RETURN jsonb_build_object(
            'schema_version', v_plan_version,
            'plan_version', v_plan_version,
            'selection', v_selection,
            'table_name', v_canonical,
            'tracking_id', v_tracking_id,
            'status', 'error',
            'code', 'no_drop_event',
            'identity_conflict', v_identity_conflict,
            'capture_frontier_lsn', v_frontier,
            'current_wal_lsn', pg_current_wal_lsn(),
            'duration_estimate', 'unknown',
            'blockers', jsonb_build_array('no DROP event for selection after capture frontier caught up')
        );
    END IF;

    -- Exact event-bound manifest only: disaster_event_id must equal the
    -- selected DROP. Never fall back to another generation's latest row.
    SELECT m.manifest, m.manifest_id, m.has_unsupported, m.cascade_requested,
           m.source_xid, flashback_sha256(m.manifest::text) AS manifest_hash
      INTO v_manifest_row
    FROM flashback.drop_dependency_manifests m
    WHERE m.tracking_id = v_tracking_id
      AND v_row.disaster_event_id IS NOT NULL
      AND m.disaster_event_id = v_row.disaster_event_id
    ORDER BY m.captured_at DESC, m.manifest_id DESC
    LIMIT 1;

    IF v_manifest_row.manifest_id IS NULL THEN
        -- Worker may still be binding disaster_event_id onto a freshly captured
        -- pre-DROP manifest; try bind once, then ask callers to poll.
        PERFORM flashback_bind_drop_dependency_manifests();
        SELECT m.manifest, m.manifest_id, m.has_unsupported, m.cascade_requested,
               m.source_xid, flashback_sha256(m.manifest::text) AS manifest_hash
          INTO v_manifest_row
        FROM flashback.drop_dependency_manifests m
        WHERE m.tracking_id = v_tracking_id
          AND v_row.disaster_event_id IS NOT NULL
          AND m.disaster_event_id = v_row.disaster_event_id
        ORDER BY m.captured_at DESC, m.manifest_id DESC
        LIMIT 1;
    END IF;

    IF v_manifest_row.manifest_id IS NULL THEN
        RETURN jsonb_build_object(
            'schema_version', v_plan_version,
            'plan_version', v_plan_version,
            'selection', v_selection,
            'table_name', v_canonical,
            'tracking_id', v_tracking_id,
            'disaster_event_id', v_row.disaster_event_id,
            'generation_id', v_row.generation_id,
            'target_lsn', v_row.safe_target_lsn,
            'disaster_commit_lsn', v_row.disaster_commit_lsn,
            'disaster_time', v_row.disaster_time,
            'status', 'error',
            'code', 'exact_manifest_pending',
            'reason', format(
                'exact pre-DROP dependency manifest for disaster_event_id=%s is not bound yet',
                v_row.disaster_event_id
            ),
            'identity_conflict', v_identity_conflict,
            'dependency_manifest', NULL,
            'dependency_manifest_id', NULL,
            'blockers', jsonb_build_array(
                'exact event-bound dependency manifest missing; waiting for ProcessUtility capture + worker bind'
            ),
            'action', 'wait briefly for worker bind then replan; recovery refuses without the exact manifest',
            'duration_estimate', 'unknown'
        );
    END IF;

    v_manifest := v_manifest_row.manifest;

    -- Target schema epoch compatibility: never promise a recovery that the
    -- restore engine cannot reconstruct/verify. Prefer the exact
    -- schema_versions row at/under the DROP's safe target LSN; fall back to
    -- the generation's boundary snapshot schema_def.
    IF v_row.generation_id IS NOT NULL THEN
        SELECT COALESCE(
                   sv.schema_def,
                   jsonb_build_object(
                       'schema', split_part(v_canonical, '.', 1),
                       'table', split_part(v_canonical, '.', 2),
                       'columns', COALESCE(sv.columns, '[]'::jsonb),
                       'primary_key', COALESCE(sv.primary_key, '[]'::jsonb),
                       'constraints', COALESCE(sv.constraints -> 'check_unique_fk', '[]'::jsonb),
                       'indexes', COALESCE(sv.constraints -> 'indexes', '[]'::jsonb),
                       'partition_by', sv.constraints -> 'partition_by',
                       'partitions', sv.constraints -> 'partitions',
                       'triggers', COALESCE(sv.constraints -> 'triggers', '[]'::jsonb),
                       'rls_policies', COALESCE(sv.constraints -> 'rls_policies', '[]'::jsonb),
                       'rls_enabled', COALESCE((sv.constraints -> 'rls_enabled')::boolean, false),
                       'force_rls', COALESCE((sv.constraints -> 'force_rls')::boolean, false)
                   )
               )
          INTO v_epoch_schema_def
        FROM flashback.schema_versions sv
        WHERE sv.tracking_id = v_tracking_id
          AND sv.generation_id = v_row.generation_id
          AND sv.commit_lsn IS NOT NULL
          AND sv.commit_lsn <= COALESCE(v_row.safe_target_lsn, v_row.disaster_commit_lsn)
        ORDER BY sv.commit_lsn DESC, sv.schema_version DESC
        LIMIT 1;

        IF v_epoch_schema_def IS NULL THEN
            SELECT snap.schema_def INTO v_epoch_schema_def
            FROM flashback.coverage_generations cg
            JOIN flashback.snapshots snap
              ON snap.snapshot_id = cg.boundary_snapshot_id
             AND snap.tracking_id = cg.tracking_id
            WHERE cg.generation_id = v_row.generation_id
              AND cg.tracking_id = v_tracking_id;
        END IF;

        IF v_epoch_schema_def IS NOT NULL THEN
            v_epoch_compat := flashback_local_compatibility_schema_def(v_epoch_schema_def);
        END IF;
    END IF;

    v_status := v_row.status;
    v_code := CASE
        WHEN v_identity_conflict THEN 'identity_conflict'
        WHEN COALESCE(v_manifest_row.has_unsupported, false)
             OR COALESCE((v_manifest->>'has_unsupported')::boolean, false)
             THEN 'unsupported_dependencies'
        WHEN v_epoch_compat IS NOT NULL
             AND NOT COALESCE((v_epoch_compat->>'supported')::boolean, true)
             THEN 'unsupported_schema_epoch'
        WHEN v_status = 'restorable' THEN 'ok'
        WHEN v_row.reason ILIKE '%ambiguous%' THEN 'ambiguous_coverage_generation'
        WHEN v_row.reason ILIKE '%gap%' THEN 'coverage_gap'
        ELSE 'non_restorable'
    END;
    -- Prefer an operator-visible identity conflict reason over the DROP's
    -- coverage reason when the live OID no longer matches the tracked lifecycle.
    IF v_identity_conflict THEN
        v_status := 'non_restorable';
        v_row.reason := format(
            'identity conflict: live relation OID differs from tracked lifecycle OID for %s',
            v_canonical
        );
    ELSIF v_code = 'unsupported_dependencies' THEN
        v_status := 'non_restorable';
        v_row.reason := 'pre-DROP dependency manifest reports unsupported or unqualified dependencies';
    ELSIF v_code = 'unsupported_schema_epoch' THEN
        v_status := 'non_restorable';
        v_row.reason := format(
            'target schema epoch is not supported by the local DROP recovery product: %s',
            COALESCE(v_epoch_compat->>'reason', 'unknown')
        );
    END IF;

    v_token := flashback_sha256(format('%s|%s|%s|%s|%s|%s|%s|%s',
        v_plan_version,
        v_tracking_id,
        COALESCE(v_row.disaster_event_id::text, ''),
        COALESCE(v_row.generation_id::text, ''),
        COALESCE(v_row.safe_target_lsn::text, ''),
        v_canonical,
        COALESCE(v_manifest_row.manifest_id::text, ''),
        COALESCE(v_manifest_row.manifest_hash, '')));

    RETURN jsonb_build_object(
        'schema_version', v_plan_version,
        'plan_version', v_plan_version,
        'plan_token', v_token,
        'selection', v_selection,
        'table_name', v_canonical,
        'tracking_id', v_tracking_id,
        'disaster_event_id', v_row.disaster_event_id,
        'generation_id', v_row.generation_id,
        'target_lsn', v_row.safe_target_lsn,
        'disaster_commit_lsn', v_row.disaster_commit_lsn,
        'disaster_time', v_row.disaster_time,
        'status', v_status,
        'code', v_code,
        'reason', v_row.reason,
        'identity_conflict', v_identity_conflict,
        'dependency_manifest', v_manifest,
        'dependency_manifest_id', v_manifest_row.manifest_id,
        'dependency_manifest_hash', v_manifest_row.manifest_hash,
        'schema_epoch_compatibility', v_epoch_compat,
        'blockers', CASE
            WHEN v_code = 'unsupported_dependencies' THEN
                COALESCE(v_manifest->'views', '[]'::jsonb)
                || COALESCE(v_manifest->'matviews', '[]'::jsonb)
                || COALESCE(v_manifest->'incoming_fk', '[]'::jsonb)
                || COALESCE(v_manifest->'inheritance_children', '[]'::jsonb)
                || COALESCE(v_manifest->'inheritance_parents', '[]'::jsonb)
            WHEN v_code = 'unsupported_schema_epoch' THEN
                COALESCE(v_epoch_compat->'rejected_features', '[]'::jsonb)
            ELSE '[]'::jsonb
        END,
        'duration_estimate', 'unknown',
        'note', 'plan_token is identity/stale guard only; RBAC authorizes execute; dry-run does not authorize'
    );
END;
$$;

-- TX A: durable recover attempt header. Commit before destructive restore.
CREATE OR REPLACE FUNCTION flashback_recover_begin(
    p_table text,
    p_plan_token text,
    p_lookback interval DEFAULT interval '24 hours',
    p_disaster_event_id bigint DEFAULT NULL,
    p_at timestamptz DEFAULT NULL,
    p_lsn pg_lsn DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_plan jsonb;
    v_op bigint;
    v_token text;
    v_tracking_id bigint;
    v_table text;
BEGIN
    PERFORM flashback_require_primary('flashback_recover_begin');

    -- Resolve once via the canonical resolver (no name-parsing algorithm of
    -- our own), then lock the row by tracking_id -- never by re-searching on
    -- the caller's name string a second time.
    SELECT r.tracking_id INTO v_tracking_id
    FROM public.flashback_internal_resolve_tracked_table(p_table) r;

    IF v_tracking_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: no local_delta lifecycle for %', p_table
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    PERFORM 1 FROM flashback.tracked_tables tt
    WHERE tt.tracking_id = v_tracking_id
    FOR UPDATE OF tt;

    v_plan := flashback_recover_plan(p_table, p_lookback, p_disaster_event_id, p_at, p_lsn);
    v_token := v_plan->>'plan_token';
    IF v_token IS NULL OR v_token IS DISTINCT FROM p_plan_token THEN
        RAISE EXCEPTION 'pg_flashback: recover plan_token mismatch (stale selection); replan required'
            USING ERRCODE = 'serialization_failure',
                  HINT = 'Call flashback_recover_plan again and retry begin with the new plan_token.';
    END IF;
    IF COALESCE(v_plan->>'status', '') <> 'restorable' THEN
        RAISE EXCEPTION 'pg_flashback: recover plan is not restorable (%)',
            COALESCE(v_plan->>'code', 'unknown')
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    -- The lock above pins a specific tracking_id; if the freshly recomputed
    -- plan (under lock) resolved a different lifecycle, the picture changed
    -- between the two resolves (e.g. a concurrent unprotect+reprotect
    -- committed in between) -- fail closed rather than begin against a
    -- lifecycle that was never actually locked.
    IF (v_plan->>'tracking_id')::bigint IS DISTINCT FROM v_tracking_id THEN
        RAISE EXCEPTION 'pg_flashback: concurrent lifecycle change detected for %; replan required', p_table
            USING ERRCODE = 'serialization_failure',
                  HINT = 'Call flashback_recover_plan again and retry begin with the new plan_token.';
    END IF;

    v_tracking_id := (v_plan->>'tracking_id')::bigint;
    v_table := v_plan->>'table_name';

    v_op := flashback_operation_begin(
        'recover', v_table, v_tracking_id,
        (v_plan->>'plan_version')::integer, v_token,
        (v_plan->>'disaster_event_id')::bigint,
        (v_plan->>'generation_id')::bigint,
        (v_plan->>'target_lsn')::pg_lsn,
        jsonb_build_object(
            'plan', v_plan,
            'protocol', 'begin_then_execute',
            'note', 'Commit this transaction before flashback_recover_execute'
        )
    );

    RETURN jsonb_build_object(
        'schema_version', 1,
        'status', 'started',
        'code', 'ok',
        'operation_id', v_op,
        'table_name', v_table,
        'plan_token', v_token,
        'tracking_id', v_tracking_id,
        'target_lsn', v_plan->>'target_lsn',
        'note', 'Commit before calling flashback_recover_execute(operation_id, plan_token, ...). On execute failure call flashback_recover_mark_failed in a new transaction. verified requires restore_verification.status=passed on the operation header.'
    );
END;
$$;

-- TX B: destructive restore against a committed operation_id from begin.
-- Does NOT create the operation header. On failure, RAISE without appending
-- failed (that append must happen in TX C via flashback_recover_mark_failed).
CREATE OR REPLACE FUNCTION flashback_recover_execute(
    p_table text,
    p_plan_token text,
    p_lookback interval DEFAULT interval '24 hours',
    p_disaster_event_id bigint DEFAULT NULL,
    p_at timestamptz DEFAULT NULL,
    p_lsn pg_lsn DEFAULT NULL,
    p_operation_id bigint DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_plan jsonb;
    v_op bigint := p_operation_id;
    v_rows bigint;
    v_token text;
    v_lsn pg_lsn;
    v_tracking_id bigint;
    v_table text;
    v_header_command text;
    v_header_token text;
    v_header_tracking bigint;
    v_header_state text;
    v_binding jsonb;
    v_proof_payload jsonb;
BEGIN
    PERFORM flashback_require_primary('flashback_recover_execute');

    IF v_op IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: flashback_recover_execute requires operation_id from flashback_recover_begin'
            USING ERRCODE = 'invalid_parameter_value',
                  HINT = 'Call flashback_recover_begin, COMMIT, then flashback_recover_execute(..., p_operation_id => ...).';
    END IF;

    SELECT o.command, o.plan_token, o.tracking_id, s.state
      INTO v_header_command, v_header_token, v_header_tracking, v_header_state
    FROM flashback.operations o
    JOIN flashback.operation_current_state s ON s.operation_id = o.operation_id
    WHERE o.operation_id = v_op
    FOR UPDATE OF o;

    IF v_header_command IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: unknown recover operation_id %', v_op
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF v_header_command IS DISTINCT FROM 'recover' THEN
        RAISE EXCEPTION 'pg_flashback: operation % is not a recover attempt', v_op
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF v_header_state IS DISTINCT FROM 'started' THEN
        RAISE EXCEPTION 'pg_flashback: operation % is not in started state (got %)',
            v_op, v_header_state
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF v_header_token IS DISTINCT FROM p_plan_token THEN
        RAISE EXCEPTION 'pg_flashback: operation plan_token mismatch'
            USING ERRCODE = 'serialization_failure';
    END IF;

    -- Resolve once via the canonical resolver, then lock by tracking_id --
    -- never by re-searching on the caller's name string a second time.
    SELECT r.tracking_id INTO v_tracking_id
    FROM public.flashback_internal_resolve_tracked_table(p_table) r;

    IF v_tracking_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: no local_delta lifecycle for %', p_table
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    PERFORM 1 FROM flashback.tracked_tables tt
    WHERE tt.tracking_id = v_tracking_id
    FOR UPDATE OF tt;

    IF v_tracking_id IS DISTINCT FROM v_header_tracking THEN
        RAISE EXCEPTION 'pg_flashback: operation tracking_id drift versus locked lifecycle for %', p_table
            USING ERRCODE = 'serialization_failure';
    END IF;

    v_plan := flashback_recover_plan(p_table, p_lookback, p_disaster_event_id, p_at, p_lsn);
    v_token := v_plan->>'plan_token';
    IF v_token IS NULL OR v_token IS DISTINCT FROM p_plan_token THEN
        RAISE EXCEPTION 'pg_flashback: recover plan_token mismatch (stale selection); replan required'
            USING ERRCODE = 'serialization_failure',
                  HINT = 'Call flashback_recover_plan again, begin a new operation, and retry.';
    END IF;
    IF COALESCE(v_plan->>'status', '') <> 'restorable' THEN
        RAISE EXCEPTION 'pg_flashback: recover plan is not restorable (%)',
            COALESCE(v_plan->>'code', 'unknown')
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF (v_plan->>'tracking_id')::bigint IS DISTINCT FROM v_header_tracking THEN
        RAISE EXCEPTION 'pg_flashback: operation tracking_id drift versus recomputed plan'
            USING ERRCODE = 'serialization_failure';
    END IF;

    v_lsn := (v_plan->>'target_lsn')::pg_lsn;
    v_tracking_id := (v_plan->>'tracking_id')::bigint;
    v_table := v_plan->>'table_name';

    -- Backend-local Rust execution context, not a user-settable GUC: see
    -- flashback_internal_set_audited_recover_context. Cleared on both the
    -- success and exception paths here; a permanent transaction-end callback
    -- also force-clears it on commit/abort as a backstop against a bug in
    -- this exception handling ever leaking the context into a later,
    -- unrelated restore in the same backend/connection.
    PERFORM flashback_internal_set_audited_recover_context(v_op);
    BEGIN
        v_rows := flashback_restore_lsn(v_table, v_lsn);
    EXCEPTION WHEN OTHERS THEN
        PERFORM flashback_internal_clear_audited_recover_context();
        RAISE;
    END;
    PERFORM flashback_internal_clear_audited_recover_context();

    SELECT COALESCE(e.payload->'successor', '{}'::jsonb), e.payload
      INTO v_binding, v_proof_payload
    FROM flashback.operation_events e
    WHERE e.operation_id = v_op
      AND e.event_type = 'applied_coverage_pending'
    ORDER BY e.event_id DESC
    LIMIT 1;

    RETURN jsonb_build_object(
        'schema_version', 1,
        'status', 'applied_coverage_pending',
        'code', 'ok',
        'operation_id', v_op,
        'table_name', v_table,
        'rows_affected', v_rows,
        'plan_token', v_token,
        'successor', v_binding,
        'note', 'verified is written by worker/finalizer after restore_verification passes and the exact successor boundary is healthy'
    );
END;
$$;
