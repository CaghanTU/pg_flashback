-- =================================================================
-- Unified operation authority: immutable header + append-only events.
-- Current state and restore_log are projections/views only.
-- =================================================================

DO $$
BEGIN
    IF to_regclass('flashback.operations') IS NULL THEN
        EXECUTE $ddl$
            CREATE TABLE flashback.operations (
                operation_id      BIGSERIAL PRIMARY KEY,
                command           TEXT NOT NULL,
                database_name     TEXT NOT NULL DEFAULT current_database(),
                session_user_name TEXT NOT NULL DEFAULT session_user::text,
                tracking_id       BIGINT,
                table_name        TEXT,
                plan_version      INTEGER,
                plan_token        TEXT,
                disaster_event_id BIGINT,
                generation_id     BIGINT,
                target_lsn        PG_LSN,
                created_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                details           JSONB NOT NULL DEFAULT '{}'::jsonb
            )
        $ddl$;
        EXECUTE 'CREATE INDEX operations_tracking_idx ON flashback.operations (tracking_id, created_at DESC)';
        EXECUTE 'CREATE INDEX operations_plan_token_idx ON flashback.operations (plan_token)';
    END IF;

    IF to_regclass('flashback.operation_events') IS NULL THEN
        EXECUTE $ddl$
            CREATE TABLE flashback.operation_events (
                event_id       BIGSERIAL PRIMARY KEY,
                operation_id   BIGINT NOT NULL
                    REFERENCES flashback.operations(operation_id),
                event_type     TEXT NOT NULL,
                recorded_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                sqlstate       TEXT,
                error_code     TEXT,
                message        TEXT,
                payload        JSONB NOT NULL DEFAULT '{}'::jsonb
            )
        $ddl$;
        EXECUTE 'CREATE INDEX operation_events_op_idx
                 ON flashback.operation_events (operation_id, event_id)';
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION flashback_operations_guard_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'pg_flashback: operations rows are strictly immutable once created'
        USING ERRCODE = 'object_not_in_prerequisite_state',
              HINT = 'Header details cannot be updated; append to operation_events instead.';
END;
$$;

CREATE OR REPLACE FUNCTION flashback_operation_events_guard_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'pg_flashback: operation_events rows are strictly append-only'
        USING ERRCODE = 'object_not_in_prerequisite_state',
              HINT = 'Events cannot be modified or deleted once recorded.';
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'trg_flashback_operations_immutable'
    ) THEN
        CREATE TRIGGER trg_flashback_operations_immutable
        BEFORE UPDATE OR DELETE ON flashback.operations
        FOR EACH ROW EXECUTE FUNCTION public.flashback_operations_guard_trigger();
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'trg_flashback_operation_events_immutable'
    ) THEN
        CREATE TRIGGER trg_flashback_operation_events_immutable
        BEFORE UPDATE OR DELETE ON flashback.operation_events
        FOR EACH ROW EXECUTE FUNCTION public.flashback_operation_events_guard_trigger();
    END IF;
END $$;

-- Projected current state: last event_type wins (no UPDATE of authority rows).
CREATE OR REPLACE VIEW flashback.operation_current_state AS
SELECT DISTINCT ON (o.operation_id)
    o.operation_id,
    o.command,
    o.database_name,
    o.session_user_name,
    o.tracking_id,
    o.table_name,
    o.plan_version,
    o.plan_token,
    o.disaster_event_id,
    o.generation_id,
    o.target_lsn,
    o.created_at,
    e.event_type AS state,
    e.recorded_at AS state_at,
    e.sqlstate,
    e.error_code,
    e.message,
    e.payload
FROM flashback.operations o
JOIN flashback.operation_events e ON e.operation_id = o.operation_id
ORDER BY o.operation_id, e.event_id DESC;

-- Compatibility projection over recover operations (not a second write path).
CREATE OR REPLACE VIEW flashback.restore_log_v AS
SELECT
    o.operation_id AS restore_id,
    o.table_name,
    NULL::timestamptz AS target_time,
    o.target_lsn,
    o.session_user_name AS restored_by,
    o.created_at AS restored_at,
    COALESCE((e.payload->>'rows_affected')::bigint, 0) AS rows_affected,
    (e.event_type = 'verified') AS success,
    CASE WHEN e.event_type = 'failed' THEN e.message ELSE NULL END AS error_message
FROM flashback.operations o
JOIN LATERAL (
    SELECT *
    FROM flashback.operation_events ev
    WHERE ev.operation_id = o.operation_id
    ORDER BY ev.event_id DESC
    LIMIT 1
) e ON true
WHERE o.command IN ('recover', 'restore_lsn');

CREATE OR REPLACE FUNCTION flashback_operation_begin(
    p_command text,
    p_table text DEFAULT NULL,
    p_tracking_id bigint DEFAULT NULL,
    p_plan_version integer DEFAULT NULL,
    p_plan_token text DEFAULT NULL,
    p_disaster_event_id bigint DEFAULT NULL,
    p_generation_id bigint DEFAULT NULL,
    p_target_lsn pg_lsn DEFAULT NULL,
    p_details jsonb DEFAULT '{}'::jsonb
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_id bigint;
BEGIN
    INSERT INTO flashback.operations (
        command, session_user_name, tracking_id, table_name,
        plan_version, plan_token, disaster_event_id, generation_id, target_lsn, details
    ) VALUES (
        p_command, session_user::text, p_tracking_id, p_table,
        p_plan_version, p_plan_token, p_disaster_event_id, p_generation_id, p_target_lsn,
        COALESCE(p_details, '{}'::jsonb)
    ) RETURNING operation_id INTO v_id;

    INSERT INTO flashback.operation_events(operation_id, event_type, payload)
    VALUES (v_id, 'started', jsonb_build_object('phase', 'begin'));

    RETURN v_id;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_operation_append_event(
    p_operation_id bigint,
    p_event_type text,
    p_sqlstate text DEFAULT NULL,
    p_error_code text DEFAULT NULL,
    p_message text DEFAULT NULL,
    p_payload jsonb DEFAULT '{}'::jsonb
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_command text;
    v_cur_event_type text;
    v_cur_payload jsonb;
    v_cur_sqlstate text;
    v_cur_error_code text;
    v_cur_message text;
    v_is_terminal boolean := false;
    v_legal boolean := false;
BEGIN
    IF p_operation_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: operation_append_event requires operation_id'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_event_type IS NULL OR btrim(p_event_type) = '' THEN
        RAISE EXCEPTION 'pg_flashback: operation_append_event requires event_type'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT command INTO v_command
    FROM flashback.operations
    WHERE operation_id = p_operation_id
    FOR UPDATE;

    IF v_command IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: unknown operation_id %', p_operation_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT event_type, payload, sqlstate, error_code, message
      INTO v_cur_event_type, v_cur_payload, v_cur_sqlstate, v_cur_error_code, v_cur_message
    FROM flashback.operation_events
    WHERE operation_id = p_operation_id
    ORDER BY event_id DESC
    LIMIT 1;

    v_is_terminal := (v_cur_event_type IN ('verified', 'failed', 'abandoned', 'unprotected', 'cleaned', 'sealed'));

    IF v_is_terminal THEN
        IF p_event_type IS NOT DISTINCT FROM v_cur_event_type THEN
            IF COALESCE(p_payload, '{}'::jsonb) IS NOT DISTINCT FROM COALESCE(v_cur_payload, '{}'::jsonb)
               AND p_sqlstate IS NOT DISTINCT FROM v_cur_sqlstate
               AND p_error_code IS NOT DISTINCT FROM v_cur_error_code
               AND p_message IS NOT DISTINCT FROM v_cur_message
            THEN
                RETURN;
            ELSE
                RAISE EXCEPTION 'pg_flashback: conflicting terminal event retry for operation % (state %)',
                    p_operation_id, v_cur_event_type
                    USING ERRCODE = 'serialization_failure';
            END IF;
        ELSE
            RAISE EXCEPTION 'pg_flashback: refuse event % on terminal operation % (state %)',
                p_event_type, p_operation_id, v_cur_event_type
                USING ERRCODE = 'object_not_in_prerequisite_state';
        END IF;
    END IF;

    v_legal := CASE
        WHEN v_command IN ('recover', 'restore_lsn') THEN
            (v_cur_event_type = 'started' AND p_event_type = 'applied_coverage_pending')
            OR (v_cur_event_type = 'applied_coverage_pending' AND p_event_type = 'verified')
            OR (v_cur_event_type IN ('started', 'applied_coverage_pending') AND p_event_type = 'failed')
            OR (v_cur_event_type = 'started' AND p_event_type = 'abandoned')
        WHEN v_command = 'unprotect' THEN
            (v_cur_event_type = 'started' AND p_event_type = 'stopping')
            OR (v_cur_event_type = 'stopping' AND p_event_type = 'unprotected')
            OR (v_cur_event_type IN ('started', 'stopping') AND p_event_type = 'failed')
            OR (v_cur_event_type = 'started' AND p_event_type = 'abandoned')
        WHEN v_command = 'cleanup' THEN
            (v_cur_event_type = 'started' AND p_event_type = 'cleaned')
            OR (v_cur_event_type = 'started' AND p_event_type = 'failed')
            OR (v_cur_event_type = 'started' AND p_event_type = 'abandoned')
        WHEN v_command = 'maintain' THEN
            (v_cur_event_type = 'started' AND p_event_type = 'sealed')
            OR (v_cur_event_type = 'started' AND p_event_type = 'failed')
            OR (v_cur_event_type = 'started' AND p_event_type = 'abandoned')
        ELSE false
    END;

    IF NOT v_legal THEN
        RAISE EXCEPTION 'pg_flashback: illegal journal transition % -> % for command % (operation %)',
            v_cur_event_type, p_event_type, v_command, p_operation_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    INSERT INTO flashback.operation_events (
        operation_id, event_type, sqlstate, error_code, message, payload
    ) VALUES (
        p_operation_id, p_event_type, p_sqlstate, p_error_code, p_message,
        COALESCE(p_payload, '{}'::jsonb)
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_operation_history(
    p_table text DEFAULT NULL,
    p_lookback interval DEFAULT interval '7 days'
)
RETURNS TABLE (
    operation_id bigint,
    command text,
    table_name text,
    state text,
    session_user_name text,
    created_at timestamptz,
    state_at timestamptz,
    plan_token text,
    target_lsn pg_lsn,
    error_code text,
    message text
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
    SELECT
        s.operation_id, s.command, s.table_name, s.state, s.session_user_name,
        s.created_at, s.state_at, s.plan_token, s.target_lsn, s.error_code, s.message
    FROM flashback.operation_current_state s
    WHERE s.created_at >= clock_timestamp() - p_lookback
      AND (p_table IS NULL
           OR s.table_name = p_table
           OR s.table_name = format('public.%s', p_table))
    ORDER BY s.created_at DESC, s.operation_id DESC;
$$;

COMMENT ON FUNCTION public.flashback_operation_history(text, interval) IS
    'Operator operation history (not row-change payloads). Source of truth is append-only operation_events.';

-- Client/reconciler: durable failure after a failed or abandoned execute TX.
-- Must be called in a NEW transaction (PostgreSQL cannot keep the failed
-- append if it shares the aborted restore transaction).
CREATE OR REPLACE FUNCTION flashback_recover_mark_failed(
    p_operation_id bigint,
    p_sqlstate text DEFAULT NULL,
    p_error_code text DEFAULT 'restore_failed',
    p_message text DEFAULT NULL,
    p_payload jsonb DEFAULT '{}'::jsonb
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_state text;
BEGIN
    SELECT state INTO v_state
    FROM flashback.operation_current_state
    WHERE operation_id = p_operation_id;

    IF v_state IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: unknown recover operation_id %', p_operation_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF v_state IN ('verified', 'failed', 'abandoned') THEN
        RETURN;
    END IF;
    IF v_state NOT IN ('started', 'applied_coverage_pending') THEN
        RAISE EXCEPTION 'pg_flashback: refuse mark_failed for operation % in state %',
            p_operation_id, v_state
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    PERFORM public.flashback_operation_append_event(
        p_operation_id,
        'failed',
        p_sqlstate,
        COALESCE(p_error_code, 'restore_failed'),
        COALESCE(p_message, 'recover execute failed'),
        COALESCE(p_payload, '{}'::jsonb) || jsonb_build_object('phase', 'mark_failed')
    );
END;
$$;

-- Reconciler: classify durable `started` ops that never reached applied/failed.
-- Never guesses success — only marks abandoned/failed after an explicit window.
CREATE OR REPLACE FUNCTION flashback_reconcile_recover_operations(
    p_stale_after interval DEFAULT interval '5 minutes'
)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    r record;
    v_n integer := 0;
BEGIN
    FOR r IN
        SELECT s.*
        FROM flashback.operation_current_state s
        WHERE s.command IN ('recover', 'restore_lsn')
          AND s.state = 'started'
          AND s.state_at < clock_timestamp() - p_stale_after
    LOOP
        PERFORM public.flashback_operation_append_event(
            r.operation_id, 'abandoned', NULL, 'recover_abandoned',
            'recover begin committed but execute never reached applied_coverage_pending',
            jsonb_build_object(
                'finalizer', 'flashback_reconcile_recover_operations',
                'stale_after', p_stale_after::text
            )
        );
        v_n := v_n + 1;
    END LOOP;
    RETURN v_n;
END;
$$;

-- Worker/finalizer: mark recover ops verified only when the exact successor
-- generation bound at apply time is healthy/active with resolved boundary.
CREATE OR REPLACE FUNCTION flashback_finalize_recover_operations()
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    r record;
    v_payload jsonb;
    v_succ jsonb;
    v_tracking_id bigint;
    v_generation_id bigint;
    v_stream_id bigint;
    v_boundary_xid bigint;
    v_boundary_marker text;
    v_cg record;
    v_health record;
    v_gap boolean;
    v_restore_verification jsonb;
    v_verification_status text;
    v_n integer := 0;
BEGIN
    FOR r IN
        SELECT s.*
        FROM flashback.operation_current_state s
        WHERE s.command IN ('recover', 'restore_lsn')
          AND s.state = 'applied_coverage_pending'
    LOOP
        SELECT e.payload INTO v_payload
        FROM flashback.operation_events e
        WHERE e.operation_id = r.operation_id
          AND e.event_type = 'applied_coverage_pending'
        ORDER BY e.event_id DESC
        LIMIT 1;

        v_restore_verification := v_payload->'restore_verification';
        v_verification_status := v_restore_verification->>'status';
        IF v_verification_status IS NULL THEN
            CONTINUE;
        END IF;
        IF v_verification_status = 'failed' THEN
            PERFORM public.flashback_operation_append_event(
                r.operation_id, 'failed', NULL, 'restore_verification_failed',
                'restore verification proof marked failed',
                COALESCE(v_restore_verification, '{}'::jsonb)
            );
            CONTINUE;
        END IF;
        IF v_verification_status IS DISTINCT FROM 'passed' THEN
            CONTINUE;
        END IF;

        v_succ := COALESCE(v_payload->'successor', '{}'::jsonb);
        v_tracking_id := NULLIF(v_succ->>'tracking_id', '')::bigint;
        v_generation_id := NULLIF(v_succ->>'generation_id', '')::bigint;
        v_stream_id := NULLIF(v_succ->>'stream_id', '')::bigint;
        v_boundary_xid := NULLIF(v_succ->>'boundary_xid', '')::bigint;
        v_boundary_marker := NULLIF(v_succ->>'boundary_marker', '');

        IF v_tracking_id IS NULL OR v_generation_id IS NULL THEN
            -- Incomplete binding: never verify by table_name alone.
            CONTINUE;
        END IF;
        IF r.tracking_id IS NOT NULL AND r.tracking_id IS DISTINCT FROM v_tracking_id THEN
            CONTINUE;
        END IF;

        -- A4: 'verified' is the durable, externally-trusted outcome of an
        -- audited recover operation. Re-check the restore's own independent
        -- proof (expected_proof.binding, built from the pre-swap shadow and
        -- lock-phase disaster identity) against the operation HEADER
        -- (flashback.operations, set at flashback_recover_begin time) one
        -- more time here -- never write 'verified' on a payload whose target
        -- LSN or disaster identity has drifted from what this operation was
        -- actually authorized to restore.
        IF r.target_lsn IS NOT NULL
           AND NULLIF(v_payload->'expected_proof'->'binding'->>'target_lsn', '')::pg_lsn
               IS DISTINCT FROM r.target_lsn
        THEN
            CONTINUE;
        END IF;
        IF r.disaster_event_id IS NOT NULL
           AND NULLIF(v_payload->'expected_proof'->'binding'->>'disaster_event_id', '')::bigint
               IS DISTINCT FROM r.disaster_event_id
        THEN
            CONTINUE;
        END IF;

        SELECT cg.* INTO v_cg
        FROM flashback.coverage_generations cg
        WHERE cg.generation_id = v_generation_id
          AND cg.tracking_id = v_tracking_id
        LIMIT 1;

        IF v_cg.generation_id IS NULL THEN
            CONTINUE;
        END IF;
        IF v_cg.state IS DISTINCT FROM 'active' THEN
            CONTINUE;
        END IF;
        IF v_cg.boundary_lsn IS NULL THEN
            CONTINUE;
        END IF;
        IF v_stream_id IS NOT NULL AND v_cg.stream_id IS DISTINCT FROM v_stream_id THEN
            CONTINUE;
        END IF;
        IF v_boundary_xid IS NOT NULL
           AND v_cg.boundary_xid IS DISTINCT FROM v_boundary_xid THEN
            CONTINUE;
        END IF;
        IF v_boundary_marker IS NOT NULL
           AND v_cg.boundary_marker IS DISTINCT FROM v_boundary_marker THEN
            CONTINUE;
        END IF;

        SELECT EXISTS (
            SELECT 1
            FROM flashback.coverage_gaps gap
            WHERE gap.tracking_id = v_tracking_id
              AND gap.reanchored_by_generation_id IS NULL
              AND gap.gap_start_lsn IS NOT NULL
              AND gap.gap_start_lsn <= v_cg.boundary_lsn
              AND (gap.gap_end_lsn IS NULL OR gap.gap_end_lsn > v_cg.boundary_lsn)
        ) INTO v_gap;
        IF v_gap THEN
            CONTINUE;
        END IF;

        SELECT h.* INTO v_health
        FROM public.flashback_health() h
        WHERE h.tracking_id = v_tracking_id
          AND h.generation_id = v_generation_id
        LIMIT 1;

        IF v_health.tracking_id IS NULL OR v_health.health IS DISTINCT FROM 'healthy' THEN
            CONTINUE;
        END IF;
        IF v_health.valid_through_lsn IS NULL
           OR v_health.valid_through_lsn < v_cg.boundary_lsn THEN
            CONTINUE;
        END IF;

        PERFORM public.flashback_operation_append_event(
            r.operation_id, 'verified', NULL, NULL,
            'exact successor coverage healthy',
            jsonb_build_object(
                'finalizer', 'flashback_finalize_recover_operations',
                'tracking_id', v_tracking_id,
                'generation_id', v_generation_id,
                'boundary_lsn', v_cg.boundary_lsn,
                'stream_id', v_cg.stream_id
            )
        );
        v_n := v_n + 1;
    END LOOP;
    RETURN v_n;
END;
$$;
