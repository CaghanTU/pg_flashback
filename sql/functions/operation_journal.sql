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
SET search_path = pg_catalog, flashback, public
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
SET search_path = pg_catalog, flashback, public
AS $$
BEGIN
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
SET search_path = pg_catalog, flashback, public
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

COMMENT ON FUNCTION flashback_operation_history(text, interval) IS
    'Operator operation history (not row-change payloads). Source of truth is append-only operation_events.';

-- Worker/finalizer: mark recover ops verified after successor coverage is healthy.
CREATE OR REPLACE FUNCTION flashback_finalize_recover_operations()
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    r record;
    v_health text;
    v_n integer := 0;
BEGIN
    FOR r IN
        SELECT s.*
        FROM flashback.operation_current_state s
        WHERE s.command IN ('recover', 'restore_lsn')
          AND s.state = 'applied_coverage_pending'
    LOOP
        SELECT h.health INTO v_health
        FROM flashback_health() h
        WHERE h.table_name = r.table_name
        LIMIT 1;

        IF v_health = 'healthy' THEN
            PERFORM flashback_operation_append_event(
                r.operation_id, 'verified', NULL, NULL,
                'successor coverage healthy',
                jsonb_build_object('finalizer', 'flashback_finalize_recover_operations')
            );
            v_n := v_n + 1;
        END IF;
    END LOOP;
    RETURN v_n;
END;
$$;
