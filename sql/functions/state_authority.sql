-- =================================================================
-- Centralized state / progress / lock mutation authority.
--
-- Domain commands call these primitives; trigger/CHECK guards remain
-- the last line of defense. No generic flashback_set_state().
-- =================================================================

-- ------------------------------------------------------------------
-- Lock namespaces (stable; do not renumber)
--   358945 = database / capture-stream advisory lock
--   358944 = lifecycle (tracking_id) advisory lock
-- Session drain locks in the Rust worker use 358945 and stay session-scoped.
-- Transaction lifecycle/stream locks below are xact-scoped only.
-- Canonical order: stream → lifecycle(s) sorted → relation → payload
-- ------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flashback_internal_lock_ns_stream()
RETURNS integer
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$ SELECT 358945::integer $$;

CREATE OR REPLACE FUNCTION flashback_internal_lock_ns_lifecycle()
RETURNS integer
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$ SELECT 358944::integer $$;

CREATE OR REPLACE FUNCTION flashback_internal_lock_database_stream(
    p_database_oid oid
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
BEGIN
    IF p_database_oid IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: database stream lock requires database_oid'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    PERFORM pg_advisory_xact_lock(
        public.flashback_internal_lock_ns_stream(),
        p_database_oid::integer
    );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_internal_lock_lifecycle(
    p_tracking_id bigint
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
BEGIN
    IF p_tracking_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: lifecycle lock requires tracking_id'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    PERFORM pg_advisory_xact_lock(
        public.flashback_internal_lock_ns_lifecycle(),
        hashint8(p_tracking_id)
    );
END;
$$;

-- Sort + DISTINCT tracking_ids, then lock in ascending order (deadlock-safe).
CREATE OR REPLACE FUNCTION flashback_internal_lock_lifecycles(
    p_tracking_ids bigint[]
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_id bigint;
BEGIN
    IF p_tracking_ids IS NULL OR cardinality(p_tracking_ids) = 0 THEN
        RETURN;
    END IF;
    FOR v_id IN
        SELECT DISTINCT tid
        FROM unnest(p_tracking_ids) AS tid
        WHERE tid IS NOT NULL
        ORDER BY 1
    LOOP
        PERFORM public.flashback_internal_lock_lifecycle(v_id);
    END LOOP;
END;
$$;

-- Non-blocking lifecycle pin used by capture batch paths.
CREATE OR REPLACE FUNCTION flashback_internal_try_lock_lifecycle(
    p_tracking_id bigint
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
BEGIN
    IF p_tracking_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: lifecycle try-lock requires tracking_id'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    RETURN pg_try_advisory_xact_lock(
        public.flashback_internal_lock_ns_lifecycle(),
        hashint8(p_tracking_id)
    );
END;
$$;

-- ------------------------------------------------------------------
-- Capture stream state transitions (CAS)
-- Legal edges (from schema CHECK + runtime usage; no invented edges):
--   initializing → active
--   initializing → broken
--   active       → broken
--   active       → retired
--   broken       → retired
--   initializing → retired
-- Retry: already in target with matching terminal stamps → no-op success.
-- ------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flashback_internal_transition_capture_stream(
    p_stream_id bigint,
    p_expected_states text[],
    p_new_state text,
    p_reason text DEFAULT NULL,
    p_details jsonb DEFAULT '{}'::jsonb
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_row flashback.capture_streams%ROWTYPE;
    v_n integer;
    v_legal boolean := false;
BEGIN
    IF p_stream_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: capture stream transition requires stream_id'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_new_state IS NULL OR btrim(p_new_state) = '' THEN
        RAISE EXCEPTION 'pg_flashback: capture stream transition requires new state'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_expected_states IS NULL OR cardinality(p_expected_states) = 0 THEN
        RAISE EXCEPTION 'pg_flashback: capture stream transition requires expected state(s)'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT * INTO v_row
    FROM flashback.capture_streams
    WHERE stream_id = p_stream_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_flashback: unknown capture stream %', p_stream_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- Idempotent retry: already at target.
    IF v_row.state = p_new_state THEN
        IF p_new_state = 'broken' THEN
            IF p_reason IS NOT NULL AND v_row.invalidation_reason IS NOT NULL
               AND p_reason <> v_row.invalidation_reason
            THEN
                RAISE EXCEPTION 'pg_flashback: conflicting retry for broken stream % (reason mismatch: % vs %)',
                    p_stream_id, p_reason, v_row.invalidation_reason
                    USING ERRCODE = 'serialization_failure';
            END IF;
        END IF;
        RETURN false;
    END IF;

    IF NOT (v_row.state = ANY (p_expected_states)) THEN
        RAISE EXCEPTION
            'pg_flashback: capture stream % CAS failed: have state %, expected one of %, wanted %',
            p_stream_id, v_row.state, p_expected_states, p_new_state
            USING ERRCODE = 'serialization_failure';
    END IF;

    -- Exact legal graph (narrow fail-closed).
    v_legal :=
        (v_row.state = 'initializing' AND p_new_state = 'active')
        OR (v_row.state = 'initializing' AND p_new_state = 'broken')
        OR (v_row.state = 'active' AND p_new_state = 'broken')
        OR (v_row.state = 'active' AND p_new_state = 'retired')
        OR (v_row.state = 'broken' AND p_new_state = 'retired')
        OR (v_row.state = 'initializing' AND p_new_state = 'retired');

    IF NOT v_legal THEN
        RAISE EXCEPTION
            'pg_flashback: illegal capture stream transition % -> % (stream %)',
            v_row.state, p_new_state, p_stream_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    IF p_new_state = 'broken'
       AND (p_reason IS NULL OR btrim(p_reason) = '')
    THEN
        RAISE EXCEPTION 'pg_flashback: breaking capture stream % requires reason',
            p_stream_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    UPDATE flashback.capture_streams
       SET state = p_new_state,
           activated_at = CASE
               WHEN p_new_state = 'active'
                    THEN COALESCE(activated_at, clock_timestamp())
               ELSE activated_at
           END,
           invalidated_at = CASE
               WHEN p_new_state = 'broken'
                    THEN COALESCE(invalidated_at, clock_timestamp())
               ELSE invalidated_at
           END,
           invalidation_reason = CASE
               WHEN p_new_state = 'broken'
                    THEN COALESCE(p_reason, invalidation_reason)
               ELSE invalidation_reason
           END,
           retired_at = CASE
               WHEN p_new_state = 'retired'
                    THEN COALESCE(retired_at, clock_timestamp())
               ELSE retired_at
           END,
           details = details || COALESCE(p_details, '{}'::jsonb)
     WHERE stream_id = p_stream_id
       AND state = v_row.state;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> 1 THEN
        RAISE EXCEPTION
            'pg_flashback: capture stream % transition raced (expected exactly 1 row)',
            p_stream_id
            USING ERRCODE = 'serialization_failure';
    END IF;
    RETURN true;
END;
$$;

-- Monotonic capture-stream progress. Never heals broken streams.
CREATE OR REPLACE FUNCTION flashback_internal_advance_capture_stream_progress(
    p_stream_id bigint,
    p_valid_through_lsn pg_lsn DEFAULT NULL,
    p_valid_through_time timestamptz DEFAULT NULL,
    p_confirmed_flush_lsn pg_lsn DEFAULT NULL,
    p_restart_lsn pg_lsn DEFAULT NULL,
    p_details_merge jsonb DEFAULT NULL,
    p_details_remove text[] DEFAULT NULL
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_row flashback.capture_streams%ROWTYPE;
    v_n integer;
    v_new_valid_lsn pg_lsn;
    v_new_valid_time timestamptz;
    v_new_confirmed pg_lsn;
    v_new_restart pg_lsn;
    v_details jsonb;
    v_key text;
BEGIN
    IF p_stream_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: stream progress requires stream_id'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT * INTO v_row
    FROM flashback.capture_streams
    WHERE stream_id = p_stream_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_flashback: unknown capture stream %', p_stream_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    IF v_row.state IS DISTINCT FROM 'active' THEN
        -- Soft refuse for batch callers; hard domain break uses transition API.
        RETURN false;
    END IF;

    v_new_valid_lsn := v_row.valid_through_lsn;
    v_new_valid_time := v_row.valid_through_time;
    IF p_valid_through_lsn IS NOT NULL THEN
        IF v_row.valid_through_lsn IS NOT NULL AND p_valid_through_lsn < v_row.valid_through_lsn THEN
            RAISE EXCEPTION
                'pg_flashback: capture stream % valid_through_lsn cannot move backward',
                p_stream_id
                USING ERRCODE = 'invalid_parameter_value';
        END IF;
        v_new_valid_lsn := p_valid_through_lsn;
        IF p_valid_through_time IS NOT NULL AND (v_row.valid_through_time IS NULL OR p_valid_through_time >= v_row.valid_through_time) THEN
            v_new_valid_time := p_valid_through_time;
        END IF;
    END IF;

    v_new_confirmed := COALESCE(p_confirmed_flush_lsn, v_row.confirmed_flush_lsn);
    IF p_confirmed_flush_lsn IS NOT NULL
       AND v_row.confirmed_flush_lsn IS NOT NULL
       AND p_confirmed_flush_lsn < v_row.confirmed_flush_lsn
    THEN
        RAISE EXCEPTION
            'pg_flashback: capture stream % confirmed_flush_lsn cannot move backward',
            p_stream_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    v_new_restart := COALESCE(p_restart_lsn, v_row.restart_lsn);
    -- restart_lsn may move with PostgreSQL slot semantics; reject only a
    -- clear backward move when both sides are present.
    IF p_restart_lsn IS NOT NULL
       AND v_row.restart_lsn IS NOT NULL
       AND p_restart_lsn < v_row.restart_lsn
    THEN
        RAISE EXCEPTION
            'pg_flashback: capture stream % restart_lsn cannot move backward',
            p_stream_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    v_details := COALESCE(v_row.details, '{}'::jsonb);
    IF p_details_merge IS NOT NULL THEN
        v_details := v_details || p_details_merge;
    END IF;
    IF p_details_remove IS NOT NULL THEN
        FOREACH v_key IN ARRAY p_details_remove LOOP
            v_details := v_details - v_key;
        END LOOP;
    END IF;

    UPDATE flashback.capture_streams
       SET valid_through_lsn = v_new_valid_lsn,
           valid_through_time = v_new_valid_time,
           confirmed_flush_lsn = v_new_confirmed,
           restart_lsn = v_new_restart,
           details = v_details
     WHERE stream_id = p_stream_id
       AND state = 'active';
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> 1 THEN
        RAISE EXCEPTION
            'pg_flashback: capture stream % progress raced or left active',
            p_stream_id
            USING ERRCODE = 'serialization_failure';
    END IF;
    RETURN true;
END;
$$;

-- ------------------------------------------------------------------
-- Coverage generation transitions (matches flashback_guard graph)
--   building → active | aborted
--   active   → sealed
--   sealed   → retired
-- ------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flashback_internal_transition_coverage_generation(
    p_generation_id bigint,
    p_tracking_id bigint,
    p_expected_state text,
    p_new_state text,
    p_state_reason text DEFAULT NULL,
    p_boundary_lsn pg_lsn DEFAULT NULL,
    p_boundary_time timestamptz DEFAULT NULL,
    p_valid_through_lsn pg_lsn DEFAULT NULL,
    p_valid_through_time timestamptz DEFAULT NULL,
    p_superseded_before_lsn pg_lsn DEFAULT NULL,
    p_superseded_before_time timestamptz DEFAULT NULL,
    p_details jsonb DEFAULT '{}'::jsonb
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_row flashback.coverage_generations%ROWTYPE;
    v_n integer;
    v_legal boolean;
BEGIN
    IF p_generation_id IS NULL OR p_tracking_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: generation transition requires generation_id and tracking_id'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_expected_state IS NULL OR p_new_state IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: generation transition requires expected and new state'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT * INTO v_row
    FROM flashback.coverage_generations
    WHERE generation_id = p_generation_id
      AND tracking_id = p_tracking_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'pg_flashback: unknown coverage generation % (tracking %)',
            p_generation_id, p_tracking_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    IF v_row.state = p_new_state THEN
        IF p_new_state = 'active' THEN
            IF (p_boundary_lsn IS NOT NULL AND v_row.boundary_lsn IS NOT NULL AND p_boundary_lsn IS DISTINCT FROM v_row.boundary_lsn)
               OR (p_boundary_time IS NOT NULL AND v_row.boundary_time IS NOT NULL AND p_boundary_time IS DISTINCT FROM v_row.boundary_time)
               OR (p_valid_through_lsn IS NOT NULL AND v_row.valid_through_lsn IS NOT NULL AND p_valid_through_lsn < v_row.valid_through_lsn)
            THEN
                RAISE EXCEPTION 'pg_flashback: conflicting idempotent retry for active generation % (boundary/valid_through mismatch)',
                    p_generation_id
                    USING ERRCODE = 'serialization_failure';
            END IF;
        ELSIF p_new_state = 'sealed' THEN
            IF (p_superseded_before_lsn IS NOT NULL AND v_row.superseded_before_lsn IS NOT NULL AND p_superseded_before_lsn IS DISTINCT FROM v_row.superseded_before_lsn)
               OR (p_superseded_before_time IS NOT NULL AND v_row.superseded_before_time IS NOT NULL AND p_superseded_before_time IS DISTINCT FROM v_row.superseded_before_time)
            THEN
                RAISE EXCEPTION 'pg_flashback: conflicting idempotent retry for sealed generation % (superseded boundary mismatch)',
                    p_generation_id
                    USING ERRCODE = 'serialization_failure';
            END IF;
        END IF;
        RETURN false;
    END IF;

    IF v_row.state IS DISTINCT FROM p_expected_state THEN
        RAISE EXCEPTION
            'pg_flashback: generation % CAS failed: have %, expected %, wanted %',
            p_generation_id, v_row.state, p_expected_state, p_new_state
            USING ERRCODE = 'serialization_failure';
    END IF;

    v_legal :=
        (p_expected_state = 'building' AND p_new_state = 'active')
        OR (p_expected_state = 'building' AND p_new_state = 'aborted')
        OR (p_expected_state = 'active' AND p_new_state = 'sealed')
        OR (p_expected_state = 'sealed' AND p_new_state = 'retired');

    IF NOT v_legal THEN
        RAISE EXCEPTION
            'pg_flashback: illegal generation transition % -> % (generation %)',
            p_expected_state, p_new_state, p_generation_id
            USING ERRCODE = 'invalid_parameter_value',
                  HINT = 'Use active → sealed → retired; direct active → retired is forbidden.';
    END IF;

    UPDATE flashback.coverage_generations
       SET state = p_new_state,
           boundary_lsn = CASE
               WHEN p_new_state = 'active' AND p_boundary_lsn IS NOT NULL
                    THEN p_boundary_lsn
               ELSE boundary_lsn
           END,
           boundary_time = CASE
               WHEN p_new_state = 'active' AND p_boundary_time IS NOT NULL
                    THEN p_boundary_time
               ELSE boundary_time
           END,
           valid_through_lsn = CASE
               WHEN p_new_state = 'active' AND p_valid_through_lsn IS NOT NULL
                    THEN p_valid_through_lsn
               WHEN p_new_state = 'sealed' AND p_valid_through_lsn IS NOT NULL
                    THEN p_valid_through_lsn
               ELSE valid_through_lsn
           END,
           valid_through_time = CASE
               WHEN p_new_state = 'active' AND p_valid_through_time IS NOT NULL
                    THEN p_valid_through_time
               WHEN p_new_state = 'sealed' AND p_valid_through_time IS NOT NULL
                    THEN p_valid_through_time
               ELSE valid_through_time
           END,
           superseded_before_lsn = CASE
               WHEN p_new_state = 'sealed' THEN p_superseded_before_lsn
               ELSE superseded_before_lsn
           END,
           superseded_before_time = CASE
               WHEN p_new_state = 'sealed' THEN p_superseded_before_time
               ELSE superseded_before_time
           END,
           activated_at = CASE
               WHEN p_new_state = 'active'
                    THEN COALESCE(activated_at, clock_timestamp())
               ELSE activated_at
           END,
           sealed_at = CASE
               WHEN p_new_state = 'sealed'
                    THEN COALESCE(sealed_at, clock_timestamp())
               ELSE sealed_at
           END,
           retired_at = CASE
               WHEN p_new_state = 'retired'
                    THEN COALESCE(retired_at, clock_timestamp())
               ELSE retired_at
           END,
           aborted_at = CASE
               WHEN p_new_state = 'aborted'
                    THEN COALESCE(aborted_at, clock_timestamp())
               ELSE aborted_at
           END,
           state_reason = COALESCE(p_state_reason, state_reason),
           details = CASE
               WHEN p_new_state = 'aborted' OR v_row.state = 'building'
                    THEN details || COALESCE(p_details, '{}'::jsonb)
               ELSE details
           END
     WHERE generation_id = p_generation_id
       AND tracking_id = p_tracking_id
       AND state = p_expected_state;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> 1 THEN
        RAISE EXCEPTION
            'pg_flashback: generation % transition raced (expected exactly 1 row)',
            p_generation_id
            USING ERRCODE = 'serialization_failure';
    END IF;
    RETURN true;
END;
$$;

-- Monotonic generation watermark. Refuses when owning stream is not active
-- (prevents false-healthy progress after break/gap).
CREATE OR REPLACE FUNCTION flashback_internal_advance_generation_watermark(
    p_generation_id bigint,
    p_tracking_id bigint,
    p_valid_through_lsn pg_lsn,
    p_valid_through_time timestamptz DEFAULT NULL
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_row flashback.coverage_generations%ROWTYPE;
    v_stream_state text;
    v_new_lsn pg_lsn;
    v_new_time timestamptz;
    v_n integer;
BEGIN
    IF p_generation_id IS NULL OR p_tracking_id IS NULL OR p_valid_through_lsn IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: watermark advance requires generation_id, tracking_id, LSN'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT * INTO v_row
    FROM flashback.coverage_generations
    WHERE generation_id = p_generation_id
      AND tracking_id = p_tracking_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_flashback: unknown generation %', p_generation_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    IF v_row.state NOT IN ('active', 'sealed') THEN
        RAISE EXCEPTION
            'pg_flashback: refuse watermark on generation % in state %',
            p_generation_id, v_row.state
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    SELECT state INTO v_stream_state
    FROM flashback.capture_streams
    WHERE stream_id = v_row.stream_id;

    -- Soft refuse: do not invent health on a broken stream or open gap.
    -- Callers in batch promote paths treat false as skip, not transaction abort.
    IF v_stream_state IS DISTINCT FROM 'active' THEN
        RETURN false;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM flashback.coverage_gaps g
        WHERE g.tracking_id = p_tracking_id
          AND g.source_generation_id = p_generation_id
          AND g.reanchored_by_generation_id IS NULL
    ) THEN
        RETURN false;
    END IF;

    v_new_lsn := LEAST(
        p_valid_through_lsn,
        COALESCE(v_row.superseded_before_lsn, p_valid_through_lsn)
    );

    IF v_row.valid_through_lsn IS NOT NULL AND v_new_lsn < v_row.valid_through_lsn THEN
        -- No-op clamp (already at or past frontier under superseded_before).
        RETURN false;
    END IF;

    IF v_row.valid_through_lsn IS NOT NULL AND v_new_lsn = v_row.valid_through_lsn THEN
        RETURN false;
    END IF;

    v_new_time := CASE
        WHEN v_row.superseded_before_lsn IS NULL
             OR p_valid_through_lsn < v_row.superseded_before_lsn
            THEN COALESCE(p_valid_through_time, v_row.valid_through_time)
        ELSE v_row.valid_through_time
    END;

    UPDATE flashback.coverage_generations
       SET valid_through_lsn = v_new_lsn,
           valid_through_time = v_new_time
     WHERE generation_id = p_generation_id
       AND tracking_id = p_tracking_id
       AND state IN ('active', 'sealed');
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> 1 THEN
        RAISE EXCEPTION 'pg_flashback: generation % watermark raced', p_generation_id
            USING ERRCODE = 'serialization_failure';
    END IF;
    RETURN true;
END;
$$;

-- ------------------------------------------------------------------
-- Payload retirement: retiring → removed only
-- ------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flashback_internal_transition_retirement(
    p_retirement_id bigint,
    p_expected_state text,
    p_new_state text,
    p_delta_rows_removed bigint DEFAULT NULL,
    p_schema_rows_removed bigint DEFAULT NULL
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_row flashback.generation_payload_retirements%ROWTYPE;
    v_n integer;
BEGIN
    IF p_retirement_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: retirement transition requires retirement_id'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT * INTO v_row
    FROM flashback.generation_payload_retirements
    WHERE retirement_id = p_retirement_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_flashback: unknown retirement %', p_retirement_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    IF v_row.state = p_new_state THEN
        IF p_new_state = 'removed' THEN
            IF (p_delta_rows_removed IS NOT NULL AND v_row.delta_rows_removed IS NOT NULL AND p_delta_rows_removed IS DISTINCT FROM v_row.delta_rows_removed)
               OR (p_schema_rows_removed IS NOT NULL AND v_row.schema_rows_removed IS NOT NULL AND p_schema_rows_removed IS DISTINCT FROM v_row.schema_rows_removed)
            THEN
                RAISE EXCEPTION 'pg_flashback: conflicting idempotent retry for retirement % (removed row count mismatch)',
                    p_retirement_id
                    USING ERRCODE = 'serialization_failure';
            END IF;
        END IF;
        RETURN false;
    END IF;

    IF v_row.state IS DISTINCT FROM p_expected_state THEN
        RAISE EXCEPTION
            'pg_flashback: retirement % CAS failed: have %, expected %, wanted %',
            p_retirement_id, v_row.state, p_expected_state, p_new_state
            USING ERRCODE = 'serialization_failure';
    END IF;

    IF NOT (p_expected_state = 'retiring' AND p_new_state = 'removed') THEN
        RAISE EXCEPTION
            'pg_flashback: illegal retirement transition % -> %',
            p_expected_state, p_new_state
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    UPDATE flashback.generation_payload_retirements
       SET state = 'removed',
           delta_rows_removed = p_delta_rows_removed,
           schema_rows_removed = p_schema_rows_removed,
           removed_at = clock_timestamp(),
           removed_by = current_user
     WHERE retirement_id = p_retirement_id
       AND state = 'retiring';
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n <> 1 THEN
        RAISE EXCEPTION 'pg_flashback: retirement % transition raced', p_retirement_id
            USING ERRCODE = 'serialization_failure';
    END IF;
    RETURN true;
END;
$$;

-- ------------------------------------------------------------------
-- Typed constructors for initial state authority
-- ------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flashback_internal_create_capture_stream(
    p_database_oid oid DEFAULT NULL,
    p_initial_state text DEFAULT 'initializing',
    p_epoch_no bigint DEFAULT NULL,
    p_timeline_id bigint DEFAULT 1,
    p_slot_name text DEFAULT NULL,
    p_plugin_name text DEFAULT 'pg_flashback_decoder',
    p_confirmed_flush_lsn pg_lsn DEFAULT NULL,
    p_restart_lsn pg_lsn DEFAULT NULL,
    p_details jsonb DEFAULT '{}'::jsonb
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_db_oid oid := COALESCE(p_database_oid, (SELECT oid FROM pg_database WHERE datname = current_database()));
    v_db_name text := current_database();
    v_epoch bigint;
    v_stream_id bigint;
BEGIN
    IF v_db_oid IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: create stream requires database_oid'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_initial_state NOT IN ('initializing', 'active') THEN
        RAISE EXCEPTION 'pg_flashback: illegal initial capture stream state %', p_initial_state
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    PERFORM public.flashback_internal_lock_database_stream(v_db_oid);

    IF p_epoch_no IS NOT NULL THEN
        v_epoch := p_epoch_no;
    ELSE
        SELECT COALESCE(max(epoch_no), 0) + 1 INTO v_epoch
        FROM flashback.capture_streams
        WHERE database_oid = v_db_oid;
    END IF;

    INSERT INTO flashback.capture_streams (
        database_oid, database_name, epoch_no, capture_mode, timeline_id,
        slot_name, plugin_name, state, valid_through_lsn,
        confirmed_flush_lsn, restart_lsn, activated_at, details
    ) VALUES (
        v_db_oid, v_db_name, v_epoch, 'wal', COALESCE(p_timeline_id, 1),
        p_slot_name, p_plugin_name, p_initial_state, p_confirmed_flush_lsn,
        p_confirmed_flush_lsn, p_restart_lsn,
        CASE WHEN p_initial_state = 'active' THEN clock_timestamp() ELSE NULL END,
        COALESCE(p_details, '{}'::jsonb)
    )
    RETURNING stream_id INTO v_stream_id;

    RETURN v_stream_id;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_internal_create_coverage_generation(
    p_tracking_id bigint,
    p_generation_no bigint,
    p_stream_id bigint,
    p_boundary_kind text DEFAULT 'initial_track',
    p_rel_oid_at_boundary oid DEFAULT NULL,
    p_boundary_snapshot_id bigint DEFAULT NULL,
    p_boundary_lsn pg_lsn DEFAULT NULL,
    p_boundary_time timestamptz DEFAULT NULL,
    p_boundary_xid bigint DEFAULT NULL,
    p_boundary_marker text DEFAULT NULL,
    p_parent_generation_id bigint DEFAULT NULL,
    p_recovery_profile text DEFAULT 'local_delta',
    p_details jsonb DEFAULT '{}'::jsonb
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_gen_id bigint;
    v_stream_state text;
BEGIN
    IF p_tracking_id IS NULL OR p_generation_no IS NULL OR p_stream_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: create generation requires tracking_id, generation_no, stream_id'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_rel_oid_at_boundary IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: create generation requires rel_oid_at_boundary'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    PERFORM public.flashback_internal_lock_lifecycle(p_tracking_id);

    SELECT state INTO v_stream_state
    FROM flashback.capture_streams
    WHERE stream_id = p_stream_id;

    IF v_stream_state IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: stream % does not exist', p_stream_id
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, stream_id, recovery_profile, state,
        boundary_kind, rel_oid_at_boundary, boundary_snapshot_id,
        boundary_lsn, boundary_time, boundary_xid, boundary_marker,
        parent_generation_id, details
    ) VALUES (
        p_tracking_id, p_generation_no, p_stream_id, COALESCE(p_recovery_profile, 'local_delta'), 'building',
        p_boundary_kind, p_rel_oid_at_boundary, p_boundary_snapshot_id,
        p_boundary_lsn, p_boundary_time, p_boundary_xid, p_boundary_marker,
        p_parent_generation_id, COALESCE(p_details, '{}'::jsonb)
    )
    RETURNING generation_id INTO v_gen_id;

    RETURN v_gen_id;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_internal_create_retirement_intent(
    p_generation_id bigint,
    p_tracking_id bigint,
    p_reason text,
    p_snapshot_id bigint,
    p_snapshot_table text,
    p_snapshot_rel_oid oid,
    p_snapshot_row_count bigint,
    p_snapshot_schema_fingerprint text,
    p_expected_delta_rows bigint,
    p_expected_schema_rows bigint,
    p_first_delta_lsn pg_lsn DEFAULT NULL,
    p_last_delta_lsn pg_lsn DEFAULT NULL,
    p_details jsonb DEFAULT '{}'::jsonb
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, pg_temp
AS $$
DECLARE
    v_ret_id bigint;
BEGIN
    IF p_generation_id IS NULL OR p_tracking_id IS NULL OR p_reason IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: create retirement intent requires generation_id, tracking_id, reason'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    PERFORM public.flashback_internal_lock_lifecycle(p_tracking_id);

    INSERT INTO flashback.generation_payload_retirements (
        generation_id, tracking_id, state, reason,
        snapshot_id, snapshot_table, snapshot_rel_oid, snapshot_row_count,
        snapshot_schema_fingerprint, expected_delta_rows, expected_schema_rows,
        first_delta_lsn, last_delta_lsn, details
    ) VALUES (
        p_generation_id, p_tracking_id, 'retiring', p_reason,
        p_snapshot_id, p_snapshot_table, p_snapshot_rel_oid, p_snapshot_row_count,
        p_snapshot_schema_fingerprint, p_expected_delta_rows, p_expected_schema_rows,
        p_first_delta_lsn, p_last_delta_lsn, COALESCE(p_details, '{}'::jsonb)
    )
    RETURNING retirement_id INTO v_ret_id;

    RETURN v_ret_id;
END;
$$;

REVOKE ALL ON FUNCTION public.flashback_internal_lock_ns_stream() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_lock_ns_lifecycle() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_lock_database_stream(oid) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_lock_lifecycle(bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_lock_lifecycles(bigint[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_try_lock_lifecycle(bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_transition_capture_stream(bigint, text[], text, text, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_advance_capture_stream_progress(bigint, pg_lsn, timestamptz, pg_lsn, pg_lsn, jsonb, text[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_transition_coverage_generation(bigint, bigint, text, text, text, pg_lsn, timestamptz, pg_lsn, timestamptz, pg_lsn, timestamptz, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_advance_generation_watermark(bigint, bigint, pg_lsn, timestamptz) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_transition_retirement(bigint, text, text, bigint, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_create_capture_stream(oid, text, bigint, bigint, text, text, pg_lsn, pg_lsn, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_create_coverage_generation(bigint, bigint, bigint, text, oid, bigint, pg_lsn, timestamptz, bigint, text, bigint, text, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.flashback_internal_create_retirement_intent(bigint, bigint, text, bigint, text, oid, bigint, text, bigint, bigint, pg_lsn, pg_lsn, jsonb) FROM PUBLIC;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flashback_admin') THEN
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_lock_database_stream(oid) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_lock_lifecycle(bigint) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_lock_lifecycles(bigint[]) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_try_lock_lifecycle(bigint) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_transition_capture_stream(bigint, text[], text, text, jsonb) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_advance_capture_stream_progress(bigint, pg_lsn, timestamptz, pg_lsn, pg_lsn, jsonb, text[]) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_transition_coverage_generation(bigint, bigint, text, text, text, pg_lsn, timestamptz, pg_lsn, timestamptz, pg_lsn, timestamptz, jsonb) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_advance_generation_watermark(bigint, bigint, pg_lsn, timestamptz) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_transition_retirement(bigint, text, text, bigint, bigint) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_create_capture_stream(oid, text, bigint, bigint, text, text, pg_lsn, pg_lsn, jsonb) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_create_coverage_generation(bigint, bigint, bigint, text, oid, bigint, pg_lsn, timestamptz, bigint, text, bigint, text, jsonb) FROM flashback_admin';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_create_retirement_intent(bigint, bigint, text, bigint, text, oid, bigint, text, bigint, bigint, pg_lsn, pg_lsn, jsonb) FROM flashback_admin';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pg_monitor') THEN
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_lock_database_stream(oid) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_lock_lifecycle(bigint) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_lock_lifecycles(bigint[]) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_try_lock_lifecycle(bigint) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_transition_capture_stream(bigint, text[], text, text, jsonb) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_advance_capture_stream_progress(bigint, pg_lsn, timestamptz, pg_lsn, pg_lsn, jsonb, text[]) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_transition_coverage_generation(bigint, bigint, text, text, text, pg_lsn, timestamptz, pg_lsn, timestamptz, pg_lsn, timestamptz, jsonb) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_advance_generation_watermark(bigint, bigint, pg_lsn, timestamptz) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_transition_retirement(bigint, text, text, bigint, bigint) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_create_capture_stream(oid, text, bigint, bigint, text, text, pg_lsn, pg_lsn, jsonb) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_create_coverage_generation(bigint, bigint, bigint, text, oid, bigint, pg_lsn, timestamptz, bigint, text, bigint, text, jsonb) FROM pg_monitor';
        EXECUTE 'REVOKE ALL ON FUNCTION public.flashback_internal_create_retirement_intent(bigint, bigint, text, bigint, text, oid, bigint, text, bigint, bigint, pg_lsn, pg_lsn, jsonb) FROM pg_monitor';
    END IF;
END
$$;
