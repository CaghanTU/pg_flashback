-- =================================================================
-- Worker admission / readiness projection.
-- One SQL surface over the Rust canonical database-list contract.
-- Observational only: never starts workers, creates slots, or repairs state.
-- =================================================================

CREATE OR REPLACE FUNCTION flashback_capture_worker_pid()
RETURNS integer
LANGUAGE sql
STABLE
PARALLEL SAFE
SET search_path = pg_catalog, pg_temp
AS $$
    SELECT a.pid
    FROM pg_stat_activity a
    WHERE a.datname = current_database()
      AND (
          a.backend_type = 'pg_flashback delta worker'
          OR a.backend_type ~ '^pg_flashback delta worker [0-9]+$'
      )
    ORDER BY a.pid
    LIMIT 1;
$$;

CREATE OR REPLACE FUNCTION flashback_maintenance_worker_pid()
RETURNS integer
LANGUAGE sql
STABLE
PARALLEL SAFE
SET search_path = pg_catalog, pg_temp
AS $$
    SELECT a.pid
    FROM pg_stat_activity a
    WHERE a.datname = current_database()
      AND (
          a.backend_type = 'pg_flashback maintenance worker'
          OR a.backend_type ~ '^pg_flashback maintenance worker [0-9]+$'
      )
    ORDER BY a.pid
    LIMIT 1;
$$;

CREATE OR REPLACE FUNCTION flashback_worker_readiness()
RETURNS TABLE (
    database_name text,
    configured_index integer,
    admitted boolean,
    admission_state text,
    capture_worker_pid integer,
    maintenance_worker_pid integer,
    capture_running boolean,
    maintenance_running boolean,
    configured_pair_demand integer,
    admitted_pair_count integer,
    max_workers integer,
    max_worker_processes integer,
    bgworker_slots_required integer,
    reason text
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_db text := current_database();
    v_configured text[];
    v_admitted text[];
    v_max_workers integer;
    v_max_worker_processes integer;
    v_idx integer;
    v_admitted_bool boolean;
    v_capture_pid integer;
    v_maintenance_pid integer;
    v_state text;
    v_reason text;
    v_demand integer;
    v_admitted_count integer;
    v_slots_required integer;
BEGIN
    v_configured := COALESCE(flashback_canonical_target_databases(), ARRAY[]::text[]);
    v_admitted := COALESCE(flashback_admitted_target_databases(), ARRAY[]::text[]);
    v_max_workers := flashback_max_worker_pairs();
    v_max_worker_processes := current_setting('max_worker_processes')::integer;
    v_demand := COALESCE(cardinality(v_configured), 0);
    v_admitted_count := COALESCE(cardinality(v_admitted), 0);
    v_slots_required := v_admitted_count * 2;

    SELECT ord::integer - 1
      INTO v_idx
    FROM unnest(v_configured) WITH ORDINALITY AS u(name, ord)
    WHERE u.name = v_db
    LIMIT 1;

    v_admitted_bool := EXISTS (
        SELECT 1 FROM unnest(v_admitted) AS a(name) WHERE a.name = v_db
    );

    v_capture_pid := flashback_capture_worker_pid();
    v_maintenance_pid := flashback_maintenance_worker_pid();

    IF v_idx IS NULL THEN
        v_state := 'not_configured';
        v_reason := format(
            'database %s is absent from pg_flashback.target_databases/target_database',
            v_db
        );
    ELSIF NOT v_admitted_bool THEN
        v_state := 'beyond_max_workers';
        v_reason := format(
            'database %s is configured at index %s but pg_flashback.max_workers=%s admits only the first %s database(s)',
            v_db, v_idx, v_max_workers, v_max_workers
        );
    ELSIF v_slots_required > v_max_worker_processes THEN
        v_state := 'capacity_insufficient';
        v_reason := format(
            'admitted worker pairs require %s background-worker slots but max_worker_processes=%s',
            v_slots_required, v_max_worker_processes
        );
    ELSIF v_capture_pid IS NULL AND v_maintenance_pid IS NULL THEN
        v_state := 'capture_missing';
        v_reason := format(
            'database %s is admitted but neither capture nor maintenance worker is running (startup/restart delay or missing bgworker capacity)',
            v_db
        );
    ELSIF v_capture_pid IS NULL THEN
        v_state := 'capture_missing';
        v_reason := format(
            'database %s is admitted but the capture worker is not running (startup/restart delay or process loss)',
            v_db
        );
    ELSIF v_maintenance_pid IS NULL THEN
        v_state := 'maintenance_missing';
        v_reason := format(
            'database %s capture worker pid=%s is running but the maintenance worker is missing',
            v_db, v_capture_pid
        );
    ELSE
        v_state := 'ready';
        v_reason := format(
            'capture pid=%s and maintenance pid=%s are running for admitted database %s',
            v_capture_pid, v_maintenance_pid, v_db
        );
    END IF;

    database_name := v_db;
    configured_index := v_idx;
    admitted := v_admitted_bool;
    admission_state := v_state;
    capture_worker_pid := v_capture_pid;
    maintenance_worker_pid := v_maintenance_pid;
    capture_running := v_capture_pid IS NOT NULL;
    maintenance_running := v_maintenance_pid IS NOT NULL;
    configured_pair_demand := v_demand;
    admitted_pair_count := v_admitted_count;
    max_workers := v_max_workers;
    max_worker_processes := v_max_worker_processes;
    bgworker_slots_required := v_slots_required;
    reason := v_reason;
    RETURN NEXT;
END;
$$;

-- Fail closed before any tracking lifecycle mutation when capture is absent.
-- Missing maintenance is visible via flashback_worker_readiness()/health as
-- degraded; initial track still requires a live admitted capture worker so WAL
-- consumption cannot silently stall after a false-success track.
CREATE OR REPLACE FUNCTION flashback_require_admitted_capture_worker(p_api text)
RETURNS void
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    r record;
BEGIN
    SELECT * INTO STRICT r FROM flashback_worker_readiness();

    IF r.admission_state = 'ready' OR r.admission_state = 'maintenance_missing' THEN
        IF r.capture_running THEN
            RETURN;
        END IF;
    END IF;

    IF r.admission_state = 'not_configured' THEN
        RAISE EXCEPTION
            'pg_flashback: % refused: database % is not listed in pg_flashback.target_databases/target_database',
            p_api, r.database_name
            USING HINT = 'Add this database to pg_flashback.target_databases and restart PostgreSQL before tracking.';
    ELSIF r.admission_state = 'beyond_max_workers' THEN
        RAISE EXCEPTION
            'pg_flashback: % refused: database % is beyond pg_flashback.max_workers=%s (configured index %s)',
            p_api, r.database_name, r.max_workers, r.configured_index
            USING HINT = 'Raise pg_flashback.max_workers (and max_worker_processes), reorder target_databases, or remove unserved databases, then restart PostgreSQL.';
    ELSIF r.admission_state = 'capacity_insufficient' THEN
        RAISE EXCEPTION
            'pg_flashback: % refused: PostgreSQL max_worker_processes=%s is insufficient for %s required pg_flashback background-worker slots',
            p_api, r.max_worker_processes, r.bgworker_slots_required
            USING HINT = 'Raise max_worker_processes to at least the admitted pair demand times two, then restart PostgreSQL.';
    ELSE
        RAISE EXCEPTION
            'pg_flashback: % refused: admitted capture worker is not running for database % (%)',
            p_api, r.database_name, r.reason
            USING HINT = 'Wait for automatic worker restart (bounded), inspect pg_stat_activity by datname+backend_type, and verify max_worker_processes / shared_preload_libraries.';
    END IF;
END;
$$;

COMMENT ON FUNCTION flashback_canonical_target_databases() IS
    'Canonical configured target database list: trim, drop empties, first-wins dedupe. Shared by worker registration and SQL admission.';
COMMENT ON FUNCTION flashback_admitted_target_databases() IS
    'Configured databases truncated to pg_flashback.max_workers in canonical order. Only these receive registered worker pairs.';
COMMENT ON FUNCTION flashback_max_worker_pairs() IS
    'Effective pg_flashback.max_workers clamp used when registering and admitting worker pairs.';
COMMENT ON FUNCTION flashback_worker_readiness() IS
    'Read-only admission projection for the current database: list membership, max_workers truncation, live capture/maintenance process identity, and bgworker capacity.';
COMMENT ON FUNCTION flashback_require_admitted_capture_worker(text) IS
    'Fail-closed gate used by flashback_track/flashback_track_backup before creating a lifecycle without a running admitted capture worker.';
