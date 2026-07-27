-- =================================================================
-- Operator-facing SECURITY DEFINER projections.
-- CLI must not SELECT flashback.* payload/catalog tables directly.
-- PUBLIC denied by rbac finalize; flashback_admin gets EXECUTE only.
-- =================================================================

CREATE OR REPLACE FUNCTION flashback_resolve_lifecycle_name(p_table text)
RETURNS text
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_name text;
BEGIN
    IF to_regclass(p_table) IS NOT NULL THEN
        SELECT format('%I.%I', n.nspname, c.relname)
          INTO v_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.oid = to_regclass(p_table);
        IF v_name IS NOT NULL THEN
            RETURN v_name;
        END IF;
    END IF;

    -- Historical-fallback mode of the single canonical resolver: this is a
    -- display helper, so a table that was tracked, later unprotected, and
    -- never retracked must still resolve to its canonical name -- but an
    -- ambiguous unqualified name (active or historical) must still be
    -- rejected here too, never silently resolved to one of several
    -- same-named lifecycles across schemas. A not-found name is not an
    -- error for this display helper -- it falls back to the raw input.
    SELECT format('%I.%I', r.schema_name, r.table_name)
      INTO v_name
    FROM public.flashback_internal_resolve_tracked_table_any(p_table) r;

    RETURN COALESCE(v_name, p_table);
END;
$$;

CREATE OR REPLACE FUNCTION flashback_list_lifecycles()
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'table_name', format('%I.%I', tt.schema_name, tt.table_name),
        'tracking_id', tt.tracking_id,
        'is_active', tt.is_active,
        'protection_state', COALESCE(tt.protection_state, 'active'),
        'tracked_since', tt.tracked_since,
        'retention_interval', tt.retention_interval
    ) ORDER BY tt.schema_name, tt.table_name), '[]'::jsonb)
    FROM flashback.tracked_tables tt
    WHERE tt.recovery_profile = 'local_delta';
$$;

CREATE OR REPLACE FUNCTION flashback_is_actively_protected(p_table text)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
    -- Single canonical, ambiguity-safe resolver. An ambiguous unqualified
    -- name raises rather than returning a possibly-wrong true/false.
    SELECT EXISTS (SELECT 1 FROM public.flashback_internal_resolve_tracked_table(p_table));
$$;

CREATE OR REPLACE FUNCTION flashback_lifecycle_health(p_table text)
RETURNS text
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
    SELECT COALESCE(
        (
            SELECT h.health
            FROM flashback_health() h
            WHERE h.table_name = flashback_resolve_lifecycle_name(p_table)
               OR h.table_name = p_table
            ORDER BY CASE h.health WHEN 'healthy' THEN 0 ELSE 1 END
            LIMIT 1
        ),
        'missing'
    );
$$;

CREATE OR REPLACE FUNCTION flashback_operation_state(p_operation_id bigint)
RETURNS text
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
    SELECT s.state
    FROM flashback.operation_current_state s
    WHERE s.operation_id = p_operation_id;
$$;

CREATE OR REPLACE FUNCTION flashback_status_snapshot(p_table text DEFAULT NULL)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_workers record;
    v_slot record;
    v_disk bigint;
    v_tables jsonb;
    v_latest_drop jsonb;
    v_requested text := NULLIF(p_table, '');
BEGIN
    SELECT * INTO STRICT v_workers FROM flashback_worker_readiness();
    SELECT * INTO v_slot FROM flashback_slot_status_snapshot() LIMIT 1;

    SELECT COALESCE(sum(pg_total_relation_size(c.oid)), 0)
      INTO v_disk
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'flashback';

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'table_name', h.table_name,
        'tracking_id', h.tracking_id,
        'protected', true,
        'health', h.health,
        'recommended_action', h.recommended_action,
        'open_gap_count', h.open_gap_count,
        'reason', h.reason,
        'valid_through_lsn', h.valid_through_lsn,
        'recovery_profile', h.recovery_profile,
        'generation_id', h.generation_id,
        'generation_state', h.generation_state
    ) ORDER BY h.table_name), '[]'::jsonb)
      INTO v_tables
    FROM flashback_health() h
    WHERE v_requested IS NULL
       OR h.table_name = v_requested
       OR h.table_name = flashback_resolve_lifecycle_name(v_requested);

    -- Latest DROP recoverability is separate from coverage health.
    IF v_requested IS NOT NULL THEN
        SELECT COALESCE(jsonb_agg(jsonb_build_object(
            'table_name', d.table_name,
            'disaster_commit_lsn', d.disaster_commit_lsn,
            'disaster_time', d.disaster_time,
            'status', d.status,
            'reason', d.reason,
            'restorable', (d.status = 'restorable' AND d.safe_target_lsn IS NOT NULL),
            'safe_target_lsn', d.safe_target_lsn
        ) ORDER BY d.disaster_commit_lsn DESC NULLS LAST), '[]'::jsonb)
          INTO v_latest_drop
        FROM (
            SELECT *
            FROM flashback_disaster_points(
                flashback_resolve_lifecycle_name(v_requested),
                interval '30 days'
            ) d
            WHERE d.event_type = 'DROP'
            ORDER BY d.disaster_commit_lsn DESC NULLS LAST
            LIMIT 5
        ) d;
    ELSE
        SELECT COALESCE(jsonb_agg(x.obj ORDER BY x.table_name), '[]'::jsonb)
          INTO v_latest_drop
        FROM (
            SELECT h.table_name,
                   jsonb_build_object(
                       'table_name', h.table_name,
                       'disaster_commit_lsn', d.disaster_commit_lsn,
                       'disaster_time', d.disaster_time,
                       'status', d.status,
                       'reason', d.reason,
                       'restorable', (d.status = 'restorable' AND d.safe_target_lsn IS NOT NULL),
                       'safe_target_lsn', d.safe_target_lsn
                   ) AS obj
            FROM flashback_health() h
            LEFT JOIN LATERAL (
                SELECT *
                FROM flashback_disaster_points(h.table_name, interval '30 days') dp
                WHERE dp.event_type = 'DROP'
                ORDER BY dp.disaster_commit_lsn DESC NULLS LAST
                LIMIT 1
            ) d ON true
            WHERE d.table_name IS NOT NULL
        ) x;
    END IF;

    RETURN jsonb_build_object(
        'workers', jsonb_build_object(
            'admission_state', v_workers.admission_state,
            'capture_running', v_workers.capture_running,
            'maintenance_running', v_workers.maintenance_running,
            'reason', v_workers.reason
        ),
        'slot', CASE WHEN v_slot.slot_name IS NULL THEN NULL ELSE jsonb_build_object(
            'name', v_slot.slot_name,
            'wal_status', v_slot.wal_status,
            'retained_bytes', v_slot.retained_wal_bytes,
            'safe_wal_size', v_slot.safe_wal_size
        ) END,
        'flashback_disk_bytes', v_disk,
        'disk_retention', flashback_disk_retention_status(),
        'tables', v_tables,
        'latest_drop_recoverability', v_latest_drop,
        'note', 'coverage health and latest DROP recoverability are independent signals'
    );
END;
$$;

COMMENT ON FUNCTION flashback_resolve_lifecycle_name(text) IS
    'Resolve a live or historically tracked local_delta table name without granting SELECT on flashback.tracked_tables.';
COMMENT ON FUNCTION flashback_list_lifecycles() IS
    'Operator projection of local_delta lifecycles for CLI list.';
COMMENT ON FUNCTION flashback_status_snapshot(text) IS
    'Operator status projection: workers, slot, disk, coverage health, and latest DROP recoverability.';
