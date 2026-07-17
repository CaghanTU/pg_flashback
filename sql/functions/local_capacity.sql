-- =================================================================
-- Shared local-profile capacity and write-stall admission.
--
-- One model serves track, re-anchor and restore. CTAS copies heap+TOAST only;
-- restore peak additionally budgets rebuilt indexes on the shadow relation and
-- a successor base. Filesystem free-space checks are OS-backed estimates that
-- race with concurrent writers; they never claim perfect future-space
-- prediction. Correctness depends on the hard guard, not flashback_advise().
-- =================================================================

CREATE OR REPLACE FUNCTION flashback_local_guc_bytes(p_name text, p_default text DEFAULT NULL)
RETURNS bigint
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    v_raw text;
    v_bytes bigint;
BEGIN
    v_raw := NULLIF(current_setting(p_name, true), '');
    IF v_raw IS NULL THEN
        IF p_default IS NULL THEN
            RETURN NULL;
        END IF;
        v_raw := p_default;
    END IF;
    BEGIN
        v_bytes := pg_size_bytes(v_raw);
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'pg_flashback: invalid % value %', p_name, v_raw;
    END;
    IF v_bytes < 0 THEN
        RAISE EXCEPTION 'pg_flashback: % must be non-negative', p_name;
    END IF;
    RETURN v_bytes;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_local_guc_int(p_name text, p_default integer)
RETURNS integer
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    v_setting text;
    v_value integer;
BEGIN
    -- pg_settings.setting returns the raw base-unit value even when SHOW
    -- pretty-prints UNIT_MS GUCs as "1min" / "30s".
    SELECT s.setting INTO v_setting
    FROM pg_settings s
    WHERE s.name = p_name;
    IF v_setting IS NULL OR v_setting = '' THEN
        RETURN p_default;
    END IF;
    BEGIN
        v_value := v_setting::integer;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'pg_flashback: invalid % value %', p_name, v_setting;
    END;
    RETURN v_value;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_local_capacity_override_active()
RETURNS boolean
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    v_raw text;
BEGIN
    v_raw := lower(COALESCE(current_setting('pg_flashback.local_capacity_override', true), 'off'));
    IF v_raw NOT IN ('on', 'true', '1', 'yes') THEN
        RETURN false;
    END IF;
    -- Privileged roles only so the override cannot silently become an
    -- unprivileged default path.
    IF NOT (
        pg_catalog.pg_has_role(session_user, 'flashback_admin', 'MEMBER')
        OR EXISTS (
            SELECT 1
            FROM pg_roles
            WHERE rolname = session_user
              AND rolsuper
        )
    ) THEN
        RAISE EXCEPTION 'pg_flashback.local_capacity_override requires flashback_admin or superuser'
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_payload_tablespace_oid()
RETURNS oid
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
    -- Payload tables live in schema flashback under the database default
    -- tablespace (relation-level tablespaces are out of first-release scope).
    SELECT d.dattablespace
    FROM pg_database d
    WHERE d.datname = current_database();
$$;

CREATE OR REPLACE FUNCTION flashback_measure_local_capacity(p_rel regclass)
RETURNS TABLE (
    rel oid,
    live_heap_bytes bigint,
    live_toast_bytes bigint,
    live_index_bytes bigint,
    live_total_bytes bigint,
    projected_base_snapshot_bytes bigint,
    projected_restore_shadow_bytes bigint,
    projected_successor_base_bytes bigint,
    retained_local_payload_bytes bigint,
    configured_max_snapshot_bytes bigint,
    configured_max_restore_peak_bytes bigint,
    configured_min_filesystem_bytes bigint,
    configured_safety_reserve_bytes bigint,
    configured_write_stall_ms integer,
    assumed_copy_mib_per_sec integer,
    projected_track_bytes bigint,
    projected_reanchor_bytes bigint,
    projected_restore_peak_bytes bigint,
    estimated_copy_ms bigint,
    filesystem_available_bytes bigint,
    capacity_override boolean
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_rel oid := p_rel;
    v_heap bigint;
    v_toast bigint;
    v_index bigint;
    v_retained bigint := 0;
    v_tracking_id bigint;
    v_max_snapshot bigint;
    v_max_peak bigint;
    v_min_fs bigint;
    v_reserve bigint;
    v_stall_ms integer;
    v_mib_per_sec integer;
    v_fs_available bigint;
    v_payload_ts oid;
BEGIN
    IF v_rel IS NULL THEN
        RAISE EXCEPTION 'flashback_measure_local_capacity: relation is NULL';
    END IF;

    v_heap := pg_relation_size(v_rel);
    SELECT COALESCE(pg_relation_size(c.reltoastrelid), 0)
      INTO v_toast
    FROM pg_class c
    WHERE c.oid = v_rel;
    v_toast := COALESCE(v_toast, 0);
    v_index := COALESCE(pg_indexes_size(v_rel), 0);

    SELECT tt.tracking_id INTO v_tracking_id
    FROM flashback.tracked_tables tt
    WHERE tt.rel_oid = v_rel
      AND tt.is_active
    LIMIT 1;

    IF v_tracking_id IS NOT NULL THEN
        SELECT COALESCE(sum(pg_total_relation_size(to_regclass(s.snapshot_table))), 0)
          INTO v_retained
        FROM flashback.snapshots s
        WHERE s.tracking_id = v_tracking_id
          AND COALESCE(s.payload_state, 'available') = 'available'
          AND NULLIF(s.snapshot_table, '') IS NOT NULL
          AND to_regclass(s.snapshot_table) IS NOT NULL;
        v_retained := COALESCE(v_retained, 0) + COALESCE((
            SELECT sum(pg_column_size(dl))::bigint
            FROM flashback.delta_log dl
            WHERE dl.tracking_id = v_tracking_id
        ), 0);
    END IF;

    v_max_snapshot := flashback_local_guc_bytes('pg_flashback.local_max_snapshot_bytes');
    v_max_peak := flashback_local_guc_bytes('pg_flashback.local_max_restore_peak_bytes');
    -- Accept the historical restore GUC name during the unreleased cutover.
    IF v_max_peak IS NULL THEN
        v_max_peak := flashback_local_guc_bytes('pg_flashback.local_restore_max_peak_bytes');
    END IF;
    v_min_fs := flashback_local_guc_bytes('pg_flashback.local_min_filesystem_bytes');
    v_reserve := COALESCE(
        flashback_local_guc_bytes('pg_flashback.local_safety_reserve_bytes'),
        flashback_local_guc_bytes('pg_flashback.local_restore_safety_reserve_bytes'),
        pg_size_bytes('64MB')
    );
    v_stall_ms := flashback_local_guc_int('pg_flashback.local_boundary_write_stall_ms', 30000);
    v_mib_per_sec := flashback_local_guc_int('pg_flashback.local_assumed_copy_mib_per_sec', 32);
    IF v_stall_ms < 1 THEN
        RAISE EXCEPTION 'pg_flashback.local_boundary_write_stall_ms must be >= 1';
    END IF;
    IF v_mib_per_sec < 1 THEN
        RAISE EXCEPTION 'pg_flashback.local_assumed_copy_mib_per_sec must be >= 1';
    END IF;

    v_payload_ts := flashback_payload_tablespace_oid();
    v_fs_available := flashback_tablespace_filesystem_available_bytes(v_payload_ts);

    rel := v_rel;
    live_heap_bytes := v_heap;
    live_toast_bytes := v_toast;
    live_index_bytes := v_index;
    live_total_bytes := v_heap + v_toast + v_index;
    -- CTAS / base snapshot copies heap+TOAST only.
    projected_base_snapshot_bytes := v_heap + v_toast;
    -- Restore shadow rebuilds indexes on top of heap+TOAST.
    projected_restore_shadow_bytes := v_heap + v_toast + v_index;
    projected_successor_base_bytes := v_heap + v_toast;
    retained_local_payload_bytes := COALESCE(v_retained, 0);
    configured_max_snapshot_bytes := v_max_snapshot;
    configured_max_restore_peak_bytes := v_max_peak;
    configured_min_filesystem_bytes := v_min_fs;
    configured_safety_reserve_bytes := v_reserve;
    configured_write_stall_ms := v_stall_ms;
    assumed_copy_mib_per_sec := v_mib_per_sec;
    projected_track_bytes := (v_heap + v_toast) + v_reserve;
    projected_reanchor_bytes := (v_heap + v_toast) + v_reserve;
    projected_restore_peak_bytes :=
        (v_heap + v_toast + v_index) + (v_heap + v_toast) + v_reserve;
    estimated_copy_ms := CEIL(
        ((v_heap + v_toast)::numeric * 1000.0)
        / (v_mib_per_sec::numeric * 1048576.0)
    )::bigint;
    filesystem_available_bytes := v_fs_available;
    capacity_override := flashback_local_capacity_override_active();
    RETURN NEXT;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_apply_local_boundary_lock_timeout()
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_stall_ms integer;
BEGIN
    v_stall_ms := flashback_local_guc_int('pg_flashback.local_boundary_write_stall_ms', 30000);
    IF v_stall_ms < 1 THEN
        RAISE EXCEPTION 'pg_flashback.local_boundary_write_stall_ms must be >= 1';
    END IF;
    PERFORM set_config('lock_timeout', v_stall_ms::text || 'ms', true);
    RETURN v_stall_ms;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_admit_local_capacity(
    p_rel regclass,
    p_operation text
)
RETURNS bigint
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    m record;
    v_needed bigint;
    v_budget bigint;
    v_override boolean;
    v_reasons text[] := ARRAY[]::text[];
BEGIN
    IF p_operation NOT IN ('track', 'reanchor', 'restore') THEN
        RAISE EXCEPTION 'flashback_admit_local_capacity: unknown operation %', p_operation;
    END IF;

    SELECT * INTO STRICT m FROM flashback_measure_local_capacity(p_rel);
    v_override := m.capacity_override;

    IF p_operation IN ('track', 'reanchor') THEN
        v_needed := CASE
            WHEN p_operation = 'track' THEN m.projected_track_bytes
            ELSE m.projected_reanchor_bytes
        END;
        v_budget := m.configured_max_snapshot_bytes;
        IF v_budget IS NULL OR v_budget = 0 THEN
            v_reasons := v_reasons || ARRAY[
                'pg_flashback.local_max_snapshot_bytes is unset; qualified local admission is fail-closed'
            ];
        ELSIF m.projected_base_snapshot_bytes > v_budget THEN
            v_reasons := v_reasons || ARRAY[format(
                'projected base snapshot %s bytes exceeds local_max_snapshot_bytes=%s',
                m.projected_base_snapshot_bytes, v_budget
            )];
        END IF;
        IF m.estimated_copy_ms > m.configured_write_stall_ms THEN
            v_reasons := v_reasons || ARRAY[format(
                'estimated CTAS copy %s ms exceeds local_boundary_write_stall_ms=%s',
                m.estimated_copy_ms, m.configured_write_stall_ms
            )];
        END IF;
    ELSE
        v_needed := m.projected_restore_peak_bytes;
        v_budget := m.configured_max_restore_peak_bytes;
        IF v_budget IS NULL OR v_budget = 0 THEN
            v_reasons := v_reasons || ARRAY[
                'pg_flashback.local_max_restore_peak_bytes is unset; qualified local admission is fail-closed'
            ];
        ELSIF m.projected_restore_peak_bytes > v_budget THEN
            v_reasons := v_reasons || ARRAY[format(
                'projected restore peak %s bytes exceeds local_max_restore_peak_bytes=%s',
                m.projected_restore_peak_bytes, v_budget
            )];
        END IF;
    END IF;

    IF m.configured_min_filesystem_bytes IS NULL
       OR m.configured_min_filesystem_bytes = 0
    THEN
        v_reasons := v_reasons || ARRAY[
            'pg_flashback.local_min_filesystem_bytes is unset; qualified local admission is fail-closed'
        ];
    ELSIF m.filesystem_available_bytes < (v_needed + m.configured_min_filesystem_bytes) THEN
        v_reasons := v_reasons || ARRAY[format(
            'filesystem available %s bytes is below projected need %s plus min reserve %s',
            m.filesystem_available_bytes, v_needed, m.configured_min_filesystem_bytes
        )];
    END IF;

    IF array_length(v_reasons, 1) IS NOT NULL THEN
        IF v_override THEN
            RAISE WARNING
                'pg_flashback: local capacity override admitting % for % despite: %',
                p_operation, p_rel, array_to_string(v_reasons, '; ');
        ELSE
            RAISE EXCEPTION
                'pg_flashback: local % capacity admission failed: %',
                p_operation, array_to_string(v_reasons, '; ')
                USING ERRCODE = 'disk_full',
                      HINT = 'Raise the configured local budgets, free filesystem space, use the backup profile, or set pg_flashback.local_capacity_override only as an explicit privileged escape hatch.';
        END IF;
    END IF;

    RETURN v_needed;
END;
$$;

-- Compatibility wrappers used by existing restore callers and RBAC grants.
CREATE OR REPLACE FUNCTION flashback_estimate_local_restore_peak_bytes(p_rel regclass)
RETURNS bigint
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
    SELECT projected_restore_peak_bytes
    FROM flashback_measure_local_capacity(p_rel);
$$;

CREATE OR REPLACE FUNCTION flashback_local_restore_preflight(p_rel regclass)
RETURNS bigint
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
    SELECT flashback_admit_local_capacity(p_rel, 'restore');
$$;

CREATE OR REPLACE FUNCTION flashback_advise(p_rel regclass)
RETURNS TABLE (
    table_name text,
    live_heap_bytes bigint,
    live_toast_bytes bigint,
    live_index_bytes bigint,
    projected_base_snapshot_bytes bigint,
    projected_restore_shadow_bytes bigint,
    projected_successor_base_bytes bigint,
    retained_local_payload_bytes bigint,
    projected_track_bytes bigint,
    projected_restore_peak_bytes bigint,
    filesystem_available_bytes bigint,
    configured_max_snapshot_bytes bigint,
    configured_max_restore_peak_bytes bigint,
    configured_min_filesystem_bytes bigint,
    configured_safety_reserve_bytes bigint,
    configured_write_stall_ms integer,
    estimated_copy_ms bigint,
    estimated_lock_copy_risk text,
    capacity_override boolean,
    recommendation text
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    m record;
    v_risk text;
    v_rec text;
BEGIN
    SELECT * INTO STRICT m FROM flashback_measure_local_capacity(p_rel);

    IF m.estimated_copy_ms > m.configured_write_stall_ms THEN
        v_risk := 'high';
    ELSIF m.estimated_copy_ms > (m.configured_write_stall_ms / 2) THEN
        v_risk := 'elevated';
    ELSE
        v_risk := 'bounded';
    END IF;

    IF m.configured_max_snapshot_bytes IS NULL
       OR m.configured_max_snapshot_bytes = 0
       OR m.configured_max_restore_peak_bytes IS NULL
       OR m.configured_max_restore_peak_bytes = 0
       OR m.configured_min_filesystem_bytes IS NULL
       OR m.configured_min_filesystem_bytes = 0
    THEN
        v_rec := 'configure local_max_snapshot_bytes, local_max_restore_peak_bytes and local_min_filesystem_bytes before qualified local operations';
    ELSIF m.projected_base_snapshot_bytes > COALESCE(m.configured_max_snapshot_bytes, 0)
       OR m.projected_restore_peak_bytes > COALESCE(m.configured_max_restore_peak_bytes, 0)
       OR m.filesystem_available_bytes
            < (GREATEST(m.projected_track_bytes, m.projected_restore_peak_bytes)
               + m.configured_min_filesystem_bytes)
       OR m.estimated_copy_ms > m.configured_write_stall_ms
    THEN
        v_rec := 'reject local profile; use backup profile or raise configured budgets';
    ELSE
        v_rec := 'local profile admission is currently within configured budgets';
    END IF;

    IF m.capacity_override THEN
        v_rec := v_rec || '; local_capacity_override is active';
    END IF;

    table_name := p_rel::text;
    live_heap_bytes := m.live_heap_bytes;
    live_toast_bytes := m.live_toast_bytes;
    live_index_bytes := m.live_index_bytes;
    projected_base_snapshot_bytes := m.projected_base_snapshot_bytes;
    projected_restore_shadow_bytes := m.projected_restore_shadow_bytes;
    projected_successor_base_bytes := m.projected_successor_base_bytes;
    retained_local_payload_bytes := m.retained_local_payload_bytes;
    projected_track_bytes := m.projected_track_bytes;
    projected_restore_peak_bytes := m.projected_restore_peak_bytes;
    filesystem_available_bytes := m.filesystem_available_bytes;
    configured_max_snapshot_bytes := m.configured_max_snapshot_bytes;
    configured_max_restore_peak_bytes := m.configured_max_restore_peak_bytes;
    configured_min_filesystem_bytes := m.configured_min_filesystem_bytes;
    configured_safety_reserve_bytes := m.configured_safety_reserve_bytes;
    configured_write_stall_ms := m.configured_write_stall_ms;
    estimated_copy_ms := m.estimated_copy_ms;
    estimated_lock_copy_risk := v_risk;
    capacity_override := m.capacity_override;
    recommendation := v_rec;
    RETURN NEXT;
END;
$$;

COMMENT ON FUNCTION flashback_advise(regclass) IS
    'Advisory local capacity/write-stall projection. Correctness depends on flashback_admit_local_capacity(), not this helper. Free-space figures are estimates and race with concurrent writers.';

COMMENT ON FUNCTION flashback_admit_local_capacity(regclass, text) IS
    'Fail-closed local capacity and write-stall admission shared by track, re-anchor and restore.';
