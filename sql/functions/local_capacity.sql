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
    -- unprivileged default path. Non-privileged callers see override as
    -- inactive (advise/health stay readable); admit still fail-closes on budgets.
    IF NOT (
        pg_catalog.pg_has_role(session_user, 'flashback_admin', 'MEMBER')
        OR EXISTS (
            SELECT 1
            FROM pg_roles
            WHERE rolname = session_user
              AND rolsuper
        )
    ) THEN
        RETURN false;
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
        SELECT COALESCE(sum(size_bytes), 0)
          INTO v_retained
        FROM flashback_internal_snapshot_sizes(v_tracking_id, ARRAY['available']);
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
    -- Restore peak must budget the shadow (heap+TOAST+indexes), the live
    -- relation retained until commit (DROP is deferred), the successor base
    -- CTAS (heap+TOAST), and the configured safety reserve. See STORAGE_POLICY.
    projected_restore_peak_bytes :=
        (v_heap + v_toast + v_index)
        + (v_heap + v_toast + v_index)
        + (v_heap + v_toast)
        + v_reserve;
    -- CTAS duration estimate for one heap+TOAST copy (track/re-anchor).
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

CREATE OR REPLACE FUNCTION flashback_measure_external_snapshot_capacity(p_rel regclass)
RETURNS TABLE (
    rel oid,
    source_heap_bytes bigint,
    source_toast_bytes bigint,
    source_index_bytes bigint,
    projected_artifact_bytes bigint,
    configured_min_free_bytes bigint,
    configured_safety_reserve_bytes bigint,
    filesystem_available_bytes bigint,
    admissible boolean
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
    v_min_free bigint;
    v_reserve bigint;
    v_available bigint;
    v_projected bigint;
BEGIN
    IF v_rel IS NULL THEN
        RAISE EXCEPTION 'flashback_measure_external_snapshot_capacity: relation is NULL';
    END IF;
    v_heap := pg_relation_size(v_rel);
    SELECT COALESCE(pg_relation_size(c.reltoastrelid), 0)
      INTO v_toast
    FROM pg_class c
    WHERE c.oid = v_rel;
    v_toast := COALESCE(v_toast, 0);
    v_index := COALESCE(pg_indexes_size(v_rel), 0);
    v_min_free := flashback_local_guc_bytes(
        'pg_flashback.external_snapshot_min_free_bytes'
    );
    v_reserve := flashback_local_guc_bytes(
        'pg_flashback.external_snapshot_safety_reserve_bytes'
    );
    IF v_min_free IS NULL OR v_min_free <= 0
       OR v_reserve IS NULL OR v_reserve <= 0
    THEN
        RAISE EXCEPTION 'pg_flashback: external snapshot capacity GUCs must both be configured above zero'
            USING ERRCODE = 'invalid_parameter_value',
                  HINT = 'Set pg_flashback.external_snapshot_min_free_bytes and pg_flashback.external_snapshot_safety_reserve_bytes, then reload.';
    END IF;
    v_available := public.flashback_external_filesystem_available_bytes();
    -- Artifact compression is never assumed for admission. Heap+TOAST is the
    -- conservative source-footprint proxy; the writer still fails closed on
    -- ENOSPC and never publishes a partial artifact.
    v_projected := v_heap + v_toast;

    rel := v_rel;
    source_heap_bytes := v_heap;
    source_toast_bytes := v_toast;
    source_index_bytes := v_index;
    projected_artifact_bytes := v_projected;
    configured_min_free_bytes := v_min_free;
    configured_safety_reserve_bytes := v_reserve;
    filesystem_available_bytes := v_available;
    admissible := v_available >= v_projected + v_min_free + v_reserve;
    RETURN NEXT;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_admit_external_snapshot_capacity(p_rel regclass)
RETURNS bigint
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    m record;
BEGIN
    SELECT * INTO STRICT m
    FROM public.flashback_measure_external_snapshot_capacity(p_rel);
    IF NOT m.admissible THEN
        RAISE EXCEPTION 'pg_flashback: external snapshot capacity admission failed'
            USING ERRCODE = 'disk_full',
                  DETAIL = format(
                      'available=%s projected=%s min_free=%s reserve=%s',
                      m.filesystem_available_bytes,
                      m.projected_artifact_bytes,
                      m.configured_min_free_bytes,
                      m.configured_safety_reserve_bytes
                  ),
                  HINT = 'Free space on external_snapshot_root or adjust the explicit external capacity budgets.';
    END IF;
    RETURN m.projected_artifact_bytes;
END;
$$;

CREATE OR REPLACE FUNCTION flashback_local_restore_preflight_snapshot(
    p_snapshot_id bigint,
    p_tracking_id bigint
)
RETURNS bigint
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    s flashback.snapshots%ROWTYPE;
    v_peak bigint;
    v_max_peak bigint;
    v_min_fs bigint;
    v_reserve bigint;
    v_available bigint;
    v_payload regclass;
BEGIN
    SELECT * INTO s
    FROM flashback.snapshots
    WHERE snapshot_id = p_snapshot_id AND tracking_id = p_tracking_id;
    IF NOT FOUND OR s.payload_state <> 'available' THEN
        RAISE EXCEPTION 'pg_flashback: restore boundary snapshot % is not available',
            p_snapshot_id USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    IF s.storage_backend = 'heap_v1' THEN
        SELECT r.payload_relid INTO v_payload
        FROM public.flashback_internal_snapshot_resolve(
            p_snapshot_id, p_tracking_id
        ) r;
        RETURN public.flashback_local_restore_preflight(v_payload);
    END IF;
    IF s.storage_backend <> 'external_zstd'
       OR s.external_uncompressed_bytes IS NULL
       OR s.external_uncompressed_bytes <= 0
    THEN
        RAISE EXCEPTION 'pg_flashback: snapshot % has no trustworthy restore-size evidence',
            p_snapshot_id USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    v_max_peak := COALESCE(
        flashback_local_guc_bytes('pg_flashback.local_max_restore_peak_bytes'),
        flashback_local_guc_bytes('pg_flashback.local_restore_max_peak_bytes')
    );
    v_min_fs := flashback_local_guc_bytes('pg_flashback.local_min_filesystem_bytes');
    v_reserve := COALESCE(
        flashback_local_guc_bytes('pg_flashback.local_safety_reserve_bytes'),
        flashback_local_guc_bytes('pg_flashback.local_restore_safety_reserve_bytes'),
        pg_size_bytes('64MB')
    );
    IF v_max_peak IS NULL OR v_max_peak <= 0 OR v_min_fs IS NULL OR v_min_fs <= 0 THEN
        RAISE EXCEPTION 'pg_flashback: local restore capacity GUCs must be configured above zero';
    END IF;
    -- Shadow + live replacement/successor allowance. Index sizes cannot be
    -- observed after DROP, so use a deliberately conservative 3x logical
    -- artifact size envelope plus the configured reserve.
    v_peak := (s.external_uncompressed_bytes * 3) + v_reserve;
    v_available := public.flashback_tablespace_filesystem_available_bytes(0::oid);
    IF v_peak > v_max_peak OR v_available < v_peak + v_min_fs THEN
        RAISE EXCEPTION 'pg_flashback: external-boundary restore capacity admission failed'
            USING ERRCODE = 'disk_full',
                  DETAIL = format(
                      'projected_peak=%s max_peak=%s available=%s min_free=%s',
                      v_peak, v_max_peak, v_available, v_min_fs
                  );
    END IF;
    RETURN v_peak;
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
        -- ACCESS EXCLUSIVE hold covers shadow heap/TOAST copy plus successor
        -- base CTAS (about two heap+TOAST copies). lock_timeout only bounds
        -- wait-to-acquire; this compares estimated hold duration to the stall budget.
        IF (2 * m.estimated_copy_ms) > m.configured_write_stall_ms THEN
            v_reasons := v_reasons || ARRAY[format(
                'estimated restore hold %s ms exceeds local_boundary_write_stall_ms=%s',
                (2 * m.estimated_copy_ms), m.configured_write_stall_ms
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
                      HINT = 'Raise the configured local budgets, free filesystem space, or set pg_flashback.local_capacity_override only as an explicit privileged escape hatch.';
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
       OR (2 * m.estimated_copy_ms) > m.configured_write_stall_ms
    THEN
        v_rec := 'reject local profile; raise configured budgets or free filesystem space';
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
COMMENT ON FUNCTION flashback_measure_external_snapshot_capacity(regclass) IS
    'Read-only external_zstd persist estimate using the configured artifact filesystem and explicit free-space reserves.';
COMMENT ON FUNCTION flashback_admit_external_snapshot_capacity(regclass) IS
    'Fail-closed external_zstd snapshot persist admission; compression savings are never assumed.';
COMMENT ON FUNCTION flashback_local_restore_preflight_snapshot(bigint, bigint) IS
    'Backend-neutral restore capacity admission for a retained SnapshotStore boundary, including DROP recovery from external_zstd.';

-- =================================================================
-- Read-only cluster/database config recommendation for local_delta.
-- Never mutates postgresql.conf. Operators copy/apply lines explicitly.
-- =================================================================
CREATE OR REPLACE FUNCTION flashback_config_recommend(
    p_rel regclass DEFAULT NULL,
    p_target_database text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_db text := COALESCE(NULLIF(p_target_database, ''), current_database());
    v_fs bigint;
    v_heap bigint := 0;
    v_toast bigint := 0;
    v_base bigint;
    v_peak bigint;
    v_snap_budget text;
    v_peak_budget text;
    v_min_fs text;
    v_safety text;
    v_lines text[] := ARRAY[]::text[];
    v_notes text[] := ARRAY[]::text[];
    v_missing text[] := ARRAY[]::text[];
    v_cur_snap text;
    v_cur_peak text;
    v_cur_min text;
    v_workers integer;
    v_max_workers integer;
    v_slots integer;
    v_senders integer;
    v_slot_keep text;
    v_targets text;
    v_output_plugin_applicable boolean;
BEGIN
    -- Filesystem free space for the default tablespace (cluster-local estimate).
    SELECT flashback_tablespace_filesystem_available_bytes(0::oid) INTO v_fs;
    IF v_fs IS NULL OR v_fs < 0 THEN
        v_fs := 0;
    END IF;

    IF p_rel IS NOT NULL THEN
        SELECT COALESCE(pg_relation_size(p_rel), 0),
               COALESCE(pg_total_relation_size(p_rel) - pg_relation_size(p_rel), 0)
          INTO v_heap, v_toast;
        -- Index bytes are not part of base CTAS, but restore rebuilds them.
        v_base := v_heap + GREATEST(v_toast, 0);
        v_peak := (2 * v_base) + GREATEST(pg_indexes_size(p_rel), 0);
    ELSE
        -- Conservative defaults when no sample table is provided: assume a
        -- few hundred MiB ordinary table envelope for first-time setup.
        v_base := 512 * 1024 * 1024;
        v_peak := 2 * v_base;
        v_notes := v_notes || ARRAY[
            'no sample table provided; budgets use a conservative 512MiB base / 1GiB peak envelope — re-run with a regclass for tighter advice'
        ];
    END IF;

    -- Round budgets up to nice pg_size_bytes strings with headroom.
    v_snap_budget := pg_size_pretty(GREATEST(v_base * 2, 256 * 1024 * 1024));
    v_peak_budget := pg_size_pretty(GREATEST(v_peak * 2, 512 * 1024 * 1024));
    v_min_fs := pg_size_pretty(GREATEST((v_fs / 10), 1024 * 1024 * 1024)); -- ~10% free or 1GiB
    IF v_fs > 0 AND (v_fs / 10) < (1024 * 1024 * 1024) THEN
        v_min_fs := pg_size_pretty(GREATEST(v_fs / 10, 256 * 1024 * 1024));
    END IF;
    v_safety := '64MB';

    v_cur_snap := NULLIF(current_setting('pg_flashback.local_max_snapshot_bytes', true), '');
    v_cur_peak := NULLIF(current_setting('pg_flashback.local_max_restore_peak_bytes', true), '');
    v_cur_min := NULLIF(current_setting('pg_flashback.local_min_filesystem_bytes', true), '');
    IF v_cur_snap IS NULL THEN
        v_missing := v_missing || ARRAY['pg_flashback.local_max_snapshot_bytes'];
    END IF;
    IF v_cur_peak IS NULL THEN
        v_missing := v_missing || ARRAY['pg_flashback.local_max_restore_peak_bytes'];
    END IF;
    IF v_cur_min IS NULL THEN
        v_missing := v_missing || ARRAY['pg_flashback.local_min_filesystem_bytes'];
    END IF;

    v_targets := COALESCE(
        NULLIF(current_setting('pg_flashback.target_databases', true), ''),
        NULLIF(current_setting('pg_flashback.target_database', true), ''),
        v_db
    );
    v_workers := COALESCE(NULLIF(current_setting('pg_flashback.max_workers', true), '')::integer, 4);
    v_max_workers := current_setting('max_worker_processes')::integer;
    v_slots := current_setting('max_replication_slots')::integer;
    v_senders := current_setting('max_wal_senders')::integer;
    v_slot_keep := current_setting('max_slot_wal_keep_size', true);

    -- output_plugin_libraries only exists on PostgreSQL minors that carry the
    -- GUC (current security-patched minors of every supported major); missing
    -- on older minors is not-applicable, never a false recommendation.
    v_output_plugin_applicable := current_setting('output_plugin_libraries', true) IS NOT NULL;

    v_lines := ARRAY[
        format('shared_preload_libraries = ''pg_flashback'''),
        format('wal_level = logical'),
        format('max_worker_processes = %s', GREATEST(v_max_workers, v_workers * 2 + 4)),
        format('max_replication_slots = %s', GREATEST(v_slots, 8)),
        format('max_wal_senders = %s', GREATEST(v_senders, 8)),
        format('pg_flashback.enabled = on'),
        format('pg_flashback.capture_mode = wal'),
        format('pg_flashback.target_databases = ''%s''', replace(v_targets, '''', '''''')),
        format('pg_flashback.max_workers = %s', v_workers),
        format('pg_flashback.local_max_snapshot_bytes = ''%s''', v_snap_budget),
        format('pg_flashback.local_max_restore_peak_bytes = ''%s''', v_peak_budget),
        format('pg_flashback.local_min_filesystem_bytes = ''%s''', v_min_fs),
        format('pg_flashback.local_safety_reserve_bytes = ''%s''', v_safety)
    ];

    IF v_output_plugin_applicable THEN
        v_lines := v_lines || ARRAY[format('output_plugin_libraries = ''pg_flashback''')];
    END IF;

    IF v_slot_keep IS NULL OR v_slot_keep IN ('-1', '') THEN
        v_notes := v_notes || ARRAY[
            'max_slot_wal_keep_size is unlimited (-1); prefer a finite cap so slot loss + open coverage gap fails closed before the filesystem fills'
        ];
        v_lines := v_lines || ARRAY['max_slot_wal_keep_size = ''4GB''  # suggested finite cap; tune to disk'];
    END IF;

    IF v_missing <> ARRAY[]::text[] THEN
        v_notes := v_notes || ARRAY[
            'capacity GUCs are unset; protect/restore fail closed until they are configured and PostgreSQL is restarted (postmaster GUCs) or reloaded where Userset applies'
        ];
    END IF;

    RETURN jsonb_build_object(
        'schema_version', 1,
        'profile', 'local_delta',
        'database', v_db,
        'sample_relation', p_rel::text,
        'filesystem_available_bytes', v_fs,
        'projected_base_snapshot_bytes', v_base,
        'projected_restore_peak_bytes', v_peak,
        'missing_capacity_gucs', to_jsonb(v_missing),
        'current', jsonb_build_object(
            'local_max_snapshot_bytes', v_cur_snap,
            'local_max_restore_peak_bytes', v_cur_peak,
            'local_min_filesystem_bytes', v_cur_min,
            'target_databases', v_targets,
            'max_worker_processes', v_max_workers,
            'max_replication_slots', v_slots,
            'max_wal_senders', v_senders,
            'max_slot_wal_keep_size', v_slot_keep
        ),
        'recommended_postgresql_conf_lines', to_jsonb(v_lines),
        'apply', jsonb_build_object(
            'automatic', false,
            'privileged_opt_in', false,
            'note', 'pg_flashback never edits postgresql.conf silently; copy the lines below, then restart PostgreSQL because shared_preload_libraries / wal_level / worker and slot limits require a postmaster restart'
        ),
        'restart_required', true,
        'reload_sufficient', false,
        'notes', to_jsonb(v_notes),
        'operator_role_sql', format($sql$
-- After CREATE EXTENSION, grant a login operator (not superuser) the admin API:
CREATE ROLE pgfb_operator LOGIN PASSWORD '...';  -- use your secret management
GRANT flashback_admin TO pgfb_operator;
-- Connect as pgfb_operator for protect/status/recover/unprotect/cleanup.
$sql$)
    );
END;
$$;

COMMENT ON FUNCTION flashback_config_recommend(regclass, text) IS
    'Read-only local_delta configuration recommendation. Returns postgresql.conf lines and operator-role SQL; never mutates cluster configuration.';
