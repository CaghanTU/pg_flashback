-- =================================================================
-- WAL coverage runtime: stream epochs, target admission, timestamp
-- resolution and health projection.
-- =================================================================

CREATE OR REPLACE FUNCTION flashback_current_timeline_id()
RETURNS bigint
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
    SELECT timeline_id::bigint FROM pg_control_checkpoint();
$$;

CREATE OR REPLACE FUNCTION flashback_mark_capture_stream_broken(
    p_stream_id bigint,
    p_reason text,
    p_details jsonb DEFAULT '{}'::jsonb
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback
AS $$
DECLARE
    v_discarded_pending bigint := 0;
    v_discarded_building bigint := 0;
    v_database_oid oid;
    v_payload_oid regclass;
    build_rec record;
    lock_rec record;
BEGIN
    IF p_reason IS NULL OR btrim(p_reason) = '' THEN
        RAISE EXCEPTION 'flashback_mark_capture_stream_broken: reason is required';
    END IF;

    -- Take the database-stream lock here as well as at the normal callers.
    -- This primitive is deliberately callable by internal maintenance paths;
    -- direct invocation must not bypass the outer serialization contract.
    SELECT database_oid INTO v_database_oid
    FROM flashback.capture_streams
    WHERE stream_id = p_stream_id;
    IF v_database_oid IS NOT NULL THEN
        PERFORM pg_advisory_xact_lock(358945::integer, v_database_oid::integer);
    END IF;

    -- Pin affected lifecycles in stable-ID order before freezing watermarks so
    -- a restore, query or retention transition cannot cross the break record.
    FOR lock_rec IN
        SELECT cg.tracking_id
        FROM flashback.coverage_generations cg
        WHERE cg.stream_id = p_stream_id
          AND cg.state IN ('building', 'active')
        ORDER BY cg.tracking_id
    LOOP
        PERFORM pg_advisory_xact_lock(358944::integer,
                                      hashint8(lock_rec.tracking_id));
    END LOOP;

    UPDATE flashback.capture_streams
       SET state = 'broken',
           invalidation_reason = p_reason,
           invalidated_at = clock_timestamp(),
           details = details || COALESCE(p_details, '{}'::jsonb)
     WHERE stream_id = p_stream_id
       AND state = 'active';

    IF NOT FOUND THEN
        RETURN;
    END IF;

    -- These rows depended on COMMIT records from the broken slot epoch. They
    -- can never be promoted safely after a discontinuity; the durable gap is
    -- the audit record, while retaining full DDL snapshots here would leak
    -- storage forever.
    DELETE FROM flashback.pending_wal_events
    WHERE stream_id = p_stream_id;
    GET DIAGNOSTICS v_discarded_pending = ROW_COUNT;
    IF v_discarded_pending > 0 THEN
        UPDATE flashback.capture_streams
           SET details = details || jsonb_build_object(
               'discarded_pending_wal_events', v_discarded_pending
           )
         WHERE stream_id = p_stream_id;
    END IF;

    -- A committed post-restore/re-anchor draft has no canonical boundary yet.
    -- Keeping it as `building` after its stream is broken would block every
    -- future re-anchor forever (`one building generation`) and retain a
    -- snapshot that can no longer be proved. Remove only its physical payload,
    -- then preserve the generation as an immutable `aborted` audit tombstone.
    -- The parent remains active with the open gap inserted below. The table
    -- itself is left at its current physical OID (a restore may already have
    -- swapped it); the next explicit re-anchor establishes a fresh exact base.
    FOR build_rec IN
        SELECT cg.generation_id, cg.tracking_id, cg.boundary_snapshot_id,
               snap.snapshot_table
        FROM flashback.coverage_generations cg
        LEFT JOIN flashback.snapshots snap
          ON snap.snapshot_id = cg.boundary_snapshot_id
         AND snap.tracking_id = cg.tracking_id
        WHERE cg.stream_id = p_stream_id
          AND cg.state = 'building'
        ORDER BY cg.tracking_id, cg.generation_id
    LOOP
        v_payload_oid := to_regclass(build_rec.snapshot_table);
        IF v_payload_oid IS NOT NULL THEN
            IF public.flashback_payload_kind(v_payload_oid) IS NULL THEN
                RAISE EXCEPTION
                    'pg_flashback: pending generation % references an unrecognized payload relation %',
                    build_rec.generation_id, build_rec.snapshot_table
                    USING HINT = 'Repair the pending snapshot metadata; the stream break was rolled back fail-closed.';
            END IF;
            PERFORM public.flashback_drop_payload_table(v_payload_oid);
        END IF;

        DELETE FROM flashback.schema_versions
        WHERE generation_id = build_rec.generation_id
          AND tracking_id = build_rec.tracking_id;

        DELETE FROM flashback.delta_log
        WHERE generation_id = build_rec.generation_id
          AND tracking_id = build_rec.tracking_id;

        UPDATE flashback.snapshots
           SET payload_state = 'missing',
               retired_at = COALESCE(retired_at, clock_timestamp())
         WHERE snapshot_id = build_rec.boundary_snapshot_id
           AND tracking_id = build_rec.tracking_id
           AND payload_state = 'available';

        -- Avoid leaving a dangling current-binding pointer after a failed
        -- post-restore boundary.  Re-anchor will replace it atomically.
        UPDATE flashback.tracked_tables
           SET base_snapshot_table = NULL
         WHERE tracking_id = build_rec.tracking_id
           AND base_snapshot_table = build_rec.snapshot_table;

        UPDATE flashback.coverage_generations
           SET state = 'aborted',
               aborted_at = clock_timestamp(),
               state_reason = COALESCE(state_reason, p_reason),
               details = details || jsonb_build_object(
                   'aborted_reason', p_reason,
                   'aborted_stream_id', p_stream_id
               )
         WHERE generation_id = build_rec.generation_id
           AND tracking_id = build_rec.tracking_id
           AND state = 'building';
        v_discarded_building := v_discarded_building + 1;
    END LOOP;

    IF v_discarded_building > 0 THEN
        UPDATE flashback.capture_streams
           SET details = details || jsonb_build_object(
               'discarded_building_generations', v_discarded_building
           )
         WHERE stream_id = p_stream_id;
    END IF;

    INSERT INTO flashback.coverage_gaps (
        tracking_id, source_generation_id, reason,
        gap_start_time, gap_start_lsn, lower_bound_inclusive, details
    )
    SELECT
        cg.tracking_id, cg.generation_id, p_reason,
        cg.valid_through_time, cg.valid_through_lsn, false,
        jsonb_build_object('stream_id', p_stream_id) || COALESCE(p_details, '{}'::jsonb)
    FROM flashback.coverage_generations cg
    WHERE cg.stream_id = p_stream_id
      AND cg.state = 'active'
      AND NOT EXISTS (
          SELECT 1
          FROM flashback.coverage_gaps gap
          WHERE gap.tracking_id = cg.tracking_id
            AND gap.source_generation_id = cg.generation_id
            AND gap.reanchored_by_generation_id IS NULL
      );
END;
$$;

-- Apply a changed worker configuration only after its discontinuity is
-- durable. A SIGHUP/session GUC cannot write catalog state from a GUC assign
-- hook, so the worker calls this in a LOGGED transaction before acting on the
-- new enabled/mode value. WAL produced in the short detection interval stays
-- in the slot; the conservative gap starts at the previous proven watermark.
CREATE OR REPLACE FUNCTION flashback_reconcile_capture_configuration()
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_database_oid oid;
    v_stream flashback.capture_streams%ROWTYPE;
    v_enabled boolean;
    v_mode text;
    v_reason text;
BEGIN
    IF to_regclass('flashback.capture_streams') IS NULL THEN
        RETURN 'extension_not_ready';
    END IF;

    v_database_oid := (SELECT oid FROM pg_database WHERE datname = current_database());
    v_enabled := COALESCE(current_setting('pg_flashback.enabled', true), 'on') <> 'off';
    v_mode := flashback_effective_capture_mode();

    PERFORM pg_advisory_xact_lock(358945::integer, v_database_oid::integer);
    SELECT * INTO v_stream
    FROM flashback.capture_streams
    WHERE database_oid = v_database_oid
      AND state = 'active'
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN CASE
            WHEN NOT v_enabled THEN 'disabled'
            WHEN v_mode <> 'wal' THEN 'unqualified_mode'
            ELSE 'no_active_stream'
        END;
    END IF;

    v_reason := CASE
        WHEN NOT v_enabled THEN 'capture_disabled'
        WHEN v_mode <> 'wal' THEN 'capture_mode_changed'
        ELSE NULL
    END;
    IF v_reason IS NULL THEN
        RETURN 'active';
    END IF;

    PERFORM flashback_mark_capture_stream_broken(
        v_stream.stream_id,
        v_reason,
        jsonb_build_object(
            'configured_enabled', v_enabled,
            'configured_effective_mode', v_mode,
            'configured_capture_mode', COALESCE(
                current_setting('pg_flashback.capture_mode', true), 'auto'
            )
        )
    );
    RETURN v_reason;
END;
$$;

-- Capture hooks run in the user's backend, while the worker observes only the
-- postmaster/SIGHUP GUC values.  `pg_flashback.enabled` and
-- `pg_flashback.capture_mode` are SUSET for compatibility, which means a
-- session can otherwise use SET LOCAL to bypass the worker's reconciliation
-- loop for one transaction.  Every hook calls this guard before recording a
-- row.  A session-local change is therefore converted into the same durable
-- stream break as a worker-observed change, and the affected event is refused
-- once the break is committed with the user's transaction.
CREATE OR REPLACE FUNCTION flashback_capture_configuration_guard(
    p_rel_oid oid DEFAULT NULL
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_enabled boolean;
    v_mode text;
    v_has_qualified boolean;
    v_stream_state text;
    v_generation_state text;
BEGIN
    IF to_regclass('flashback.capture_streams') IS NULL
       OR to_regclass('flashback.coverage_generations') IS NULL
    THEN
        RETURN true;
    END IF;

    v_enabled := COALESCE(current_setting('pg_flashback.enabled', true), 'on') <> 'off';
    v_mode := flashback_effective_capture_mode();

    SELECT EXISTS (
        SELECT 1
        FROM flashback.tracked_tables tt
        JOIN flashback.coverage_generations cg
          ON cg.tracking_id = tt.tracking_id
         AND cg.state IN ('building', 'active', 'sealed')
        WHERE tt.is_active
          AND tt.recovery_profile = 'local_delta'
          AND (p_rel_oid IS NULL OR tt.rel_oid = p_rel_oid)
    ) INTO v_has_qualified;

    -- Legacy trigger lifecycles have no correctness-qualified generation. They
    -- retain their historical enabled switch semantics; the strict epoch
    -- protocol applies only once a lifecycle has a WAL generation.
    IF NOT v_has_qualified THEN
        RETURN v_enabled;
    END IF;

    IF NOT v_enabled OR v_mode IS DISTINCT FROM 'wal' THEN
        -- This is a LOGGED write and takes the database-stream/lifecycle locks
        -- in the canonical order.  If this transaction aborts, the user DML
        -- also aborts, so no false gap can survive an aborted change.
        PERFORM flashback_reconcile_capture_configuration();
    END IF;

    SELECT cs.state, cg.state
      INTO v_stream_state, v_generation_state
    FROM flashback.tracked_tables tt
    JOIN flashback.coverage_generations cg
      ON cg.tracking_id = tt.tracking_id
     AND cg.state IN ('building', 'active', 'sealed')
    JOIN flashback.capture_streams cs ON cs.stream_id = cg.stream_id
    WHERE tt.is_active
      AND tt.recovery_profile = 'local_delta'
      AND (p_rel_oid IS NULL OR tt.rel_oid = p_rel_oid)
    ORDER BY CASE cg.state WHEN 'active' THEN 0 WHEN 'building' THEN 1 ELSE 2 END,
             cg.generation_no DESC
    LIMIT 1;

    RETURN v_enabled
       AND v_mode = 'wal'
       AND v_generation_state = 'active'
       AND v_stream_state = 'active';
END;
$$;

-- Return the active WAL stream after proving that the slot has not vanished,
-- changed identity or advanced outside pg_flashback.  A discontinuity is
-- committed as a broken epoch plus durable gaps; a fresh epoch may then be
-- used only by newly anchored generations.
CREATE OR REPLACE FUNCTION flashback_ensure_active_wal_stream()
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_stream flashback.capture_streams%ROWTYPE;
    v_slot record;
    v_timeline bigint;
    v_epoch bigint;
    v_stream_id bigint;
    v_reason text;
BEGIN
    IF COALESCE(current_setting('pg_flashback.enabled', true), 'on') = 'off' THEN
        PERFORM flashback_reconcile_capture_configuration();
        RETURN NULL;
    END IF;
    IF current_setting('wal_level') IS DISTINCT FROM 'logical' THEN
        RAISE EXCEPTION 'pg_flashback: wal_level must be logical for release-qualified local tracking'
            USING HINT = 'Set wal_level=logical in postgresql.conf and restart PostgreSQL.';
    END IF;
    IF flashback_effective_capture_mode() IS DISTINCT FROM 'wal' THEN
        PERFORM flashback_reconcile_capture_configuration();
        RETURN NULL;
    END IF;

    PERFORM pg_advisory_xact_lock(358945::integer, (SELECT oid::integer FROM pg_database WHERE datname = current_database()));
    v_timeline := flashback_current_timeline_id();

    SELECT slot_name, plugin, database, restart_lsn, confirmed_flush_lsn
      INTO v_slot
    FROM pg_replication_slots
    WHERE slot_name = flashback_effective_slot_name();

    IF NOT FOUND THEN
        SELECT * INTO v_stream
        FROM flashback.capture_streams
        WHERE database_oid = (SELECT oid FROM pg_database WHERE datname = current_database())
          AND state = 'active'
        FOR UPDATE;
        IF FOUND THEN
            PERFORM flashback_mark_capture_stream_broken(
                v_stream.stream_id,
                'replication_slot_missing',
                jsonb_build_object('slot_name', flashback_effective_slot_name())
            );
        END IF;
        RETURN NULL;
    END IF;

    IF v_slot.database IS DISTINCT FROM current_database()
       OR v_slot.plugin IS DISTINCT FROM 'pg_flashback'
    THEN
        RAISE EXCEPTION 'pg_flashback: slot % has database/plugin %/%, expected %/pg_flashback',
            v_slot.slot_name, v_slot.database, v_slot.plugin, current_database();
    END IF;

    SELECT * INTO v_stream
    FROM flashback.capture_streams
    WHERE database_oid = (SELECT oid FROM pg_database WHERE datname = current_database())
      AND state = 'active'
    FOR UPDATE;

    IF FOUND THEN
        v_reason := CASE
            WHEN v_stream.capture_mode IS DISTINCT FROM 'wal' THEN 'capture_mode_changed'
            WHEN v_stream.slot_name IS DISTINCT FROM v_slot.slot_name THEN 'replication_slot_changed'
            WHEN v_stream.plugin_name IS DISTINCT FROM v_slot.plugin THEN 'output_plugin_changed'
            WHEN v_stream.timeline_id IS DISTINCT FROM v_timeline THEN 'timeline_changed'
            WHEN v_stream.confirmed_flush_lsn IS DISTINCT FROM v_slot.confirmed_flush_lsn
                 AND v_slot.confirmed_flush_lsn >= v_stream.confirmed_flush_lsn
                 AND v_slot.confirmed_flush_lsn <= NULLIF(
                        v_stream.details->>'safe_slot_advance_upto_lsn', ''
                     )::pg_lsn
                THEN NULL
            WHEN v_stream.confirmed_flush_lsn IS DISTINCT FROM v_slot.confirmed_flush_lsn
                THEN 'replication_slot_advanced_externally'
            ELSE NULL
        END;

        IF v_reason IS NULL THEN
            RETURN v_stream.stream_id;
        END IF;

        PERFORM flashback_mark_capture_stream_broken(
            v_stream.stream_id,
            v_reason,
            jsonb_build_object(
                'catalog_confirmed_flush_lsn', v_slot.confirmed_flush_lsn,
                'recorded_confirmed_flush_lsn', v_stream.confirmed_flush_lsn,
                'catalog_restart_lsn', v_slot.restart_lsn,
                'timeline_id', v_timeline
            )
        );
    END IF;

    SELECT COALESCE(max(epoch_no), 0) + 1 INTO v_epoch
    FROM flashback.capture_streams
    WHERE database_oid = (SELECT oid FROM pg_database WHERE datname = current_database());

    INSERT INTO flashback.capture_streams (
        database_oid, database_name, epoch_no, capture_mode, timeline_id,
        slot_name, plugin_name, state, valid_through_lsn,
        confirmed_flush_lsn, restart_lsn, activated_at, details
    ) VALUES (
        (SELECT oid FROM pg_database WHERE datname = current_database()),
        current_database(), v_epoch, 'wal', v_timeline,
        v_slot.slot_name, v_slot.plugin, 'active', v_slot.confirmed_flush_lsn,
        v_slot.confirmed_flush_lsn, v_slot.restart_lsn, clock_timestamp(),
        jsonb_build_object('initial_confirmed_flush_lsn', v_slot.confirmed_flush_lsn)
    )
    RETURNING stream_id INTO v_stream_id;

    RETURN v_stream_id;
END;
$$;

-- Establish a new exact local base after a stream discontinuity (or as an
-- explicit maintenance boundary).  The successor remains non-eligible until
-- the worker observes this transaction's real COMMIT LSN.  A cross-stream
-- handoff preserves the predecessor's frozen watermark and permanently closes
-- the intervening gap only at the new boundary.
CREATE OR REPLACE FUNCTION flashback_reanchor(p_target_table text)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_tracking_id bigint;
    v_rel_oid oid;
    v_schema_name text;
    v_table_name text;
    v_parent_generation_id bigint;
    v_stream_id bigint;
    v_generation_id bigint;
    v_generation_no bigint;
    v_snapshot_id bigint;
    v_snapshot_name text;
    v_boundary_xid bigint;
    v_provisional_lsn pg_lsn;
    v_schema_version bigint;
    v_schema_def jsonb;
    v_row_count bigint;
BEGIN
    IF flashback_effective_capture_mode() IS DISTINCT FROM 'wal' THEN
        RAISE EXCEPTION 'pg_flashback: local re-anchor requires WAL capture';
    END IF;
    IF txid_current_if_assigned() IS NOT NULL THEN
        RAISE EXCEPTION 'pg_flashback: flashback_reanchor() must run before any write in a dedicated transaction'
            USING HINT = 'COMMIT or ROLLBACK, then call flashback_reanchor() as the first write in a new READ COMMITTED transaction.';
    END IF;
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'pg_flashback: flashback_reanchor() requires READ COMMITTED isolation for a fresh post-lock snapshot';
    END IF;

    SELECT tt.tracking_id, tt.rel_oid, tt.schema_name, tt.table_name
      INTO v_tracking_id, v_rel_oid, v_schema_name, v_table_name
    FROM flashback.tracked_tables tt
    WHERE tt.is_active
      AND tt.recovery_profile = 'local_delta'
      AND (
          tt.rel_oid = to_regclass(p_target_table)::oid
          OR format('%I.%I', tt.schema_name, tt.table_name) = p_target_table
          OR (position('.' IN p_target_table) = 0 AND tt.table_name = p_target_table)
      )
    ORDER BY (tt.rel_oid = to_regclass(p_target_table)::oid) DESC
    LIMIT 1;
    IF v_tracking_id IS NULL THEN
        RAISE EXCEPTION 'flashback_reanchor: table % is not actively tracked with local_delta',
            p_target_table;
    END IF;

    -- Database stream serialization is the outermost runtime lock, matching
    -- worker/restore ordering.  Slot recreation is never attached to an old
    -- generation; ensure_active creates a fresh stream epoch first.
    v_stream_id := flashback_ensure_active_wal_stream();
    IF v_stream_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: no usable logical slot exists for re-anchor'
            USING HINT = 'Recreate the pg_flashback logical slot, then retry flashback_reanchor() in a new transaction.';
    END IF;

    PERFORM pg_advisory_xact_lock(358944::integer, hashint8(v_tracking_id));

    SELECT tt.rel_oid, tt.schema_name, tt.table_name
      INTO v_rel_oid, v_schema_name, v_table_name
    FROM flashback.tracked_tables tt
    WHERE tt.tracking_id = v_tracking_id
      AND tt.is_active
      AND tt.recovery_profile = 'local_delta'
    FOR UPDATE;
    IF NOT FOUND OR to_regclass(format('%I.%I', v_schema_name, v_table_name))::oid
                       IS DISTINCT FROM v_rel_oid
    THEN
        RAISE EXCEPTION 'pg_flashback: tracked table identity changed before re-anchor';
    END IF;

    IF EXISTS (
        SELECT 1 FROM flashback.coverage_generations
        WHERE tracking_id = v_tracking_id AND state = 'building'
    ) THEN
        RAISE EXCEPTION 'pg_flashback: lifecycle % already has a pending generation',
            v_tracking_id;
    END IF;

    SELECT generation_id INTO v_parent_generation_id
    FROM flashback.coverage_generations
    WHERE tracking_id = v_tracking_id
      AND state = 'active'
    FOR UPDATE;
    IF v_parent_generation_id IS NULL THEN
        -- Initial tracking can lose its stream before the worker observes its
        -- boundary COMMIT. The failed draft is retained as `aborted`; use the
        -- latest tombstone as lineage while establishing the first eligible
        -- generation instead of leaving the lifecycle permanently stuck.
        SELECT generation_id INTO v_parent_generation_id
        FROM flashback.coverage_generations
        WHERE tracking_id = v_tracking_id
          AND state = 'aborted'
        ORDER BY generation_no DESC
        LIMIT 1
        FOR UPDATE;
        IF v_parent_generation_id IS NULL THEN
            RAISE EXCEPTION 'pg_flashback: lifecycle % has no active or aborted predecessor to re-anchor',
                v_tracking_id;
        END IF;
    END IF;

    -- ACCESS EXCLUSIVE is intentionally taken from the outset: it blocks all
    -- writes while the exact base is scanned and avoids a later lock upgrade.
    PERFORM flashback_admit_local_capacity(v_rel_oid, 'reanchor');
    PERFORM flashback_apply_local_boundary_lock_timeout();
    BEGIN
        EXECUTE format('LOCK TABLE %I.%I IN ACCESS EXCLUSIVE MODE',
                       v_schema_name, v_table_name);
    EXCEPTION WHEN lock_not_available THEN
        RAISE EXCEPTION 'pg_flashback: local re-anchor lock wait exceeded local_boundary_write_stall_ms'
            USING ERRCODE = 'lock_not_available',
                  HINT = 'Retry when the table is idle, raise the write-stall budget, or use the backup profile.';
    END;

    IF to_regclass(format('%I.%I', v_schema_name, v_table_name))::oid
           IS DISTINCT FROM v_rel_oid
    THEN
        RAISE EXCEPTION 'pg_flashback: table identity changed while acquiring the re-anchor lock';
    END IF;

    -- Revalidate identity and capacity under the locked boundary before CTAS.
    SELECT tt.rel_oid, tt.schema_name, tt.table_name
      INTO v_rel_oid, v_schema_name, v_table_name
    FROM flashback.tracked_tables tt
    WHERE tt.tracking_id = v_tracking_id
      AND tt.is_active
      AND tt.recovery_profile = 'local_delta';
    IF NOT FOUND OR to_regclass(format('%I.%I', v_schema_name, v_table_name))::oid
                       IS DISTINCT FROM v_rel_oid
    THEN
        RAISE EXCEPTION 'pg_flashback: tracked table identity changed under the re-anchor lock';
    END IF;
    PERFORM flashback_admit_local_capacity(v_rel_oid, 'reanchor');

    -- Preserve full old-row WAL images for the successor generation.
    EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY FULL',
                   v_schema_name, v_table_name);

    v_boundary_xid := (txid_current() % 4294967296)::bigint;
    SELECT COALESCE(max(generation_no), 0) + 1
      INTO v_generation_no
    FROM flashback.coverage_generations
    WHERE tracking_id = v_tracking_id;
    v_provisional_lsn := pg_current_wal_insert_lsn();
    v_schema_def := COALESCE(flashback_collect_schema_def(v_rel_oid), '{}'::jsonb);

    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn,
        schema_def, row_count, captured_at
    ) VALUES (
        v_rel_oid, v_tracking_id, '', v_provisional_lsn,
        v_schema_def, 0, clock_timestamp()
    ) RETURNING snapshot_id INTO v_snapshot_id;

    -- Use the established reserved payload namespace so ownership validation
    -- remains deny-by-default instead of widening its name allowlist.
    v_snapshot_name := format('snap_%s_%s', v_tracking_id, v_snapshot_id);
    EXECUTE format('CREATE TABLE flashback.%I AS TABLE %I.%I',
                   v_snapshot_name, v_schema_name, v_table_name);
    PERFORM public.flashback_own_payload_table(
        to_regclass(format('flashback.%I', v_snapshot_name))
    );
    EXECUTE format('SELECT count(*) FROM flashback.%I', v_snapshot_name)
      INTO v_row_count;
    UPDATE flashback.snapshots
       SET snapshot_table = format('flashback.%I', v_snapshot_name),
           row_count = v_row_count
     WHERE snapshot_id = v_snapshot_id;

    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, parent_generation_id, stream_id,
        recovery_profile, state, boundary_kind, rel_oid_at_boundary,
        boundary_snapshot_id, boundary_xid, boundary_marker, details
    ) VALUES (
        v_tracking_id, v_generation_no, v_parent_generation_id, v_stream_id,
        'local_delta', 'building', 'maintenance_reanchor', v_rel_oid,
        v_snapshot_id, v_boundary_xid,
        format('reanchor:%s:%s:%s',
               v_tracking_id, v_generation_no, v_boundary_xid),
        jsonb_build_object('provisional_snapshot_lsn', v_provisional_lsn)
    ) RETURNING generation_id INTO v_generation_id;

    SELECT COALESCE(max(schema_version), 0) + 1 INTO v_schema_version
    FROM flashback.schema_versions
    WHERE tracking_id = v_tracking_id;

    INSERT INTO flashback.schema_versions (
        rel_oid, tracking_id, generation_id, stream_id, source_xid,
        schema_version, applied_at, applied_lsn,
        columns, primary_key, constraints, helper_schema_sha256
    ) VALUES (
        v_rel_oid, v_tracking_id, v_generation_id, v_stream_id, v_boundary_xid,
        v_schema_version, clock_timestamp(), v_provisional_lsn,
        COALESCE(v_schema_def->'columns', '[]'::jsonb),
        COALESCE(v_schema_def->'primary_key', '[]'::jsonb),
        jsonb_build_object(
            'check_unique_fk', COALESCE(v_schema_def->'constraints', '[]'::jsonb),
            'indexes', COALESCE(v_schema_def->'indexes', '[]'::jsonb),
            'partition_by', v_schema_def->'partition_by',
            'partitions', v_schema_def->'partitions',
            'triggers', COALESCE(v_schema_def->'triggers', '[]'::jsonb),
            'rls_policies', COALESCE(v_schema_def->'rls_policies', '[]'::jsonb),
            'rls_enabled', COALESCE((v_schema_def->'rls_enabled')::boolean, false)
        ),
        flashback_helper_schema_sha256(v_rel_oid)
    );

    UPDATE flashback.tracked_tables
       SET base_snapshot_table = format('flashback.%I', v_snapshot_name),
           schema_version = v_schema_version
     WHERE tracking_id = v_tracking_id;

    PERFORM pg_logical_emit_message(
        true,
        'pg_flashback',
        jsonb_build_object(
            'op', 'BOUNDARY',
            'kind', 'maintenance_reanchor',
            'tracking_id', v_tracking_id,
            'generation_id', v_generation_id
        )::text
    );

    RETURN v_generation_id;
END;
$$;

-- Internal admission primitive shared by restore/query/recover and the
-- timestamp convenience planner. Exactly one immutable generation must own
-- the requested prefix and every required asset must still be available.
CREATE OR REPLACE FUNCTION flashback_admit_lsn_target(
    p_target_table text,
    p_target_lsn pg_lsn
)
RETURNS TABLE (
    tracking_id bigint,
    generation_id bigint,
    stream_id bigint,
    rel_oid oid,
    schema_name text,
    table_name text,
    boundary_snapshot_id bigint,
    snapshot_table text,
    boundary_time timestamptz,
    boundary_lsn pg_lsn,
    valid_through_time timestamptz,
    valid_through_lsn pg_lsn
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_tracking_id bigint;
    v_candidate_count integer;
BEGIN
    IF p_target_lsn IS NULL THEN
        RAISE EXCEPTION 'flashback target LSN is required';
    END IF;

    SELECT tt.tracking_id
      INTO v_tracking_id
    FROM flashback.tracked_tables tt
    WHERE tt.is_active
      AND tt.recovery_profile = 'local_delta'
      AND (
          tt.rel_oid = to_regclass(p_target_table)::oid
          OR format('%I.%I', tt.schema_name, tt.table_name) = p_target_table
          OR (position('.' IN p_target_table) = 0 AND tt.table_name = p_target_table)
      )
    ORDER BY
        (tt.rel_oid = to_regclass(p_target_table)::oid) DESC,
        (format('%I.%I', tt.schema_name, tt.table_name) = p_target_table) DESC
    LIMIT 1;

    IF v_tracking_id IS NULL THEN
        RAISE EXCEPTION 'pg_flashback: table % is not actively tracked with local_delta', p_target_table;
    END IF;

    -- Pin the complete generation payload for the caller transaction. Every
    -- retention/restore/checkpoint path uses this same stable lifecycle key,
    -- so a durable retirement intent cannot appear between admission and
    -- materialization (query/recover previously lacked this pin).
    PERFORM pg_advisory_xact_lock(358944::integer, hashint8(v_tracking_id));

    IF NOT EXISTS (
        SELECT 1
        FROM flashback.tracked_tables tt
        WHERE tt.tracking_id = v_tracking_id
          AND tt.is_active
          AND tt.recovery_profile = 'local_delta'
    ) THEN
        RAISE EXCEPTION 'pg_flashback: tracking lifecycle % changed while target admission waited',
            v_tracking_id;
    END IF;

    IF EXISTS (
        SELECT 1 FROM flashback.coverage_generations pending
        WHERE pending.tracking_id = v_tracking_id
          AND pending.state = 'building'
          AND pending.boundary_kind = 'post_restore'
    ) THEN
        RAISE EXCEPTION 'pg_flashback: table % has an unresolved post-restore boundary', p_target_table
            USING HINT = 'Wait for the WAL worker to resolve the swap COMMIT LSN; flashback_health() shows the pending generation.';
    END IF;

    SELECT count(*) INTO v_candidate_count
    FROM flashback.coverage_generations cg
    JOIN flashback.capture_streams cs ON cs.stream_id = cg.stream_id
    JOIN flashback.snapshots snap
      ON snap.snapshot_id = cg.boundary_snapshot_id
     AND snap.tracking_id = cg.tracking_id
     AND snap.snapshot_lsn = cg.boundary_lsn
    WHERE cg.tracking_id = v_tracking_id
      AND cg.recovery_profile = 'local_delta'
      AND cg.state IN ('active', 'sealed')
      AND cg.boundary_lsn <= p_target_lsn
      AND p_target_lsn <= cg.valid_through_lsn
      AND p_target_lsn <= cs.valid_through_lsn
      AND (cg.superseded_before_lsn IS NULL OR p_target_lsn < cg.superseded_before_lsn)
      AND snap.payload_state = 'available'
      AND to_regclass(snap.snapshot_table) IS NOT NULL
      AND public.flashback_payload_is_owned(to_regclass(snap.snapshot_table))
      AND NOT EXISTS (
          SELECT 1
          FROM flashback.generation_payload_retirements retirement
          WHERE retirement.generation_id = cg.generation_id
      )
      AND NOT EXISTS (
          SELECT 1
          FROM flashback.coverage_gaps gap
          WHERE gap.tracking_id = cg.tracking_id
            AND EXISTS (
                SELECT 1
                FROM flashback.coverage_generations gap_source
                WHERE gap_source.generation_id = gap.source_generation_id
                  AND gap_source.tracking_id = gap.tracking_id
                  AND gap_source.stream_id = cg.stream_id
            )
            AND gap.gap_start_lsn IS NOT NULL
            AND (
                (gap.lower_bound_inclusive AND p_target_lsn >= gap.gap_start_lsn)
                OR (NOT gap.lower_bound_inclusive AND p_target_lsn > gap.gap_start_lsn)
            )
            AND (gap.gap_end_lsn IS NULL OR p_target_lsn < gap.gap_end_lsn)
      );

    IF v_candidate_count <> 1 THEN
        RAISE EXCEPTION 'pg_flashback: target LSN % for % is owned by % eligible generations',
            p_target_lsn, p_target_table, v_candidate_count
            USING HINT = 'The target must be inside exactly one proven generation, at or before its frozen watermark, and outside every durable gap.';
    END IF;

    RETURN QUERY
    SELECT
        cg.tracking_id, cg.generation_id, cg.stream_id,
        tt.rel_oid, tt.schema_name, tt.table_name,
        cg.boundary_snapshot_id, snap.snapshot_table,
        cg.boundary_time, cg.boundary_lsn,
        cg.valid_through_time, cg.valid_through_lsn
    FROM flashback.coverage_generations cg
    JOIN flashback.tracked_tables tt ON tt.tracking_id = cg.tracking_id
    JOIN flashback.capture_streams cs ON cs.stream_id = cg.stream_id
    JOIN flashback.snapshots snap
      ON snap.snapshot_id = cg.boundary_snapshot_id
     AND snap.tracking_id = cg.tracking_id
     AND snap.snapshot_lsn = cg.boundary_lsn
    WHERE cg.tracking_id = v_tracking_id
      AND cg.state IN ('active', 'sealed')
      AND cg.boundary_lsn <= p_target_lsn
      AND p_target_lsn <= cg.valid_through_lsn
      AND p_target_lsn <= cs.valid_through_lsn
      AND (cg.superseded_before_lsn IS NULL OR p_target_lsn < cg.superseded_before_lsn)
      AND snap.payload_state = 'available'
      AND to_regclass(snap.snapshot_table) IS NOT NULL
      AND public.flashback_payload_is_owned(to_regclass(snap.snapshot_table))
      AND NOT EXISTS (
          SELECT 1
          FROM flashback.generation_payload_retirements retirement
          WHERE retirement.generation_id = cg.generation_id
      )
      AND NOT EXISTS (
          SELECT 1 FROM flashback.coverage_gaps gap
          WHERE gap.tracking_id = cg.tracking_id
            AND EXISTS (
                SELECT 1
                FROM flashback.coverage_generations gap_source
                WHERE gap_source.generation_id = gap.source_generation_id
                  AND gap_source.tracking_id = gap.tracking_id
                  AND gap_source.stream_id = cg.stream_id
            )
            AND gap.gap_start_lsn IS NOT NULL
            AND ((gap.lower_bound_inclusive AND p_target_lsn >= gap.gap_start_lsn)
                 OR (NOT gap.lower_bound_inclusive AND p_target_lsn > gap.gap_start_lsn))
            AND (gap.gap_end_lsn IS NULL OR p_target_lsn < gap.gap_end_lsn)
      );
END;
$$;

CREATE OR REPLACE FUNCTION flashback_resolve_target(
    p_target_table text,
    p_target_time timestamptz
)
RETURNS TABLE (
    tracking_id bigint,
    generation_id bigint,
    stream_id bigint,
    requested_time timestamptz,
    resolved_lsn pg_lsn,
    resolved_commit_time timestamptz,
    pinned_frontier_lsn pg_lsn,
    pinned_frontier_time timestamptz,
    resolution_note text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, flashback, public
AS $$
DECLARE
    v_tracking_id bigint;
    gen record;
    v_candidate_lsn pg_lsn;
    v_candidate_time timestamptz;
    v_frontier_time timestamptz;
    v_collision_count integer;
    v_resolved_count integer := 0;
    v_bad_before boolean;
    v_bad_after boolean;
BEGIN
    SELECT tt.tracking_id INTO v_tracking_id
    FROM flashback.tracked_tables tt
    WHERE tt.is_active
      AND tt.recovery_profile = 'local_delta'
      AND (
          tt.rel_oid = to_regclass(p_target_table)::oid
          OR format('%I.%I', tt.schema_name, tt.table_name) = p_target_table
          OR (position('.' IN p_target_table) = 0 AND tt.table_name = p_target_table)
      )
    ORDER BY (tt.rel_oid = to_regclass(p_target_table)::oid) DESC
    LIMIT 1;

    IF v_tracking_id IS NULL THEN
        RAISE EXCEPTION 'flashback_resolve_target: table % is not actively tracked', p_target_table;
    END IF;

    -- Pin the complete lifecycle while reading its commit-time/LSN ledger.
    -- Retention and stream-break paths acquire this same key before changing
    -- generation/frontier state. This path deliberately does not take the
    -- database-wide stream key: a long query for one table must not stall WAL
    -- capture for every other tracked table in the database.
    PERFORM pg_advisory_xact_lock(358944::integer, hashint8(v_tracking_id));

    FOR gen IN
        SELECT cg.*, LEAST(cg.valid_through_lsn, cs.valid_through_lsn) AS frontier_lsn
        FROM flashback.coverage_generations cg
        JOIN flashback.capture_streams cs ON cs.stream_id = cg.stream_id
        WHERE cg.tracking_id = v_tracking_id
          AND cg.state IN ('active', 'sealed')
        ORDER BY cg.generation_no
    LOOP
        SELECT cc.committed_at INTO v_frontier_time
        FROM flashback.capture_commits cc
        WHERE cc.stream_id = gen.stream_id
          AND cc.commit_lsn = gen.frontier_lsn;
        v_frontier_time := COALESCE(v_frontier_time, gen.valid_through_time);

        IF gen.boundary_time IS NULL OR v_frontier_time IS NULL
           OR p_target_time < gen.boundary_time
           OR p_target_time > v_frontier_time
        THEN
            CONTINUE;
        END IF;

        SELECT count(*) INTO v_collision_count
        FROM flashback.capture_commits cc
        WHERE cc.stream_id = gen.stream_id
          AND cc.commit_lsn >= gen.boundary_lsn
          AND cc.commit_lsn <= gen.frontier_lsn
          AND cc.committed_at = p_target_time;
        IF v_collision_count > 1 THEN
            RAISE EXCEPTION 'pg_flashback: timestamp % maps to % commits in generation %',
                p_target_time, v_collision_count, gen.generation_id
                USING HINT = 'Use flashback_restore_lsn/query_lsn/recover_deleted_lsn with an explicit COMMIT LSN.';
        END IF;

        SELECT cc.commit_lsn, cc.committed_at
          INTO v_candidate_lsn, v_candidate_time
        FROM flashback.capture_commits cc
        WHERE cc.stream_id = gen.stream_id
          AND cc.commit_lsn >= gen.boundary_lsn
          AND cc.commit_lsn <= gen.frontier_lsn
          AND cc.committed_at <= p_target_time
        ORDER BY cc.commit_lsn DESC
        LIMIT 1;

        IF v_candidate_lsn IS NULL THEN
            v_candidate_lsn := gen.boundary_lsn;
            v_candidate_time := gen.boundary_time;
        END IF;

        SELECT EXISTS (
            SELECT 1 FROM flashback.capture_commits cc
            WHERE cc.stream_id = gen.stream_id
              AND cc.commit_lsn >= gen.boundary_lsn
              AND cc.commit_lsn <= v_candidate_lsn
              AND cc.committed_at > p_target_time
        ) INTO v_bad_before;
        SELECT EXISTS (
            SELECT 1 FROM flashback.capture_commits cc
            WHERE cc.stream_id = gen.stream_id
              AND cc.commit_lsn > v_candidate_lsn
              AND cc.commit_lsn <= gen.frontier_lsn
              AND cc.committed_at <= p_target_time
        ) INTO v_bad_after;

        IF v_bad_before OR v_bad_after THEN
            RAISE EXCEPTION 'pg_flashback: timestamp % is not a WAL-prefix cut in generation %',
                p_target_time, gen.generation_id
                USING DETAIL = format(
                    'A commit timestamp inversion straddles candidate LSN %s inside pinned frontier %s.',
                    v_candidate_lsn, gen.frontier_lsn
                ),
                      HINT = 'Use an explicit COMMIT LSN. The resolver never invents a state by filtering non-prefix transactions.';
        END IF;

        -- Reuse the canonical admission checks (assets, watermark and gaps).
        PERFORM 1 FROM flashback_admit_lsn_target(p_target_table, v_candidate_lsn);
        v_resolved_count := v_resolved_count + 1;
        IF v_resolved_count > 1 THEN
            RAISE EXCEPTION 'pg_flashback: timestamp % resolves in multiple generations for %',
                p_target_time, p_target_table
                USING HINT = 'Use an explicit generation-qualified COMMIT LSN.';
        END IF;

        tracking_id := gen.tracking_id;
        generation_id := gen.generation_id;
        stream_id := gen.stream_id;
        requested_time := p_target_time;
        resolved_lsn := v_candidate_lsn;
        resolved_commit_time := v_candidate_time;
        pinned_frontier_lsn := gen.frontier_lsn;
        pinned_frontier_time := v_frontier_time;
        resolution_note := 'unique observed WAL prefix; execution must use resolved_lsn';
    END LOOP;

    IF v_resolved_count = 0 THEN
        RAISE EXCEPTION 'pg_flashback: timestamp % cannot be proven inside a closed WAL frontier for %',
            p_target_time, p_target_table
            USING HINT = 'Wait for capture to advance beyond the requested time or provide an explicit COMMIT LSN.';
    END IF;

    RETURN NEXT;
END;
$$;

-- flashback_health() is defined in health_runtime.sql (actionable projection).

