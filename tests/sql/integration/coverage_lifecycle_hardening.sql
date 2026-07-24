-- Adversarial qualification for generation retirement and runtime capture
-- configuration transitions. Full two-transaction cleanup is exercised by
-- run_wal_e2e.sh; pg_test itself intentionally wraps a case in one transaction.
DROP TABLE IF EXISTS public.it_retention_hardening CASCADE;
DROP TABLE IF EXISTS public.it_blocked_hardening CASCADE;
DROP TABLE IF EXISTS public.it_enabled_hardening CASCADE;
DROP TABLE IF EXISTS flashback.it_enabled_hardening_snapshot CASCADE;

CREATE TABLE public.it_retention_hardening (id integer PRIMARY KEY, note text);
INSERT INTO public.it_retention_hardening VALUES (1, 'base');
CREATE TABLE public.it_blocked_hardening (id integer PRIMARY KEY, note text);
INSERT INTO public.it_blocked_hardening VALUES (1, 'blocked');
CREATE TABLE public.it_enabled_hardening (id integer PRIMARY KEY, note text);
CREATE TABLE flashback.it_enabled_hardening_snapshot
AS TABLE public.it_enabled_hardening;

DO $test$
DECLARE
    v_tracking_id bigint;
    v_blocked_tracking_id bigint;
    v_enabled_tracking_id bigint;
    v_stream_id bigint;
    v_enabled_stream_id bigint;
    v_snapshot_1 bigint;
    v_snapshot_2 bigint;
    v_snapshot_3 bigint;
    v_enabled_snapshot bigint;
    v_generation_1 bigint;
    v_generation_2 bigint;
    v_generation_3 bigint;
    v_pending_snapshot bigint;
    v_pending_generation bigint;
    v_blocked_snapshot bigint;
    v_blocked_generation bigint;
    v_enabled_generation bigint;
    v_snapshot_name text;
    v_retirement_id bigint;
    v_base timestamptz := clock_timestamp() - interval '2 days';
    v_failed boolean;
BEGIN
    PERFORM set_config('pg_flashback.enabled', 'on', true);
    PERFORM set_config('pg_flashback.capture_mode', 'wal', true);

    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table,
        recovery_profile, retention_interval, is_active
    ) VALUES (
        'public.it_retention_hardening'::regclass,
        'public', 'it_retention_hardening', NULL,
        'local_delta', interval '0 seconds', true
    ) RETURNING tracking_id INTO v_tracking_id;

    INSERT INTO flashback.capture_streams (
        database_oid, database_name, epoch_no, capture_mode, timeline_id,
        slot_name, plugin_name, state, valid_through_time, valid_through_lsn,
        confirmed_flush_lsn, restart_lsn, activated_at
    ) VALUES (
        (SELECT oid FROM pg_database WHERE datname = current_database()),
        current_database(), 1, 'wal', 1,
        'it_lifecycle_slot_1', 'pg_flashback', 'active',
        v_base + interval '4 seconds', '0/4000', '0/4000', '0/1000',
        clock_timestamp()
    ) RETURNING stream_id INTO v_stream_id;

    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn,
        schema_def, row_count, captured_at
    ) VALUES (
        'public.it_retention_hardening'::regclass, v_tracking_id,
        '', '0/1000', '{}'::jsonb, 1, v_base
    ) RETURNING snapshot_id INTO v_snapshot_1;
    v_snapshot_name := format('snap_%s_%s', v_tracking_id, v_snapshot_1);
    EXECUTE format(
        'CREATE TABLE flashback.%I AS TABLE public.it_retention_hardening',
        v_snapshot_name
    );
    PERFORM public.flashback_own_payload_table(
        to_regclass(format('flashback.%I', v_snapshot_name))
    );
    UPDATE flashback.snapshots
       SET snapshot_table = format('flashback.%I', v_snapshot_name),
           storage_backend = 'heap_v1',
           locator = jsonb_build_object('schema', 'flashback', 'relation', v_snapshot_name),
           payload_state = 'available',
           available_at = clock_timestamp()
     WHERE snapshot_id = v_snapshot_1;

    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, stream_id, recovery_profile, state,
        boundary_kind, rel_oid_at_boundary, boundary_snapshot_id,
        boundary_time, boundary_lsn, valid_through_time, valid_through_lsn,
        activated_at
    ) VALUES (
        v_tracking_id, 1, v_stream_id, 'local_delta', 'active',
        'retention_fixture', 'public.it_retention_hardening'::regclass,
        v_snapshot_1, v_base, '0/1000',
        v_base + interval '2 seconds', '0/2000', clock_timestamp()
    ) RETURNING generation_id INTO v_generation_1;
    UPDATE flashback.coverage_generations
       SET state = 'sealed',
           superseded_before_time = v_base + interval '3 seconds',
           superseded_before_lsn = '0/3000',
           sealed_at = clock_timestamp() - interval '1 day',
           state_reason = 'test_successor'
     WHERE generation_id = v_generation_1;

    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn,
        schema_def, row_count, captured_at
    ) VALUES (
        'public.it_retention_hardening'::regclass, v_tracking_id,
        '', '0/3000', '{}'::jsonb, 1, v_base + interval '3 seconds'
    ) RETURNING snapshot_id INTO v_snapshot_2;
    v_snapshot_name := format('snap_%s_%s', v_tracking_id, v_snapshot_2);
    EXECUTE format(
        'CREATE TABLE flashback.%I AS TABLE public.it_retention_hardening',
        v_snapshot_name
    );
    PERFORM public.flashback_own_payload_table(
        to_regclass(format('flashback.%I', v_snapshot_name))
    );
    UPDATE flashback.snapshots
       SET snapshot_table = format('flashback.%I', v_snapshot_name),
           storage_backend = 'heap_v1',
           locator = jsonb_build_object('schema', 'flashback', 'relation', v_snapshot_name),
           payload_state = 'available',
           available_at = clock_timestamp()
     WHERE snapshot_id = v_snapshot_2;

    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, parent_generation_id, stream_id,
        recovery_profile, state, boundary_kind, rel_oid_at_boundary,
        boundary_snapshot_id, boundary_time, boundary_lsn,
        valid_through_time, valid_through_lsn, activated_at
    ) VALUES (
        v_tracking_id, 2, v_generation_1, v_stream_id,
        'local_delta', 'active', 'retention_successor',
        'public.it_retention_hardening'::regclass,
        v_snapshot_2, v_base + interval '3 seconds', '0/3000',
        v_base + interval '4 seconds', '0/4000', clock_timestamp()
    ) RETURNING generation_id INTO v_generation_2;

    -- A second lifecycle is deliberately sealed with a complete watermark
    -- but no newer active anchor.  Retention must skip it (and report the
    -- blocked condition through health) while still creating an intent for
    -- the independently retireable predecessor above.
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table,
        recovery_profile, retention_interval, is_active
    ) VALUES (
        'public.it_blocked_hardening'::regclass,
        'public', 'it_blocked_hardening', NULL,
        'local_delta', interval '0 seconds', true
    ) RETURNING tracking_id INTO v_blocked_tracking_id;

    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn,
        schema_def, row_count, captured_at
    ) VALUES (
        'public.it_blocked_hardening'::regclass, v_blocked_tracking_id,
        '', '0/5000', '{}'::jsonb, 1, v_base
    ) RETURNING snapshot_id INTO v_blocked_snapshot;
    v_snapshot_name := format('snap_%s_%s', v_blocked_tracking_id, v_blocked_snapshot);
    EXECUTE format(
        'CREATE TABLE flashback.%I AS TABLE public.it_blocked_hardening',
        v_snapshot_name
    );
    PERFORM public.flashback_own_payload_table(
        to_regclass(format('flashback.%I', v_snapshot_name))
    );
    UPDATE flashback.snapshots
       SET snapshot_table = format('flashback.%I', v_snapshot_name),
           storage_backend = 'heap_v1',
           locator = jsonb_build_object('schema', 'flashback', 'relation', v_snapshot_name),
           payload_state = 'available',
           available_at = clock_timestamp()
     WHERE snapshot_id = v_blocked_snapshot;

    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, stream_id, recovery_profile, state,
        boundary_kind, rel_oid_at_boundary, boundary_snapshot_id,
        boundary_time, boundary_lsn, valid_through_time, valid_through_lsn,
        activated_at
    ) VALUES (
        v_blocked_tracking_id, 1, v_stream_id, 'local_delta', 'active',
        'retention_blocked_fixture', 'public.it_blocked_hardening'::regclass,
        v_blocked_snapshot, v_base, '0/5000',
        v_base + interval '8 seconds', '0/6000', clock_timestamp()
    ) RETURNING generation_id INTO v_blocked_generation;
    UPDATE flashback.coverage_generations
       SET state = 'sealed',
           superseded_before_time = v_base + interval '9 seconds',
           superseded_before_lsn = '0/6000',
           sealed_at = clock_timestamp() - interval '1 day',
           state_reason = 'retention_blocked_fixture'
     WHERE generation_id = v_blocked_generation;

    INSERT INTO flashback.delta_log (
        event_time, event_type, table_name, rel_oid, source_xid,
        tracking_id, generation_id, stream_id,
        committed_at, commit_lsn, schema_version, new_data, lsn
    ) VALUES (
        v_base + interval '1 second', 'INSERT',
        'public.it_retention_hardening',
        'public.it_retention_hardening'::regclass, 1001,
        v_tracking_id, v_generation_1, v_stream_id,
        v_base + interval '1 second', '0/2000', 1,
        '{"id":2,"note":"old"}'::jsonb, '0/1800'
    );

    -- A sealed generation whose backlog has not reached its immutable upper
    -- bound must not even receive a destructive intent.  Exercise the public
    -- retention driver too: the eligibility predicate must skip this row,
    -- rather than merely relying on callers to invoke the begin primitive.
    PERFORM flashback_apply_retention();
    IF EXISTS (
        SELECT 1
        FROM flashback.generation_payload_retirements
        WHERE generation_id = v_generation_1
    ) THEN
        RAISE EXCEPTION 'retention driver created intent for an undrained generation';
    END IF;

    v_failed := false;
    BEGIN
        PERFORM flashback_begin_generation_retirement(v_generation_1);
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM LIKE '%backlog has not drained%' THEN
            v_failed := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'undrained generation received a retirement intent';
    END IF;

    UPDATE flashback.coverage_generations
       SET valid_through_lsn = '0/3000',
           valid_through_time = v_base + interval '3 seconds'
     WHERE generation_id = v_generation_1;

    PERFORM flashback_apply_retention();
    IF EXISTS (
        SELECT 1
        FROM flashback.generation_payload_retirements
        WHERE generation_id = v_blocked_generation
    ) THEN
        RAISE EXCEPTION 'retention created an intent for a generation without a newer active anchor';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM flashback_health()
        WHERE tracking_id = v_blocked_tracking_id
          AND health = 'maintenance_required'
          AND reason LIKE '%retention%blocked%'
    ) THEN
        RAISE EXCEPTION 'blocked sealed generation was not surfaced as maintenance_required health';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM flashback.generation_payload_retirements
        WHERE generation_id = v_generation_1
          AND state = 'retiring'
    ) THEN
        RAISE EXCEPTION 'blocked generation prevented an independently retireable generation from receiving an intent';
    END IF;

    v_retirement_id := flashback_begin_generation_retirement(v_generation_1);
    IF NOT EXISTS (
        SELECT 1
        FROM flashback.generation_payload_retirements
        WHERE retirement_id = v_retirement_id
          AND generation_id = v_generation_1
          AND state = 'retiring'
          AND expected_delta_rows = 1
          AND snapshot_rel_oid = to_regclass(snapshot_table)::oid
    ) THEN
        RAISE EXCEPTION 'durable retirement intent did not freeze payload evidence';
    END IF;

    v_failed := false;
    BEGIN
        PERFORM * FROM flashback_admit_lsn_target(
            'public.it_retention_hardening', '0/2000'
        );
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM LIKE '%owned by 0 eligible generations%' THEN
            v_failed := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'retiring generation remained admissible';
    END IF;
    IF (SELECT count(*) FROM flashback_admit_lsn_target(
            'public.it_retention_hardening', '0/3500')) <> 1
    THEN
        RAISE EXCEPTION 'active successor was harmed by predecessor retirement intent';
    END IF;

    v_failed := false;
    BEGIN
        PERFORM flashback_resume_generation_retirement(v_generation_1);
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM LIKE '%intent must commit%' THEN
            v_failed := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'payload was removable in the intent transaction';
    END IF;

    v_failed := false;
    BEGIN
        PERFORM flashback_untrack('public.it_retention_hardening');
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM LIKE '%unfinished retention cleanup%' THEN
            v_failed := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'untrack bypassed a durable retention intent';
    END IF;

    BEGIN
        UPDATE flashback.generation_payload_retirements
           SET expected_delta_rows = 0
         WHERE retirement_id = v_retirement_id;
        RAISE EXCEPTION 'retirement evidence was mutable';
    EXCEPTION WHEN integrity_constraint_violation THEN
        NULL;
    END;

    -- A caller-local illegal capture_mode GUC cannot reroute DDL from an
    -- already-qualified WAL generation. The synchronous guard freezes the
    -- epoch and fails closed.
    PERFORM set_config('pg_flashback.capture_mode', 'trigger', true);
    v_failed := false;
    BEGIN
        ALTER TABLE public.it_retention_hardening ADD COLUMN mode_probe integer;
        PERFORM flashback_capture_ddl_event(
            'ALTER', 'public', 'it_retention_hardening'
        );
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM LIKE '%capture configuration%' OR
           SQLERRM LIKE '%WAL stream%' OR
           SQLERRM LIKE '%not supported%' THEN
            v_failed := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'session capture_mode=trigger bypassed the qualified DDL guard';
    END IF;
    IF EXISTS (
        SELECT 1 FROM flashback.pending_wal_events
        WHERE generation_id = v_generation_2
          AND event_type = 'ALTER'
    ) THEN
        RAISE EXCEPTION 'session capture_mode rerouted qualified DDL into a payload after the epoch break';
    END IF;

    IF flashback_reconcile_capture_configuration() <> 'capture_mode_changed' THEN
        RAISE EXCEPTION 'illegal capture_mode transition was not reconciled as a break';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM flashback.capture_streams
        WHERE stream_id = v_stream_id
          AND state = 'broken'
          AND invalidation_reason = 'capture_mode_changed'
    ) OR NOT EXISTS (
        SELECT 1 FROM flashback.coverage_gaps
        WHERE tracking_id = v_tracking_id
          AND source_generation_id = v_generation_2
          AND reason = 'capture_mode_changed'
    ) THEN
        RAISE EXCEPTION 'illegal capture_mode transition did not durably freeze coverage';
    END IF;

    v_failed := false;
    BEGIN
        ALTER TABLE public.it_retention_hardening ADD COLUMN after_break integer;
        PERFORM flashback_capture_ddl_event(
            'ALTER', 'public', 'it_retention_hardening'
        );
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM LIKE '%DDL capture refused because WAL stream%'
           OR SQLERRM LIKE '%capture configuration%'
           OR SQLERRM LIKE '%not supported%' THEN
            v_failed := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'DDL on a broken qualified stream was allowed';
    END IF;

    -- Re-enabled WAL creates a new stream epoch; disabling it is reconciled
    -- before worker idling and opens exactly one gap per active lifecycle.
    PERFORM set_config('pg_flashback.capture_mode', 'wal', true);
    INSERT INTO flashback.capture_streams (
        database_oid, database_name, epoch_no, capture_mode, timeline_id,
        slot_name, plugin_name, state, valid_through_time, valid_through_lsn,
        confirmed_flush_lsn, restart_lsn, activated_at
    ) VALUES (
        (SELECT oid FROM pg_database WHERE datname = current_database()),
        current_database(), 2, 'wal', 1,
        'it_lifecycle_slot_2', 'pg_flashback', 'active',
        v_base + interval '8 seconds', '0/8000', '0/8000', '0/7000',
        clock_timestamp()
    ) RETURNING stream_id INTO v_enabled_stream_id;

    -- A stream break followed by a new exact anchor leaves the predecessor's
    -- old-stream watermark below the successor boundary.  The closed durable
    -- gap is the proof that this hand-off interval is intentionally
    -- unservable; retention must still retire the complete old payload.
    UPDATE flashback.coverage_generations
       SET state = 'sealed',
           superseded_before_time = v_base + interval '7 seconds',
           superseded_before_lsn = '0/7000',
           sealed_at = clock_timestamp() - interval '1 day',
           state_reason = 'cross_stream_successor'
     WHERE generation_id = v_generation_2;

    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn,
        schema_def, row_count, captured_at
    ) VALUES (
        'public.it_retention_hardening'::regclass, v_tracking_id,
        '', '0/7000', '{}'::jsonb, 1, v_base + interval '7 seconds'
    ) RETURNING snapshot_id INTO v_snapshot_3;
    v_snapshot_name := format('snap_%s_%s', v_tracking_id, v_snapshot_3);
    EXECUTE format(
        'CREATE TABLE flashback.%I AS TABLE public.it_retention_hardening',
        v_snapshot_name
    );
    PERFORM public.flashback_own_payload_table(
        to_regclass(format('flashback.%I', v_snapshot_name))
    );
    UPDATE flashback.snapshots
       SET snapshot_table = format('flashback.%I', v_snapshot_name),
           storage_backend = 'heap_v1',
           locator = jsonb_build_object('schema', 'flashback', 'relation', v_snapshot_name),
           payload_state = 'available',
           available_at = clock_timestamp()
     WHERE snapshot_id = v_snapshot_3;

    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, parent_generation_id, stream_id,
        recovery_profile, state, boundary_kind, rel_oid_at_boundary,
        boundary_snapshot_id, boundary_time, boundary_lsn,
        valid_through_time, valid_through_lsn, activated_at
    ) VALUES (
        v_tracking_id, 3, v_generation_2, v_enabled_stream_id,
        'local_delta', 'active', 'cross_stream_successor',
        'public.it_retention_hardening'::regclass, v_snapshot_3,
        v_base + interval '7 seconds', '0/7000',
        v_base + interval '8 seconds', '0/8000', clock_timestamp()
    ) RETURNING generation_id INTO v_generation_3;

    INSERT INTO flashback.coverage_gaps (
        tracking_id, source_generation_id, reason,
        gap_start_lsn, gap_end_lsn, reanchored_by_generation_id,
        reanchored_at, details
    ) VALUES (
        v_tracking_id, v_generation_2, 'replication_slot_missing',
        '0/4000', '0/7000', v_generation_3,
        clock_timestamp(), jsonb_build_object('stream_handoff', true)
    );

    v_retirement_id := flashback_begin_generation_retirement(v_generation_2);
    IF NOT EXISTS (
        SELECT 1
        FROM flashback.generation_payload_retirements
        WHERE retirement_id = v_retirement_id
          AND generation_id = v_generation_2
          AND state = 'retiring'
    ) THEN
        RAISE EXCEPTION 'cross-stream sealed predecessor was not retirement-eligible';
    END IF;

    -- A post-restore/re-anchor draft may be committed as `building` while the
    -- slot is still healthy.  If that stream then breaks, the draft has no
    -- canonical boundary and must not survive to block the next re-anchor or
    -- retain an unreachable payload.  The break transaction owns cleanup.
    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn,
        schema_def, row_count, captured_at
    ) VALUES (
        'public.it_blocked_hardening'::regclass, v_blocked_tracking_id,
        '', '0/9000', '{}'::jsonb, 1, v_base + interval '9 seconds'
    ) RETURNING snapshot_id INTO v_pending_snapshot;
    v_snapshot_name := format('snap_%s_%s', v_blocked_tracking_id, v_pending_snapshot);
    EXECUTE format(
        'CREATE TABLE flashback.%I AS TABLE public.it_blocked_hardening',
        v_snapshot_name
    );
    PERFORM public.flashback_own_payload_table(
        to_regclass(format('flashback.%I', v_snapshot_name))
    );
    UPDATE flashback.snapshots
       SET snapshot_table = format('flashback.%I', v_snapshot_name),
           storage_backend = 'heap_v1',
           locator = jsonb_build_object('schema', 'flashback', 'relation', v_snapshot_name),
           payload_state = 'available',
           available_at = clock_timestamp()
     WHERE snapshot_id = v_pending_snapshot;
    UPDATE flashback.tracked_tables
       SET base_snapshot_table = format('flashback.%I', v_snapshot_name)
     WHERE tracking_id = v_blocked_tracking_id;

    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, parent_generation_id, stream_id,
        recovery_profile, state, boundary_kind, rel_oid_at_boundary,
        boundary_snapshot_id, boundary_xid, boundary_marker
    ) VALUES (
        v_blocked_tracking_id, 2, v_blocked_generation, v_enabled_stream_id,
        'local_delta', 'building', 'post_restore_pending',
        'public.it_blocked_hardening'::regclass, v_pending_snapshot,
        9001, 'pending-boundary'
    ) RETURNING generation_id INTO v_pending_generation;

    PERFORM flashback_mark_capture_stream_broken(
        v_enabled_stream_id,
        'pending_builder_slot_loss',
        jsonb_build_object('test_pending_generation', v_pending_generation)
    );
    IF NOT EXISTS (
        SELECT 1
        FROM flashback.coverage_generations
        WHERE generation_id = v_pending_generation
          AND state = 'aborted'
          AND aborted_at IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'broken stream did not preserve the pending generation as an aborted tombstone';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM flashback.snapshots
        WHERE snapshot_id = v_pending_snapshot
          AND (payload_state <> 'missing' OR retired_at IS NULL)
    ) THEN
        RAISE EXCEPTION 'broken stream did not invalidate the pending snapshot';
    END IF;
    IF to_regclass(format('flashback.%I', v_snapshot_name)) IS NOT NULL THEN
        RAISE EXCEPTION 'broken stream left the pending snapshot payload table';
    END IF;
    IF (SELECT base_snapshot_table
        FROM flashback.tracked_tables
        WHERE tracking_id = v_blocked_tracking_id) IS NOT NULL THEN
        RAISE EXCEPTION 'broken stream left a dangling pending base binding';
    END IF;
    IF (SELECT details ->> 'discarded_building_generations'
        FROM flashback.capture_streams
        WHERE stream_id = v_enabled_stream_id) <> '1' THEN
        RAISE EXCEPTION 'broken stream did not record discarded building generation';
    END IF;

    -- The lifecycle used by the enabled=off guard gets a fresh stream epoch;
    -- the broken epoch above must not be reused by later capture work.
    INSERT INTO flashback.capture_streams (
        database_oid, database_name, epoch_no, capture_mode, timeline_id,
        slot_name, plugin_name, state, valid_through_time, valid_through_lsn,
        confirmed_flush_lsn, restart_lsn, activated_at
    ) VALUES (
        (SELECT oid FROM pg_database WHERE datname = current_database()),
        current_database(), 3, 'wal', 1,
        'it_lifecycle_slot_3', 'pg_flashback', 'active',
        v_base + interval '10 seconds', '0/A000', '0/A000', '0/9000',
        clock_timestamp()
    ) RETURNING stream_id INTO v_enabled_stream_id;

    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table,
        recovery_profile, is_active
    ) VALUES (
        'public.it_enabled_hardening'::regclass,
        'public', 'it_enabled_hardening',
        'flashback.it_enabled_hardening_snapshot',
        'local_delta', true
    ) RETURNING tracking_id INTO v_enabled_tracking_id;
    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn,
        schema_def, row_count, captured_at
    ) VALUES (
        'public.it_enabled_hardening'::regclass, v_enabled_tracking_id,
        '', '0/7000', '{}'::jsonb, 0, v_base + interval '7 seconds'
    ) RETURNING snapshot_id INTO v_enabled_snapshot;
    PERFORM flashback_internal_snapshot_transition(
        v_enabled_snapshot, v_enabled_tracking_id, ARRAY['creating'], 'available',
        'heap_v1',
        jsonb_build_object('schema', 'flashback', 'relation', 'it_enabled_hardening_snapshot'),
        'flashback.it_enabled_hardening_snapshot', 0, NULL
    );
    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, stream_id, recovery_profile, state,
        boundary_kind, rel_oid_at_boundary, boundary_snapshot_id,
        boundary_time, boundary_lsn, valid_through_time, valid_through_lsn,
        activated_at
    ) VALUES (
        v_enabled_tracking_id, 1, v_enabled_stream_id,
        'local_delta', 'active', 'enabled_fixture',
        'public.it_enabled_hardening'::regclass, v_enabled_snapshot,
        v_base + interval '7 seconds', '0/7000',
        v_base + interval '8 seconds', '0/8000', clock_timestamp()
    ) RETURNING generation_id INTO v_enabled_generation;

    -- A session-local SUSET override must not bypass the worker's epoch
    -- protocol. enabled=off reconciles as a break and opens one LOGGED gap.
    PERFORM set_config('pg_flashback.enabled', 'off', true);
    IF flashback_reconcile_capture_configuration() NOT IN ('capture_disabled', 'disabled') THEN
        RAISE EXCEPTION 'enabled=off transition was not reconciled as a break';
    END IF;
    IF (SELECT count(*) FROM flashback.coverage_gaps
        WHERE tracking_id = v_enabled_tracking_id
          AND source_generation_id = v_enabled_generation
          AND reason = 'capture_disabled') <> 1
    THEN
        RAISE EXCEPTION 'enabled=off did not open exactly one durable gap';
    END IF;
    PERFORM flashback_reconcile_capture_configuration();
    IF (SELECT count(*) FROM flashback.coverage_gaps
        WHERE tracking_id = v_enabled_tracking_id
          AND source_generation_id = v_enabled_generation
          AND reason = 'capture_disabled') <> 1
    THEN
        RAISE EXCEPTION 'configuration reconciliation duplicated the durable gap';
    END IF;
END;
$test$;

-- ---------------------------------------------------------------------
-- Retirement resume validates IDENTITY and NEED, not content (policy B).
-- The resume path must refuse payload removal when the newer active anchor
-- was lost between the durable intent and cleanup, and must succeed —
-- regardless of payload content drift — once a valid successor anchors
-- coverage again. A fixture intent row with a foreign intent_txid crosses
-- the "intent must commit first" guard inside the single test transaction.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS public.it_retire_resume CASCADE;
DROP TABLE IF EXISTS flashback.base_snapshot_990001 CASCADE;
DROP TABLE IF EXISTS flashback.base_snapshot_990002 CASCADE;

CREATE TABLE public.it_retire_resume (id integer PRIMARY KEY, note text);
INSERT INTO public.it_retire_resume VALUES (1, 'base');
CREATE TABLE flashback.base_snapshot_990001 AS TABLE public.it_retire_resume;
CREATE TABLE flashback.base_snapshot_990002 AS TABLE public.it_retire_resume;
SELECT flashback_own_payload_table(
    'flashback.base_snapshot_990001'::regclass
);
SELECT flashback_own_payload_table(
    'flashback.base_snapshot_990002'::regclass
);

DO $test$
DECLARE
    v_tracking_id bigint;
    v_stream_id bigint;
    v_pred_snapshot bigint;
    v_pred_generation bigint;
    v_succ_snapshot bigint;
    v_base timestamptz := clock_timestamp() - interval '1 day';
    v_failed boolean;
BEGIN
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table,
        recovery_profile, retention_interval, is_active
    ) VALUES (
        'public.it_retire_resume'::regclass,
        'public', 'it_retire_resume', NULL,
        'local_delta', interval '0 seconds', true
    ) RETURNING tracking_id INTO v_tracking_id;

    INSERT INTO flashback.capture_streams (
        database_oid, database_name, epoch_no, capture_mode, timeline_id,
        slot_name, plugin_name, state, valid_through_time, valid_through_lsn,
        confirmed_flush_lsn, restart_lsn, activated_at
    ) VALUES (
        (SELECT oid FROM pg_database WHERE datname = current_database()),
        current_database(), 91, 'wal', 1,
        'it_retire_resume_slot', 'pg_flashback', 'active',
        v_base + interval '10 seconds', '0/700', '0/700', '0/50',
        clock_timestamp()
    ) RETURNING stream_id INTO v_stream_id;

    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn,
        schema_def, row_count, captured_at
    ) VALUES (
        'public.it_retire_resume'::regclass, v_tracking_id,
        '', '0/100', '{}'::jsonb, 1, v_base
    ) RETURNING snapshot_id INTO v_pred_snapshot;
    PERFORM flashback_internal_snapshot_transition(
        v_pred_snapshot, v_tracking_id, ARRAY['creating'], 'available',
        'heap_v1',
        jsonb_build_object('schema', 'flashback', 'relation', 'base_snapshot_990001'),
        'flashback.base_snapshot_990001', 1, NULL
    );

    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, stream_id, recovery_profile, state,
        boundary_kind, rel_oid_at_boundary, boundary_snapshot_id,
        boundary_time, boundary_lsn, valid_through_time, valid_through_lsn,
        activated_at
    ) VALUES (
        v_tracking_id, 1, v_stream_id, 'local_delta', 'active',
        'retire_resume_fixture', 'public.it_retire_resume'::regclass,
        v_pred_snapshot, v_base, '0/100',
        v_base + interval '4 seconds', '0/400',
        clock_timestamp()
    ) RETURNING generation_id INTO v_pred_generation;
    UPDATE flashback.coverage_generations
       SET state = 'sealed',
           superseded_before_time = v_base + interval '5 seconds',
           superseded_before_lsn = '0/500',
           sealed_at = clock_timestamp() - interval '1 day',
           state_reason = 'retire_resume_fixture'
     WHERE generation_id = v_pred_generation;

    -- Durable intent from a "previous" transaction (foreign intent_txid).
    INSERT INTO flashback.generation_payload_retirements (
        generation_id, tracking_id, reason, intent_txid,
        snapshot_id, snapshot_table, snapshot_rel_oid, snapshot_row_count,
        snapshot_schema_fingerprint, snapshot_storage_backend, snapshot_locator,
        expected_delta_rows, expected_schema_rows
    ) VALUES (
        v_pred_generation, v_tracking_id, 'retire_resume_fixture', 1,
        v_pred_snapshot, 'flashback.base_snapshot_990001',
        'flashback.base_snapshot_990001'::regclass::oid, 1,
        flashback_payload_schema_fingerprint(
            'flashback.base_snapshot_990001'::regclass
        ),
        'heap_v1', jsonb_build_object('schema', 'flashback', 'relation', 'base_snapshot_990001'),
        0, 0
    );

    -- No active successor exists: resume must fail closed.
    v_failed := false;
    BEGIN
        PERFORM flashback_resume_generation_retirement(v_pred_generation);
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM LIKE '%lost its newer active retained anchor%' THEN
            v_failed := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'retirement resume removed payload without a retained newer anchor';
    END IF;
    IF to_regclass('flashback.base_snapshot_990001') IS NULL THEN
        RAISE EXCEPTION 'fail-closed resume must not touch the payload table';
    END IF;

    -- A valid successor re-anchors coverage.
    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn,
        schema_def, row_count, captured_at
    ) VALUES (
        'public.it_retire_resume'::regclass, v_tracking_id,
        '', '0/600', '{}'::jsonb, 1, v_base + interval '6 seconds'
    ) RETURNING snapshot_id INTO v_succ_snapshot;
    PERFORM flashback_internal_snapshot_transition(
        v_succ_snapshot, v_tracking_id, ARRAY['creating'], 'available',
        'heap_v1',
        jsonb_build_object('schema', 'flashback', 'relation', 'base_snapshot_990002'),
        'flashback.base_snapshot_990002', 1, NULL
    );

    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, stream_id, recovery_profile, state,
        boundary_kind, rel_oid_at_boundary, boundary_snapshot_id,
        boundary_time, boundary_lsn, valid_through_time, valid_through_lsn,
        activated_at
    ) VALUES (
        v_tracking_id, 2, v_stream_id, 'local_delta', 'active',
        'retire_resume_fixture', 'public.it_retire_resume'::regclass,
        v_succ_snapshot, v_base + interval '6 seconds', '0/600',
        v_base + interval '8 seconds', '0/700',
        clock_timestamp()
    );

    -- Policy B ignores heap-content drift, not physical identity/schema drift.
    -- The exception subtransaction rolls the ALTER back so the subsequent
    -- content-only drift can prove the successful path.
    v_failed := false;
    BEGIN
        ALTER TABLE flashback.base_snapshot_990001
            ADD COLUMN forged_column text;
        PERFORM flashback_resume_generation_retirement(v_pred_generation);
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM LIKE '%snapshot evidence changed before cleanup%' THEN
            v_failed := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'retirement accepted physical snapshot schema drift';
    END IF;
    IF to_regclass('flashback.base_snapshot_990001') IS NULL THEN
        RAISE EXCEPTION 'schema-drift rejection must not remove payload';
    END IF;

    -- Content drift in a payload we are about to discard must not block
    -- cleanup: deletion verifies identity and need, not content.
    INSERT INTO flashback.base_snapshot_990001 VALUES (999, 'content drift');

    IF flashback_resume_generation_retirement(v_pred_generation) <> 1 THEN
        RAISE EXCEPTION 'retirement resume did not report payload removal';
    END IF;
    IF to_regclass('flashback.base_snapshot_990001') IS NOT NULL THEN
        RAISE EXCEPTION 'resume left the retired payload table behind';
    END IF;
    IF (SELECT state FROM flashback.generation_payload_retirements
        WHERE generation_id = v_pred_generation) <> 'removed'
    THEN
        RAISE EXCEPTION 'resume did not finalize the retirement audit';
    END IF;
    IF (SELECT snapshot_row_count FROM flashback.generation_payload_retirements
        WHERE generation_id = v_pred_generation) <> 1
    THEN
        RAISE EXCEPTION 'intent-time forensic row count must stay immutable';
    END IF;
    IF (SELECT state FROM flashback.coverage_generations
        WHERE generation_id = v_pred_generation) <> 'retired'
    THEN
        RAISE EXCEPTION 'resume did not retire the predecessor generation';
    END IF;
END;
$test$;
