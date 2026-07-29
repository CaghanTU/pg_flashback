-- Coverage model schema contract. This validates inert metadata scaffolding;
-- it deliberately does not claim that restore/retention runtime uses it yet.
DROP TABLE IF EXISTS public.it_cov_contract CASCADE;
DROP TABLE IF EXISTS public.it_cov_replacement CASCADE;
DROP TABLE IF EXISTS public.it_cov_other CASCADE;
DROP TABLE IF EXISTS public.it_cov_retrack CASCADE;
DROP TABLE IF EXISTS flashback.it_cov_snapshot CASCADE;
DROP TABLE IF EXISTS flashback.it_cov_snapshot_2 CASCADE;
DROP TABLE IF EXISTS flashback.it_cov_snapshot_3 CASCADE;
DROP TABLE IF EXISTS flashback.it_cov_snapshot_other CASCADE;

CREATE TABLE public.it_cov_contract (id integer PRIMARY KEY, note text);
CREATE TABLE public.it_cov_replacement (id integer PRIMARY KEY, note text);
CREATE TABLE public.it_cov_other (id integer PRIMARY KEY, note text);
CREATE TABLE public.it_cov_retrack (id integer PRIMARY KEY, note text);
CREATE TABLE flashback.it_cov_snapshot AS TABLE public.it_cov_contract;
CREATE TABLE flashback.it_cov_snapshot_2 AS TABLE public.it_cov_contract;
CREATE TABLE flashback.it_cov_snapshot_3 AS TABLE public.it_cov_contract;
CREATE TABLE flashback.it_cov_snapshot_other AS TABLE public.it_cov_other;

DO $tv$
DECLARE
    v_tracking_id bigint;
    v_other_tracking_id bigint;
    v_retired_tracking_id bigint;
    v_retracked_tracking_id bigint;
    v_original_oid oid := 'public.it_cov_contract'::regclass;
    v_replacement_oid oid := 'public.it_cov_replacement'::regclass;
    v_other_oid oid := 'public.it_cov_other'::regclass;
    v_retrack_oid oid := 'public.it_cov_retrack'::regclass;
    v_stream_id bigint;
    v_other_stream_id bigint;
    v_other_snapshot_id bigint;
    v_snapshot_id bigint;
    v_snapshot_id_2 bigint;
    v_snapshot_id_3 bigint;
    v_generation_1 bigint;
    v_generation_2 bigint;
    v_other_generation bigint;
    v_gap_id bigint;
    v_boundary timestamptz := clock_timestamp();
    v_lsn pg_lsn := '0/1000';
    v_lsn_2 pg_lsn := '0/2000';
    v_config_count integer;
BEGIN
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table
    ) VALUES (
        v_original_oid, 'public', 'it_cov_contract',
        'flashback.it_cov_snapshot'
    )
    RETURNING tracking_id INTO v_tracking_id;

    IF v_tracking_id IS NULL THEN
        RAISE EXCEPTION 'tracking_id was not allocated';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM flashback.tracking_lifecycles
        WHERE tracking_id = v_tracking_id
          AND recovery_profile = 'local_delta'
          AND retired_at IS NULL
          AND initial_rel_oid = v_original_oid
    ) THEN
        RAISE EXCEPTION 'current binding did not create its immutable lifecycle parent';
    END IF;

    -- Untrack removes only the current binding, retires immutable audit, and a
    -- later track of the same relation/name must allocate a different ID.
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table
    ) VALUES (
        v_retrack_oid, 'public', 'it_cov_retrack', NULL
    ) RETURNING tracking_id INTO v_retired_tracking_id;

    DELETE FROM flashback.tracked_tables
    WHERE tracking_id = v_retired_tracking_id;

    IF NOT EXISTS (
        SELECT 1 FROM flashback.tracking_lifecycles
        WHERE tracking_id = v_retired_tracking_id
          AND retired_at IS NOT NULL
          AND retirement_reason = 'current_binding_removed'
          AND retired_rel_oid = v_retrack_oid
    ) THEN
        RAISE EXCEPTION 'untrack did not retain and retire lifecycle audit';
    END IF;

    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table
    ) VALUES (
        v_retrack_oid, 'public', 'it_cov_retrack', NULL
    ) RETURNING tracking_id INTO v_retracked_tracking_id;

    IF v_retracked_tracking_id = v_retired_tracking_id
       OR NOT EXISTS (
           SELECT 1 FROM flashback.tracking_lifecycles
           WHERE tracking_id = v_retracked_tracking_id
             AND retired_at IS NULL
       )
    THEN
        RAISE EXCEPTION 'retrack reused retired identity instead of creating a lifecycle';
    END IF;

    BEGIN
        DELETE FROM flashback.tracking_lifecycles
        WHERE tracking_id = v_retired_tracking_id;
        RAISE EXCEPTION 'immutable lifecycle audit was deletable';
    EXCEPTION WHEN integrity_constraint_violation THEN
        NULL;
    END;

    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table,
        recovery_profile
    ) VALUES (
        v_other_oid, 'public', 'it_cov_other', NULL, 'local_delta'
    ) RETURNING tracking_id INTO v_other_tracking_id;

    INSERT INTO flashback.capture_streams (
        database_oid, database_name, epoch_no, capture_mode, timeline_id, slot_name,
        plugin_name, state, valid_through_time, valid_through_lsn, activated_at
    ) VALUES (
        (SELECT oid FROM pg_database WHERE datname = current_database()),
        current_database(), 1, 'wal', 1, 'it_cov_slot', 'pg_flashback', 'active',
        v_boundary + interval '30 seconds', v_lsn, clock_timestamp()
    )
    RETURNING stream_id INTO v_stream_id;

    -- A second, distinct stream row is all this needs (used below only as
    -- an unrelated stream_id for the cross-stream FK rejection check); it
    -- must be created via a legal initial state (flashback_guard_capture_stream
    -- rejects a row inserted already-terminal, matching how
    -- flashback_internal_create_capture_stream itself never creates one).
    INSERT INTO flashback.capture_streams (
        database_oid, database_name, epoch_no, capture_mode, state,
        timeline_id, slot_name, plugin_name
    ) VALUES (
        (SELECT oid FROM pg_database WHERE datname = current_database()),
        current_database(), 97, 'wal', 'initializing',
        1, 'it_cov_slot_retired', 'pg_flashback'
    ) RETURNING stream_id INTO v_other_stream_id;

    -- Non-wal capture_mode rows are rejected by the WAL-only check constraint.
    BEGIN
        INSERT INTO flashback.capture_streams (
            database_oid, database_name, epoch_no, capture_mode, slot_name,
            plugin_name, state, activated_at, timeline_id
        ) VALUES (
            (SELECT oid FROM pg_database WHERE datname = current_database()),
            current_database(), 98, 'trigger', 'it_cov_slot_bad', 'pg_flashback',
            'active', clock_timestamp(), 1
        );
        RAISE EXCEPTION 'non-wal capture_mode stream was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;

    -- Only one active stream epoch may exist for this database.
    BEGIN
        INSERT INTO flashback.capture_streams (
            database_oid, database_name, epoch_no, capture_mode, state,
            activated_at, timeline_id, slot_name, plugin_name
        ) VALUES (
            (SELECT oid FROM pg_database WHERE datname = current_database()),
            current_database(), 2, 'wal', 'active', clock_timestamp(),
            1, 'it_cov_slot_2', 'pg_flashback'
        );
        RAISE EXCEPTION 'second active capture stream was accepted';
    EXCEPTION WHEN unique_violation THEN
        NULL;
    END;

    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn,
        schema_def, row_count, captured_at
    ) VALUES (
        v_original_oid, v_tracking_id, 'flashback.it_cov_snapshot', v_lsn,
        '{}'::jsonb, 0, v_boundary
    )
    RETURNING snapshot_id INTO v_snapshot_id;

    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn,
        schema_def, row_count, captured_at
    ) VALUES (
        v_original_oid, v_tracking_id, 'flashback.it_cov_snapshot_2', v_lsn_2,
        '{}'::jsonb, 0, v_boundary + interval '2 minutes'
    )
    RETURNING snapshot_id INTO v_snapshot_id_2;

    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn,
        schema_def, row_count, captured_at
    ) VALUES (
        v_original_oid, v_tracking_id, 'flashback.it_cov_snapshot_3', v_lsn_2,
        '{}'::jsonb, 0, v_boundary + interval '3 minutes'
    )
    RETURNING snapshot_id INTO v_snapshot_id_3;

    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, stream_id, recovery_profile, state,
        boundary_kind, rel_oid_at_boundary, boundary_snapshot_id,
        boundary_time, boundary_lsn, valid_through_time, valid_through_lsn,
        activated_at
    ) VALUES (
        v_tracking_id, 1, v_stream_id, 'local_delta', 'active',
        'track', v_original_oid, v_snapshot_id,
        v_boundary, v_lsn, v_boundary + interval '30 seconds', v_lsn,
        clock_timestamp()
    )
    RETURNING generation_id INTO v_generation_1;

    BEGIN
        DELETE FROM flashback.tracked_tables WHERE tracking_id = v_tracking_id;
        RAISE EXCEPTION 'current binding with an active generation was deletable';
    EXCEPTION WHEN integrity_constraint_violation THEN
        NULL;
    END;

    BEGIN
        UPDATE flashback.tracking_lifecycles
           SET initial_table_name = 'it_cov_renamed'
         WHERE tracking_id = v_tracking_id;
        RAISE EXCEPTION 'immutable lifecycle identity was mutable';
    EXCEPTION WHEN integrity_constraint_violation THEN
        NULL;
    END;

    -- v_other gets a plain active local_delta generation so the cross-tracking
    -- ownership checks below still exercise a real second lifecycle.
    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn,
        schema_def, row_count, captured_at
    ) VALUES (
        v_other_oid, v_other_tracking_id, 'flashback.it_cov_snapshot_other', v_lsn,
        '{}'::jsonb, 0, v_boundary
    ) RETURNING snapshot_id INTO v_other_snapshot_id;

    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, stream_id, recovery_profile, state,
        boundary_kind, rel_oid_at_boundary, boundary_snapshot_id,
        boundary_time, boundary_lsn, valid_through_time, valid_through_lsn,
        activated_at
    ) VALUES (
        v_other_tracking_id, 1, v_stream_id, 'local_delta', 'active',
        'track', v_other_oid, v_other_snapshot_id,
        v_boundary, v_lsn, v_boundary + interval '30 seconds', v_lsn,
        clock_timestamp()
    ) RETURNING generation_id INTO v_other_generation;

    IF (SELECT canonical_coordinate_kind
        FROM flashback.coverage_generations
        WHERE generation_id = v_generation_1) <> 'commit_lsn'
    THEN
        RAISE EXCEPTION 'local_delta canonical coordinate kind is incorrect';
    END IF;

    -- Payload identity is one tuple, not three independently trusted columns.
    BEGIN
        INSERT INTO flashback.delta_log (
            event_time, committed_at, event_type, table_name, rel_oid,
            tracking_id, generation_id, stream_id
        ) VALUES (
            v_boundary, v_boundary, 'INSERT', 'public.it_cov_contract',
            v_original_oid, v_tracking_id, v_generation_1, v_stream_id
        );
        RAISE EXCEPTION 'qualified WAL payload without COMMIT LSN was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;

    BEGIN
        INSERT INTO flashback.delta_log (
            event_time, committed_at, event_type, table_name, rel_oid,
            tracking_id, generation_id, stream_id, commit_lsn
        ) VALUES (
            v_boundary, v_boundary, 'INSERT', 'public.it_cov_contract',
            v_original_oid, v_tracking_id, v_generation_1,
            v_other_stream_id, v_lsn
        );
        SET CONSTRAINTS flashback.delta_log_generation_tracking_stream_fk IMMEDIATE;
        RAISE EXCEPTION 'cross-stream payload binding was accepted';
    EXCEPTION WHEN foreign_key_violation THEN
        SET CONSTRAINTS flashback.delta_log_generation_tracking_stream_fk DEFERRED;
    END;

    BEGIN
        INSERT INTO flashback.pending_wal_events (
            tracking_id, generation_id, stream_id, event_type, source_xid, event_lsn
        ) VALUES (
            v_tracking_id, v_generation_1, v_stream_id, 'INSERT', 1, v_lsn
        );
        RAISE EXCEPTION 'partial pending_wal coverage binding was accepted';
    EXCEPTION WHEN check_violation OR not_null_violation OR foreign_key_violation THEN
        NULL;
    END;

    -- Parent/source/re-anchor ownership is constrained by stable tracking ID.
    BEGIN
        INSERT INTO flashback.coverage_gaps (
            tracking_id, source_generation_id, reason, gap_start_time
        ) VALUES (
            v_tracking_id, v_other_generation, 'cross_tracking_reference',
            v_boundary + interval '1 minute'
        );
        SET CONSTRAINTS flashback.coverage_gaps_source_tracking_fk IMMEDIATE;
        RAISE EXCEPTION 'cross-tracking generation reference was accepted';
    EXCEPTION WHEN foreign_key_violation THEN
        SET CONSTRAINTS flashback.coverage_gaps_source_tracking_fk DEFERRED;
    END;

    -- Active uniqueness is a schema invariant, not an application convention.
    BEGIN
        INSERT INTO flashback.coverage_generations (
            tracking_id, generation_no, stream_id, recovery_profile, state,
            boundary_kind, rel_oid_at_boundary, boundary_snapshot_id,
            boundary_time, boundary_lsn, valid_through_lsn, activated_at
        ) VALUES (
            v_tracking_id, 99, v_stream_id, 'local_delta', 'active',
            'invalid-second-active', v_original_oid, v_snapshot_id_3,
            v_boundary, v_lsn, v_lsn,
            clock_timestamp()
        );
        RAISE EXCEPTION 'second active generation was accepted';
    EXCEPTION WHEN unique_violation THEN
        NULL;
    END;

    -- One physical base image can anchor only one exact generation boundary.
    BEGIN
        INSERT INTO flashback.coverage_generations (
            tracking_id, generation_no, parent_generation_id, stream_id,
            recovery_profile, state, boundary_kind, rel_oid_at_boundary,
            boundary_snapshot_id, boundary_marker
        ) VALUES (
            v_tracking_id, 98, v_generation_1, v_stream_id,
            'local_delta', 'building', 'invalid-snapshot-reuse', v_original_oid,
            v_snapshot_id, 'it_cov_invalid_snapshot_reuse'
        );
        RAISE EXCEPTION 'one boundary snapshot was accepted by two generations';
    EXCEPTION WHEN unique_violation THEN
        NULL;
    END;

    -- Snapshot identity and its exact boundary LSN are one proof tuple.
    BEGIN
        INSERT INTO flashback.coverage_generations (
            tracking_id, generation_no, stream_id, recovery_profile, state,
            boundary_kind, rel_oid_at_boundary, boundary_snapshot_id,
            boundary_time, boundary_lsn
        ) VALUES (
            v_tracking_id, 97, v_stream_id, 'local_delta', 'building',
            'invalid-snapshot-lsn', v_original_oid, v_snapshot_id_3,
            v_boundary, v_lsn
        );
        SET CONSTRAINTS flashback.coverage_generations_boundary_snapshot_tracking_fk IMMEDIATE;
        RAISE EXCEPTION 'snapshot with a mismatched boundary LSN was accepted';
    EXCEPTION WHEN foreign_key_violation THEN
        SET CONSTRAINTS flashback.coverage_generations_boundary_snapshot_tracking_fk DEFERRED;
    END;

    -- One pending building generation may coexist with the current active
    -- generation. Its own commit boundary is not knowable inside this XID.
    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, parent_generation_id, stream_id,
        recovery_profile, state, boundary_kind, rel_oid_at_boundary,
        boundary_snapshot_id, boundary_xid, boundary_marker
    ) VALUES (
        v_tracking_id, 2, v_generation_1, v_stream_id,
        'local_delta', 'building', 'maintenance', v_original_oid,
        v_snapshot_id_2, txid_current()::bigint, 'it_cov_pending_generation_2'
    )
    RETURNING generation_id INTO v_generation_2;

    BEGIN
        INSERT INTO flashback.coverage_generations (
            tracking_id, generation_no, stream_id, recovery_profile, state,
            boundary_kind, rel_oid_at_boundary, boundary_snapshot_id,
            boundary_marker
        ) VALUES (
            v_tracking_id, 3, v_stream_id, 'local_delta', 'building',
            'invalid-second-building', v_original_oid, v_snapshot_id_3,
            'it_cov_invalid_second_building'
        );
        RAISE EXCEPTION 'second building generation was accepted';
    EXCEPTION WHEN unique_violation THEN
        NULL;
    END;

    -- A qualified generation's watermark and boundary can never move backward
    -- or be rewritten to different evidence.
    BEGIN
        UPDATE flashback.coverage_generations
           SET valid_through_lsn = '0/0'
         WHERE generation_id = v_generation_1;
        RAISE EXCEPTION 'backward generation watermark was accepted';
    EXCEPTION WHEN integrity_constraint_violation THEN
        NULL;
    END;

    BEGIN
        UPDATE flashback.coverage_generations
           SET boundary_lsn = v_lsn_2
         WHERE generation_id = v_generation_1;
        RAISE EXCEPTION 'qualified generation boundary was mutable';
    EXCEPTION WHEN integrity_constraint_violation THEN
        NULL;
    END;

    INSERT INTO flashback.coverage_gaps (
        tracking_id, source_generation_id, reason,
        gap_start_time, lower_bound_inclusive, source_xid
    ) VALUES (
        v_tracking_id, v_generation_1, 'row_too_large',
        v_boundary + interval '1 minute', true, txid_current()::bigint
    )
    RETURNING gap_id INTO v_gap_id;

    UPDATE flashback.coverage_generations
       SET state = 'sealed',
           sealed_at = clock_timestamp(),
           superseded_before_time = v_boundary + interval '2 minutes',
           superseded_before_lsn = v_lsn_2
     WHERE generation_id = v_generation_1;
    UPDATE flashback.coverage_generations
       SET state = 'active',
           boundary_time = v_boundary + interval '2 minutes',
           boundary_lsn = v_lsn_2,
           valid_through_time = v_boundary + interval '2 minutes',
           valid_through_lsn = v_lsn_2,
           activated_at = clock_timestamp()
     WHERE generation_id = v_generation_2;

    -- Sealed backlog may drain monotonically to (but never past) the immutable
    -- applicability bound; the lifecycle cannot be reopened afterward.
    UPDATE flashback.coverage_generations
       SET valid_through_lsn = v_lsn_2
     WHERE generation_id = v_generation_1;
    BEGIN
        UPDATE flashback.coverage_generations
           SET state = 'active'
         WHERE generation_id = v_generation_1;
        RAISE EXCEPTION 'sealed generation was reactivated';
    EXCEPTION WHEN integrity_constraint_violation THEN
        NULL;
    END;

    IF NOT EXISTS (
        SELECT 1
        FROM flashback.coverage_generations
        WHERE generation_id = v_generation_2
          AND state = 'active'
          AND boundary_xid IS NOT NULL
          AND boundary_marker = 'it_cov_pending_generation_2'
          AND boundary_time IS NOT NULL
          AND boundary_lsn = v_lsn_2
    ) THEN
        RAISE EXCEPTION 'pending building generation was not qualified to an exact active boundary';
    END IF;

    UPDATE flashback.coverage_gaps
       SET gap_end_time = v_boundary + interval '2 minutes',
           reanchored_by_generation_id = v_generation_2,
           reanchored_at = clock_timestamp()
     WHERE gap_id = v_gap_id;

    IF NOT EXISTS (
        SELECT 1 FROM flashback.coverage_gaps
        WHERE gap_id = v_gap_id
          AND reanchored_by_generation_id = v_generation_2
          AND reason = 'row_too_large'
    ) THEN
        RAISE EXCEPTION 're-anchor erased or failed to preserve permanent gap';
    END IF;

    -- A snapshot artifact can never be removed at all: SnapshotStore's
    -- immutable-audit-trail guard (flashback_guard_snapshot_artifact)
    -- refuses every DELETE unconditionally, which is a strictly stronger
    -- invariant than (and now fires before) the FK that used to be the
    -- only thing stopping this specific referenced-while-in-use case.
    -- PG18 reports ON DELETE RESTRICT as restrict_violation; older majors
    -- use foreign_key_violation for that FK path, kept here as a fallback
    -- in case the guard trigger is ever bypassed by a future schema change.
    BEGIN
        DELETE FROM flashback.snapshots WHERE snapshot_id = v_snapshot_id;
        SET CONSTRAINTS flashback.coverage_generations_boundary_snapshot_tracking_fk IMMEDIATE;
        RAISE EXCEPTION 'referenced boundary snapshot was deletable';
    EXCEPTION WHEN integrity_constraint_violation OR foreign_key_violation OR restrict_violation THEN
        SET CONSTRAINTS flashback.coverage_generations_boundary_snapshot_tracking_fk DEFERRED;
    END;

    -- Shadow swap changes rel_oid, never the stable tracking identity.
    UPDATE flashback.tracked_tables
       SET rel_oid = v_replacement_oid
     WHERE tracking_id = v_tracking_id;
    IF (SELECT tracking_id FROM flashback.tracked_tables
        WHERE rel_oid = v_replacement_oid) <> v_tracking_id THEN
        RAISE EXCEPTION 'tracking identity changed with rel_oid';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'flashback'
          AND c.relname IN (
              'tracking_lifecycles', 'capture_streams',
              'coverage_generations', 'coverage_gaps',
              'pending_wal_events', 'generation_payload_retirements'
          )
          AND c.relpersistence <> 'p'
    ) THEN
        RAISE EXCEPTION 'coverage metadata must be LOGGED';
    END IF;

    IF EXISTS (
        SELECT required.table_name, required.column_name
        FROM (VALUES
            ('tracking_lifecycles', 'tracking_id'),
            ('tracking_lifecycles', 'recovery_profile'),
            ('tracking_lifecycles', 'retired_at'),
            ('tracking_lifecycles', 'retirement_reason'),
            ('capture_streams', 'timeline_id'),
            ('delta_log', 'tracking_id'),
            ('delta_log', 'generation_id'),
            ('delta_log', 'stream_id'),
            ('delta_log', 'commit_lsn'),
            ('snapshots', 'tracking_id'),
            ('snapshots', 'payload_state'),
            ('snapshots', 'retired_at'),
            ('schema_versions', 'tracking_id'),
            ('schema_versions', 'generation_id'),
            ('schema_versions', 'stream_id'),
            ('schema_versions', 'source_xid'),
            ('schema_versions', 'committed_at'),
            ('schema_versions', 'commit_lsn'),
            ('schema_versions', 'schema_def'),
            ('generation_payload_retirements', 'tracking_id'),
            ('generation_payload_retirements', 'generation_id'),
            ('generation_payload_retirements', 'snapshot_id'),
            ('generation_payload_retirements', 'intent_txid'),
            ('generation_payload_retirements', 'state'),
            ('coverage_generations', 'boundary_xid'),
            ('coverage_generations', 'boundary_marker'),
            ('coverage_generations', 'canonical_coordinate_kind'),
            ('coverage_generations', 'superseded_before_time'),
            ('coverage_generations', 'superseded_before_lsn'),
            ('pending_wal_events', 'tracking_id'),
            ('pending_wal_events', 'generation_id'),
            ('pending_wal_events', 'stream_id'),
            ('pending_wal_events', 'source_xid'),
            ('pending_wal_events', 'event_lsn')
        ) AS required(table_name, column_name)
        LEFT JOIN information_schema.columns c
          ON c.table_schema = 'flashback'
         AND c.table_name = required.table_name
         AND c.column_name = required.column_name
        WHERE c.column_name IS NULL
    ) THEN
        RAISE EXCEPTION 'coverage payload-binding scaffold is incomplete';
    END IF;

    IF (
        SELECT count(*)
        FROM pg_constraint
        WHERE confrelid = 'flashback.tracking_lifecycles'::regclass
          AND conrelid IN (
              'flashback.tracked_tables'::regclass,
              'flashback.delta_log'::regclass,
              'flashback.snapshots'::regclass,
              'flashback.coverage_generations'::regclass,
              'flashback.coverage_gaps'::regclass,
              'flashback.schema_versions'::regclass
          )
          AND contype = 'f'
          AND condeferrable
          AND condeferred
    ) <> 6 THEN
        RAISE EXCEPTION 'lifecycle children are not bound to immutable parent';
    END IF;

    IF (
        SELECT count(*)
        FROM pg_constraint
        WHERE conrelid = 'flashback.generation_payload_retirements'::regclass
          AND conname IN (
              'generation_payload_retirements_generation_tracking_fk',
              'generation_payload_retirements_snapshot_tracking_fk'
          )
          AND contype = 'f'
          AND condeferrable
          AND condeferred
    ) <> 2 THEN
        RAISE EXCEPTION 'retirement audit is not bound to immutable generation payload identity';
    END IF;

    IF (
        SELECT count(*)
        FROM pg_constraint
        WHERE conrelid IN (
              'flashback.delta_log'::regclass,
              'flashback.pending_wal_events'::regclass
          )
          AND conname IN (
              'delta_log_generation_tracking_stream_fk',
              'pending_wal_events_generation_fk'
          )
          AND contype = 'f'
          AND condeferrable
          AND condeferred
    ) <> 2 THEN
        RAISE EXCEPTION 'payload tuples are not bound to generation/stream identity';
    END IF;

    IF (
        SELECT count(*)
        FROM pg_constraint
        WHERE conrelid = 'flashback.schema_versions'::regclass
          AND conname IN (
              'schema_versions_tracking_fk',
              'schema_versions_generation_tracking_fk',
              'schema_versions_generation_tracking_stream_fk',
              'schema_versions_stream_fk'
          )
          AND contype = 'f'
          AND condeferrable
          AND condeferred
    ) <> 4 THEN
        RAISE EXCEPTION 'schema-version commit qualification FKs are not initially deferred';
    END IF;

    SELECT count(*) INTO v_config_count
    FROM pg_extension e
    CROSS JOIN LATERAL unnest(e.extconfig) AS config_oid
    WHERE e.extname = 'pg_flashback';
    IF v_config_count <> 0 THEN
        RAISE EXCEPTION 'tracking/recovery state must not survive logical config dump';
    END IF;

    IF NOT has_table_privilege('pg_monitor', 'flashback.tracking_lifecycles', 'SELECT')
       OR NOT has_table_privilege('pg_monitor', 'flashback.capture_streams', 'SELECT')
       OR NOT has_table_privilege('pg_monitor', 'flashback.coverage_generations', 'SELECT')
       OR NOT has_table_privilege('pg_monitor', 'flashback.coverage_gaps', 'SELECT')
       OR NOT has_table_privilege('pg_monitor', 'flashback.generation_payload_retirements', 'SELECT')
    THEN
        RAISE EXCEPTION 'pg_monitor cannot inspect coverage metadata';
    END IF;
END;
$tv$;
