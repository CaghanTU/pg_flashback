-- Coverage model schema contract. This validates inert metadata scaffolding;
-- it deliberately does not claim that restore/retention runtime uses it yet.
DROP TABLE IF EXISTS public.it_cov_contract CASCADE;
DROP TABLE IF EXISTS public.it_cov_replacement CASCADE;
DROP TABLE IF EXISTS public.it_cov_other CASCADE;
DROP TABLE IF EXISTS public.it_cov_retrack CASCADE;
DROP TABLE IF EXISTS flashback.it_cov_snapshot CASCADE;
DROP TABLE IF EXISTS flashback.it_cov_snapshot_2 CASCADE;
DROP TABLE IF EXISTS flashback.it_cov_snapshot_3 CASCADE;

CREATE TABLE public.it_cov_contract (id integer PRIMARY KEY, note text);
CREATE TABLE public.it_cov_replacement (id integer PRIMARY KEY, note text);
CREATE TABLE public.it_cov_other (id integer PRIMARY KEY, note text);
CREATE TABLE public.it_cov_retrack (id integer PRIMARY KEY, note text);
CREATE TABLE flashback.it_cov_snapshot AS TABLE public.it_cov_contract;
CREATE TABLE flashback.it_cov_snapshot_2 AS TABLE public.it_cov_contract;
CREATE TABLE flashback.it_cov_snapshot_3 AS TABLE public.it_cov_contract;

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
    v_backup_anchor_id bigint;
    v_snapshot_id bigint;
    v_snapshot_id_2 bigint;
    v_snapshot_id_3 bigint;
    v_generation_1 bigint;
    v_generation_2 bigint;
    v_other_generation bigint;
    v_other_pending_generation bigint;
    v_gap_id bigint;
    v_boundary timestamptz := clock_timestamp();
    v_lsn pg_lsn := '0/1000';
    v_lsn_2 pg_lsn := '0/2000';
    v_backup_marker_lsn pg_lsn := '0/100';
    v_backup_start_lsn pg_lsn := '0/200';
    v_backup_stop_lsn pg_lsn := '0/400';
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
        v_other_oid, 'public', 'it_cov_other', NULL, 'backup'
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

    INSERT INTO flashback.capture_streams (
        database_oid, database_name, epoch_no, capture_mode, state, retired_at
    ) VALUES (
        (SELECT oid FROM pg_database WHERE datname = current_database()),
        current_database(), 97, 'trigger', 'retired', clock_timestamp()
    ) RETURNING stream_id INTO v_other_stream_id;

    -- Trigger epochs cannot carry logical-slot/plugin identity.
    BEGIN
        INSERT INTO flashback.capture_streams (
            database_oid, database_name, epoch_no, capture_mode, slot_name,
            plugin_name, state, activated_at
        ) VALUES (
            (SELECT oid FROM pg_database WHERE datname = current_database()),
            current_database(), 98, 'trigger', NULL, 'pg_flashback',
            'active', clock_timestamp()
        );
        RAISE EXCEPTION 'trigger stream with a plugin identity was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;

    -- Only one active stream epoch may exist for this database.
    BEGIN
        INSERT INTO flashback.capture_streams (
            database_oid, database_name, epoch_no, capture_mode, state,
            activated_at
        ) VALUES (
            (SELECT oid FROM pg_database WHERE datname = current_database()),
            current_database(), 2, 'trigger', 'active', clock_timestamp()
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
           SET recovery_profile = 'backup'
         WHERE tracking_id = v_tracking_id;
        RAISE EXCEPTION 'immutable lifecycle profile was mutable';
    EXCEPTION WHEN integrity_constraint_violation THEN
        NULL;
    END;

    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, recovery_profile, state,
        boundary_kind, rel_oid_at_boundary, boundary_marker
    ) VALUES (
        v_other_tracking_id, 2, 'backup', 'building',
        'post_restore', v_other_oid, 'it_cov_pending_backup_generation'
    ) RETURNING generation_id INTO v_other_pending_generation;

    IF NOT EXISTS (
        SELECT 1
        FROM flashback.coverage_generations
        WHERE generation_id = v_other_pending_generation
          AND state = 'building'
          AND boundary_marker = 'it_cov_pending_backup_generation'
          AND boundary_lsn IS NULL
    ) THEN
        RAISE EXCEPTION 'pending backup generation without a stop LSN was rejected';
    END IF;

    -- A pending backup boundary may exist without its stop LSN, but it cannot
    -- be published as active until that exact LSN has been qualified.
    BEGIN
        UPDATE flashback.coverage_generations
           SET state = 'active',
               boundary_time = v_boundary,
               activated_at = clock_timestamp()
         WHERE generation_id = v_other_pending_generation;
        RAISE EXCEPTION 'active backup generation without a boundary LSN was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;

    -- A verified full backup must begin/checkpoint strictly after its durable
    -- tracking marker and carries immutable repository/cluster identity.
    BEGIN
        INSERT INTO flashback.backup_anchors (
            tracking_id, helper_profile, repository_key, stanza, backup_label,
            backup_type, database_system_identifier, timeline_id,
            manifest_reference, manifest_sha256, tracking_marker_lsn,
            backup_start_lsn, backup_stop_lsn, verified_at
        ) VALUES (
            v_other_tracking_id, 'it_helper', 'repo1', 'it_stanza',
            '20260716-000000F-invalid', 'full', 123456789, 1,
            'repo1/it_stanza/invalid/backup.manifest', repeat('a', 64),
            v_backup_marker_lsn, v_backup_marker_lsn,
            v_backup_stop_lsn, clock_timestamp()
        );
        RAISE EXCEPTION 'overlapping/pre-marker backup anchor was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;

    INSERT INTO flashback.backup_anchors (
        tracking_id, helper_profile, repository_key, stanza, backup_label,
        backup_type, database_system_identifier, timeline_id,
        manifest_reference, manifest_sha256, tracking_marker_lsn,
        backup_start_lsn, backup_stop_lsn, verified_at
    ) VALUES (
        v_other_tracking_id, 'it_helper', 'repo1', 'it_stanza',
        '20260716-000000F', 'full', 123456789, 1,
        'repo1/it_stanza/20260716-000000F/backup.manifest', repeat('b', 64),
        v_backup_marker_lsn, v_backup_start_lsn,
        v_backup_stop_lsn, clock_timestamp()
    ) RETURNING backup_anchor_id INTO v_backup_anchor_id;

    BEGIN
        UPDATE flashback.backup_anchors
           SET manifest_sha256 = repeat('c', 64)
         WHERE backup_anchor_id = v_backup_anchor_id;
        RAISE EXCEPTION 'verified backup anchor was mutable';
    EXCEPTION WHEN integrity_constraint_violation THEN
        NULL;
    END;

    BEGIN
        INSERT INTO flashback.coverage_generations (
            tracking_id, generation_no, recovery_profile, state,
            boundary_kind, rel_oid_at_boundary, backup_anchor_id,
            boundary_time, boundary_lsn, valid_through_lsn, activated_at
        ) VALUES (
            v_other_tracking_id, 97, 'backup', 'active',
            'invalid-backup-stop', v_other_oid, v_backup_anchor_id,
            v_boundary, '0/401'::pg_lsn, '0/401'::pg_lsn,
            clock_timestamp()
        );
        SET CONSTRAINTS flashback.coverage_generations_backup_anchor_tracking_lsn_fk IMMEDIATE;
        RAISE EXCEPTION 'backup generation accepted a stop LSN outside its anchor';
    EXCEPTION WHEN foreign_key_violation THEN
        SET CONSTRAINTS flashback.coverage_generations_backup_anchor_tracking_lsn_fk DEFERRED;
    END;

    INSERT INTO flashback.coverage_generations (
        tracking_id, generation_no, recovery_profile, state,
        boundary_kind, rel_oid_at_boundary, backup_anchor_id,
        boundary_time, boundary_lsn, valid_through_lsn, activated_at
    ) VALUES (
        v_other_tracking_id, 1, 'backup', 'active',
        'verified_full_backup', v_other_oid, v_backup_anchor_id,
        v_boundary, v_backup_stop_lsn, v_backup_stop_lsn, clock_timestamp()
    ) RETURNING generation_id INTO v_other_generation;

    IF (SELECT canonical_coordinate_kind
        FROM flashback.coverage_generations
        WHERE generation_id = v_other_generation) <> 'physical_lsn'
       OR (SELECT canonical_coordinate_kind
           FROM flashback.coverage_generations
           WHERE generation_id = v_generation_1) <> 'commit_lsn'
    THEN
        RAISE EXCEPTION 'profile canonical coordinate kind is incorrect';
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
        INSERT INTO flashback.staging_events (
            rel_oid, tracking_id, event_type, table_name
        ) VALUES (
            v_original_oid, v_tracking_id, 'INSERT', 'public.it_cov_contract'
        );
        RAISE EXCEPTION 'partial staging coverage binding was accepted';
    EXCEPTION WHEN check_violation THEN
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

    -- A boundary snapshot cannot be removed while generation metadata uses it.
    BEGIN
        DELETE FROM flashback.snapshots WHERE snapshot_id = v_snapshot_id;
        SET CONSTRAINTS flashback.coverage_generations_boundary_snapshot_tracking_fk IMMEDIATE;
        RAISE EXCEPTION 'referenced boundary snapshot was deletable';
    -- PG18 reports ON DELETE RESTRICT as restrict_violation; older majors use
    -- foreign_key_violation for this path.
    EXCEPTION WHEN foreign_key_violation OR restrict_violation THEN
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
              'backup_anchors', 'coverage_generations', 'coverage_gaps',
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
            ('backup_anchors', 'backup_anchor_id'),
            ('backup_anchors', 'backup_label'),
            ('backup_anchors', 'database_system_identifier'),
            ('backup_anchors', 'timeline_id'),
            ('backup_anchors', 'manifest_sha256'),
            ('backup_anchors', 'tracking_marker_lsn'),
            ('backup_anchors', 'backup_start_lsn'),
            ('backup_anchors', 'backup_stop_lsn'),
            ('capture_streams', 'timeline_id'),
            ('delta_log', 'tracking_id'),
            ('delta_log', 'generation_id'),
            ('delta_log', 'stream_id'),
            ('delta_log', 'commit_lsn'),
            ('staging_events', 'tracking_id'),
            ('staging_events', 'generation_id'),
            ('staging_events', 'stream_id'),
            ('snapshots', 'tracking_id'),
            ('snapshots', 'payload_state'),
            ('snapshots', 'retired_at'),
            ('schema_versions', 'tracking_id'),
            ('schema_versions', 'generation_id'),
            ('schema_versions', 'stream_id'),
            ('schema_versions', 'source_xid'),
            ('schema_versions', 'committed_at'),
            ('schema_versions', 'commit_lsn'),
            ('backup_restore_requests', 'tracking_id'),
            ('backup_restore_requests', 'generation_id'),
            ('generation_payload_retirements', 'tracking_id'),
            ('generation_payload_retirements', 'generation_id'),
            ('generation_payload_retirements', 'snapshot_id'),
            ('generation_payload_retirements', 'intent_txid'),
            ('generation_payload_retirements', 'state'),
            ('coverage_generations', 'boundary_xid'),
            ('coverage_generations', 'boundary_marker'),
            ('coverage_generations', 'backup_anchor_id'),
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
              'flashback.staging_events'::regclass,
              'flashback.snapshots'::regclass,
              'flashback.backup_anchors'::regclass,
              'flashback.coverage_generations'::regclass,
              'flashback.coverage_gaps'::regclass,
              'flashback.schema_versions'::regclass,
              'flashback.backup_restore_requests'::regclass
          )
          AND contype = 'f'
          AND condeferrable
          AND condeferred
    ) <> 9 THEN
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
              'flashback.staging_events'::regclass,
              'flashback.pending_wal_events'::regclass
          )
          AND conname IN (
              'delta_log_generation_tracking_stream_fk',
              'staging_events_generation_tracking_stream_fk',
              'pending_wal_events_generation_fk'
          )
          AND contype = 'f'
          AND condeferrable
          AND condeferred
    ) <> 3 THEN
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
       OR NOT has_table_privilege('pg_monitor', 'flashback.backup_anchors', 'SELECT')
       OR NOT has_table_privilege('pg_monitor', 'flashback.coverage_generations', 'SELECT')
       OR NOT has_table_privilege('pg_monitor', 'flashback.coverage_gaps', 'SELECT')
       OR NOT has_table_privilege('pg_monitor', 'flashback.generation_payload_retirements', 'SELECT')
    THEN
        RAISE EXCEPTION 'pg_monitor cannot inspect coverage metadata';
    END IF;
END;
$tv$;
