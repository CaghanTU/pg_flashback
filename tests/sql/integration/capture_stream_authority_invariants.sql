-- A6: flashback_guard_capture_stream is the trigger-level backstop for
-- flashback.capture_streams, mirroring flashback_guard_coverage_generation
-- (which already existed). Before this trigger, capture_streams had a
-- row-shape CHECK constraint but nothing independently enforcing the legal
-- transition graph, identity immutability, or monotonic LSN progress across
-- an UPDATE -- any future direct-UPDATE bug on this table had no backstop
-- beyond the CAS function's own application-level logic. This test drives
-- flashback.capture_streams directly (bypassing the CAS functions in
-- state_authority.sql) to prove the trigger itself is the one rejecting each
-- violation, independent of any caller's own care.
DO $tv$
DECLARE
    v_stream_id bigint;
    v_db_oid oid := (SELECT oid FROM pg_database WHERE datname = current_database());
    v_failed boolean;
    v_err text;
BEGIN
    -- INSERT must be created initializing or active, never a terminal state.
    v_failed := false;
    BEGIN
        INSERT INTO flashback.capture_streams (
            database_oid, database_name, epoch_no, capture_mode, timeline_id,
            slot_name, plugin_name, state, invalidated_at, invalidation_reason
        ) VALUES (
            v_db_oid, current_database(), 901, 'wal', 1,
            'it_a6_bad_initial', 'pg_flashback', 'broken', clock_timestamp(), 'bogus'
        );
    EXCEPTION WHEN integrity_constraint_violation THEN
        v_failed := true;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'capture stream was created directly in a terminal state';
    END IF;

    INSERT INTO flashback.capture_streams (
        database_oid, database_name, epoch_no, capture_mode, timeline_id,
        slot_name, plugin_name, state, activated_at,
        valid_through_lsn, confirmed_flush_lsn, restart_lsn
    ) VALUES (
        v_db_oid, current_database(), 902, 'wal', 1,
        'it_a6_stream', 'pg_flashback', 'active', clock_timestamp(),
        '0/2000'::pg_lsn, '0/2000'::pg_lsn, '0/2000'::pg_lsn
    ) RETURNING stream_id INTO v_stream_id;

    -- Identity fields (slot_name, database_oid, epoch_no, ...) are immutable.
    v_failed := false;
    BEGIN
        UPDATE flashback.capture_streams
           SET slot_name = 'it_a6_renamed'
         WHERE stream_id = v_stream_id;
    EXCEPTION WHEN integrity_constraint_violation THEN
        v_failed := true;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'capture stream slot_name identity was mutated';
    END IF;

    -- Illegal transition: active -> initializing has no edge in the graph.
    v_failed := false;
    BEGIN
        UPDATE flashback.capture_streams
           SET state = 'initializing'
         WHERE stream_id = v_stream_id;
    EXCEPTION WHEN integrity_constraint_violation THEN
        v_failed := true;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'illegal active -> initializing capture stream transition was accepted';
    END IF;

    -- Monotonic LSN fields cannot move backward.
    v_failed := false;
    BEGIN
        UPDATE flashback.capture_streams
           SET valid_through_lsn = '0/1000'::pg_lsn
         WHERE stream_id = v_stream_id;
    EXCEPTION WHEN integrity_constraint_violation THEN
        v_failed := true;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'capture stream valid_through_lsn moved backward';
    END IF;

    v_failed := false;
    BEGIN
        UPDATE flashback.capture_streams
           SET confirmed_flush_lsn = '0/1000'::pg_lsn
         WHERE stream_id = v_stream_id;
    EXCEPTION WHEN integrity_constraint_violation THEN
        v_failed := true;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'capture stream confirmed_flush_lsn moved backward';
    END IF;

    -- Legal transition (active -> broken) via the real CAS function still
    -- works, and required fields may be set as part of that transition.
    PERFORM flashback_internal_transition_capture_stream(
        v_stream_id, ARRAY['active'], 'broken', 'it_a6_test_break', '{}'::jsonb
    );
    IF NOT EXISTS (
        SELECT 1 FROM flashback.capture_streams
        WHERE stream_id = v_stream_id AND state = 'broken'
          AND invalidation_reason = 'it_a6_test_break'
    ) THEN
        RAISE EXCEPTION 'legal active -> broken transition via the CAS function was rejected';
    END IF;

    -- Once broken (no further transition), details are frozen -- a bare
    -- same-state UPDATE is exactly the kind of state-authority bypass this
    -- trigger exists to catch (flashback_mark_capture_stream_broken folds
    -- its own diagnostic counts into the transition's own details payload
    -- for precisely this reason).
    v_failed := false;
    BEGIN
        UPDATE flashback.capture_streams
           SET details = details || '{"tampered": true}'::jsonb
         WHERE stream_id = v_stream_id;
    EXCEPTION WHEN integrity_constraint_violation THEN
        v_failed := true;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'details on an already-broken capture stream were mutated without a transition';
    END IF;

    -- Illegal transition: broken -> active has no edge in the graph.
    v_failed := false;
    BEGIN
        UPDATE flashback.capture_streams
           SET state = 'active'
         WHERE stream_id = v_stream_id;
    EXCEPTION WHEN integrity_constraint_violation THEN
        v_failed := true;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'illegal broken -> active capture stream transition was accepted';
    END IF;

    -- Legal terminal transition (broken -> retired), then fully immutable.
    PERFORM flashback_internal_transition_capture_stream(
        v_stream_id, ARRAY['broken'], 'retired', NULL, '{}'::jsonb
    );

    v_failed := false;
    BEGIN
        UPDATE flashback.capture_streams
           SET slot_name = slot_name
         WHERE stream_id = v_stream_id;
        RAISE EXCEPTION 'no-op update on a retired stream should still hit the terminal-state guard first';
    EXCEPTION WHEN integrity_constraint_violation THEN
        NULL;
    END;

    v_failed := false;
    BEGIN
        DELETE FROM flashback.capture_streams WHERE stream_id = v_stream_id;
    EXCEPTION WHEN integrity_constraint_violation THEN
        v_failed := true;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'capture stream row was deleted; audit rows must be immutable';
    END IF;
END;
$tv$;
