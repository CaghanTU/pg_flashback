-- Step 9: flashback_doctor()'s aggregate tracked_lifecycle_health status
-- for the 'capturing' pending state, isolated in its own transaction.
--
-- flashback_doctor() aggregates flashback_health() across every tracked
-- table in the database into one status per check_name -- unlike
-- capturing_state_authority.sql (which deliberately leaves several
-- generations in states -- an aborted-only lifecycle among them -- that
-- would independently contaminate that same aggregate), this file keeps
-- exactly the fixtures each assertion needs, adding each new tracked_tables
-- row only immediately before it is driven straight into a generation --
-- a generation-less tracked_tables row is itself already unhealthy
-- ('reanchor_recommended': "no eligible coverage generation"), so even a
-- brief idle window would contaminate the very aggregate under test.
--
-- Generation age plays no role in this file's classification. A pending
-- generation of any age is 'warning' until something with real evidence
-- (an open gap, a broken/lost slot, an explicit operation-failure record,
-- a missing/corrupt artifact result, an expired heartbeat/lease) says
-- otherwise -- none of which exist yet for this parentless lifecycle (that
-- is the orchestration/reconciler phase, not this stage), so this file has
-- nothing to construct a genuine 'error' fixture from. It proves only what
-- is provable now: pending stays 'warning' regardless of how many pending
-- generations exist, and never 'ok'/'healthy'.
DO $tv$
DECLARE
    v_db oid := (SELECT oid FROM pg_database WHERE datname = current_database());
    v_stream bigint;
    v_tracking bigint;
    v_tracking2 bigint;
    v_rel oid;
    v_rel2 oid;
    v_row RECORD;
    v_row2 RECORD;
    v_bind RECORD;
    v_bind2 RECORD;
    v_gen_row flashback.coverage_generations%ROWTYPE;
    v_doctor_status text;
    v_health text;
    v_action text;
BEGIN
    CREATE TABLE IF NOT EXISTS public.it_capturing_doctor (id int PRIMARY KEY);
    v_rel := 'public.it_capturing_doctor'::regclass;

    v_stream := public.flashback_internal_create_capture_stream(
        p_database_oid => v_db, p_initial_state => 'active',
        p_slot_name => 'it_capturing_doctor_slot', p_plugin_name => 'pg_flashback_decoder'
    );

    -- Immediately followed by reservation: no generation-less window.
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile
    ) VALUES (v_rel, 'public', 'it_capturing_doctor', NULL, 'local_delta')
    RETURNING tracking_id INTO v_tracking;
    SELECT * INTO v_row
    FROM public.flashback_internal_reserve_online_generation(
        p_tracking_id => v_tracking, p_rel_oid => v_rel,
        p_stream_id => v_stream, p_generation_no => 1,
        p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
        p_operation_nonce => 930001
    );

    -- Drive to 'capturing': boundary bound + resolved.
    SELECT * INTO v_bind
    FROM public.flashback_internal_bind_online_boundary(
        v_row.generation_id, v_tracking, v_row.snapshot_id, v_rel
    );
    INSERT INTO flashback.capture_commits(stream_id, commit_lsn, source_xid, committed_at)
    VALUES (v_stream, '0/10000'::pg_lsn, v_bind.boundary_xid, TIMESTAMPTZ '2024-01-01 00:00:10+00');
    PERFORM public.flashback_internal_snapshot_refine_boundary(
        v_row.snapshot_id, v_tracking, v_row.generation_id, v_stream,
        '0/10000'::pg_lsn, TIMESTAMPTZ '2024-01-01 00:00:10+00'
    );
    PERFORM public.flashback_internal_transition_coverage_generation(
        v_row.generation_id, v_tracking, 'building', 'capturing', 'boundary_commit_observed',
        '0/10000'::pg_lsn, TIMESTAMPTZ '2024-01-01 00:00:10+00', NULL, NULL, NULL, NULL, '{}'::jsonb
    );
    SELECT * INTO v_gen_row FROM flashback.coverage_generations WHERE generation_id = v_row.generation_id;
    IF v_gen_row.state IS DISTINCT FROM 'capturing' THEN
        RAISE EXCEPTION 'fixture setup failed: expected capturing, got %', v_gen_row.state;
    END IF;

    -- Assertion 3: this capturing lifecycle is the sole tracked table so
    -- far -- doctor must be 'warning' (a normally-progressing pending
    -- state; a legitimate large external copy can run well past any
    -- short fixed window, so nothing here is age-gated), never the
    -- generic 'error' a genuine fault (open gap / broken slot / explicit
    -- operation failure / missing-corrupt artifact / expired heartbeat)
    -- would report, and never 'ok' (not yet activated).
    SELECT status INTO v_doctor_status
    FROM flashback_doctor() WHERE check_name = 'tracked_lifecycle_health';
    IF v_doctor_status IS DISTINCT FROM 'warning' THEN
        RAISE EXCEPTION 'assertion 3 failed: a capturing-only lifecycle must be doctor warning, got %', v_doctor_status;
    END IF;
    SELECT h.health INTO v_health FROM flashback_health() h WHERE h.tracking_id = v_tracking;
    IF v_health = 'healthy' THEN
        RAISE EXCEPTION 'assertion 3 failed: capturing-only lifecycle must never report row-level healthy either';
    END IF;

    -- Assertion 4 ("active after verified publication returns
    -- healthy/none") is NOT proven anywhere in this pgrx harness, and no
    -- test here or elsewhere in this stage may be cited as proving it.
    -- flashback_health()'s slot_lost branch (health_runtime.sql,
    -- `COALESCE(slot.wal_status, '') = 'missing' AND rec.stream_id IS NOT
    -- NULL` -- rec.stream_id only becomes non-NULL once a generation is
    -- active) fires for any row with an active generation whose stream
    -- has no real, non-missing PostgreSQL replication slot. No
    -- sql_test!-registered integration file can construct one
    -- (pg_create_logical_replication_slot refuses once its session has
    -- performed any write, and _common_setup.sql's own TRUNCATE already
    -- is one before this file's DO block even starts). A bare #[pg_test]
    -- Rust function was also tried, using the real-slot-as-literal-first-
    -- statement technique the WAL-gap proof elsewhere in this stage
    -- established works for its own, different purpose -- it did not
    -- reliably reproduce here, for reasons this investigation could not
    -- fully isolate, and was removed rather than left as an unreliable
    -- test. active -> healthy/ok remains a mandatory assertion for the
    -- later real-decoder, real-cluster qualification stage (stage 7/8),
    -- not something this commit demonstrates.

    -- A second, independent capturing lifecycle -- proving doctor stays
    -- 'warning' (not 'error') with more than one pending generation on
    -- record, and that the first fixture's own per-row health is
    -- unaffected by the second one's mere existence.
    CREATE TABLE IF NOT EXISTS public.it_capturing_doctor_2 (id int PRIMARY KEY);
    v_rel2 := 'public.it_capturing_doctor_2'::regclass;
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile
    ) VALUES (v_rel2, 'public', 'it_capturing_doctor_2', NULL, 'local_delta')
    RETURNING tracking_id INTO v_tracking2;
    SELECT * INTO v_row2
    FROM public.flashback_internal_reserve_online_generation(
        p_tracking_id => v_tracking2, p_rel_oid => v_rel2,
        p_stream_id => v_stream, p_generation_no => 1,
        p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
        p_operation_nonce => 930002
    );
    SELECT * INTO v_bind2
    FROM public.flashback_internal_bind_online_boundary(
        v_row2.generation_id, v_tracking2, v_row2.snapshot_id, v_rel2
    );
    INSERT INTO flashback.capture_commits(stream_id, commit_lsn, source_xid, committed_at)
    VALUES (v_stream, '0/20000'::pg_lsn, v_bind2.boundary_xid, TIMESTAMPTZ '2024-01-01 00:00:20+00');
    PERFORM public.flashback_internal_snapshot_refine_boundary(
        v_row2.snapshot_id, v_tracking2, v_row2.generation_id, v_stream,
        '0/20000'::pg_lsn, TIMESTAMPTZ '2024-01-01 00:00:20+00'
    );
    PERFORM public.flashback_internal_transition_coverage_generation(
        v_row2.generation_id, v_tracking2, 'building', 'capturing', 'boundary_commit_observed',
        '0/20000'::pg_lsn, TIMESTAMPTZ '2024-01-01 00:00:20+00', NULL, NULL, NULL, NULL, '{}'::jsonb
    );

    SELECT status INTO v_doctor_status
    FROM flashback_doctor() WHERE check_name = 'tracked_lifecycle_health';
    IF v_doctor_status IS DISTINCT FROM 'warning' THEN
        RAISE EXCEPTION 'assertion (two pending) failed: two capturing lifecycles must still be doctor warning, got %', v_doctor_status;
    END IF;
    SELECT h.health, h.recommended_action INTO v_health, v_action
    FROM flashback_health() h WHERE h.tracking_id = v_tracking;
    IF v_health IS DISTINCT FROM 'maintenance_required'
       OR v_action IS DISTINCT FROM 'wait_for_external_snapshot_publish'
    THEN
        RAISE EXCEPTION 'assertion (two pending) failed: the first fixture must be unaffected by the second one''s existence, got health=%, action=%',
            v_health, v_action;
    END IF;
    SELECT h.health, h.recommended_action INTO v_health, v_action
    FROM flashback_health() h WHERE h.tracking_id = v_tracking2;
    IF v_health IS DISTINCT FROM 'maintenance_required'
       OR v_action IS DISTINCT FROM 'wait_for_external_snapshot_publish'
    THEN
        RAISE EXCEPTION 'assertion (two pending) failed: the second fixture must report the same exact pending code, got health=%, action=%',
            v_health, v_action;
    END IF;

    PERFORM flashback_set_restore_in_progress(true);
    BEGIN
        DROP TABLE IF EXISTS public.it_capturing_doctor CASCADE;
        DROP TABLE IF EXISTS public.it_capturing_doctor_2 CASCADE;
    EXCEPTION WHEN OTHERS THEN
        PERFORM flashback_set_restore_in_progress(false);
        RAISE;
    END;
    PERFORM flashback_set_restore_in_progress(false);
END;
$tv$;
