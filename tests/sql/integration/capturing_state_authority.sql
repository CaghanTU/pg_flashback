-- Step 9: structural invariants around the 'capturing' state, exercised
-- directly and adversarially against the SQL authority functions --
-- complements (does not replace) the coordinator's real-marker-transaction
-- proof (external_zstd_coordinator.rs), which only ever exercises the happy
-- path. Covers:
--   1. the parentless-reservation invariant (generation_no=1, external_zstd
--      only, zero prior coverage_generations rows of ANY state)
--   2. capturing's details/boundary immutability under a raw same-state
--      UPDATE that bypasses the centralized authority entirely
--   3. the cross-table snapshot-eligibility check building->capturing must
--      perform itself, not merely assume from a same-row CHECK constraint
DO $tv$
DECLARE
    v_db oid := (SELECT oid FROM pg_database WHERE datname = current_database());
    v_stream bigint;
    v_tracking bigint;
    v_tracking2 bigint;
    v_tracking_c1 bigint;
    v_tracking_c1_donor bigint;
    v_tracking_c2 bigint;
    v_tracking_c2_donor bigint;
    v_tracking_c3 bigint;
    v_tracking_c3_donor bigint;
    v_tracking_c4 bigint;
    v_tracking_parent_child bigint;
    v_rel oid;
    v_rel2 oid;
    v_rel_c1 oid;
    v_rel_c1_donor oid;
    v_rel_c2 oid;
    v_rel_c2_donor oid;
    v_rel_c3 oid;
    v_rel_c3_donor oid;
    v_rel_c4 oid;
    v_rel_pc oid;
    v_row RECORD;
    v_row2 RECORD;
    v_row_c1 RECORD;
    v_row_c2 RECORD;
    v_row_c3 RECORD;
    v_row_c4 RECORD;
    v_bind RECORD;
    v_gen_row flashback.coverage_generations%ROWTYPE;
    v_snap_row flashback.snapshots%ROWTYPE;
    v_locator jsonb;
    v_raised boolean;
    v_msg text;
    v_ok boolean;
    v_health text;
    v_action text;
    v_reason text;
    v_parent_gen bigint;
    v_parent_snap bigint;
    v_snap_c1_donor bigint;
    v_snap_c2_donor bigint;
    v_snap_c3_donor bigint;
BEGIN
    CREATE TABLE IF NOT EXISTS public.it_capturing_auth (id int PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS public.it_capturing_auth_2 (id int PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS public.it_capturing_auth_c1 (id int PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS public.it_capturing_auth_c1_donor (id int PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS public.it_capturing_auth_c2 (id int PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS public.it_capturing_auth_c2_donor (id int PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS public.it_capturing_auth_c3 (id int PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS public.it_capturing_auth_c3_donor (id int PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS public.it_capturing_auth_c4 (id int PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS public.it_capturing_auth_pc (id int PRIMARY KEY);
    v_rel := 'public.it_capturing_auth'::regclass;
    v_rel2 := 'public.it_capturing_auth_2'::regclass;
    v_rel_c1 := 'public.it_capturing_auth_c1'::regclass;
    v_rel_c1_donor := 'public.it_capturing_auth_c1_donor'::regclass;
    v_rel_c2 := 'public.it_capturing_auth_c2'::regclass;
    v_rel_c2_donor := 'public.it_capturing_auth_c2_donor'::regclass;
    v_rel_c3 := 'public.it_capturing_auth_c3'::regclass;
    v_rel_c3_donor := 'public.it_capturing_auth_c3_donor'::regclass;
    v_rel_c4 := 'public.it_capturing_auth_c4'::regclass;
    v_rel_pc := 'public.it_capturing_auth_pc'::regclass;

    v_stream := public.flashback_internal_create_capture_stream(
        p_database_oid => v_db, p_initial_state => 'active',
        p_slot_name => 'it_capturing_auth_slot', p_plugin_name => 'pg_flashback_decoder'
    );

    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile
    ) VALUES (v_rel, 'public', 'it_capturing_auth', NULL, 'local_delta')
    RETURNING tracking_id INTO v_tracking;
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile
    ) VALUES (v_rel2, 'public', 'it_capturing_auth_2', NULL, 'local_delta')
    RETURNING tracking_id INTO v_tracking2;
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile
    ) VALUES (v_rel_c1, 'public', 'it_capturing_auth_c1', NULL, 'local_delta')
    RETURNING tracking_id INTO v_tracking_c1;
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile
    ) VALUES (v_rel_c1_donor, 'public', 'it_capturing_auth_c1_donor', NULL, 'local_delta')
    RETURNING tracking_id INTO v_tracking_c1_donor;
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile
    ) VALUES (v_rel_c2, 'public', 'it_capturing_auth_c2', NULL, 'local_delta')
    RETURNING tracking_id INTO v_tracking_c2;
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile
    ) VALUES (v_rel_c2_donor, 'public', 'it_capturing_auth_c2_donor', NULL, 'local_delta')
    RETURNING tracking_id INTO v_tracking_c2_donor;
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile
    ) VALUES (v_rel_c3, 'public', 'it_capturing_auth_c3', NULL, 'local_delta')
    RETURNING tracking_id INTO v_tracking_c3;
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile
    ) VALUES (v_rel_c3_donor, 'public', 'it_capturing_auth_c3_donor', NULL, 'local_delta')
    RETURNING tracking_id INTO v_tracking_c3_donor;
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile
    ) VALUES (v_rel_c4, 'public', 'it_capturing_auth_c4', NULL, 'local_delta')
    RETURNING tracking_id INTO v_tracking_c4;
    INSERT INTO flashback.tracked_tables (
        rel_oid, schema_name, table_name, base_snapshot_table, recovery_profile
    ) VALUES (v_rel_pc, 'public', 'it_capturing_auth_pc', NULL, 'local_delta')
    RETURNING tracking_id INTO v_tracking_parent_child;

    -- =====================================================================
    -- 1a. A parentless reservation must be generation_no 1. No row left
    --     behind by the rejected attempt.
    -- =====================================================================
    v_raised := false;
    BEGIN
        PERFORM public.flashback_internal_reserve_online_generation(
            p_tracking_id => v_tracking, p_rel_oid => v_rel,
            p_stream_id => v_stream, p_generation_no => 2,
            p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
            p_operation_nonce => 920001
        );
    EXCEPTION WHEN invalid_parameter_value THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT ILIKE '%generation_no 1%' THEN
        RAISE EXCEPTION '1a failed: parentless generation_no<>1 must fail closed, got: %', v_msg;
    END IF;
    IF EXISTS (SELECT 1 FROM flashback.coverage_generations WHERE tracking_id = v_tracking) THEN
        RAISE EXCEPTION '1a failed: rejected generation_no=2 attempt left a generation row behind';
    END IF;

    -- =====================================================================
    -- 1b. Aborted history (a terminal, non-pending state) must still block
    --     a fresh parentless reservation -- proves the guard is not scoped
    --     to 'building'/'capturing'/'active' only.
    -- =====================================================================
    SELECT * INTO v_row
    FROM public.flashback_internal_reserve_online_generation(
        p_tracking_id => v_tracking, p_rel_oid => v_rel,
        p_stream_id => v_stream, p_generation_no => 1,
        p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
        p_operation_nonce => 920002
    );
    PERFORM public.flashback_internal_transition_coverage_generation(
        v_row.generation_id, v_tracking, 'building', 'aborted', 'history_fixture',
        NULL, NULL, NULL, NULL, NULL, NULL, '{}'::jsonb
    );
    v_raised := false;
    BEGIN
        PERFORM public.flashback_internal_reserve_online_generation(
            p_tracking_id => v_tracking, p_rel_oid => v_rel,
            p_stream_id => v_stream, p_generation_no => 1,
            p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
            p_operation_nonce => 920003
        );
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT ILIKE '%already has a coverage generation%' THEN
        RAISE EXCEPTION '1b failed: parentless reservation after aborted history must fail closed, got: %', v_msg;
    END IF;

    -- =====================================================================
    -- 2 + 1c. Drive v_tracking2's generation legitimately all the way
    -- through building -> capturing -> active -> sealed -> retired (the
    -- real production sequence, called directly), using each waypoint for
    -- its own proof:
    --   - 'capturing': the details-immutability negative regression (2)
    --   - 'sealed'/'retired': parentless-after-history, sealed/retired
    --     specifically, not just aborted (1c)
    -- =====================================================================
    SELECT * INTO v_row2
    FROM public.flashback_internal_reserve_online_generation(
        p_tracking_id => v_tracking2, p_rel_oid => v_rel2,
        p_stream_id => v_stream, p_generation_no => 1,
        p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
        p_operation_nonce => 920010
    );

    -- Health assertion 1: 'building' (boundary not yet bound/resolved at
    -- all) must report the boundary-resolution action/reason -- these
    -- values are only ever correct for 'building', never 'capturing'.
    SELECT h.health, h.recommended_action, h.reason
      INTO v_health, v_action, v_reason
    FROM flashback_health() h
    WHERE h.tracking_id = v_tracking2;
    IF v_health IS DISTINCT FROM 'maintenance_required'
       OR v_action IS DISTINCT FROM 'wait_for_boundary_commit_resolution'
       OR v_reason IS DISTINCT FROM 'generation boundary awaiting COMMIT LSN'
    THEN
        RAISE EXCEPTION 'health assertion 1 failed: building must report boundary-resolution action, got health=%, action=%, reason=%',
            v_health, v_action, v_reason;
    END IF;

    SELECT * INTO v_bind
    FROM public.flashback_internal_bind_online_boundary(
        v_row2.generation_id, v_tracking2, v_row2.snapshot_id, v_rel2
    );
    INSERT INTO flashback.capture_commits(stream_id, commit_lsn, source_xid, committed_at)
    VALUES (v_stream, '0/10000'::pg_lsn, v_bind.boundary_xid, TIMESTAMPTZ '2024-01-01 00:00:10+00');
    PERFORM public.flashback_internal_snapshot_refine_boundary(
        v_row2.snapshot_id, v_tracking2, v_row2.generation_id, v_stream,
        '0/10000'::pg_lsn, TIMESTAMPTZ '2024-01-01 00:00:10+00'
    );
    PERFORM public.flashback_internal_transition_coverage_generation(
        v_row2.generation_id, v_tracking2, 'building', 'capturing', 'boundary_commit_observed',
        '0/10000'::pg_lsn, TIMESTAMPTZ '2024-01-01 00:00:10+00', NULL, NULL, NULL, NULL, '{}'::jsonb
    );
    SELECT * INTO v_gen_row FROM flashback.coverage_generations WHERE generation_id = v_row2.generation_id;
    IF v_gen_row.state IS DISTINCT FROM 'capturing' THEN
        RAISE EXCEPTION 'fixture setup failed: expected capturing, got %', v_gen_row.state;
    END IF;

    -- Health assertion 2: 'capturing' (boundary already resolved) must
    -- report the external-publish action/reason, never the
    -- boundary-resolution wording -- its boundary is a resolved fact by
    -- construction (state_authority.sql's own cross-table check refuses
    -- the transition otherwise).
    SELECT h.health, h.recommended_action, h.reason
      INTO v_health, v_action, v_reason
    FROM flashback_health() h
    WHERE h.tracking_id = v_tracking2;
    IF v_health IS DISTINCT FROM 'maintenance_required'
       OR v_action IS DISTINCT FROM 'wait_for_external_snapshot_publish'
       OR v_reason IS DISTINCT FROM 'external snapshot copy/publish verification is in progress'
    THEN
        RAISE EXCEPTION 'health assertion 2 failed: capturing must report external-publish action, got health=%, action=%, reason=%',
            v_health, v_action, v_reason;
    END IF;

    -- Health assertion 3 (row-level half): never healthy while only
    -- 'capturing'. The doctor()-aggregate half (never 'ok', and 'warning'
    -- not the generic 'error' while young) is asserted in
    -- capturing_health_doctor.sql instead of here: flashback_doctor() is a
    -- database-wide aggregate, and this file deliberately leaves several
    -- other fixtures (an aborted-only lifecycle among them) in states that
    -- independently affect that same aggregate, which would make a
    -- doctor()-status assertion here attributable to the wrong fixture.
    IF v_health = 'healthy' THEN
        RAISE EXCEPTION 'health assertion 3 failed: capturing-only lifecycle must never report healthy';
    END IF;

    -- 2. A same-state 'capturing' raw UPDATE mutating details must fail
    --    closed -- proves the trigger-level lockdown, independent of
    --    whatever the centralized authority function does or doesn't call.
    v_raised := false;
    BEGIN
        UPDATE flashback.coverage_generations
           SET details = details || jsonb_build_object('smuggled', true)
         WHERE generation_id = v_row2.generation_id;
    EXCEPTION WHEN integrity_constraint_violation THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT ILIKE '%details are immutable%' THEN
        RAISE EXCEPTION '2 failed: a same-state capturing details UPDATE must fail closed, got: %', v_msg;
    END IF;
    SELECT * INTO v_gen_row FROM flashback.coverage_generations WHERE generation_id = v_row2.generation_id;
    IF v_gen_row.details ? 'smuggled' THEN
        RAISE EXCEPTION '2 failed: rejected raw UPDATE still smuggled details through';
    END IF;

    -- Positive complement: details augmentation IS permitted as an
    -- incidental side effect of the real capturing -> active exit.
    v_locator := jsonb_build_object(
        'system_identifier', (pg_control_system()).system_identifier::text,
        'database_oid', v_db::text,
        'tracking_id', v_tracking2::text,
        'snapshot_id', v_row2.snapshot_id::text,
        'nonce', '920010'
    );
    SELECT * INTO v_snap_row FROM flashback.snapshots WHERE snapshot_id = v_row2.snapshot_id;
    PERFORM public.flashback_internal_publish_external_snapshot(
        v_row2.snapshot_id, v_tracking2, v_row2.generation_id, 920010,
        v_locator, 0, 'zstd', 1, 1, 1, repeat('c', 64),
        v_snap_row.external_column_contract,
        public.flashback_sha256(v_snap_row.schema_def::text)
    );
    IF NOT public.flashback_internal_activate_external_generation(
        v_row2.generation_id, v_tracking2, v_row2.snapshot_id
    ) THEN
        RAISE EXCEPTION 'fixture setup failed: capturing -> active activation reported idempotent retry';
    END IF;
    SELECT * INTO v_gen_row FROM flashback.coverage_generations WHERE generation_id = v_row2.generation_id;
    IF v_gen_row.state IS DISTINCT FROM 'active' THEN
        RAISE EXCEPTION 'fixture setup failed: expected active after publish+activate, got %', v_gen_row.state;
    END IF;

    -- Health assertion 4 ("active after verified publication returns
    -- healthy/none") is NOT proven anywhere in this repository as of this
    -- commit, at either the flashback_health() row level or the
    -- flashback_doctor() aggregate level, in this file, in
    -- capturing_health_doctor.sql, or in external_zstd_coordinator.rs's
    -- own #[pg_test] (which proves the WAL-gap claim only as far as
    -- 'capturing' -- it never publishes or activates, so it does not touch
    -- this claim at all). This fixture's stream has no real, non-missing
    -- PostgreSQL replication slot (this file, like every other fixture in
    -- this test suite, uses flashback_internal_create_capture_stream
    -- directly with a slot_name column that names no actual OS-level slot
    -- -- creating one is blocked by pg_create_logical_replication_slot's
    -- "transaction has performed writes" restriction, which
    -- _common_setup.sql's own TRUNCATE already trips before this file's DO
    -- block even starts), so health_runtime.sql's slot_lost branch
    -- (`COALESCE(slot.wal_status, '') = 'missing' AND rec.stream_id IS NOT
    -- NULL` -- rec.stream_id only becomes non-NULL once a generation is
    -- active) fires for ANY row with an active generation -- found
    -- directly, by driving this exact fixture through publish+activate and
    -- observing health=slot_lost, not healthy. A bare #[pg_test] Rust
    -- function with no common-setup TRUNCATE was tried as a way around
    -- this, using the real-slot-as-literal-first-statement technique that
    -- happened to work once for a different fixture in this stage's
    -- WAL-gap proof -- it did not reliably reproduce for this specific
    -- assertion, for reasons this investigation could not fully isolate,
    -- and was removed rather than left as an unreliable test. active ->
    -- healthy/ok remains a mandatory live-cluster assertion for the later
    -- real-decoder qualification stage (stage 7/8, a real slot against a
    -- normally-running PostgreSQL instance); it is not proven by this pgrx
    -- harness, and no test in this repository as of this commit may be
    -- cited as proving it. Once a real, non-missing slot exists,
    -- wal_status is not 'missing', slot_lost does not fire, and a row
    -- falls through correctly to the healthy branch -- provable today only
    -- by reading health_runtime.sql's own ELSIF chain, not by a live call.

    -- Seal, then retire, to reach the two remaining terminal histories.
    v_ok := public.flashback_internal_transition_coverage_generation(
        v_row2.generation_id, v_tracking2, 'active', 'sealed', 'history_fixture',
        NULL, NULL, '0/20000'::pg_lsn, TIMESTAMPTZ '2024-01-01 00:00:20+00',
        '0/20000'::pg_lsn, TIMESTAMPTZ '2024-01-01 00:00:20+00', '{}'::jsonb
    );
    IF NOT v_ok THEN
        RAISE EXCEPTION 'fixture setup failed: active -> sealed did not mutate';
    END IF;

    -- 1c (sealed). A parentless reservation must still be impossible with
    -- only a *sealed* (not active, not building/capturing) generation on
    -- record.
    v_raised := false;
    BEGIN
        PERFORM public.flashback_internal_reserve_online_generation(
            p_tracking_id => v_tracking2, p_rel_oid => v_rel2,
            p_stream_id => v_stream, p_generation_no => 1,
            p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
            p_operation_nonce => 920011
        );
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT ILIKE '%already has a coverage generation%' THEN
        RAISE EXCEPTION '1c (sealed) failed: parentless reservation after sealed history must fail closed, got: %', v_msg;
    END IF;

    PERFORM public.flashback_internal_transition_coverage_generation(
        v_row2.generation_id, v_tracking2, 'sealed', 'retired', 'history_fixture',
        NULL, NULL, NULL, NULL, NULL, NULL, '{}'::jsonb
    );

    -- 1c (retired).
    v_raised := false;
    BEGIN
        PERFORM public.flashback_internal_reserve_online_generation(
            p_tracking_id => v_tracking2, p_rel_oid => v_rel2,
            p_stream_id => v_stream, p_generation_no => 1,
            p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
            p_operation_nonce => 920012
        );
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT ILIKE '%already has a coverage generation%' THEN
        RAISE EXCEPTION '1c (retired) failed: parentless reservation after retired history must fail closed, got: %', v_msg;
    END IF;

    -- =====================================================================
    -- 1d. Has-parent maintenance reservation remains unaffected by any of
    -- the parentless-only guards above -- a normal, legitimate has-parent
    -- reservation alongside an already-active parent still succeeds.
    -- =====================================================================
    v_parent_snap := public.flashback_internal_snapshot_create(
        v_tracking_parent_child, v_rel_pc, 'public', 'it_capturing_auth_pc',
        '0/1'::pg_lsn, 'initial_track'
    );
    v_parent_gen := public.flashback_internal_create_coverage_generation(
        p_tracking_id => v_tracking_parent_child, p_generation_no => 1, p_stream_id => v_stream,
        p_boundary_kind => 'initial_track', p_rel_oid_at_boundary => v_rel_pc,
        p_boundary_snapshot_id => v_parent_snap, p_boundary_xid => txid_current(),
        p_boundary_marker => 'pc-parent-fixture'
    );
    PERFORM public.flashback_internal_transition_coverage_generation(
        v_parent_gen, v_tracking_parent_child, 'building', 'active', 'activate',
        '0/1'::pg_lsn, clock_timestamp(), '0/1'::pg_lsn, clock_timestamp(), NULL, NULL, '{}'::jsonb
    );
    -- Unaffected: has-parent reservation succeeds normally even though this
    -- tracking_id already has an active generation on record (exactly the
    -- coexistence the parentless-only guards must never restrict).
    PERFORM public.flashback_internal_reserve_online_generation(
        p_tracking_id => v_tracking_parent_child, p_rel_oid => v_rel_pc,
        p_stream_id => v_stream, p_generation_no => 2,
        p_parent_generation_id => v_parent_gen, p_storage_backend => 'external_zstd',
        p_operation_nonce => 920020
    );
    IF NOT EXISTS (
        SELECT 1 FROM flashback.coverage_generations
        WHERE tracking_id = v_tracking_parent_child AND generation_no = 2 AND state = 'building'
    ) THEN
        RAISE EXCEPTION '1d failed: has-parent reservation alongside an active parent must still succeed';
    END IF;

    -- =====================================================================
    -- 3. Cross-table capturing-transition validation: building -> capturing
    -- must itself verify the linked snapshot's tracking_id, storage_backend,
    -- payload_state and resolved snapshot_lsn -- not merely trust a
    -- same-row CHECK constraint (which cannot see the snapshots table at
    -- all).
    -- =====================================================================

    -- 3a. Wrong backend: a 'creating' snapshot can only ever be
    -- external_zstd (snapshots_lsn_shape_check enforces this at the row
    -- level, unconditionally -- CHECK constraints, unlike triggers, are
    -- never skipped by session_replication_role, so an in-place backend
    -- mutation on the real reservation's own snapshot is not constructible
    -- at all). The only way this mismatch can happen is a corrupted
    -- boundary_snapshot_id *pointer* -- so point it at a real, independent,
    -- legitimately 'available' heap_v1 snapshot instead (bypassing
    -- coverage_generations_guard's ownership-immutability check via
    -- session_replication_role purely to construct this fixture; boundary_lsn
    -- is still NULL at this point, so the cross-table FK -- MATCH SIMPLE,
    -- any NULL referencing column skips enforcement -- stays trivially
    -- satisfied regardless of which snapshot_id/tracking_id it points to).
    PERFORM public.flashback_internal_snapshot_create(
        v_tracking_c1_donor, v_rel_c1_donor, 'public', 'it_capturing_auth_c1_donor',
        '0/1'::pg_lsn, 'initial_track'
    );
    SELECT * INTO v_row_c1
    FROM public.flashback_internal_reserve_online_generation(
        p_tracking_id => v_tracking_c1, p_rel_oid => v_rel_c1,
        p_stream_id => v_stream, p_generation_no => 1,
        p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
        p_operation_nonce => 920030
    );
    PERFORM public.flashback_internal_bind_online_boundary(
        v_row_c1.generation_id, v_tracking_c1, v_row_c1.snapshot_id, v_rel_c1
    );
    SELECT snapshot_id INTO v_snap_c1_donor
    FROM flashback.snapshots
    WHERE tracking_id = v_tracking_c1_donor AND payload_state = 'available';
    PERFORM set_config('session_replication_role', 'replica', true);
    UPDATE flashback.coverage_generations
       SET boundary_snapshot_id = v_snap_c1_donor
     WHERE generation_id = v_row_c1.generation_id;
    PERFORM set_config('session_replication_role', 'origin', true);

    v_raised := false;
    BEGIN
        PERFORM public.flashback_internal_transition_coverage_generation(
            v_row_c1.generation_id, v_tracking_c1, 'building', 'capturing', 'test_wrong_backend',
            '0/21000'::pg_lsn, clock_timestamp(), NULL, NULL, NULL, NULL, '{}'::jsonb
        );
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT ILIKE '%not eligible for capturing%' THEN
        RAISE EXCEPTION '3a failed: building->capturing with a heap_v1-backed snapshot must fail closed, got: %', v_msg;
    END IF;
    SELECT * INTO v_gen_row FROM flashback.coverage_generations WHERE generation_id = v_row_c1.generation_id;
    IF v_gen_row.state IS DISTINCT FROM 'building' THEN
        RAISE EXCEPTION '3a failed: rejected transition must leave the generation in building, got %', v_gen_row.state;
    END IF;

    -- 3b. Wrong snapshot payload_state ('aborted', not 'creating'). Same
    -- reasoning as 3a: an in-place payload_state mutation on a NULL-lsn row
    -- to anything outside ('creating','aborted') would itself violate
    -- snapshots_lsn_shape_check, so 'aborted' (still schema-legal with a
    -- NULL lsn) is the only in-place mutation possible, and it already
    -- proves the point (aborted <> creating). Uses flashback_internal_
    -- snapshot_reserve directly (not paired with a generation via
    -- flashback_internal_reserve_online_generation, which would claim it
    -- through coverage_generations_boundary_snapshot_once_idx -- a snapshot
    -- may be a generation's boundary_snapshot_id at most once, ever -- so
    -- an already-generation-owned donor could never be redirected onto a
    -- second generation the way 3a's non-generation-owned heap_v1 donor
    -- could).
    v_snap_c2_donor := public.flashback_internal_snapshot_reserve(
        v_tracking_c2_donor, v_rel_c2_donor, 'external_zstd'
    );
    PERFORM public.flashback_internal_snapshot_abort(v_snap_c2_donor, v_tracking_c2_donor);

    SELECT * INTO v_row_c2
    FROM public.flashback_internal_reserve_online_generation(
        p_tracking_id => v_tracking_c2, p_rel_oid => v_rel_c2,
        p_stream_id => v_stream, p_generation_no => 1,
        p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
        p_operation_nonce => 920032
    );
    PERFORM public.flashback_internal_bind_online_boundary(
        v_row_c2.generation_id, v_tracking_c2, v_row_c2.snapshot_id, v_rel_c2
    );
    PERFORM set_config('session_replication_role', 'replica', true);
    UPDATE flashback.coverage_generations
       SET boundary_snapshot_id = v_snap_c2_donor
     WHERE generation_id = v_row_c2.generation_id;
    PERFORM set_config('session_replication_role', 'origin', true);

    v_raised := false;
    BEGIN
        PERFORM public.flashback_internal_transition_coverage_generation(
            v_row_c2.generation_id, v_tracking_c2, 'building', 'capturing', 'test_wrong_payload_state',
            '0/21100'::pg_lsn, clock_timestamp(), NULL, NULL, NULL, NULL, '{}'::jsonb
        );
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT ILIKE '%not eligible for capturing%' THEN
        RAISE EXCEPTION '3b failed: building->capturing with a non-creating snapshot must fail closed, got: %', v_msg;
    END IF;

    -- 3c. Wrong tracking identity: point the generation's boundary_snapshot_id
    -- at a snapshot legitimately reserved for a *different* tracking_id
    -- (bypassing coverage_generations_guard's own ownership-immutability
    -- check via session_replication_role, purely to construct the fixture;
    -- boundary_lsn is still NULL at this point so the cross-table FK stays
    -- trivially satisfied regardless of which snapshot_id is pointed to).
    -- Uses flashback_internal_snapshot_reserve directly, same reason as 3b:
    -- a donor snapshot already claimed as another generation's own
    -- boundary_snapshot_id could never be redirected onto a second one.
    SELECT * INTO v_row_c3
    FROM public.flashback_internal_reserve_online_generation(
        p_tracking_id => v_tracking_c3, p_rel_oid => v_rel_c3,
        p_stream_id => v_stream, p_generation_no => 1,
        p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
        p_operation_nonce => 920040
    );
    PERFORM public.flashback_internal_bind_online_boundary(
        v_row_c3.generation_id, v_tracking_c3, v_row_c3.snapshot_id, v_rel_c3
    );
    v_snap_c3_donor := public.flashback_internal_snapshot_reserve(
        v_tracking_c3_donor, v_rel_c3_donor, 'external_zstd'
    );
    PERFORM set_config('session_replication_role', 'replica', true);
    UPDATE flashback.coverage_generations
       SET boundary_snapshot_id = v_snap_c3_donor
     WHERE generation_id = v_row_c3.generation_id;
    PERFORM set_config('session_replication_role', 'origin', true);

    v_raised := false;
    BEGIN
        PERFORM public.flashback_internal_transition_coverage_generation(
            v_row_c3.generation_id, v_tracking_c3, 'building', 'capturing', 'test_wrong_tracking',
            '0/21200'::pg_lsn, clock_timestamp(), NULL, NULL, NULL, NULL, '{}'::jsonb
        );
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT ILIKE '%not eligible for capturing%' THEN
        RAISE EXCEPTION '3c failed: building->capturing with a foreign-tracking snapshot must fail closed, got: %', v_msg;
    END IF;

    -- 3d. Boundary mismatch: the snapshot's actual resolved snapshot_lsn
    -- does not equal the boundary_lsn this call claims -- requires no
    -- fixture corruption at all, just a wrong parameter.
    SELECT * INTO v_row_c4
    FROM public.flashback_internal_reserve_online_generation(
        p_tracking_id => v_tracking_c4, p_rel_oid => v_rel_c4,
        p_stream_id => v_stream, p_generation_no => 1,
        p_parent_generation_id => NULL, p_storage_backend => 'external_zstd',
        p_operation_nonce => 920042
    );
    SELECT * INTO v_bind
    FROM public.flashback_internal_bind_online_boundary(
        v_row_c4.generation_id, v_tracking_c4, v_row_c4.snapshot_id, v_rel_c4
    );
    INSERT INTO flashback.capture_commits(stream_id, commit_lsn, source_xid, committed_at)
    VALUES (v_stream, '0/23000'::pg_lsn, v_bind.boundary_xid, TIMESTAMPTZ '2024-01-01 00:00:30+00');
    PERFORM public.flashback_internal_snapshot_refine_boundary(
        v_row_c4.snapshot_id, v_tracking_c4, v_row_c4.generation_id, v_stream,
        '0/23000'::pg_lsn, TIMESTAMPTZ '2024-01-01 00:00:30+00'
    );
    -- Snapshot is now correctly resolved to 0/23000; claim a different LSN.
    v_raised := false;
    BEGIN
        PERFORM public.flashback_internal_transition_coverage_generation(
            v_row_c4.generation_id, v_tracking_c4, 'building', 'capturing', 'test_boundary_mismatch',
            '0/23999'::pg_lsn, TIMESTAMPTZ '2024-01-01 00:00:30+00', NULL, NULL, NULL, NULL, '{}'::jsonb
        );
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT ILIKE '%not eligible for capturing%' THEN
        RAISE EXCEPTION '3d failed: building->capturing with a boundary_lsn not matching the resolved snapshot_lsn must fail closed, got: %', v_msg;
    END IF;
    SELECT * INTO v_gen_row FROM flashback.coverage_generations WHERE generation_id = v_row_c4.generation_id;
    IF v_gen_row.state IS DISTINCT FROM 'building' OR v_gen_row.boundary_lsn IS NOT NULL THEN
        RAISE EXCEPTION '3d failed: rejected transition must not durably set boundary_lsn, got state=%, boundary_lsn=%',
            v_gen_row.state, v_gen_row.boundary_lsn;
    END IF;
    -- And the correct LSN still transitions it cleanly (proves 3a-3d were
    -- real rejections of a bad call, not a permanently broken generation).
    PERFORM public.flashback_internal_transition_coverage_generation(
        v_row_c4.generation_id, v_tracking_c4, 'building', 'capturing', 'test_boundary_correct',
        '0/23000'::pg_lsn, TIMESTAMPTZ '2024-01-01 00:00:30+00', NULL, NULL, NULL, NULL, '{}'::jsonb
    );
    SELECT * INTO v_gen_row FROM flashback.coverage_generations WHERE generation_id = v_row_c4.generation_id;
    IF v_gen_row.state IS DISTINCT FROM 'capturing' THEN
        RAISE EXCEPTION '3d failed: the correct boundary_lsn must still transition cleanly, got %', v_gen_row.state;
    END IF;

    PERFORM flashback_set_restore_in_progress(true);
    BEGIN
        DROP TABLE IF EXISTS public.it_capturing_auth CASCADE;
        DROP TABLE IF EXISTS public.it_capturing_auth_2 CASCADE;
        DROP TABLE IF EXISTS public.it_capturing_auth_c1 CASCADE;
        DROP TABLE IF EXISTS public.it_capturing_auth_c1_donor CASCADE;
        DROP TABLE IF EXISTS public.it_capturing_auth_c2 CASCADE;
        DROP TABLE IF EXISTS public.it_capturing_auth_c2_donor CASCADE;
        DROP TABLE IF EXISTS public.it_capturing_auth_c3 CASCADE;
        DROP TABLE IF EXISTS public.it_capturing_auth_c3_donor CASCADE;
        DROP TABLE IF EXISTS public.it_capturing_auth_c4 CASCADE;
        DROP TABLE IF EXISTS public.it_capturing_auth_pc CASCADE;
    EXCEPTION WHEN OTHERS THEN
        PERFORM flashback_set_restore_in_progress(false);
        RAISE;
    END;
    PERFORM flashback_set_restore_in_progress(false);
END;
$tv$;
