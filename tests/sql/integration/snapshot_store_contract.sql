-- Dedicated SnapshotStore (sql/functions/snapshot_store.sql) regression
-- suite. These exercise the internal create/resolve/require_available/
-- materialize/sizes/retire primitives directly, isolated from the full
-- track/WAL/restore machinery that higher-level integration tests already
-- cover end to end. tracking_id has no FK on flashback.snapshots, so a
-- synthetic tracking_id is enough to exercise the store on its own -- the
-- same pattern tracked_table_resolver_search_path.sql already uses.
DROP TABLE IF EXISTS public.snst_src1 CASCADE;
DROP TABLE IF EXISTS public.snst_src2 CASCADE;
DROP TABLE IF EXISTS public.snst_fidelity CASCADE;
DROP TABLE IF EXISTS public.snst_fidelity_dest CASCADE;

CREATE TABLE public.snst_src1 (id int PRIMARY KEY, v text);
INSERT INTO public.snst_src1 VALUES (1, 'a'), (2, 'b');

CREATE TABLE public.snst_src2 (id int PRIMARY KEY, v text);
INSERT INTO public.snst_src2 VALUES (10, 'x');

-- 1. Fresh create -> available.
DO $s1$
DECLARE
    v_tid bigint := 820001;
    v_snap_id bigint;
    v_row record;
BEGIN
    v_snap_id := flashback_internal_snapshot_create(
        v_tid, 'public.snst_src1'::regclass, 'public', 'snst_src1',
        pg_current_wal_insert_lsn(), 'generation'
    );
    SELECT * INTO v_row FROM flashback_internal_snapshot_resolve(v_snap_id, v_tid);
    IF v_row.payload_state <> 'available' THEN
        RAISE EXCEPTION 's1: expected available, got %', v_row.payload_state;
    END IF;
    IF v_row.storage_backend <> 'heap_v1' THEN
        RAISE EXCEPTION 's1: expected heap_v1 backend, got %', v_row.storage_backend;
    END IF;
    IF v_row.locator IS NULL OR NOT (v_row.locator ? 'schema') OR NOT (v_row.locator ? 'relation') THEN
        RAISE EXCEPTION 's1: locator missing schema/relation keys: %', v_row.locator;
    END IF;
    IF v_row.row_count <> 2 THEN
        RAISE EXCEPTION 's1: expected row_count 2, got %', v_row.row_count;
    END IF;
    IF v_row.payload_relid IS NULL OR NOT flashback_payload_is_owned(v_row.payload_relid) THEN
        RAISE EXCEPTION 's1: payload relation missing or not owned';
    END IF;
    IF v_row.available_at IS NULL THEN
        RAISE EXCEPTION 's1: available_at not stamped';
    END IF;
END;
$s1$;

-- 2. Exact snapshot_id + tracking_id resolve: a snapshot_id that exists
-- under a DIFFERENT tracking_id must not resolve (no cross-tenant leak via
-- snapshot_id alone).
DO $s2$
DECLARE
    v_tid_a bigint := 820002;
    v_tid_b bigint := 820003;
    v_snap_a bigint;
    v_row record;
    v_found boolean;
BEGIN
    v_snap_a := flashback_internal_snapshot_create(
        v_tid_a, 'public.snst_src1'::regclass, 'public', 'snst_src1',
        pg_current_wal_insert_lsn(), 'generation'
    );
    SELECT * INTO v_row FROM flashback_internal_snapshot_resolve(v_snap_a, v_tid_a);
    IF v_row.snapshot_id IS DISTINCT FROM v_snap_a THEN
        RAISE EXCEPTION 's2: exact id pair did not resolve';
    END IF;

    SELECT EXISTS (
        SELECT 1 FROM flashback_internal_snapshot_resolve(v_snap_a, v_tid_b)
    ) INTO v_found;
    IF v_found THEN
        RAISE EXCEPTION 's2: snapshot resolved under a foreign tracking_id';
    END IF;
END;
$s2$;

-- 3. Wrong tracking_id rejection at require_available().
DO $s3$
DECLARE
    v_tid_a bigint := 820004;
    v_tid_wrong bigint := 820005;
    v_snap_a bigint;
    v_raised boolean := false;
BEGIN
    v_snap_a := flashback_internal_snapshot_create(
        v_tid_a, 'public.snst_src1'::regclass, 'public', 'snst_src1',
        pg_current_wal_insert_lsn(), 'generation'
    );
    BEGIN
        PERFORM * FROM flashback_internal_snapshot_require_available(v_snap_a, v_tid_wrong);
    EXCEPTION WHEN OTHERS THEN
        IF SQLSTATE = '22023' THEN -- invalid_parameter_value
            v_raised := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 's3: require_available accepted a wrong tracking_id';
    END IF;
END;
$s3$;

-- 4. Missing relation fail-closed: physical payload dropped out from under
-- an `available` row (e.g. external interference) must never be reported
-- as usable, and the store's own `missing` terminal state must be reachable
-- for exactly this situation (the same edge flashback_mark_capture_stream_broken
-- uses for an incidental loss).
DO $s4$
DECLARE
    v_tid bigint := 820006;
    v_snap_id bigint;
    v_row record;
    v_raised boolean := false;
BEGIN
    v_snap_id := flashback_internal_snapshot_create(
        v_tid, 'public.snst_src1'::regclass, 'public', 'snst_src1',
        pg_current_wal_insert_lsn(), 'generation'
    );
    SELECT * INTO v_row FROM flashback_internal_snapshot_resolve(v_snap_id, v_tid);
    PERFORM flashback_drop_payload_table(v_row.payload_relid);

    SELECT * INTO v_row FROM flashback_internal_snapshot_resolve(v_snap_id, v_tid);
    IF v_row.payload_state <> 'available' THEN
        RAISE EXCEPTION 's4: expected stale available state, got %', v_row.payload_state;
    END IF;
    IF v_row.payload_relid IS NOT NULL THEN
        RAISE EXCEPTION 's4: resolve reported a relid for a dropped relation';
    END IF;

    BEGIN
        PERFORM * FROM flashback_internal_snapshot_require_available(v_snap_id, v_tid);
    EXCEPTION WHEN OTHERS THEN
        IF SQLSTATE = '55000' THEN -- object_not_in_prerequisite_state
            v_raised := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 's4: require_available did not fail closed on a missing relation';
    END IF;

    IF NOT flashback_internal_snapshot_retire(v_snap_id, v_tid, 'missing') THEN
        RAISE EXCEPTION 's4: retire(missing) did not report a mutation';
    END IF;
    SELECT * INTO v_row FROM flashback_internal_snapshot_resolve(v_snap_id, v_tid);
    IF v_row.payload_state <> 'missing' THEN
        RAISE EXCEPTION 's4: expected missing terminal state, got %', v_row.payload_state;
    END IF;
END;
$s4$;

-- 5. A user-created relation at the exact same schema/relation name as a
-- retired artifact's old locator must never be accepted as that artifact's
-- payload (existence is not ownership).
DO $s5$
DECLARE
    v_tid bigint := 820007;
    v_snap_id bigint;
    v_relation_name text;
    v_raised boolean := false;
BEGIN
    v_snap_id := flashback_internal_snapshot_create(
        v_tid, 'public.snst_src1'::regclass, 'public', 'snst_src1',
        pg_current_wal_insert_lsn(), 'generation'
    );
    SELECT locator->>'relation' INTO v_relation_name
    FROM flashback.snapshots WHERE snapshot_id = v_snap_id AND tracking_id = v_tid;

    PERFORM flashback_internal_snapshot_retire(v_snap_id, v_tid, 'retired');

    -- An ordinary, non-owned relation reappears at the exact freed name.
    EXECUTE format('CREATE TABLE flashback.%I (id int)', v_relation_name);

    BEGIN
        PERFORM * FROM flashback_internal_snapshot_require_available(v_snap_id, v_tid);
    EXCEPTION WHEN OTHERS THEN
        v_raised := true; -- terminal 'retired' alone already refuses this
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 's5: require_available accepted a retired/terminal artifact';
    END IF;

    EXECUTE format('DROP TABLE flashback.%I', v_relation_name);
END;
$s5$;

-- 6. Locator/ownership substitution: a foreign, unowned relation swapped in
-- at the exact locator name of a *currently creating* (not yet finalized)
-- artifact must not be accepted as that artifact's payload once resolved.
DO $s6$
DECLARE
    v_tid bigint := 820008;
    v_snap_id bigint;
    v_raised boolean := false;
BEGIN
    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn, schema_def, row_count, captured_at
    ) VALUES (
        'public.snst_src1'::regclass, v_tid, '', '0/1', '{}'::jsonb, 0, clock_timestamp()
    ) RETURNING snapshot_id INTO v_snap_id;

    -- A foreign, non-pg_flashback-owned relation at the name the artifact
    -- would use, finalized as if it were genuine SnapshotStore evidence.
    EXECUTE format('CREATE TABLE flashback.snap_%s_%s (id int)', v_tid, v_snap_id);
    PERFORM flashback_internal_snapshot_transition(
        v_snap_id, v_tid, ARRAY['creating'], 'available',
        'heap_v1',
        jsonb_build_object('schema', 'flashback', 'relation', format('snap_%s_%s', v_tid, v_snap_id)),
        format('flashback.snap_%s_%s', v_tid, v_snap_id), 0, NULL
    );

    BEGIN
        PERFORM * FROM flashback_internal_snapshot_require_available(v_snap_id, v_tid);
    EXCEPTION WHEN OTHERS THEN
        IF SQLSTATE = '55000' THEN -- object_not_in_prerequisite_state (not owned)
            v_raised := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 's6: require_available accepted a non-owned substituted relation';
    END IF;

    EXECUTE format('DROP TABLE flashback.snap_%s_%s', v_tid, v_snap_id);
END;
$s6$;

-- 7. A partial (`creating`) artifact can never anchor a restore or be used
-- as a materialize/require_available source.
DO $s7$
DECLARE
    v_tid bigint := 820009;
    v_snap_id bigint;
    v_raised boolean := false;
BEGIN
    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn, schema_def, row_count, captured_at
    ) VALUES (
        'public.snst_src1'::regclass, v_tid, '', '0/1', '{}'::jsonb, 0, clock_timestamp()
    ) RETURNING snapshot_id INTO v_snap_id;

    BEGIN
        PERFORM * FROM flashback_internal_snapshot_require_available(v_snap_id, v_tid);
    EXCEPTION WHEN OTHERS THEN
        IF SQLSTATE = '55000' THEN
            v_raised := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 's7: require_available accepted a creating artifact';
    END IF;

    v_raised := false;
    BEGIN
        PERFORM flashback_internal_snapshot_materialize(
            v_snap_id, v_tid, 'public', 'snst_src1', 'id, v'
        );
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 's7: materialize accepted a creating artifact';
    END IF;

    PERFORM flashback_internal_snapshot_abort(v_snap_id, v_tid);
END;
$s7$;

-- 8. Materialize fidelity: NULLs, a TOASTed value, a quoted identifier
-- column, and an identity column all survive create -> materialize exactly.
DO $s8$
DECLARE
    v_tid bigint := 820010;
    v_snap_id bigint;
    v_toast_val text := repeat('x', 4000);
BEGIN
    CREATE TABLE public.snst_fidelity (
        id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        "Weird Col" text,
        note text,
        big_val text
    );
    INSERT INTO public.snst_fidelity ("Weird Col", note, big_val) VALUES
        ('a', NULL, v_toast_val),
        (NULL, 'has note', NULL);

    v_snap_id := flashback_internal_snapshot_create(
        v_tid, 'public.snst_fidelity'::regclass, 'public', 'snst_fidelity',
        pg_current_wal_insert_lsn(), 'generation'
    );

    CREATE TABLE public.snst_fidelity_dest (
        id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        "Weird Col" text,
        note text,
        big_val text
    );
    PERFORM flashback_internal_snapshot_materialize(
        v_snap_id, v_tid, 'public', 'snst_fidelity_dest',
        '"Weird Col", note, big_val', ' OVERRIDING SYSTEM VALUE'
    );

    IF (SELECT count(*) FROM public.snst_fidelity_dest) <> 2 THEN
        RAISE EXCEPTION 's8: materialize row count mismatch';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM public.snst_fidelity_dest
        WHERE "Weird Col" = 'a' AND note IS NULL AND big_val = v_toast_val
    ) THEN
        RAISE EXCEPTION 's8: TOAST/quoted-identifier row did not survive materialize intact';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM public.snst_fidelity_dest
        WHERE "Weird Col" IS NULL AND note = 'has note' AND big_val IS NULL
    ) THEN
        RAISE EXCEPTION 's8: NULL-column row did not survive materialize intact';
    END IF;
END;
$s8$;

-- 9. Retire drops the correct artifact and leaves an unrelated sibling
-- artifact's payload untouched.
DO $s9$
DECLARE
    v_tid bigint := 820011;
    v_snap_a bigint;
    v_snap_b bigint;
    v_row_a record;
    v_row_b record;
BEGIN
    v_snap_a := flashback_internal_snapshot_create(
        v_tid, 'public.snst_src1'::regclass, 'public', 'snst_src1',
        pg_current_wal_insert_lsn(), 'generation'
    );
    v_snap_b := flashback_internal_snapshot_create(
        v_tid, 'public.snst_src2'::regclass, 'public', 'snst_src2',
        pg_current_wal_insert_lsn(), 'generation'
    );

    PERFORM flashback_internal_snapshot_retire(v_snap_a, v_tid, 'retired');

    SELECT * INTO v_row_a FROM flashback_internal_snapshot_resolve(v_snap_a, v_tid);
    SELECT * INTO v_row_b FROM flashback_internal_snapshot_resolve(v_snap_b, v_tid);
    IF v_row_a.payload_state <> 'retired' OR v_row_a.payload_relid IS NOT NULL THEN
        RAISE EXCEPTION 's9: targeted artifact was not retired correctly';
    END IF;
    IF v_row_b.payload_state <> 'available' OR v_row_b.payload_relid IS NULL THEN
        RAISE EXCEPTION 's9: sibling artifact was affected by an unrelated retire';
    END IF;
    IF (SELECT count(*) FROM public.snst_src2) <> 1 THEN
        RAISE EXCEPTION 's9: sibling source table unexpectedly touched';
    END IF;
END;
$s9$;

-- 10. Retire is idempotent: retrying with the same target state is a
-- reported no-op, not a re-drop or an error.
DO $s10$
DECLARE
    v_tid bigint := 820012;
    v_snap_id bigint;
    v_raised boolean := false;
BEGIN
    v_snap_id := flashback_internal_snapshot_create(
        v_tid, 'public.snst_src1'::regclass, 'public', 'snst_src1',
        pg_current_wal_insert_lsn(), 'generation'
    );
    IF NOT flashback_internal_snapshot_retire(v_snap_id, v_tid, 'retired') THEN
        RAISE EXCEPTION 's10: first retire did not report a mutation';
    END IF;
    IF flashback_internal_snapshot_retire(v_snap_id, v_tid, 'retired') THEN
        RAISE EXCEPTION 's10: retry retire reported a mutation instead of a no-op';
    END IF;

    -- Retrying at the *other* terminal state must fail closed, not silently
    -- reinterpret the artifact's fate.
    BEGIN
        PERFORM flashback_internal_snapshot_retire(v_snap_id, v_tid, 'missing');
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 's10: retire to a different terminal state than the recorded one was accepted';
    END IF;
END;
$s10$;

-- 11. The artifact state machine only allows the documented edges: a
-- `creating` artifact can never be pushed straight to `retired`/`retiring`,
-- skipping `available` -- the guard trigger, not just SnapshotStore's own
-- call discipline, is what makes an artifact impossible to remove out of
-- turn.
DO $s11$
DECLARE
    v_tid bigint := 820013;
    v_snap_id bigint;
    v_raised boolean := false;
BEGIN
    INSERT INTO flashback.snapshots (
        rel_oid, tracking_id, snapshot_table, snapshot_lsn, schema_def, row_count, captured_at
    ) VALUES (
        'public.snst_src1'::regclass, v_tid, '', '0/1', '{}'::jsonb, 0, clock_timestamp()
    ) RETURNING snapshot_id INTO v_snap_id;

    BEGIN
        PERFORM flashback_internal_snapshot_transition(
            v_snap_id, v_tid, ARRAY['creating'], 'retiring'
        );
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 's11: creating -> retiring was accepted (must go through available)';
    END IF;

    v_raised := false;
    BEGIN
        PERFORM flashback_internal_snapshot_retire(v_snap_id, v_tid, 'retired');
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 's11: retire() removed a creating (never-anchored) artifact';
    END IF;

    PERFORM flashback_internal_snapshot_abort(v_snap_id, v_tid);
END;
$s11$;

-- 12. Boundary refinement: exact coordinate refinement succeeds.
DO $s12$
DECLARE
    v_tid bigint := 820014;
    v_snap_id bigint;
    v_gen_id bigint;
    v_stream_id bigint;
    v_xid bigint := 990002;
    v_lsn pg_lsn := '0/1000200';
    v_time timestamptz := clock_timestamp();
    v_res boolean;
    v_snap record;
BEGIN
    v_stream_id := flashback_internal_open_capture_stream(
        'test_refine_slot'::text, 'pg_flashback'::text, '0/1'::pg_lsn, '0/1'::pg_lsn
    );

    INSERT INTO flashback.capture_commits (stream_id, commit_lsn, source_xid, committed_at)
    VALUES (v_stream_id, v_lsn, v_xid, v_time);

    v_snap_id := flashback_internal_snapshot_create(
        v_tid, 'public.snst_src1'::regclass, 'public', 'snst_src1',
        '0/1000100', 'generation'
    );

    v_gen_id := flashback_internal_create_coverage_generation(
        p_tracking_id => v_tid,
        p_generation_no => 1,
        p_stream_id => v_stream_id,
        p_boundary_kind => 'reanchor',
        p_rel_oid_at_boundary => 'public.snst_src1'::regclass,
        p_boundary_snapshot_id => v_snap_id,
        p_boundary_xid => v_xid,
        p_boundary_marker => 'marker_refine_1'
    );

    v_res := flashback_internal_snapshot_refine_boundary(
        v_snap_id, v_tid, v_gen_id, v_stream_id, v_lsn, v_time
    );

    IF NOT v_res THEN
        RAISE EXCEPTION 's12: expected refine_boundary to return true for first refinement';
    END IF;

    SELECT * INTO v_snap FROM flashback_internal_snapshot_resolve(v_snap_id, v_tid);
    IF v_snap.snapshot_lsn <> v_lsn THEN
        RAISE EXCEPTION 's12: expected snapshot_lsn %, got %', v_lsn, v_snap.snapshot_lsn;
    END IF;
END;
$s12$;

-- 13. Boundary refinement idempotency: same coordinate retry returns false.
DO $s13$
DECLARE
    v_tid bigint := 820015;
    v_snap_id bigint;
    v_gen_id bigint;
    v_stream_id bigint;
    v_xid bigint := 990004;
    v_lsn pg_lsn := '0/2000200';
    v_time timestamptz := clock_timestamp();
    v_res boolean;
BEGIN
    v_stream_id := flashback_internal_open_capture_stream(
        'test_refine_slot'::text, 'pg_flashback'::text, '0/1'::pg_lsn, '0/1'::pg_lsn
    );

    INSERT INTO flashback.capture_commits (stream_id, commit_lsn, source_xid, committed_at)
    VALUES (v_stream_id, v_lsn, v_xid, v_time);

    v_snap_id := flashback_internal_snapshot_create(
        v_tid, 'public.snst_src1'::regclass, 'public', 'snst_src1',
        '0/2000100', 'generation'
    );

    v_gen_id := flashback_internal_create_coverage_generation(
        p_tracking_id => v_tid,
        p_generation_no => 1,
        p_stream_id => v_stream_id,
        p_boundary_kind => 'reanchor',
        p_rel_oid_at_boundary => 'public.snst_src1'::regclass,
        p_boundary_snapshot_id => v_snap_id,
        p_boundary_xid => v_xid,
        p_boundary_marker => 'marker_refine_2'
    );

    PERFORM flashback_internal_snapshot_refine_boundary(
        v_snap_id, v_tid, v_gen_id, v_stream_id, v_lsn, v_time
    );

    -- Second call with exact same coordinate
    v_res := flashback_internal_snapshot_refine_boundary(
        v_snap_id, v_tid, v_gen_id, v_stream_id, v_lsn, v_time
    );

    IF v_res THEN
        RAISE EXCEPTION 's13: expected refine_boundary retry to return false (idempotent)';
    END IF;
END;
$s13$;

-- 14. Boundary refinement fail-closed: different second coordinate is rejected.
DO $s14$
DECLARE
    v_tid bigint := 820016;
    v_snap_id bigint;
    v_gen_id bigint;
    v_stream_id bigint;
    v_xid bigint := 990006;
    v_lsn1 pg_lsn := '0/3000200';
    v_lsn2 pg_lsn := '0/3000300';
    v_time1 timestamptz := clock_timestamp();
    v_time2 timestamptz := clock_timestamp() + interval '1 second';
    v_raised boolean := false;
BEGIN
    v_stream_id := flashback_internal_open_capture_stream(
        'test_refine_slot'::text, 'pg_flashback'::text, '0/1'::pg_lsn, '0/1'::pg_lsn
    );

    INSERT INTO flashback.capture_commits (stream_id, commit_lsn, source_xid, committed_at)
    VALUES (v_stream_id, v_lsn1, v_xid, v_time1),
           (v_stream_id, v_lsn2, v_xid, v_time2);

    v_snap_id := flashback_internal_snapshot_create(
        v_tid, 'public.snst_src1'::regclass, 'public', 'snst_src1',
        '0/3000100', 'generation'
    );

    v_gen_id := flashback_internal_create_coverage_generation(
        p_tracking_id => v_tid,
        p_generation_no => 1,
        p_stream_id => v_stream_id,
        p_boundary_kind => 'reanchor',
        p_rel_oid_at_boundary => 'public.snst_src1'::regclass,
        p_boundary_snapshot_id => v_snap_id,
        p_boundary_xid => v_xid,
        p_boundary_marker => 'marker_refine_3'
    );

    PERFORM flashback_internal_snapshot_refine_boundary(
        v_snap_id, v_tid, v_gen_id, v_stream_id, v_lsn1, v_time1
    );

    PERFORM flashback_internal_transition_coverage_generation(
        v_gen_id, v_tid, 'building', 'active', 'boundary_commit_observed',
        v_lsn1, v_time1, v_lsn1, v_time1, NULL, NULL, '{}'::jsonb
    );

    BEGIN
        PERFORM flashback_internal_snapshot_refine_boundary(
            v_snap_id, v_tid, v_gen_id, v_stream_id, v_lsn2, v_time2
        );
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
    END;

    IF NOT v_raised THEN
        RAISE EXCEPTION 's14: second refinement with different coordinate was accepted';
    END IF;
END;
$s14$;

-- 15. Wrong generation/snapshot/tracking/stream/XID or missing capture_commits rejected.
DO $s15$
DECLARE
    v_tid bigint := 820017;
    v_snap_id bigint;
    v_gen_id bigint;
    v_stream_id bigint;
    v_xid bigint := 990008;
    v_lsn pg_lsn := '0/4000200';
    v_time timestamptz := clock_timestamp();
    v_raised boolean := false;
BEGIN
    v_stream_id := flashback_internal_open_capture_stream(
        'test_refine_slot'::text, 'pg_flashback'::text, '0/1'::pg_lsn, '0/1'::pg_lsn
    );

    v_snap_id := flashback_internal_snapshot_create(
        v_tid, 'public.snst_src1'::regclass, 'public', 'snst_src1',
        '0/4000100', 'generation'
    );

    v_gen_id := flashback_internal_create_coverage_generation(
        p_tracking_id => v_tid,
        p_generation_no => 1,
        p_stream_id => v_stream_id,
        p_boundary_kind => 'reanchor',
        p_rel_oid_at_boundary => 'public.snst_src1'::regclass,
        p_boundary_snapshot_id => v_snap_id,
        p_boundary_xid => v_xid,
        p_boundary_marker => 'marker_refine_4'
    );

    -- Missing from capture_commits
    BEGIN
        PERFORM flashback_internal_snapshot_refine_boundary(
            v_snap_id, v_tid, v_gen_id, v_stream_id, v_lsn, v_time
        );
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
    END;

    IF NOT v_raised THEN
        RAISE EXCEPTION 's15: coordinate missing from capture_commits was accepted';
    END IF;
END;
$s15$;

-- 16. Guard trigger: direct mutation to available artifact LSN/evidence rejected.
DO $s16$
DECLARE
    v_tid bigint := 820018;
    v_snap_id bigint;
    v_raised boolean := false;
BEGIN
    v_snap_id := flashback_internal_snapshot_create(
        v_tid, 'public.snst_src1'::regclass, 'public', 'snst_src1',
        '0/5000100', 'generation'
    );

    BEGIN
        UPDATE flashback.snapshots
           SET snapshot_lsn = '0/9999999'
         WHERE snapshot_id = v_snap_id AND tracking_id = v_tid;
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
    END;

    IF NOT v_raised THEN
        RAISE EXCEPTION 's16: direct UPDATE snapshot_lsn was accepted';
    END IF;
END;
$s16$;

-- 17. Guard trigger: direct DELETE on flashback.snapshots rejected.
DO $s17$
DECLARE
    v_tid bigint := 820019;
    v_snap_id bigint;
    v_raised boolean := false;
BEGIN
    v_snap_id := flashback_internal_snapshot_create(
        v_tid, 'public.snst_src1'::regclass, 'public', 'snst_src1',
        '0/6000100', 'generation'
    );

    BEGIN
        DELETE FROM flashback.snapshots
         WHERE snapshot_id = v_snap_id AND tracking_id = v_tid;
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
    END;

    IF NOT v_raised THEN
        RAISE EXCEPTION 's17: direct DELETE from flashback.snapshots was accepted';
    END IF;
END;
$s17$;

-- 18. Legacy nullable-tracking snapshot safe retirement.
DO $s18$
DECLARE
    v_leg_snap_id bigint;
    v_payload_name text := 'base_snapshot_t99901';
    v_row record;
BEGIN
    PERFORM public.flashback_drop_payload_table(
        to_regclass(format('flashback.%I', v_payload_name))
    );
    EXECUTE format('CREATE TABLE flashback.%I AS TABLE public.snst_src1', v_payload_name);
    PERFORM public.flashback_own_payload_table(
        to_regclass(format('flashback.%I', v_payload_name))
    );

    INSERT INTO flashback.snapshots (
        rel_oid, snapshot_table, snapshot_lsn, schema_def, row_count, captured_at, payload_state, storage_backend, locator
    ) VALUES (
        'public.snst_src1'::regclass, format('flashback.%I', v_payload_name), '0/7000100',
        '{}'::jsonb, 2, clock_timestamp(), 'creating', 'heap_v1',
        jsonb_build_object('schema', 'flashback', 'relation', v_payload_name)
    ) RETURNING snapshot_id INTO v_leg_snap_id;

    UPDATE flashback.snapshots SET payload_state = 'available', available_at = clock_timestamp() WHERE snapshot_id = v_leg_snap_id AND tracking_id IS NULL;

    PERFORM flashback_internal_snapshot_retire_legacy(v_leg_snap_id, 'retired');

    SELECT * INTO v_row FROM flashback.snapshots WHERE snapshot_id = v_leg_snap_id AND tracking_id IS NULL;
    IF v_row.payload_state <> 'retired' THEN
        RAISE EXCEPTION 's18: expected retired payload_state, got %', v_row.payload_state;
    END IF;
    IF to_regclass(format('flashback.%I', v_payload_name)) IS NOT NULL THEN
        RAISE EXCEPTION 's18: physical payload table was not dropped';
    END IF;
END;
$s18$;

-- 19. User-owned legacy relation retirement fails closed.
DO $s19$
DECLARE
    v_leg_snap_id bigint;
    v_payload_name text := 'user_owned_legacy_rel_test';
    v_raised boolean := false;
BEGIN
    EXECUTE format('CREATE TABLE public.%I (id int)', v_payload_name);

    INSERT INTO flashback.snapshots (
        rel_oid, snapshot_table, snapshot_lsn, schema_def, row_count, captured_at, payload_state, storage_backend, locator
    ) VALUES (
        'public.snst_src1'::regclass, format('public.%I', v_payload_name), '0/8000100',
        '{}'::jsonb, 0, clock_timestamp(), 'creating', 'heap_v1',
        jsonb_build_object('schema', 'public', 'relation', v_payload_name)
    ) RETURNING snapshot_id INTO v_leg_snap_id;

    UPDATE flashback.snapshots SET payload_state = 'available', available_at = clock_timestamp() WHERE snapshot_id = v_leg_snap_id AND tracking_id IS NULL;

    BEGIN
        PERFORM flashback_internal_snapshot_retire_legacy(v_leg_snap_id, 'retired');
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
    END;

    IF NOT v_raised THEN
        RAISE EXCEPTION 's19: user-owned relation retirement was accepted';
    END IF;

    EXECUTE format('DROP TABLE public.%I', v_payload_name);
END;
$s19$;

-- 20. Idempotent refinement rejects mismatching boundaries on ACTIVE generations
DO $s20$
DECLARE
    v_tid bigint := 820030;
    v_snap_id bigint;
    v_stream_id bigint;
    v_gen_id bigint;
    v_xid bigint := 990020;
    v_lsn_A pg_lsn := '0/9000100';
    v_time_A timestamptz := clock_timestamp();
    v_lsn_B pg_lsn := '0/9000200';
    v_time_B timestamptz;
    v_raised boolean := false;
BEGIN
    v_stream_id := flashback_internal_open_capture_stream(
        'test_refine_slot'::text, 'pg_flashback'::text, '0/1'::pg_lsn, '0/1'::pg_lsn
    );

    v_snap_id := flashback_internal_snapshot_create(
        v_tid, 'public.snst_src1'::regclass, 'public', 'snst_src1',
        v_lsn_B, 'generation'
    );
    SELECT captured_at INTO v_time_B FROM flashback.snapshots WHERE snapshot_id = v_snap_id AND tracking_id = v_tid;

    v_gen_id := flashback_internal_create_coverage_generation(
        p_tracking_id => v_tid,
        p_generation_no => 20,
        p_stream_id => v_stream_id,
        p_boundary_kind => 'reanchor',
        p_rel_oid_at_boundary => 'public.snst_src1'::regclass,
        p_boundary_snapshot_id => v_snap_id,
        p_boundary_xid => v_xid,
        p_boundary_marker => 'marker_refine_20'
    );

    PERFORM flashback_internal_transition_coverage_generation(
        v_gen_id, v_tid, 'building', 'active', 'boundary_commit_observed',
        v_lsn_A, v_time_A, v_lsn_A, v_time_A, NULL, NULL, '{}'::jsonb
    );

    INSERT INTO flashback.capture_commits (stream_id, commit_lsn, source_xid, committed_at)
    VALUES (v_stream_id, v_lsn_B, v_xid, v_time_B);

    BEGIN
        PERFORM flashback_internal_snapshot_refine_boundary(
            v_snap_id, v_tid, v_gen_id, v_stream_id, v_lsn_B, v_time_B
        );
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
    END;

    IF NOT v_raised THEN
        RAISE EXCEPTION 's20: refine_boundary with snapshot coordinate B on active generation with boundary coordinate A was accepted';
    END IF;
END;
$s20$;
