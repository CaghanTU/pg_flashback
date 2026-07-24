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

-- Scenarios 12-15 (existing-snapshot upgrade backfill copies no data,
-- pg_dump/restore preserves locator/ownership, capacity/health calculations
-- match prior results, full track->DML->DROP->recover->successor->retention
-- lifecycle) are not representable inside cargo pgrx test's single
-- always-rolled-back transaction: backfill needs a genuinely pre-migration
-- on-disk schema, and pg_dump/restore needs a real committed database and
-- an external pg_dump process. They are covered instead by
-- scripts/run_extension_upgrade_e2e.sh, scripts/run_pgdump_e2e.sh, and the
-- existing tests::pg_it_local_capacity_admission /
-- tests::pg_it_slot_health_actions / tests::pg_it_coverage_lifecycle_hardening
-- / tests::pg_it_recover_deleted_rows suites, all of which exercise the
-- SnapshotStore-migrated call sites in local_capacity.sql, monitoring_cache.sql,
-- health_runtime.sql and the full track/DML/DROP/recover/reanchor/retention
-- chain respectively.
