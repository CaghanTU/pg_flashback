-- Test: restore with parallel worker GUCs enabled (LSN API; timestamp parallel
-- wrapper is disabled for correctness-qualified WAL generations).
DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_boundary_lsn pg_lsn;
    v_cnt bigint;
    v_events_json jsonb;
BEGIN
    DROP TABLE IF EXISTS public.it_parallel_restore CASCADE;

    CREATE TABLE public.it_parallel_restore (
        id    serial PRIMARY KEY,
        val   text,
        score integer
    );

    SELECT flashback_test_bootstrap_lifecycle('public.it_parallel_restore') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;
    v_boundary_lsn := (v_boot->>'boundary_lsn')::pg_lsn;

    INSERT INTO public.it_parallel_restore (id, val, score)
    SELECT gs.id, 'item_' || gs.id, gs.id FROM generate_series(1, 200) AS gs(id);
    PERFORM setval(pg_get_serial_sequence('public.it_parallel_restore', 'id'), 200, true);

    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'op', 'INSERT',
            'new', jsonb_build_object(
                'id', gs.id,
                'val', 'item_' || gs.id,
                'score', gs.id
            )
        )
        ORDER BY gs.id
    ), '[]'::jsonb)
      INTO v_events_json
    FROM generate_series(1, 200) AS gs(id);

    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        TIMESTAMPTZ '2026-07-24 10:00:00+00',
        957001,
        v_events_json
    );

    UPDATE public.it_parallel_restore SET val = 'DISASTER', score = -1;
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'op', 'UPDATE',
            'old', jsonb_build_object('id', gs.id, 'val', 'item_' || gs.id, 'score', gs.id),
            'new', jsonb_build_object('id', gs.id, 'val', 'DISASTER', 'score', -1)
        )
        ORDER BY gs.id
    ), '[]'::jsonb)
      INTO v_events_json
    FROM generate_series(1, 200) AS gs(id);

    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        957002,
        v_events_json
    );

    SELECT count(*) INTO v_cnt FROM public.it_parallel_restore WHERE val = 'DISASTER';
    IF v_cnt <> 200 THEN
        RAISE EXCEPTION 'setup: expected 200 DISASTER rows, got %', v_cnt;
    END IF;

    PERFORM set_config('max_parallel_workers_per_gather', '2', true);
    PERFORM set_config('max_parallel_maintenance_workers', '2', true);
    PERFORM set_config('parallel_leader_participation', 'on', true);

    PERFORM flashback_test_restore_lsn('public.it_parallel_restore', '0/2000'::pg_lsn);

    SELECT count(*) INTO v_cnt FROM public.it_parallel_restore WHERE val LIKE 'item_%';
    IF v_cnt <> 200 THEN
        RAISE EXCEPTION 'parallel restore: expected 200 original rows, got %', v_cnt;
    END IF;

    -- No terminal DROP TABLE: pg_test rolls back this whole transaction, and
    -- the restore just performed leaves the successor generation "building"
    -- (not yet active) until that rollback/commit is observed, so a
    -- same-transaction DROP here would trip the schema-contract guard.
END;
$tv$;
