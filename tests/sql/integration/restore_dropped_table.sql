DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/2000'::pg_lsn;
    v_xid bigint;
    v_count bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_restore_dropped CASCADE;
    CREATE TABLE public.it_restore_dropped (
        id   SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );

    SELECT flashback_test_bootstrap_lifecycle('public.it_restore_dropped') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO public.it_restore_dropped (id, name)
    VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
    PERFORM setval(pg_get_serial_sequence('public.it_restore_dropped', 'id'), 3, true);
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        v_point_lsn,
        clock_timestamp(),
        941001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"name":"alice"}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":2,"name":"bob"}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":3,"name":"carol"}'::jsonb)
        )
    );

    v_xid := (txid_current() % 4294967296)::bigint;
    -- The DDL hook already captures this literal DROP for real (manifest +
    -- pending event under the current transaction's real xid); only finalize
    -- that pending event here, never restage a second, competing one.
    DROP TABLE public.it_restore_dropped CASCADE;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        v_xid,
        '[]'::jsonb
    );
    PERFORM flashback_bind_drop_dependency_manifests();

    PERFORM flashback_test_restore_lsn('public.it_restore_dropped', v_point_lsn);
    PERFORM flashback_test_resolve_post_restore_boundary(v_tracking_id, '0/3500'::pg_lsn);

    IF to_regclass('public.it_restore_dropped') IS NULL THEN
        RAISE EXCEPTION 'Table should exist after restore, but does not';
    END IF;

    SELECT count(*) INTO v_count FROM public.it_restore_dropped;
    IF v_count <> 3 THEN
        RAISE EXCEPTION 'Expected 3 rows after restore, got %', v_count;
    END IF;

    SELECT count(*) INTO v_count
    FROM flashback.tracked_tables
    WHERE schema_name = 'public' AND table_name = 'it_restore_dropped' AND is_active;
    IF v_count <> 1 THEN
        RAISE EXCEPTION 'Tracking should be re-activated after restore, but is not';
    END IF;

    PERFORM flashback_untrack('public.it_restore_dropped');
    DROP TABLE IF EXISTS public.it_restore_dropped CASCADE;
END;
$tv$;
