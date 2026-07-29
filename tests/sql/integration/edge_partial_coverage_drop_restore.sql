DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_boundary_lsn pg_lsn;
    v_xid bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_edge_partial;
    CREATE TABLE public.it_edge_partial (id int primary key, v text);
    INSERT INTO public.it_edge_partial VALUES (1, 'x');

    SELECT flashback_test_bootstrap_lifecycle('public.it_edge_partial') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;
    v_boundary_lsn := (v_boot->>'boundary_lsn')::pg_lsn;

    v_xid := (txid_current() % 4294967296)::bigint;
    -- The DDL hook already captures this literal DROP for real (manifest +
    -- pending event under the current transaction's real xid); only finalize
    -- that pending event here, never restage a second, competing one.
    DROP TABLE public.it_edge_partial;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        v_xid,
        '[]'::jsonb
    );
    PERFORM flashback_bind_drop_dependency_manifests();

    PERFORM flashback_test_restore_lsn('public.it_edge_partial', v_boundary_lsn);
    IF to_regclass('public.it_edge_partial') IS NULL THEN
        RAISE EXCEPTION 'partial drop restore failed';
    END IF;
END;
$tv$;
