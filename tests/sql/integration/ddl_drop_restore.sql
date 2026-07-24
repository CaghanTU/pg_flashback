DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_boundary_lsn pg_lsn;
    v_xid bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_ddl_drop;
    CREATE TABLE public.it_ddl_drop (id int primary key, status text);
    INSERT INTO public.it_ddl_drop VALUES (1, 'a'), (2, 'b');

    SELECT flashback_test_bootstrap_lifecycle('public.it_ddl_drop') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;
    v_boundary_lsn := (v_boot->>'boundary_lsn')::pg_lsn;

    PERFORM flashback_capture_drop_dependency_manifest('public', 'it_ddl_drop', false);
    v_xid := (txid_current() % 4294967296)::bigint;
    DROP TABLE public.it_ddl_drop;
    PERFORM flashback_test_inject_ddl_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        v_xid,
        'DROP'
    );
    PERFORM flashback_bind_drop_dependency_manifests();

    PERFORM flashback_test_restore_lsn('public.it_ddl_drop', v_boundary_lsn);
    IF to_regclass('public.it_ddl_drop') IS NULL THEN
        RAISE EXCEPTION 'drop restore did not recreate table';
    END IF;
    IF (SELECT count(*) FROM public.it_ddl_drop) <> 2 THEN
        RAISE EXCEPTION 'drop restore row count mismatch';
    END IF;
END;
$tv$;
