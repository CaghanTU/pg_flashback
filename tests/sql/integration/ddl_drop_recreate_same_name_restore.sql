DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_boundary_lsn pg_lsn;
    v_xid bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_ddl_drop_recreate;
    CREATE TABLE public.it_ddl_drop_recreate (id int primary key, status text);
    INSERT INTO public.it_ddl_drop_recreate VALUES (1, 'old');

    SELECT flashback_test_bootstrap_lifecycle('public.it_ddl_drop_recreate') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;
    v_boundary_lsn := (v_boot->>'boundary_lsn')::pg_lsn;

    PERFORM flashback_capture_drop_dependency_manifest(
        'public', 'it_ddl_drop_recreate', false
    );
    v_xid := (txid_current() % 4294967296)::bigint;
    DROP TABLE public.it_ddl_drop_recreate;
    PERFORM flashback_test_inject_ddl_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        v_xid,
        'DROP'
    );
    PERFORM flashback_bind_drop_dependency_manifests();

    CREATE TABLE public.it_ddl_drop_recreate (
        id int primary key, status text, note text
    );
    INSERT INTO public.it_ddl_drop_recreate VALUES (2, 'new', 'x');

    BEGIN
        PERFORM flashback_test_restore_lsn('public.it_ddl_drop_recreate', v_boundary_lsn);
        RAISE EXCEPTION 'expected restore to refuse identity-mismatched live relation';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM NOT ILIKE '%different relation already uses that name%'
           AND SQLERRM NOT ILIKE '%identity-mismatched%'
           AND SQLERRM NOT ILIKE '%refusing restore%'
        THEN
            RAISE;
        END IF;
    END;

    DROP TABLE public.it_ddl_drop_recreate;
    PERFORM flashback_test_restore_lsn('public.it_ddl_drop_recreate', v_boundary_lsn);

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'it_ddl_drop_recreate'
          AND column_name = 'note'
    ) THEN
        RAISE EXCEPTION 'restored table still has recreated shape';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM public.it_ddl_drop_recreate WHERE id = 1 AND status = 'old'
    ) THEN
        RAISE EXCEPTION 'old table content not restored';
    END IF;
END;
$tv$;
