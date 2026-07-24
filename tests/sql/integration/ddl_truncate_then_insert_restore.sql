DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_boundary_lsn pg_lsn;
    v_xid bigint;
    c bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_ddl_trunc_ins;
    CREATE TABLE public.it_ddl_trunc_ins (id int primary key, status text);
    INSERT INTO public.it_ddl_trunc_ins VALUES (1,'a'),(2,'b');

    SELECT flashback_test_bootstrap_lifecycle('public.it_ddl_trunc_ins') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;
    v_boundary_lsn := (v_boot->>'boundary_lsn')::pg_lsn;

    v_xid := (txid_current() % 4294967296)::bigint;
    TRUNCATE TABLE public.it_ddl_trunc_ins;
    INSERT INTO public.it_ddl_trunc_ins VALUES (99,'z');
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        v_xid,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":99,"status":"z"}'::jsonb)
        )
    );

    PERFORM flashback_restore_lsn('public.it_ddl_trunc_ins', v_boundary_lsn);
    SELECT count(*) INTO c FROM public.it_ddl_trunc_ins;
    IF c <> 2 THEN RAISE EXCEPTION 'truncate+insert restore failed'; END IF;
END;
$tv$;
