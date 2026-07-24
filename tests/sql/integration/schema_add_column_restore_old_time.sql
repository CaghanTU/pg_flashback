DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/2000'::pg_lsn;
    v_xid bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_schema_add;
    CREATE TABLE public.it_schema_add (id int primary key, name text, status text);

    SELECT flashback_test_bootstrap_lifecycle('public.it_schema_add') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO public.it_schema_add VALUES (1,'n','a');
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        v_point_lsn,
        clock_timestamp(),
        931001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"name":"n","status":"a"}'::jsonb)
        )
    );

    PERFORM set_config('pg_flashback.capture_mode', 'wal', true);
    v_xid := (txid_current() % 4294967296)::bigint;
    ALTER TABLE public.it_schema_add ADD COLUMN discount numeric DEFAULT 0;
    PERFORM flashback_capture_ddl_event('ALTER', 'public', 'it_schema_add');
    UPDATE public.it_schema_add SET discount=10 WHERE id=1;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        v_xid,
        jsonb_build_array(
            jsonb_build_object(
                'op', 'UPDATE',
                'old', '{"id":1,"name":"n","status":"a"}'::jsonb,
                'new', '{"id":1,"name":"n","status":"a","discount":10}'::jsonb
            )
        )
    );

    PERFORM flashback_restore_lsn('public.it_schema_add', v_point_lsn);
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='it_schema_add' AND column_name='discount') THEN
      RAISE EXCEPTION 'discount should not exist at old time';
    END IF;
END;
$tv$;
