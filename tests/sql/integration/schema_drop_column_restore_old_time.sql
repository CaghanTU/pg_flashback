DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/2000'::pg_lsn;
    v_xid bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_schema_dropcol;
    CREATE TABLE public.it_schema_dropcol (id int primary key, name text, status text);

    SELECT flashback_test_bootstrap_lifecycle('public.it_schema_dropcol') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO public.it_schema_dropcol VALUES (1,'n','a');
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        v_point_lsn,
        clock_timestamp(),
        932001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"name":"n","status":"a"}'::jsonb)
        )
    );
    v_xid := (txid_current() % 4294967296)::bigint;
    ALTER TABLE public.it_schema_dropcol DROP COLUMN status;
    PERFORM flashback_test_inject_ddl_commit(
        v_tracking_id,
        '0/2600'::pg_lsn,
        clock_timestamp(),
        (txid_current() % 4294967296)::bigint,
        'ALTER'
    );
    UPDATE public.it_schema_dropcol SET name='x' WHERE id=1;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        v_xid,
        jsonb_build_array(
            jsonb_build_object(
                'op', 'UPDATE',
                'old', '{"id":1,"name":"n","status":"a"}'::jsonb,
                'new', '{"id":1,"name":"x"}'::jsonb
            )
        )
    );

    PERFORM flashback_test_restore_lsn('public.it_schema_dropcol', v_point_lsn);
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='it_schema_dropcol' AND column_name='status') THEN
      RAISE EXCEPTION 'status should be present at old time';
    END IF;
END;
$tv$;
