DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/2000'::pg_lsn;
    v_xid bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_schema_type;
    CREATE TABLE public.it_schema_type (id int primary key, amount int);

    SELECT flashback_test_bootstrap_lifecycle('public.it_schema_type') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO public.it_schema_type VALUES (1, 10);
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        v_point_lsn,
        clock_timestamp(),
        933001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"amount":10}'::jsonb)
        )
    );
    v_xid := (txid_current() % 4294967296)::bigint;
    ALTER TABLE public.it_schema_type ALTER COLUMN amount TYPE numeric USING amount::numeric;
    PERFORM flashback_test_inject_ddl_commit(
        v_tracking_id,
        '0/2600'::pg_lsn,
        clock_timestamp(),
        (txid_current() % 4294967296)::bigint,
        'ALTER'
    );
    UPDATE public.it_schema_type SET amount=12.5 WHERE id=1;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        v_xid,
        jsonb_build_array(
            jsonb_build_object(
                'op', 'UPDATE',
                'old', '{"id":1,"amount":10}'::jsonb,
                'new', '{"id":1,"amount":12.5}'::jsonb
            )
        )
    );

    PERFORM flashback_test_restore_lsn('public.it_schema_type', v_point_lsn);
    IF (SELECT data_type FROM information_schema.columns WHERE table_schema='public' AND table_name='it_schema_type' AND column_name='amount') <> 'integer' THEN
      RAISE EXCEPTION 'amount should be integer at old time';
    END IF;
END;
$tv$;
