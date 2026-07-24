DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/2000'::pg_lsn;
    v_xid bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_schema_multi;
    CREATE TABLE public.it_schema_multi (id int primary key, a text);

    SELECT flashback_test_bootstrap_lifecycle('public.it_schema_multi') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO public.it_schema_multi VALUES (1,'x');
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        v_point_lsn,
        clock_timestamp(),
        934001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"a":"x"}'::jsonb)
        )
    );

    PERFORM set_config('pg_flashback.capture_mode', 'wal', true);
    v_xid := (txid_current() % 4294967296)::bigint;
    ALTER TABLE public.it_schema_multi ADD COLUMN b int DEFAULT 0;
    PERFORM flashback_capture_ddl_event('ALTER', 'public', 'it_schema_multi');
    ALTER TABLE public.it_schema_multi ALTER COLUMN a TYPE varchar(50);
    PERFORM flashback_capture_ddl_event('ALTER', 'public', 'it_schema_multi');
    ALTER TABLE public.it_schema_multi DROP COLUMN b;
    PERFORM flashback_capture_ddl_event('ALTER', 'public', 'it_schema_multi');
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        v_xid,
        '[]'::jsonb
    );

    PERFORM flashback_restore_lsn('public.it_schema_multi', v_point_lsn);
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='it_schema_multi' AND column_name='b') THEN
      RAISE EXCEPTION 'b should not exist at oldest time';
    END IF;
END;
$tv$;
