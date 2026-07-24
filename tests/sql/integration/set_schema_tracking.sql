DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_schema text;
    v_count bigint;
    v_xid bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_setschema_tbl;
    DROP SCHEMA IF EXISTS it_setschema_ns CASCADE;
    CREATE SCHEMA it_setschema_ns;

    CREATE TABLE public.it_setschema_tbl (id SERIAL PRIMARY KEY, val INT);

    SELECT flashback_test_bootstrap_lifecycle('public.it_setschema_tbl') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO public.it_setschema_tbl (id, val) VALUES (1, 10), (2, 20), (3, 30);
    PERFORM setval(pg_get_serial_sequence('public.it_setschema_tbl', 'id'), 3, true);
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        958001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"val":10}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":2,"val":20}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":3,"val":30}'::jsonb)
        )
    );

    v_xid := (txid_current() % 4294967296)::bigint;
    EXECUTE 'ALTER TABLE public.it_setschema_tbl SET SCHEMA it_setschema_ns';
    PERFORM flashback_test_inject_ddl_commit(
        v_tracking_id,
        '0/2500'::pg_lsn,
        clock_timestamp(),
        v_xid,
        'ALTER'
    );

    SELECT tt.schema_name INTO v_schema
    FROM flashback.tracked_tables tt
    WHERE tt.tracking_id = v_tracking_id;

    IF v_schema <> 'it_setschema_ns' THEN
        RAISE EXCEPTION 'B14: expected schema=it_setschema_ns, got %', v_schema;
    END IF;

    INSERT INTO it_setschema_ns.it_setschema_tbl (id, val) VALUES (4, 40), (5, 50);
    PERFORM setval(
        pg_get_serial_sequence('it_setschema_ns.it_setschema_tbl', 'id'), 5, true
    );
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        958002,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":4,"val":40}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"id":5,"val":50}'::jsonb)
        )
    );

    SELECT count(*) INTO v_count
    FROM flashback.delta_log
    WHERE tracking_id = v_tracking_id
      AND event_type = 'INSERT';

    IF v_count < 5 THEN
        RAISE EXCEPTION 'B14: expected >=5 INSERT events, got %', v_count;
    END IF;

    PERFORM flashback_untrack('it_setschema_ns.it_setschema_tbl');
    DROP TABLE IF EXISTS it_setschema_ns.it_setschema_tbl;
    DROP SCHEMA IF EXISTS it_setschema_ns CASCADE;
END;
$tv$;
