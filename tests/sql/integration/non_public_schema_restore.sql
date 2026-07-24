-- Test: Tables in a non-public schema are tracked and restored correctly.
-- Validates that schema qualification is handled throughout capture/restore.
DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/2000'::pg_lsn;
    v_cnt bigint;
    v_val text;
BEGIN
    CREATE SCHEMA IF NOT EXISTS it_ns;

    DROP TABLE IF EXISTS it_ns.orders CASCADE;
    CREATE TABLE it_ns.orders (
        order_id   serial PRIMARY KEY,
        customer   text   NOT NULL,
        total      numeric(10,2)
    );

    SELECT flashback_test_bootstrap_lifecycle('it_ns.orders') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO it_ns.orders (order_id, customer, total) VALUES
        (1, 'Alice', 99.99),
        (2, 'Bob', 149.50),
        (3, 'Carol', 9.99);
    PERFORM setval(pg_get_serial_sequence('it_ns.orders', 'order_id'), 3, true);
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        v_point_lsn,
        clock_timestamp(),
        943001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"order_id":1,"customer":"Alice","total":99.99}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"order_id":2,"customer":"Bob","total":149.50}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"order_id":3,"customer":"Carol","total":9.99}'::jsonb)
        )
    );

    DELETE FROM it_ns.orders;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        943002,
        jsonb_build_array(
            jsonb_build_object('op', 'DELETE', 'old', '{"order_id":1,"customer":"Alice","total":99.99}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"order_id":2,"customer":"Bob","total":149.50}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"order_id":3,"customer":"Carol","total":9.99}'::jsonb)
        )
    );

    IF (SELECT count(*) FROM it_ns.orders) <> 0 THEN
        RAISE EXCEPTION 'delete did not take effect';
    END IF;

    PERFORM flashback_restore_lsn('it_ns.orders', v_point_lsn);

    SELECT count(*) INTO v_cnt FROM it_ns.orders;
    IF v_cnt <> 3 THEN
        RAISE EXCEPTION 'expected 3 rows after restore, got %', v_cnt;
    END IF;

    SELECT customer INTO v_val FROM it_ns.orders WHERE total = 99.99;
    IF v_val <> 'Alice' THEN
        RAISE EXCEPTION 'wrong customer data after restore: got ''%''', v_val;
    END IF;

    DROP TABLE IF EXISTS it_ns.orders CASCADE;
    DROP SCHEMA IF EXISTS it_ns CASCADE;
END;
$tv$;
