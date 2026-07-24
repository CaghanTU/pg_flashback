-- Test: Table with a composite (multi-column) primary key restores correctly.
-- Composite PKs use batch net-effect replay path just like single-column PKs.
DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/3000'::pg_lsn;
    v_cnt    bigint;
    v_qty    int;
BEGIN
    DROP TABLE IF EXISTS public.it_cpk CASCADE;
    CREATE TABLE public.it_cpk (
        warehouse_id   int  NOT NULL,
        product_sku    text NOT NULL,
        qty            int  NOT NULL DEFAULT 0,
        PRIMARY KEY (warehouse_id, product_sku)
    );

    SELECT flashback_test_bootstrap_lifecycle('public.it_cpk') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO public.it_cpk VALUES
        (1, 'SKU-A', 100),
        (1, 'SKU-B', 200),
        (2, 'SKU-A', 50),
        (2, 'SKU-C', 75);
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        939001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"warehouse_id":1,"product_sku":"SKU-A","qty":100}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"warehouse_id":1,"product_sku":"SKU-B","qty":200}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"warehouse_id":2,"product_sku":"SKU-A","qty":50}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"warehouse_id":2,"product_sku":"SKU-C","qty":75}'::jsonb)
        )
    );

    UPDATE public.it_cpk SET qty = 999 WHERE warehouse_id = 1 AND product_sku = 'SKU-A';
    UPDATE public.it_cpk SET qty = 888 WHERE warehouse_id = 2 AND product_sku = 'SKU-A';
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        v_point_lsn,
        clock_timestamp(),
        939002,
        jsonb_build_array(
            jsonb_build_object('op', 'UPDATE', 'old', '{"warehouse_id":1,"product_sku":"SKU-A","qty":100}'::jsonb, 'new', '{"warehouse_id":1,"product_sku":"SKU-A","qty":999}'::jsonb),
            jsonb_build_object('op', 'UPDATE', 'old', '{"warehouse_id":2,"product_sku":"SKU-A","qty":50}'::jsonb, 'new', '{"warehouse_id":2,"product_sku":"SKU-A","qty":888}'::jsonb)
        )
    );

    DELETE FROM public.it_cpk WHERE warehouse_id = 1;
    UPDATE public.it_cpk SET qty = 0 WHERE warehouse_id = 2;
    INSERT INTO public.it_cpk VALUES (3, 'SKU-X', 1);
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/4000'::pg_lsn,
        clock_timestamp(),
        939003,
        jsonb_build_array(
            jsonb_build_object('op', 'DELETE', 'old', '{"warehouse_id":1,"product_sku":"SKU-A","qty":999}'::jsonb),
            jsonb_build_object('op', 'DELETE', 'old', '{"warehouse_id":1,"product_sku":"SKU-B","qty":200}'::jsonb),
            jsonb_build_object('op', 'UPDATE', 'old', '{"warehouse_id":2,"product_sku":"SKU-A","qty":888}'::jsonb, 'new', '{"warehouse_id":2,"product_sku":"SKU-A","qty":0}'::jsonb),
            jsonb_build_object('op', 'UPDATE', 'old', '{"warehouse_id":2,"product_sku":"SKU-C","qty":75}'::jsonb, 'new', '{"warehouse_id":2,"product_sku":"SKU-C","qty":0}'::jsonb),
            jsonb_build_object('op', 'INSERT', 'new', '{"warehouse_id":3,"product_sku":"SKU-X","qty":1}'::jsonb)
        )
    );

    PERFORM flashback_restore_lsn('public.it_cpk', v_point_lsn);

    SELECT count(*) INTO v_cnt FROM public.it_cpk;
    IF v_cnt <> 4 THEN
        RAISE EXCEPTION 'expected 4 rows, got %', v_cnt;
    END IF;

    SELECT qty INTO v_qty
    FROM public.it_cpk WHERE warehouse_id = 1 AND product_sku = 'SKU-A';
    IF v_qty <> 999 THEN
        RAISE EXCEPTION 'composite PK row (1, SKU-A) wrong qty: expected 999, got %', v_qty;
    END IF;

    SELECT qty INTO v_qty
    FROM public.it_cpk WHERE warehouse_id = 2 AND product_sku = 'SKU-A';
    IF v_qty <> 888 THEN
        RAISE EXCEPTION 'composite PK row (2, SKU-A) wrong qty: expected 888, got %', v_qty;
    END IF;

    IF EXISTS (SELECT 1 FROM public.it_cpk WHERE warehouse_id = 3) THEN
        RAISE EXCEPTION 'post-snapshot row (3, SKU-X) should not exist after restore';
    END IF;

    DROP TABLE IF EXISTS public.it_cpk CASCADE;
END;
$tv$;
