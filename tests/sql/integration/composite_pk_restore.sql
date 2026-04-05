-- Test: Table with a composite (multi-column) primary key restores correctly.
-- Composite PKs use batch net-effect replay path just like single-column PKs.
DO $tv$
DECLARE
    t_before timestamptz;
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

    PERFORM flashback_track('public.it_cpk');
    PERFORM flashback_test_attach_capture_trigger('public.it_cpk'::regclass);

    INSERT INTO public.it_cpk VALUES
        (1, 'SKU-A', 100),
        (1, 'SKU-B', 200),
        (2, 'SKU-A', 50),
        (2, 'SKU-C', 75);

    -- Several updates before the checkpoint time
    UPDATE public.it_cpk SET qty = 999 WHERE warehouse_id = 1 AND product_sku = 'SKU-A';
    UPDATE public.it_cpk SET qty = 888 WHERE warehouse_id = 2 AND product_sku = 'SKU-A';

    t_before := clock_timestamp();

    -- Post-snapshot mutations we want undone
    DELETE FROM public.it_cpk WHERE warehouse_id = 1;
    UPDATE public.it_cpk SET qty = 0 WHERE warehouse_id = 2;
    INSERT INTO public.it_cpk VALUES (3, 'SKU-X', 1);

    PERFORM flashback_restore('public.it_cpk', t_before);

    -- Expect exactly the 4 rows that existed at t_before
    SELECT count(*) INTO v_cnt FROM public.it_cpk;
    IF v_cnt <> 4 THEN
        RAISE EXCEPTION 'expected 4 rows, got %', v_cnt;
    END IF;

    -- Verify specific composite PK row
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

    -- (3, SKU-X) must NOT exist (it was inserted after t_before)
    IF EXISTS (SELECT 1 FROM public.it_cpk WHERE warehouse_id = 3) THEN
        RAISE EXCEPTION 'post-snapshot row (3, SKU-X) should not exist after restore';
    END IF;

    DROP TABLE IF EXISTS public.it_cpk CASCADE;
END;
$tv$;
