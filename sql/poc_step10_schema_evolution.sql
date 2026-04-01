DROP EXTENSION IF EXISTS pg_flashback CASCADE;
CREATE EXTENSION pg_flashback;

SELECT pg_drop_replication_slot('flashback_slot')
WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'flashback_slot');

SELECT * FROM pg_create_logical_replication_slot('flashback_slot', 'pg_flashback');

TRUNCATE flashback.delta_log;
TRUNCATE flashback.snapshots RESTART IDENTITY;
TRUNCATE flashback.tracked_tables;
TRUNCATE flashback.schema_versions RESTART IDENTITY;

DROP TABLE IF EXISTS public.orders CASCADE;
CREATE TABLE public.orders (
    id int PRIMARY KEY,
    name text,
    status text
);

SELECT flashback_track('public.orders');

INSERT INTO public.orders VALUES (1, 'test', 'active');

SELECT clock_timestamp() AS before_alter_time \gset

ALTER TABLE public.orders ADD COLUMN discount numeric DEFAULT 0;

SELECT clock_timestamp() AS after_alter_time \gset

UPDATE public.orders SET discount = 10 WHERE id = 1;

SELECT pg_sleep(1.2);

SELECT * FROM pg_logical_slot_peek_changes('flashback_slot', NULL, 200);
SELECT pg_sleep(1.0);

SELECT rel_oid, schema_version, table_name
FROM flashback.tracked_tables
WHERE table_name = 'orders';

SELECT schema_version, applied_at
FROM flashback.schema_versions
WHERE rel_oid = to_regclass('public.orders')::oid
ORDER BY schema_version;

SELECT flashback_restore('public.orders', :'before_alter_time'::timestamptz) AS restored_before_alter;

DO $$
DECLARE
    has_discount boolean;
    row_ok boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'orders'
          AND column_name = 'discount'
    )
      INTO has_discount;

    IF has_discount THEN
        RAISE EXCEPTION 'Expected no discount column after restore to pre-ALTER point';
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM public.orders
        WHERE id = 1
          AND name = 'test'
          AND status = 'active'
    )
      INTO row_ok;

    IF NOT row_ok THEN
        RAISE EXCEPTION 'Expected row id=1,name=test,status=active after restore to pre-ALTER point';
    END IF;
END;
$$;

SELECT flashback_restore('public.orders', :'after_alter_time'::timestamptz) AS restored_after_alter;

DO $$
DECLARE
    has_discount boolean;
    discount_zero boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'orders'
          AND column_name = 'discount'
    )
      INTO has_discount;

    IF NOT has_discount THEN
        RAISE EXCEPTION 'Expected discount column after restore to post-ALTER point';
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM public.orders
        WHERE id = 1
          AND COALESCE(discount, 0) = 0
    )
      INTO discount_zero;

    IF NOT discount_zero THEN
        RAISE EXCEPTION 'Expected discount default value for version-1 INSERT after restore to post-ALTER point';
    END IF;
END;
$$;

TABLE public.orders;
