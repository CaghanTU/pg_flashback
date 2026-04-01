-- =============================================================
-- pg_flashback DBA Scenario: E-Commerce Disaster Recovery
-- =============================================================
-- This script is meant to be executed with psql (-v ON_ERROR_STOP=1).

\set ON_ERROR_STOP on

CREATE EXTENSION IF NOT EXISTS pg_flashback;

-- Idempotent cleanup for previous interrupted runs.
WITH stale AS (
    SELECT rel_oid
    FROM flashback.tracked_tables
    WHERE schema_name = 'public' AND table_name IN ('orders', 'order_items')
)
DELETE FROM flashback.delta_log d
USING stale s
WHERE d.rel_oid = s.rel_oid;

WITH stale AS (
    SELECT rel_oid
    FROM flashback.tracked_tables
    WHERE schema_name = 'public' AND table_name IN ('orders', 'order_items')
)
DELETE FROM flashback.snapshots sn
USING stale s
WHERE sn.rel_oid = s.rel_oid;

DELETE FROM flashback.schema_versions
WHERE rel_oid IN (
    SELECT rel_oid
    FROM flashback.tracked_tables
    WHERE schema_name = 'public' AND table_name IN ('orders', 'order_items')
);

DELETE FROM flashback.tracked_tables
WHERE schema_name = 'public' AND table_name IN ('orders', 'order_items');

-- Trigger-mode scenario does not require a logical replication slot.

DROP TABLE IF EXISTS public.order_items CASCADE;
DROP TABLE IF EXISTS public.orders CASCADE;
DROP TABLE IF EXISTS public.customers CASCADE;

CREATE TABLE public.customers (
    id          serial PRIMARY KEY,
    name        text NOT NULL,
    email       text NOT NULL UNIQUE,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE public.orders (
    id          serial PRIMARY KEY,
    customer_id integer NOT NULL REFERENCES public.customers(id),
    status      text NOT NULL DEFAULT 'pending',
    total       numeric(12,2) NOT NULL DEFAULT 0,
    notes       text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_orders_status ON public.orders(status);
CREATE INDEX idx_orders_customer ON public.orders(customer_id);

CREATE TABLE public.order_items (
    id          serial PRIMARY KEY,
    order_id    integer NOT NULL REFERENCES public.orders(id) ON DELETE CASCADE,
    product     text NOT NULL,
    qty         integer NOT NULL DEFAULT 1,
    price       numeric(10,2) NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_items_order ON public.order_items(order_id);

CREATE TEMP TABLE scenario_metrics (
    metric text PRIMARY KEY,
    value  bigint NOT NULL
);

\echo 'PHASE 0: Seed data'
INSERT INTO public.customers (name, email)
SELECT 'Customer ' || g, 'cust' || g || '@example.com'
FROM generate_series(1, 100) g;

INSERT INTO public.orders (customer_id, status, total, notes)
SELECT
    (g % 100) + 1,
    CASE g % 5
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'shipped'
        WHEN 2 THEN 'delivered'
        WHEN 3 THEN 'pending'
        WHEN 4 THEN 'processing'
    END,
    round((random() * 500 + 10)::numeric, 2),
    CASE WHEN g % 3 = 0 THEN 'Rush delivery' ELSE NULL END
FROM generate_series(1, 500) g;

INSERT INTO public.order_items (order_id, product, qty, price)
SELECT
    ((g - 1) % 500) + 1,
    'Product-' || (g % 50),
    (g % 5) + 1,
    round((random() * 100 + 5)::numeric, 2)
FROM generate_series(1, 1500) g;

SELECT 'phase0_seed' AS phase,
       (SELECT count(*) FROM public.customers) AS customers,
       (SELECT count(*) FROM public.orders) AS orders,
       (SELECT count(*) FROM public.order_items) AS items;

\echo 'PHASE 1: Start tracking'
SELECT flashback_track('public.orders');
SELECT flashback_track('public.order_items');

SELECT 'phase1_tracking' AS phase, count(*) AS tracked_count
FROM flashback.tracked_tables
WHERE schema_name = 'public' AND table_name IN ('orders', 'order_items');

\echo 'PHASE 2: Normal workload'
INSERT INTO public.orders (customer_id, status, total)
SELECT (g % 100) + 1, 'pending', round((random() * 200 + 20)::numeric, 2)
FROM generate_series(501, 550) g;

INSERT INTO public.order_items (order_id, product, qty, price)
SELECT o.id, 'NewProduct-' || (o.id % 10), 2, 49.99
FROM public.orders o WHERE o.id > 500;

UPDATE public.orders SET status = 'shipped', updated_at = now()
WHERE status = 'pending' AND id % 7 = 0;

UPDATE public.order_items SET qty = qty + 1
WHERE order_id IN (SELECT id FROM public.orders WHERE status = 'shipped' LIMIT 20);

DELETE FROM public.order_items WHERE order_id IN (
    SELECT id FROM public.orders WHERE id % 50 = 0 AND status = 'pending'
);
DELETE FROM public.orders WHERE id % 50 = 0 AND status = 'pending';

SELECT pg_sleep(1);

SELECT 'phase2_normal_workload' AS phase,
       (SELECT count(*) FROM public.orders) AS orders,
       (SELECT count(*) FROM public.order_items) AS items;

\echo 'PHASE 3: Take checkpoint'
SELECT flashback_checkpoint('public.orders');
SELECT flashback_checkpoint('public.order_items');

SELECT 'phase3_checkpoint' AS phase,
         (SELECT count(*)
             FROM flashback.snapshots s
            WHERE s.rel_oid IN ('public.orders'::regclass::oid, 'public.order_items'::regclass::oid)) AS snapshot_rows;

\echo 'PHASE 4: More normal workload'
UPDATE public.orders SET total = total * 1.1 WHERE status = 'delivered';
INSERT INTO public.order_items (order_id, product, qty, price)
VALUES (1, 'BonusItem', 1, 0.00);

SELECT pg_sleep(1);

SELECT 'phase4_post_checkpoint_workload' AS phase,
       (SELECT count(*) FROM public.orders) AS orders,
       (SELECT count(*) FROM public.order_items) AS items;

\echo 'PHASE 5: Record pre-disaster state'
SELECT clock_timestamp() AS disaster_timestamp \gset
SELECT pg_sleep(1);

INSERT INTO scenario_metrics(metric, value)
SELECT 'phase5_orders_before', count(*) FROM public.orders;
INSERT INTO scenario_metrics(metric, value)
SELECT 'phase5_items_before', count(*) FROM public.order_items;
INSERT INTO scenario_metrics(metric, value)
SELECT 'phase5_pending_before', count(*) FROM public.orders WHERE status = 'pending';

SELECT 'phase5_pre_disaster' AS phase,
       (SELECT value FROM scenario_metrics WHERE metric = 'phase5_orders_before') AS orders_before,
       (SELECT value FROM scenario_metrics WHERE metric = 'phase5_items_before') AS items_before,
       (SELECT value FROM scenario_metrics WHERE metric = 'phase5_pending_before') AS pending_before,
       :'disaster_timestamp'::timestamptz AS restore_target_time;

\echo 'PHASE 6: Disaster (bad deployment)'
UPDATE public.orders
SET status = 'cancelled', updated_at = now(), notes = 'auto-cancelled by deploy v2.3.1'
WHERE status = 'pending';

DELETE FROM public.order_items
WHERE order_id IN (SELECT id FROM public.orders WHERE status = 'cancelled');

SELECT pg_sleep(1);

SELECT 'phase6_disaster' AS phase,
       (SELECT count(*) FROM public.orders) AS orders_after,
       (SELECT count(*) FROM public.order_items) AS items_after,
       (SELECT count(*) FROM public.orders WHERE status = 'cancelled') AS cancelled_orders;

\echo 'PHASE 7: Multi-table restore'
SELECT flashback_restore(
    ARRAY['public.orders', 'public.order_items'],
    :'disaster_timestamp'::timestamptz
) AS restored_events;

SELECT 'phase7_restore_complete' AS phase,
       (SELECT count(*) FROM public.orders) AS orders,
       (SELECT count(*) FROM public.order_items) AS items;

\echo 'PHASE 8: Verification'
INSERT INTO scenario_metrics(metric, value)
SELECT 'phase8_orders_restored', count(*) FROM public.orders;
INSERT INTO scenario_metrics(metric, value)
SELECT 'phase8_items_restored', count(*) FROM public.order_items;
INSERT INTO scenario_metrics(metric, value)
SELECT 'phase8_pending_restored', count(*) FROM public.orders WHERE status = 'pending';

SELECT 'phase8_verification' AS phase,
       (SELECT value FROM scenario_metrics WHERE metric = 'phase8_orders_restored') AS orders_restored,
       (SELECT value FROM scenario_metrics WHERE metric = 'phase8_items_restored') AS items_restored,
       (SELECT value FROM scenario_metrics WHERE metric = 'phase8_pending_restored') AS pending_restored,
       (SELECT count(*)
          FROM public.order_items oi
          LEFT JOIN public.orders o ON o.id = oi.order_id
         WHERE o.id IS NULL) AS orphan_items,
       CASE
           WHEN (SELECT value FROM scenario_metrics WHERE metric = 'phase5_orders_before')
              = (SELECT value FROM scenario_metrics WHERE metric = 'phase8_orders_restored')
            AND (SELECT value FROM scenario_metrics WHERE metric = 'phase5_items_before')
              = (SELECT value FROM scenario_metrics WHERE metric = 'phase8_items_restored')
            AND (SELECT value FROM scenario_metrics WHERE metric = 'phase5_pending_before')
              = (SELECT value FROM scenario_metrics WHERE metric = 'phase8_pending_restored')
            AND (SELECT count(*)
                   FROM public.order_items oi
                   LEFT JOIN public.orders o ON o.id = oi.order_id
                  WHERE o.id IS NULL) = 0
           THEN 'PASS'
           ELSE 'FAIL'
       END AS verification_status;

\echo 'PHASE 9: History query'
SELECT event_time, event_type, row_identity
FROM flashback_history('public.orders', interval '10 minutes')
ORDER BY event_time DESC
LIMIT 20;

\echo 'PHASE 10: Schema evolution'
SELECT pg_sleep(1);
SELECT clock_timestamp() AS pre_alter_timestamp \gset

ALTER TABLE public.orders ADD COLUMN priority integer DEFAULT 0;
UPDATE public.orders SET priority = 1 WHERE status = 'shipped';

SELECT pg_sleep(1);

SELECT flashback_restore('public.orders', :'pre_alter_timestamp'::timestamptz) AS restored_before_alter;

SELECT 'phase10_schema_evolution' AS phase,
       EXISTS (
           SELECT 1
             FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'orders'
              AND column_name = 'priority'
       ) AS has_priority_column;

\echo 'PHASE 11: TRUNCATE restore'
SELECT pg_sleep(1);
SELECT clock_timestamp() AS pre_truncate_timestamp \gset

TRUNCATE public.order_items;

SELECT flashback_restore('public.order_items', :'pre_truncate_timestamp'::timestamptz) AS restored_after_truncate;

SELECT 'phase11_truncate_restore' AS phase,
       (SELECT count(*) FROM public.order_items) AS items_after_truncate_restore;

\echo 'PHASE 12: DROP TABLE restore'
SELECT pg_sleep(1);
SELECT clock_timestamp() AS pre_drop_timestamp \gset

DROP TABLE public.order_items;

SELECT flashback_restore('public.order_items', :'pre_drop_timestamp'::timestamptz) AS restored_after_drop;

SELECT 'phase12_drop_restore' AS phase,
       (SELECT count(*) FROM public.order_items) AS items_after_drop_restore;

\echo 'PHASE 13: Cleanup'
SELECT flashback_untrack('public.orders');
SELECT flashback_untrack('public.order_items');

SELECT 'phase13_cleanup' AS phase,
    (SELECT count(*) FROM flashback.tracked_tables WHERE schema_name = 'public' AND table_name IN ('orders', 'order_items')) AS tracked_after_cleanup,
    (SELECT count(*) FROM flashback.delta_log WHERE rel_oid IN ('public.orders'::regclass::oid, 'public.order_items'::regclass::oid)) AS deltas_after_cleanup,
    (SELECT count(*) FROM flashback.snapshots WHERE rel_oid IN ('public.orders'::regclass::oid, 'public.order_items'::regclass::oid)) AS snapshots_after_cleanup;

SELECT 'ALL PHASES COMPLETE' AS phase;
