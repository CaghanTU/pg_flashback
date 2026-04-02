-- Step 8 PoC: checkpoint + multi-table restore in one flow
-- Usage:
--   /usr/local/pgsql-17/bin/psql -h /home/caghan.linux/.pgrx -p 28817 -d postgres -v ON_ERROR_STOP=1 -f sql/poc_step8_checkpoint_multitable.sql

SELECT pg_drop_replication_slot('flashback_slot')
WHERE EXISTS (
    SELECT 1
    FROM pg_replication_slots
    WHERE slot_name = 'flashback_slot'
);

SELECT *
FROM pg_create_logical_replication_slot('flashback_slot', 'pg_flashback');

TRUNCATE flashback.delta_log;
TRUNCATE flashback.tracked_tables;
TRUNCATE flashback.snapshots;

DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    id bigint PRIMARY KEY,
    status text NOT NULL,
    amount numeric(12,2) NOT NULL
);

CREATE TABLE order_items (
    id bigint PRIMARY KEY,
    order_id bigint NOT NULL REFERENCES orders(id),
    product_id bigint NOT NULL,
    qty integer NOT NULL
);

INSERT INTO orders (id, status, amount)
VALUES
    (5, 'pending', 100.00),
    (6, 'pending', 80.00);

INSERT INTO order_items (id, order_id, product_id, qty)
VALUES
    (1001, 5, 1, 1),
    (1002, 5, 2, 2),
    (2001, 6, 3, 1);

SELECT flashback_track('orders');
SELECT flashback_track('order_items');

UPDATE flashback.tracked_tables
SET checkpoint_interval = interval '1 second'
WHERE table_name IN ('orders', 'order_items');

UPDATE orders SET status = 'processing' WHERE id = 5;
UPDATE order_items SET qty = 3 WHERE id = 1002;

SELECT lsn, xid, data
FROM pg_logical_slot_get_changes('flashback_slot', NULL, NULL)
WHERE data LIKE '%table=orders%'
   OR data LIKE '%table=order_items%'
ORDER BY lsn;

SELECT pg_sleep(2);

SELECT flashback_checkpoint('orders') AS manual_checkpoint_id;

UPDATE orders SET status = 'shipped' WHERE id = 5;
INSERT INTO order_items (id, order_id, product_id, qty)
VALUES (1003, 5, 9, 1);

SELECT lsn, xid, data
FROM pg_logical_slot_get_changes('flashback_slot', NULL, NULL)
WHERE data LIKE '%table=orders%'
   OR data LIKE '%table=order_items%'
ORDER BY lsn;

SELECT pg_sleep(2);

CREATE TEMP TABLE restore_target AS
SELECT clock_timestamp() AS target_time;

TRUNCATE orders CASCADE;
DELETE FROM order_items WHERE order_id = 5;

SELECT lsn, xid, data
FROM pg_logical_slot_get_changes('flashback_slot', NULL, NULL)
WHERE data LIKE '%table=orders%'
   OR data LIKE '%table=order_items%'
ORDER BY lsn;

SELECT pg_sleep(1);

SELECT tt.table_name, count(*) AS snapshot_count
FROM flashback.snapshots s
JOIN flashback.tracked_tables tt ON tt.rel_oid = s.rel_oid
GROUP BY tt.table_name
ORDER BY tt.table_name;

SELECT flashback_restore(
    tables := ARRAY['orders', 'order_items'],
    target_time := (SELECT target_time FROM restore_target)
) AS restored_events;

SELECT id, status, amount
FROM orders
ORDER BY id;

SELECT id, order_id, product_id, qty
FROM order_items
ORDER BY id;
