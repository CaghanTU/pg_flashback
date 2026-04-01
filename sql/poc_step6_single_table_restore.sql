-- Step 6 PoC: single-table end-to-end restore (track + capture + reverse replay)
-- Usage:
--   /usr/local/pgsql-17/bin/psql -h /home/caghan.linux/.pgrx -p 28817 -d postgres -v ON_ERROR_STOP=1 -f sql/poc_step6_single_table_restore.sql

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

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    id bigint PRIMARY KEY,
    customer_name text,
    status text,
    amount numeric(12,2)
);

INSERT INTO orders (id, customer_name, status, amount)
VALUES
    (5, 'Ada', 'pending', 120.00),
    (10, 'Lin', 'approved', 300.00),
    (20, 'Mira', 'packed', 99.50);

SELECT flashback_track('public.orders');

CREATE TEMP TABLE restore_target AS
SELECT clock_timestamp() AS target_time;

UPDATE orders SET status = 'cancelled' WHERE id = 5;
DELETE FROM orders WHERE id = 10;
INSERT INTO orders (id, customer_name, status, amount)
VALUES (30, 'Zed', 'new', 42.42);

-- Drives logical decoding callbacks so enqueue path runs.
SELECT lsn, xid, data
FROM pg_logical_slot_get_changes('flashback_slot', NULL, NULL)
WHERE data LIKE '%table=orders%'
ORDER BY lsn;

-- Give background worker a moment to flush.
SELECT pg_sleep(1);

SELECT event_id, event_time, event_type, table_name, rel_oid, old_data, new_data
FROM flashback.delta_log
WHERE table_name = 'public.orders'
ORDER BY event_id;

SELECT flashback_restore('public.orders', (SELECT target_time FROM restore_target)) AS restored_events;

SELECT id, customer_name, status, amount
FROM orders
ORDER BY id;
