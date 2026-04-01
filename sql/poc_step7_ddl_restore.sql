-- Step 7 PoC: utility-hook based DDL capture and restore (TRUNCATE + DROP)
-- Usage:
--   /usr/local/pgsql-17/bin/psql -h /home/caghan.linux/.pgrx -p 28817 -d postgres -v ON_ERROR_STOP=1 -f sql/poc_step7_ddl_restore.sql

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
    (1, 'Ada', 'new', 50.00),
    (2, 'Lin', 'ready', 75.50),
    (3, 'Mira', 'paid', 99.99);

SELECT flashback_track('public.orders');

CREATE TEMP TABLE truncate_target AS
SELECT clock_timestamp() AS target_time;

TRUNCATE TABLE orders;

SELECT event_id, event_type, table_name, old_data, ddl_info
FROM flashback.delta_log
WHERE table_name = 'public.orders'
  AND event_type = 'TRUNCATE'
ORDER BY event_id;

SELECT flashback_restore('public.orders', (SELECT target_time FROM truncate_target)) AS truncate_restored;

SELECT id, customer_name, status, amount
FROM orders
ORDER BY id;

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
    (11, 'Arda', 'new', 10.00),
    (12, 'Cem', 'paid', 20.00);

SELECT flashback_track('public.orders');

CREATE TEMP TABLE drop_target AS
SELECT clock_timestamp() AS target_time;

DROP TABLE orders;

SELECT event_id, event_type, table_name, old_data, ddl_info
FROM flashback.delta_log
WHERE table_name = 'public.orders'
  AND event_type = 'DROP'
ORDER BY event_id;

SELECT flashback_restore('public.orders', (SELECT target_time FROM drop_target)) AS drop_restored;

SELECT id, customer_name, status, amount
FROM orders
ORDER BY id;
