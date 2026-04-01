-- Step 9 PoC: history API + untrack cleanup + worker retention purge
-- Usage:
--   /usr/local/pgsql-17/bin/psql -h /home/caghan.linux/.pgrx -p 28817 -d postgres -v ON_ERROR_STOP=1 -f sql/poc_step9_untrack_retention_history.sql

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

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    id bigint PRIMARY KEY,
    status text NOT NULL,
    amount numeric(12,2) NOT NULL
);

INSERT INTO orders (id, status, amount)
VALUES
    (1, 'new', 10.00),
    (2, 'new', 20.00);

SELECT flashback_track('orders');

UPDATE flashback.tracked_tables
SET retention_interval = interval '30 minutes',
    checkpoint_interval = interval '1 hour'
WHERE table_name = 'orders';

UPDATE orders SET status = 'paid' WHERE id = 1;
INSERT INTO orders (id, status, amount) VALUES (3, 'new', 30.00);
DELETE FROM orders WHERE id = 2;

SELECT lsn, xid, data
FROM pg_logical_slot_get_changes('flashback_slot', NULL, NULL)
WHERE data LIKE '%table=orders%'
ORDER BY lsn;

SELECT pg_sleep(1);

SELECT event_time, event_type, row_identity, old_data, new_data
FROM flashback_history('orders', interval '5 minutes')
ORDER BY event_time DESC;

SELECT flashback_checkpoint('orders') AS checkpoint_id;

UPDATE flashback.tracked_tables
SET retention_interval = interval '2 seconds'
WHERE table_name = 'orders';

-- Wait enough for retention worker to purge old delta rows and snapshots.
SELECT pg_sleep(8);

SELECT count(*) AS delta_after_retention
FROM flashback.delta_log
WHERE table_name = 'public.orders';

SELECT
    count(*) AS snapshots_after_retention,
    COALESCE(extract(epoch FROM max(clock_timestamp() - s.captured_at)), 0) AS oldest_snapshot_age_seconds
FROM flashback.snapshots s
JOIN flashback.tracked_tables tt ON tt.rel_oid = s.rel_oid
WHERE tt.table_name = 'orders';

SELECT flashback_untrack('orders') AS untracked;

SELECT count(*) AS tracked_rows
FROM flashback.tracked_tables
WHERE table_name = 'orders';

SELECT count(*) AS delta_rows
FROM flashback.delta_log
WHERE table_name = 'public.orders';

SELECT count(*) AS snapshot_rows
FROM flashback.snapshots
WHERE rel_oid = to_regclass('public.orders')::oid;
