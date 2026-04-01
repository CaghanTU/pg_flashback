-- Step 4 PoC: SPI persist from logical decoding change_cb
-- Usage:
--   /usr/local/pgsql-17/bin/psql -h /home/caghan.linux/.pgrx -p 28817 -d postgres -v ON_ERROR_STOP=1 -f sql/poc_step4_spi_persist.sql

SELECT pg_drop_replication_slot('flashback_slot')
WHERE EXISTS (
    SELECT 1
    FROM pg_replication_slots
    WHERE slot_name = 'flashback_slot'
);

SELECT *
FROM pg_create_logical_replication_slot('flashback_slot', 'pg_flashback');

TRUNCATE flashback.delta_log;

DROP TABLE IF EXISTS test_track;
CREATE TABLE test_track (
    id serial PRIMARY KEY,
    name text,
    status text
);

ALTER TABLE test_track REPLICA IDENTITY FULL;

INSERT INTO test_track (name, status)
VALUES ('test1', 'active');

UPDATE test_track
SET status = 'done'
WHERE name = 'test1';

DELETE FROM test_track
WHERE name = 'test1';

-- This call triggers logical decoding callbacks and therefore SPI persistence attempts.
SELECT lsn, xid, data
FROM pg_logical_slot_get_changes('flashback_slot', NULL, NULL)
WHERE data LIKE '%table=test_track%'
ORDER BY lsn;

SELECT event_id, event_type, table_name, old_data, new_data
FROM flashback.delta_log
WHERE table_name = 'public.test_track'
ORDER BY event_id;
