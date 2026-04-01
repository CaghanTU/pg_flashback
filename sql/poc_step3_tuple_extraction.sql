-- Step 3 PoC: tuple extraction with REPLICA IDENTITY FULL
-- Usage:
--   /usr/local/pgsql-17/bin/psql -h /home/caghan.linux/.pgrx -p 28817 -d postgres -v ON_ERROR_STOP=1 -f sql/poc_step3_tuple_extraction.sql

SELECT pg_drop_replication_slot('flashback_slot')
WHERE EXISTS (
    SELECT 1
    FROM pg_replication_slots
    WHERE slot_name = 'flashback_slot'
);

SELECT *
FROM pg_create_logical_replication_slot('flashback_slot', 'pg_flashback');

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

SELECT lsn, xid, data
FROM pg_logical_slot_get_changes('flashback_slot', NULL, NULL)
WHERE data LIKE '%table=test_track%'
ORDER BY lsn;
