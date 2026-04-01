-- Step 2 PoC: create logical slot, generate DML, and read plugin output
-- Usage:
--   /usr/local/pgsql-17/bin/psql -h /home/caghan.linux/.pgrx -p 28817 -d postgres -v ON_ERROR_STOP=1 -f sql/poc_step2_logical_slot.sql

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

INSERT INTO test_track (name, status)
VALUES ('order1', 'pending');

UPDATE test_track
SET status = 'shipped'
WHERE id = 1;

DELETE FROM test_track
WHERE id = 1;

SELECT lsn, xid, data
FROM pg_logical_slot_get_changes('flashback_slot', NULL, NULL)
WHERE data LIKE '%table=test_track%';
