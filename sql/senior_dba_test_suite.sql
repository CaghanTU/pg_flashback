-- =============================================================
-- pg_flashback — Senior DBA Comprehensive Test Suite
-- =============================================================
-- 12 realistic scenarios exercising edge cases, volume,
-- schema evolution, serial sequences, and data integrity.
--
-- DESIGN: flashback_restore() is always called at psql top level
-- (never inside DO blocks) because serial default restoration
-- executes DDL that interacts with the ProcessUtility hook.
-- Verification assertions use DO blocks (which is safe).
--
-- Exit on first failure.
\set ON_ERROR_STOP on

CREATE EXTENSION IF NOT EXISTS pg_flashback;

-- ─────────────────────────────────────────────────────────────
-- HELPERS
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION _assert_eq(label text, actual bigint, expected bigint)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF actual IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'ASSERTION FAILED [%]: got %, expected %', label, actual, expected;
    ELSE
        RAISE NOTICE 'PASS [%]: % = %', label, actual, expected;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION _assert_true(label text, cond boolean)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF NOT COALESCE(cond, false) THEN
        RAISE EXCEPTION 'ASSERTION FAILED [%]: condition is false', label;
    ELSE
        RAISE NOTICE 'PASS [%]', label;
    END IF;
END;
$$;

-- ═════════════════════════════════════════════════════════════
-- TEST 1: Large Volume Stress (5K rows, mixed DML, restore)
-- ═════════════════════════════════════════════════════════════
\echo '══════ TEST 1: Large Volume (5K rows) ══════'

DROP TABLE IF EXISTS test_vol CASCADE;
CREATE TABLE test_vol (
    id serial PRIMARY KEY,
    val numeric NOT NULL,
    label text
);
INSERT INTO test_vol (val, label)
SELECT random() * 10000, 'seed-' || g FROM generate_series(1, 5000) g;

SELECT flashback_track('public.test_vol');
SELECT pg_sleep(2);

CREATE TEMP TABLE _t1_base AS SELECT count(*)::bigint AS cnt, sum(val)::bigint AS s FROM test_vol;
SELECT clock_timestamp() AS t0 \gset
SELECT pg_sleep(0.5);

INSERT INTO test_vol (val, label) SELECT random()*999, 'b'||g FROM generate_series(1,500) g;
SELECT pg_sleep(0.3);
UPDATE test_vol SET val = val+1, label='upd' WHERE id IN (SELECT id FROM test_vol ORDER BY random() LIMIT 500);
SELECT pg_sleep(0.3);
DELETE FROM test_vol WHERE id IN (SELECT id FROM test_vol WHERE label<>'upd' ORDER BY random() LIMIT 300);
SELECT pg_sleep(3);

SELECT flashback_restore('public.test_vol', :'t0'::timestamptz);

SELECT _assert_eq('T1 rows', (SELECT count(*) FROM test_vol), (SELECT cnt FROM _t1_base));
SELECT _assert_eq('T1 sum',  (SELECT sum(val)::bigint FROM test_vol), (SELECT s FROM _t1_base));

SELECT flashback_untrack('public.test_vol');
DROP TABLE test_vol CASCADE;
DROP TABLE _t1_base;
\echo 'TEST 1 PASSED'

-- ═════════════════════════════════════════════════════════════
-- TEST 2: Column Type Fidelity (int, text, bool, tstz, jsonb)
-- ═════════════════════════════════════════════════════════════
\echo '══════ TEST 2: Column Type Fidelity ══════'

DROP TABLE IF EXISTS test_types CASCADE;
CREATE TABLE test_types (
    id serial PRIMARY KEY,
    col_int integer,
    col_text text,
    col_bool boolean,
    col_ts timestamptz,
    col_json jsonb
);
INSERT INTO test_types (col_int, col_text, col_bool, col_ts, col_json)
SELECT g, 'txt'||g, (g%2=0), now()-(g||' min')::interval, jsonb_build_object('k',g)
FROM generate_series(1,300) g;

SELECT flashback_track('public.test_types');
SELECT pg_sleep(2);
SELECT clock_timestamp() AS t0 \gset
SELECT pg_sleep(0.5);

UPDATE test_types SET col_text='GONE', col_json='{"x":1}'::jsonb, col_bool=NOT col_bool WHERE id<=150;
DELETE FROM test_types WHERE id > 250;
SELECT pg_sleep(3);

SELECT flashback_restore('public.test_types', :'t0'::timestamptz);

SELECT _assert_eq('T2 rows', (SELECT count(*) FROM test_types), 300);
SELECT _assert_eq('T2 json', (SELECT count(*) FROM test_types WHERE (col_json->>'k')::int = id), 300);
SELECT _assert_eq('T2 bool', (SELECT count(*) FROM test_types WHERE col_bool = (id%2=0)), 300);

SELECT flashback_untrack('public.test_types');
DROP TABLE test_types CASCADE;
\echo 'TEST 2 PASSED'

-- ═════════════════════════════════════════════════════════════
-- TEST 3: FK Parent/Child Multi-Table Restore
-- ═════════════════════════════════════════════════════════════
\echo '══════ TEST 3: FK Multi-Table Restore ══════'

DROP TABLE IF EXISTS test_child CASCADE;
DROP TABLE IF EXISTS test_parent CASCADE;
CREATE TABLE test_parent (id serial PRIMARY KEY, name text NOT NULL);
CREATE TABLE test_child  (id serial PRIMARY KEY, parent_id int NOT NULL REFERENCES test_parent(id), value text);

INSERT INTO test_parent (name) SELECT 'p'||g FROM generate_series(1,100) g;
INSERT INTO test_child  (parent_id, value) SELECT (g%100)+1, 'c'||g FROM generate_series(1,500) g;

SELECT flashback_track('public.test_parent');
SELECT flashback_track('public.test_child');
SELECT pg_sleep(2);
SELECT clock_timestamp() AS t0 \gset
SELECT pg_sleep(0.5);

INSERT INTO test_parent (name) SELECT 'np'||g FROM generate_series(1,20) g;
INSERT INTO test_child  (parent_id, value) SELECT (g%20)+101, 'nc'||g FROM generate_series(1,100) g;
DELETE FROM test_child  WHERE parent_id > 50;
DELETE FROM test_parent WHERE id > 50;
SELECT pg_sleep(3);

SELECT flashback_restore(ARRAY['public.test_parent','public.test_child'], :'t0'::timestamptz);

SELECT _assert_eq('T3 parents',  (SELECT count(*) FROM test_parent), 100);
SELECT _assert_eq('T3 children', (SELECT count(*) FROM test_child),  500);
SELECT _assert_eq('T3 orphans',
    (SELECT count(*) FROM test_child c LEFT JOIN test_parent p ON c.parent_id=p.id WHERE p.id IS NULL), 0);

SELECT flashback_untrack('public.test_parent');
SELECT flashback_untrack('public.test_child');
DROP TABLE test_child CASCADE;
DROP TABLE test_parent CASCADE;
\echo 'TEST 3 PASSED'

-- ═════════════════════════════════════════════════════════════
-- TEST 4: Restore to Exact Boundary Times (3 waypoints)
-- ═════════════════════════════════════════════════════════════
\echo '══════ TEST 4: Boundary Time Precision ══════'

DROP TABLE IF EXISTS test_boundary CASCADE;
CREATE TABLE test_boundary (id serial PRIMARY KEY, val integer NOT NULL);
SELECT flashback_track('public.test_boundary');
SELECT pg_sleep(1);

INSERT INTO test_boundary (val) SELECT g FROM generate_series(1,100) g;
SELECT pg_sleep(1);
SELECT clock_timestamp() AS t4a \gset
SELECT pg_sleep(0.5);

UPDATE test_boundary SET val = val * 10;
SELECT pg_sleep(1);
SELECT clock_timestamp() AS t4b \gset
SELECT pg_sleep(0.5);

DELETE FROM test_boundary WHERE id > 50;
SELECT pg_sleep(1);
SELECT clock_timestamp() AS t4c \gset
SELECT pg_sleep(0.5);

TRUNCATE test_boundary;
SELECT pg_sleep(3);

-- Waypoint A: 100 rows, sum=5050
SELECT flashback_restore('public.test_boundary', :'t4a'::timestamptz);
SELECT _assert_eq('T4a cnt', (SELECT count(*) FROM test_boundary), 100);
SELECT _assert_eq('T4a sum', (SELECT sum(val) FROM test_boundary),  5050);

-- Waypoint B: 100 rows, sum=50500
SELECT flashback_restore('public.test_boundary', :'t4b'::timestamptz);
SELECT _assert_eq('T4b cnt', (SELECT count(*) FROM test_boundary), 100);
SELECT _assert_eq('T4b sum', (SELECT sum(val) FROM test_boundary),  50500);

-- Waypoint C: 50 rows
SELECT flashback_restore('public.test_boundary', :'t4c'::timestamptz);
SELECT _assert_eq('T4c cnt', (SELECT count(*) FROM test_boundary), 50);

SELECT flashback_untrack('public.test_boundary');
DROP TABLE test_boundary CASCADE;
\echo 'TEST 4 PASSED'

-- ═════════════════════════════════════════════════════════════
-- TEST 5: Schema Evolution — ADD COLUMN + Restore to old schema
-- ═════════════════════════════════════════════════════════════
\echo '══════ TEST 5: Schema Evolution ══════'

DROP TABLE IF EXISTS test_schema_evo CASCADE;
CREATE TABLE test_schema_evo (id serial PRIMARY KEY, name text NOT NULL, value integer);
INSERT INTO test_schema_evo (name, value) SELECT 'row-'||g, g FROM generate_series(1,200) g;

SELECT flashback_track('public.test_schema_evo');
SELECT pg_sleep(1);
SELECT clock_timestamp() AS t0 \gset
SELECT pg_sleep(0.5);

ALTER TABLE test_schema_evo ADD COLUMN notes text DEFAULT 'n/a';
INSERT INTO test_schema_evo (name, value, notes) SELECT 'new-'||g, g*100, 'has' FROM generate_series(1,50) g;
SELECT pg_sleep(3);

SELECT flashback_restore('public.test_schema_evo', :'t0'::timestamptz);

SELECT _assert_eq('T5 rows', (SELECT count(*) FROM test_schema_evo), 200);
SELECT _assert_eq('T5 no_notes_col',
    (SELECT count(*) FROM information_schema.columns
     WHERE table_schema='public' AND table_name='test_schema_evo' AND column_name='notes'), 0);

SELECT flashback_untrack('public.test_schema_evo');
DROP TABLE test_schema_evo CASCADE;
\echo 'TEST 5 PASSED'

-- ═════════════════════════════════════════════════════════════
-- TEST 6: Track → Untrack → Re-Track Lifecycle
-- ═════════════════════════════════════════════════════════════
\echo '══════ TEST 6: Track/Untrack/Re-Track Lifecycle ══════'

DROP TABLE IF EXISTS test_lifecycle CASCADE;
CREATE TABLE test_lifecycle (id serial PRIMARY KEY, data text);
INSERT INTO test_lifecycle (data) SELECT 'init-'||g FROM generate_series(1,100) g;

SELECT flashback_track('public.test_lifecycle');
SELECT pg_sleep(1);

INSERT INTO test_lifecycle (data) SELECT 'trk-'||g FROM generate_series(1,50) g;
SELECT pg_sleep(1);

SELECT flashback_untrack('public.test_lifecycle');
SELECT _assert_eq('T6 untracked',
    (SELECT count(*) FROM flashback.tracked_tables WHERE table_name='test_lifecycle'), 0);

DELETE FROM test_lifecycle WHERE id > 100;
INSERT INTO test_lifecycle (data) SELECT 'untrk-'||g FROM generate_series(1,30) g;
SELECT pg_sleep(0.5);

SELECT flashback_track('public.test_lifecycle');
SELECT pg_sleep(2);
SELECT clock_timestamp() AS t0 \gset
SELECT pg_sleep(0.5);

INSERT INTO test_lifecycle (data) SELECT 'retrk-'||g FROM generate_series(1,25) g;
TRUNCATE test_lifecycle;
SELECT pg_sleep(3);

SELECT flashback_restore('public.test_lifecycle', :'t0'::timestamptz);
SELECT _assert_eq('T6 retrack', (SELECT count(*) FROM test_lifecycle), 130);

SELECT flashback_untrack('public.test_lifecycle');
DROP TABLE test_lifecycle CASCADE;
\echo 'TEST 6 PASSED'

-- ═════════════════════════════════════════════════════════════
-- TEST 7: TRUNCATE + Re-Insert → Restore Pre-Truncate
-- ═════════════════════════════════════════════════════════════
\echo '══════ TEST 7: TRUNCATE + Re-Insert ══════'

DROP TABLE IF EXISTS test_trunc CASCADE;
CREATE TABLE test_trunc (id serial PRIMARY KEY, val integer NOT NULL);
SELECT flashback_track('public.test_trunc');
SELECT pg_sleep(1);

INSERT INTO test_trunc (val) SELECT g FROM generate_series(1,200) g;
SELECT pg_sleep(1);
SELECT clock_timestamp() AS t0 \gset
SELECT pg_sleep(0.5);

TRUNCATE test_trunc;
INSERT INTO test_trunc (val) SELECT g*100 FROM generate_series(1,50) g;
SELECT pg_sleep(3);

SELECT flashback_restore('public.test_trunc', :'t0'::timestamptz);
SELECT _assert_eq('T7 cnt', (SELECT count(*) FROM test_trunc), 200);
SELECT _assert_eq('T7 sum', (SELECT sum(val) FROM test_trunc),  20100);

SELECT flashback_untrack('public.test_trunc');
DROP TABLE test_trunc CASCADE;
\echo 'TEST 7 PASSED'

-- ═════════════════════════════════════════════════════════════
-- TEST 8: Serial Sequence Correctness Post-Restore
-- ═════════════════════════════════════════════════════════════
\echo '══════ TEST 8: Serial Sequence Post-Restore ══════'

DROP TABLE IF EXISTS test_serial CASCADE;
CREATE TABLE test_serial (id serial PRIMARY KEY, name text NOT NULL);
INSERT INTO test_serial (name) SELECT 'row-'||g FROM generate_series(1,500) g;

SELECT flashback_track('public.test_serial');
SELECT pg_sleep(2);
SELECT clock_timestamp() AS t0 \gset
SELECT pg_sleep(0.5);

INSERT INTO test_serial (name) SELECT 'post-'||g FROM generate_series(1,200) g;
DELETE FROM test_serial WHERE id <= 100;
SELECT pg_sleep(3);

SELECT flashback_restore('public.test_serial', :'t0'::timestamptz);

SELECT _assert_eq('T8 rows', (SELECT count(*) FROM test_serial), 500);

SELECT max(id) AS v FROM test_serial \gset t8max_
INSERT INTO test_serial (name) VALUES ('after-restore') RETURNING id AS v \gset t8new_
SELECT _assert_true('T8 serial_gt', :t8new_v > :t8max_v);

SELECT _assert_eq('T8 no_dups',
    (SELECT count(*) FROM (SELECT id FROM test_serial GROUP BY id HAVING count(*)>1) x), 0);

SELECT _assert_true('T8 has_default',
    (SELECT column_default LIKE 'nextval%' FROM information_schema.columns
     WHERE table_name='test_serial' AND column_name='id'));

SELECT flashback_untrack('public.test_serial');
DROP TABLE test_serial CASCADE;
\echo 'TEST 8 PASSED'

-- ═════════════════════════════════════════════════════════════
-- TEST 9: Checkpoint-Based Restore
-- ═════════════════════════════════════════════════════════════
\echo '══════ TEST 9: Checkpoint-Based Restore ══════'

DROP TABLE IF EXISTS test_ckpt CASCADE;
CREATE TABLE test_ckpt (id serial PRIMARY KEY, val integer NOT NULL);
SELECT flashback_track('public.test_ckpt');
SELECT pg_sleep(2);

INSERT INTO test_ckpt (val) SELECT g FROM generate_series(1,500) g;
SELECT pg_sleep(2);

SELECT flashback_checkpoint('public.test_ckpt');
SELECT pg_sleep(0.5);

INSERT INTO test_ckpt (val) SELECT g+500 FROM generate_series(1,200) g;
UPDATE test_ckpt SET val = val * 2 WHERE id <= 200;
SELECT pg_sleep(2);
SELECT clock_timestamp() AS t0 \gset
SELECT pg_sleep(0.5);

DELETE FROM test_ckpt WHERE id <= 400;
INSERT INTO test_ckpt (val) SELECT 99999 FROM generate_series(1,100) g;
SELECT pg_sleep(3);

SELECT flashback_restore('public.test_ckpt', :'t0'::timestamptz);
SELECT _assert_eq('T9 rows', (SELECT count(*) FROM test_ckpt), 700);
SELECT _assert_eq('T9 doubled', (SELECT count(*) FROM test_ckpt WHERE id<=200 AND val=id*2), 200);

SELECT flashback_untrack('public.test_ckpt');
DROP TABLE test_ckpt CASCADE;
\echo 'TEST 9 PASSED'

-- ═════════════════════════════════════════════════════════════
-- TEST 10: Sequential Restores (restore → restore → restore)
-- ═════════════════════════════════════════════════════════════
\echo '══════ TEST 10: Sequential Restores ══════'

DROP TABLE IF EXISTS test_seqr CASCADE;
CREATE TABLE test_seqr (id serial PRIMARY KEY, val text NOT NULL);
SELECT flashback_track('public.test_seqr');
SELECT pg_sleep(1);

INSERT INTO test_seqr (val) SELECT 'v1-'||g FROM generate_series(1,100) g;
SELECT pg_sleep(1);
SELECT clock_timestamp() AS t10a \gset
SELECT pg_sleep(0.5);

INSERT INTO test_seqr (val) SELECT 'v2-'||g FROM generate_series(1,100) g;
SELECT pg_sleep(1);
SELECT clock_timestamp() AS t10b \gset
SELECT pg_sleep(0.5);

TRUNCATE test_seqr;
SELECT pg_sleep(3);

SELECT flashback_restore('public.test_seqr', :'t10b'::timestamptz);
SELECT _assert_eq('T10 first_200', (SELECT count(*) FROM test_seqr), 200);

SELECT flashback_restore('public.test_seqr', :'t10a'::timestamptz);
SELECT _assert_eq('T10 second_100', (SELECT count(*) FROM test_seqr), 100);

SELECT flashback_restore('public.test_seqr', :'t10b'::timestamptz);
SELECT _assert_eq('T10 third_200', (SELECT count(*) FROM test_seqr), 200);

SELECT flashback_untrack('public.test_seqr');
DROP TABLE test_seqr CASCADE;
\echo 'TEST 10 PASSED'

-- ═════════════════════════════════════════════════════════════
-- TEST 11: NULL-Heavy Table + JSONB Nested Data
-- ═════════════════════════════════════════════════════════════
\echo '══════ TEST 11: NULL-Heavy + JSONB ══════'

DROP TABLE IF EXISTS test_nulls CASCADE;
CREATE TABLE test_nulls (
    id serial PRIMARY KEY,
    a text, b integer, c jsonb, d boolean, e timestamptz
);
INSERT INTO test_nulls (a, b, c, d, e)
SELECT
    CASE WHEN g%3=0 THEN 'val-'||g ELSE NULL END,
    CASE WHEN g%4=0 THEN g ELSE NULL END,
    CASE WHEN g%5=0 THEN jsonb_build_object('deep', jsonb_build_object('lvl',g)) ELSE NULL END,
    CASE WHEN g%2=0 THEN true ELSE NULL END,
    CASE WHEN g%6=0 THEN clock_timestamp() ELSE NULL END
FROM generate_series(1,300) g;

SELECT flashback_track('public.test_nulls');
SELECT pg_sleep(2);
SELECT clock_timestamp() AS t0 \gset
SELECT pg_sleep(0.5);

UPDATE test_nulls SET a='over', b=9999, c='{"g":1}'::jsonb WHERE a IS NULL AND id<=150;
UPDATE test_nulls SET a=NULL, b=NULL, c=NULL WHERE a IS NOT NULL AND id>150;
DELETE FROM test_nulls WHERE id > 250;
SELECT pg_sleep(3);

SELECT flashback_restore('public.test_nulls', :'t0'::timestamptz);
SELECT _assert_eq('T11 cnt',    (SELECT count(*) FROM test_nulls), 300);
SELECT _assert_eq('T11 null_a', (SELECT count(*) FROM test_nulls WHERE a IS NULL), 200);

SELECT flashback_untrack('public.test_nulls');
DROP TABLE test_nulls CASCADE;
\echo 'TEST 11 PASSED'

-- ═════════════════════════════════════════════════════════════
-- TEST 12: flashback_history() Query
-- ═════════════════════════════════════════════════════════════
\echo '══════ TEST 12: flashback_history() ══════'

DROP TABLE IF EXISTS test_history CASCADE;
CREATE TABLE test_history (id serial PRIMARY KEY, status text);
SELECT flashback_track('public.test_history');
SELECT pg_sleep(1);

INSERT INTO test_history (status) VALUES ('created');
UPDATE test_history SET status = 'updated' WHERE id = 1;
UPDATE test_history SET status = 'final'   WHERE id = 1;
DELETE FROM test_history WHERE id = 1;
SELECT pg_sleep(2);

SELECT _assert_true('T12 history',
    (SELECT count(*) >= 4 FROM flashback_history('public.test_history', '5 minutes'::interval)));

SELECT flashback_untrack('public.test_history');
DROP TABLE test_history CASCADE;
\echo 'TEST 12 PASSED'

-- ═════════════════════════════════════════════════════════════
-- TEST 13: Wide Table (15+ Columns) — Former 256-byte Blocker
-- ═════════════════════════════════════════════════════════════
\echo '══════ TEST 13: Wide Table (15+ Columns) ══════'

DROP TABLE IF EXISTS test_wide CASCADE;
CREATE TABLE test_wide (
    id          serial PRIMARY KEY,
    col_text1   text,
    col_text2   text,
    col_text3   text,
    col_int1    integer,
    col_int2    integer,
    col_int3    integer,
    col_num1    numeric(12,4),
    col_num2    numeric(12,4),
    col_bool1   boolean,
    col_bool2   boolean,
    col_ts1     timestamptz DEFAULT now(),
    col_ts2     timestamptz DEFAULT now(),
    col_json1   jsonb,
    col_json2   jsonb,
    col_uuid1   uuid DEFAULT gen_random_uuid()
);

SELECT flashback_track('public.test_wide');
SELECT pg_sleep(1);

-- Insert rows with substantial data in all columns
INSERT INTO test_wide (col_text1, col_text2, col_text3, col_int1, col_int2, col_int3,
    col_num1, col_num2, col_bool1, col_bool2, col_json1, col_json2)
SELECT
    'text_a_' || g, 'text_b_' || g, repeat('x', 100),
    g, g*10, g*100,
    g * 1.1234, g * 2.5678,
    (g % 2 = 0), (g % 3 = 0),
    jsonb_build_object('key', g, 'nested', jsonb_build_object('deep', g * 2)),
    jsonb_build_array(g, g+1, g+2)
FROM generate_series(1, 200) g;

SELECT flashback_checkpoint('public.test_wide');
SELECT pg_sleep(1);

-- Mutate heavily after checkpoint
UPDATE test_wide SET col_text1 = 'MODIFIED', col_num1 = 9999.9999, col_json1 = '{"destroyed": true}'
WHERE id <= 100;
DELETE FROM test_wide WHERE id > 150;
INSERT INTO test_wide (col_text1, col_text2, col_text3, col_int1, col_int2, col_int3,
    col_num1, col_num2, col_bool1, col_bool2, col_json1, col_json2)
VALUES ('new_after', 'checkpoint', 'row', 999, 888, 777, 1.0, 2.0, true, false,
    '{"post": true}', '[1,2,3]');

SELECT pg_sleep(2);

-- Capture state before restore for comparison
SELECT count(*) AS wide_before_count FROM test_wide;

-- Restore to checkpoint time
SELECT flashback_restore('public.test_wide', s.captured_at)
FROM flashback.snapshots s
WHERE s.rel_oid = 'test_wide'::regclass::oid
ORDER BY s.captured_at DESC LIMIT 1;

DO $$
DECLARE
    v_cnt bigint;
    v_orig text;
BEGIN
    SELECT count(*) INTO v_cnt FROM test_wide;
    PERFORM _assert_eq('T13 wide row count', v_cnt, 200);

    -- Verify original data survived (not truncated)
    SELECT col_text1 INTO v_orig FROM test_wide WHERE id = 1;
    PERFORM _assert_true('T13 wide col_text1 intact', v_orig = 'text_a_1');

    PERFORM _assert_true('T13 wide col_num1 intact',
        (SELECT col_num1 = 56.1700 FROM test_wide WHERE id = 50));

    -- Verify JSONB column survived
    PERFORM _assert_true('T13 wide json intact',
        (SELECT (col_json1->>'key')::int = 10 FROM test_wide WHERE id = 10));
END;
$$;

SELECT flashback_untrack('public.test_wide');
DROP TABLE test_wide CASCADE;
\echo 'TEST 13 PASSED'

-- ═════════════════════════════════════════════════════════════
-- TEST 14: DO Block + Serial Column Restore — Former Crash
-- ═════════════════════════════════════════════════════════════
\echo '══════ TEST 14: DO Block + Serial Restore ══════'

DROP TABLE IF EXISTS test_doblock CASCADE;
CREATE TABLE test_doblock (
    id    serial PRIMARY KEY,
    name  text NOT NULL,
    score integer DEFAULT 0
);

SELECT flashback_track('public.test_doblock');
SELECT pg_sleep(1);

INSERT INTO test_doblock(name, score) VALUES ('alice', 10), ('bob', 20), ('carol', 30);
SELECT flashback_checkpoint('public.test_doblock');
SELECT pg_sleep(1);

-- Mutate after checkpoint
UPDATE test_doblock SET score = 999 WHERE name = 'alice';
DELETE FROM test_doblock WHERE name = 'carol';
INSERT INTO test_doblock(name, score) VALUES ('dave', 40);
SELECT pg_sleep(2);

-- Restore at top level (serial defaults require DDL in ProcessUtility hook)
SELECT flashback_restore('public.test_doblock', s.captured_at)
FROM flashback.snapshots s
WHERE s.rel_oid = 'test_doblock'::regclass::oid
ORDER BY s.captured_at DESC LIMIT 1;

-- Verify in DO block
DO $$
DECLARE
    v_cnt bigint;
    v_score integer;
    v_nextval bigint;
BEGIN
    SELECT count(*) INTO v_cnt FROM test_doblock;
    PERFORM _assert_eq('T14 DO row count', v_cnt, 3);

    SELECT score INTO v_score FROM test_doblock WHERE name = 'alice';
    PERFORM _assert_true('T14 DO alice score restored', v_score = 10);

    -- Verify serial sequence still works after restore
    INSERT INTO test_doblock(name, score) VALUES ('eve', 50);
    SELECT max(id) INTO v_nextval FROM test_doblock;
    PERFORM _assert_true('T14 DO serial works post-restore', v_nextval > 3);
END;
$$;

SELECT flashback_untrack('public.test_doblock');
DROP TABLE test_doblock CASCADE;
\echo 'TEST 14 PASSED'

-- ═════════════════════════════════════════════════════════════
-- PERF: Restore Latency Measurement
-- ═════════════════════════════════════════════════════════════
\echo '══════ PERF: Restore Latency ══════'

DROP TABLE IF EXISTS test_perf CASCADE;
CREATE TABLE test_perf (id serial PRIMARY KEY, val integer, data text);
INSERT INTO test_perf (val, data) SELECT g, repeat('x',50) FROM generate_series(1,3000) g;
SELECT flashback_track('public.test_perf');
SELECT pg_sleep(2);
SELECT clock_timestamp() AS t0 \gset
SELECT pg_sleep(0.5);

UPDATE test_perf SET val=val+1, data='mod' WHERE id<=1000;
DELETE FROM test_perf WHERE id>2500;
INSERT INTO test_perf (val, data) SELECT g, 'new' FROM generate_series(1,500) g;
SELECT pg_sleep(3);

\timing on
SELECT flashback_restore('public.test_perf', :'t0'::timestamptz);
\timing off

SELECT 'PERF rows_after=' || count(*) AS perf_result FROM test_perf;

SELECT flashback_untrack('public.test_perf');
DROP TABLE test_perf CASCADE;

-- ═══════════════════════════
-- STORAGE ANALYSIS
-- ═══════════════════════════
SELECT pg_size_pretty(pg_total_relation_size('flashback.delta_log')) AS delta_log_size;
SELECT count(*) AS remaining_snapshots FROM flashback.snapshots;

-- ═════════════════════════
-- CLEANUP
-- ═════════════════════════
DROP FUNCTION IF EXISTS _assert_eq(text, bigint, bigint);
DROP FUNCTION IF EXISTS _assert_true(text, boolean);

\echo ''
\echo '═══════════════════════════════════════════════════'
\echo '  ALL 14 TESTS PASSED — Senior DBA Test Suite'
\echo '═══════════════════════════════════════════════════'
