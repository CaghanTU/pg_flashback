DROP EXTENSION IF EXISTS pg_flashback CASCADE;
CREATE EXTENSION pg_flashback;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'flashback_perf_slot') THEN
        PERFORM pg_drop_replication_slot('flashback_perf_slot');
    END IF;
EXCEPTION WHEN OTHERS THEN
    NULL;
END;
$$;

SELECT * FROM pg_create_logical_replication_slot('flashback_perf_slot', 'pg_flashback');

TRUNCATE flashback.delta_log;
TRUNCATE flashback.snapshots RESTART IDENTITY;
TRUNCATE flashback.tracked_tables;
TRUNCATE flashback.schema_versions RESTART IDENTITY;

CREATE TABLE IF NOT EXISTS flashback.perf_results (
    test_name text NOT NULL,
    metric text NOT NULL,
    value_numeric numeric,
    unit text,
    details jsonb,
    captured_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

TRUNCATE flashback.perf_results;

CREATE TEMP TABLE perf_marks (
    name text PRIMARY KEY,
    ts timestamptz,
    n bigint
);

-- Test 1: Delta capture throughput (1M rows, 10k updates)
DROP TABLE IF EXISTS public.perf_t1;
CREATE TABLE public.perf_t1 (
    id bigint PRIMARY KEY,
    qty integer NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'new',
    payload text
);

INSERT INTO public.perf_t1(id, qty, status, payload)
SELECT g, 0, 'new', repeat('x', 80)
FROM generate_series(1, 1000000) AS g;

SELECT flashback_track('public.perf_t1');

INSERT INTO perf_marks(name, n)
VALUES (
    't1_delta_event_id_before',
    COALESCE((SELECT max(event_id) FROM flashback.delta_log), 0)
)
ON CONFLICT (name) DO UPDATE SET n = EXCLUDED.n;

INSERT INTO perf_marks(name, ts)
VALUES ('t1_workload_start', clock_timestamp())
ON CONFLICT (name) DO UPDATE SET ts = EXCLUDED.ts;

UPDATE public.perf_t1
SET qty = qty + 1,
    status = 'upd'
WHERE id % 100 = 0;

INSERT INTO perf_marks(name, ts)
VALUES ('t1_workload_end', clock_timestamp())
ON CONFLICT (name) DO UPDATE SET ts = EXCLUDED.ts;

SELECT 0 AS consumed_changes;

DO $$
DECLARE
    v_rel_oid oid;
    v_observed bigint;
    v_expected bigint := 10000;
    v_event_id_before bigint;
    v_wait_start timestamptz := clock_timestamp();
    v_wait_end timestamptz;
    v_workload_start timestamptz;
    v_workload_end timestamptz;
    v_workload_ms numeric;
    v_flush_wait_ms numeric;
    v_total_ms numeric;
    v_rows_per_sec numeric;
BEGIN
    SELECT to_regclass('public.perf_t1')::oid INTO v_rel_oid;
    SELECT ts INTO v_workload_start FROM perf_marks WHERE name = 't1_workload_start';
    SELECT ts INTO v_workload_end FROM perf_marks WHERE name = 't1_workload_end';
        SELECT n INTO v_event_id_before FROM perf_marks WHERE name = 't1_delta_event_id_before';

    LOOP
        SELECT count(*)
          INTO v_observed
        FROM flashback.delta_log
        WHERE rel_oid = v_rel_oid
          AND event_type = 'UPDATE'
                    AND event_id > COALESCE(v_event_id_before, 0);

        EXIT WHEN v_observed >= v_expected
              OR clock_timestamp() > v_wait_start + interval '90 seconds';

        PERFORM pg_sleep(0.05);
    END LOOP;

    v_wait_end := clock_timestamp();
    v_workload_ms := extract(epoch from (v_workload_end - v_workload_start)) * 1000.0;
    v_flush_wait_ms := extract(epoch from (v_wait_end - v_wait_start)) * 1000.0;
    v_total_ms := extract(epoch from (v_wait_end - v_workload_start)) * 1000.0;
    v_rows_per_sec := CASE WHEN v_total_ms > 0 THEN (v_expected * 1000.0 / v_total_ms) ELSE NULL END;

    INSERT INTO flashback.perf_results(test_name, metric, value_numeric, unit, details)
    VALUES
        ('test1_delta_capture', 'workload_duration', v_workload_ms, 'ms', NULL),
        ('test1_delta_capture', 'worker_flush_wait', v_flush_wait_ms, 'ms', NULL),
        ('test1_delta_capture', 'observed_update_rows', v_observed, 'rows', jsonb_build_object('expected_rows', v_expected)),
        ('test1_delta_capture', 'capture_completeness_ratio', CASE WHEN v_expected > 0 THEN v_observed::numeric / v_expected::numeric ELSE NULL END, 'ratio', NULL),
        ('test1_delta_capture', 'queue_overflow_detected', CASE WHEN v_observed < v_expected THEN 1 ELSE 0 END, 'bool_0_1', jsonb_build_object('expected_rows', v_expected, 'observed_rows', v_observed)),
        ('test1_delta_capture', 'effective_throughput', v_rows_per_sec, 'rows_per_sec', NULL);
END;
$$;

-- Test 2A: 10k-delta restore duration (no checkpoint path)
DROP TABLE IF EXISTS public.perf_t2a;
CREATE TABLE public.perf_t2a (
    id integer PRIMARY KEY,
    v integer NOT NULL
);
INSERT INTO public.perf_t2a VALUES (1, 0);

SELECT flashback_track('public.perf_t2a');

INSERT INTO perf_marks(name, ts)
VALUES ('t2a_target', clock_timestamp())
ON CONFLICT (name) DO UPDATE SET ts = EXCLUDED.ts;

DO $$
DECLARE i int;
BEGIN
    FOR i IN 1..10000 LOOP
        UPDATE public.perf_t2a SET v = i WHERE id = 1;
    END LOOP;
END;
$$;

SELECT 0;

DO $$
DECLARE
    v_target timestamptz;
    v_start timestamptz;
    v_end timestamptz;
BEGIN
    SELECT ts INTO v_target FROM perf_marks WHERE name = 't2a_target';
    v_start := clock_timestamp();
    PERFORM flashback_restore('public.perf_t2a', v_target);
    v_end := clock_timestamp();

    INSERT INTO flashback.perf_results(test_name, metric, value_numeric, unit, details)
    VALUES ('test2_restore_10k', 'restore_duration', extract(epoch from (v_end - v_start)) * 1000.0, 'ms', jsonb_build_object('mode', 'no_checkpoint_forward_replay'));
END;
$$;

-- Test 2B: 50k-delta restore duration with checkpoint baseline
DROP TABLE IF EXISTS public.perf_t2b;
CREATE TABLE public.perf_t2b (
    id integer PRIMARY KEY,
    v integer NOT NULL
);
INSERT INTO public.perf_t2b VALUES (1, 0);

SELECT flashback_track('public.perf_t2b');
SELECT flashback_checkpoint('public.perf_t2b');

DO $$
DECLARE i int;
BEGIN
    FOR i IN 1..50000 LOOP
        UPDATE public.perf_t2b SET v = i WHERE id = 1;
    END LOOP;
END;
$$;

INSERT INTO perf_marks(name, ts)
VALUES ('t2b_target', clock_timestamp())
ON CONFLICT (name) DO UPDATE SET ts = EXCLUDED.ts;

SELECT 0;

DO $$
DECLARE
    v_target timestamptz;
    v_start timestamptz;
    v_end timestamptz;
BEGIN
    SELECT ts INTO v_target FROM perf_marks WHERE name = 't2b_target';
    v_start := clock_timestamp();
    PERFORM flashback_restore('public.perf_t2b', v_target);
    v_end := clock_timestamp();

    INSERT INTO flashback.perf_results(test_name, metric, value_numeric, unit, details)
    VALUES ('test2_restore_50k_checkpoint', 'restore_duration', extract(epoch from (v_end - v_start)) * 1000.0, 'ms', jsonb_build_object('mode', 'checkpoint_rebuild_plus_forward_replay'));
END;
$$;

-- Test 3: Storage overhead (10k DML + checkpoint snapshot)
DROP TABLE IF EXISTS public.perf_t3;
CREATE TABLE public.perf_t3 (
    id integer PRIMARY KEY,
    v integer NOT NULL,
    payload text
);
INSERT INTO public.perf_t3(id, v, payload)
SELECT g, 0, repeat('p', 120)
FROM generate_series(1, 100000) g;

SELECT flashback_track('public.perf_t3');

INSERT INTO perf_marks(name, n)
VALUES ('t3_delta_size_before', pg_total_relation_size('flashback.delta_log'::regclass))
ON CONFLICT (name) DO UPDATE SET n = EXCLUDED.n;

UPDATE public.perf_t3
SET v = v + 1
WHERE id % 10 = 0;

SELECT 0;
SELECT flashback_checkpoint('public.perf_t3');

DO $$
DECLARE
    v_before bigint;
    v_after bigint;
    v_table_size bigint;
    v_snapshot_table text;
    v_snapshot_size bigint;
BEGIN
    SELECT n INTO v_before FROM perf_marks WHERE name = 't3_delta_size_before';
    SELECT pg_total_relation_size('flashback.delta_log'::regclass) INTO v_after;
    SELECT pg_total_relation_size('public.perf_t3'::regclass) INTO v_table_size;

    SELECT s.snapshot_table
      INTO v_snapshot_table
    FROM flashback.snapshots s
    WHERE s.rel_oid = to_regclass('public.perf_t3')::oid
    ORDER BY s.snapshot_id DESC
    LIMIT 1;

    IF v_snapshot_table IS NOT NULL AND v_snapshot_table <> '' THEN
        EXECUTE format('SELECT pg_total_relation_size(%L::regclass)', v_snapshot_table)
          INTO v_snapshot_size;
    ELSE
        v_snapshot_size := NULL;
    END IF;

    INSERT INTO flashback.perf_results(test_name, metric, value_numeric, unit, details)
    VALUES
        ('test3_storage_overhead', 'delta_log_growth', (v_after - v_before), 'bytes', jsonb_build_object('dml_rows', 10000)),
        ('test3_storage_overhead', 'tracked_table_size', v_table_size, 'bytes', jsonb_build_object('table', 'public.perf_t3')),
        ('test3_storage_overhead', 'checkpoint_snapshot_size', v_snapshot_size, 'bytes', jsonb_build_object('snapshot_table', v_snapshot_table));
END;
$$;

-- Test 4: Write amplification (tracking OFF vs ON on separate identical tables)
DROP TABLE IF EXISTS public.perf_t4_off;
DROP TABLE IF EXISTS public.perf_t4_on;
CREATE TABLE public.perf_t4_off (
    id integer PRIMARY KEY,
    v integer NOT NULL,
    payload text
);
CREATE TABLE public.perf_t4_on (
    id integer PRIMARY KEY,
    v integer NOT NULL,
    payload text
);
INSERT INTO public.perf_t4_off(id, v, payload)
SELECT g, 0, repeat('w', 100)
FROM generate_series(1, 200000) g;
INSERT INTO public.perf_t4_on(id, v, payload)
SELECT id, v, payload
FROM public.perf_t4_off;

DO $$
DECLARE
    v_lsn_start pg_lsn;
    v_lsn_after_update pg_lsn;
    v_lsn_after_flush pg_lsn;
    v_wal_off numeric;
    v_io_off bigint;
    v_io_on bigint;
    v_wal_on_foreground numeric;
    v_wal_on_total numeric;
BEGIN
    BEGIN
        PERFORM pg_stat_reset_shared('io');
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;

    v_lsn_start := pg_current_wal_lsn();
    UPDATE public.perf_t4_off SET v = v + 1 WHERE id % 10 = 0;
    v_lsn_after_update := pg_current_wal_lsn();
    v_wal_off := pg_wal_lsn_diff(v_lsn_after_update, v_lsn_start);

    BEGIN
        SELECT COALESCE(sum(write_bytes), 0)
          INTO v_io_off
        FROM pg_stat_io
        WHERE backend_type IN ('client backend', 'background worker')
          AND object = 'relation';
    EXCEPTION WHEN OTHERS THEN
        v_io_off := NULL;
    END;

    PERFORM flashback_track('public.perf_t4_on');

    BEGIN
        PERFORM pg_stat_reset_shared('io');
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;

    v_lsn_start := pg_current_wal_lsn();
    UPDATE public.perf_t4_on SET v = v + 1 WHERE id % 10 = 0;
    v_lsn_after_update := pg_current_wal_lsn();
    v_wal_on_foreground := pg_wal_lsn_diff(v_lsn_after_update, v_lsn_start);

    -- Include async worker flush effect in an end-to-end metric.
    PERFORM pg_sleep(1.0);
    v_lsn_after_flush := pg_current_wal_lsn();
    v_wal_on_total := pg_wal_lsn_diff(v_lsn_after_flush, v_lsn_start);

            PERFORM 0;

    BEGIN
        SELECT COALESCE(sum(write_bytes), 0)
          INTO v_io_on
        FROM pg_stat_io
        WHERE backend_type IN ('client backend', 'background worker')
          AND object = 'relation';
    EXCEPTION WHEN OTHERS THEN
        v_io_on := NULL;
    END;

    INSERT INTO flashback.perf_results(test_name, metric, value_numeric, unit, details)
    VALUES
        ('test4_write_amplification', 'wal_bytes_tracking_off', v_wal_off, 'bytes', NULL),
        ('test4_write_amplification', 'wal_bytes_tracking_on_foreground', v_wal_on_foreground, 'bytes', jsonb_build_object('note', 'update transaction only')),
        ('test4_write_amplification', 'wal_bytes_tracking_on_total', v_wal_on_total, 'bytes', jsonb_build_object('note', 'update + async worker flush window')),
        ('test4_write_amplification', 'wal_amplification_ratio_foreground', CASE WHEN v_wal_off > 0 THEN v_wal_on_foreground / v_wal_off ELSE NULL END, 'ratio', jsonb_build_object('baseline', 'tracking_off_same_workload')),
        ('test4_write_amplification', 'wal_amplification_ratio_total', CASE WHEN v_wal_off > 0 THEN v_wal_on_total / v_wal_off ELSE NULL END, 'ratio', jsonb_build_object('baseline', 'tracking_off_same_workload')),
        ('test4_write_amplification', 'io_bytes_tracking_off', v_io_off, 'bytes', jsonb_build_object('source', 'pg_stat_io')),
        ('test4_write_amplification', 'io_bytes_tracking_on', v_io_on, 'bytes', jsonb_build_object('source', 'pg_stat_io')),
        ('test4_write_amplification', 'io_amplification_ratio', CASE WHEN v_io_off > 0 THEN v_io_on::numeric / v_io_off::numeric ELSE NULL END, 'ratio', NULL);
END;
$$;

SELECT
    test_name,
    metric,
    round(value_numeric::numeric, 3) AS value,
    unit,
    details
FROM flashback.perf_results
ORDER BY test_name, metric;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'flashback_perf_slot') THEN
        PERFORM pg_drop_replication_slot('flashback_perf_slot');
    END IF;
EXCEPTION WHEN OTHERS THEN
    NULL;
END;
$$;
