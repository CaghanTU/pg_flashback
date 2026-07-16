-- Regression: flashback_apply_retention must never drop the partition that
-- covers the current retention window. The old implementation extracted the
-- LOWER bound of the partition (the first quoted value in the bound
-- expression) and treated it as the upper bound, which dropped the active
-- month's partition — with every fresh delta in it — as soon as
-- day-of-month exceeded the retention interval.
DO $tv$
DECLARE
    v_active_part text := 'delta_log_' || to_char(CURRENT_DATE, 'YYYY_MM');
    v_delta_count bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_ret_guard;
    CREATE TABLE public.it_ret_guard (id int primary key, val text);
    PERFORM flashback_track('public.it_ret_guard');

    -- Partition covering today (what the worker's ensure-partitions step creates)
    PERFORM flashback_ensure_delta_partition(CURRENT_DATE);
    IF to_regclass(format('flashback.%I', v_active_part)) IS NULL THEN
        RAISE EXCEPTION 'expected active partition % to exist', v_active_part;
    END IF;

    -- A fresh captured event inside the retention window
    INSERT INTO flashback.delta_log (
        event_time, event_type, table_name, rel_oid, source_xid,
        committed_at, schema_version, old_data, new_data
    )
    VALUES (
        clock_timestamp(), 'INSERT', 'public.it_ret_guard',
        'public.it_ret_guard'::regclass::oid, 1,
        clock_timestamp(), 1, NULL, '{"id": 1, "val": "fresh"}'
    );

    -- An obsolete partition entirely outside any retention window
    PERFORM flashback__create_range_partition(
        'delta_log_it_ret_old',
        '2020-01-01'::timestamptz,
        '2020-02-01'::timestamptz
    );

    PERFORM flashback_apply_retention();

    -- The active partition and its fresh delta must survive
    IF to_regclass(format('flashback.%I', v_active_part)) IS NULL THEN
        RAISE EXCEPTION 'retention dropped the active partition %', v_active_part;
    END IF;

    SELECT count(*) INTO v_delta_count
    FROM flashback.delta_log
    WHERE table_name = 'public.it_ret_guard';
    IF v_delta_count <> 1 THEN
        RAISE EXCEPTION 'retention destroyed fresh deltas (expected 1 event, found %)', v_delta_count;
    END IF;

    -- The obsolete partition must be gone
    IF to_regclass('flashback.delta_log_it_ret_old') IS NOT NULL THEN
        RAISE EXCEPTION 'retention failed to drop the obsolete partition';
    END IF;
END;
$tv$;
