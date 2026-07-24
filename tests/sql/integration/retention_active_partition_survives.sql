-- Regression: flashback_apply_retention must never drop the partition that
-- covers the current retention window. The old implementation extracted the
-- LOWER bound of the partition (the first quoted value in the bound
-- expression) and treated it as the upper bound, which dropped the active
-- month's partition — with every fresh delta in it — as soon as
-- day-of-month exceeded the retention interval.
DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_active_part text := 'delta_log_' || to_char(CURRENT_DATE, 'YYYY_MM');
    v_delta_count bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_ret_guard;
    CREATE TABLE public.it_ret_guard (id int primary key, val text);

    SELECT flashback_test_bootstrap_lifecycle('public.it_ret_guard') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    PERFORM flashback_ensure_delta_partition(CURRENT_DATE);
    IF to_regclass(format('flashback.%I', v_active_part)) IS NULL THEN
        RAISE EXCEPTION 'expected active partition % to exist', v_active_part;
    END IF;

    INSERT INTO public.it_ret_guard VALUES (1, 'fresh');
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/2000'::pg_lsn,
        clock_timestamp(),
        950001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"val":"fresh"}'::jsonb)
        )
    );

    PERFORM flashback__create_range_partition(
        'delta_log_it_ret_old',
        '2020-01-01'::timestamptz,
        '2020-02-01'::timestamptz
    );

    PERFORM flashback_apply_retention();

    IF to_regclass(format('flashback.%I', v_active_part)) IS NULL THEN
        RAISE EXCEPTION 'retention dropped the active partition %', v_active_part;
    END IF;

    SELECT count(*) INTO v_delta_count
    FROM flashback.delta_log
    WHERE table_name = 'public.it_ret_guard';
    IF v_delta_count <> 1 THEN
        RAISE EXCEPTION 'retention destroyed fresh deltas (expected 1 event, found %)', v_delta_count;
    END IF;

    IF to_regclass('flashback.delta_log_it_ret_old') IS NOT NULL THEN
        RAISE EXCEPTION 'retention failed to drop the obsolete partition';
    END IF;
END;
$tv$;
