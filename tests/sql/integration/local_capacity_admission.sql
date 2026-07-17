-- Adversarial local capacity / write-stall admission.
-- Fail-closed budgets must reject before CTAS / swap; override is privileged
-- and visible; successful bounded operations still work.
DROP TABLE IF EXISTS public.it_capacity_guard CASCADE;

CREATE TABLE public.it_capacity_guard (
    id integer PRIMARY KEY,
    payload text
);
INSERT INTO public.it_capacity_guard
SELECT g, repeat('x', 200)
FROM generate_series(1, 200) AS g;

DO $test$
DECLARE
    v_failed boolean;
    v_msg text;
    v_advice record;
    v_tracked boolean;
BEGIN
    PERFORM set_config('pg_flashback.capture_mode', 'wal', true);
    PERFORM set_config('pg_flashback.local_max_snapshot_bytes', '1kB', true);
    PERFORM set_config('pg_flashback.local_max_restore_peak_bytes', '16GB', true);
    PERFORM set_config('pg_flashback.local_min_filesystem_bytes', '64MB', true);
    PERFORM set_config('pg_flashback.local_safety_reserve_bytes', '1kB', true);
    PERFORM set_config('pg_flashback.local_capacity_override', 'off', true);

    SELECT * INTO v_advice FROM flashback_advise('public.it_capacity_guard'::regclass);
    IF v_advice.recommendation IS NULL
       OR v_advice.recommendation NOT LIKE '%reject local profile%'
    THEN
        RAISE EXCEPTION 'flashback_advise did not recommend rejecting undersized snapshot budget: %',
            v_advice.recommendation;
    END IF;

    v_failed := false;
    BEGIN
        -- Dedicated transaction semantics are enforced by track itself; this
        -- DO block already assigned an XID, so WAL track would reject for that
        -- reason first. Exercise the shared admission primitive directly.
        PERFORM flashback_admit_local_capacity('public.it_capacity_guard'::regclass, 'track');
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_failed OR v_msg NOT LIKE '%capacity admission failed%' THEN
        RAISE EXCEPTION 'track capacity admission did not fail closed: %', v_msg;
    END IF;

    -- TOAST-heavy estimate must count toast separately from indexes.
    IF v_advice.live_toast_bytes < 0 OR v_advice.live_index_bytes < 0 THEN
        RAISE EXCEPTION 'capacity model returned negative toast/index bytes';
    END IF;
    IF v_advice.projected_base_snapshot_bytes
           <> (v_advice.live_heap_bytes + v_advice.live_toast_bytes)
    THEN
        RAISE EXCEPTION 'base snapshot projection incorrectly includes indexes';
    END IF;
    IF v_advice.projected_restore_shadow_bytes
           <> (v_advice.live_heap_bytes + v_advice.live_toast_bytes + v_advice.live_index_bytes)
    THEN
        RAISE EXCEPTION 'restore shadow projection missing rebuilt indexes';
    END IF;

    -- Restore peak budget rejection.
    PERFORM set_config('pg_flashback.local_max_snapshot_bytes', '8GB', true);
    PERFORM set_config('pg_flashback.local_max_restore_peak_bytes', '1kB', true);
    v_failed := false;
    BEGIN
        PERFORM flashback_admit_local_capacity('public.it_capacity_guard'::regclass, 'restore');
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_failed OR v_msg NOT LIKE '%capacity admission failed%' THEN
        RAISE EXCEPTION 'restore capacity admission did not fail closed: %', v_msg;
    END IF;

    -- Privileged override is visible and admits the same failure.
    PERFORM set_config('pg_flashback.local_capacity_override', 'on', true);
    PERFORM flashback_admit_local_capacity('public.it_capacity_guard'::regclass, 'restore');
    SELECT * INTO v_advice FROM flashback_advise('public.it_capacity_guard'::regclass);
    IF NOT v_advice.capacity_override THEN
        RAISE EXCEPTION 'capacity override was not visible in flashback_advise()';
    END IF;
    IF v_advice.recommendation NOT LIKE '%local_capacity_override is active%' THEN
        RAISE EXCEPTION 'override was not reflected in recommendation: %',
            v_advice.recommendation;
    END IF;

    -- Successful bounded admission still works.
    PERFORM set_config('pg_flashback.local_capacity_override', 'off', true);
    PERFORM set_config('pg_flashback.local_max_snapshot_bytes', '8GB', true);
    PERFORM set_config('pg_flashback.local_max_restore_peak_bytes', '16GB', true);
    PERFORM flashback_admit_local_capacity('public.it_capacity_guard'::regclass, 'track');
    PERFORM flashback_admit_local_capacity('public.it_capacity_guard'::regclass, 'reanchor');
    PERFORM flashback_admit_local_capacity('public.it_capacity_guard'::regclass, 'restore');

    -- Unset budgets remain fail-closed.
    PERFORM set_config('pg_flashback.local_max_snapshot_bytes', '0', true);
    v_failed := false;
    BEGIN
        PERFORM flashback_admit_local_capacity('public.it_capacity_guard'::regclass, 'track');
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_failed OR v_msg NOT LIKE '%unset%' THEN
        RAISE EXCEPTION 'unset snapshot budget did not fail closed: %', v_msg;
    END IF;

    -- Filesystem probe must return a positive estimate on a live cluster.
    IF flashback_relation_filesystem_available_bytes('public.it_capacity_guard'::regclass) <= 0 THEN
        RAISE EXCEPTION 'filesystem available bytes probe returned non-positive value';
    END IF;

    -- Trigger-mode track still functions for legacy tests without depending on
    -- the WAL-only lock/copy path exercised above.
    PERFORM set_config('pg_flashback.capture_mode', 'trigger', true);
    PERFORM set_config('pg_flashback.local_max_snapshot_bytes', '8GB', true);
    v_tracked := flashback_track('public.it_capacity_guard');
    IF NOT v_tracked THEN
        RAISE EXCEPTION 'bounded trigger track failed unexpectedly';
    END IF;
    PERFORM flashback_untrack('public.it_capacity_guard');
END;
$test$;
