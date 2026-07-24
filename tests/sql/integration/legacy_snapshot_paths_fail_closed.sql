-- Every `local_delta` tracked table always has a coverage_generations row
-- immediately at bootstrap in this WAL-only architecture. The pre-generation
-- legacy APIs below therefore have direct-snapshot-payload code paths (raw
-- CTAS / UPDATE flashback.snapshots / to_regclass(snapshot_table)) that are
-- unreachable in production, since each function raises before ever getting
-- there. This test is the required proof that those legacy paths are
-- fail-closed rather than silently left as a second, un-migrated route to
-- flashback.snapshots.
DROP TABLE IF EXISTS public.it_legacy_fail_closed CASCADE;

CREATE TABLE public.it_legacy_fail_closed (id integer PRIMARY KEY, note text);
INSERT INTO public.it_legacy_fail_closed VALUES (1, 'v0');

DO $test$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_failed boolean;
BEGIN
    SELECT flashback_test_bootstrap_lifecycle('public.it_legacy_fail_closed') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    IF NOT EXISTS (
        SELECT 1 FROM flashback.coverage_generations WHERE tracking_id = v_tracking_id
    ) THEN
        RAISE EXCEPTION 'test setup invariant broken: bootstrap did not create a generation';
    END IF;

    -- flashback_checkpoint: legacy full-table checkpoint API.
    v_failed := false;
    BEGIN
        PERFORM flashback_checkpoint('public.it_legacy_fail_closed');
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM LIKE '%disabled for correctness-qualified WAL generations%' THEN
            v_failed := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'flashback_checkpoint accepted a correctness-qualified generation';
    END IF;

    -- flashback_query(timestamp): legacy point-in-time SELECT AS OF.
    v_failed := false;
    BEGIN
        PERFORM 1 FROM flashback_query(
            'public.it_legacy_fail_closed', clock_timestamp(), 'id integer'
        ) AS q(id integer);
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM LIKE '%disabled for correctness-qualified WAL coverage%' THEN
            v_failed := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'flashback_query(timestamp) accepted a correctness-qualified generation';
    END IF;

    -- flashback_recover_deleted(timestamp): legacy timestamp-based recovery.
    v_failed := false;
    BEGIN
        PERFORM flashback_recover_deleted(
            'public.it_legacy_fail_closed', clock_timestamp()
        );
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM LIKE '%disabled for correctness-qualified WAL coverage%' THEN
            v_failed := true;
        ELSE
            RAISE;
        END IF;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'flashback_recover_deleted(timestamp) accepted a correctness-qualified generation';
    END IF;
END;
$test$;
