-- Metadata commands on related objects are not all table-node ProcessUtility
-- commands (CREATE INDEX/TRIGGER/POLICY, GRANT/COMMENT, ALTER SEQUENCE, ...).
-- Until every class has an exact lifecycle mapping, a later DROP must compare
-- the live full schema contract with the current captured epoch and refuse
-- rather than silently restore stale metadata.
DO $test$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_before bigint;
    v_failed boolean := false;
    v_state text;
BEGIN
    DROP TABLE IF EXISTS public.it_pre_drop_drift CASCADE;
    CREATE TABLE public.it_pre_drop_drift(
        id serial PRIMARY KEY,
        payload text NOT NULL
    );
    INSERT INTO public.it_pre_drop_drift(payload) VALUES ('baseline');

    v_boot := flashback_test_bootstrap_lifecycle(
        'public.it_pre_drop_drift'
    );
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    CREATE INDEX it_pre_drop_drift_payload_idx
        ON public.it_pre_drop_drift(payload);

    SELECT count(*) INTO v_before
    FROM flashback.pending_wal_events
    WHERE tracking_id = v_tracking_id
      AND event_type = 'DROP';

    BEGIN
        PERFORM flashback_stage_local_delta_ddl_event(
            v_tracking_id,
            'DROP',
            99101,
            '0/9910'::pg_lsn,
            flashback_collect_schema_def(
                'public.it_pre_drop_drift'::regclass
            ),
            false
        );
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE;
        IF v_state IS DISTINCT FROM '55000' THEN
            RAISE;
        END IF;
        v_failed := true;
    END;

    IF NOT v_failed THEN
        RAISE EXCEPTION
            'pre-DROP schema drift was not rejected';
    END IF;
    IF (SELECT count(*) FROM flashback.pending_wal_events
        WHERE tracking_id = v_tracking_id
          AND event_type = 'DROP') IS DISTINCT FROM v_before
    THEN
        RAISE EXCEPTION
            'rejected pre-DROP drift left a pending DROP event';
    END IF;

    -- Returning the live catalog to the captured epoch must make the exact
    -- same staging path eligible again.
    DROP INDEX public.it_pre_drop_drift_payload_idx;
    PERFORM flashback_stage_local_delta_ddl_event(
        v_tracking_id,
        'DROP',
        99102,
        '0/9920'::pg_lsn,
        flashback_collect_schema_def(
            'public.it_pre_drop_drift'::regclass
        ),
        false
    );
    IF (SELECT count(*) FROM flashback.pending_wal_events
        WHERE tracking_id = v_tracking_id
          AND event_type = 'DROP') IS DISTINCT FROM v_before + 1
    THEN
        RAISE EXCEPTION
            'matching pre-DROP schema contract did not stage exactly once';
    END IF;
END;
$test$;
