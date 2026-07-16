-- Trigger-pipeline test: exercises the production TRIGGER capture path
--   production triggers -> staging_events -> flashback_flush_staging -> delta_log -> restore
-- Every other DML test swaps the production triggers for a test-only trigger
-- that writes delta_log directly; this one deliberately does not.
-- Scope note: this covers the trigger pipeline only. The background worker,
-- logical slot, WAL consume and cross-database behavior need committed
-- transactions and separate sessions — covered by scripts/run_wal_e2e.sh.
DO $tv$
DECLARE
    t_mid timestamptz;
    v_staged bigint;
    v_flushed integer;
    v_count bigint;
    v_val text;
BEGIN
    DROP TABLE IF EXISTS public.it_real_pipeline;
    CREATE TABLE public.it_real_pipeline (id int primary key, val text);
    PERFORM flashback_track('public.it_real_pipeline');
    -- NOTE: no flashback_test_attach_capture_trigger here — real triggers stay.

    INSERT INTO public.it_real_pipeline VALUES (1, 'keep'), (2, 'victim');
    UPDATE public.it_real_pipeline SET val = 'keep-updated' WHERE id = 1;

    -- Events must have landed in staging via the production triggers
    SELECT count(*) INTO v_staged FROM flashback.staging_events
    WHERE rel_oid = 'public.it_real_pipeline'::regclass::oid;
    IF v_staged < 3 THEN
        RAISE EXCEPTION 'real capture triggers produced % staging events, expected >= 3', v_staged;
    END IF;

    PERFORM pg_sleep(0.01);
    t_mid := clock_timestamp();
    PERFORM pg_sleep(0.01);

    -- Disaster after t_mid
    DELETE FROM public.it_real_pipeline WHERE id = 2;
    UPDATE public.it_real_pipeline SET val = 'clobbered' WHERE id = 1;

    -- Promote staging -> delta_log the way the worker would
    SELECT flashback_flush_staging(1000) INTO v_flushed;
    IF v_flushed < 5 THEN
        RAISE EXCEPTION 'flush promoted % events, expected >= 5', v_flushed;
    END IF;

    PERFORM flashback_restore('public.it_real_pipeline', t_mid);

    SELECT count(*) INTO v_count FROM public.it_real_pipeline;
    IF v_count <> 2 THEN
        RAISE EXCEPTION 'expected 2 rows after restore, found %', v_count;
    END IF;
    SELECT val INTO v_val FROM public.it_real_pipeline WHERE id = 1;
    IF v_val <> 'keep-updated' THEN
        RAISE EXCEPTION 'expected id=1 val=keep-updated after restore, found %', v_val;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.it_real_pipeline WHERE id = 2 AND val = 'victim') THEN
        RAISE EXCEPTION 'row id=2 was not recovered by restore';
    END IF;
END;
$tv$;
