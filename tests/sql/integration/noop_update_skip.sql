-- Test: no-op UPDATE is skipped by capture trigger.
-- When UPDATE doesn't change any column values, the trigger should
-- not insert any event into staging_events.
-- NOTE: This test requires trigger mode, force it explicitly.
DO $tv$
DECLARE
    v_count bigint;
BEGIN
    PERFORM set_config('pg_flashback.capture_mode', 'trigger', true);

    DROP TABLE IF EXISTS public.it_noop_update;
    CREATE TABLE public.it_noop_update (id int PRIMARY KEY, name text, val int);
    INSERT INTO public.it_noop_update VALUES (1, 'test', 42);

    PERFORM flashback_track('public.it_noop_update');
    -- flashback_track attaches real capture triggers in trigger mode
    -- flashback_track already attaches them

    -- Clear staging
    DELETE FROM flashback.staging_events
    WHERE rel_oid = 'public.it_noop_update'::regclass::oid;

    -- Perform a no-op update (SET with same values)
    UPDATE public.it_noop_update SET name = 'test', val = 42 WHERE id = 1;

    -- Check: should be 0 events in staging for this table
    SELECT count(*) INTO v_count
    FROM flashback.staging_events
    WHERE rel_oid = 'public.it_noop_update'::regclass::oid;

    IF v_count <> 0 THEN
        RAISE EXCEPTION 'no-op UPDATE should produce 0 events, got %', v_count;
    END IF;

    -- Now do a real update
    UPDATE public.it_noop_update SET val = 99 WHERE id = 1;

    SELECT count(*) INTO v_count
    FROM flashback.staging_events
    WHERE rel_oid = 'public.it_noop_update'::regclass::oid;

    IF v_count <> 1 THEN
        RAISE EXCEPTION 'real UPDATE should produce 1 event, got %', v_count;
    END IF;
END;
$tv$;
