-- Illegal capture_mode values fail closed: track rejected, no leftover
-- metadata, doctor reports error.
DO $tv$
DECLARE
    v_raised boolean;
    v_msg text;
    v_doctor_err bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_bad_mode CASCADE;
    CREATE TABLE public.it_bad_mode (id int PRIMARY KEY, val text);

    -- trigger
    PERFORM set_config('pg_flashback.capture_mode', 'trigger', true);
    v_raised := false;
    BEGIN
        PERFORM flashback_track('public.it_bad_mode');
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'track under capture_mode=trigger must fail';
    END IF;
    IF EXISTS (SELECT 1 FROM flashback.tracked_tables WHERE table_name = 'it_bad_mode')
       OR EXISTS (
            SELECT 1 FROM flashback.snapshots s
            JOIN pg_class c ON c.oid = s.rel_oid
            WHERE c.relname = 'it_bad_mode'
       )
       OR EXISTS (
            SELECT 1 FROM flashback.coverage_generations g
            JOIN flashback.tracked_tables t ON t.tracking_id = g.tracking_id
            WHERE t.table_name = 'it_bad_mode'
       )
    THEN
        RAISE EXCEPTION 'trigger-mode track left leftover metadata';
    END IF;

    SELECT count(*) INTO v_doctor_err
    FROM flashback_doctor()
    WHERE check_name = 'effective_capture_mode'
      AND status = 'error';
    IF v_doctor_err <> 1 THEN
        RAISE EXCEPTION 'doctor must report effective_capture_mode error under trigger, got %',
            v_doctor_err;
    END IF;

    -- auto
    PERFORM set_config('pg_flashback.capture_mode', 'auto', true);
    v_raised := false;
    BEGIN
        PERFORM flashback_track('public.it_bad_mode');
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'track under capture_mode=auto must fail';
    END IF;
    IF EXISTS (SELECT 1 FROM flashback.tracked_tables WHERE table_name = 'it_bad_mode') THEN
        RAISE EXCEPTION 'auto-mode track left leftover metadata';
    END IF;

    SELECT count(*) INTO v_doctor_err
    FROM flashback_doctor()
    WHERE check_name = 'effective_capture_mode'
      AND status = 'error';
    IF v_doctor_err <> 1 THEN
        RAISE EXCEPTION 'doctor must report effective_capture_mode error under auto, got %',
            v_doctor_err;
    END IF;

    PERFORM set_config('pg_flashback.capture_mode', 'wal', true);
    DROP TABLE IF EXISTS public.it_bad_mode CASCADE;
END;
$tv$;
