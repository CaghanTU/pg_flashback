-- WAL-only capture_mode contract (negative): trigger/auto rejected; default wal;
-- rejected track leaves no tracking metadata.
--
-- Successful production WAL track requires a dedicated transaction (no prior
-- write). The pgrx harness runs each test inside one write-dirty transaction,
-- so flashback_track() must fail closed here — that is intentional.

DO $tv$
DECLARE
    v_mode text;
    v_raised boolean;
    v_msg text;
BEGIN
    -- Default / empty / wal resolve to wal.
    PERFORM set_config('pg_flashback.capture_mode', 'wal', true);
    SELECT flashback_effective_capture_mode() INTO v_mode;
    IF v_mode <> 'wal' THEN
        RAISE EXCEPTION 'explicit wal mode should return wal, got: %', v_mode;
    END IF;

    PERFORM set_config('pg_flashback.capture_mode', '', true);
    SELECT flashback_effective_capture_mode() INTO v_mode;
    IF v_mode <> 'wal' THEN
        RAISE EXCEPTION 'empty capture_mode should default to wal, got: %', v_mode;
    END IF;

    -- Explicit trigger rejected.
    PERFORM set_config('pg_flashback.capture_mode', 'trigger', true);
    v_raised := false;
    BEGIN
        PERFORM flashback_effective_capture_mode();
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT LIKE '%not supported%' THEN
        RAISE EXCEPTION 'trigger capture_mode must be rejected, got: %', v_msg;
    END IF;

    -- Explicit auto rejected.
    PERFORM set_config('pg_flashback.capture_mode', 'auto', true);
    v_raised := false;
    BEGIN
        PERFORM flashback_effective_capture_mode();
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT LIKE '%not supported%' THEN
        RAISE EXCEPTION 'auto capture_mode must be rejected, got: %', v_msg;
    END IF;

    -- Rejected track under illegal mode leaves no metadata.
    PERFORM set_config('pg_flashback.capture_mode', 'trigger', true);
    DROP TABLE IF EXISTS public.it_wal_behaviors_rej CASCADE;
    CREATE TABLE public.it_wal_behaviors_rej (id int PRIMARY KEY, val text);
    v_raised := false;
    BEGIN
        PERFORM flashback_track('public.it_wal_behaviors_rej');
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'flashback_track under capture_mode=trigger must fail';
    END IF;
    IF EXISTS (
        SELECT 1 FROM flashback.tracked_tables
        WHERE table_name = 'it_wal_behaviors_rej'
    ) OR EXISTS (
        SELECT 1 FROM flashback.snapshots s
        JOIN flashback.tracked_tables t ON t.tracking_id = s.tracking_id
        WHERE t.table_name = 'it_wal_behaviors_rej'
    ) OR EXISTS (
        SELECT 1 FROM flashback.coverage_generations g
        JOIN flashback.tracked_tables t ON t.tracking_id = g.tracking_id
        WHERE t.table_name = 'it_wal_behaviors_rej'
    ) THEN
        RAISE EXCEPTION 'rejected track left leftover tracking metadata';
    END IF;

    -- WAL mode still fail-closed outside a dedicated transaction.
    PERFORM set_config('pg_flashback.capture_mode', 'wal', true);
    DROP TABLE IF EXISTS public.it_wal_failclosed CASCADE;
    CREATE TABLE public.it_wal_failclosed (id int PRIMARY KEY, val text);
    v_raised := false;
    BEGIN
        PERFORM flashback_track('public.it_wal_failclosed');
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM NOT LIKE '%dedicated transaction%' THEN
            RAISE;
        END IF;
        v_raised := true;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'flashback_track in WAL mode must fail closed outside a dedicated transaction';
    END IF;
    IF EXISTS (
        SELECT 1 FROM flashback.tracked_tables
        WHERE table_name = 'it_wal_failclosed'
    ) THEN
        RAISE EXCEPTION 'fail-closed track must not leave a tracked_tables entry behind';
    END IF;

    DROP TABLE IF EXISTS public.it_wal_behaviors_rej CASCADE;
    DROP TABLE IF EXISTS public.it_wal_failclosed CASCADE;
END;
$tv$;
