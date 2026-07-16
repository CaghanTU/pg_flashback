-- Test: WAL capture mode behaviors (harness-testable subset)
--
-- The pgrx harness runs each test inside ONE write-dirty transaction, while
-- correctness-qualified WAL tracking requires a dedicated transaction with no
-- prior write — so a successful WAL-mode flashback_track() is impossible here
-- BY DESIGN (fail-closed). This file
-- therefore verifies:
--   1. flashback_effective_capture_mode() returns 'wal' when wal_level=logical
--      and capture_mode='auto' (the default).
--   2. SET pg_flashback.capture_mode='trigger' overrides auto-detection.
--   3. Explicit 'wal' override works.
--   4. flashback_track() in trigger mode does NOT set REPLICA IDENTITY FULL.
--   5. flashback_track() in WAL mode FAILS CLOSED with an exception when the
--      slot is missing and cannot be created — succeeding without a slot
--      would silently capture nothing, which is exactly the disaster this
--      guard prevents. Nothing may be persisted on failure.
--
-- Everything that needs a SUCCESSFUL WAL-mode track (REPLICA IDENTITY FULL,
-- worker consumption, commit-time stamping, restore semantics, untrack RI
-- restore, cross-DB coverage warning) runs in scripts/run_wal_e2e.sh against
-- a live instance with separate committed transactions.

DO $tv$
DECLARE
    v_mode text;
    v_replica_identity char;
BEGIN
    -- ----------------------------------------------------------------
    -- 1. Default auto mode: should resolve to 'wal' when wal_level=logical
    -- ----------------------------------------------------------------
    -- The postgresql_conf_options in pg_test sets wal_level=logical and
    -- capture_mode='trigger' for the test suite. Override here to test auto.
    PERFORM set_config('pg_flashback.capture_mode', 'auto', true);

    SELECT flashback_effective_capture_mode() INTO v_mode;
    IF v_mode <> 'wal' THEN
        RAISE EXCEPTION 'auto mode with wal_level=logical should return wal, got: %', v_mode;
    END IF;

    -- ----------------------------------------------------------------
    -- 2. Explicit 'trigger' override beats auto-detection
    -- ----------------------------------------------------------------
    PERFORM set_config('pg_flashback.capture_mode', 'trigger', true);
    SELECT flashback_effective_capture_mode() INTO v_mode;
    IF v_mode <> 'trigger' THEN
        RAISE EXCEPTION 'explicit trigger mode should return trigger, got: %', v_mode;
    END IF;

    -- ----------------------------------------------------------------
    -- 3. Explicit 'wal' override
    -- ----------------------------------------------------------------
    PERFORM set_config('pg_flashback.capture_mode', 'wal', true);
    SELECT flashback_effective_capture_mode() INTO v_mode;
    IF v_mode <> 'wal' THEN
        RAISE EXCEPTION 'explicit wal mode should return wal, got: %', v_mode;
    END IF;

    -- ----------------------------------------------------------------
    -- 4. flashback_track() in trigger mode does NOT set REPLICA IDENTITY FULL
    -- ----------------------------------------------------------------
    PERFORM set_config('pg_flashback.capture_mode', 'trigger', true);

    DROP TABLE IF EXISTS public.it_wal_behaviors_trig CASCADE;
    CREATE TABLE public.it_wal_behaviors_trig (id int PRIMARY KEY, val text);
    PERFORM flashback_track('public.it_wal_behaviors_trig');

    SELECT c.relreplident
      INTO v_replica_identity
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relname = 'it_wal_behaviors_trig';

    IF v_replica_identity = 'f' THEN
        RAISE EXCEPTION 'trigger mode track should NOT set REPLICA IDENTITY FULL';
    END IF;

    -- ----------------------------------------------------------------
    -- 5. Fail-closed: WAL-mode track must reject the harness's write-dirty
    --    transaction before it attempts slot or lifecycle mutation.
    -- ----------------------------------------------------------------
    PERFORM set_config('pg_flashback.capture_mode', 'wal', true);

    -- Make sure no slot lingers from an earlier run in the same pgdata
    -- (pg_drop_replication_slot is non-transactional and takes effect now).
    PERFORM pg_drop_replication_slot(slot_name)
    FROM pg_replication_slots
    WHERE slot_name = flashback_effective_slot_name()
      AND database = current_database();

    DROP TABLE IF EXISTS public.it_wal_failclosed CASCADE;
    CREATE TABLE public.it_wal_failclosed (id int PRIMARY KEY, val text);
    DECLARE
        v_raised boolean := false;
    BEGIN
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
        -- Fail-closed means nothing was persisted either
        IF EXISTS (
            SELECT 1 FROM flashback.tracked_tables
            WHERE table_name = 'it_wal_failclosed'
        ) THEN
            RAISE EXCEPTION 'fail-closed track must not leave a tracked_tables entry behind';
        END IF;
    END;

    -- Cleanup
    PERFORM set_config('pg_flashback.capture_mode', 'trigger', true);
    DROP TABLE IF EXISTS public.it_wal_behaviors_trig CASCADE;
    DROP TABLE IF EXISTS public.it_wal_failclosed CASCADE;
END;
$tv$;
