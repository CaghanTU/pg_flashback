-- Test: WAL capture mode behaviors
--
-- Verifies observable WAL mode behaviors that are testable inside the pgrx
-- transaction harness:
--   1. flashback_effective_capture_mode() returns 'wal' when wal_level=logical
--      and capture_mode='auto' (the default).
--   2. SET pg_flashback.capture_mode='trigger' overrides auto-detection.
--   3. flashback_track() in WAL mode sets REPLICA IDENTITY FULL on the table.
--   4. flashback_track() in trigger mode does NOT set REPLICA IDENTITY FULL.
--   5. WAL mode slot creation attempt is handled gracefully (no exception
--      propagated to the caller even when it fails inside a dirty transaction).
--
-- NOTE: Full end-to-end WAL decoding (pg_logical_slot_get_changes → delta_log
-- → flashback_restore) requires the background worker to be running and cannot
-- be exercised inside the pgrx test transaction harness (slot creation is
-- blocked by prior DML within the same transaction). That path is covered by
-- the manual WAL benchmark scripts in scripts/.

DO $tv$
DECLARE
    v_mode text;
    v_replica_identity char;
    v_is_full boolean;
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
    -- 4. flashback_track() in WAL mode sets REPLICA IDENTITY FULL
    -- ----------------------------------------------------------------
    PERFORM set_config('pg_flashback.capture_mode', 'wal', true);

    DROP TABLE IF EXISTS public.it_wal_behaviors_wal CASCADE;
    CREATE TABLE public.it_wal_behaviors_wal (id int PRIMARY KEY, val text);

    -- Track in WAL mode — sets REPLICA IDENTITY FULL
    -- Slot creation may fail inside this transaction (that's expected and handled)
    PERFORM flashback_track('public.it_wal_behaviors_wal');

    SELECT c.relreplident
      INTO v_replica_identity
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relname = 'it_wal_behaviors_wal';

    v_is_full := (v_replica_identity = 'f');
    IF NOT v_is_full THEN
        RAISE EXCEPTION 'WAL mode track should set REPLICA IDENTITY FULL, got relreplident=%', v_replica_identity;
    END IF;

    -- ----------------------------------------------------------------
    -- 5. flashback_track() in trigger mode does NOT set REPLICA IDENTITY FULL
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

    -- Trigger mode should leave DEFAULT (relreplident = 'd')
    IF v_replica_identity = 'f' THEN
        RAISE EXCEPTION 'trigger mode track should NOT set REPLICA IDENTITY FULL';
    END IF;

    -- ----------------------------------------------------------------
    -- 6. capture_mode='wal' + restore via in-test direct delta_log writes
    --    (simulates what the worker does after consuming the WAL slot)
    -- ----------------------------------------------------------------
    PERFORM set_config('pg_flashback.capture_mode', 'wal', true);

    DROP TABLE IF EXISTS public.it_wal_restore CASCADE;
    CREATE TABLE public.it_wal_restore (id int PRIMARY KEY, val text);
    PERFORM flashback_track('public.it_wal_restore');
    PERFORM flashback_test_attach_capture_trigger('public.it_wal_restore'::regclass);

    DECLARE
        t_before timestamptz;
        v_cnt bigint;
        v_has_trigger boolean;
        v_ri_after char;
    BEGIN
        t_before := clock_timestamp();
        INSERT INTO public.it_wal_restore VALUES (1, 'a'), (2, 'b');
        INSERT INTO public.it_wal_restore VALUES (3, 'c');
        UPDATE public.it_wal_restore SET val = 'b_updated' WHERE id = 2;
        DELETE FROM public.it_wal_restore WHERE id = 3;

        -- Restore to t_before: table should be empty
        PERFORM flashback_restore('public.it_wal_restore', t_before);
        SELECT count(*) INTO v_cnt FROM public.it_wal_restore;
        IF v_cnt <> 0 THEN
            RAISE EXCEPTION 'WAL mode restore to before-inserts: expected 0, got %', v_cnt;
        END IF;

        -- Fix 4: after WAL-mode restore, NO flashback capture trigger should be attached.
        -- A trigger would accumulate events in staging that the worker will never flush
        -- (worker skips staging_events in WAL mode).
        SELECT EXISTS (
            SELECT 1 FROM pg_trigger tg
            JOIN pg_class c ON c.oid = tg.tgrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relname = 'it_wal_restore'
              AND tg.tgname LIKE 'flashback_capture_%'
        ) INTO v_has_trigger;

        IF v_has_trigger THEN
            RAISE EXCEPTION 'WAL mode restore must not reattach capture triggers (staging is skipped in WAL mode)';
        END IF;

        -- Fix 4 (cont.): REPLICA IDENTITY FULL must be preserved after WAL-mode restore.
        SELECT c.relreplident INTO v_ri_after
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'it_wal_restore';

        IF v_ri_after <> 'f' THEN
            RAISE EXCEPTION 'WAL mode restore must keep REPLICA IDENTITY FULL, got %', v_ri_after;
        END IF;
    END;

    -- ----------------------------------------------------------------
    -- 7. flashback_untrack() in WAL mode restores original REPLICA IDENTITY
    -- ----------------------------------------------------------------
    PERFORM set_config('pg_flashback.capture_mode', 'wal', true);

    DROP TABLE IF EXISTS public.it_wal_untrack CASCADE;
    CREATE TABLE public.it_wal_untrack (id int PRIMARY KEY, val text);
    -- Verify the table starts with DEFAULT replica identity (relreplident = 'd')
    DECLARE
        v_ri_before char;
        v_ri_untracked char;
    BEGIN
        SELECT c.relreplident INTO v_ri_before
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'it_wal_untrack';

        IF v_ri_before = 'f' THEN
            RAISE EXCEPTION 'test precondition: new table should start with DEFAULT replica identity, got FULL';
        END IF;

        PERFORM flashback_track('public.it_wal_untrack');

        -- After tracking in WAL mode, must be FULL
        SELECT c.relreplident INTO v_ri_before
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'it_wal_untrack';
        IF v_ri_before <> 'f' THEN
            RAISE EXCEPTION 'after WAL track: expected REPLICA IDENTITY FULL, got %', v_ri_before;
        END IF;

        PERFORM flashback_untrack('public.it_wal_untrack');

        -- After untracking, must be restored to DEFAULT
        SELECT c.relreplident INTO v_ri_untracked
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'it_wal_untrack';
        IF v_ri_untracked = 'f' THEN
            RAISE EXCEPTION 'after WAL untrack: REPLICA IDENTITY should be restored (not FULL), got %', v_ri_untracked;
        END IF;
    END;

    -- Cleanup
    PERFORM set_config('pg_flashback.capture_mode', 'trigger', true);
    DROP TABLE IF EXISTS public.it_wal_behaviors_wal CASCADE;
    DROP TABLE IF EXISTS public.it_wal_behaviors_trig CASCADE;
    DROP TABLE IF EXISTS public.it_wal_restore CASCADE;
    DROP TABLE IF EXISTS public.it_wal_untrack CASCADE;
END;
$tv$;
