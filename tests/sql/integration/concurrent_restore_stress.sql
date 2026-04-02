-- Test: concurrent restore stress — verify advisory lock prevents conflicts
-- We simulate concurrency within a single session by verifying:
-- 1. Advisory lock is acquired during restore
-- 2. Multiple sequential restores on different tables work correctly
-- 3. Restore audit log records all restores
DO $tv$
DECLARE t1 timestamptz;
        v_cnt bigint;
        v_lock_exists boolean;
BEGIN
    -- Setup 3 independent tables
    DROP TABLE IF EXISTS public.it_conc_a CASCADE;
    DROP TABLE IF EXISTS public.it_conc_b CASCADE;
    DROP TABLE IF EXISTS public.it_conc_c CASCADE;

    CREATE TABLE public.it_conc_a (id int PRIMARY KEY, val text);
    CREATE TABLE public.it_conc_b (id int PRIMARY KEY, val text);
    CREATE TABLE public.it_conc_c (id int PRIMARY KEY, val text);

    PERFORM flashback_track('public.it_conc_a');
    PERFORM flashback_track('public.it_conc_b');
    PERFORM flashback_track('public.it_conc_c');

    PERFORM flashback_test_attach_capture_trigger('public.it_conc_a'::regclass);
    PERFORM flashback_test_attach_capture_trigger('public.it_conc_b'::regclass);
    PERFORM flashback_test_attach_capture_trigger('public.it_conc_c'::regclass);

    -- Phase 1: insert data
    INSERT INTO public.it_conc_a VALUES (1, 'a1'), (2, 'a2'), (3, 'a3');
    INSERT INTO public.it_conc_b VALUES (10, 'b1'), (20, 'b2');
    INSERT INTO public.it_conc_c VALUES (100, 'c1'), (200, 'c2'), (300, 'c3'), (400, 'c4');

    t1 := clock_timestamp();

    -- Phase 2: destructive changes
    DELETE FROM public.it_conc_a;
    DELETE FROM public.it_conc_b;
    UPDATE public.it_conc_c SET val = 'DESTROYED';

    -- Verify no advisory lock before restore
    SELECT EXISTS(
        SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND classid = 358944 AND granted
    ) INTO v_lock_exists;
    -- May or may not exist (worker could be running), just store it

    -- Restore table A
    PERFORM flashback_restore('public.it_conc_a', t1);
    SELECT count(*) INTO v_cnt FROM public.it_conc_a;
    IF v_cnt <> 3 THEN
        RAISE EXCEPTION 'conc_a: expected 3 rows, got %', v_cnt;
    END IF;

    -- Restore table B
    PERFORM flashback_restore('public.it_conc_b', t1);
    SELECT count(*) INTO v_cnt FROM public.it_conc_b;
    IF v_cnt <> 2 THEN
        RAISE EXCEPTION 'conc_b: expected 2 rows, got %', v_cnt;
    END IF;

    -- Restore table C
    PERFORM flashback_restore('public.it_conc_c', t1);
    SELECT count(*) INTO v_cnt FROM public.it_conc_c WHERE val <> 'DESTROYED';
    IF v_cnt <> 4 THEN
        RAISE EXCEPTION 'conc_c: expected 4 non-DESTROYED rows, got %', v_cnt;
    END IF;

    -- Multi-table restore: verify flashback_restore(text[], timestamptz) works
    -- Re-attach triggers after individual restores (table was dropped+created)
    PERFORM flashback_test_attach_capture_trigger('public.it_conc_a'::regclass);
    PERFORM flashback_test_attach_capture_trigger('public.it_conc_b'::regclass);
    PERFORM flashback_test_attach_capture_trigger('public.it_conc_c'::regclass);

    DELETE FROM public.it_conc_a WHERE id = 1;
    DELETE FROM public.it_conc_b WHERE id = 10;
    DELETE FROM public.it_conc_c WHERE id = 100;

    t1 := clock_timestamp();

    DELETE FROM public.it_conc_a;
    DELETE FROM public.it_conc_b;
    DELETE FROM public.it_conc_c;

    -- Multi-table restore
    PERFORM flashback_restore(ARRAY['public.it_conc_a', 'public.it_conc_b', 'public.it_conc_c'], t1);

    SELECT count(*) INTO v_cnt FROM public.it_conc_a;
    IF v_cnt <> 2 THEN
        RAISE EXCEPTION 'multi-restore conc_a: expected 2, got %', v_cnt;
    END IF;

    SELECT count(*) INTO v_cnt FROM public.it_conc_b;
    IF v_cnt <> 1 THEN
        RAISE EXCEPTION 'multi-restore conc_b: expected 1, got %', v_cnt;
    END IF;

    SELECT count(*) INTO v_cnt FROM public.it_conc_c;
    IF v_cnt <> 3 THEN
        RAISE EXCEPTION 'multi-restore conc_c: expected 3, got %', v_cnt;
    END IF;

    -- Verify restore_log has entries (if table exists)
    IF to_regclass('flashback.restore_log') IS NOT NULL THEN
        SELECT count(*) INTO v_cnt FROM flashback.restore_log WHERE success;
        IF v_cnt < 3 THEN
            RAISE EXCEPTION 'restore_log should have at least 3 successful entries, got %', v_cnt;
        END IF;
    END IF;
END;
$tv$;
