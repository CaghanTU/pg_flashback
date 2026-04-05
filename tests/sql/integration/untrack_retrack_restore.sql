-- Test: flashback_untrack() followed by flashback_track() on the same table
-- does not corrupt state or leave stale metadata.
-- After re-tracking and inserting new events, restore must work correctly.
DO $tv$
DECLARE
    t1       timestamptz;
    t2       timestamptz;
    v_cnt    bigint;
    v_tracked bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_retrack CASCADE;
    CREATE TABLE public.it_retrack (id int PRIMARY KEY, val text);

    -- ── First tracking session ────────────────────────────────
    PERFORM flashback_track('public.it_retrack');
    PERFORM flashback_test_attach_capture_trigger('public.it_retrack'::regclass);

    INSERT INTO public.it_retrack VALUES (1, 'first'), (2, 'track');
    t1 := clock_timestamp();
    DELETE FROM public.it_retrack;

    -- Untrack: should remove all metadata
    PERFORM flashback_untrack('public.it_retrack');

    -- Confirm untracked
    SELECT count(*) INTO v_tracked
    FROM flashback.tracked_tables
    WHERE rel_oid = 'public.it_retrack'::regclass::oid AND is_active;

    IF v_tracked <> 0 THEN
        RAISE EXCEPTION 'table still appears tracked after untrack';
    END IF;

    -- ── Second tracking session ───────────────────────────────
    PERFORM flashback_track('public.it_retrack');
    PERFORM flashback_test_attach_capture_trigger('public.it_retrack'::regclass);

    INSERT INTO public.it_retrack VALUES (10, 'second'), (20, 'track'), (30, 'session');
    t2 := clock_timestamp();
    DELETE FROM public.it_retrack;

    -- Restore should use the NEW tracking session only
    PERFORM flashback_restore('public.it_retrack', t2);

    SELECT count(*) INTO v_cnt FROM public.it_retrack;
    IF v_cnt <> 3 THEN
        RAISE EXCEPTION 'expected 3 rows from second session, got %', v_cnt;
    END IF;

    -- Rows from first session (id=1,2) must not appear
    IF EXISTS (SELECT 1 FROM public.it_retrack WHERE id IN (1, 2)) THEN
        RAISE EXCEPTION 'stale rows from first tracking session leaked into restore';
    END IF;

    DROP TABLE IF EXISTS public.it_retrack CASCADE;
END;
$tv$;
