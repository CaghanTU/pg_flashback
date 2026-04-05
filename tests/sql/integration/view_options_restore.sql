-- Test: View reloptions (security_barrier, check_option) survive flashback_restore().
-- A security_barrier view prevents early filter pushdown into untrusted functions.
-- WITH CHECK OPTION enforces that INSERTs/UPDATEs via the view satisfy the view predicate.
DO $tv$
DECLARE
    t_before    timestamptz;
    v_barrier   bool;
    v_check_opt text;
BEGIN
    -- ── Base table ───────────────────────────────────────────
    DROP TABLE IF EXISTS public.it_vopts_base CASCADE;
    CREATE TABLE public.it_vopts_base (id int PRIMARY KEY, active bool, val text);

    -- ── security_barrier view ────────────────────────────────
    CREATE VIEW public.it_vopts_secure WITH (security_barrier = true) AS
        SELECT id, val FROM public.it_vopts_base WHERE active = true;

    -- ── WITH LOCAL CHECK OPTION view ─────────────────────────
    CREATE VIEW public.it_vopts_check AS
        SELECT id, val FROM public.it_vopts_base WHERE active = true
        WITH LOCAL CHECK OPTION;

    -- ── Track + populate ─────────────────────────────────────
    PERFORM flashback_track('public.it_vopts_base');
    PERFORM flashback_test_attach_capture_trigger('public.it_vopts_base'::regclass);

    INSERT INTO public.it_vopts_base VALUES (1, true, 'visible'), (2, false, 'hidden');
    t_before := clock_timestamp();
    DELETE FROM public.it_vopts_base;

    PERFORM flashback_restore('public.it_vopts_base', t_before);

    -- ── Verify data ──────────────────────────────────────────
    IF (SELECT count(*) FROM public.it_vopts_base) <> 2 THEN
        RAISE EXCEPTION 'data not restored (expected 2 rows)';
    END IF;

    -- ── Verify security_barrier preserved ─────────────────────
    SELECT (reloptions::text[] @> ARRAY['security_barrier=true'])
      INTO v_barrier
    FROM pg_class
    WHERE oid = 'public.it_vopts_secure'::regclass;

    IF v_barrier IS NOT TRUE THEN
        RAISE EXCEPTION 'security_barrier option lost after restore';
    END IF;

    -- ── Verify check_option preserved ────────────────────────
    SELECT option_name || '=' || option_value INTO v_check_opt
    FROM pg_options_to_table(
        (SELECT reloptions FROM pg_class WHERE oid = 'public.it_vopts_check'::regclass)
    )
    WHERE option_name = 'check_option';

    IF v_check_opt IS NULL THEN
        RAISE EXCEPTION 'check_option not preserved after restore';
    END IF;

    -- ── Cleanup ───────────────────────────────────────────────
    DROP VIEW IF EXISTS public.it_vopts_check;
    DROP VIEW IF EXISTS public.it_vopts_secure;
    DROP TABLE IF EXISTS public.it_vopts_base CASCADE;
END;
$tv$;
