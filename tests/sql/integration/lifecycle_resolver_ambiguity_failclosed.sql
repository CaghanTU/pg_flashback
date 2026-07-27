-- A1: every public/operator-facing entry point that resolves a caller-
-- supplied table name against flashback.tracked_tables must go through the
-- single canonical resolver (flashback_internal_resolve_tracked_table) and
-- fail closed -- never silently pick one row -- when an unqualified name
-- matches more than one actively-tracked lifecycle across schemas.
CREATE SCHEMA IF NOT EXISTS it_amb_s1;
CREATE SCHEMA IF NOT EXISTS it_amb_s2;
DROP TABLE IF EXISTS it_amb_s1.dup_name CASCADE;
DROP TABLE IF EXISTS it_amb_s2.dup_name CASCADE;
CREATE TABLE it_amb_s1.dup_name (id int PRIMARY KEY);
CREATE TABLE it_amb_s2.dup_name (id int PRIMARY KEY);

DO $test$
DECLARE
    v_tid_s1 bigint;
    v_tid_s2 bigint;
    v_plan jsonb;
    v_failed boolean;
BEGIN
    v_tid_s1 := (flashback_test_bootstrap_lifecycle('it_amb_s1.dup_name')->>'tracking_id')::bigint;
    v_tid_s2 := (flashback_test_bootstrap_lifecycle('it_amb_s2.dup_name')->>'tracking_id')::bigint;

    -- flashback_recover_plan: unqualified name is ambiguous -> structured
    -- JSON error, never a silently-picked row (plan keeps its "always JSON,
    -- never raises for a bad argument" contract).
    v_plan := flashback_recover_plan('dup_name');
    IF COALESCE(v_plan->>'status', '') <> 'error'
       OR COALESCE(v_plan->>'code', '') <> 'ambiguous_table' THEN
        RAISE EXCEPTION 'recover orders (unqualified, ambiguous) must reject; got %', v_plan;
    END IF;

    -- Schema-qualified selects exactly one lifecycle.
    v_plan := flashback_recover_plan('it_amb_s1.dup_name');
    IF COALESCE(v_plan->>'code', '') = 'ambiguous_table' THEN
        RAISE EXCEPTION 'recover s1.dup_name must not be ambiguous; got %', v_plan;
    END IF;
    IF (v_plan->>'tracking_id')::bigint IS DISTINCT FROM v_tid_s1 THEN
        RAISE EXCEPTION 'recover s1.dup_name resolved wrong tracking_id: got % want %',
            v_plan->>'tracking_id', v_tid_s1;
    END IF;

    v_plan := flashback_recover_plan('it_amb_s2.dup_name');
    IF (v_plan->>'tracking_id')::bigint IS DISTINCT FROM v_tid_s2 THEN
        RAISE EXCEPTION 'recover s2.dup_name resolved wrong tracking_id: got % want %',
            v_plan->>'tracking_id', v_tid_s2;
    END IF;

    -- flashback_recover_begin: unqualified ambiguous name must raise, not
    -- silently begin against one of the two lifecycles.
    v_failed := false;
    BEGIN
        PERFORM flashback_recover_begin('dup_name', 'irrelevant-token');
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'recover_begin(dup_name) must reject ambiguous unqualified name';
    END IF;

    -- flashback_is_actively_protected: ambiguous unqualified name must raise
    -- rather than returning a possibly-wrong true/false.
    v_failed := false;
    BEGIN
        PERFORM flashback_is_actively_protected('dup_name');
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'flashback_is_actively_protected(dup_name) must reject ambiguous name';
    END IF;
    IF NOT flashback_is_actively_protected('it_amb_s1.dup_name') THEN
        RAISE EXCEPTION 'flashback_is_actively_protected(s1.dup_name) should be true';
    END IF;

    -- flashback_resolve_lifecycle_name: ambiguous unqualified name must
    -- raise rather than silently canonicalizing to one schema's table.
    v_failed := false;
    BEGIN
        PERFORM flashback_resolve_lifecycle_name('dup_name');
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'flashback_resolve_lifecycle_name(dup_name) must reject ambiguous name';
    END IF;

    -- flashback_disaster_points: ambiguous unqualified name must raise.
    v_failed := false;
    BEGIN
        PERFORM 1 FROM flashback_disaster_points('dup_name', interval '1 hour');
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'flashback_disaster_points(dup_name) must reject ambiguous name';
    END IF;

    -- flashback_unprotect: ambiguous unqualified name must raise, and must
    -- never unprotect the wrong (or an arbitrary) lifecycle as a side effect.
    v_failed := false;
    BEGIN
        PERFORM flashback_unprotect('dup_name');
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'flashback_unprotect(dup_name) must reject ambiguous name';
    END IF;
    IF EXISTS (
        SELECT 1 FROM flashback.tracked_tables
        WHERE tracking_id IN (v_tid_s1, v_tid_s2)
          AND COALESCE(protection_state, 'active') <> 'active'
    ) THEN
        RAISE EXCEPTION 'ambiguous unprotect must not have touched either lifecycle';
    END IF;

    -- Qualified unprotect succeeds and targets exactly the named schema.
    PERFORM flashback_unprotect('it_amb_s1.dup_name');
    IF (SELECT protection_state FROM flashback.tracked_tables WHERE tracking_id = v_tid_s1)
       <> 'stopping' THEN
        RAISE EXCEPTION 'qualified unprotect(s1.dup_name) did not transition s1 lifecycle';
    END IF;
    IF (SELECT protection_state FROM flashback.tracked_tables WHERE tracking_id = v_tid_s2)
       IS DISTINCT FROM 'active' THEN
        RAISE EXCEPTION 'qualified unprotect(s1.dup_name) must not have touched s2 lifecycle';
    END IF;
END;
$test$;

-- Unprotect -> reprotect: recover must resolve to the new lifecycle, never
-- the retired one, once the old row's is_active flips to false.
DROP TABLE IF EXISTS public.it_amb_reanchor CASCADE;
CREATE TABLE public.it_amb_reanchor (id int PRIMARY KEY);

DO $test2$
DECLARE
    v_tid_old bigint;
    v_tid_new bigint;
    v_plan jsonb;
BEGIN
    v_tid_old := (flashback_test_bootstrap_lifecycle('public.it_amb_reanchor')->>'tracking_id')::bigint;

    -- Force the old lifecycle straight to unprotected/inactive without the
    -- async stop-marker consumption path (test-only direct transition; the
    -- resolver contract under test only cares that is_active = false here).
    UPDATE flashback.tracked_tables
       SET is_active = false, protection_state = 'unprotected', unprotected_at = clock_timestamp()
     WHERE tracking_id = v_tid_old;

    v_tid_new := (flashback_test_bootstrap_lifecycle('public.it_amb_reanchor')->>'tracking_id')::bigint;
    IF v_tid_new IS NULL OR v_tid_new = v_tid_old THEN
        RAISE EXCEPTION 'test setup: reprotect must create a new active lifecycle';
    END IF;

    v_plan := flashback_recover_plan('public.it_amb_reanchor');
    IF (v_plan->>'tracking_id')::bigint IS DISTINCT FROM v_tid_new THEN
        RAISE EXCEPTION 'recover after unprotect+reprotect resolved stale lifecycle % (want %)',
            v_plan->>'tracking_id', v_tid_new;
    END IF;
    IF NOT flashback_is_actively_protected('public.it_amb_reanchor') THEN
        RAISE EXCEPTION 'reprotected lifecycle should be actively protected';
    END IF;
END;
$test2$;

-- A1 follow-up: flashback_resolve_lifecycle_name and flashback_disaster_points
-- use the resolver's historical-fallback mode (flashback_internal_resolve_tracked_table_any),
-- not the active-only mode -- they must still be able to name/report on a
-- lifecycle that was unprotected and never retracked, while staying fail-closed
-- on ambiguity within each tier and preferring an active match over a
-- same-named historical one.
DO $test3$
DECLARE
    v_tid_hist bigint;
    v_tid_hist2 bigint;
    v_resolved text;
    v_failed boolean;
BEGIN
    DROP TABLE IF EXISTS it_amb_s1.hist_only CASCADE;
    CREATE TABLE it_amb_s1.hist_only (id int PRIMARY KEY);
    v_tid_hist := (flashback_test_bootstrap_lifecycle('it_amb_s1.hist_only')->>'tracking_id')::bigint;
    UPDATE flashback.tracked_tables
       SET is_active = false, protection_state = 'unprotected', unprotected_at = clock_timestamp()
     WHERE tracking_id = v_tid_hist;

    -- Only a historical (inactive) lifecycle exists for this name: display
    -- resolution must still find it, not fall back to the raw input.
    v_resolved := flashback_resolve_lifecycle_name('hist_only');
    IF v_resolved <> 'it_amb_s1.hist_only' THEN
        RAISE EXCEPTION 'flashback_resolve_lifecycle_name lost historical resolution: got %', v_resolved;
    END IF;
    -- Must not raise "no tracking lifecycle" for a historical-only name.
    PERFORM 1 FROM flashback_disaster_points('hist_only', interval '1 hour');

    -- A second, unrelated historical lifecycle with the SAME unqualified
    -- name in a different schema: now two inactive candidates, zero active
    -- -- must fail closed, never pick the most recently unprotected one.
    DROP TABLE IF EXISTS it_amb_s2.hist_only CASCADE;
    CREATE TABLE it_amb_s2.hist_only (id int PRIMARY KEY);
    v_tid_hist2 := (flashback_test_bootstrap_lifecycle('it_amb_s2.hist_only')->>'tracking_id')::bigint;
    UPDATE flashback.tracked_tables
       SET is_active = false, protection_state = 'unprotected', unprotected_at = clock_timestamp()
     WHERE tracking_id = v_tid_hist2;

    v_failed := false;
    BEGIN
        PERFORM flashback_resolve_lifecycle_name('hist_only');
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'flashback_resolve_lifecycle_name did not fail closed on ambiguous historical name';
    END IF;

    v_failed := false;
    BEGIN
        PERFORM 1 FROM flashback_disaster_points('hist_only', interval '1 hour');
    EXCEPTION WHEN OTHERS THEN
        v_failed := true;
    END;
    IF NOT v_failed THEN
        RAISE EXCEPTION 'flashback_disaster_points did not fail closed on ambiguous historical name';
    END IF;

    -- Retrack one of them: an active match must now be preferred over the
    -- still-ambiguous historical pair, not itself treated as a third
    -- candidate in one flat ambiguity check.
    DROP TABLE it_amb_s1.hist_only;
    CREATE TABLE it_amb_s1.hist_only (id int PRIMARY KEY);
    PERFORM flashback_test_bootstrap_lifecycle('it_amb_s1.hist_only');

    v_resolved := flashback_resolve_lifecycle_name('hist_only');
    IF v_resolved <> 'it_amb_s1.hist_only' THEN
        RAISE EXCEPTION 'active lifecycle should be preferred over ambiguous historical pair: got %', v_resolved;
    END IF;
END;
$test3$;
