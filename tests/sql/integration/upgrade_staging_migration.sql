-- Ownership-proven WAL-only finalize regressions (production routine only).
-- Membership assertions use pg_depend directly: the internal helper is
-- REVOKE'd from PUBLIC and must not be required by regression callers.
DO $tv$
DECLARE
    v_raised boolean;
    v_msg text;
    v_fn_oid oid;
    v_def_before text;
    v_def_after text;
    v_tg_before oid;
    v_row_count bigint;
    v_ext_member boolean;
    v_staging oid;
BEGIN
    -- ------------------------------------------------------------------
    -- Shared fixtures cleanup
    -- ------------------------------------------------------------------
    DROP TABLE IF EXISTS public.it_upgrade_mig CASCADE;
    DROP TABLE IF EXISTS public.it_upgrade_collision CASCADE;
    DROP FUNCTION IF EXISTS public.flashback_capture_insert_trigger() CASCADE;
    DROP FUNCTION IF EXISTS public.flashback_capture_insert_row_trigger() CASCADE;
    DROP FUNCTION IF EXISTS public.flashback_flush_staging(integer) CASCADE;
    DROP FUNCTION IF EXISTS public.it_upgrade_user_trg_fn() CASCADE;
    DROP FUNCTION IF EXISTS public.it_upgrade_user_ins_fn() CASCADE;

    v_staging := to_regclass('flashback.staging_events');
    IF v_staging IS NOT NULL THEN
        SELECT EXISTS (
            SELECT 1
            FROM pg_depend d
            JOIN pg_extension e
              ON e.oid = d.refobjid
             AND d.refclassid = 'pg_extension'::regclass
            WHERE d.classid = 'pg_class'::regclass
              AND d.objid = v_staging
              AND d.deptype = 'e'
              AND e.extname = 'pg_flashback'
        ) INTO v_ext_member;
        IF NOT COALESCE(v_ext_member, false) THEN
            DROP TABLE flashback.staging_events;
        ELSE
            BEGIN
                PERFORM flashback_internal_finalize_wal_only_upgrade();
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'fixture setup: cannot clear leftover staging_events: %', SQLERRM;
            END;
        END IF;
    END IF;

    CREATE FUNCTION public.it_upgrade_user_trg_fn()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $fn$
    BEGIN
        RETURN NEW;
    END;
    $fn$;

    CREATE TABLE public.it_upgrade_mig (id int PRIMARY KEY, val text);
    CREATE TRIGGER it_upgrade_user_trg
        BEFORE INSERT ON public.it_upgrade_mig
        FOR EACH ROW EXECUTE FUNCTION public.it_upgrade_user_trg_fn();

    -- ==================================================================
    -- A. Real extension-owned legacy objects clean up; second call no-op;
    --    ordinary trigger preserved.
    -- ==================================================================
    CREATE FUNCTION public.flashback_capture_insert_trigger()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $fn$
    BEGIN
        RETURN NEW;
    END;
    $fn$;
    ALTER EXTENSION pg_flashback ADD FUNCTION public.flashback_capture_insert_trigger();

    CREATE TRIGGER flashback_capture_ins
        AFTER INSERT ON public.it_upgrade_mig
        FOR EACH ROW EXECUTE FUNCTION public.flashback_capture_insert_trigger();

    CREATE TABLE flashback.staging_events (
        event_id bigserial PRIMARY KEY,
        rel_oid oid,
        tracking_id bigint,
        generation_id bigint,
        stream_id bigint,
        event_type text,
        table_name text,
        old_data jsonb,
        new_data jsonb,
        captured_at timestamptz DEFAULT clock_timestamp()
    );
    ALTER EXTENSION pg_flashback ADD TABLE flashback.staging_events;

    PERFORM flashback_internal_finalize_wal_only_upgrade();

    IF to_regclass('flashback.staging_events') IS NOT NULL THEN
        RAISE EXCEPTION 'A: empty extension-owned staging_events was not dropped';
    END IF;
    IF to_regprocedure('public.flashback_capture_insert_trigger()') IS NOT NULL THEN
        RAISE EXCEPTION 'A: extension-owned capture function survived finalize';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = 'public.it_upgrade_mig'::regclass
          AND tgname = 'flashback_capture_ins'
          AND NOT tgisinternal
    ) THEN
        RAISE EXCEPTION 'A: flashback_capture_ins survived finalize';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = 'public.it_upgrade_mig'::regclass
          AND tgname = 'it_upgrade_user_trg'
          AND NOT tgisinternal
    ) THEN
        RAISE EXCEPTION 'A: ordinary user trigger was removed';
    END IF;

    -- Second call must be a no-op.
    PERFORM flashback_internal_finalize_wal_only_upgrade();

    -- ==================================================================
    -- B. Prefix collision: flashback_capture_custom + user-owned function
    -- ==================================================================
    CREATE FUNCTION public.it_upgrade_user_ins_fn()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $fn$
    BEGIN
        RETURN NEW;
    END;
    $fn$;
    CREATE TRIGGER flashback_capture_custom
        AFTER INSERT ON public.it_upgrade_mig
        FOR EACH ROW EXECUTE FUNCTION public.it_upgrade_user_ins_fn();

    PERFORM flashback_internal_finalize_wal_only_upgrade();

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = 'public.it_upgrade_mig'::regclass
          AND tgname = 'flashback_capture_custom'
          AND NOT tgisinternal
    ) THEN
        RAISE EXCEPTION 'B: flashback_capture_custom user trigger was removed';
    END IF;
    IF to_regprocedure('public.it_upgrade_user_ins_fn()') IS NULL THEN
        RAISE EXCEPTION 'B: user trigger function was removed';
    END IF;

    -- ==================================================================
    -- C. Exact trigger-name collision (user-owned function) → fail-closed
    --    + rollback of a prior successful legacy drop in the same call.
    -- ==================================================================
    CREATE FUNCTION public.flashback_capture_delete_row_trigger()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $fn$
    BEGIN
        RETURN OLD;
    END;
    $fn$;
    ALTER EXTENSION pg_flashback ADD FUNCTION public.flashback_capture_delete_row_trigger();
    -- Alphabetically before flashback_capture_ins, so finalize drops this first.
    CREATE TRIGGER flashback_capture_del
        AFTER DELETE ON public.it_upgrade_mig
        FOR EACH ROW EXECUTE FUNCTION public.flashback_capture_delete_row_trigger();

    -- Exact legacy NAME bound to a user-owned function.
    CREATE TRIGGER flashback_capture_ins
        AFTER INSERT ON public.it_upgrade_mig
        FOR EACH ROW EXECUTE FUNCTION public.it_upgrade_user_ins_fn();

    v_fn_oid := 'public.flashback_capture_delete_row_trigger()'::regprocedure;
    v_raised := false;
    BEGIN
        PERFORM flashback_internal_finalize_wal_only_upgrade();
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT ILIKE '%non-extension function%' THEN
        RAISE EXCEPTION 'C: exact-name collision must fail-closed, got: %', v_msg;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = 'public.it_upgrade_mig'::regclass
          AND tgname = 'flashback_capture_ins'
          AND NOT tgisinternal
    ) THEN
        RAISE EXCEPTION 'C: colliding trigger must remain after refused finalize';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = 'public.it_upgrade_mig'::regclass
          AND tgname = 'flashback_capture_del'
          AND NOT tgisinternal
    ) THEN
        RAISE EXCEPTION 'C: earlier legacy trigger drop must roll back';
    END IF;
    IF to_regprocedure('public.flashback_capture_delete_row_trigger()') IS NULL THEN
        RAISE EXCEPTION 'C: earlier legacy function must roll back';
    END IF;
    SELECT EXISTS (
        SELECT 1
        FROM pg_depend d
        JOIN pg_extension e
          ON e.oid = d.refobjid
         AND d.refclassid = 'pg_extension'::regclass
        WHERE d.classid = 'pg_proc'::regclass
          AND d.objid = v_fn_oid
          AND d.deptype = 'e'
          AND e.extname = 'pg_flashback'
    ) INTO v_ext_member;
    IF NOT COALESCE(v_ext_member, false) THEN
        RAISE EXCEPTION 'C: earlier function lost extension membership after refuse';
    END IF;

    DROP TRIGGER flashback_capture_ins ON public.it_upgrade_mig;
    DROP TRIGGER flashback_capture_del ON public.it_upgrade_mig;
    ALTER EXTENSION pg_flashback DROP FUNCTION public.flashback_capture_delete_row_trigger();
    DROP FUNCTION public.flashback_capture_delete_row_trigger();

    -- ==================================================================
    -- D. Function-name collision (user-owned, not extension member)
    -- ==================================================================
    CREATE FUNCTION public.flashback_capture_insert_trigger()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $fn$
    BEGIN
        RETURN NEW;
    END;
    $fn$;
    v_def_before := pg_get_functiondef(
        'public.flashback_capture_insert_trigger()'::regprocedure
    );

    v_raised := false;
    BEGIN
        PERFORM flashback_internal_finalize_wal_only_upgrade();
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT ILIKE '%not a pg_flashback extension member%' THEN
        RAISE EXCEPTION 'D: function collision must fail-closed, got: %', v_msg;
    END IF;
    v_def_after := pg_get_functiondef(
        'public.flashback_capture_insert_trigger()'::regprocedure
    );
    IF v_def_before IS DISTINCT FROM v_def_after THEN
        RAISE EXCEPTION 'D: user function definition changed during refused finalize';
    END IF;

    DROP FUNCTION public.flashback_capture_insert_trigger();

    -- ==================================================================
    -- E. Fake staging table collision (not extension member)
    -- ==================================================================
    CREATE TABLE flashback.staging_events (
        event_id bigserial PRIMARY KEY,
        note text
    );
    INSERT INTO flashback.staging_events (note) VALUES ('user-owned');
    v_row_count := (SELECT count(*) FROM flashback.staging_events);

    v_raised := false;
    BEGIN
        PERFORM flashback_internal_finalize_wal_only_upgrade();
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT ILIKE '%not a pg_flashback extension member%' THEN
        RAISE EXCEPTION 'E: fake staging collision must fail-closed, got: %', v_msg;
    END IF;
    IF (SELECT count(*) FROM flashback.staging_events) IS DISTINCT FROM v_row_count THEN
        RAISE EXCEPTION 'E: fake staging rows must remain';
    END IF;

    DROP TABLE flashback.staging_events;

    -- ==================================================================
    -- F. Nonempty real legacy staging → fail-closed; rows preserved
    -- ==================================================================
    CREATE FUNCTION public.flashback_capture_insert_trigger()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $fn$
    BEGIN
        RETURN NEW;
    END;
    $fn$;
    ALTER EXTENSION pg_flashback ADD FUNCTION public.flashback_capture_insert_trigger();
    CREATE TRIGGER flashback_capture_ins
        AFTER INSERT ON public.it_upgrade_mig
        FOR EACH ROW EXECUTE FUNCTION public.flashback_capture_insert_trigger();

    CREATE TABLE flashback.staging_events (
        event_id bigserial PRIMARY KEY,
        rel_oid oid,
        event_type text,
        table_name text
    );
    ALTER EXTENSION pg_flashback ADD TABLE flashback.staging_events;
    INSERT INTO flashback.staging_events (rel_oid, event_type, table_name)
    VALUES ('public.it_upgrade_mig'::regclass, 'INSERT', 'public.it_upgrade_mig');

    v_raised := false;
    BEGIN
        PERFORM flashback_internal_finalize_wal_only_upgrade();
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT ILIKE '%still contains%row%' THEN
        RAISE EXCEPTION 'F: nonempty staging must fail-closed, got: %', v_msg;
    END IF;
    IF (SELECT count(*) FROM flashback.staging_events) <> 1 THEN
        RAISE EXCEPTION 'F: nonempty staging row must remain';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = 'public.it_upgrade_mig'::regclass
          AND tgname = 'flashback_capture_ins'
    ) THEN
        RAISE EXCEPTION 'F: legacy trigger must remain when nonempty staging refused';
    END IF;
    IF to_regprocedure('public.flashback_capture_insert_trigger()') IS NULL THEN
        RAISE EXCEPTION 'F: legacy function must remain when nonempty staging refused';
    END IF;

    -- Manual cleanup of F fixtures (extension-owned) for next cases.
    DROP TRIGGER flashback_capture_ins ON public.it_upgrade_mig;
    ALTER EXTENSION pg_flashback DROP TABLE flashback.staging_events;
    DROP TABLE flashback.staging_events;
    ALTER EXTENSION pg_flashback DROP FUNCTION public.flashback_capture_insert_trigger();
    DROP FUNCTION public.flashback_capture_insert_trigger();

    -- ==================================================================
    -- G. Unexpected dependency on extension-owned legacy function
    -- ==================================================================
    CREATE FUNCTION public.flashback_capture_insert_trigger()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $fn$
    BEGIN
        RETURN NEW;
    END;
    $fn$;
    ALTER EXTENSION pg_flashback ADD FUNCTION public.flashback_capture_insert_trigger();

    -- Non-allowlisted trigger name still depends on the capture function.
    CREATE TRIGGER it_upgrade_unexpected_dep
        AFTER INSERT ON public.it_upgrade_mig
        FOR EACH ROW EXECUTE FUNCTION public.flashback_capture_insert_trigger();

    v_tg_before := (
        SELECT t.oid FROM pg_trigger t
        WHERE t.tgrelid = 'public.it_upgrade_mig'::regclass
          AND t.tgname = 'it_upgrade_unexpected_dep'
    );
    v_fn_oid := 'public.flashback_capture_insert_trigger()'::regprocedure;

    v_raised := false;
    BEGIN
        PERFORM flashback_internal_finalize_wal_only_upgrade();
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised
       OR (v_msg NOT ILIKE '%dependent%' AND v_msg NOT ILIKE '%unexpected%')
    THEN
        RAISE EXCEPTION 'G: unexpected dependency must fail-closed, got: %', v_msg;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE oid = v_tg_before
    ) THEN
        RAISE EXCEPTION 'G: unexpected dependent trigger must remain';
    END IF;
    IF to_regprocedure('public.flashback_capture_insert_trigger()') IS NULL THEN
        RAISE EXCEPTION 'G: legacy function must remain after refused DROP';
    END IF;
    SELECT EXISTS (
        SELECT 1
        FROM pg_depend d
        JOIN pg_extension e
          ON e.oid = d.refobjid
         AND d.refclassid = 'pg_extension'::regclass
        WHERE d.classid = 'pg_proc'::regclass
          AND d.objid = v_fn_oid
          AND d.deptype = 'e'
          AND e.extname = 'pg_flashback'
    ) INTO v_ext_member;
    IF NOT COALESCE(v_ext_member, false) THEN
        RAISE EXCEPTION 'G: function must remain extension-owned after refused finalize';
    END IF;

    DROP TRIGGER it_upgrade_unexpected_dep ON public.it_upgrade_mig;
    ALTER EXTENSION pg_flashback DROP FUNCTION public.flashback_capture_insert_trigger();
    DROP FUNCTION public.flashback_capture_insert_trigger();

    -- ==================================================================
    -- RBAC: internal finalize must not be PUBLIC/admin/monitor executable
    -- ==================================================================
    IF has_function_privilege(
        'public',
        'flashback_internal_finalize_wal_only_upgrade()',
        'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'PUBLIC must not EXECUTE finalize routine';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flashback_admin')
       AND has_function_privilege(
            'flashback_admin',
            'flashback_internal_finalize_wal_only_upgrade()',
            'EXECUTE'
       )
    THEN
        RAISE EXCEPTION 'flashback_admin must not EXECUTE finalize routine';
    END IF;
    IF has_function_privilege(
        'pg_monitor',
        'flashback_internal_finalize_wal_only_upgrade()',
        'EXECUTE'
    ) THEN
        RAISE EXCEPTION 'pg_monitor must not EXECUTE finalize routine';
    END IF;

    -- Test wrapper still delegates to production (no forked algorithm).
    PERFORM flashback_test_wal_only_staging_cleanup();

    -- Leave catalog clean.
    DROP TRIGGER IF EXISTS flashback_capture_custom ON public.it_upgrade_mig;
    DROP TABLE IF EXISTS public.it_upgrade_mig CASCADE;
    DROP FUNCTION IF EXISTS public.it_upgrade_user_trg_fn() CASCADE;
    DROP FUNCTION IF EXISTS public.it_upgrade_user_ins_fn() CASCADE;
END;
$tv$;
