-- Simulate legacy trigger-install leftovers and exercise WAL-only staging cleanup.
DO $tv$
DECLARE
    v_raised boolean;
    v_msg text;
    v_user_trg bigint;
BEGIN
    DROP TABLE IF EXISTS public.it_upgrade_mig CASCADE;
    DROP FUNCTION IF EXISTS public.flashback_capture_insert_trigger() CASCADE;
    DROP FUNCTION IF EXISTS public.it_upgrade_user_trg_fn() CASCADE;
    DROP TABLE IF EXISTS flashback.staging_events CASCADE;

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

    -- Stub legacy capture function + named capture trigger + empty staging table.
    CREATE FUNCTION public.flashback_capture_insert_trigger()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $fn$
    BEGIN
        RETURN NEW;
    END;
    $fn$;

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

    -- Empty case: cleanup drops staging + capture trigger; user trigger remains.
    PERFORM flashback_test_wal_only_staging_cleanup();

    IF to_regclass('flashback.staging_events') IS NOT NULL THEN
        RAISE EXCEPTION 'empty staging_events was not dropped by cleanup';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = 'public.it_upgrade_mig'::regclass
          AND tgname = 'flashback_capture_ins'
    ) THEN
        RAISE EXCEPTION 'flashback_capture_ins trigger survived empty cleanup';
    END IF;
    SELECT count(*) INTO v_user_trg
    FROM pg_trigger
    WHERE tgrelid = 'public.it_upgrade_mig'::regclass
      AND tgname = 'it_upgrade_user_trg'
      AND NOT tgisinternal;
    IF v_user_trg <> 1 THEN
        RAISE EXCEPTION 'ordinary user trigger was removed by cleanup';
    END IF;

    -- Nonempty case: insert a row, assert cleanup raises and row remains.
    CREATE FUNCTION public.flashback_capture_insert_trigger()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $fn$
    BEGIN
        RETURN NEW;
    END;
    $fn$;
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
    CREATE TRIGGER flashback_capture_ins
        AFTER INSERT ON public.it_upgrade_mig
        FOR EACH ROW EXECUTE FUNCTION public.flashback_capture_insert_trigger();
    INSERT INTO flashback.staging_events (rel_oid, event_type, table_name)
    VALUES ('public.it_upgrade_mig'::regclass, 'INSERT', 'public.it_upgrade_mig');

    v_raised := false;
    BEGIN
        PERFORM flashback_test_wal_only_staging_cleanup();
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised OR v_msg NOT LIKE '%still contains%row%' THEN
        RAISE EXCEPTION 'nonempty staging cleanup must raise, got: %', v_msg;
    END IF;
    IF (SELECT count(*) FROM flashback.staging_events) <> 1 THEN
        RAISE EXCEPTION 'nonempty staging row must remain after refused cleanup';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = 'public.it_upgrade_mig'::regclass
          AND tgname = 'it_upgrade_user_trg'
    ) THEN
        RAISE EXCEPTION 'ordinary user trigger must remain after refused cleanup';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = 'public.it_upgrade_mig'::regclass
          AND tgname = 'flashback_capture_ins'
    ) THEN
        RAISE EXCEPTION 'capture trigger must remain when cleanup refuses nonempty staging';
    END IF;

    -- Leave catalog clean for subsequent tests in this session.
    DROP TRIGGER IF EXISTS flashback_capture_ins ON public.it_upgrade_mig;
    DROP TABLE IF EXISTS flashback.staging_events CASCADE;
    DROP FUNCTION IF EXISTS public.flashback_capture_insert_trigger() CASCADE;
    DROP TABLE IF EXISTS public.it_upgrade_mig CASCADE;
    DROP FUNCTION IF EXISTS public.it_upgrade_user_trg_fn() CASCADE;
END;
$tv$;
