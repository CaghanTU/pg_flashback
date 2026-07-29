-- Fresh WAL-only install surface: no staging_events / capture trigger APIs;
-- ordinary user triggers survive seam restore; no flashback_capture_* triggers.
DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/2000'::pg_lsn;
    v_xid bigint;
    v_mode text;
    v_trg_cnt bigint;
    v_def text;
BEGIN
    IF to_regclass('flashback.staging_events') IS NOT NULL THEN
        RAISE EXCEPTION 'fresh install must not create flashback.staging_events';
    END IF;

    IF to_regprocedure('flashback_flush_staging(integer)') IS NOT NULL
       OR to_regprocedure('flashback_attach_capture_trigger(text,text)') IS NOT NULL
       OR to_regprocedure('flashback_detach_capture_trigger(text,text)') IS NOT NULL
       OR to_regprocedure('flashback_capture_insert_trigger()') IS NOT NULL
       OR to_regprocedure('flashback_capture_update_trigger()') IS NOT NULL
       OR to_regprocedure('flashback_capture_delete_trigger()') IS NOT NULL
    THEN
        RAISE EXCEPTION 'legacy trigger capture functions must not exist after install';
    END IF;

    SELECT flashback_effective_capture_mode() INTO v_mode;
    IF v_mode <> 'wal' THEN
        RAISE EXCEPTION 'effective capture mode must be wal, got %', v_mode;
    END IF;

    DROP TABLE IF EXISTS public.it_wal_fresh CASCADE;
    DROP FUNCTION IF EXISTS public.it_wal_fresh_trg_fn() CASCADE;

    CREATE FUNCTION public.it_wal_fresh_trg_fn()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $fn$
    BEGIN
        NEW.val := COALESCE(NEW.val, '') || '|touched';
        RETURN NEW;
    END;
    $fn$;

    CREATE TABLE public.it_wal_fresh (
        id int PRIMARY KEY,
        val text
    );
    CREATE TRIGGER it_wal_fresh_ordinary
        BEFORE INSERT ON public.it_wal_fresh
        FOR EACH ROW EXECUTE FUNCTION public.it_wal_fresh_trg_fn();

    SELECT flashback_test_bootstrap_lifecycle('public.it_wal_fresh') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    INSERT INTO public.it_wal_fresh VALUES (1, 'a');
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        v_point_lsn,
        clock_timestamp(),
        960001,
        jsonb_build_array(
            jsonb_build_object('op', 'INSERT', 'new', '{"id":1,"val":"a"}'::jsonb)
        )
    );

    v_xid := (txid_current() % 4294967296)::bigint;
    -- The DDL hook already captures this literal DROP for real (manifest +
    -- pending event under the current transaction's real xid); only finalize
    -- that pending event here, never restage a second, competing one.
    DROP TABLE public.it_wal_fresh CASCADE;
    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        v_xid,
        '[]'::jsonb
    );
    PERFORM flashback_bind_drop_dependency_manifests();

    PERFORM flashback_test_restore_lsn('public.it_wal_fresh', v_point_lsn);

    IF NOT EXISTS (SELECT 1 FROM public.it_wal_fresh WHERE id = 1) THEN
        RAISE EXCEPTION 'restored table missing expected row';
    END IF;

    SELECT count(*) INTO v_trg_cnt
    FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relname = 'it_wal_fresh'
      AND NOT t.tgisinternal
      AND t.tgname = 'it_wal_fresh_ordinary';
    IF v_trg_cnt <> 1 THEN
        RAISE EXCEPTION 'ordinary user trigger was not restored, count=%', v_trg_cnt;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relname = 'it_wal_fresh'
          AND NOT t.tgisinternal
          AND t.tgname LIKE 'flashback_capture_%'
    ) THEN
        RAISE EXCEPTION 'flashback_capture_* trigger present on restored table';
    END IF;

    SELECT pg_get_triggerdef(t.oid) INTO v_def
    FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relname = 'it_wal_fresh'
      AND t.tgname = 'it_wal_fresh_ordinary';
    IF v_def IS NULL OR position('it_wal_fresh_trg_fn' IN v_def) = 0 THEN
        RAISE EXCEPTION 'ordinary trigger definition not restored: %', v_def;
    END IF;

    -- No terminal DROP TABLE: pg_test rolls back this whole transaction, and
    -- the restore just performed leaves the successor generation "building"
    -- (not yet active) until that rollback/commit is observed, so a
    -- same-transaction DROP here would trip the schema-contract guard.
END;
$tv$;
