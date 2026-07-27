-- Production catalog must not expose legacy trigger-capture CREATE surface.
DO $tv$
DECLARE
    v_cnt bigint;
    v_schema jsonb;
BEGIN
    IF to_regclass('flashback.staging_events') IS NOT NULL THEN
        RAISE EXCEPTION 'staging_events must not exist';
    END IF;

    SELECT count(*) INTO v_cnt
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'public'
      AND p.proname IN (
          'flashback_attach_capture_trigger',
          'flashback_detach_capture_trigger',
          'flashback_flush_staging',
          'flashback_capture_insert_trigger',
          'flashback_capture_insert_row_trigger',
          'flashback_capture_update_trigger',
          'flashback_capture_delete_trigger',
          'flashback_capture_delete_row_trigger'
      );
    IF v_cnt <> 0 THEN
        RAISE EXCEPTION 'legacy capture CREATE surface still present (% functions)', v_cnt;
    END IF;

    -- DDL staging core / pending_wal_events remain part of the WAL product.
    IF to_regprocedure(
           'flashback_stage_local_delta_ddl_event(bigint,text,bigint,pg_lsn,jsonb,boolean)'
       ) IS NULL
    THEN
        RAISE EXCEPTION 'DDL staging core must still exist';
    END IF;
    IF to_regclass('flashback.pending_wal_events') IS NULL THEN
        RAISE EXCEPTION 'pending_wal_events must still exist';
    END IF;

    -- collect_schema_def still works and mentions ordinary triggers.
    DROP TABLE IF EXISTS public.it_surface_ord CASCADE;
    DROP FUNCTION IF EXISTS public.it_surface_ord_fn() CASCADE;
    CREATE FUNCTION public.it_surface_ord_fn()
    RETURNS trigger LANGUAGE plpgsql AS $fn$ BEGIN RETURN NEW; END; $fn$;
    CREATE TABLE public.it_surface_ord (id int PRIMARY KEY);
    CREATE TRIGGER it_surface_ord_trg
        BEFORE INSERT ON public.it_surface_ord
        FOR EACH ROW EXECUTE FUNCTION public.it_surface_ord_fn();

    SELECT flashback_collect_schema_def('public.it_surface_ord'::regclass) INTO v_schema;
    IF v_schema->'triggers' IS NULL
       OR jsonb_array_length(v_schema->'triggers') < 1
    THEN
        RAISE EXCEPTION 'collect_schema_def must include ordinary triggers';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements(v_schema->'triggers') t
        WHERE t->>'name' LIKE 'flashback_capture_%'
    ) THEN
        RAISE EXCEPTION 'collect_schema_def must not invent flashback_capture_* triggers';
    END IF;

    DROP TABLE IF EXISTS public.it_surface_ord CASCADE;
    DROP FUNCTION IF EXISTS public.it_surface_ord_fn() CASCADE;
END;
$tv$;
