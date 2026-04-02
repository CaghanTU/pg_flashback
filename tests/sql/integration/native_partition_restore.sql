-- Test: native partitioned table support using standard flashback_track
-- Verifies that flashback_attach_capture_trigger auto-selects per-row triggers
-- for partitioned tables (no custom trigger workaround needed).
DO $tv$
DECLARE
    t_before timestamptz;
    v_cnt    bigint;
    v_ins_fn text;
    v_del_fn text;
    v_parent_oid oid;
BEGIN
    DROP TABLE IF EXISTS public.it_native_part CASCADE;

    CREATE TABLE public.it_native_part (
        id    serial,
        val   text NOT NULL,
        yr    int  NOT NULL,
        PRIMARY KEY (id, yr)
    ) PARTITION BY RANGE (yr);

    CREATE TABLE public.it_native_part_2025
        PARTITION OF public.it_native_part FOR VALUES FROM (2025) TO (2026);
    CREATE TABLE public.it_native_part_2026
        PARTITION OF public.it_native_part FOR VALUES FROM (2026) TO (2027);

    PERFORM flashback_track('public.it_native_part');
    v_parent_oid := 'public.it_native_part'::regclass::oid;

    -- ── Verify per-row triggers were attached to the parent ───────
    SELECT p.proname INTO v_ins_fn
    FROM pg_trigger tg
    JOIN pg_class c ON c.oid = tg.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_proc p ON p.oid = tg.tgfoid
    WHERE n.nspname = 'public' AND c.relname = 'it_native_part'
      AND tg.tgname = 'flashback_capture_ins';

    IF v_ins_fn <> 'flashback_capture_insert_row_trigger' THEN
        RAISE EXCEPTION 'partition trigger test: expected flashback_capture_insert_row_trigger, got: %',
            COALESCE(v_ins_fn, '(null)');
    END IF;

    SELECT p.proname INTO v_del_fn
    FROM pg_trigger tg
    JOIN pg_class c ON c.oid = tg.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_proc p ON p.oid = tg.tgfoid
    WHERE n.nspname = 'public' AND c.relname = 'it_native_part'
      AND tg.tgname = 'flashback_capture_del';

    IF v_del_fn <> 'flashback_capture_delete_row_trigger' THEN
        RAISE EXCEPTION 'partition trigger test: expected flashback_capture_delete_row_trigger, got: %',
            COALESCE(v_del_fn, '(null)');
    END IF;

    -- ── Replace real triggers with test triggers that write directly to delta_log ─
    -- Required because staging_events->delta_log flush needs the worker,
    -- but tests run in a single transaction.  Reuse the partition-aware test helper.
    EXECUTE 'DROP TRIGGER IF EXISTS flashback_capture_ins ON public.it_native_part';
    EXECUTE 'DROP TRIGGER IF EXISTS flashback_capture_upd ON public.it_native_part';
    EXECUTE 'DROP TRIGGER IF EXISTS flashback_capture_del ON public.it_native_part';

    CREATE OR REPLACE FUNCTION _it_native_part_trigger()
    RETURNS trigger LANGUAGE plpgsql AS $fn$
    DECLARE
        v_par oid;
        v_sv  bigint;
    BEGIN
        SELECT inhparent INTO v_par FROM pg_inherits WHERE inhrelid = TG_RELID;
        IF v_par IS NULL THEN v_par := TG_RELID; END IF;
        SELECT schema_version INTO v_sv
        FROM flashback.tracked_tables WHERE rel_oid = v_par AND is_active;
        IF v_sv IS NULL THEN v_sv := 1; END IF;
        IF TG_OP = 'INSERT' THEN
            INSERT INTO flashback.delta_log(event_time,event_type,table_name,rel_oid,schema_version,old_data,new_data,committed_at)
            VALUES (clock_timestamp(),'INSERT','public.it_native_part',v_par,v_sv,NULL,to_jsonb(NEW),clock_timestamp());
            RETURN NEW;
        ELSIF TG_OP = 'UPDATE' THEN
            INSERT INTO flashback.delta_log(event_time,event_type,table_name,rel_oid,schema_version,old_data,new_data,committed_at)
            VALUES (clock_timestamp(),'UPDATE','public.it_native_part',v_par,v_sv,to_jsonb(OLD),to_jsonb(NEW),clock_timestamp());
            RETURN NEW;
        ELSIF TG_OP = 'DELETE' THEN
            INSERT INTO flashback.delta_log(event_time,event_type,table_name,rel_oid,schema_version,old_data,new_data,committed_at)
            VALUES (clock_timestamp(),'DELETE','public.it_native_part',v_par,v_sv,to_jsonb(OLD),NULL,clock_timestamp());
            RETURN OLD;
        END IF;
        RETURN NULL;
    END;
    $fn$;

    CREATE TRIGGER it_native_part_capture
        AFTER INSERT OR UPDATE OR DELETE ON public.it_native_part
        FOR EACH ROW EXECUTE FUNCTION _it_native_part_trigger();

    -- ── DML: insert across both partitions ───────────────────────
    INSERT INTO public.it_native_part (val, yr) VALUES
        ('a', 2025), ('b', 2025), ('c', 2026), ('d', 2026), ('e', 2026);

    t_before := clock_timestamp();
    PERFORM pg_sleep(0.01);

    -- Disaster: delete all 2026 rows and UPDATE 2025 rows
    DELETE FROM public.it_native_part WHERE yr = 2026;
    UPDATE public.it_native_part SET val = 'CHANGED' WHERE yr = 2025;

    SELECT count(*) INTO v_cnt FROM public.it_native_part;
    IF v_cnt <> 2 THEN
        RAISE EXCEPTION 'pre-restore: expected 2 rows, got %', v_cnt;
    END IF;

    PERFORM flashback_restore('public.it_native_part', t_before);

    SELECT count(*) INTO v_cnt FROM public.it_native_part;
    IF v_cnt <> 5 THEN
        RAISE EXCEPTION 'post-restore: expected 5 rows, got % (partition restore failed)', v_cnt;
    END IF;

    -- Verify original values came back
    IF NOT EXISTS (SELECT 1 FROM public.it_native_part WHERE val = 'a' AND yr = 2025) THEN
        RAISE EXCEPTION 'partition restore: row (a, 2025) missing or wrong value';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.it_native_part WHERE val = 'c' AND yr = 2026) THEN
        RAISE EXCEPTION 'partition restore: row (c, 2026) missing or wrong value';
    END IF;

    -- Verify partition routing is intact after restore
    IF NOT EXISTS (SELECT 1 FROM public.it_native_part_2025 WHERE val = 'b') THEN
        RAISE EXCEPTION 'partition routing broken: 2025 partition missing row b';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.it_native_part_2026 WHERE val = 'e') THEN
        RAISE EXCEPTION 'partition routing broken: 2026 partition missing row e';
    END IF;

    DROP TABLE IF EXISTS public.it_native_part CASCADE;
    DROP FUNCTION IF EXISTS _it_native_part_trigger();
END;
$tv$;
