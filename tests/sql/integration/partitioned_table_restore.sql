-- Test: partitioned table track → insert → disaster → restore
-- The test trigger must record delta_log with the PARENT oid (not the
-- partition oid), because flashback_track registers the parent.
DO $tv$
DECLARE t_before timestamptz;
        v_cnt   bigint;
        v_parent_oid oid;
BEGIN
    DROP TABLE IF EXISTS public.it_partitioned CASCADE;
    DROP FUNCTION IF EXISTS flashback_test_partition_dml_trigger() CASCADE;

    CREATE TABLE public.it_partitioned (
        id    serial,
        val   text NOT NULL,
        ts    date NOT NULL,
        PRIMARY KEY (id, ts)
    ) PARTITION BY RANGE (ts);

    CREATE TABLE public.it_part_2025 PARTITION OF public.it_partitioned
        FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
    CREATE TABLE public.it_part_2026 PARTITION OF public.it_partitioned
        FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

    PERFORM flashback_track('public.it_partitioned');

    v_parent_oid := 'public.it_partitioned'::regclass::oid;

    -- Create a partition-aware test trigger that always records the parent OID.
    CREATE OR REPLACE FUNCTION flashback_test_partition_dml_trigger()
    RETURNS trigger
    LANGUAGE plpgsql AS $fn$
    DECLARE
        v_par oid;
        v_sv  bigint;
    BEGIN
        -- Resolve parent OID (if TG_RELID is a partition)
        SELECT inhparent INTO v_par FROM pg_inherits WHERE inhrelid = TG_RELID;
        IF v_par IS NULL THEN v_par := TG_RELID; END IF;

        SELECT schema_version INTO v_sv
        FROM flashback.tracked_tables WHERE rel_oid = v_par AND is_active;
        IF v_sv IS NULL THEN v_sv := 1; END IF;

        IF TG_OP = 'INSERT' THEN
            INSERT INTO flashback.delta_log(event_time,event_type,table_name,rel_oid,schema_version,old_data,new_data,committed_at)
            VALUES (clock_timestamp(),'INSERT','public.it_partitioned',v_par,v_sv,NULL,to_jsonb(NEW),clock_timestamp());
            RETURN NEW;
        ELSIF TG_OP = 'UPDATE' THEN
            INSERT INTO flashback.delta_log(event_time,event_type,table_name,rel_oid,schema_version,old_data,new_data,committed_at)
            VALUES (clock_timestamp(),'UPDATE','public.it_partitioned',v_par,v_sv,to_jsonb(OLD),to_jsonb(NEW),clock_timestamp());
            RETURN NEW;
        ELSIF TG_OP = 'DELETE' THEN
            INSERT INTO flashback.delta_log(event_time,event_type,table_name,rel_oid,schema_version,old_data,new_data,committed_at)
            VALUES (clock_timestamp(),'DELETE','public.it_partitioned',v_par,v_sv,to_jsonb(OLD),NULL,clock_timestamp());
            RETURN OLD;
        END IF;
        RETURN NULL;
    END;
    $fn$;

    -- Drop any existing triggers on parent
    EXECUTE 'DROP TRIGGER IF EXISTS flashback_capture_ins ON public.it_partitioned';
    EXECUTE 'DROP TRIGGER IF EXISTS flashback_capture_upd ON public.it_partitioned';
    EXECUTE 'DROP TRIGGER IF EXISTS flashback_capture_del ON public.it_partitioned';

    -- Attach on parent; PG propagates AFTER ROW triggers to all partitions
    CREATE TRIGGER flashback_test_capture
        AFTER INSERT OR UPDATE OR DELETE ON public.it_partitioned
        FOR EACH ROW EXECUTE FUNCTION flashback_test_partition_dml_trigger();

    INSERT INTO public.it_partitioned (val, ts) VALUES
        ('a', '2025-06-01'),
        ('b', '2025-11-15'),
        ('c', '2026-02-14'),
        ('d', '2026-08-20');

    t_before := clock_timestamp();

    DELETE FROM public.it_partitioned WHERE ts >= '2026-01-01';

    SELECT count(*) INTO v_cnt FROM public.it_partitioned;
    IF v_cnt <> 2 THEN
        RAISE EXCEPTION 'pre-restore: expected 2 rows, got %', v_cnt;
    END IF;

    PERFORM flashback_restore('public.it_partitioned', t_before);

    SELECT count(*) INTO v_cnt FROM public.it_partitioned;
    IF v_cnt <> 4 THEN
        RAISE EXCEPTION 'post-restore: expected 4 rows, got %', v_cnt;
    END IF;

    -- Verify partition routing still works
    IF NOT EXISTS (SELECT 1 FROM public.it_part_2025 WHERE val = 'a') THEN
        RAISE EXCEPTION 'partition routing broken: 2025 partition missing row a';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.it_part_2026 WHERE val = 'c') THEN
        RAISE EXCEPTION 'partition routing broken: 2026 partition missing row c';
    END IF;
END;
$tv$;
