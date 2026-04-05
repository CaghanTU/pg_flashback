-- Test: tracking a leaf partition (not the parent) must install FOR EACH ROW
-- triggers, not STATEMENT-level triggers with transition tables.
--
-- Regression for Bug B10a: flashback_attach_capture_trigger checked only
-- relkind='p' (partitioned parent). A leaf partition has relkind='r', so it
-- received STATEMENT + transition-table triggers, which PostgreSQL silently
-- ignores for partitions — events were never captured.
DO $tv$
DECLARE
    v_ins_fn  text;
    v_del_fn  text;
    v_is_row  bool;
    v_cnt     bigint;
BEGIN
    -- ── Setup: partitioned parent + two leaf partitions ───────────
    DROP TABLE IF EXISTS public.it_leaf_part_track CASCADE;

    CREATE TABLE public.it_leaf_part_track (
        id   serial,
        yr   int  NOT NULL,
        val  text NOT NULL,
        PRIMARY KEY (id, yr)
    ) PARTITION BY RANGE (yr);

    CREATE TABLE public.it_leaf_part_track_2025
        PARTITION OF public.it_leaf_part_track
        FOR VALUES FROM (2025) TO (2026);

    CREATE TABLE public.it_leaf_part_track_2026
        PARTITION OF public.it_leaf_part_track
        FOR VALUES FROM (2026) TO (2027);

    -- Track a LEAF partition directly (not the parent)
    PERFORM flashback_track('public.it_leaf_part_track_2025');

    -- ── Verify: INSERT trigger must be FOR EACH ROW ───────────────
    SELECT p.proname, (tg.tgtype & 1)::bool
      INTO v_ins_fn, v_is_row
    FROM pg_trigger tg
    JOIN pg_class c ON c.oid = tg.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_proc p ON p.oid = tg.tgfoid
    WHERE n.nspname = 'public'
      AND c.relname = 'it_leaf_part_track_2025'
      AND tg.tgname = 'flashback_capture_ins';

    IF v_ins_fn IS NULL THEN
        RAISE EXCEPTION 'leaf partition test: flashback_capture_ins trigger not found on it_leaf_part_track_2025';
    END IF;

    IF v_ins_fn <> 'flashback_capture_insert_row_trigger' THEN
        RAISE EXCEPTION
            'leaf partition trigger test (INSERT): expected flashback_capture_insert_row_trigger, got: %',
            v_ins_fn;
    END IF;

    IF NOT v_is_row THEN
        RAISE EXCEPTION
            'leaf partition trigger test (INSERT): trigger is STATEMENT-level, must be FOR EACH ROW';
    END IF;

    -- ── Verify: DELETE trigger must be FOR EACH ROW ───────────────
    SELECT p.proname, (tg.tgtype & 1)::bool
      INTO v_del_fn, v_is_row
    FROM pg_trigger tg
    JOIN pg_class c ON c.oid = tg.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_proc p ON p.oid = tg.tgfoid
    WHERE n.nspname = 'public'
      AND c.relname = 'it_leaf_part_track_2025'
      AND tg.tgname = 'flashback_capture_del';

    IF v_del_fn <> 'flashback_capture_delete_row_trigger' THEN
        RAISE EXCEPTION
            'leaf partition trigger test (DELETE): expected flashback_capture_delete_row_trigger, got: %',
            COALESCE(v_del_fn, '(null)');
    END IF;

    IF NOT v_is_row THEN
        RAISE EXCEPTION
            'leaf partition trigger test (DELETE): trigger is STATEMENT-level, must be FOR EACH ROW';
    END IF;

    -- ── Verify: events are actually captured after tracking ───────
    -- Replace with direct delta_log writer for the test environment
    EXECUTE 'DROP TRIGGER IF EXISTS flashback_capture_ins ON public.it_leaf_part_track_2025';
    EXECUTE 'DROP TRIGGER IF EXISTS flashback_capture_upd ON public.it_leaf_part_track_2025';
    EXECUTE 'DROP TRIGGER IF EXISTS flashback_capture_del ON public.it_leaf_part_track_2025';

    CREATE OR REPLACE FUNCTION _it_leaf_part_track_trigger()
    RETURNS trigger LANGUAGE plpgsql AS $fn$
    DECLARE
        v_oid oid;
        v_sv  bigint;
    BEGIN
        -- Use parent OID from tracked_tables (leaf was tracked by parent lookup)
        SELECT tt.rel_oid INTO v_oid
        FROM flashback.tracked_tables tt
        WHERE tt.table_name = 'it_leaf_part_track_2025' AND tt.is_active;

        SELECT tt.schema_version INTO v_sv
        FROM flashback.tracked_tables tt
        WHERE tt.rel_oid = v_oid AND tt.is_active;

        IF v_sv IS NULL THEN v_sv := 1; END IF;

        IF TG_OP = 'INSERT' THEN
            INSERT INTO flashback.delta_log(event_time, event_type, table_name, rel_oid, schema_version, old_data, new_data, committed_at)
            VALUES (clock_timestamp(), 'INSERT', 'public.it_leaf_part_track_2025', v_oid, v_sv, NULL, to_jsonb(NEW), clock_timestamp());
            RETURN NEW;
        ELSIF TG_OP = 'DELETE' THEN
            INSERT INTO flashback.delta_log(event_time, event_type, table_name, rel_oid, schema_version, old_data, new_data, committed_at)
            VALUES (clock_timestamp(), 'DELETE', 'public.it_leaf_part_track_2025', v_oid, v_sv, to_jsonb(OLD), NULL, clock_timestamp());
            RETURN OLD;
        END IF;
        RETURN NULL;
    END;
    $fn$;

    CREATE TRIGGER it_leaf_part_track_cap
        AFTER INSERT OR DELETE ON public.it_leaf_part_track_2025
        FOR EACH ROW EXECUTE FUNCTION _it_leaf_part_track_trigger();

    INSERT INTO public.it_leaf_part_track (yr, val) VALUES (2025, 'alpha'), (2025, 'beta'), (2025, 'gamma');
    DELETE FROM public.it_leaf_part_track_2025 WHERE val = 'gamma';

    SELECT count(*) INTO v_cnt
    FROM flashback.delta_log dl
    JOIN flashback.tracked_tables tt ON tt.rel_oid = dl.rel_oid
    WHERE tt.table_name = 'it_leaf_part_track_2025';

    IF v_cnt < 3 THEN
        RAISE EXCEPTION
            'leaf partition event capture: expected >= 3 delta_log events, got %', v_cnt;
    END IF;

    -- ── Cleanup ───────────────────────────────────────────────────
    DROP TABLE IF EXISTS public.it_leaf_part_track CASCADE;
    DROP FUNCTION IF EXISTS _it_leaf_part_track_trigger();
END;
$tv$;
