-- Test: restoring a leaf partition must keep it attached to its partitioned
-- parent.  Regression for Bug B10b: flashback_finalize_shadow_swap used
-- DROP TABLE CASCADE, which removed the partition from pg_inherits; after the
-- shadow rename the restored table was orphaned from the parent tree.
--
-- Fix: restore_helpers.sql now captures the partition parent and bound
-- expression BEFORE the DROP and issues ALTER TABLE parent ATTACH PARTITION
-- after the RENAME to reconnect the leaf to its parent.
DO $tv$
DECLARE
    t_before      timestamptz;
    v_cnt         bigint;
    v_leaf_oid    oid;
    v_parent_oid  oid;
    v_inh_count   bigint;
BEGIN
    -- ── Setup: partitioned parent + two leaf partitions ───────────
    DROP TABLE IF EXISTS public.it_leaf_restore CASCADE;
    DROP FUNCTION IF EXISTS _it_leaf_restore_trigger();

    CREATE TABLE public.it_leaf_restore (
        id   serial,
        yr   int  NOT NULL,
        val  text NOT NULL,
        PRIMARY KEY (id, yr)
    ) PARTITION BY RANGE (yr);

    CREATE TABLE public.it_leaf_restore_2025
        PARTITION OF public.it_leaf_restore
        FOR VALUES FROM (2025) TO (2026);

    CREATE TABLE public.it_leaf_restore_2026
        PARTITION OF public.it_leaf_restore
        FOR VALUES FROM (2026) TO (2027);

    -- Track only the 2025 leaf partition directly (not the parent)
    PERFORM flashback_track('public.it_leaf_restore_2025');

    v_leaf_oid   := 'public.it_leaf_restore_2025'::regclass::oid;
    v_parent_oid := 'public.it_leaf_restore'::regclass::oid;

    -- ── Replace real triggers with direct delta_log writers ───────
    -- The worker is not running in test mode; write events directly.
    EXECUTE 'DROP TRIGGER IF EXISTS flashback_capture_ins ON public.it_leaf_restore_2025';
    EXECUTE 'DROP TRIGGER IF EXISTS flashback_capture_upd ON public.it_leaf_restore_2025';
    EXECUTE 'DROP TRIGGER IF EXISTS flashback_capture_del ON public.it_leaf_restore_2025';

    CREATE OR REPLACE FUNCTION _it_leaf_restore_trigger()
    RETURNS trigger LANGUAGE plpgsql AS $fn$
    DECLARE
        v_oid oid;
        v_sv  bigint;
    BEGIN
        SELECT tt.rel_oid, tt.schema_version
          INTO v_oid, v_sv
        FROM flashback.tracked_tables tt
        WHERE tt.table_name = 'it_leaf_restore_2025' AND tt.is_active;

        IF v_oid IS NULL THEN RETURN NULL; END IF;
        IF v_sv  IS NULL THEN v_sv := 1; END IF;

        IF TG_OP = 'INSERT' THEN
            INSERT INTO flashback.delta_log(event_time,event_type,table_name,rel_oid,schema_version,old_data,new_data,committed_at)
            VALUES (clock_timestamp(),'INSERT','public.it_leaf_restore_2025',v_oid,v_sv,NULL,to_jsonb(NEW),clock_timestamp());
            RETURN NEW;
        ELSIF TG_OP = 'UPDATE' THEN
            INSERT INTO flashback.delta_log(event_time,event_type,table_name,rel_oid,schema_version,old_data,new_data,committed_at)
            VALUES (clock_timestamp(),'UPDATE','public.it_leaf_restore_2025',v_oid,v_sv,to_jsonb(OLD),to_jsonb(NEW),clock_timestamp());
            RETURN NEW;
        ELSIF TG_OP = 'DELETE' THEN
            INSERT INTO flashback.delta_log(event_time,event_type,table_name,rel_oid,schema_version,old_data,new_data,committed_at)
            VALUES (clock_timestamp(),'DELETE','public.it_leaf_restore_2025',v_oid,v_sv,to_jsonb(OLD),NULL,clock_timestamp());
            RETURN OLD;
        END IF;
        RETURN NULL;
    END;
    $fn$;

    CREATE TRIGGER it_leaf_restore_capture
        AFTER INSERT OR UPDATE OR DELETE ON public.it_leaf_restore_2025
        FOR EACH ROW EXECUTE FUNCTION _it_leaf_restore_trigger();

    -- ── Baseline DML: insert rows into 2025 leaf ──────────────────
    -- Also insert into 2026 (untracked) to confirm the parent still routes.
    INSERT INTO public.it_leaf_restore (yr, val) VALUES
        (2025, 'alpha'), (2025, 'beta'), (2025, 'gamma'),
        (2026, 'delta'), (2026, 'epsilon');

    t_before := clock_timestamp();
    PERFORM pg_sleep(0.01);

    -- ── Disaster: destroy all 2025 rows and corrupt one ──────────
    DELETE FROM public.it_leaf_restore_2025 WHERE val = 'gamma';
    UPDATE public.it_leaf_restore_2025 SET val = 'CORRUPTED' WHERE val = 'alpha';

    SELECT count(*) INTO v_cnt FROM public.it_leaf_restore_2025;
    IF v_cnt <> 2 THEN
        RAISE EXCEPTION 'pre-restore: expected 2 rows in 2025 leaf, got %', v_cnt;
    END IF;

    -- ── Restore the leaf partition to t_before ────────────────────
    PERFORM flashback_restore('public.it_leaf_restore_2025', t_before);

    -- ── B10b CRITICAL: verify leaf is still attached to parent ────
    SELECT count(*) INTO v_inh_count
    FROM pg_inherits
    WHERE inhrelid  = 'public.it_leaf_restore_2025'::regclass::oid
      AND inhparent = 'public.it_leaf_restore'::regclass::oid;

    IF v_inh_count = 0 THEN
        RAISE EXCEPTION
            'B10b regression: after restore, it_leaf_restore_2025 is detached from parent '
            '(pg_inherits entry missing) — DROP TABLE CASCADE orphaned the partition';
    END IF;

    -- ── Verify restored data is correct ──────────────────────────
    SELECT count(*) INTO v_cnt FROM public.it_leaf_restore_2025;
    IF v_cnt <> 3 THEN
        RAISE EXCEPTION 'post-restore: expected 3 rows in 2025 leaf, got %', v_cnt;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM public.it_leaf_restore_2025 WHERE val = 'alpha') THEN
        RAISE EXCEPTION 'post-restore: row alpha missing from 2025 leaf';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.it_leaf_restore_2025 WHERE val = 'beta') THEN
        RAISE EXCEPTION 'post-restore: row beta missing from 2025 leaf';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.it_leaf_restore_2025 WHERE val = 'gamma') THEN
        RAISE EXCEPTION 'post-restore: row gamma missing from 2025 leaf (delete not reversed)';
    END IF;

    -- ── Verify parent routes correctly to BOTH partitions ─────────
    -- 2026 rows must still be accessible via the parent (untouched partition)
    SELECT count(*) INTO v_cnt
    FROM public.it_leaf_restore
    WHERE yr = 2026;

    IF v_cnt <> 2 THEN
        RAISE EXCEPTION
            'post-restore: parent routing broken — expected 2 rows in yr=2026, got %', v_cnt;
    END IF;

    -- Full parent scan: all 5 rows (3 from 2025, 2 from 2026)
    SELECT count(*) INTO v_cnt FROM public.it_leaf_restore;
    IF v_cnt <> 5 THEN
        RAISE EXCEPTION
            'post-restore: parent table total count mismatch — expected 5, got %', v_cnt;
    END IF;

    -- ── Cleanup ───────────────────────────────────────────────────
    DROP TABLE IF EXISTS public.it_leaf_restore CASCADE;
    DROP FUNCTION IF EXISTS _it_leaf_restore_trigger();
END;
$tv$;
