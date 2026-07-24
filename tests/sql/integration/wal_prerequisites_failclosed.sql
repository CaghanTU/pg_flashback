-- WAL prerequisites fail closed in pg_test.
-- Note: wal_level cannot be flipped mid-test (requires restart); illegal mode
-- and non-dedicated-transaction paths are exercised here.
DO $tv$
DECLARE
    v_raised boolean;
    v_msg text;
BEGIN
    DROP TABLE IF EXISTS public.it_wal_prereq CASCADE;
    CREATE TABLE public.it_wal_prereq (id int PRIMARY KEY, val text);

    -- Illegal mode + write-dirty txn: no metadata leftover.
    PERFORM set_config('pg_flashback.capture_mode', 'trigger', true);
    v_raised := false;
    BEGIN
        PERFORM flashback_track('public.it_wal_prereq');
    EXCEPTION WHEN OTHERS THEN
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'illegal capture_mode track must fail';
    END IF;
    IF EXISTS (SELECT 1 FROM flashback.tracked_tables WHERE table_name = 'it_wal_prereq')
       OR EXISTS (
            SELECT 1 FROM flashback.coverage_generations g
            JOIN flashback.tracked_tables t ON t.tracking_id = g.tracking_id
            WHERE t.table_name = 'it_wal_prereq'
       )
       OR EXISTS (
            SELECT 1 FROM flashback.snapshots s
            JOIN pg_class c ON c.oid = s.rel_oid
            WHERE c.relname = 'it_wal_prereq'
       )
    THEN
        RAISE EXCEPTION 'illegal-mode track left leftover metadata';
    END IF;

    -- Legal wal mode still refuses the harness's write-dirty transaction.
    PERFORM set_config('pg_flashback.capture_mode', 'wal', true);
    -- A prior write already happened (CREATE TABLE); track must refuse.
    v_raised := false;
    BEGIN
        PERFORM flashback_track('public.it_wal_prereq');
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM NOT LIKE '%dedicated transaction%' THEN
            RAISE;
        END IF;
        v_raised := true;
        v_msg := SQLERRM;
    END;
    IF NOT v_raised THEN
        RAISE EXCEPTION 'track after a write in the same txn must fail closed';
    END IF;
    IF EXISTS (SELECT 1 FROM flashback.tracked_tables WHERE table_name = 'it_wal_prereq') THEN
        RAISE EXCEPTION 'non-dedicated-txn track left leftover metadata';
    END IF;

    DROP TABLE IF EXISTS public.it_wal_prereq CASCADE;
END;
$tv$;
