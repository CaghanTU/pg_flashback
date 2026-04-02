-- Test: diff-only UPDATE events are correctly restored.
-- Manually inserts diff-only UPDATE events (PK + changed cols only)
-- into delta_log and verifies flashback_restore applies them correctly.
DO $tv$
DECLARE
    t_before timestamptz;
    v_rel_oid oid;
    v_row record;
BEGIN
    DROP TABLE IF EXISTS public.it_diff_update;
    CREATE TABLE public.it_diff_update (
        id int PRIMARY KEY,
        name text,
        val int,
        status text DEFAULT 'active'
    );
    INSERT INTO public.it_diff_update VALUES
        (1, 'alice', 10, 'active'),
        (2, 'bob', 20, 'active'),
        (3, 'carol', 30, 'active');

    PERFORM flashback_track('public.it_diff_update');
    v_rel_oid := 'public.it_diff_update'::regclass::oid;

    t_before := clock_timestamp();
    PERFORM pg_sleep(0.02);

    -- Simulate diff-only UPDATE events (PK + changed cols only)
    -- UPDATE 1: id=1, change val from 10 to 99 (only id + val)
    INSERT INTO flashback.delta_log(event_time, event_type, table_name, rel_oid,
        schema_version, old_data, new_data, committed_at)
    VALUES (clock_timestamp(), 'UPDATE', 'public.it_diff_update', v_rel_oid,
        1, '{"id": 1, "val": 10}'::jsonb,
           '{"id": 1, "val": 99}'::jsonb,
        clock_timestamp());

    -- UPDATE 2: id=2, change name and status (id + name + status)
    INSERT INTO flashback.delta_log(event_time, event_type, table_name, rel_oid,
        schema_version, old_data, new_data, committed_at)
    VALUES (clock_timestamp(), 'UPDATE', 'public.it_diff_update', v_rel_oid,
        1, '{"id": 2, "name": "bob", "status": "active"}'::jsonb,
           '{"id": 2, "name": "robert", "status": "inactive"}'::jsonb,
        clock_timestamp());

    -- UPDATE 3: id=3, change only status (id + status)
    INSERT INTO flashback.delta_log(event_time, event_type, table_name, rel_oid,
        schema_version, old_data, new_data, committed_at)
    VALUES (clock_timestamp(), 'UPDATE', 'public.it_diff_update', v_rel_oid,
        1, '{"id": 3, "status": "active"}'::jsonb,
           '{"id": 3, "status": "done"}'::jsonb,
        clock_timestamp());

    -- Apply the updates directly to the table (so the restore has correct base)
    UPDATE public.it_diff_update SET val = 99 WHERE id = 1;
    UPDATE public.it_diff_update SET name = 'robert', status = 'inactive' WHERE id = 2;
    UPDATE public.it_diff_update SET status = 'done' WHERE id = 3;

    -- Restore to before the updates
    PERFORM flashback_restore('public.it_diff_update', t_before);

    -- Verify all rows restored to original values
    SELECT * INTO v_row FROM public.it_diff_update WHERE id = 1;
    IF v_row.val <> 10 OR v_row.name <> 'alice' OR v_row.status <> 'active' THEN
        RAISE EXCEPTION 'diff-only update restore failed for id=1: got val=%, name=%, status=%',
            v_row.val, v_row.name, v_row.status;
    END IF;

    SELECT * INTO v_row FROM public.it_diff_update WHERE id = 2;
    IF v_row.name <> 'bob' OR v_row.status <> 'active' THEN
        RAISE EXCEPTION 'diff-only update restore failed for id=2: got name=%, status=%',
            v_row.name, v_row.status;
    END IF;

    SELECT * INTO v_row FROM public.it_diff_update WHERE id = 3;
    IF v_row.status <> 'active' THEN
        RAISE EXCEPTION 'diff-only update restore failed for id=3: got status=%',
            v_row.status;
    END IF;
END;
$tv$;
