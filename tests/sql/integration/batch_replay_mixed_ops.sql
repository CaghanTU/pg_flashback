-- Test: batch replay with mixed operations (INSERT/UPDATE/DELETE).
-- Verifies net-effect computation handles insert-update-delete chains
-- and correctly computes the final table state.
DO $tv$
DECLARE
    t_before timestamptz;
    v_rel_oid oid;
    v_count bigint;
    v_row record;
BEGIN
    DROP TABLE IF EXISTS public.it_batch_replay;
    CREATE TABLE public.it_batch_replay (
        id int PRIMARY KEY, name text, score int
    );
    INSERT INTO public.it_batch_replay VALUES
        (1, 'a', 10), (2, 'b', 20), (3, 'c', 30),
        (4, 'd', 40), (5, 'e', 50);

    PERFORM flashback_track('public.it_batch_replay');
    v_rel_oid := 'public.it_batch_replay'::regclass::oid;

    t_before := clock_timestamp();
    PERFORM pg_sleep(0.02);

    -- Generate mixed DML events (all going into delta_log directly)
    -- UPDATE id=1 (diff-only, only score changes)
    INSERT INTO flashback.delta_log(event_time, event_type, table_name, rel_oid,
        schema_version, old_data, new_data, committed_at)
    VALUES (clock_timestamp(), 'UPDATE', 'public.it_batch_replay', v_rel_oid, 1,
        '{"id": 1, "score": 10}'::jsonb,
        '{"id": 1, "score": 100}'::jsonb, clock_timestamp());

    -- DELETE id=2
    INSERT INTO flashback.delta_log(event_time, event_type, table_name, rel_oid,
        schema_version, old_data, new_data, committed_at)
    VALUES (clock_timestamp(), 'DELETE', 'public.it_batch_replay', v_rel_oid, 1,
        '{"id": 2, "name": "b", "score": 20}'::jsonb, NULL, clock_timestamp());

    -- INSERT new id=6
    INSERT INTO flashback.delta_log(event_time, event_type, table_name, rel_oid,
        schema_version, old_data, new_data, committed_at)
    VALUES (clock_timestamp(), 'INSERT', 'public.it_batch_replay', v_rel_oid, 1,
        NULL, '{"id": 6, "name": "f", "score": 60}'::jsonb, clock_timestamp());

    -- UPDATE id=3 twice (chain: update score, then update name)
    INSERT INTO flashback.delta_log(event_time, event_type, table_name, rel_oid,
        schema_version, old_data, new_data, committed_at)
    VALUES (clock_timestamp(), 'UPDATE', 'public.it_batch_replay', v_rel_oid, 1,
        '{"id": 3, "score": 30}'::jsonb,
        '{"id": 3, "score": 300}'::jsonb, clock_timestamp());

    INSERT INTO flashback.delta_log(event_time, event_type, table_name, rel_oid,
        schema_version, old_data, new_data, committed_at)
    VALUES (clock_timestamp(), 'UPDATE', 'public.it_batch_replay', v_rel_oid, 1,
        '{"id": 3, "name": "c"}'::jsonb,
        '{"id": 3, "name": "charlie"}'::jsonb, clock_timestamp());

    -- DELETE id=4 then re-INSERT with different data (net: new row)
    INSERT INTO flashback.delta_log(event_time, event_type, table_name, rel_oid,
        schema_version, old_data, new_data, committed_at)
    VALUES (clock_timestamp(), 'DELETE', 'public.it_batch_replay', v_rel_oid, 1,
        '{"id": 4, "name": "d", "score": 40}'::jsonb, NULL, clock_timestamp());

    INSERT INTO flashback.delta_log(event_time, event_type, table_name, rel_oid,
        schema_version, old_data, new_data, committed_at)
    VALUES (clock_timestamp(), 'INSERT', 'public.it_batch_replay', v_rel_oid, 1,
        NULL, '{"id": 4, "name": "delta", "score": 400}'::jsonb, clock_timestamp());

    -- Apply changes to actual table for correct base state
    UPDATE public.it_batch_replay SET score = 100 WHERE id = 1;
    DELETE FROM public.it_batch_replay WHERE id = 2;
    INSERT INTO public.it_batch_replay VALUES (6, 'f', 60);
    UPDATE public.it_batch_replay SET score = 300 WHERE id = 3;
    UPDATE public.it_batch_replay SET name = 'charlie' WHERE id = 3;
    DELETE FROM public.it_batch_replay WHERE id = 4;
    INSERT INTO public.it_batch_replay VALUES (4, 'delta', 400);

    -- Restore to before all changes — uses batch replay for PK table
    PERFORM flashback_restore('public.it_batch_replay', t_before);

    -- Verify original state restored
    SELECT count(*) INTO v_count FROM public.it_batch_replay;
    IF v_count <> 5 THEN
        RAISE EXCEPTION 'batch replay restore: expected 5 rows, got %', v_count;
    END IF;

    SELECT * INTO v_row FROM public.it_batch_replay WHERE id = 1;
    IF v_row.score <> 10 THEN
        RAISE EXCEPTION 'batch replay: id=1 score should be 10, got %', v_row.score;
    END IF;

    SELECT * INTO v_row FROM public.it_batch_replay WHERE id = 2;
    IF v_row IS NULL THEN
        RAISE EXCEPTION 'batch replay: id=2 should exist after restore';
    END IF;

    SELECT * INTO v_row FROM public.it_batch_replay WHERE id = 3;
    IF v_row.name <> 'c' OR v_row.score <> 30 THEN
        RAISE EXCEPTION 'batch replay: id=3 should be (c, 30), got (%, %)',
            v_row.name, v_row.score;
    END IF;

    SELECT * INTO v_row FROM public.it_batch_replay WHERE id = 4;
    IF v_row.name <> 'd' OR v_row.score <> 40 THEN
        RAISE EXCEPTION 'batch replay: id=4 should be (d, 40), got (%, %)',
            v_row.name, v_row.score;
    END IF;

    IF EXISTS (SELECT 1 FROM public.it_batch_replay WHERE id = 6) THEN
        RAISE EXCEPTION 'batch replay: id=6 should not exist after restore';
    END IF;
END;
$tv$;
