-- Test: diff-only UPDATE events are correctly restored.
DO $tv$
DECLARE
    v_boot jsonb;
    v_tracking_id bigint;
    v_point_lsn pg_lsn := '0/2000'::pg_lsn;
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

    SELECT flashback_test_bootstrap_lifecycle('public.it_diff_update') INTO v_boot;
    v_tracking_id := (v_boot->>'tracking_id')::bigint;

    UPDATE public.it_diff_update SET val = 99 WHERE id = 1;
    UPDATE public.it_diff_update SET name = 'robert', status = 'inactive' WHERE id = 2;
    UPDATE public.it_diff_update SET status = 'done' WHERE id = 3;

    PERFORM flashback_test_inject_commit(
        v_tracking_id,
        '0/3000'::pg_lsn,
        clock_timestamp(),
        946001,
        jsonb_build_array(
            jsonb_build_object('op', 'UPDATE', 'old', '{"id": 1, "val": 10}'::jsonb, 'new', '{"id": 1, "val": 99}'::jsonb),
            jsonb_build_object('op', 'UPDATE', 'old', '{"id": 2, "name": "bob", "status": "active"}'::jsonb, 'new', '{"id": 2, "name": "robert", "status": "inactive"}'::jsonb),
            jsonb_build_object('op', 'UPDATE', 'old', '{"id": 3, "status": "active"}'::jsonb, 'new', '{"id": 3, "status": "done"}'::jsonb)
        )
    );

    PERFORM flashback_test_restore_lsn('public.it_diff_update', v_point_lsn);

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
